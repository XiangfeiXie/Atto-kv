/*=============================================================================
 *  rdma.c — kvstore 用 RDMA 做全量同步的控制层
 *
 *  职责：用 RDMA 传输引擎（rdma_transport.c）做全量同步
 *   - 服务端：整个 RDB 序列化进发送暂存块，一次性流水线推送
 *   - 客户端：等尾部 IMM 到达后，就地一次性解析加载
 *
 *  传输原语（连接/分块窗口/IMM 通知）都在 rdma_transport.c，
 *  本层只做 kvstore 业务控制（RDB 序列化 / 加载 / 性能统计），不触碰 verbs。
 *  上层由 repl_fullsync.c 按配置开关直接调用。
 *
 *  线上格式（单块，大小上限 = RDMA_BUF_SIZE）：
 *   [流头 token 8B][RDB 流: key_count 4B + entries...]
 *   数据长度由 RDB 流头 key_count 自描述：只解析 key_count 条，
 *   数量不一致默认加载失败（无需 magic/total_len 等校验字段）
 *============================================================================*/

#define _GNU_SOURCE
#include "rdma.h"
#include "rdma_transport.h"
#include "config.h"       /* kvs_config */
#include "kv_dict.h"      /* g_dict / dict_reserve / dict_clear */
#include "persistence.h"  /* rdb_serialize_* / rdb_load_stream_* */
#include "kvstore.h"      /* now_monotonic_ns */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <endian.h>      /* htobe64 / be64toh */

/*=============================================================================
 *  工具
 *============================================================================*/

#define RDB_STREAM_HDR_LEN  4   /* RDB 流头：key_count uint32（序列化器先写 4B 头部） */

/*=============================================================================
 *  入口
 *============================================================================*/

/**
 * RDMA 服务端入口
 *
 * 流程：
 * 1. 整个 RDB 序列化进调用方自持的暂存块（流头 + key_count + entries）
 * 2. rdma_xport_send：监听 + 接受 + 分块流水线推送（内部含完成排空）
 *
 * @param token 快照 offset（会话元数据）
 * @param ready_fd 就绪管道 fd：监听成功后写 1 字节通知父进程
 * @return 0 成功，-1 失败
 */
int rdma_server_run(uint64_t token, int ready_fd)
{
    setlinebuf(stdout); setlinebuf(stderr);
    const char *ip = kvs_config.fullsync_listen_ip;
    if (!strcmp(ip, "0.0.0.0"))
        ip = NULL;

    /* 先建连接：监听必须在对端连接之前就绪。大快照序列化耗时数百 ms,
     * 若后起监听, 对端 REQ 落在监听就绪前会被丢弃、rdma_connect 永远
     * 等不到事件, 双方挂死。因此 conn_open 先行, 再序列化, 最后直写 */
    rdma_xport_conn_t *conn = NULL;
    uint64_t t0 = now_monotonic_ns();   /* 连接建立计时起点 */
    if (rdma_xport_conn_open(ip, kvs_config.fullsync_port, ready_fd, &conn) < 0)
        return -1;
    uint64_t t1 = now_monotonic_ns();   /* 连接就绪: xfer 从此起算(序列化+直写, 不含连接建立) */

    /* 发送暂存块（调用方自持，整份快照序列化目标） */
    char *buf = malloc(RDMA_BUF_SIZE);
    if (!buf) {
        fprintf(stderr, "[rdma] malloc(%zu) failed\n", (size_t)RDMA_BUF_SIZE);
        rdma_xport_conn_close(conn);
        return -1;
    }

    /* 快照流头：仅 token（快照 offset），数据长度由 RDB 流头 key_count 自描述 */
    struct batch_hdr bh = { .token = htobe64(token) };
    memcpy(buf, &bh, sizeof(bh));
    size_t off = sizeof(bh);

    /* 流式序列化整个 RDB 写入暂存块 */
    uint64_t prep_ns = 0;
    rdb_serializer_t rs;
    rdb_serialize_begin(&rs);
    while (1) {
        int done = 0;
        uint64_t s1 = now_monotonic_ns();
        size_t n = rdb_serialize_next(&rs, buf + off, RDMA_BUF_SIZE - off, &done);
        prep_ns += now_monotonic_ns() - s1;
        off += n;

        if (done)
            break;
        if (n == 0) {
            fprintf(stderr,
                    "[rdma] snapshot exceeds transport buffer (%zu B), aborting\n",
                    (size_t)RDMA_BUF_SIZE);
            rdb_serialize_end(&rs);
            free(buf);
            return -1;
        }
    }
    rdb_serialize_end(&rs);

    /* 传输阶段：分块流水线直写（连接已就绪, 内含完成排空 + 断连清理） */
    printf("[rdma] xfer-begin\n");
    int rc = rdma_xport_conn_send(conn, buf, off);
    uint64_t t2 = now_monotonic_ns();
    free(buf);
    if (rc < 0)
        return -1;

    /* 性能统计（与 sendfile 路径同口径: 不含连接建立）
     * xfer = 序列化 + 直写 (t1 在连接就绪后起算), mbps 按此计算 */
    double conn_ms = (t1 > t0) ? (double)(t1 - t0) / 1e6 : 0.0;
    double prep_ms = (double)prep_ns / 1e6;
    double xfer_ms = (t2 > t1) ? (double)(t2 - t1) / 1e6 : 0.0;
    double mbps = (xfer_ms > 0.0)
                ? (off / (xfer_ms / 1000.0) / (1024.0 * 1024.0)) : 0.0;
    printf("[rdma] xfer-end: bytes=%zu conn=%.1f prep=%.1f xfer=%.1f mbps=%.2f batches=%d\n",
           off, conn_ms, prep_ms, xfer_ms, mbps, 1);

    return 0;
}

/**
 * RDMA 客户端入口
 *
 * 流程：
 * 1. rdma_xport_recv：连接 + 注册接收缓冲 + 发缓冲凭证 + 等尾部 IMM
 * 2. 解出流头 token → 按 RDB 流头 key_count 限条数加载 → 数量不一致即失败
 *
 * @param out_token 输出：从流头解出的 token（快照 offset）
 * @return 0 成功，-1 失败
 */
int rdma_client_run(uint64_t *out_token)
{
    setlinebuf(stdout); setlinebuf(stderr);

    /* 接收缓冲（调用方自持，远程写目标） */
    char *buf = calloc(1, RDMA_BUF_SIZE);
    if (!buf) {
        fprintf(stderr, "[rdma] malloc(%zu) failed\n", (size_t)RDMA_BUF_SIZE);
        return -1;
    }

    uint64_t t1 = now_monotonic_ns();
    printf("[rdma] xfer-begin\n");
    int rc = rdma_xport_recv(kvs_config.fullsync_master_ip,
                             kvs_config.fullsync_port, buf, RDMA_BUF_SIZE);
    uint64_t t2 = now_monotonic_ns();
    if (rc < 0) {
        fprintf(stderr, "[rdma] recv failed\n");
        free(buf);
        return -1;
    }

    /* 快照流头：token（快照 offset）+ RDB 流（key_count 头 + entries） */
    struct batch_hdr bh;
    memcpy(&bh, buf, sizeof(bh));
    uint64_t snap = be64toh(bh.token);
    if (out_token)
        *out_token = snap;

    /* 数据长度由 RDB 流头 key_count 自描述：
     * 只解析 key_count 条，数量不一致默认加载失败（无 magic 校验） */
    int status = 0;
    long loaded = -1;
    size_t consumed = 0;
    const char *stream = buf + sizeof(bh);            /* 跳过流头 token */
    int expected = rdb_load_stream_begin(stream, RDB_STREAM_HDR_LEN);
    if (expected >= 0) {
        if (expected > 0)
            dict_reserve(&g_dict, (uint32_t)expected);
        loaded = rdb_load_stream_append_n(stream + RDB_STREAM_HDR_LEN,
                                          RDMA_BUF_SIZE - sizeof(bh)
                                          - RDB_STREAM_HDR_LEN,
                                          expected, &consumed);
        if (loaded != expected) {
            fprintf(stderr, "[rdma] key count mismatch: expected %d, got %ld\n",
                    expected, loaded);
            dict_clear(&g_dict);
            status = -1;                      /* 疑似传输截断 */
        }
    } else {
        status = -1;
    }
    free(buf);

    /* 性能统计 */
    size_t total_bytes = (status == 0)
                       ? sizeof(bh) + RDB_STREAM_HDR_LEN + consumed : 0;
    double ms = (t2 > t1) ? (double)(t2 - t1) / 1e6 : 0.0;
    double mbps = (ms > 0)
                ? (total_bytes / (ms / 1000.0) / (1024.0 * 1024.0))
                : 0;
    printf("[rdma] xfer-end: entries=%ld bytes=%zu ms=%.1f mbps=%.2f batches=%d\n",
           (status == 0) ? loaded : 0, total_bytes, ms, mbps, 1);

    return status;
}
