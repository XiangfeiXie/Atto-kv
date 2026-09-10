/*=============================================================================
 *  rdma.c — RDMA 全量同步的控制层
 *
 *  分层定位（自上而下，每层只依赖下一层）：
 *    repl_fullsync.c    全量同步编排 + sendfile/RDMA 按配置分派（fork 传输子进程）
 *    rdma.c（本文件）   业务控制：RDB 无盘流 序列化 / 加载 / 性能统计，不触碰 verbs
 *    rdma_transport.c   verbs 数据面：连接 / 分块窗口 / IMM 通知
 *
 *  本文件对标 Redis diskless 全量同步：RDMA 只是本项目的一种传输方式。与
 *  sendfile 路径（fork 子进程落盘 dump.rdb 后转送）同构——都是"fork 子进程
 *  COW 快照 → 数据面子进程发送 → slave 就地加载"，差异只在数据面字节如何送达：
 *    - 服务端（rdma_server_run）：整份 RDB 无盘流序列化进发送暂存块，
 *      rdma_xport 一次性流水线直写对端
 *    - 客户端（rdma_client_run）：等尾部 IMM 到达后就地一次性解析加载
 *
 *  线上格式（单块，大小上限 = RDMA_BUF_SIZE）：
 *   [流头 token 8B][RDB 无盘流: key_count 4B + entries...]
 *   RDB 无盘流与磁盘 dump.rdb 字节序完全一致（③ 加载端同一解析，见
 *   persist_rdb.c），唯一差别是不带 crc 槽：无盘传输不落盘、无防篡改需求，
 *   完整性靠 RC 可靠有序 + key_count 数量核对。数据长度由 RDB 流头
 *   key_count 自描述（slave 只解析 key_count 条），数量不一致即加载失败，
 *   无需 magic/total_len 等额外校验字段。
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

/* RDB 无盘流头长度（RDMA 全量）：仅 key_count[4]，无 crc 槽
 * 磁盘 dump.rdb 头是 RDB_FILE_HDR_LEN=8（key_count[4] + crc 槽[4]），那是
 * SAVE 落盘路径的事，与 RDMA 无关。两格式条目字节序一致（同一 ③ 加载端
 * 解析，见 persist_rdb.c），只差这个 crc 槽 */
#define RDB_STREAM_HDR_LEN  4

/*=============================================================================
 *  入口
 *============================================================================*/

/**
 * RDMA 服务端入口（master 全量同步传输子进程，fork 由 repl_fullsync.c 触发）
 *
 * 流程：
 * 1. rdma_xport_conn_open：监听 + 接受 + 收对端接收缓冲凭证。
 *    必须先于序列化（见调用点注释：对端 REQ 早到会被丢、双方挂死）
 * 2. 整个 RDB 无盘流（key_count + entries，无 crc 槽）序列化进暂存块
 *    （④ 序列化器，persist_rdb.c；装不下 RDMA_BUF_SIZE 即失败，不留半截快照）
 * 3. rdma_xport_conn_send：分块流水线直写对端（内部含完成排空，末块 IMM 通知）
 *
 * @param token 快照 offset（会话元数据，随流头直传对端续传）
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
    rdb_serialize_begin(&rs, 0);   /* RDMA 无盘流：无 crc 槽，纯 key_count + entries */
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
 * RDMA 客户端入口（slave 端全量同步：+FULLRESYNC 握手后由 repl_fullsync.c 编排）
 *
 * 流程：
 * 1. rdma_xport_recv：连接 + 注册接收缓冲 + 发缓冲凭证 + 等尾部 IMM（整块落地）
 * 2. 解出流头 token → 按 RDB 流头 key_count 限条数就地解析加载
 *    （无盘流无 crc 槽：完整性 = RC 可靠有序 + loaded==expected 数量核对）
 *
 * 长度语义：真实数据字节数无法先验（条目变长），故 append_n 喂的是"流头后
 * 的整块尾部缓冲"，以 limit=key_count 在第 key_count 条处截停，consumed 输出
 * 真实消费字节；尾部未用缓冲是 calloc 的空白，不会被误解析。
 *
 * @param out_token 输出：从流头解出的 token（快照 offset，供增量续传对齐）
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

    /* 快照流头：token（快照 offset）+ RDB 无盘流（key_count + entries） */
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
