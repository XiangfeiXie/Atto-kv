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
#include "kvstore.h"
#include "kv_dict.h"
#include "persistence.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <endian.h>      /* htobe64 / be64toh */

/*=============================================================================
 *  入口
 *============================================================================*/

/**
 * RDMA 服务端入口
 *
 * 流程：
 * 1. rdma_xport_server_accept：监听 + 接受连接 + 收对端缓冲凭证
 * 2. 整个 RDB 序列化进暂存块（头 + key_count + entries）
 * 3. rdma_xport_send：分块流水线推送（内部含完成排空）
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

    rdma_xport_t *xport = rdma_xport_server_accept(ip,
                                                   kvs_config.fullsync_port,
                                                   ready_fd);
    if (!xport)
        return -1;

    printf("[rdma] xfer-begin\n");
    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t1);

    size_t capacity = 0;
    char *dst = rdma_xport_send_buf(xport, &capacity);
    if (!dst) {
        fprintf(stderr, "[rdma] no send buffer\n");
        rdma_xport_destroy(xport);
        return -1;
    }

    /* 快照流头：仅 token（快照 offset），数据长度由 RDB 流头 key_count 自描述 */
    struct batch_hdr bh = { .token = htobe64(token) };
    memcpy(dst, &bh, sizeof(bh));
    size_t off = sizeof(bh);

    /* 流式序列化整个 RDB 写入暂存块 */
    uint64_t prep_ns = 0;
    rdb_serializer_t rs;
    rdb_serialize_begin(&rs);
    while (1) {
        int done = 0;
        struct timespec s1, s2;
        clock_gettime(CLOCK_MONOTONIC, &s1);
        size_t n = rdb_serialize_next(&rs, dst + off, capacity - off, &done);
        clock_gettime(CLOCK_MONOTONIC, &s2);
        prep_ns += (uint64_t)(s2.tv_sec - s1.tv_sec) * 1000000000ULL
                 + (uint64_t)(s2.tv_nsec - s1.tv_nsec);
        off += n;

        if (done)
            break;
        if (n == 0) {
            fprintf(stderr,
                    "[rdma] snapshot exceeds transport buffer (%zu B), aborting\n",
                    capacity);
            rdb_serialize_end(&rs);
            rdma_xport_destroy(xport);
            return -1;
        }
    }
    rdb_serialize_end(&rs);

    if (rdma_xport_send(xport, dst, off) < 0) {
        rdma_xport_destroy(xport);
        return -1;
    }

    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    /* 性能统计（与 sendfile 路径日志键保持一致） */
    uint64_t t1_ns = (uint64_t)t1.tv_sec * 1000000000ULL
                   + (uint64_t)t1.tv_nsec;
    uint64_t wall_ns = ((uint64_t)ts.tv_sec * 1000000000ULL
                      + (uint64_t)ts.tv_nsec) > t1_ns
                     ? ((uint64_t)ts.tv_sec * 1000000000ULL
                      + (uint64_t)ts.tv_nsec) - t1_ns : 0;
    uint64_t xfer_ns = (wall_ns > prep_ns) ? (wall_ns - prep_ns) : 0;
    double prep_ms = (double)prep_ns / 1e6;
    double xfer_ms = (double)xfer_ns / 1e6;
    double wall_ms = (double)wall_ns / 1e6;
    double mbps = (xfer_ms > 0.0)
                ? (off / (xfer_ms / 1000.0) / (1024.0 * 1024.0)) : 0.0;
    printf("[rdma] xfer-end: bytes=%zu wall=%.1f prep=%.1f xfer=%.1f mbps=%.2f batches=%d\n",
           off, wall_ms, prep_ms, xfer_ms, mbps, 1);

    rdma_xport_destroy(xport);
    return 0;
}

/**
 * RDMA 客户端入口
 *
 * 流程：
 * 1. rdma_xport_client_connect：连接 + 注册接收缓冲 + 发缓冲凭证
 * 2. rdma_xport_wait_done：等尾部 IMM（整块已落入接收缓冲）
 * 3. 解出流头 token → 按 RDB 流头 key_count 限条数加载 → 数量不一致即失败
 *
 * @param out_token 输出：从流头解出的 token（快照 offset）
 * @return 0 成功，-1 失败
 */
int rdma_client_run(uint64_t *out_token)
{
    setlinebuf(stdout); setlinebuf(stderr);

    rdma_xport_t *xport = rdma_xport_client_connect(kvs_config.fullsync_master_ip,
                                                    kvs_config.fullsync_port);
    if (!xport)
        return -1;

    printf("[rdma] xfer-begin\n");
    struct timespec t1, t2;
    clock_gettime(CLOCK_MONOTONIC, &t1);

    const char *data = rdma_xport_recv_buf(xport);
    if (!data || rdma_xport_wait_done(xport) < 0) {
        fprintf(stderr, "[rdma] recv failed\n");
        rdma_xport_destroy(xport);
        return -1;
    }

    /* 快照流头：token（快照 offset）+ RDB 流（key_count 头 + entries） */
    struct batch_hdr bh;
    memcpy(&bh, data, sizeof(bh));
    uint64_t snap = be64toh(bh.token);
    if (out_token)
        *out_token = snap;

    /* 数据长度由 RDB 流头 key_count 自描述：
     * 只解析 key_count 条，数量不一致默认加载失败（无 magic 校验） */
    int status = 0;
    long loaded = -1;
    size_t consumed = 0;
    int expected = rdb_load_stream_begin(data + sizeof(bh), 4);
    if (expected >= 0) {
        if (expected > 0)
            dict_reserve(&g_dict, (uint32_t)expected);
        loaded = rdb_load_stream_append_n(data + sizeof(bh) + 4,
                                          RDMA_BUF_SIZE - sizeof(bh) - 4,
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

    clock_gettime(CLOCK_MONOTONIC, &t2);

    /* 性能统计 */
    size_t total_bytes = (status == 0) ? sizeof(bh) + 4 + consumed : 0;
    double ms = (t2.tv_sec - t1.tv_sec) * 1000.0
              + (t2.tv_nsec - t1.tv_nsec) / 1e6;
    double mbps = (ms > 0)
                ? (total_bytes / (ms / 1000.0) / (1024.0 * 1024.0))
                : 0;
    printf("[rdma] xfer-end: entries=%ld bytes=%zu ms=%.1f mbps=%.2f batches=%d\n",
           (status == 0) ? loaded : 0, total_bytes, ms, mbps, 1);

    rdma_xport_destroy(xport);
    return status;
}
