/*=============================================================================
 *  rdma.c — kvstore 用 RDMA 做全量同步的控制层
 *
 *  职责：用 RDMA 传输引擎（rdma_transport.c）做全量同步
 *   - 服务端：整个 RDB 序列化进发送缓冲，一次性流式推送
 *   - 客户端：一次性接收整个快照，RDB stream 解析加载，解出快照 offset
 *
 *  传输原语 + 传输协议（连接/chunk 流式/IMM_DONE）都在 rdma_transport.c
 *  本层只做 kvstore 业务控制（RDB 序列化 / 加载 / 性能统计），不触碰 verbs
 *  上层由 repl_fullsync.c 按配置开关直接调用
 *
 *  传输格式（一次连接传一整份快照）：
 *   [4B total_len][batch_hdr + RDB header + RDB entries]
 *   单份快照 ≤ 传输缓冲大小（RDMA_BUF_SIZE），超限报错中止
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
#include <arpa/inet.h>   /* htonl / ntohl */
#include <endian.h>      /* htobe64 / be64toh */

/*=============================================================================
 *  服务端：整个快照序列化进发送缓冲
 *============================================================================*/

/**
 * 整个 RDB 序列化进发送缓冲（单缓冲，无需分批）
 *
 * @param xport 传输句柄
 * @param bh 快照头部 batch_hdr（magic + token）
 * @param out_payload_len 输出：负载字节数（不含长度前缀）
 * @param prep_ns 输出：累加所有 rdb_serialize_next() 耗时
 * @return 0 成功，-1 失败（快照超过传输缓冲）
 */
static int serialize_all(rdma_xport_t *xport, const struct batch_hdr *bh,
                         uint32_t *out_payload_len, uint64_t *prep_ns) {
    rdb_serializer_t rs;
    rdb_serialize_begin(&rs);

    size_t capacity;
    char *dst = rdma_xport_send_buf(xport, &capacity);
    if (!dst) { rdb_serialize_end(&rs); return -1; }

    /* 快照头 */
    memcpy(dst, bh, sizeof(*bh));
    size_t off = sizeof(*bh);
    *prep_ns = 0;

    /* 流式序列化整个 RDB 写入发送缓冲 */
    while (1) {
        int done = 0;
        struct timespec s1, s2;
        clock_gettime(CLOCK_MONOTONIC, &s1);
        size_t n = rdb_serialize_next(&rs, dst + off, capacity - off, &done);
        clock_gettime(CLOCK_MONOTONIC, &s2);
        *prep_ns += (uint64_t)(s2.tv_sec - s1.tv_sec) * 1000000000ULL
                  + (uint64_t)(s2.tv_nsec - s1.tv_nsec);
        off += n;

        if (done) break;

        /**
         * 守卫：缓冲填满但快照未序列化完（或单条 entry 超过剩余空间）
         * demo 传输缓冲固定 256MB，快照超限直接报错中止
         */
        if (n == 0 || off >= capacity) {
            fprintf(stderr, "[rdma] snapshot exceeds transport buffer (%zu B), aborting fullsync\n",
                    capacity);
            rdb_serialize_end(&rs);
            return -1;
        }
    }

    rdb_serialize_end(&rs);
    *out_payload_len = (uint32_t)off;
    return 0;
}

/*=============================================================================
 *  客户端：一次性加载整块快照
 *============================================================================*/

/**
 * 加载整块快照（只调一次）
 *
 * 载荷开头是 batch_hdr（magic 校验 + token），随后是 RDB header + entries
 * 返回成功加载的 entry 数（-1 表示致命错误）
 * 从流头解出的 token 写入 *out_snap 交还上层（全量同步里 = 快照 offset）
 */
static int load_batch(const char *data, uint32_t data_len,
                      int is_first, int *stream_began, uint64_t *out_snap,
                      int *out_expected) {
    if (is_first) {
        const struct batch_hdr *bh = (const struct batch_hdr *)data;
        if (ntohl(bh->magic) != RDMA_MAGIC) {
            fprintf(stderr, "[rdma] bad magic 0x%x\n", ntohl(bh->magic));
            return -1;
        }
        if (out_snap) *out_snap = be64toh(bh->token);
        data     += sizeof(*bh);
        data_len -= sizeof(*bh);

        /* RDB 头仅 4 字节 key_count（与 rdb_serialize_next / rdb_save_sync 一致） */
        int expected = rdb_load_stream_begin(data,
                                             data_len >= 4 ? 4 : data_len);
        if (expected < 0) return -1;
        *stream_began = 1;
        if (out_expected) *out_expected = expected;

        /* 批量插入前预分配桶，避免加载过程中反复扩容 */
        if (expected > 0) {
            dict_reserve(&g_dict, (uint32_t)expected);
        }

        /* 头部之后的 entry 数据 */
        if (data_len > 4) {
            int n = rdb_load_stream_append(data + 4, data_len - 4);
            return (n >= 0) ? n : -1;
        }
        return 0;
    } else {
        if (!*stream_began) {
            fprintf(stderr, "[rdma] entry batch before header\n");
            return -1;
        }
        if (data_len == 0) return 0;
        int n = rdb_load_stream_append(data, data_len);
        return (n >= 0) ? n : -1;
    }
}

/*=============================================================================
 *  入口
 *============================================================================*/

/**
 * RDMA 服务端入口
 *
 * 流程：
 * 1. rdma_xport_server_accept：监听 + 接受连接 + 交换缓冲信息
 * 2. 整个 RDB 序列化进发送缓冲
 * 3. rdma_xport_send：chunk 窗口流式一次性推送（尾部 IMM_DONE）
 *
 * @param token 快照 offset（会话元数据）
 * @param ready_fd 就绪管道 fd：监听成功后写 1 字节通知父进程
 * @return 0 成功，-1 失败
 */
int rdma_server_run(uint64_t token, int ready_fd) {
    setlinebuf(stdout); setlinebuf(stderr);
    const char *ip = kvs_config.fullsync_listen_ip;
    if (!strcmp(ip, "0.0.0.0")) ip = NULL;

    rdma_xport_t *xport = rdma_xport_server_accept(ip,
                                                   kvs_config.fullsync_port,
                                                   ready_fd);
    if (!xport) return -1;

    /* 协议信封（快照头） */
    struct batch_hdr bh = {
        .magic = htonl(RDMA_MAGIC),
        .token = htobe64(token)
    };

    printf("[rdma] xfer-begin\n");
    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t1);

    uint64_t prep_ns = 0;
    uint32_t payload_len = 0;
    if (serialize_all(xport, &bh, &payload_len, &prep_ns) < 0) {
        rdma_xport_destroy(xport);
        return -1;
    }

    /* 一次性流式发送（send CQ 全部回收后返回） */
    if (rdma_xport_send(xport, payload_len) < 0) {
        rdma_xport_destroy(xport);
        return -1;
    }

    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    uint64_t xfer_end_ns = (uint64_t)ts.tv_sec * 1000000000ULL
                         + (uint64_t)ts.tv_nsec;

    size_t total_bytes = (size_t)payload_len + 4;  /* 长度前缀 */

    /* 性能统计 */
    uint64_t t1_ns = (uint64_t)t1.tv_sec * 1000000000ULL
                   + (uint64_t)t1.tv_nsec;
    uint64_t wall_ns = (xfer_end_ns > t1_ns) ? (xfer_end_ns - t1_ns) : 0;
    uint64_t xfer_ns = (wall_ns > prep_ns) ? (wall_ns - prep_ns) : 0;
    double prep_ms   = (double)prep_ns / 1e6;
    double xfer_ms   = (double)xfer_ns / 1e6;
    double wall_ms   = (double)wall_ns / 1e6;
    double mbps      = (xfer_ms > 0.0)
                     ? (total_bytes / (xfer_ms / 1000.0) / (1024.0 * 1024.0)) : 0.0;
    printf("[rdma] xfer-end: bytes=%zu wall=%.1f prep=%.1f xfer=%.1f mbps=%.2f batches=%d\n",
           total_bytes, wall_ms, prep_ms, xfer_ms, mbps, 1);

    rdma_xport_destroy(xport);
    return 0;
}

/**
 * RDMA 客户端入口
 *
 * 流程：
 * 1. rdma_xport_client_connect：连接 + 注册接收缓冲 + 交换缓冲信息
 * 2. rdma_xport_recv：一次性接收整块快照（阻塞等尾部 IMM_DONE）
 * 3. load_batch：整块加载到字典，解出快照 offset
 *
 * @param out_token 输出：从流头解出的 token（快照 offset）
 * @return 0 成功，-1 失败
 */
int rdma_client_run(uint64_t *out_token) {
    setlinebuf(stdout); setlinebuf(stderr);

    rdma_xport_t *xport = rdma_xport_client_connect(kvs_config.fullsync_master_ip,
                                                    kvs_config.fullsync_port);
    if (!xport) return -1;

    printf("[rdma] xfer-begin\n");
    struct timespec t1, t2;
    clock_gettime(CLOCK_MONOTONIC, &t1);

    /* 一次性接收整块快照（等 IMM_DONE，数据已全部落进缓冲） */
    const void *payload;
    uint32_t payload_len;
    if (rdma_xport_recv(xport, &payload, &payload_len) < 0) {
        fprintf(stderr, "[rdma] recv failed\n");
        rdma_xport_destroy(xport);
        return -1;
    }

    /* 整块加载（首次即最后一次） */
    int stream_began = 0, expected = 0;
    uint64_t snap = 0;
    int total_e = load_batch((const char *)payload, payload_len, 1,
                             &stream_began, &snap, &expected);

    clock_gettime(CLOCK_MONOTONIC, &t2);

    /* 性能统计 */
    size_t total_bytes = (size_t)payload_len + 4;
    double ms = (t2.tv_sec - t1.tv_sec) * 1000.0
              + (t2.tv_nsec - t1.tv_nsec) / 1e6;
    double mbps = (ms > 0)
                ? (total_bytes / (ms / 1000.0) / (1024.0 * 1024.0))
                : 0;
    printf("[rdma] xfer-end: entries=%d bytes=%zu ms=%.1f mbps=%.2f batches=%d\n",
           total_e, total_bytes, ms, mbps, 1);

    if (out_token) *out_token = snap;

    /* 与文件路径 rdb_load_from_buf 对齐：核对实际加载数 == 头里 key_count */
    int status = 0;
    if (total_e < 0) {
        status = -1;
    } else if (stream_began && total_e != expected) {
        fprintf(stderr, "[rdma] key count mismatch: expected %d, got %d\n",
                expected, total_e);
        dict_clear(&g_dict);
        status = -1;
    }

    rdma_xport_destroy(xport);
    return status;
}
