/*=============================================================================
 *  rdma.c — kvstore 用 RDMA 做全量同步的控制层
 *
 *  职责：用 RDMA 传输引擎（rdma_transport.c）做全量同步
 *   - 服务端：RDB 流式序列化 → 段池帧生产，双缓冲泵推进
 *     （buf_get 填一段 / buf_post 发一段；空闲即回收重叠传输）
 *   - 客户端：逐帧接收 → 就地流式加载进字典 → ack 归还槽位
 *
 *  传输原语 + 帧协议（段池/WRITE_WITH_IMM/ack 信用）都在 rdma_transport.c
 *  本层只做 kvstore 业务控制（RDB 序列化 / 加载 / 性能统计），不触碰 verbs
 *  上层由 repl_fullsync.c 按配置开关直接调用
 *
 *  线上格式（流式帧流）：
 *   首帧：[batch_hdr(magic+token)][RDB 头 key_count][entries...]
 *   后续帧：[entries...]（RDB 流的纯续段）
 *   快照大小不再受单缓冲限制
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
 *  服务端：双缓冲泵 —— 序列化与发送流水线化
 *============================================================================*/

/**
 * 泵式序列化整个 RDB：
 * 循环 [取空闲段 → 尽量填满 → 提交成帧]，直到 rdb_serialize_next 报告完成。
 * RDB 流可从任意字节边界续写，所以条目天然允许跨帧。
 *
 * @param xport 传输句柄
 * @param bh 快照头部 batch_hdr（magic + token），写在首帧开头
 * @param out_total 输出：累计发出字节数（含首帧头）
 * @param prep_ns 输出：累加所有 rdb_serialize_next() 耗时
 * @param out_batches 输出：提交帧数
 * @return 0 成功，-1 失败
 */
static int pump_serialize(rdma_xport_t *xport, const struct batch_hdr *bh,
                          size_t *out_total, uint64_t *prep_ns,
                          int *out_batches)
{
    rdb_serializer_t rs;
    rdb_serialize_begin(&rs);

    size_t total = 0;
    int batches = 0;
    int first = 1;

    while (1) {
        int done = 0;
        size_t capacity;
        char *dst = rdma_xport_buf_get(xport, &capacity);
        if (!dst) {
            rdb_serialize_end(&rs);
            return -1;
        }

        size_t off = 0;
        if (first) {
            memcpy(dst, bh, sizeof(*bh));
            off = sizeof(*bh);
        }

        while (off < capacity) {
            struct timespec s1, s2;
            clock_gettime(CLOCK_MONOTONIC, &s1);
            size_t n = rdb_serialize_next(&rs, dst + off,
                                          capacity - off, &done);
            clock_gettime(CLOCK_MONOTONIC, &s2);
            *prep_ns += (uint64_t)(s2.tv_sec - s1.tv_sec) * 1000000000ULL
                      + (uint64_t)(s2.tv_nsec - s1.tv_nsec);
            off += n;

            if (done)
                break;
            /**
             * n==0：当前段剩余空间放不下下一个完整 entry。
             * 段内已有内容就先提交，把该 entry 挪到下一段；
             * 若是空段（或只剩头部）都放不下，属于单条超大异常，报错中止
             */
            if (n == 0 && off <= sizeof(*bh)) {
                fprintf(stderr,
                        "[rdma] single entry exceeds segment (%zu B), aborting\n",
                        capacity);
                rdb_serialize_end(&rs);
                return -1;
            }
            if (n == 0)
                break;
        }

        if (rdma_xport_buf_post(xport, off, done) < 0) {
            rdb_serialize_end(&rs);
            return -1;
        }
        total += off;
        batches++;
        first = 0;

        if (done)
            break;
    }

    rdb_serialize_end(&rs);

    /* 关键收尾：等全部帧本地发送完成再返回（调用方随后才关连接），
     * 否则排队中的 WRITE 会被断连丢弃，对端将永远等不到末帧的 IMM */
    if (rdma_xport_flush(xport) < 0)
        return -1;

    *out_total = total;
    *out_batches = batches;
    return 0;
}

/*=============================================================================
 *  客户端：帧循环加载
 *============================================================================*/

/**
 * 加载一帧
 *
 * 首帧从 batch_hdr 做 magic 校验、解出 token（快照 offset）、读 RDB 头
 * key_count 并预分配桶；随后把本帧余下的 entry 流追加进字典。
 * 返回成功加载的 entry 数（-1 表示致命错误）
 */
static int load_batch(const char *data, uint32_t data_len,
                      int is_first, int *stream_began, uint64_t *out_snap,
                      int *out_expected)
{
    if (!is_first) {
        if (!*stream_began) {
            fprintf(stderr, "[rdma] entry batch before header\n");
            return -1;
        }
        if (data_len == 0)
            return 0;
        int n = rdb_load_stream_append(data, data_len);
        return (n >= 0) ? n : -1;
    }

    const struct batch_hdr *bh = (const struct batch_hdr *)data;
    if (ntohl(bh->magic) != RDMA_MAGIC) {
        fprintf(stderr, "[rdma] bad magic 0x%x\n", ntohl(bh->magic));
        return -1;
    }
    if (out_snap)
        *out_snap = be64toh(bh->token);
    data     += sizeof(*bh);
    data_len -= sizeof(*bh);

    if (data_len < 4) {
        fprintf(stderr, "[rdma] first frame missing rdb header (%u B)\n",
                data_len);
        return -1;
    }

    /* RDB 头仅 4 字节 key_count（与 rdb_serialize_next / rdb_save_sync 一致） */
    int expected = rdb_load_stream_begin(data, 4);
    if (expected < 0)
        return -1;
    *stream_began = 1;
    if (out_expected)
        *out_expected = expected;

    /* 批量插入前预分配桶，避免加载过程中反复扩容 */
    if (expected > 0)
        dict_reserve(&g_dict, (uint32_t)expected);

    if (data_len > 4) {
        int n = rdb_load_stream_append(data + 4, data_len - 4);
        return (n >= 0) ? n : -1;
    }
    return 0;
}

/*=============================================================================
 *  入口
 *============================================================================*/

/**
 * RDMA 服务端入口
 *
 * 流程：
 * 1. rdma_xport_server_accept：监听 + 接受连接 + 交换缓冲信息
 * 2. pump_serialize：流式序列化并逐帧推送（尾帧带 IMM_DONE）
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

    /* 协议信封（快照头，随首帧发出） */
    struct batch_hdr bh = {
        .magic = htonl(RDMA_MAGIC),
        .token = htobe64(token)
    };

    printf("[rdma] xfer-begin\n");
    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t1);

    uint64_t prep_ns = 0;
    size_t total_bytes = 0;
    int batches = 0;
    int rc = pump_serialize(xport, &bh, &total_bytes, &prep_ns, &batches);

    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    uint64_t end_ns = (uint64_t)ts.tv_sec * 1000000000ULL
                    + (uint64_t)ts.tv_nsec;

    if (rc == 0) {
        /* 性能统计 */
        uint64_t t1_ns = (uint64_t)t1.tv_sec * 1000000000ULL
                       + (uint64_t)t1.tv_nsec;
        uint64_t wall_ns = (end_ns > t1_ns) ? (end_ns - t1_ns) : 0;
        uint64_t xfer_ns = (wall_ns > prep_ns) ? (wall_ns - prep_ns) : 0;
        double prep_ms = (double)prep_ns / 1e6;
        double xfer_ms = (double)xfer_ns / 1e6;
        double wall_ms = (double)wall_ns / 1e6;
        double mbps = (xfer_ms > 0.0)
                    ? (total_bytes / (xfer_ms / 1000.0) / (1024.0 * 1024.0))
                    : 0.0;
        printf("[rdma] xfer-end: bytes=%zu wall=%.1f prep=%.1f xfer=%.1f mbps=%.2f batches=%d\n",
               total_bytes, wall_ms, prep_ms, xfer_ms, mbps, batches);
    }

    rdma_xport_destroy(xport);
    return rc;
}

/**
 * RDMA 客户端入口
 *
 * 流程：
 * 1. rdma_xport_client_connect：连接 + 注册段池 + 交换缓冲信息
 * 2. 帧循环：recv → load_batch 就地加载 → ack 归还信用
 * 3. 全部数据落地后核对 key_count 并解出快照 offset
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

    int stream_began = 0, expected = 0;
    long total_e = 0;
    size_t total_bytes = 0;
    int batches = 0;
    uint64_t snap = 0;
    int failed = 0;
    int last = 0;

    while (!last) {
        const void *data = NULL;
        uint32_t flen = 0;
        if (rdma_xport_frame_recv(xport, &data, &flen, &last) < 0) {
            fprintf(stderr, "[rdma] recv failed\n");
            failed = 1;
            break;
        }

        int n = load_batch((const char *)data, flen, !stream_began,
                           &stream_began, &snap, &expected);
        rdma_xport_frame_ack(xport);      /* 数据已消费，归还信用 */

        if (n < 0) {
            failed = 1;
            break;
        }
        total_e += n;
        total_bytes += flen;
        batches++;
    }

    clock_gettime(CLOCK_MONOTONIC, &t2);

    /* 性能统计 */
    double ms = (t2.tv_sec - t1.tv_sec) * 1000.0
              + (t2.tv_nsec - t1.tv_nsec) / 1e6;
    double mbps = (ms > 0)
                ? (total_bytes / (ms / 1000.0) / (1024.0 * 1024.0))
                : 0;
    printf("[rdma] xfer-end: entries=%ld bytes=%zu ms=%.1f mbps=%.2f batches=%d\n",
           total_e, total_bytes, ms, mbps, batches);

    if (out_token)
        *out_token = snap;

    /* 与文件路径 rdb_load_from_buf 对齐：核对实际加载数 == 头里 key_count */
    int status = 0;
    if (failed || total_e < 0) {
        status = -1;
    } else if (stream_began && total_e != expected) {
        fprintf(stderr, "[rdma] key count mismatch: expected %d, got %ld\n",
                expected, total_e);
        dict_clear(&g_dict);
        status = -1;
    }

    rdma_xport_destroy(xport);
    return status;
}
