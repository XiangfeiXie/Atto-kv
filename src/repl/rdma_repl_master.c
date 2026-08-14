/**
 * rdma_repl_master.c — Master 全量同步（rdma_listen + RDMA_WRITE 响应推送）
 *
 * 收到 Slave 的 RDMA 连接后：
 *   收目标 MR → 增量序列化直写 MR buffer → WRITE_WITH_IMM
 *
 * 传输格式（与 sendfile 路径统一为 RDB 二进制）：
 *   batch_hdr + RDB header + RDB entries (首批)
 *   后续批次仅携带 RDB entries
 *   单 batch ≤ FULLSYNC_BUF_SIZE，最后一批带 IMM_DONE
 */

#define _GNU_SOURCE
#include "rdma_repl.h"
#include "rdma_transport.h"
#include "kvstore.h"
#include "kv_dict.h"
#include "persistence.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

static void die(const char *msg) { perror(msg); exit(1); }

/* ── Poll recv CQ, return ack slot id + slave buffer index ─────────── */

static int poll_recv_ack(rdma_xport_t *xport, struct ack_msg *acks,
                         int *out_buf_idx) {
    struct ibv_wc wc;
    if (rdma_xport_poll_recv(xport, &wc) < 0) {
        fprintf(stderr, "[master] recv CQ error\n");
        exit(1);
    }
    *out_buf_idx = (int)acks[wc.wr_id].buf_idx;
    return (int)wc.wr_id;  /* ack slot index for re-post */
}

/* ── Free-buffer pool (tracks available slave buffers) ─────────────── */

struct buf_pool {
    int  free_q[FULLSYNC_NUM_BUFS];
    int  head, tail;
    int  count;
};

static void bp_init(struct buf_pool *bp, int n) {
    bp->head = 0; bp->tail = n; bp->count = n;
    for (int i = 0; i < n; i++) bp->free_q[i] = i;
}

static int bp_empty(struct buf_pool *bp)      { return bp->count == 0; }
static int bp_available(struct buf_pool *bp)  { return bp->count; }

static int bp_acquire(struct buf_pool *bp) {
    int idx = bp->free_q[bp->head];
    bp->head = (bp->head + 1) % FULLSYNC_NUM_BUFS;
    bp->count--;
    return idx;
}

static void bp_release(struct buf_pool *bp, int idx) {
    bp->free_q[bp->tail] = idx;
    bp->tail = (bp->tail + 1) % FULLSYNC_NUM_BUFS;
    bp->count++;
}

/* ── 分批发送 RDB 数据（增量序列化 + 空闲 buffer 池）────────────────
 *
 * 返回总传输字节数。
 * *prep_ns  累加所有 rdb_serialize_next() 耗时
 * *xfer_end_ns 在最后一个 WRITE 的本地 send CQ 完成时记录。        */

static size_t send_rdb_batches(rdma_xport_t *xport, struct ibv_mr *buf_mr,
                               void *buf, struct buf_info *binfo,
                               struct ack_msg *acks, struct ibv_mr *ack_mr,
                               const struct batch_hdr *bh, int *nr_batch,
                               uint64_t *prep_ns, uint64_t *xfer_end_ns) {
    rdb_serializer_t rs;
    rdb_serialize_begin(&rs);

    struct buf_pool bp;
    bp_init(&bp, binfo->num_bufs);

    size_t total_bytes = 0;
    int is_first_batch = 1;
    int ack_inflight   = 0;
    const size_t hdr_size = sizeof(*bh);
    *prep_ns = 0;

    while (1) {
        /* ── 确保有空闲 slave buffer ── */
        while (bp_empty(&bp) && !rs.exhausted) {
            int buf_idx, slot = poll_recv_ack(xport, acks, &buf_idx);
            bp_release(&bp, buf_idx);
            ack_inflight--;
            /* re-post recv for this ack slot */
            rdma_post_recv(xport->id, NULL, &acks[slot], sizeof(acks[0]), ack_mr);
        }

        /* All data serialised — drain remaining ACKs and exit */
        if (rs.exhausted) {
            while (ack_inflight > 0) {
                int buf_idx, slot = poll_recv_ack(xport, acks, &buf_idx);
                bp_release(&bp, buf_idx);
                ack_inflight--;
                rdma_post_recv(xport->id, NULL, &acks[slot], sizeof(acks[0]), ack_mr);
            }
            break;
        }

        if (bp_empty(&bp))
            continue;  /* wait for ACKs to free buffers */

        /* ── 序列化下一批，直接写入 MR buffer ── */
        int buf_idx = bp_acquire(&bp);
        char *dst   = (char *)buf;
        size_t off  = 4;                           /* length-prefix slot */
        size_t max_data = FULLSYNC_BUF_SIZE - 4;

        if (is_first_batch) {
            memcpy(dst + off, bh, hdr_size);
            off      += hdr_size;
            max_data -= hdr_size;
            is_first_batch = 0;
        }

        int batch_done = 0;
        struct timespec s1, s2;
        clock_gettime(CLOCK_MONOTONIC, &s1);
        size_t n = rdb_serialize_next(&rs, dst + off, max_data, &batch_done);
        clock_gettime(CLOCK_MONOTONIC, &s2);
        *prep_ns += (uint64_t)(s2.tv_sec - s1.tv_sec) * 1000000000ULL
                  + (uint64_t)(s2.tv_nsec - s1.tv_nsec);
        off += n;
        *(uint32_t *)dst = htonl((uint32_t)(off - 4));

        /* ── 流水线 WRITE（chunk 拆分 + 滑动窗口 + 选择性信号）── */
        uint32_t imm = batch_done ? FULLSYNC_IMM_DONE : 0;
        if (rdma_xport_write(xport, buf, (uint32_t)off, buf_mr->lkey,
                             binfo->addr[buf_idx], binfo->rkey[buf_idx],
                             htonl(imm)) < 0)
            die("rdma_xport_write");
        if (rdma_xport_flush(xport) < 0)
            die("rdma_xport_flush");

        total_bytes   += off;
        ack_inflight  += 1;
        (*nr_batch)++;

        if (batch_done) {
            /* flush 已完成所有 send CQE — 记录传输结束时间 */
            struct timespec ts;
            clock_gettime(CLOCK_MONOTONIC, &ts);
            *xfer_end_ns = (uint64_t)ts.tv_sec * 1000000000ULL
                         + (uint64_t)ts.tv_nsec;
            rs.exhausted = 1;
        }
    }

    rdb_serialize_end(&rs);
    return total_bytes;
}

/* ═════════════════════════════════════════════════════════════════════ */

int master_fullsync_child(uint64_t repl_offset, int ready_fd) {
    setlinebuf(stdout); setlinebuf(stderr);
    const char *ip = kvs_config.rdma_listen_ip;
    if (!strcmp(ip, "0.0.0.0")) ip = NULL;
    char port[16]; snprintf(port, sizeof(port), "%d", kvs_config.rdma_listen_port);

    /* ── listener ── */
    struct rdma_addrinfo hints = {0}, *res;
    hints.ai_flags     = RAI_PASSIVE;
    hints.ai_port_space = RDMA_PS_TCP;
    if (rdma_getaddrinfo(ip, port, &hints, &res)) die("rdma_getaddrinfo");

    struct ibv_qp_init_attr qa = {0};
    qa.cap.max_send_wr  = FULLSYNC_QP_SEND_WR;
    qa.cap.max_recv_wr  = FULLSYNC_QP_RECV_WR;
    qa.cap.max_send_sge = 1;
    qa.cap.max_recv_sge = 1;
    qa.sq_sig_all       = 0;   /* 选择性信号: transport 控制 CQE 生成 */

    struct rdma_cm_id *listen_id;
    if (rdma_create_ep(&listen_id, res, NULL, &qa)) die("rdma_create_ep");
    rdma_freeaddrinfo(res);
    if (rdma_listen(listen_id, 1)) die("rdma_listen");
    if (ready_fd >= 0) { char r = 1; ssize_t _wr = write(ready_fd, &r, 1); (void)_wr; close(ready_fd); }

    /* ── accept ── */
    struct rdma_cm_id *id;
    if (rdma_get_request(listen_id, &id)) die("rdma_get_request");
    printf("[master] slave connected\n");

    /* ── 创建传输层（包装已有 QP）── */
    rdma_xport_t *xport = rdma_xport_create(id);
    if (!xport) die("rdma_xport_create");

    /* ── 注册本地 MR buffer（RDB 数据直接写到这里）── */
    char *buf = malloc(FULLSYNC_BUF_SIZE);
    if (!buf) die("malloc");
    struct ibv_mr *buf_mr = rdma_reg_msgs(id, buf, FULLSYNC_BUF_SIZE);
    if (!buf_mr) die("rdma_reg_msgs");

    /* ── 收目标 buffer 信息 ── */
    struct buf_info binfo;
    struct ibv_mr *binfo_mr = rdma_reg_msgs(id, &binfo, sizeof(binfo));
    if (!binfo_mr) die("rdma_reg_msgs(binfo)");
    rdma_post_recv(id, NULL, &binfo, sizeof(binfo), binfo_mr);
    if (rdma_accept(id, NULL)) die("rdma_accept");
    {
        struct ibv_wc wc;
        if (rdma_xport_poll_recv(xport, &wc) < 0) die("poll_recv(buf_info)");
    }

    /* ── ACK 接收槽（数量 = slave buffer 数）── */
    int num_acks = binfo.num_bufs;
    struct ack_msg *acks = calloc((size_t)num_acks, sizeof(struct ack_msg));
    if (!acks) die("calloc(acks)");
    struct ibv_mr *ack_mr = rdma_reg_msgs(id, acks,
                                          num_acks * sizeof(struct ack_msg));
    if (!ack_mr) die("rdma_reg_msgs(ack)");
    for (int i = 0; i < num_acks; i++)
        rdma_post_recv(id, NULL, &acks[i], sizeof(acks[i]), ack_mr);

    /* ── 协议信封（仅首批携带）── */
    struct batch_hdr bh = {
        .magic   = htonl(FULLSYNC_MAGIC),
        .version = htons(FULLSYNC_VERSION),
        .flags   = 0,
        .repl_offset = htobe64(repl_offset)
    };

    /* ── 分批推送 ── */
    printf("[master] xfer-begin\n");
    struct timespec t1; clock_gettime(CLOCK_MONOTONIC, &t1);

    int nr_batch = 0;
    uint64_t prep_ns = 0, xfer_end_ns = 0;
    size_t total_bytes = send_rdb_batches(xport, buf_mr, buf,
                                          &binfo, acks, ack_mr,
                                          &bh, &nr_batch,
                                          &prep_ns, &xfer_end_ns);


    uint64_t t1_ns = (uint64_t)t1.tv_sec * 1000000000ULL
                   + (uint64_t)t1.tv_nsec;
    uint64_t wall_ns = (xfer_end_ns > t1_ns) ? (xfer_end_ns - t1_ns) : 0;
    uint64_t xfer_ns = (wall_ns > prep_ns) ? (wall_ns - prep_ns) : 0;
    double prep_ms   = (double)prep_ns / 1e6;
    double xfer_ms   = (double)xfer_ns / 1e6;
    double wall_ms   = (double)wall_ns / 1e6;
    double mbps      = (xfer_ms > 0.0)
                     ? (total_bytes / (xfer_ms / 1000.0) / (1024.0 * 1024.0)) : 0.0;
    printf("[master] xfer-end: bytes=%zu wall=%.1f prep=%.1f xfer=%.1f mbps=%.2f batches=%d\n",
           total_bytes, wall_ms, prep_ms, xfer_ms, mbps, nr_batch);

    rdma_dereg_mr(ack_mr);  free(acks);
    rdma_dereg_mr(binfo_mr);
    rdma_dereg_mr(buf_mr);  free(buf);
    rdma_xport_destroy(xport);
    rdma_disconnect(id);    rdma_destroy_ep(id);
    rdma_destroy_ep(listen_id);
    return 0;
}
