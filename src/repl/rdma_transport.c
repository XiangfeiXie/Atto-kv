/**
 * rdma_transport.c — RDMA 传输层实现
 *
 * 封装 chunk 拆分、滑动窗口流水线、选择性信号与 CQ 轮询。
 * 包装已有 rdma_cm_id / QP，不管理连接生命周期。
 */

#define _GNU_SOURCE
#include "rdma_transport.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int do_poll_send(struct ibv_cq *cq)
{
    struct ibv_wc wc;
    int n;
    do { n = ibv_poll_cq(cq, 1, &wc); } while (n == 0);
    if (n < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "[xport] send CQ error n=%d status=%d wr_id=%lu\n",
                n, wc.status, wc.wr_id);
        return -1;
    }
    return 0;
}

/* ── 生命周期 ──────────────────────────────────────────────────────── */

rdma_xport_t *rdma_xport_create(struct rdma_cm_id *id)
{
    rdma_xport_t *x = calloc(1, sizeof(*x));
    if (!x) return NULL;
    x->id = id;
    return x;
}

void rdma_xport_destroy(rdma_xport_t *x)
{
    if (!x) return;
    /* 不销毁 id — 连接生命周期由上层管理 */
    free(x);
}

/* ── 流水线写入 ────────────────────────────────────────────────────── */

int rdma_xport_write(rdma_xport_t *x,
                     const void *data, size_t len, uint32_t lkey,
                     uint64_t remote_addr, uint32_t rkey,
                     uint32_t imm_data)
{
    if (len == 0) return 0;

    const int num_chunks = (int)((len + RDMA_XPORT_CHUNK_SIZE - 1)
                                 / RDMA_XPORT_CHUNK_SIZE);
    int posted = 0;

    for (int i = 0; i < num_chunks; i++) {
        const int is_last_chunk = (i == num_chunks - 1);
        const size_t local_off  = (size_t)i * RDMA_XPORT_CHUNK_SIZE;
        const size_t remote_off = (size_t)i * RDMA_XPORT_CHUNK_SIZE;
        const size_t cur_len    = is_last_chunk
                                  ? (len - local_off)
                                  : RDMA_XPORT_CHUNK_SIZE;

        /* ── 构建 SGE ── */
        struct ibv_sge sge = {
            .addr   = (uintptr_t)((const char *)data + local_off),
            .length = (uint32_t)cur_len,
            .lkey   = lkey
        };

        /* ── 构建 WR ── */
        struct ibv_send_wr wr = {0}, *bad_wr = NULL;

        wr.wr_id      = (uint64_t)i;
        wr.sg_list    = &sge;
        wr.num_sge    = 1;

        /* 末尾 chunk: WRITE_WITH_IMM + SIGNALED */
        if (is_last_chunk) {
            wr.opcode     = IBV_WR_RDMA_WRITE_WITH_IMM;
            wr.imm_data   = imm_data;
            wr.send_flags = IBV_SEND_SIGNALED;
            x->signaled_wr++;
        } else {
            /* 非末尾: WRITE, 选择性信号 */
            wr.opcode     = IBV_WR_RDMA_WRITE;
            const int window_edge = ((x->unpolled_wr + 1) % RDMA_XPORT_QP_DEPTH) == 0;
            if (window_edge) {
                wr.send_flags = IBV_SEND_SIGNALED;
                x->signaled_wr++;
            } else {
                wr.send_flags = 0;  /* unsignaled — 不产生 CQE */
            }
        }

        wr.wr.rdma.remote_addr = remote_addr + remote_off;
        wr.wr.rdma.rkey        = rkey;

        if (ibv_post_send(x->id->qp, &wr, &bad_wr)) {
            perror("[xport] ibv_post_send");
            return -1;
        }

        x->unpolled_wr++;
        posted++;

        /* 窗口满: 自动排空，避免 SQ 溢出 */
        if (x->unpolled_wr >= RDMA_XPORT_QP_DEPTH) {
            if (rdma_xport_flush(x) < 0) return -1;
        }
    }

    return posted;
}

/* ── 排空 SQ ───────────────────────────────────────────────────────── */

int rdma_xport_flush(rdma_xport_t *x)
{
    if (x->signaled_wr == 0) return 0;

    for (int i = 0; i < x->signaled_wr; i++) {
        if (do_poll_send(x->id->send_cq) < 0) return -1;
    }

    x->unpolled_wr = 0;
    x->signaled_wr = 0;
    return 0;
}

/* ── CQ poll 原语 ─────────────────────────────────────────────────── */

int rdma_xport_poll_send(rdma_xport_t *x)
{
    return do_poll_send(x->id->send_cq);
}

int rdma_xport_poll_recv(rdma_xport_t *x, struct ibv_wc *wc)
{
    int n;
    do { n = ibv_poll_cq(x->id->recv_cq, 1, wc); } while (n == 0);
    if (n < 0 || wc->status != IBV_WC_SUCCESS) {
        fprintf(stderr, "[xport] recv CQ error n=%d status=%d wr_id=%lu\n",
                n, wc->status, wc->wr_id);
        return -1;
    }
    return 0;
}
