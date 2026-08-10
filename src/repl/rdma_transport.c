/**
 * rdma_transport.c — RDMA 传输层实现
 *
 * 核心引擎提取自 tools/rdma_bw.c，升级点：
 *   P0  选择性信号 — sq_sig_all=0，仅每 QP_DEPTH 的最后一个 WR 产生 CQE
 *   P1  变长消息   — 任意 len，自动按 CHUNK_SIZE 拆分
 *   P2  窗口内推土 — 不等上层 flush，满 QP_DEPTH 自动排空
 *   P3  持久连接   — 包装已有 rdma_cm_id，不管生命周期
 */

#define _GNU_SOURCE
#include "rdma_transport.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── 内部 helpers ─────────────────────────────────────────────────── */

static const char *wc_status_str(enum ibv_wc_status s)
{
    switch (s) {
    case IBV_WC_SUCCESS:          return "SUCCESS";
    case IBV_WC_LOC_LEN_ERR:      return "LOC_LEN_ERR";
    case IBV_WC_LOC_QP_OP_ERR:    return "LOC_QP_OP_ERR";
    case IBV_WC_LOC_EEC_OP_ERR:   return "LOC_EEC_OP_ERR";
    case IBV_WC_LOC_PROT_ERR:     return "LOC_PROT_ERR";
    case IBV_WC_WR_FLUSH_ERR:     return "WR_FLUSH_ERR";
    case IBV_WC_MW_BIND_ERR:      return "MW_BIND_ERR";
    case IBV_WC_BAD_RESP_ERR:     return "BAD_RESP_ERR";
    case IBV_WC_LOC_ACCESS_ERR:   return "LOC_ACCESS_ERR";
    case IBV_WC_REM_INV_REQ_ERR:  return "REM_INV_REQ_ERR";
    case IBV_WC_REM_ACCESS_ERR:   return "REM_ACCESS_ERR";
    case IBV_WC_REM_OP_ERR:       return "REM_OP_ERR";
    case IBV_WC_RETRY_EXC_ERR:    return "RETRY_EXC_ERR";
    case IBV_WC_RNR_RETRY_EXC_ERR:return "RNR_RETRY_EXC_ERR";
    case IBV_WC_LOC_RDD_VIOL_ERR: return "LOC_RDD_VIOL_ERR";
    case IBV_WC_REM_INV_RD_REQ_ERR:return "REM_INV_RD_REQ_ERR";
    case IBV_WC_REM_ABORT_ERR:    return "REM_ABORT_ERR";
    case IBV_WC_INV_EECN_ERR:     return "INV_EECN_ERR";
    case IBV_WC_INV_EEC_STATE_ERR:return "INV_EEC_STATE_ERR";
    case IBV_WC_FATAL_ERR:        return "FATAL_ERR";
    case IBV_WC_RESP_TIMEOUT_ERR: return "RESP_TIMEOUT_ERR";
    case IBV_WC_GENERAL_ERR:      return "GENERAL_ERR";
    default:                      return "UNKNOWN";
    }
}

static int do_poll_send(struct ibv_cq *cq)
{
    struct ibv_wc wc;
    int n;
    do { n = ibv_poll_cq(cq, 1, &wc); } while (n == 0);
    if (n < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "[xport] send CQ error n=%d status=%d (%s) wr_id=%lu\n",
                n, wc.status, wc_status_str(wc.status), wc.wr_id);
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
        fprintf(stderr, "[xport] recv CQ error n=%d status=%d (%s) wr_id=%lu\n",
                n, wc->status, wc_status_str(wc->status), wc->wr_id);
        return -1;
    }
    return 0;
}
