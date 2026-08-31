/*=============================================================================
 *  rdma_transport.c — RDMA 整块流式传输层
 *
 *  数据面：服务端分块流水线 WRITE，末块 WRITE_WITH_IMM；客户端单 dummy
 *  recv 捕获该 IMM 即整块就绪。sq_sig_all=1 全部信号化，窗口满批量回收，
 *  无 ACK / 无环形槽位（RC 可靠有序 + 接收缓冲固定 → 天然无溢出）。
 *
 *  两个入口均为单次调用：连接建立 + 数据面 + 清理收敛进一个函数，
 *  无句柄、无跨调用状态。安全收尾：服务端发送循环结束前排空全部 send
 *  CQE 再断连销毁——否则排队分块被丢弃、对端永远等不到尾部 IMM。
 *============================================================================*/

#define _GNU_SOURCE
#include "rdma_transport.h"
#include "kvstore.h" /* now_monotonic_ns */

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

/*=============================================================================
 *  内部类型
 *============================================================================*/

/** 对端缓冲凭证（谁接收数据，谁把自己的 addr/rkey 发给对方） */
struct buf_info {
    uint64_t addr;
    uint32_t rkey;
    uint32_t size;
} __attribute__((packed));

/*=============================================================================
 *  通用小工具
 *============================================================================*/

/**
 * 阻塞轮询一条完成（纯轮询占用 CPU 直至到达）
 */
static int poll_completion(struct ibv_cq *cq, const char *what)
{
    struct ibv_wc wc;
    int n;
    do {
        n = ibv_poll_cq(cq, 1, &wc);
    } while (n == 0);

    if (n < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "[rdma-xport] %s CQ error status=%s\n", what,
                n < 0 ? "poll-failed" : ibv_wc_status_str(wc.status));
        return -1;
    }
    return 0;
}

/* 双端一致的 QP 属性：SQ/RQ 深度 + 单 SGE + 全部信号化 */
static void xport_qp_attr(struct ibv_qp_init_attr *qa)
{
    memset(qa, 0, sizeof(*qa));
    qa->cap.max_send_wr  = RDMA_QP_SEND_WR;
    qa->cap.max_recv_wr  = RDMA_QP_RECV_WR;
    qa->cap.max_send_sge = 1;
    qa->cap.max_recv_sge = 1;
    qa->sq_sig_all       = 1;
}

/*=============================================================================
 *  服务端：两阶段
 *    conn_open  → 监听 + 接受 + 收凭证（监听尽早就绪, 对端不会提前连接被丢）
 *    conn_send  → 分块直写 + 排空 + 断连清理
 *============================================================================*/

/** 服务端连接句柄：conn_open 填充, conn_send 使用并销毁 */
struct rdma_xport_conn {
    struct rdma_cm_id *listen_id;
    struct rdma_cm_id *id;
    struct ibv_mr     *info_mr;
    struct buf_info    peer;   /* 对端（客户端）接收缓冲凭证 */
};

int rdma_xport_conn_open(const char *ip, int port, int ready_fd,
                         rdma_xport_conn_t **out)
{
    struct rdma_addrinfo hints, *res = NULL;
    char port_str[16];
    rdma_xport_conn_t *c = NULL;
    int rc = -1;

    if (!out)
        return -1;
    *out = NULL;

    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = RAI_PASSIVE;
    hints.ai_port_space = RDMA_PS_TCP;
    snprintf(port_str, sizeof(port_str), "%d", port);
    if (rdma_getaddrinfo(ip, port_str, &hints, &res)) {
        perror("[rdma-xport] getaddrinfo");
        goto out;
    }

    struct ibv_qp_init_attr qa;
    xport_qp_attr(&qa);

    c = calloc(1, sizeof(*c));
    if (!c) {
        fprintf(stderr, "[rdma-xport] calloc conn failed\n");
        goto out;
    }

    if (rdma_create_ep(&c->listen_id, res, NULL, &qa)) {
        perror("[rdma-xport] create_ep");
        goto out;
    }
    rdma_freeaddrinfo(res);
    res = NULL;

    if (rdma_listen(c->listen_id, 1)) {
        perror("[rdma-xport] listen");
        goto out;
    }

    if (ready_fd >= 0) {
        char r = 1;
        ssize_t _w = write(ready_fd, &r, 1);
        (void)_w;
        close(ready_fd);
    }

    if (rdma_get_request(c->listen_id, &c->id)) {
        perror("[rdma-xport] get_request");
        goto out;
    }

    /* 先注册并投递缓冲凭证接收，再 accept —— 客户端连接建立后会立刻
     * 发送凭证，若此刻 RQ 为空会触发 RNR NAK 直至重试耗尽毁链 */
    c->info_mr = rdma_reg_msgs(c->id, &c->peer, sizeof(c->peer));
    if (!c->info_mr) {
        perror("[rdma-xport] reg info mr");
        goto out;
    }
    if (rdma_post_recv(c->id, NULL, &c->peer, sizeof(c->peer), c->info_mr)) {
        perror("[rdma-xport] post recv info");
        goto out;
    }

    /* 显式 accept 参数：NULL 时字段内容不定，可能被对端校验拒绝 */
    struct rdma_conn_param ap;
    memset(&ap, 0, sizeof(ap));
    ap.responder_resources = 1;
    ap.initiator_depth     = 1;
    if (rdma_accept(c->id, &ap)) {
        perror("[rdma-xport] accept");
        goto out;
    }

    /* 收对端缓冲凭证（SLAVE 的接收缓冲）；连接已就绪, 可先于数据准备 */
    if (poll_completion(c->id->recv_cq, "buf_info")) {
        fprintf(stderr, "[rdma-xport] no buf_info from client\n");
        goto out;
    }

    *out = c;
    c = NULL;
    rc = 0;

out:
    if (res)
        rdma_freeaddrinfo(res);
    if (c) {
        if (c->info_mr)
            rdma_dereg_mr(c->info_mr);
        if (c->id) {
            if (c->id->qp)
                rdma_disconnect(c->id);
            rdma_destroy_ep(c->id);
        }
        if (c->listen_id)
            rdma_destroy_ep(c->listen_id);
        free(c);
    }
    return rc;
}

int rdma_xport_conn_send(rdma_xport_conn_t *conn, const void *buf, size_t len)
{
    uint64_t t0 = now_monotonic_ns();
    rdma_xport_conn_t *c = conn;
    struct ibv_mr *mr = NULL;
    int rc = -1;

    if (!c || !c->id || !buf || len == 0)
        return -1;

    if (c->peer.addr == 0 || len > c->peer.size) {
        fprintf(stderr, "[rdma-xport] bad buf_info size=%u (len=%zu)\n",
                c->peer.size, len);
        goto out;
    }

    /* 发送缓冲（调用方自持）：注册为本端 MR */
    mr = rdma_reg_msgs(c->id, (void *)buf, len);
    if (!mr) {
        perror("[rdma-xport] reg_msgs send buf");
        goto out;
    }

    /* 分块流水线直写：滑动窗口维护，末块挂 WRITE_WITH_IMM 通知 */
    int num_chunks = (int)((len + RDMA_CHUNK_SIZE - 1) / RDMA_CHUNK_SIZE);
    int unpolled = 0;
    for (int i = 0; i < num_chunks; i++) {
        size_t cur = (i == num_chunks - 1)
                   ? len - (size_t)i * RDMA_CHUNK_SIZE : RDMA_CHUNK_SIZE;

        struct ibv_sge sge = {
            .addr   = (uintptr_t)buf + (size_t)i * RDMA_CHUNK_SIZE,
            .length = (uint32_t)cur,
            .lkey   = mr->lkey,
        };
        struct ibv_send_wr wr = { 0 }, *bad = NULL;
        wr.sg_list = &sge;
        wr.num_sge = 1;

        if (i == num_chunks - 1) {
            wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;   /* 完成通知 */
            wr.imm_data = htonl(0x88888888u);
        } else {
            wr.opcode = IBV_WR_RDMA_WRITE;
        }
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.rdma.remote_addr = c->peer.addr + (size_t)i * RDMA_CHUNK_SIZE;
        wr.wr.rdma.rkey = c->peer.rkey;

        if (ibv_post_send(c->id->qp, &wr, &bad)) {
            perror("[rdma-xport] ibv_post_send");
            goto out;
        }
        unpolled++;

        /* 滑动窗口维护：窗口满或发完时批量回收本地完成 */
        if (unpolled == RDMA_QP_DEPTH || i == num_chunks - 1) {
            for (int j = 0; j < unpolled; j++)
                if (poll_completion(c->id->send_cq, "send") < 0)
                    goto out;
            unpolled = 0;
        }
    }

    printf("[rdma] sent %zu B in %d chunks (%.1fms)\n",
           len, num_chunks, (now_monotonic_ns() - t0) / 1e6);
    rc = 0;

out:
    if (mr)
        rdma_dereg_mr(mr);
    /* 发送循环结束（或失败）后必须排空/收尾再断连销毁：
     * 否则排队中的分块会被丢弃、对端永远等不到尾部 IMM */
    rdma_xport_conn_close(c);
    return rc;
}

void rdma_xport_conn_close(rdma_xport_conn_t *conn)
{
    rdma_xport_conn_t *c = conn;
    if (!c)
        return;
    if (c->info_mr)
        rdma_dereg_mr(c->info_mr);
    if (c->id) {
        if (c->id->qp)
            rdma_disconnect(c->id);
        rdma_destroy_ep(c->id);
    }
    if (c->listen_id)
        rdma_destroy_ep(c->listen_id);
    free(c);
}

/*=============================================================================
 *  客户端：连接 → 发凭证 → 等尾部 IMM → 清理
 *============================================================================*/

int rdma_xport_recv(const char *ip, int port, void *buf, size_t cap)
{
    uint64_t t0 = now_monotonic_ns();
    int rc = -1;
    struct rdma_cm_id *id = NULL;
    struct ibv_mr *mr = NULL, *info_mr = NULL, *dmr = NULL;
    struct rdma_addrinfo hints, *res = NULL;
    char port_str[16];

    if (!buf || cap == 0)
        return -1;

    memset(&hints, 0, sizeof(hints));
    hints.ai_port_space = RDMA_PS_TCP;
    snprintf(port_str, sizeof(port_str), "%d", port);
    if (rdma_getaddrinfo(ip, port_str, &hints, &res)) {
        perror("[rdma-xport] getaddrinfo");
        goto out;
    }

    struct ibv_qp_init_attr qa;
    xport_qp_attr(&qa);

    if (rdma_create_ep(&id, res, NULL, &qa)) {
        perror("[rdma-xport] create_ep");
        goto out;
    }
    rdma_freeaddrinfo(res);
    res = NULL;

    /* 接收缓冲（远程写目标）：调用方自持的单块大缓冲 */
    mr = rdma_reg_write(id, buf, cap);
    if (!mr) {
        perror("[rdma-xport] reg_write buf");
        goto out;
    }

    /* 预投递一个 dummy recv：捕获尾部 WRITE_WITH_IMM */
    {
        static char dummy;
        dmr = rdma_reg_msgs(id, &dummy, 1);
        if (!dmr) {
            perror("[rdma-xport] reg dummy mr");
            goto out;
        }
        if (rdma_post_recv(id, NULL, &dummy, 1, dmr)) {
            perror("[rdma-xport] post recv dummy");
            goto out;
        }
        rdma_dereg_mr(dmr);              /* WQE 已带 lkey 快照，可即刻注销 */
        dmr = NULL;
    }

    /* 显式连接参数（勿用 NULL：字段不定会触发对端 INVALID_RETRY_COUNT） */
    struct rdma_conn_param cp;
    memset(&cp, 0, sizeof(cp));
    cp.responder_resources = 1;
    cp.initiator_depth     = 1;
    cp.retry_count         = 7;
    cp.rnr_retry_count     = 7;
    if (rdma_connect(id, &cp)) {
        perror("[rdma-xport] connect");
        goto out;
    }

    /* 把本端缓冲凭证发给服务端（服务端据此直写本缓冲） */
    {
        struct buf_info bi = {
            .addr = (uintptr_t)buf,
            .rkey = mr->rkey,
            .size = (uint32_t)cap,
        };
        info_mr = rdma_reg_msgs(id, &bi, sizeof(bi));
        if (!info_mr) {
            perror("[rdma-xport] reg info mr");
            goto out;
        }
        if (rdma_post_send(id, NULL, &bi, sizeof(bi), info_mr,
                           IBV_SEND_SIGNALED)) {
            perror("[rdma-xport] post send buf_info");
            goto out;
        }
        if (poll_completion(id->send_cq, "buf_info-send")) {
            fprintf(stderr, "[rdma-xport] send buf_info failed\n");
            goto out;
        }
        rdma_dereg_mr(info_mr);
        info_mr = NULL;
    }

    /* 唯一的 IMM：见到即整块落地；实际长度由应用层协议头自描述 */
    if (poll_completion(id->recv_cq, "imm-done")) {
        fprintf(stderr, "[rdma-xport] no imm-done\n");
        goto out;
    }

    printf("[rdma] client received %s:%d (%.1fms)\n",
           ip, port, (now_monotonic_ns() - t0) / 1e6);
    rc = 0;

out:
    if (mr)      rdma_dereg_mr(mr);
    if (info_mr) rdma_dereg_mr(info_mr);
    if (id) {
        if (id->qp)
            rdma_disconnect(id);
        rdma_destroy_ep(id);
    }
    if (res)
        rdma_freeaddrinfo(res);
    return rc;
}
