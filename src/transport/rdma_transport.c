/*=============================================================================
 *  rdma_transport.c — RDMA 整块流式传输层
 *
 *  数据面：服务端整块一个 WRITE_WITH_IMM；客户端单 dummy recv 捕获该 IMM
 *  即整块就绪。无分块 / 无窗口 / 无 ACK（RC 可靠有序 + 接收缓冲固定 →
 *  天然无溢出，一个 WR 就够）。
 *
 *  服务端两阶段（conn_open 建连 → 调用方准备数据 → conn_send 直写），
 *  客户端单次调用 rdma_xport_recv。安全收尾：服务端等到那个唯一的 send CQE
 *  再断连销毁——否则在途 WR 被丢弃、对端永远等不到尾部 IMM。
 *
 *  计时：conn_send 自报纯传输速率（post→CQE），注册/连接时间单列不计入。
 *  接收端不出速率——它的墙钟里必然混着对端序列化，无法分离。
 *============================================================================*/

#define _GNU_SOURCE
#include "rdma_transport.h"
#include "kvstore.h" /* now_monotonic_ns */

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

/* 双端一致的 QP 属性：单 SGE + 全部信号化。
 * 深度写死 1——整块一个 WR 的模型下，每个方向实际各只用掉 1 个：
 * 服务端 = 1 个收凭证 recv + 1 个发数据 send，客户端 = 1 个发凭证 send
 * + 1 个收 IMM 的 dummy recv。（某设备若拒绝深度 1，提到 2 即可，不影响结构） */
static void xport_qp_attr(struct ibv_qp_init_attr *qa)
{
    memset(qa, 0, sizeof(*qa));
    qa->cap.max_send_wr  = 1;
    qa->cap.max_recv_wr  = 1;
    qa->cap.max_send_sge = 1;
    qa->cap.max_recv_sge = 1;
    /* 不设 sq_sig_all：两处发送各自显式带 IBV_SEND_SIGNALED，铺开只会多一个
     * 与显式标志重复的开关 */
}

/*=============================================================================
 *  服务端：两阶段
 *    conn_open  → 监听 + 接受 + 收凭证（监听尽早就绪, 对端不会提前连接被丢）
 *    conn_send  → 整块直写 + 等完成 + 断连清理
 *============================================================================*/

/** 服务端连接句柄：conn_open 填充, conn_send 使用并销毁 */
struct rdma_xport_conn {
    struct rdma_cm_id *listen_id;
    struct rdma_cm_id *id;
    struct ibv_mr     *info_mr;
    struct buf_info    peer;   /* 对端（客户端）接收缓冲凭证 */
};

int rdma_xport_conn_open(const char *ip, int port, rdma_xport_conn_t **out)
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
    /* 失败路径的收尾与主动关闭完全同构（c 由 calloc 出、指针初始全 NULL，
     * conn_close 自带 NULL 检查），直接复用它 */
    rdma_xport_conn_close(c);
    return rc;
}

int rdma_xport_conn_send(rdma_xport_conn_t *conn, const void *buf, size_t len,
                         rdma_xport_stat_t *st)
{
    rdma_xport_conn_t *c = conn;
    struct ibv_mr *mr = NULL;
    int rc = -1;

    if (st) { st->bytes = 0; st->reg_ms = 0.0; st->xfer_ms = 0.0; }

    /* 参数不合法也统一走 out：本函数的契约是"调用后 conn 被销毁"，
     * 各错误路径行为一致（rdma_xport_conn_close 自带 NULL 检查） */
    if (!c || !c->id || !buf || len == 0)
        goto out;

    if (c->peer.addr == 0 || len > c->peer.size) {
        fprintf(stderr, "[rdma-xport] bad buf_info size=%u (len=%zu)\n",
                c->peer.size, len);
        goto out;
    }
    if (len > UINT32_MAX) {   /* ibv_sge.length 是 uint32_t */
        fprintf(stderr, "[rdma-xport] len %zu exceeds single-SGE limit\n", len);
        goto out;
    }

    /* 发送缓冲（调用方自持）：注册为本端 MR。
     * 注册耗时回传给调用方单列（1GB 定稿时是秒级），不计入下面的纯传输速率 */
    uint64_t t_reg = now_monotonic_ns();
    mr = rdma_reg_msgs(c->id, (void *)buf, len);
    if (!mr) {
        perror("[rdma-xport] reg_msgs send buf");
        goto out;
    }
    uint64_t reg_ns = now_monotonic_ns() - t_reg;

    /* 整块一个 WR 直写：接收缓冲固定且有上限 ⇒ 无需分块、无需滑动窗口。
     * WRITE_WITH_IMM 挂在这唯一的 WR 上，对端收到 IMM 即整块落地。
     * imm_data 留 0：全程只有这一个 IMM，它的值不携带任何信息，接收端也不读
     * （只认"收到一条 recv 完成"这个事实），设常量只会看着像校验。 */
    struct ibv_sge sge = {
        .addr   = (uintptr_t)buf,
        .length = (uint32_t)len,
        .lkey   = mr->lkey,
    };
    struct ibv_send_wr wr = { 0 }, *bad = NULL;
    wr.sg_list    = &sge;
    wr.num_sge    = 1;
    wr.opcode     = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = c->peer.addr;
    wr.wr.rdma.rkey        = c->peer.rkey;

    /* 纯传输计时起点：post 之前（注册已完成，不计入速率） */
    uint64_t t_xfer = now_monotonic_ns();
    if (ibv_post_send(c->id->qp, &wr, &bad)) {
        perror("[rdma-xport] ibv_post_send");
        goto out;
    }

    /* 唯一一个 send CQE：RC 下它意味着对端已 ACK（数据确已写入对端内存），
     * 故这段区间就是真·端到端传输耗时 */
    if (poll_completion(c->id->send_cq, "send") < 0)
        goto out;

    /* 三段实测值回传，本层不打印（汇总结论行由控制层拼，见头文件"计时归属"） */
    if (st) {
        st->bytes   = len;
        st->reg_ms  = (double)reg_ns / 1e6;
        st->xfer_ms = (double)(now_monotonic_ns() - t_xfer) / 1e6;
    }
    rc = 0;

out:
    if (mr)
        rdma_dereg_mr(mr);
    /* 断连销毁前必须已等到那个 send CQE：
     * 否则在途的 WR 会被丢弃、对端永远等不到尾部 IMM */
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

    /* 预投递一个 dummy recv：捕获尾部 WRITE_WITH_IMM。
     * 数据不经过它（IMM 不带 payload，HCA 不往里 DMA），它只是接完成事件的
     * 一个"坑位"。因此紧跟 post 就 dereg 是安全的——前提是这块内存本身不能
     * 消失：dummy 必须是 static（WQE 里留的是注册时的 lkey，注销后内核不再
     * 跟踪，但 static 内存永远有效）。别照搬到栈/堆变量上。 */
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
        rdma_dereg_mr(dmr);
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
