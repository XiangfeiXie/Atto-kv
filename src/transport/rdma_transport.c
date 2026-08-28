/*=============================================================================
 *  rdma_transport.c — RDMA 整块流式传输层
 *
 *  数据面：服务端分块流水线 WRITE，末块 WRITE_WITH_IMM；客户端单 dummy
 *  recv 捕获该 IMM 即整块就绪。sq_sig_all=1 全部信号化，窗口满批量回收，
 *  无 ACK / 无环形槽位（RC 可靠有序 + 接收缓冲固定 → 天然无溢出）。
 *
 *  安全收尾：服务端发送循环结束前排空全部 send CQE，再由调用方断连——
 *  否则排队分块被丢弃、对端永远等不到尾部 IMM。
 *============================================================================*/

#define _GNU_SOURCE
#include "rdma_transport.h"

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

static double now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

enum { XPORT_SERVER = 0, XPORT_CLIENT = 1 };

struct rdma_xport {
    int role;

    struct rdma_cm_id       *listen; /* 服务端监听 id */
    struct rdma_cm_id       *id;     /* 数据连接 id */

    struct ibv_mr *mr;               /* 本端缓冲 MR */
    char          *buf;

    /* 握手消息 */
    struct buf_info peer;            /* 服务端：对端(客户端)缓冲凭证 */
    struct ibv_mr  *peer_mr;
};

/**
 * 阻塞轮询一条完成（kvs_rdma.c 同款：纯轮询占用 CPU 直至到达）
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

/*=============================================================================
 *  连接建立
 *============================================================================*/

rdma_xport_t *rdma_xport_server_accept(const char *ip, int port, int ready_fd)
{
    double t0 = now_ms();

    rdma_xport_t *x = calloc(1, sizeof(*x));
    if (!x)
        return NULL;
    x->role = XPORT_SERVER;

    struct rdma_addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = RAI_PASSIVE;
    hints.ai_port_space = RDMA_PS_TCP;

    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", port);
    if (rdma_getaddrinfo(ip && strcmp(ip, "0.0.0.0") ? ip : NULL,
                         port_str, &hints, &res)) {
        perror("[rdma-xport] getaddrinfo");
        goto fail;
    }

    struct ibv_qp_init_attr qa;
    memset(&qa, 0, sizeof(qa));
    qa.cap.max_send_wr = RDMA_QP_SEND_WR;
    qa.cap.max_recv_wr = RDMA_QP_RECV_WR;
    qa.cap.max_send_sge = 1;
    qa.cap.max_recv_sge = 1;
    qa.sq_sig_all = 1;

    if (rdma_create_ep(&x->listen, res, NULL, &qa)) {
        perror("[rdma-xport] create_ep");
        rdma_freeaddrinfo(res);
        goto fail;
    }
    rdma_freeaddrinfo(res);

    if (rdma_listen(x->listen, 1)) {
        perror("[rdma-xport] listen");
        goto fail;
    }

    if (ready_fd >= 0) {
        char r = 1;
        ssize_t _w = write(ready_fd, &r, 1);
        (void)_w;
        close(ready_fd);
    }

    if (rdma_get_request(x->listen, &x->id)) {
        perror("[rdma-xport] get_request");
        goto fail;
    }

    /* 先注册并投递缓冲凭证接收，再 accept —— 客户端连接建立后会立刻
     * 发送凭证，若此刻 RQ 为空会触发 RNR NAK 直至重试耗尽毁链 */
    x->peer_mr = rdma_reg_msgs(x->id, &x->peer, sizeof(x->peer));
    if (!x->peer_mr) {
        perror("[rdma-xport] reg info mr");
        goto fail;
    }
    if (rdma_post_recv(x->id, NULL, &x->peer, sizeof(x->peer), x->peer_mr)) {
        perror("[rdma-xport] post recv info");
        goto fail;
    }

    /* 显式 accept 参数：NULL 时字段内容不定，可能被对端校验拒绝 */
    struct rdma_conn_param ap;
    memset(&ap, 0, sizeof(ap));
    ap.responder_resources = 1;
    ap.initiator_depth     = 1;
    if (rdma_accept(x->id, &ap)) {
        perror("[rdma-xport] accept");
        goto fail;
    }

    /* 收对端缓冲凭证（SLAVE 的接收缓冲） */
    if (poll_completion(x->id->recv_cq, "buf_info")) {
        fprintf(stderr, "[rdma-xport] no buf_info from client\n");
        goto fail;
    }
    if (x->peer.addr == 0 ||
        x->peer.size < RDMA_CHUNK_SIZE) {
        fprintf(stderr, "[rdma-xport] bad buf_info size=%u\n", x->peer.size);
        goto fail;
    }

    /* 发送暂存块（整份快照序列化目标，随后由 rdma_xport_send 推送） */
    x->buf = malloc(RDMA_BUF_SIZE);
    if (!x->buf) {
        fprintf(stderr, "[rdma-xport] malloc(%zu) failed\n",
                (size_t)RDMA_BUF_SIZE);
        goto fail;
    }
    x->mr = rdma_reg_msgs(x->id, x->buf, RDMA_BUF_SIZE);
    if (!x->mr) {
        perror("[rdma-xport] reg_msgs stage buf");
        goto fail;
    }

    printf("[rdma] server ready %s:%d (%.1fms)\n",
           ip ? ip : "*", port, now_ms() - t0);
    return x;

fail:
    rdma_xport_destroy(x);
    return NULL;
}

rdma_xport_t *rdma_xport_client_connect(const char *ip, int port)
{
    double t0 = now_ms();

    rdma_xport_t *x = calloc(1, sizeof(*x));
    if (!x)
        return NULL;
    x->role = XPORT_CLIENT;

    struct rdma_addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));
    hints.ai_port_space = RDMA_PS_TCP;

    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", port);
    if (rdma_getaddrinfo(ip, port_str, &hints, &res)) {
        perror("[rdma-xport] getaddrinfo");
        goto fail;
    }

    struct ibv_qp_init_attr qa;
    memset(&qa, 0, sizeof(qa));
    qa.cap.max_send_wr = RDMA_QP_SEND_WR;
    qa.cap.max_recv_wr = RDMA_QP_RECV_WR;
    qa.cap.max_send_sge = 1;
    qa.cap.max_recv_sge = 1;
    qa.sq_sig_all = 1;

    if (rdma_create_ep(&x->id, res, NULL, &qa)) {
        perror("[rdma-xport] create_ep");
        rdma_freeaddrinfo(res);
        goto fail;
    }
    rdma_freeaddrinfo(res);

    /* 接收缓冲（远程写目标）：单块大缓冲 */
    x->buf = calloc(1, RDMA_BUF_SIZE);
    if (!x->buf) {
        fprintf(stderr, "[rdma-xport] malloc(%zu) failed\n",
                (size_t)RDMA_BUF_SIZE);
        goto fail;
    }
    x->mr = rdma_reg_write(x->id, x->buf, RDMA_BUF_SIZE);
    if (!x->mr) {
        perror("[rdma-xport] reg_write buf");
        goto fail;
    }

    /* 预投递一个 dummy recv：捕获尾部 WRITE_WITH_IMM */
    {
        static char dummy;
        struct ibv_mr *dmr = rdma_reg_msgs(x->id, &dummy, 1);
        struct ibv_sge dsge = {
            .addr = (uintptr_t)&dummy, .length = 1, .lkey = dmr->lkey };
        struct ibv_recv_wr rw = { 0 }, *bad = NULL;
        rw.sg_list = &dsge;
        rw.num_sge = 1;
        if (ibv_post_recv(x->id->qp, &rw, &bad)) {
            perror("[rdma-xport] post recv dummy");
            goto fail;
        }
        rdma_dereg_mr(dmr);              /* WQE 已带 lkey 快照，可即刻注销 */
    }

    /* 显式连接参数（勿用 NULL：字段不定会触发对端 INVALID_RETRY_COUNT） */
    struct rdma_conn_param cp;
    memset(&cp, 0, sizeof(cp));
    cp.responder_resources = 1;
    cp.initiator_depth     = 1;
    cp.retry_count         = 7;
    cp.rnr_retry_count     = 7;
    if (rdma_connect(x->id, &cp)) {
        perror("[rdma-xport] connect");
        goto fail;
    }

    /* 把本端缓冲凭证发给服务端 */
    {
        struct buf_info bi = {
            .addr = (uintptr_t)x->buf,
            .rkey = x->mr->rkey,
            .size = (uint32_t)RDMA_BUF_SIZE,
        };
        struct ibv_mr *imr = rdma_reg_msgs(x->id, &bi, sizeof(bi));
        int rc = -1;
        if (imr &&
            !rdma_post_send(x->id, NULL, &bi, sizeof(bi), imr,
                            IBV_SEND_SIGNALED))
            rc = poll_completion(x->id->send_cq, "buf_info-send");
        if (imr)
            rdma_dereg_mr(imr);
        if (rc < 0) {
            fprintf(stderr, "[rdma-xport] send buf_info failed\n");
            goto fail;
        }
    }

    printf("[rdma] client connected %s:%d (%.1fms)\n", ip, port,
           now_ms() - t0);
    return x;

fail:
    rdma_xport_destroy(x);
    return NULL;
}

/*=============================================================================
 *  数据面
 *============================================================================*/

void *rdma_xport_send_buf(rdma_xport_t *x, size_t *capacity)
{
    if (x->role != XPORT_SERVER || !x->buf)
        return NULL;
    if (capacity)
        *capacity = RDMA_BUF_SIZE;
    return x->buf;
}

void *rdma_xport_recv_buf(rdma_xport_t *x)
{
    return x ? x->buf : NULL;
}

int rdma_xport_send(rdma_xport_t *x, const void *buf, size_t len)
{
    if (x->role != XPORT_SERVER || !buf || len == 0 ||
        len > (size_t)x->peer.size)
        return -1;

    double t0 = now_ms();
    int num_chunks = (int)((len + RDMA_CHUNK_SIZE - 1) / RDMA_CHUNK_SIZE);
    int unpolled = 0;

    for (int i = 0; i < num_chunks; i++) {
        size_t cur = (i == num_chunks - 1)
                   ? len - (size_t)i * RDMA_CHUNK_SIZE : RDMA_CHUNK_SIZE;

        struct ibv_sge sge = {
            .addr   = (uintptr_t)buf + (size_t)i * RDMA_CHUNK_SIZE,
            .length = (uint32_t)cur,
            .lkey   = x->mr->lkey,
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
        wr.wr.rdma.remote_addr = x->peer.addr + (size_t)i * RDMA_CHUNK_SIZE;
        wr.wr.rdma.rkey = x->peer.rkey;

        if (ibv_post_send(x->id->qp, &wr, &bad)) {
            perror("[rdma-xport] ibv_post_send");
            return -1;
        }
        unpolled++;

        /* 滑动窗口维护：窗口满或发完时批量回收本地完成 */
        if (unpolled == RDMA_QP_DEPTH || i == num_chunks - 1) {
            for (int j = 0; j < unpolled; j++)
                if (poll_completion(x->id->send_cq, "send") < 0)
                    return -1;
            unpolled = 0;
        }
    }

    printf("[rdma] sent %zu B in %d chunks (%.1fms)\n",
           len, num_chunks, now_ms() - t0);
    return 0;
}

int rdma_xport_wait_done(rdma_xport_t *x)
{
    if (x->role != XPORT_CLIENT)
        return -1;
    /* 唯一的 IMM：见到即整块落地；实际长度由应用层协议头自描述 */
    return poll_completion(x->id->recv_cq, "imm-done");
}

/*=============================================================================
 *  清理
 *============================================================================*/

void rdma_xport_destroy(rdma_xport_t *x)
{
    if (!x)
        return;

    if (x->id) {
        if (x->id->qp)
            rdma_disconnect(x->id);
    }
    if (x->mr)      rdma_dereg_mr(x->mr);
    if (x->peer_mr) rdma_dereg_mr(x->peer_mr);
    free(x->buf);

    if (x->id)
        rdma_destroy_ep(x->id);
    if (x->listen)
        rdma_destroy_ep(x->listen);
    free(x);
}
