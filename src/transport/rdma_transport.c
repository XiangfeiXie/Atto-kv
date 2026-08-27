/*=============================================================================
 *  rdma_transport.c — RDMA 最小流式传输层
 *
 *  机制照搬 kvs_rdma.c 的流式传输：
 *   - 数据按 CHUNK 拆成多个 RDMA WRITE 连续发出
 *   - QP_DEPTH 滑动窗口批量回收 CQE（只控制 SQ 占用，不做业务流控）
 *   - 最后一块 WRITE_WITH_IMM 携带 IMM_DONE 通知对端整块到齐
 *   - 无 ACK / 无环形缓冲 / 无流控状态机
 *
 *  连接握手：客户端注册接收缓冲，把 {addr, rkey, size} 发给服务端；
 *  服务端注册发送缓冲，收到对端缓冲信息后即可开始推送。
 *============================================================================*/

#define _GNU_SOURCE
#include "rdma_transport.h"

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>   /* rdma_post_send / rdma_reg_msgs / rdma_reg_write */
#include <arpa/inet.h>         /* htonl / ntohl */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define RDMA_IMM_DONE 0xFFFFFFFFu  /* 尾部立即数：通知对端整块到齐 */

/* 对端缓冲描述符（握手消息，客户端 → 服务端） */
struct buf_info {
    uint64_t addr;
    uint32_t rkey;
    uint32_t size;
} __attribute__((packed));

struct rdma_xport {
    struct rdma_cm_id *id;

    char           *buf;      /* 服务端：发送缓冲 / 客户端：接收缓冲 */
    struct ibv_mr *mr;

    struct buf_info binfo;    /* 客户端 → 服务端 */
    struct ibv_mr *binfo_mr;

    char           dummy;     /* 客户端：IMM 捕获占位 */
    struct ibv_mr *dummy_mr;
};

/*=============================================================================
 *  内部辅助
 *============================================================================*/

static int poll_completion(struct ibv_cq *cq)
{
    struct ibv_wc wc;
    int n;

    do {
        n = ibv_poll_cq(cq, 1, &wc);
    } while (n == 0);

    if (n < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "[rdma-xport] CQ error n=%d status=%s wr_id=%lu\n",
                n, (n < 0) ? "poll-failed" : ibv_wc_status_str(wc.status),
                (unsigned long)wc.wr_id);
        return -1;
    }
    return 0;
}

static struct rdma_cm_id *create_ep(struct rdma_addrinfo *res, int passive)
{
    struct ibv_qp_init_attr qa;
    struct rdma_cm_id *id;

    memset(&qa, 0, sizeof(qa));
    qa.cap.max_send_wr = RDMA_QP_SEND_WR;
    qa.cap.max_recv_wr = RDMA_QP_RECV_WR;
    qa.cap.max_send_sge = 1;
    qa.cap.max_recv_sge = 1;
    qa.sq_sig_all = 1;

    if (rdma_create_ep(&id, res, NULL, &qa)) {
        perror("[rdma-xport] rdma_create_ep");
        return NULL;
    }
    if (passive) {
        if (rdma_listen(id, 1)) {
            perror("[rdma-xport] rdma_listen");
            rdma_destroy_ep(id);
            return NULL;
        }
    }
    return id;
}

/*=============================================================================
 *  连接管理
 *============================================================================*/

rdma_xport_t *rdma_xport_server_accept(const char *ip, int port, int ready_fd)
{
    struct rdma_addrinfo hints, *res;
    struct rdma_cm_id *listen_id, *id;
    rdma_xport_t *x = NULL;
    char port_str[16];

    snprintf(port_str, sizeof(port_str), "%d", port);

    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = RAI_PASSIVE;
    hints.ai_port_space = RDMA_PS_TCP;
    fprintf(stderr, "[DBG-server] getaddrinfo(PASSIVE,%s,%s)...\n", ip ? ip : "*", port_str);
    if (rdma_getaddrinfo(ip, port_str, &hints, &res)) {
        perror("[rdma-xport] rdma_getaddrinfo");
        return NULL;
    }
    fprintf(stderr, "[DBG-server] getaddrinfo OK\n");

    fprintf(stderr, "[DBG-server] create_ep+listen...\n");
    listen_id = create_ep(res, 1);
    rdma_freeaddrinfo(res);
    if (!listen_id)
        return NULL;
    fprintf(stderr, "[DBG-server] listen OK\n");

    /* 通知父进程监听已就绪，可开始接受 slave 的 PSYNC */
    if (ready_fd >= 0) {
        char r = 1;
        ssize_t _w = write(ready_fd, &r, 1);
        (void)_w;
        close(ready_fd);
    }

    fprintf(stderr, "[DBG-server] get_request...\n");
    if (rdma_get_request(listen_id, &id)) {
        perror("[rdma-xport] rdma_get_request");
        rdma_destroy_ep(listen_id);
        return NULL;
    }
    rdma_destroy_ep(listen_id);
    printf("[rdma] client connected\n");
    fprintf(stderr, "[DBG-server] get_request OK\n");

    x = calloc(1, sizeof(*x));
    if (!x) {
        rdma_destroy_ep(id);
        return NULL;
    }
    x->id = id;

    /* 发送缓冲 */
    fprintf(stderr, "[DBG-server] malloc(%u)+reg_msgs...\n", (unsigned)RDMA_BUF_SIZE);
    x->buf = malloc(RDMA_BUF_SIZE);
    x->mr = x->buf ? rdma_reg_msgs(id, x->buf, RDMA_BUF_SIZE) : NULL;
    if (!x->mr) {
        perror("[rdma-xport] server buf MR");
        rdma_xport_destroy(x);
        return NULL;
    }
    fprintf(stderr, "[DBG-server] reg_msgs OK\n");

    /* 收对端缓冲信息 */
    x->binfo_mr = rdma_reg_msgs(id, &x->binfo, sizeof(x->binfo));
    if (!x->binfo_mr) {
        perror("[rdma-xport] server binfo MR");
        rdma_xport_destroy(x);
        return NULL;
    }
    if (rdma_post_recv(id, NULL, &x->binfo, sizeof(x->binfo), x->binfo_mr)) {
        perror("[rdma-xport] server binfo recv");
        rdma_xport_destroy(x);
        return NULL;
    }
    fprintf(stderr, "[DBG-server] binfo recv posted, accept...\n");
    if (rdma_accept(id, NULL)) {
        perror("[rdma-xport] rdma_accept");
        rdma_xport_destroy(x);
        return NULL;
    }
    fprintf(stderr, "[DBG-server] accept OK, poll binfo recv...\n");
    if (poll_completion(id->recv_cq) < 0) {
        fprintf(stderr, "[rdma-xport] no buf_info from client\n");
        rdma_xport_destroy(x);
        return NULL;
    }
    fprintf(stderr, "[DBG-server] got binfo size=%u\n", x->binfo.size);
    if (x->binfo.addr == 0 || x->binfo.size < RDMA_BUF_SIZE) {
        fprintf(stderr, "[rdma-xport] bad buf_info: addr=%llx size=%u\n",
                (unsigned long long)x->binfo.addr, x->binfo.size);
        rdma_xport_destroy(x);
        return NULL;
    }
    return x;
}

rdma_xport_t *rdma_xport_client_connect(const char *ip, int port)
{
    struct rdma_addrinfo hints, *res;
    struct rdma_cm_id *id;
    rdma_xport_t *x = NULL;
    char port_str[16];

    snprintf(port_str, sizeof(port_str), "%d", port);

    memset(&hints, 0, sizeof(hints));
    hints.ai_port_space = RDMA_PS_TCP;
    fprintf(stderr, "[DBG-client] getaddrinfo(%s,%s)...\n", ip ? ip : "*", port_str);
    if (rdma_getaddrinfo(ip, port_str, &hints, &res)) {
        perror("[rdma-xport] rdma_getaddrinfo");
        return NULL;
    }
    fprintf(stderr, "[DBG-client] getaddrinfo OK\n");

    fprintf(stderr, "[DBG-client] create_ep...\n");
    id = create_ep(res, 0);
    rdma_freeaddrinfo(res);
    if (!id)
        return NULL;
    fprintf(stderr, "[DBG-client] create_ep OK id=%p\n", (void *)id);

    x = calloc(1, sizeof(*x));
    if (!x) {
        rdma_destroy_ep(id);
        return NULL;
    }
    x->id = id;

    /* 接收缓冲（远程 WRITE 目标） */
    fprintf(stderr, "[DBG-client] malloc(%u) + reg_write...\n", (unsigned)RDMA_BUF_SIZE);
    x->buf = malloc(RDMA_BUF_SIZE);
    x->mr = x->buf ? rdma_reg_write(id, x->buf, RDMA_BUF_SIZE) : NULL;
    if (!x->mr) {
        perror("[rdma-xport] client buf MR");
        rdma_xport_destroy(x);
        return NULL;
    }
    fprintf(stderr, "[DBG-client] reg_write OK rkey=%u\n", x->mr->rkey);

    /* 缓冲信息，发给服务端 */
    x->binfo.addr = (uintptr_t)x->buf;
    x->binfo.rkey = x->mr->rkey;
    x->binfo.size = RDMA_BUF_SIZE;
    x->binfo_mr = rdma_reg_msgs(id, &x->binfo, sizeof(x->binfo));
    if (!x->binfo_mr) {
        perror("[rdma-xport] client binfo MR");
        rdma_xport_destroy(x);
        return NULL;
    }

    /* 预投递一个 recv：接收尾部 WRITE_WITH_IMM 的完成通知 */
    x->dummy_mr = rdma_reg_msgs(id, &x->dummy, 1);
    if (!x->dummy_mr) {
        perror("[rdma-xport] client dummy MR");
        rdma_xport_destroy(x);
        return NULL;
    }
    struct ibv_sge dsge = {
        .addr   = (uintptr_t)&x->dummy,
        .length = 1,
        .lkey   = x->dummy_mr->lkey,
    };
    struct ibv_recv_wr recv_wr = { 0 }, *bad_wr;
    recv_wr.sg_list = &dsge;
    recv_wr.num_sge = 1;
    if (ibv_post_recv(id->qp, &recv_wr, &bad_wr)) {
        perror("[rdma-xport] ibv_post_recv(dummy)");
        rdma_xport_destroy(x);
        return NULL;
    }
    fprintf(stderr, "[DBG-client] dummy recv posted, now rdma_connect...\n");

    /* 连接 + 发缓冲信息 */
    if (rdma_connect(id, NULL)) {
        perror("[rdma-xport] rdma_connect");
        rdma_xport_destroy(x);
        return NULL;
    }
    rdma_post_send(id, NULL, &x->binfo, sizeof(x->binfo), x->binfo_mr,
                   IBV_SEND_SIGNALED);
    if (poll_completion(id->send_cq) < 0) {
        fprintf(stderr, "[rdma-xport] binfo send failed\n");
        rdma_xport_destroy(x);
        return NULL;
    }
    return x;
}

void rdma_xport_destroy(rdma_xport_t *x)
{
    if (!x)
        return;
    if (x->dummy_mr)
        rdma_dereg_mr(x->dummy_mr);
    if (x->binfo_mr)
        rdma_dereg_mr(x->binfo_mr);
    if (x->mr)
        rdma_dereg_mr(x->mr);
    free(x->buf);
    if (x->id) {
        rdma_disconnect(x->id);
        rdma_destroy_ep(x->id);
    }
    free(x);
}

/*=============================================================================
 *  服务端：发送
 *============================================================================*/

void *rdma_xport_send_buf(rdma_xport_t *x, size_t *capacity)
{
    if (!x->buf)
        return NULL;
    /* 前 4 字节留给总长前缀 */
    if (capacity)
        *capacity = RDMA_BUF_SIZE - 4;
    return x->buf + 4;
}

int rdma_xport_send(rdma_xport_t *x, uint32_t payload_len)
{
    size_t len = (size_t)payload_len + 4;
    int num_chunks = (int)((len + RDMA_XPORT_CHUNK_SIZE - 1) / RDMA_XPORT_CHUNK_SIZE);
    int unpolled = 0;

    /* 总长前缀（客户端按它解析负载） */
    *(uint32_t *)x->buf = htonl(payload_len);

    /* chunk 窗口流水线：全部 signaled，到窗口深度或发完时批量回收 CQE */
    for (int i = 0; i < num_chunks; i++) {
        size_t cur_len = (i == num_chunks - 1)
            ? len - (size_t)i * RDMA_XPORT_CHUNK_SIZE
            : RDMA_XPORT_CHUNK_SIZE;

        struct ibv_sge sge = {
            .addr   = (uintptr_t)x->buf + (size_t)i * RDMA_XPORT_CHUNK_SIZE,
            .length = cur_len,
            .lkey   = x->mr->lkey,
        };
        struct ibv_send_wr wr = { 0 }, *bad_wr;

        wr.wr_id = (uint64_t)i;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        if (i == num_chunks - 1) {
            /* 最后一块：带 IMM 通知对端整块到齐 */
            wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
            wr.imm_data = htonl(RDMA_IMM_DONE);
        } else {
            wr.opcode = IBV_WR_RDMA_WRITE;
        }
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.rdma.remote_addr = x->binfo.addr + (size_t)i * RDMA_XPORT_CHUNK_SIZE;
        wr.wr.rdma.rkey = x->binfo.rkey;

        if (ibv_post_send(x->id->qp, &wr, &bad_wr)) {
            perror("[rdma-xport] ibv_post_send");
            return -1;
        }
        unpolled++;

        if (unpolled == RDMA_XPORT_QP_DEPTH || i == num_chunks - 1) {
            for (int j = 0; j < unpolled; j++) {
                if (poll_completion(x->id->send_cq) < 0)
                    return -1;
            }
            unpolled = 0;
        }
    }
    return 0;
}

/*=============================================================================
 *  客户端：接收
 *============================================================================*/

int rdma_xport_recv(rdma_xport_t *x, const void **payload, uint32_t *payload_len)
{
    /* 等对端 WRITE_WITH_IMM 完成：此时整块数据已落进接收缓冲 */
    if (poll_completion(x->id->recv_cq) < 0)
        return -1;

    *payload = x->buf + 4;
    *payload_len = ntohl(*(uint32_t *)x->buf);
    return 0;
}
