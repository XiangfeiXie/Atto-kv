/*=============================================================================
 *  rdma_transport.c — RDMA 流式批量传输层
 *
 *  分层职责：
 *   - 连接管理：显式事件通道 + poll 超时泵，各阶段失败带日志快速返回
 *   - 握手：复用已验证的 buf_info（addr/rkey/size）SEND/RECV 交换
 *   - 数据面：段池环形调度 + 帧化 WRITE_WITH_IMM + ack 信用流控
 *
 *  段池安全不变式：
 *   1. 发送端：帧只写"本地已完成回收"且"(已发-已ack)<窗口"的槽
 *   2. 接收端：槽由 IMM 完成 CQE 的 wr_id 精确指派，消费完才 ack 归还
 *
 *  RECV WQE 补投纪律（极易出错，集中在此说明）：
 *   - 客户端每个 dummy recv 只能吃掉一帧的 IMM，消费后必须立刻原位补投，
 *     否则超过 NUM_SEGS 帧后对端 WRITE_WITH_IMM 会因无接收资源而失败
 *   - 服务端每个 ack recv 同理，收到一个 ack 后立刻原位补投
 *============================================================================*/

#define _GNU_SOURCE
#include "rdma_transport.h"

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/*=============================================================================
 *  内部类型
 *============================================================================*/

/** 对端缓冲描述符（握手消息：客户端 → 服务端） */
struct buf_info {
    uint64_t addr;          /* 对端段池基址 */
    uint32_t rkey;          /* 访问键 */
    uint32_t size;          /* 整块段池字节数 */
} __attribute__((packed));

/** ack 消息（接收端 → 发送端）：信用归还信号 */
struct ack_msg {
    uint64_t seq;
} __attribute__((packed));

enum { XPORT_SERVER = 0, XPORT_CLIENT = 1 };

struct rdma_xport {
    int role;

    struct rdma_event_channel *ch;   /* CM 事件通道 */
    struct rdma_cm_id       *listen; /* 服务端监听 id */
    struct rdma_cm_id       *id;     /* 数据连接 id */

    struct ibv_pd  *pd;
    struct ibv_cq  *scq;             /* 发送完成队列 */
    struct ibv_cq  *rcq;             /* 接收完成队列 */

    char           *slab;            /* 段池整块内存（连续虚拟地址） */
    size_t          slab_size;
    struct ibv_mr  *slab_mr;

    /* 发送侧状态机（服务端） */
    size_t   head;                   /* 已提交帧数 */
    size_t   done;                   /* 本地发送完成帧数（CQE 回收） */
    size_t   tail;                   /* 对端已确认帧数（ack 收到） */
    size_t   seq_last[RDMA_NUM_SEGS];/* 各槽最近一次占用的帧号；SIZE_MAX=从未占用 */

    /* 对端段池信息（发送方向寻址） */
    uint64_t peer_addr;
    uint32_t peer_rkey;
    uint32_t peer_size;

    /* 接收侧状态（客户端） */
    int      cur_slot;               /* 当前持有未 ack 的槽号，-1 = 无 */

    /* 握手与 ack 缓冲（生命周期到 destroy 为止） */
    struct buf_info  peer_binfo;     /* 接收方暂存对端缓冲信息 */
    struct ibv_mr   *binfo_mr;       /* peer_binfo 的注册内存 */
    struct ack_msg   ack_buf[RDMA_NUM_SEGS];
    struct ibv_mr   *ack_mr;
};

/*=============================================================================
 *  小工具
 *============================================================================*/

static double now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

/** 非阻塞回收一条发送 CQE：返回 1 有完成 / 0 无 / -1 错误 */
static int poll_send_one(struct rdma_xport *x)
{
    struct ibv_wc wc;
    int n = ibv_poll_cq(x->scq, 1, &wc);
    if (n == 0)
        return 0;
    if (n < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "[rdma-xport] send CQ error status=%s\n",
                n < 0 ? "poll-failed" : ibv_wc_status_str(wc.status));
        return -1;
    }
    x->done++;                        /* RC 保序：第 done-1 帧按提交序完成 */
    return 1;
}

/** 非阻塞取一条接收 CQE：返回 1 取到（含错误状态待判） / 0 无 / -1 失败 */
static int poll_recv_one(struct rdma_xport *x, struct ibv_wc *out)
{
    struct ibv_wc wc;
    int n = ibv_poll_cq(x->rcq, 1, &wc);
    if (n == 0)
        return 0;
    if (n < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "[rdma-xport] recv CQ error status=%s\n",
                n < 0 ? "poll-failed" : ibv_wc_status_str(wc.status));
        return -1;
    }
    if (out)
        *out = wc;
    return 1;
}

/**
 * CM 事件泵：poll 事件 fd 带超时；等到目标事件（status==0）返回 0。
 * CONNECT_REQUEST 会通过 out_new 送出新建的子 id（其余阶段可为 NULL）
 */
static int cm_pump(struct rdma_event_channel *ch, enum rdma_cm_event_type expect,
                   const char *role, const char *stage,
                   struct rdma_cm_id **out_new)
{
    double deadline = now_ms() + RDMA_CM_TIMEOUT_MS;

    while (1) {
        int remain = (int)(deadline - now_ms());
        if (remain <= 0) {
            fprintf(stderr, "[rdma-xport] %s: %s TIMEOUT (%dms)\n",
                    role, stage, RDMA_CM_TIMEOUT_MS);
            return -1;
        }

        struct pollfd pfd = { .fd = ch->fd, .events = POLLIN };
        if (poll(&pfd, 1, remain) <= 0)
            continue;                 /* 超时由循环头部统一判定 */

        struct rdma_cm_event *ev = NULL;
        if (rdma_get_cm_event(ch, &ev)) {
            perror("[rdma-xport] rdma_get_cm_event");
            return -1;
        }
        enum rdma_cm_event_type t = ev->event;
        int status = ev->status;
        struct rdma_cm_id *id = ev->id;
        rdma_ack_cm_event(ev);

        if (status) {
            fprintf(stderr, "[rdma-xport] %s: %s rejected status=%d\n",
                    role, stage, status);
            return -1;
        }
        if (t != expect) {
            fprintf(stderr, "[rdma-xport] %s: %s unexpected event %d\n",
                    role, stage, (int)t);
            return -1;
        }
        printf("[rdma-xport] %s: %s (%.1fms)\n", role, stage,
               (double)(RDMA_CM_TIMEOUT_MS - remain));
        if (out_new)
            *out_new = id;
        return 0;
    }
}

/** 建数据连接的 QP 与收发 CQ（需 addr/route 已解析） */
static int xport_setup_qp(struct rdma_xport *x)
{
    x->pd = ibv_alloc_pd(x->id->verbs);
    if (!x->pd) {
        perror("[rdma-xport] alloc_pd");
        return -1;
    }
    x->scq = ibv_create_cq(x->id->verbs, RDMA_QP_SEND_WR, NULL, NULL, 0);
    x->rcq = ibv_create_cq(x->id->verbs, RDMA_QP_RECV_WR, NULL, NULL, 0);
    if (!x->scq || !x->rcq) {
        perror("[rdma-xport] create_cq");
        return -1;
    }

    struct ibv_qp_init_attr qa;
    memset(&qa, 0, sizeof(qa));
    qa.cap.max_send_wr  = RDMA_QP_SEND_WR;
    qa.cap.max_recv_wr  = RDMA_QP_RECV_WR;
    qa.cap.max_send_sge = 1;
    qa.cap.max_recv_sge = 1;
    qa.qp_type = IBV_QPT_RC;
    qa.send_cq = x->scq;
    qa.recv_cq = x->rcq;

    if (rdma_create_qp(x->id, x->pd, &qa)) {
        perror("[rdma-xport] create_qp");
        return -1;
    }
    return 0;
}

/**
 * 分配并注册段池。remote_write：
 *   客户端为 1（允许服务端远程写）；服务端为 0（仅本地读）
 */
static int xport_alloc_slab(struct rdma_xport *x, int remote_write)
{
    x->slab_size = (size_t)RDMA_NUM_SEGS * RDMA_SEG_SIZE;
    x->slab = calloc(1, x->slab_size);
    if (!x->slab) {
        fprintf(stderr, "[rdma-xport] malloc(%zu) failed\n", x->slab_size);
        return -1;
    }
    x->slab_mr = remote_write
        ? rdma_reg_write(x->id, x->slab, x->slab_size)
        : rdma_reg_msgs(x->id, x->slab, x->slab_size);
    if (!x->slab_mr) {
        perror("[rdma-xport] reg slab mr");
        return -1;
    }
    return 0;
}

/* ---- RECV WQE 补投 ---- */

static int client_repost_dummy(struct rdma_xport *x, int slot)
{
    void *base = x->slab + (size_t)slot * RDMA_SEG_SIZE;
    return rdma_post_recv(x->id, (void *)(long)slot, base,
                          (int)sizeof(uint64_t), x->slab_mr);
}

static int server_repost_ack(struct rdma_xport *x, int slot)
{
    return rdma_post_recv(x->id, (void *)(long)slot, &x->ack_buf[slot],
                          (int)sizeof(struct ack_msg), x->ack_mr);
}

/*=============================================================================
 *  连接建立 — 服务端
 *============================================================================*/

rdma_xport_t *rdma_xport_server_accept(const char *ip, int port, int ready_fd)
{
    double t0 = now_ms();

    rdma_xport_t *x = calloc(1, sizeof(*x));
    if (!x)
        return NULL;
    x->role = XPORT_SERVER;
    x->cur_slot = -1;
    for (int i = 0; i < RDMA_NUM_SEGS; i++)
        x->seq_last[i] = SIZE_MAX;

    x->ch = rdma_create_event_channel();
    if (!x->ch) { perror("create_event_channel"); goto fail; }

    if (rdma_create_id(x->ch, &x->listen, NULL, RDMA_PS_TCP)) {
        perror("create_id"); goto fail;
    }

    {
        struct sockaddr_in sa;
        memset(&sa, 0, sizeof(sa));
        sa.sin_family = AF_INET;
        sa.sin_port = htons((uint16_t)port);
        sa.sin_addr.s_addr = (!ip || !strcmp(ip, "0.0.0.0"))
                           ? htonl(INADDR_ANY) : 0;
        if ((!ip || strcmp(ip, "0.0.0.0")) &&
            ip && !inet_pton(AF_INET, ip, &sa.sin_addr)) {
            fprintf(stderr, "[rdma-xport] bad listen ip '%s'\n", ip);
            goto fail;
        }
        if (rdma_bind_addr(x->listen, (struct sockaddr *)&sa)) {
            perror("bind_addr"); goto fail;
        }
    }
    if (rdma_listen(x->listen, 1)) {
        perror("listen"); goto fail;
    }
    printf("[rdma] transport listening %s:%d (%.1fms)\n",
           ip ? ip : "*", port, now_ms() - t0);

    /* 监听就绪，通知父进程可开始 PSYNC 应答 */
    if (ready_fd >= 0) {
        char r = 1;
        ssize_t _w = write(ready_fd, &r, 1);
        (void)_w;
        close(ready_fd);
    }

    if (cm_pump(x->ch, RDMA_CM_EVENT_CONNECT_REQUEST, "server",
                "CONNECT_REQUEST", &x->id))
        goto fail;

    if (xport_setup_qp(x)) goto fail;
    if (xport_alloc_slab(x, 0)) goto fail;

    /* ack 接收缓冲先注册好（recv 投递放到拿到 binfo 之后） */
    x->ack_mr = rdma_reg_msgs(x->id, x->ack_buf, sizeof(x->ack_buf));
    x->binfo_mr = rdma_reg_msgs(x->id, &x->peer_binfo, sizeof(x->peer_binfo));
    if (!x->ack_mr || !x->binfo_mr) {
        perror("[rdma-xport] reg handshake mr"); goto fail;
    }
    if (rdma_post_recv(x->id, NULL, &x->peer_binfo, sizeof(x->peer_binfo),
                       x->binfo_mr)) {
        perror("[rdma-xport] post recv binfo"); goto fail;
    }

    struct rdma_conn_param ap;
    memset(&ap, 0, sizeof(ap));
    ap.responder_resources = 1;
    ap.initiator_depth     = 1;

    if (rdma_accept(x->id, &ap)) {
        perror("accept"); goto fail;
    }
    if (cm_pump(x->ch, RDMA_CM_EVENT_ESTABLISHED, "server", "ESTABLISHED", NULL))
        goto fail;

    /* 等对端 buf_info 到达（首个非 IMM 消息即它） */
    {
        struct ibv_wc wc;
        int got = 0;
        while (!got) {
            int n = poll_recv_one(x, &wc);
            if (n < 0) goto fail;
            if (n == 0) continue;
            got = !(wc.wc_flags & IBV_WC_WITH_IMM);
        }
    }
    if (x->peer_binfo.addr == 0 ||
        x->peer_binfo.size < (uint32_t)x->slab_size) {
        fprintf(stderr, "[rdma-xport] bad buf_info size=%u\n",
                x->peer_binfo.size);
        goto fail;
    }
    x->peer_addr = x->peer_binfo.addr;
    x->peer_rkey = x->peer_binfo.rkey;

    /* 信用供给就绪后才可能收到首个 ack（客户端见数据才会 ack） */
    for (int i = 0; i < RDMA_NUM_SEGS; i++)
        if (server_repost_ack(x, i)) goto fail;

    printf("[rdma] server ready (%.1fms total)\n", now_ms() - t0);
    return x;

fail:
    rdma_xport_destroy(x);
    return NULL;
}

/*=============================================================================
 *  连接建立 — 客户端
 *============================================================================*/

rdma_xport_t *rdma_xport_client_connect(const char *ip, int port)
{
    double t0 = now_ms(), last = t0;

    rdma_xport_t *x = calloc(1, sizeof(*x));
    if (!x)
        return NULL;
    x->role = XPORT_CLIENT;
    x->cur_slot = -1;

    x->ch = rdma_create_event_channel();
    if (!x->ch) { perror("create_event_channel"); goto fail; }

    if (rdma_create_id(x->ch, &x->id, NULL, RDMA_PS_TCP)) {
        perror("create_id"); goto fail;
    }

    {
        struct sockaddr_in dst, src;
        memset(&dst, 0, sizeof(dst));
        dst.sin_family = AF_INET;
        dst.sin_port = htons((uint16_t)port);
        if (!ip || !inet_pton(AF_INET, ip, &dst.sin_addr)) {
            fprintf(stderr, "[rdma-xport] bad master ip '%s'\n", ip ? ip : "-");
            goto fail;
        }
        memset(&src, 0, sizeof(src));
        src.sin_family = AF_INET;
        src.sin_addr.s_addr = htonl(INADDR_ANY);

        if (rdma_resolve_addr(x->id, (struct sockaddr *)&src,
                              (struct sockaddr *)&dst, RDMA_CM_TIMEOUT_MS)) {
            perror("resolve_addr"); goto fail;
        }
    }
    if (cm_pump(x->ch, RDMA_CM_EVENT_ADDR_RESOLVED, "client",
                "ADDR_RESOLVED", NULL)) goto fail;
    last = now_ms();

    if (rdma_resolve_route(x->id, RDMA_CM_TIMEOUT_MS)) {
        perror("resolve_route"); goto fail;
    }
    if (cm_pump(x->ch, RDMA_CM_EVENT_ROUTE_RESOLVED, "client",
                "ROUTE_RESOLVED", NULL)) goto fail;
    last = now_ms();

    if (xport_setup_qp(x)) goto fail;
    if (xport_alloc_slab(x, 1)) goto fail;

    /* 预投递 dummy recv：IMM 完成时 wr_id 即落地槽号（消费后须补投） */
    for (int i = 0; i < RDMA_NUM_SEGS; i++)
        if (client_repost_dummy(x, i)) {
            perror("[rdma-xport] post recv dummy"); goto fail;
        }

    /* 显式连接参数：NULL 时各字段内容未定义，不同进程初始化状态可能
     * 让响应端以 REJ_INVALID_RETRY_COUNT 等原因拒绝（实测 proactor 下触发） */
    struct rdma_conn_param cp;
    memset(&cp, 0, sizeof(cp));
    cp.responder_resources = 1;
    cp.initiator_depth     = 1;
    cp.retry_count         = 7;
    cp.rnr_retry_count     = 7;

    if (rdma_connect(x->id, &cp)) {
        perror("connect"); goto fail;
    }
    if (cm_pump(x->ch, RDMA_CM_EVENT_ESTABLISHED, "client",
                "ESTABLISHED", NULL)) goto fail;
    printf("[rdma-xport] client: connected (%.1fms, setup %.1fms)\n",
           now_ms() - t0, now_ms() - last);

    /* 发送 buf_info（signaled，等本地完成确认链路已可用） */
    x->peer_binfo.addr = (uintptr_t)x->slab;
    x->peer_binfo.rkey = x->slab_mr->rkey;
    x->peer_binfo.size = (uint32_t)x->slab_size;
    x->binfo_mr = rdma_reg_msgs(x->id, &x->peer_binfo, sizeof(x->peer_binfo));
    x->ack_mr = rdma_reg_msgs(x->id, x->ack_buf, sizeof(x->ack_buf));
    if (!x->binfo_mr || !x->ack_mr) {
        perror("[rdma-xport] reg handshake mr"); goto fail;
    }
    {
        struct ibv_sge sge = {
            .addr = (uintptr_t)&x->peer_binfo,
            .length = (uint32_t)sizeof(x->peer_binfo),
            .lkey = x->binfo_mr->lkey,
        };
        struct ibv_send_wr wr = { 0 }, *bad = NULL;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_SEND;
        wr.send_flags = IBV_SEND_SIGNALED;

        if (ibv_post_send(x->id->qp, &wr, &bad)) {
            perror("[rdma-xport] post send binfo"); goto fail;
        }
        while (poll_send_one(x) == 0)
            ;
    }
    printf("[rdma-xport] client: buf_info sent (%.1fms total)\n",
           now_ms() - t0);
    return x;

fail:
    rdma_xport_destroy(x);
    return NULL;
}

/*=============================================================================
 *  发送端：生产帧
 *============================================================================*/

/** 推进一次回收：至多一条发送 CQE + 至多一条 ack（并补投对应 recv） */
static void xport_reclaim_once(struct rdma_xport *x)
{
    poll_send_one(x);

    struct ibv_wc wc;
    int n = poll_recv_one(x, &wc);
    if (n > 0 && !(wc.wc_flags & IBV_WC_WITH_IMM)) {
        int slot = (int)(long)wc.wr_id;
        x->tail++;                    /* RC 保序：连续帧依序确认 */
        server_repost_ack(x, slot);   /* 补投信用接收资源 */
    }
}

void *rdma_xport_buf_get(rdma_xport_t *x, size_t *capacity)
{
    if (x->role != XPORT_SERVER)
        return NULL;
    if (capacity)
        *capacity = RDMA_SEG_SIZE;

    while (1) {
        int slot = (int)(x->head % RDMA_NUM_SEGS);

        int credit_ok = (x->head - x->tail) < RDMA_ACK_WINDOW;
        int slot_free = (x->seq_last[slot] == SIZE_MAX)
                     || (x->seq_last[slot] < x->done);

        if (credit_ok && slot_free)
            return x->slab + (size_t)slot * RDMA_SEG_SIZE;

        xport_reclaim_once(x);
    }
}

int rdma_xport_buf_post(rdma_xport_t *x, size_t bytes, int last)
{
    if (x->role != XPORT_SERVER || bytes == 0 || bytes > RDMA_SEG_SIZE)
        return -1;

    int slot = (int)(x->head % RDMA_NUM_SEGS);
    uintptr_t base = (uintptr_t)(x->slab + (size_t)slot * RDMA_SEG_SIZE);
    uintptr_t remote_base = x->peer_addr + (size_t)slot * RDMA_SEG_SIZE;

    size_t off = 0;
    while (off < bytes) {
        size_t chunk = (bytes - off > RDMA_CHUNK_SIZE)
                     ? RDMA_CHUNK_SIZE : (bytes - off);
        int final_sub = (off + chunk == bytes);

        struct ibv_sge sge = {
            .addr   = base + off,
            .length = (uint32_t)chunk,
            .lkey   = x->slab_mr->lkey,
        };
        struct ibv_send_wr wr = { 0 }, *bad = NULL;
        wr.wr_id    = x->head;
        wr.sg_list  = &sge;
        wr.num_sge  = 1;
        wr.wr.rdma.remote_addr = remote_base + off;
        wr.wr.rdma.rkey = x->peer_rkey;

        if (final_sub) {
            wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
            wr.imm_data = htonl(last ? (RDMA_IMM_DONE | (uint32_t)bytes)
                                     : (uint32_t)bytes);
            wr.send_flags = IBV_SEND_SIGNALED;   /* 一帧只 signal 末尾子请求 */
        } else {
            wr.opcode = IBV_WR_RDMA_WRITE;
        }

        if (ibv_post_send(x->id->qp, &wr, &bad)) {
            perror("[rdma-xport] post_send");
            return -1;
        }
        off += chunk;
    }

    x->seq_last[slot] = x->head;                 /* 占用登记 */
    x->head++;
    return 0;
}

int rdma_xport_flush(rdma_xport_t *x)
{
    if (x->role != XPORT_SERVER)
        return -1;
    while (x->done < x->head)
        if (poll_send_one(x) < 0)
            return -1;
    return 0;
}

/*=============================================================================
 *  接收端：消费帧
 *============================================================================*/

int rdma_xport_frame_recv(rdma_xport_t *x, const void **data,
                          uint32_t *len, int *last)
{
    if (x->role != XPORT_CLIENT || x->cur_slot >= 0)
        return -1;

    while (1) {
        struct ibv_wc wc;
        int n = poll_recv_one(x, &wc);
        if (n < 0)
            return -1;
        if (n == 0)
            continue;
        if (!(wc.wc_flags & IBV_WC_WITH_IMM))
            continue;                             /* 忽略异常消息 */

        int slot = (int)(long)wc.wr_id;
        uint32_t imm = ntohl(wc.imm_data);
        uint32_t flen = RDMA_IMM_LEN(imm);
        if (flen == 0 || flen > RDMA_SEG_SIZE) {
            fprintf(stderr, "[rdma-xport] bad frame len %u\n", flen);
            return -1;
        }

        *data = x->slab + (size_t)slot * RDMA_SEG_SIZE;
        *len = flen;
        *last = (imm & RDMA_IMM_DONE) ? 1 : 0;
        x->cur_slot = slot;

        if (client_repost_dummy(x, slot)) {       /* 立刻补投，永不让 RQ 断粮 */
            perror("[rdma-xport] repost dummy");
            return -1;
        }
        return 0;
    }
}

int rdma_xport_frame_ack(rdma_xport_t *x)
{
    if (x->role != XPORT_CLIENT || x->cur_slot < 0)
        return -1;

    int slot = x->cur_slot;
    x->cur_slot = -1;

    struct ibv_sge sge = {
        .addr   = (uintptr_t)&x->ack_buf[slot],
        .length = (uint32_t)sizeof(struct ack_msg),
        .lkey   = x->ack_mr->lkey,
    };
    struct ibv_send_wr wr = { 0 }, *bad = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode  = IBV_WR_SEND;                     /* unsignaled：不必逐个等 */

    if (ibv_post_send(x->id->qp, &wr, &bad)) {
        perror("[rdma-xport] post ack");
        return -1;
    }
    return 0;
}

/*=============================================================================
 *  清理
 *============================================================================*/

void rdma_xport_destroy(rdma_xport_t *x)
{
    if (!x)
        return;

    if (x->id && x->id->qp)
        rdma_disconnect(x->id);

    if (x->slab_mr)  rdma_dereg_mr(x->slab_mr);
    if (x->binfo_mr) rdma_dereg_mr(x->binfo_mr);
    if (x->ack_mr)   rdma_dereg_mr(x->ack_mr);
    free(x->slab);

    if (x->id) {
        if (x->id->qp)
            rdma_destroy_qp(x->id);
        rdma_destroy_id(x->id);
    }
    if (x->listen)
        rdma_destroy_id(x->listen);
    if (x->scq) ibv_destroy_cq(x->scq);
    if (x->rcq) ibv_destroy_cq(x->rcq);
    if (x->pd)  ibv_dealloc_pd(x->pd);
    if (x->ch)  rdma_destroy_event_channel(x->ch);
    free(x);
}
