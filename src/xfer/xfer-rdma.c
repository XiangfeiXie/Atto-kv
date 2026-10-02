/*=============================================================================
 *  xfer-rdma.c — RDMA 传输速率测试组件（自包含，不依赖项目任何头文件）
 *
 *  只做一件事：把命令行给的一个文件整块 RDMA_WRITE 到对端内存，报发送端的
 *  纯传输速率。客户端把接收缓冲的 {addr,rkey} 发给服务端，服务端把整个文件
 *  作为**一个 WR**（WRITE_WITH_IMM）直写过去，IMM 里带上字节数当完成通知；
 *  客户端等到那条 IMM 就结束。没有分块、没有滑动窗口、没有 ACK —— RC 可靠
 *  有序 + 收发缓冲都是固定的一大块，本来就不需要。
 *
 *  与 sendfile 侧的语义差别（对比读数时要知道）：这里的 send CQE 意味着
 *  **对端已 ACK**（数据确已写进对端内存）；sendfile 的 sendfile() 返回只表示
 *  字节进了本端发送缓冲。前者是真正的端到端。
 *
 *  用法：
 *    xfer-rdma server <file> <port>
 *    xfer-rdma client <ip> <port> [--discard]
 *============================================================================*/

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

/* 收发缓冲大小 = 可传文件的上限（与 kvstore 的 RDMA_BUF_SIZE 同值） */
#define RDMA_BUF_SIZE   (512ULL * 1024 * 1024)

/* 对端缓冲凭证：谁接收数据，谁把自己的 addr/rkey 发给对方 */
struct buf_info {
    uint64_t addr;
    uint32_t rkey;
} __attribute__((packed));

/*=============================================================================
 *  小工具
 *============================================================================*/

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* 十进制 MB/s（与 sendfile 侧同口径） */
static double rate_mbs(size_t bytes, double ms)
{
    return (ms > 0.0) ? ((double)bytes / (ms / 1000.0) / 1e6) : 0.0;
}

/* 双端一致的 QP 属性：整块一个 WR 的模型下每个方向只用 1 个 WQE，深度就是 1 */
static void qp_attr(struct ibv_qp_init_attr *qa)
{
    memset(qa, 0, sizeof(*qa));
    qa->cap.max_send_wr  = 1;
    qa->cap.max_recv_wr  = 1;
    qa->cap.max_send_sge = 1;
    qa->cap.max_recv_sge = 1;
}

/* 阻塞轮询一条完成（纯轮询占 CPU 直到到达） */
static int poll_cq(struct ibv_cq *cq, struct ibv_wc *wc, const char *what)
{
    int n;
    do {
        n = ibv_poll_cq(cq, 1, wc);
    } while (n == 0);

    if (n < 0 || wc->status != IBV_WC_SUCCESS) {
        fprintf(stderr, "[rdma] %s CQ error: %s\n", what,
                n < 0 ? "poll failed" : ibv_wc_status_str(wc->status));
        return -1;
    }
    return 0;
}

/* 显式填连接参数：留 NULL 时字段内容不定，某些情况下会被对端拒（INVALID_RETRY_COUNT） */
static void conn_param(struct rdma_conn_param *cp)
{
    memset(cp, 0, sizeof(*cp));
    cp->responder_resources = 1;
    cp->initiator_depth     = 1;
    cp->retry_count         = 7;
    cp->rnr_retry_count     = 7;
}

/*=============================================================================
 *  服务端：accept → 收凭证 → 读文件 → 一个 WR 直写
 *============================================================================*/

static int run_server(const char *path, int port)
{
    struct rdma_addrinfo hints, *res = NULL;
    struct rdma_cm_id *listen_id = NULL, *id = NULL;
    struct ibv_mr *info_mr = NULL, *mr = NULL;
    struct buf_info peer;
    char *buf = NULL;
    char port_str[16];
    int rc = -1;

    memset(&hints, 0, sizeof(hints));
    hints.ai_flags      = RAI_PASSIVE;
    hints.ai_port_space = RDMA_PS_TCP;
    snprintf(port_str, sizeof(port_str), "%d", port);
    if (rdma_getaddrinfo(NULL, port_str, &hints, &res)) {
        perror("[rdma] getaddrinfo");
        return -1;
    }

    struct ibv_qp_init_attr qa;
    qp_attr(&qa);

    if (rdma_create_ep(&listen_id, res, NULL, &qa)) {
        perror("[rdma] create_ep");
        goto out;
    }
    rdma_freeaddrinfo(res);
    res = NULL;

    /* conn 计时起点：从这里到"连接可用且对端凭证到手"，含**等对端来连**的那段。
     * 所以它不是传输成本，是启动先后次序的成本 —— kvstore 里这一项 5.7s 占大头，
     * 也是这个原因（传输子进程/对端起来得比监听晚）。 */
    uint64_t t0 = now_ns();

    if (rdma_listen(listen_id, 1)) { perror("[rdma] listen"); goto out; }
    printf("[rdma] listening :%d  (%s)\n", port, path);

    if (rdma_get_request(listen_id, &id)) { perror("[rdma] get_request"); goto out; }

    /* 先投递凭证接收再 accept：客户端连上就会发凭证，此刻 RQ 为空会 RNR 重试到毁链 */
    info_mr = rdma_reg_msgs(id, &peer, sizeof(peer));
    if (!info_mr) { perror("[rdma] reg info mr"); goto out; }
    if (rdma_post_recv(id, NULL, &peer, sizeof(peer), info_mr)) {
        perror("[rdma] post recv info");
        goto out;
    }

    struct rdma_conn_param ap;
    conn_param(&ap);
    if (rdma_accept(id, &ap)) { perror("[rdma] accept"); goto out; }

    /*-----------------------------------------------------------------------
     *  连接就绪后才读文件：读 158MB 是百毫秒级，若先读后监听，对端 REQ 落在
     *  监听就绪之前会被丢、双方挂死
     *-----------------------------------------------------------------------*/
    if (poll_cq(id->recv_cq, &(struct ibv_wc){0}, "buf_info")) goto out;
    uint64_t t1 = now_ns();

    struct stat sst;
    if (stat(path, &sst) != 0) { perror(path); goto out; }
    if ((uint64_t)sst.st_size > RDMA_BUF_SIZE) {
        fprintf(stderr, "[rdma] file %llu > buf %llu, 拒绝静默截断\n",
                (unsigned long long)sst.st_size,
                (unsigned long long)RDMA_BUF_SIZE);
        goto out;
    }

    FILE *fp = fopen(path, "rb");
    if (!fp) { perror(path); goto out; }
    buf = malloc(RDMA_BUF_SIZE);
    if (!buf) { fprintf(stderr, "[rdma] malloc failed\n"); fclose(fp); goto out; }
    size_t fsize = fread(buf, 1, RDMA_BUF_SIZE, fp);
    fclose(fp);
    if (fsize != (size_t)sst.st_size) {
        fprintf(stderr, "[rdma] short read %zu/%llu\n", fsize,
                (unsigned long long)sst.st_size);
        goto out;
    }
    uint64_t t2 = now_ns();

    printf("[rdma] client connected, %zu bytes\n", fsize);

    /*-----------------------------------------------------------------------
     *  纯传输：一个 WR 整块直写 + 等唯一那条 send CQE
     *-----------------------------------------------------------------------*/
    mr = rdma_reg_msgs(id, buf, fsize);
    if (!mr) { perror("[rdma] reg_msgs"); goto out; }

    struct ibv_sge sge = {
        .addr   = (uintptr_t)buf,
        .length = (uint32_t)fsize,
        .lkey   = mr->lkey,
    };
    struct ibv_send_wr wr = { 0 }, *bad = NULL;
    wr.sg_list    = &sge;
    wr.num_sge    = 1;
    wr.opcode     = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.imm_data   = (uint32_t)fsize;          /* 客户端从 IMM 里读回字节数 */
    wr.wr.rdma.remote_addr = peer.addr;
    wr.wr.rdma.rkey        = peer.rkey;

    uint64_t t3 = now_ns();
    if (ibv_post_send(id->qp, &wr, &bad)) { perror("[rdma] post_send"); goto out; }
    if (poll_cq(id->send_cq, &(struct ibv_wc){0}, "send")) goto out;
    uint64_t t4 = now_ns();

    {
        double ms = (double)(t4 - t3) / 1e6;
        printf("[rdma] xfer-end: bytes=%zu xfer=%.1fms rate=%.2fMB/s | prep: "
               "conn=%.1fms read=%.1fms\n",
               fsize, ms, rate_mbs(fsize, ms),
               (double)(t1 - t0) / 1e6, (double)(t2 - t1) / 1e6);
    }
    rc = 0;

out:
    if (mr)        rdma_dereg_mr(mr);
    if (info_mr)   rdma_dereg_mr(info_mr);
    if (buf)       free(buf);
    if (id) {
        if (id->qp) rdma_disconnect(id);
        rdma_destroy_ep(id);
    }
    if (listen_id) rdma_destroy_ep(listen_id);
    if (res)       rdma_freeaddrinfo(res);
    return rc;
}

/*=============================================================================
 *  客户端：注册缓冲 → 发凭证 → 等 IMM
 *============================================================================*/

static int run_client(const char *ip, int port, int discard)
{
    struct rdma_addrinfo hints, *res = NULL;
    struct rdma_cm_id *id = NULL;
    struct ibv_mr *mr = NULL, *info_mr = NULL;
    char *buf = NULL;
    char port_str[16];
    int rc = -1;

    memset(&hints, 0, sizeof(hints));
    hints.ai_port_space = RDMA_PS_TCP;
    snprintf(port_str, sizeof(port_str), "%d", port);
    if (rdma_getaddrinfo(ip, port_str, &hints, &res)) {
        perror("[rdma] getaddrinfo");
        return -1;
    }

    struct ibv_qp_init_attr qa;
    qp_attr(&qa);

    if (rdma_create_ep(&id, res, NULL, &qa)) { perror("[rdma] create_ep"); goto out; }
    rdma_freeaddrinfo(res);
    res = NULL;

    /* 接收缓冲（远程写目标）：整块，service 端直接往里写 */
    buf = malloc(RDMA_BUF_SIZE);
    if (!buf) { fprintf(stderr, "[rdma] malloc failed\n"); goto out; }

    mr = rdma_reg_write(id, buf, RDMA_BUF_SIZE);
    if (!mr) { perror("[rdma] reg_write"); goto out; }

    /* 预投递一个 dummy recv 接那条 IMM：IMM 不带 payload，HCA 不往里 DMA，
     * 它只是接完成事件的一个"坑位"。所以 post 完就 dereg 是安全的 —— 但内存
     * 本身必须是 static 的（WQE 里留的是注册时的 lkey，注销后内核不再跟踪） */
    {
        static char dummy;
        struct ibv_mr *dmr = rdma_reg_msgs(id, &dummy, 1);
        if (!dmr) { perror("[rdma] reg dummy"); goto out; }
        if (rdma_post_recv(id, NULL, &dummy, 1, dmr)) {
            perror("[rdma] post recv dummy");
            goto out;
        }
        rdma_dereg_mr(dmr);
    }

    struct rdma_conn_param cp;
    conn_param(&cp);
    if (rdma_connect(id, &cp)) { perror("[rdma] connect"); goto out; }

    /* 把本端缓冲凭证发给服务端 */
    {
        struct buf_info bi = { .addr = (uintptr_t)buf, .rkey = mr->rkey };
        info_mr = rdma_reg_msgs(id, &bi, sizeof(bi));
        if (!info_mr) { perror("[rdma] reg info"); goto out; }
        if (rdma_post_send(id, NULL, &bi, sizeof(bi), info_mr, IBV_SEND_SIGNALED)) {
            perror("[rdma] post send info");
            goto out;
        }
        if (poll_cq(id->send_cq, &(struct ibv_wc){0}, "buf_info-send")) goto out;
    }

    /* 等到那条 IMM = 整块已落地；字节数从 imm_data 取 */
    struct ibv_wc wc;
    uint64_t t1 = now_ns();
    if (poll_cq(id->recv_cq, &wc, "imm")) goto out;
    uint64_t t2 = now_ns();

    /* 扫一遍内容（--discard 跳过）：只是让"真收到了"可观测，不参与速率 */
    uint32_t nbytes = wc.imm_data < RDMA_BUF_SIZE ? wc.imm_data : (uint32_t)RDMA_BUF_SIZE;
    if (!discard) {
        unsigned long sum = 0;
        for (uint32_t i = 0; i < nbytes; i++) sum += (unsigned char)buf[i];
        printf("[rdma] checksum=%lu\n", sum);
    }

    printf("[rdma] recv-end: bytes=%u wait=%.1fms%s\n",
           wc.imm_data, (double)(t2 - t1) / 1e6, discard ? "  (discard)" : "");
    rc = 0;

out:
    if (mr)      rdma_dereg_mr(mr);
    if (info_mr) rdma_dereg_mr(info_mr);
    if (buf)     free(buf);
    if (id) {
        if (id->qp) rdma_disconnect(id);
        rdma_destroy_ep(id);
    }
    if (res) rdma_freeaddrinfo(res);
    return rc;
}

/*=============================================================================
 *  main
 *============================================================================*/

int main(int argc, char **argv)
{
    setlinebuf(stdout);
    setlinebuf(stderr);

    if (argc < 4) {
        fprintf(stderr,
                "用法:\n"
                "  %s server <file> <port>\n"
                "  %s client <ip> <port> [--discard]\n"
                "  （收发缓冲固定 %llu MiB，即文件上限）\n",
                argv[0], argv[0], (unsigned long long)(RDMA_BUF_SIZE >> 20));
        return 1;
    }

    int port = atoi(argv[3]);
    if (port <= 0 || port > 65535) { fprintf(stderr, "端口不合法\n"); return 1; }

    if (!strcmp(argv[1], "server") && argc == 4)
        return run_server(argv[2], port) == 0 ? 0 : 1;

    if (!strcmp(argv[1], "client")) {
        int discard = 0;
        for (int i = 4; i < argc; i++) {
            if (!strcmp(argv[i], "--discard")) discard = 1;
            else { fprintf(stderr, "未知参数: %s\n", argv[i]); return 1; }
        }
        return run_client(argv[2], port, discard) == 0 ? 0 : 1;
    }

    fprintf(stderr, "角色只能是 server 或 client\n");
    return 1;
}
