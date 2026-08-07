/*
 * rdma_bw.c — 基于原生 RDMA verbs (Soft-RoCE / rxe) 的单向带宽基准
 *
 * 使用 librdmacm + libibverbs，RDMA_WRITE 单边写入。
 * 依赖 Soft-RoCE (rxe) 内核模块，无需特殊硬件。
 *
 * 用法:
 *   # 确保 rxe 设备已配置: rdma link add rxe0 type rxe netdev eth0
 *   ./rdma_bw --server --port 20886 --size 100M &
 *   ./rdma_bw --client 192.168.x.x --port 20886 --size 100M
 *
 * 对比:
 *   for sz in 1M 10M 50M 100M 500M 1G; do
 *     ./rdma_bw --server --size $sz &
 *     sleep 1
 *     ./rdma_bw --client 127.0.0.1 --size $sz
 *     wait
 *   done
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

#define BATCH_SIZE    (1024 * 1024)   /* 1MB per RDMA_WRITE */
#define QP_DEPTH      16              /* 滑动窗口深度 */

static const char *g_node     = "127.0.0.1";
static const char *g_port     = "20886";
static int         g_server   = 0;
static size_t      g_total    = 100 * 1024 * 1024;  /* 100MB */
static size_t      g_batch    = BATCH_SIZE;

/* RDMA 内存区域描述符 */
struct mem_info {
    uint64_t addr;
    uint32_t rkey;
};

/* ── helpers ───────────────────────────────────────────────────────── */

static long long now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

static int poll_cq(struct ibv_cq *cq) {
    struct ibv_wc wc;
    int n;
    do { n = ibv_poll_cq(cq, 1, &wc); } while (n == 0);
    if (n < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "CQ error: n=%d status=%d\n", n, wc.status);
        return -1;
    }
    return 0;
}

/* ── server ────────────────────────────────────────────────────────── */

static int run_server(void) {
    struct rdma_addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));
    hints.ai_flags     = RAI_PASSIVE;
    hints.ai_port_space = RDMA_PS_TCP;

    /* server 需要绑定到 rxe 设备对应的真实 IP，不能用 127.0.0.1 */
    const char *bind_addr = g_node;
    if (!strcmp(bind_addr, "127.0.0.1")) {
        /* 自动检测：用 hostname 或第一个非 lo 的 IP */
        char host[256];
        if (gethostname(host, sizeof(host)) == 0)
            bind_addr = host;
    }
    if (rdma_getaddrinfo(bind_addr, g_port, &hints, &res)) {
        perror("rdma_getaddrinfo"); return -1;
    }

    struct ibv_qp_init_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.cap.max_send_wr  = 16;
    attr.cap.max_recv_wr  = 16;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.sq_sig_all       = 1;

    struct rdma_cm_id *listen_id, *id;
    if (rdma_create_ep(&listen_id, res, NULL, &attr)) { perror("rdma_create_ep"); return -1; }
    rdma_freeaddrinfo(res);

    if (rdma_listen(listen_id, 1)) { perror("rdma_listen"); return -1; }
    printf("[server] listening on port %s\n", g_port);

    if (rdma_get_request(listen_id, &id)) { perror("rdma_get_request"); return -1; }

    /* 注册 MR: client 的写入目标 */
    char *buf = malloc(g_total);
    memset(buf, 0, g_total);
    struct ibv_mr *mr = rdma_reg_write(id, buf, g_total);
    if (!mr) { perror("rdma_reg_write"); return -1; }

    struct mem_info local_info;
    local_info.addr = (uintptr_t)buf;
    local_info.rkey = mr->rkey;

    /* 注册 MR info 传输用的 buffer */
    struct ibv_mr *info_mr = rdma_reg_msgs(id, &local_info, sizeof(local_info));
    if (!info_mr) { perror("rdma_reg_msgs"); return -1; }

    printf("[server] MR: buf=%p  len=%zu MB  rkey=0x%x\n",
           (void *)buf, g_total / (1024*1024), mr->rkey);

    /* 构造 dummy recv 用于捕获最后的 RDMA_WRITE_WITH_IMM */
    char dummy = 'x';
    struct ibv_mr *dummy_mr = rdma_reg_msgs(id, &dummy, 1);
    struct ibv_sge dsge = { .addr = (uintptr_t)&dummy, .length = 1, .lkey = dummy_mr->lkey };
    struct ibv_recv_wr recv_wr = {0}, *bad_wr;
    recv_wr.wr_id   = 999;
    recv_wr.sg_list = &dsge;
    recv_wr.num_sge = 1;
    if (ibv_post_recv(id->qp, &recv_wr, &bad_wr)) { perror("ibv_post_recv"); return -1; }

    if (rdma_accept(id, NULL)) { perror("rdma_accept"); return -1; }

    /* 发送本地 MR 信息给 client */
    struct timeval t1, t2;
    gettimeofday(&t1, NULL);
    rdma_post_send(id, NULL, &local_info, sizeof(local_info), info_mr, 0);
    if (poll_cq(id->send_cq) < 0) return -1;

    /* 等待最后一个 WRITE_WITH_IMM 的 recv 完成 */
    printf("[server] waiting for RDMA writes...\n");
    if (poll_cq(id->recv_cq) < 0) return -1;
    gettimeofday(&t2, NULL);

    double ms = (t2.tv_sec - t1.tv_sec) * 1000.0 + (t2.tv_usec - t1.tv_usec) / 1000.0;
    double mbps = (ms > 0) ? (g_total / (ms / 1000.0) / (1024.0 * 1024.0)) : 0;
    printf("[server] recv: %zu bytes / %.1f ms  ->  %.2f MB/s\n",
           g_total, ms, mbps);

    /* cleanup */
    rdma_dereg_mr(dummy_mr);
    rdma_dereg_mr(info_mr);
    rdma_dereg_mr(mr);
    free(buf);
    rdma_disconnect(id);
    rdma_destroy_ep(id);
    rdma_destroy_ep(listen_id);
    return 0;
}

/* ── client ────────────────────────────────────────────────────────── */

static int run_client(void) {
    struct rdma_addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));
    hints.ai_port_space = RDMA_PS_TCP;

    if (rdma_getaddrinfo(g_node, g_port, &hints, &res)) {
        perror("rdma_getaddrinfo"); return -1;
    }

    struct ibv_qp_init_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.cap.max_send_wr  = QP_DEPTH * 2;
    attr.cap.max_recv_wr  = 16;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.sq_sig_all       = 1;

    struct rdma_cm_id *id;
    if (rdma_create_ep(&id, res, NULL, &attr)) { perror("rdma_create_ep"); return -1; }
    rdma_freeaddrinfo(res);

    /* 注册发送 buffer（单 chunk 复用） */
    char *buf = malloc(g_batch);
    memset(buf, 'A', g_batch);
    struct ibv_mr *mr = rdma_reg_msgs(id, buf, g_batch);
    if (!mr) { perror("rdma_reg_msgs"); return -1; }

    /* 注册接收 buffer（接收 server 的 MR 信息） */
    struct mem_info remote_info;
    struct ibv_mr *info_mr = rdma_reg_msgs(id, &remote_info, sizeof(remote_info));
    if (!info_mr) { perror("rdma_reg_msgs(info)"); return -1; }

    /* 预投递 recv */
    rdma_post_recv(id, NULL, &remote_info, sizeof(remote_info), info_mr);

    if (rdma_connect(id, NULL)) { perror("rdma_connect"); return -1; }
    if (poll_cq(id->recv_cq) < 0) return -1;  /* 等待 server 的 MR 信息 */

    printf("[client] server MR: addr=0x%lx  rkey=0x%x\n",
           (unsigned long)remote_info.addr, remote_info.rkey);
    printf("[client] writing %zu MB in %zu KB chunks...\n",
           g_total / (1024*1024), g_batch / 1024);

    /* RDMA_WRITE 流水线 */
    int num_chunks = (int)((g_total + g_batch - 1) / g_batch);
    int unpolled   = 0;
    long long t0   = now_us();

    for (int i = 0; i < num_chunks; i++) {
        size_t cur_len = (i == num_chunks - 1)
                         ? (g_total - (size_t)i * g_batch) : g_batch;
        size_t remote_off = (size_t)i * g_batch;

        struct ibv_sge sge = {
            .addr   = (uintptr_t)buf,
            .length = (uint32_t)cur_len,
            .lkey   = mr->lkey
        };
        struct ibv_send_wr wr = {0}, *bad_wr;

        wr.wr_id      = i;
        wr.sg_list    = &sge;
        wr.num_sge    = 1;
        wr.send_flags = IBV_SEND_SIGNALED;

        if (i == num_chunks - 1) {
            wr.opcode   = IBV_WR_RDMA_WRITE_WITH_IMM;
            wr.imm_data = htonl(0x8888);
        } else {
            wr.opcode = IBV_WR_RDMA_WRITE;
        }

        wr.wr.rdma.remote_addr = remote_info.addr + remote_off;
        wr.wr.rdma.rkey        = remote_info.rkey;

        ibv_post_send(id->qp, &wr, &bad_wr);
        unpolled++;

        /* 滑动窗口: 满了或最后一轮时回收 CQE */
        if (unpolled == QP_DEPTH || i == num_chunks - 1) {
            for (int j = 0; j < unpolled; j++) {
                if (poll_cq(id->send_cq) < 0) return -1;
            }
            unpolled = 0;
        }
    }

    long long t1 = now_us();
    double ms = (t1 - t0) / 1000.0;
    double mbps = (ms > 0) ? (g_total / (ms / 1000.0) / (1024.0 * 1024.0)) : 0;
    printf("[client] write: %zu bytes in %.1f ms  ->  %.2f MB/s\n",
           g_total, ms, mbps);

    /* cleanup */
    rdma_dereg_mr(info_mr);
    rdma_dereg_mr(mr);
    free(buf);
    rdma_disconnect(id);
    rdma_destroy_ep(id);
    return 0;
}

/* ── main ──────────────────────────────────────────────────────────── */

static void usage(void) {
    fprintf(stderr,
        "Usage: rdma_bw --server|--client IP [options]\n"
        "  --server               run as RDMA target\n"
        "  --client <IP>          run as RDMA initiator (writer)\n"
        "  --port    <N>          port number      (default: 20886)\n"
        "  --size    <N>[K|M|G]  total bytes       (default: 100M)\n"
        "  --batch   <N>[K|M]    per-write chunk   (default: 1M)\n");
    exit(1);
}

static size_t parse_size(const char *s) {
    char *end;
    size_t v = (size_t)strtoull(s, &end, 10);
    if (*end == 'G' || *end == 'g') v *= 1024*1024*1024;
    else if (*end == 'M' || *end == 'm') v *= 1024*1024;
    else if (*end == 'K' || *end == 'k') v *= 1024;
    return v;
}

int main(int argc, char **argv) {
    static struct option opts[] = {
        {"server",   no_argument,       &g_server, 1},
        {"client",   required_argument, NULL, 'c'},
        {"port",     required_argument, NULL, 'P'},
        {"size",     required_argument, NULL, 's'},
        {"batch",    required_argument, NULL, 'b'},
        {"help",     no_argument,       NULL, 'h'},
        {0,0,0,0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "c:P:s:b:h", opts, NULL)) != -1) {
        switch (opt) {
        case 0: break;
        case 'c': g_node  = optarg; g_server = 0; break;
        case 'P': g_port     = optarg; break;
        case 's': g_total    = parse_size(optarg); break;
        case 'b': g_batch    = parse_size(optarg); break;
        default:  usage();
        }
    }

    if (g_total < g_batch) g_batch = g_total;

    printf("=== RDMA BW (ibverbs + rxe): size=%zu MB  batch=%zu KB  port=%s ===\n",
           g_total/(1024*1024), g_batch/1024, g_port);

    return g_server ? run_server() : run_client();
}
