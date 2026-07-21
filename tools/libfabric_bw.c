/*
 * libfabric_bw.c — libfabric 单向传输带宽基准
 *
 * 模拟 kvstore 全量同步的数据平面，但不做序列化/解析：
 *   server: passive EP → accept → 接收数据
 *   client: connect → 批量 fi_send → shutdown
 *
 * 用法:
 *   ./libfabric_bw --server --provider tcp   --port 2101
 *   ./libfabric_bw --client 127.0.0.1 --provider tcp --port 2101 --size 100M
 *
 * 对比:
 *   for sz in 1M 10M 50M 100M 500M 1G; do
 *     for prov in tcp verbs; do
 *       ./libfabric_bw --server --provider $prov &
 *       sleep 1
 *       ./libfabric_bw --client 127.0.0.1 --provider $prov --size $sz
 *       wait
 *     done
 *   done
 */

#define _GNU_SOURCE
#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <getopt.h>

#define BATCH_SIZE    (1024 * 1024)   /* 1MB per fi_send */
#define NRECV_BUFS    8

static const char *g_provider = "tcp";
static const char *g_node     = "127.0.0.1";
static const char *g_port     = "2101";
static int         g_server   = 0;
static size_t      g_total    = 100 * 1024 * 1024;  /* default 100MB */
static size_t      g_batch    = BATCH_SIZE;

/* ── helpers ───────────────────────────────────────────────────────── */

static void die(const char *msg) { fprintf(stderr, "FATAL: %s\n", msg); exit(1); }

static long long now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

static int is_verbs(void) { return strcmp(g_provider, "verbs") == 0; }

static int wait_cq_or_die(struct fid_cq *cq) {
    for (;;) {
        struct fi_cq_data_entry e;
        ssize_t r = fi_cq_read(cq, &e, 1);
        if (r > 0) return (int)e.len;
        if (r < 0 && r != -FI_EAGAIN) {
            struct fi_cq_err_entry err;
            fi_cq_readerr(cq, &err, 0);
            fprintf(stderr, "CQ error: %s (%d)\n", fi_strerror((int)(-r)), (int)(-r));
            return -1;
        }
        usleep(100);
    }
}

static void *reg_mr_if_needed(struct fid_domain *dom, void *buf, size_t len,
                              uint64_t access, struct fid_mr **out_mr) {
    *out_mr = NULL;
    if (!is_verbs()) return NULL;  /* tcp: no MR needed */
    int ret = fi_mr_reg(dom, buf, len, access, 0, 0, 0, out_mr, NULL);
    if (ret) {
        fprintf(stderr, "fi_mr_reg: %s\n", fi_strerror(-ret));
        exit(1);
    }
    return fi_mr_desc(*out_mr);
}

/* ── server ────────────────────────────────────────────────────────── */

static int run_server(void) {
    struct fi_info *hints = fi_allocinfo();
    hints->caps              = FI_MSG;
    hints->ep_attr->type     = FI_EP_MSG;
    hints->mode              = 0;
    hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED;
    hints->fabric_attr->prov_name = strdup(g_provider);

    struct fi_info *info = NULL;
    int rc = fi_getinfo(FI_VERSION(1, 17), NULL, g_port, FI_SOURCE, hints, &info);
    fi_freeinfo(hints);
    if (rc) die("fi_getinfo");

    struct fid_fabric *fabric = NULL;
    struct fid_eq      *eq     = NULL;
    struct fid_pep     *pep    = NULL;
    int ret;
    if ((ret = fi_fabric(info->fabric_attr, &fabric, NULL)))   die("fi_fabric");
    if ((ret = fi_eq_open(fabric, &(struct fi_eq_attr){
            .wait_obj = FI_WAIT_UNSPEC}, &eq, NULL)))           die("fi_eq_open");
    if ((ret = fi_passive_ep(fabric, info, &pep, NULL)))        die("fi_passive_ep");
    if ((ret = fi_pep_bind(pep, &eq->fid, 0)))                  die("fi_pep_bind");
    if ((ret = fi_listen(pep)))                                  die("fi_listen");

    printf("[server] listening on 0.0.0.0:%s  (provider=%s)\n", g_port, g_provider);

    /* wait for client */
    struct fi_eq_cm_entry entry;
    uint32_t event;
    if (fi_eq_sread(eq, &event, &entry, sizeof(entry), 60000, 0) < 0 ||
        event != FI_CONNREQ) die("no client");

    struct fid_domain *dom = NULL;
    struct fid_ep     *ep  = NULL;
    struct fid_cq     *cq  = NULL;
    if ((ret = fi_domain(fabric, entry.info, &dom, NULL)))        die("fi_domain");
    if ((ret = fi_cq_open(dom, &(struct fi_cq_attr){
            .format = FI_CQ_FORMAT_DATA, .wait_obj = FI_WAIT_UNSPEC
        }, &cq, NULL)))                                            die("fi_cq_open");
    if ((ret = fi_endpoint(dom, entry.info, &ep, NULL)))          die("fi_endpoint");
    if ((ret = fi_ep_bind(ep, &eq->fid, 0)))                      die("fi_ep_bind(eq)");
    if ((ret = fi_ep_bind(ep, &cq->fid, FI_SEND|FI_RECV)))        die("fi_ep_bind(cq)");
    if ((ret = fi_enable(ep)))                                     die("fi_enable");
    if ((ret = fi_accept(ep, NULL, 0)))                            die("fi_accept");

    if (fi_eq_sread(eq, &event, &entry, sizeof(entry), 10000, 0) < 0 ||
        event != FI_CONNECTED) die("FI_CONNECTED");

    printf("[server] client connected\n");

    /* prep recv buffers with MR (verbs) or plain (tcp) */
    char *bufs[NRECV_BUFS];
    struct fid_mr *recv_mr[NRECV_BUFS];
    for (int i = 0; i < NRECV_BUFS; i++) {
        bufs[i] = malloc(g_batch);
        void *desc = reg_mr_if_needed(dom, bufs[i], g_batch, FI_RECV, &recv_mr[i]);
        if ((ret = fi_recv(ep, bufs[i], g_batch, desc, 0, bufs[i])))
            die("fi_recv post");
    }

    /* receive loop — exits on client shutdown, prints throughput */
    size_t total = 0;
    long long t0 = 0;

    while (1) {
        struct fi_cq_data_entry ce;
        ssize_t r = fi_cq_read(cq, &ce, 1);
        if (r > 0) {
            if (!total) t0 = now_us();
            total += (size_t)ce.len;
            /* find matching MR descriptor (verbs) or NULL (tcp) */
            void *desc = NULL;
            if (is_verbs()) {
                for (int i = 0; i < NRECV_BUFS; i++) {
                    if (bufs[i] == ce.op_context) { desc = fi_mr_desc(recv_mr[i]); break; }
                }
            }
            fi_recv(ep, ce.op_context, g_batch, desc, 0, ce.op_context);
        } else if (r == -FI_EAGAIN) {
            usleep(100);
        } else {
            struct fi_cq_err_entry err;
            fi_cq_readerr(cq, &err, 0);
            if (err.err == FI_ECANCELED || err.err == FI_ESHUTDOWN ||
                err.err == FI_ENOTCONN)
                break;
            fprintf(stderr, "[server] CQ error: %d\n", err.err);
            break;
        }
        if (total >= g_total && g_total > 0) break;
    }

    long long t1 = now_us();
    double ms = (t1 - t0) / 1000.0;
    double mbps = (ms > 0) ? (total / (ms / 1000.0) / (1024.0 * 1024.0)) : 0;
    printf("[server] recv: %zu bytes / %.1f ms  →  %.2f MB/s\n",
           total, ms, mbps);

    /* cleanup */
    for (int i = 0; i < NRECV_BUFS; i++) free(bufs[i]);
    fi_close(&ep->fid);
    fi_close(&cq->fid);
    fi_close(&dom->fid);
    fi_close(&pep->fid);
    fi_close(&eq->fid);
    fi_close(&fabric->fid);
    fi_freeinfo(info);
    return 0;
}

/* ── client ────────────────────────────────────────────────────────── */

static int run_client(void) {
    struct fi_info *hints = fi_allocinfo();
    hints->caps              = FI_MSG;
    hints->ep_attr->type     = FI_EP_MSG;
    hints->mode              = 0;
    hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED;
    hints->fabric_attr->prov_name = strdup(g_provider);

    struct fi_info *info = NULL;
    int rc = fi_getinfo(FI_VERSION(1, 17), g_node, g_port, 0, hints, &info);
    fi_freeinfo(hints);
    if (rc) die("fi_getinfo");

    struct fid_fabric *fabric = NULL;
    struct fid_eq      *eq     = NULL;
    struct fid_domain  *dom    = NULL;
    struct fid_ep      *ep     = NULL;
    struct fid_cq      *cq     = NULL;
    int ret;

    if ((ret = fi_fabric(info->fabric_attr, &fabric, NULL)))   die("fi_fabric");
    if ((ret = fi_eq_open(fabric, &(struct fi_eq_attr){
            .wait_obj = FI_WAIT_UNSPEC}, &eq, NULL)))           die("fi_eq_open");
    if ((ret = fi_domain(fabric, info, &dom, NULL)))            die("fi_domain");
    if ((ret = fi_cq_open(dom, &(struct fi_cq_attr){
            .format = FI_CQ_FORMAT_DATA, .wait_obj = FI_WAIT_UNSPEC
        }, &cq, NULL)))                                          die("fi_cq_open");
    if ((ret = fi_endpoint(dom, info, &ep, NULL)))              die("fi_endpoint");
    if ((ret = fi_ep_bind(ep, &eq->fid, 0)))                    die("fi_ep_bind(eq)");
    if ((ret = fi_ep_bind(ep, &cq->fid, FI_SEND|FI_RECV)))      die("fi_ep_bind(cq)");
    if ((ret = fi_enable(ep)))                                   die("fi_enable");
    if ((ret = fi_connect(ep, info->dest_addr, NULL, 0)))       die("fi_connect");

    uint32_t event;
    struct fi_eq_cm_entry entry;
    if (fi_eq_sread(eq, &event, &entry, sizeof(entry), 10000, 0) < 0 ||
        event != FI_CONNECTED) die("FI_CONNECTED");

    printf("[client] connected, sending %zu MB...\n", g_total / (1024*1024));

    /* send data — register buffer for verbs, plain for tcp */
    char *buf = malloc(g_batch);
    memset(buf, 'A', g_batch);

    struct fid_mr *send_mr = NULL;
    void *send_desc = reg_mr_if_needed(dom, buf, g_batch, FI_SEND, &send_mr);

    size_t sent = 0;
    long long t0 = now_us();

    while (sent < g_total) {
        size_t chunk = g_batch;
        if (sent + chunk > g_total) chunk = g_total - sent;
        if ((ret = fi_send(ep, buf, chunk, send_desc, 0, NULL)))
            die("fi_send");
        if (wait_cq_or_die(cq) < 0) die("send CQ");
        sent += chunk;
    }

    fi_shutdown(ep, FI_WRITE);
    long long t1 = now_us();

    double ms = (t1 - t0) / 1000.0;
    double mbps = (ms > 0) ? (sent / (ms / 1000.0) / (1024.0 * 1024.0)) : 0;
    printf("[client] send: %zu bytes in %.1f ms  →  %.2f MB/s\n",
           sent, ms, mbps);

    free(buf);
    fi_close(&ep->fid);
    fi_close(&cq->fid);
    fi_close(&dom->fid);
    fi_close(&eq->fid);
    fi_close(&fabric->fid);
    fi_freeinfo(info);
    return 0;
}

/* ── main ──────────────────────────────────────────────────────────── */

static void usage(void) {
    fprintf(stderr,
        "Usage: libfabric_bw --server|--client IP [options]\n"
        "  --server              run as receiver\n"
        "  --client <IP>         run as sender, connect to IP\n"
        "  --provider tcp|verbs  (default: tcp)\n"
        "  --port    <N>         port number    (default: 2101)\n"
        "  --size    <N>[K|M|G] total bytes to send (default: 100M)\n"
        "  --batch   <N>[K|M]   per-send chunk size  (default: 1M)\n");
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
        {"provider", required_argument, NULL, 'p'},
        {"port",     required_argument, NULL, 'P'},
        {"size",     required_argument, NULL, 's'},
        {"batch",    required_argument, NULL, 'b'},
        {"help",     no_argument,       NULL, 'h'},
        {0,0,0,0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "c:p:P:s:b:h", opts, NULL)) != -1) {
        switch (opt) {
        case 0: break;
        case 'c': g_node  = optarg; g_server = 0; break;
        case 'p': g_provider = optarg; break;
        case 'P': g_port     = optarg; break;
        case 's': g_total    = parse_size(optarg); break;
        case 'b': g_batch    = parse_size(optarg); break;
        default:  usage();
        }
    }

    if (!g_server && !strcmp(g_node, "127.0.0.1") && optind >= argc)
        ; /* default is fine */
    (void)optind;

    printf("=== libfabric BW: provider=%s  size=%zu MB  batch=%zu KB ===\n",
           g_provider, g_total/(1024*1024), g_batch/1024);

    return g_server ? run_server() : run_client();
}
