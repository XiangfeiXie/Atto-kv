/*
 * libfabric_bw.c — libfabric RMA 单向带宽基准 (RDM, 无连接)
 *
 *   server: 注册 MR，导出地址+Rkey → 被动接收 fi_write，统计吞吐
 *   client: 导入 server MR → 批量 fi_write → 统计吞吐
 *
 * 用法:
 *   # 需先设置 rxd 底层 provider (verbs / udp)
 *   export FI_RXD_CORE_PROVIDER=verbs
 *   ./libfabric_bw --server --port 2101 --size 100M &
 *   ./libfabric_bw --client 127.0.0.1 --port 2101 --size 100M
 *
 * 对比:
 *   for sz in 1M 10M 50M 100M 500M 1G; do
 *     ./libfabric_bw --server --size $sz &
 *     sleep 1
 *     ./libfabric_bw --client 127.0.0.1 --size $sz
 *     wait
 *   done
 */

#define _GNU_SOURCE
#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <getopt.h>

#define BATCH_SIZE    (1024 * 1024)   /* 1MB per fi_write */
#define ADDR_FILE     "/tmp/libfabric_bw_addr"
#define ADDR_READY    "/tmp/libfabric_bw_ready"

static const char *g_provider = "verbs;ofi_rxm";
static const char *g_node     = "127.0.0.1";
static const char *g_port     = "2101";
static int         g_server   = 0;
static size_t      g_total    = 100 * 1024 * 1024;  /* default 100MB */
static size_t      g_batch    = BATCH_SIZE;

/* ── helpers ───────────────────────────────────────────────────────── */

static void die(const char *msg, int ret) {
    fprintf(stderr, "FATAL: %s: %s (%d)\n", msg, fi_strerror(-ret), ret);
    exit(1);
}

static long long now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

static int wait_cq(struct fid_cq *cq) {
    for (;;) {
        struct fi_cq_data_entry e;
        ssize_t r = fi_cq_read(cq, &e, 1);
        if (r > 0) return 0;
        if (r < 0 && r != -FI_EAGAIN) {
            struct fi_cq_err_entry err;
            fi_cq_readerr(cq, &err, 0);
            fprintf(stderr, "CQ error: %s (%zd)\n", fi_strerror((int)(-r)), r);
            return -1;
        }
        usleep(100);
    }
}

/* ── server ────────────────────────────────────────────────────────── */

static int run_server(void) {
    struct fi_info *hints = fi_allocinfo();
    hints->caps                   = FI_RMA;
    hints->ep_attr->type          = FI_EP_RDM;
    hints->mode                   = 0;
    hints->domain_attr->mr_mode   = FI_MR_LOCAL | FI_MR_ALLOCATED;
    /* 不设置 fabric_attr->prov_name，让 libfabric 自动选择可用 provider */

    struct fi_info *info = NULL;
    int rc = fi_getinfo(FI_VERSION(1, 17), NULL, NULL, 0, hints, &info);
    fi_freeinfo(hints);
    if (rc) { fprintf(stderr, "fi_getinfo: %s\n", fi_strerror(-rc)); die("fi_getinfo", rc); }
    printf("[server] using provider: %s\n", info->fabric_attr->prov_name);

    struct fid_fabric *fabric = NULL;
    struct fid_domain *dom    = NULL;
    struct fid_ep     *ep     = NULL;
    struct fid_cq     *cq     = NULL;
    struct fid_av     *av     = NULL;
    int ret;

    if ((ret = fi_fabric(info->fabric_attr, &fabric, NULL)))  die("fi_fabric", ret);
    if ((ret = fi_domain(fabric, info, &dom, NULL)))           die("fi_domain", ret);
    if ((ret = fi_cq_open(dom, &(struct fi_cq_attr){
            .format = FI_CQ_FORMAT_DATA, .wait_obj = FI_WAIT_UNSPEC
        }, &cq, NULL)))                                         die("fi_cq_open", ret);
    if ((ret = fi_av_open(dom, &(struct fi_av_attr){
            .type = FI_AV_MAP, .count = 1
        }, &av, NULL)))                                         die("fi_av_open", ret);
    if ((ret = fi_endpoint(dom, info, &ep, NULL)))             die("fi_endpoint", ret);
    if ((ret = fi_ep_bind(ep, &cq->fid, FI_SEND | FI_RECV)))   die("fi_ep_bind(cq)", ret);
    if ((ret = fi_ep_bind(ep, &av->fid, 0)))                   die("fi_ep_bind(av)", ret);
    if ((ret = fi_enable(ep)))                                  die("fi_enable", ret);

    /* 获取本地地址 */
    size_t addrlen = 0;
    if ((ret = fi_getname(&ep->fid, NULL, &addrlen)) != -FI_ETOOSMALL)
        die("fi_getname(size)", ret);
    void *addr = malloc(addrlen);
    if ((ret = fi_getname(&ep->fid, addr, &addrlen)))
        die("fi_getname", ret);

    /* 注册目标 MR */
    char *buf = malloc(g_total);
    memset(buf, 0, g_total);
    struct fid_mr *mr = NULL;
    if ((ret = fi_mr_reg(dom, buf, g_total, FI_REMOTE_WRITE, 0, 0, 0, &mr, NULL))) {
        fprintf(stderr, "fi_mr_reg: %s\n", fi_strerror(-ret));
        die("fi_mr_reg", ret);
    }
    uint64_t mr_key = fi_mr_key(mr);

    printf("[server] MR: buf=%p  len=%zu MB  key=%lu\n",
           (void *)buf, g_total / (1024*1024), (unsigned long)mr_key);

    /* 写入地址文件供 client 读取 */
    FILE *f = fopen(ADDR_FILE, "wb");
    if (!f) die("fopen ADDR_FILE", -1);
    fwrite(&addrlen,  sizeof(size_t),   1, f);
    fwrite(addr,      1,                addrlen, f);
    fwrite(&mr_key,   sizeof(uint64_t), 1, f);
    fwrite(&g_total,  sizeof(size_t),   1, f);
    fclose(f);

    printf("[server] addrlen=%zu  addr=", addrlen);
    for (size_t i = 0; i < addrlen && i < 32; i++)
        printf("%02x ", ((unsigned char*)addr)[i]);
    printf("\n");

    /* 创建 ready 标志文件 */
    f = fopen(ADDR_READY, "w");
    if (f) fclose(f);

    printf("[server] listening on port %s, waiting for RMA writes...\n", g_port);

    /* 等待远程写入，统计吞吐 */
    size_t total = 0;
    long long t0 = 0;
    int first = 1;

    while (total < g_total) {
        struct fi_cq_data_entry ce;
        ssize_t r = fi_cq_read(cq, &ce, 1);
        if (r > 0) {
            if (first) { t0 = now_us(); first = 0; }
            total += (size_t)ce.len;
        } else if (r == -FI_EAGAIN) {
            usleep(100);
        } else {
            struct fi_cq_err_entry err;
            fi_cq_readerr(cq, &err, 0);
            fprintf(stderr, "[server] CQ error: %d (%s)\n",
                    err.err, fi_strerror((int)(-r)));
            break;
        }
    }

    long long t1 = now_us();
    double ms = (t1 - t0) / 1000.0;
    double mbps = (ms > 0) ? (total / (ms / 1000.0) / (1024.0 * 1024.0)) : 0;
    printf("[server] recv: %zu bytes / %.1f ms  ->  %.2f MB/s\n",
           total, ms, mbps);

    /* cleanup */
    fi_close(&mr->fid);
    free(buf);
    free(addr);
    fi_close(&ep->fid);
    fi_close(&av->fid);
    fi_close(&cq->fid);
    fi_close(&dom->fid);
    fi_close(&fabric->fid);
    fi_freeinfo(info);
    remove(ADDR_FILE);
    remove(ADDR_READY);
    return 0;
}

/* ── client ────────────────────────────────────────────────────────── */

static int run_client(void) {
    struct fi_info *hints = fi_allocinfo();
    hints->caps                   = FI_RMA;
    hints->ep_attr->type          = FI_EP_RDM;
    hints->mode                   = 0;
    hints->domain_attr->mr_mode   = FI_MR_LOCAL | FI_MR_ALLOCATED;
    /* 不设置 fabric_attr->prov_name，让 libfabric 自动选择可用 provider */

    struct fi_info *info = NULL;
    int rc = fi_getinfo(FI_VERSION(1, 17), NULL, NULL, 0, hints, &info);
    fi_freeinfo(hints);
    if (rc) { fprintf(stderr, "fi_getinfo: %s\n", fi_strerror(-rc)); die("fi_getinfo", rc); }
    printf("[client] using provider: %s\n", info->fabric_attr->prov_name);

    struct fid_fabric *fabric = NULL;
    struct fid_domain *dom    = NULL;
    struct fid_ep     *ep     = NULL;
    struct fid_cq     *cq     = NULL;
    struct fid_av     *av     = NULL;
    int ret;

    if ((ret = fi_fabric(info->fabric_attr, &fabric, NULL)))  die("fi_fabric", ret);
    if ((ret = fi_domain(fabric, info, &dom, NULL)))           die("fi_domain", ret);
    if ((ret = fi_cq_open(dom, &(struct fi_cq_attr){
            .format = FI_CQ_FORMAT_DATA, .wait_obj = FI_WAIT_UNSPEC
        }, &cq, NULL)))                                         die("fi_cq_open", ret);
    if ((ret = fi_av_open(dom, &(struct fi_av_attr){
            .type = FI_AV_MAP, .count = 1
        }, &av, NULL)))                                         die("fi_av_open", ret);
    if ((ret = fi_endpoint(dom, info, &ep, NULL)))             die("fi_endpoint", ret);
    if ((ret = fi_ep_bind(ep, &cq->fid, FI_SEND | FI_RECV)))   die("fi_ep_bind(cq)", ret);
    if ((ret = fi_ep_bind(ep, &av->fid, 0)))                   die("fi_ep_bind(av)", ret);
    if ((ret = fi_enable(ep)))                                  die("fi_enable", ret);

    /* 打印 client 本地地址 (debug) */
    {
        size_t mylen = 0;
        fi_getname(&ep->fid, NULL, &mylen);
        void *myaddr = malloc(mylen);
        fi_getname(&ep->fid, myaddr, &mylen);
        printf("[client] local addrlen=%zu  addr=", mylen);
        for (size_t i = 0; i < mylen && i < 32; i++)
            printf("%02x ", ((unsigned char*)myaddr)[i]);
        printf("\n");
        free(myaddr);
    }

    /* 等待 server 准备好 */
    printf("[client] waiting for server...\n");
    for (int i = 0; i < 300; i++) {
        if (access(ADDR_READY, F_OK) == 0) break;
        usleep(100000);
    }
    if (access(ADDR_READY, F_OK) != 0) die("timeout waiting for server", -1);

    /* 读取 server 地址和 MR 信息 */
    FILE *f = fopen(ADDR_FILE, "rb");
    if (!f) die("fopen ADDR_FILE", -1);
    size_t addrlen = 0;
    fread(&addrlen, sizeof(size_t), 1, f);
    void *server_addr = malloc(addrlen);
    fread(server_addr, 1, addrlen, f);
    uint64_t remote_key = 0;
    size_t   remote_len = 0;
    fread(&remote_key, sizeof(uint64_t), 1, f);
    fread(&remote_len, sizeof(size_t), 1, f);
    fclose(f);

    printf("[client] server MR: len=%zu MB  key=%lu\n",
           remote_len / (1024*1024), (unsigned long)remote_key);
    printf("[client] addrlen=%zu  addr=", addrlen);
    for (size_t i = 0; i < addrlen && i < 32; i++)
        printf("%02x ", ((unsigned char*)server_addr)[i]);
    printf("\n");

    /* 将 server 地址插入地址向量 */
    fi_addr_t peer_fiaddr = FI_ADDR_UNSPEC;
    if ((ret = fi_av_insert(av, server_addr, 1, &peer_fiaddr, 0, NULL)) != 1)
        die("fi_av_insert", ret);
    printf("[client] fi_av_insert: peer_fiaddr=%llu\n",
           (unsigned long long)peer_fiaddr);
    free(server_addr);

    /* 注册本地 buffer */
    char *buf = malloc(g_batch);
    memset(buf, 'A', g_batch);
    struct fid_mr *local_mr = NULL;
    if ((ret = fi_mr_reg(dom, buf, g_batch, FI_WRITE, 0, 0, 0, &local_mr, NULL))) {
        fprintf(stderr, "fi_mr_reg(local): %s\n", fi_strerror(-ret));
        die("fi_mr_reg(local)", ret);
    }
    void *local_desc = fi_mr_desc(local_mr);

    printf("[client] writing %zu MB in %zu KB chunks...\n",
           g_total / (1024*1024), g_batch / 1024);

    /* fi_write 循环 */
    size_t sent = 0;
    long long t0 = now_us();

    while (sent < g_total) {
        size_t chunk = g_batch;
        if (sent + chunk > g_total) chunk = g_total - sent;
        for (int retry = 0; retry < 100; retry++) {
            ret = fi_write(ep, buf, chunk, local_desc,
                           peer_fiaddr, sent, remote_key, NULL);
            if (ret == 0) break;
            if (ret != -FI_EAGAIN) {
                fprintf(stderr, "fi_write: %s (%d)\n", fi_strerror(-ret), ret);
                die("fi_write", ret);
            }
            if (retry == 0) printf("[client] fi_write EAGAIN, retrying...\n");
            usleep(10000);
        }
        if (ret) die("fi_write EAGAIN exhausted", ret);
        if (wait_cq(cq) < 0) die("write CQ", -1);
        sent += chunk;
    }

    long long t1 = now_us();
    double ms = (t1 - t0) / 1000.0;
    double mbps = (ms > 0) ? (sent / (ms / 1000.0) / (1024.0 * 1024.0)) : 0;
    printf("[client] write: %zu bytes in %.1f ms  ->  %.2f MB/s\n",
           sent, ms, mbps);

    /* cleanup */
    fi_close(&local_mr->fid);
    free(buf);
    fi_close(&ep->fid);
    fi_close(&av->fid);
    fi_close(&cq->fid);
    fi_close(&dom->fid);
    fi_close(&fabric->fid);
    fi_freeinfo(info);
    return 0;
}

/* ── main ──────────────────────────────────────────────────────────── */

static void usage(void) {
    fprintf(stderr,
        "Usage: libfabric_bw --server|--client IP [options]\n"
        "  --server               run as RMA target (receiver)\n"
        "  --client <IP>          run as RMA initiator (writer)\n"
        "  --port    <N>          port number      (default: 2101)\n"
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

    printf("=== libfabric RMA BW: provider=%s  ep=RDM  size=%zu MB  batch=%zu KB ===\n",
           g_provider, g_total/(1024*1024), g_batch/1024);

    return g_server ? run_server() : run_client();
}
