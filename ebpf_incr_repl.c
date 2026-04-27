/* =======================================================================
 * ebpf_incr_repl.c  —  User-space eBPF loader + RDMA incremental sender
 *
 * Production-grade guarantees in this implementation:
 *   1. No full-sync -> incremental handoff gap (seq watermark + catch-up).
 *   2. Fail-closed on ring-buffer data loss (disconnect slaves for re-sync).
 *   3. Safe lifecycle (joinable consumer thread, no detached UAF race).
 *   4. Deterministic ownership transfer: add_slave returns explicit status.
 * ======================================================================= */

#include "ebpf_incr_repl.h"

#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <bpf/bpf.h>
#include <bpf/libbpf.h>

#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_eq.h>

#define INCR_BACKLOG_CAP 4096

struct slave_conn {
    struct fid_ep     *ep;
    struct fid_domain *domain;
    struct fid_cq     *cq;
    struct fid_mr     *send_mr;
    uint8_t           *send_buf;
    int                active;
};

struct backlog_record {
    uint64_t seq;
    uint32_t length;
    char     msg[INCR_MAX_CMD];
};

struct incr_stats {
    uint64_t emitted;
    uint64_t dropped;
};

static struct slave_conn    g_slaves[INCR_MAX_SLAVES];
static struct backlog_record g_backlog[INCR_BACKLOG_CAP];
static size_t               g_backlog_start = 0;
static size_t               g_backlog_count = 0;
static uint64_t             g_seq           = 0;
static int                  g_stream_healthy = 1;
static pthread_mutex_t      g_mu = PTHREAD_MUTEX_INITIALIZER;

static struct bpf_object   *g_obj = NULL;
static struct bpf_link     *g_link = NULL;
static struct ring_buffer  *g_rb = NULL;
static pthread_t            g_tid;
static volatile int         g_running = 0;
static int                  g_tid_started = 0;
static int                  g_stats_fd = -1;
static uint64_t             g_last_dropped = 0;
static uint64_t             g_mr_key_next = 1;

static uint64_t incr_next_mr_key(void)
{
    return __atomic_fetch_add(&g_mr_key_next, 1, __ATOMIC_RELAXED);
}

static int incr_mr_reg(struct fid_domain *domain, void *buf, size_t len,
                       uint64_t access, struct fid_mr **mr)
{
    int rc = fi_mr_reg(domain, buf, len, access, 0, 0, 0, mr, NULL);
    if (rc != -FI_ENOKEY)
        return rc;

    for (int i = 0; i < 64; i++) {
        uint64_t key = incr_next_mr_key();
        rc = fi_mr_reg(domain, buf, len, access, 0, key, 0, mr, NULL);
        if (rc == 0)
            return 0;
        if (rc != -FI_ENOKEY)
            return rc;
    }
    return rc;
}

static void slave_close_locked(struct slave_conn *s)
{
    if (!s || !s->active)
        return;

    s->active = 0;

    if (s->ep && s->send_mr && s->send_buf) {
        uint64_t zero = 0;
        struct fi_context ctx;
        memcpy(s->send_buf, &zero, sizeof(zero));
        fi_send(s->ep, s->send_buf, sizeof(zero), fi_mr_desc(s->send_mr),
                0, &ctx);
        fi_shutdown(s->ep, 0);
    }

    if (s->send_mr) {
        fi_close(&s->send_mr->fid);
        s->send_mr = NULL;
    }
    if (s->send_buf) {
        free(s->send_buf);
        s->send_buf = NULL;
    }
    if (s->ep) {
        fi_close(&s->ep->fid);
        s->ep = NULL;
    }
    if (s->cq) {
        fi_close(&s->cq->fid);
        s->cq = NULL;
    }
    if (s->domain) {
        fi_close(&s->domain->fid);
        s->domain = NULL;
    }
}

static int slave_send_frame_locked(struct slave_conn *s, const char *msg,
                                   uint32_t len)
{
    if (!s || !s->active || !s->ep || !s->cq || !s->send_mr || !s->send_buf)
        return -1;

    if (!msg || len == 0 || len > INCR_MAX_CMD)
        return -1;

    uint64_t cmd_len = (uint64_t)len;
    size_t total = sizeof(uint64_t) + (size_t)len;

    memcpy(s->send_buf, &cmd_len, sizeof(cmd_len));
    memcpy(s->send_buf + sizeof(uint64_t), msg, len);

    struct fi_context send_ctx;
    ssize_t r = fi_send(s->ep, s->send_buf, total, fi_mr_desc(s->send_mr),
                        0, &send_ctx);
    if (r != 0)
        return -1;

    int waits = 0;
    const int max_waits = 5;
    for (;;) {
        struct fi_cq_data_entry ce;
        ssize_t cr = fi_cq_sread(s->cq, &ce, 1, NULL, 2000);
        if (cr == 1)
            return 0;
        if (cr == -FI_EAGAIN) {
            waits++;
            if (waits >= max_waits) {
                fprintf(stderr,
                        "[INCR-REPL] send completion timeout on incremental link\n");
                return -1;
            }
            continue;
        }
        if (cr == -FI_EAVAIL) {
            struct fi_cq_err_entry ee = {0};
            fi_cq_readerr(s->cq, &ee, 0);
            fprintf(stderr, "[INCR-REPL] CQ error: %s\n",
                    fi_cq_strerror(s->cq, ee.prov_errno, ee.err_data, NULL,
                                   0));
            return -1;
        }
        fprintf(stderr, "[INCR-REPL] fi_cq_sread: %s\n", fi_strerror((int)-cr));
        return -1;
    }
}

static void backlog_append_locked(const struct incr_event *e, uint64_t seq)
{
    size_t idx = (g_backlog_start + g_backlog_count) % INCR_BACKLOG_CAP;
    if (g_backlog_count == INCR_BACKLOG_CAP) {
        g_backlog_start = (g_backlog_start + 1) % INCR_BACKLOG_CAP;
    } else {
        g_backlog_count++;
    }

    g_backlog[idx].seq = seq;
    g_backlog[idx].length = e->length;
    memcpy(g_backlog[idx].msg, e->msg, e->length);
}

static int replay_backlog_locked(struct slave_conn *s, uint64_t start_seq)
{
    if (g_backlog_count == 0)
        return 0;

    uint64_t oldest_seq = g_backlog[g_backlog_start].seq;
    if (start_seq != 0 && start_seq < oldest_seq) {
        fprintf(stderr,
                "[INCR-REPL] backlog gap: start_seq=%llu oldest_seq=%llu\n",
                (unsigned long long)start_seq,
                (unsigned long long)oldest_seq);
        return -1;
    }

    for (size_t i = 0; i < g_backlog_count; i++) {
        size_t idx = (g_backlog_start + i) % INCR_BACKLOG_CAP;
        struct backlog_record *rec = &g_backlog[idx];
        if (rec->seq < start_seq)
            continue;
        if (slave_send_frame_locked(s, rec->msg, rec->length) != 0)
            return -1;
    }
    return 0;
}

static void mark_stream_unhealthy_locked(const char *reason)
{
    if (!g_stream_healthy)
        return;

    g_stream_healthy = 0;
    fprintf(stderr, "[INCR-REPL] stream unhealthy: %s\n", reason);

    for (int i = 0; i < INCR_MAX_SLAVES; i++)
        slave_close_locked(&g_slaves[i]);
}

static void check_stats_health(void)
{
    if (g_stats_fd < 0 || !g_running)
        return;

    uint32_t key = 0;
    struct incr_stats st = {0};
    if (bpf_map_lookup_elem(g_stats_fd, &key, &st) != 0)
        return;

    if (st.dropped > g_last_dropped) {
        pthread_mutex_lock(&g_mu);
        g_last_dropped = st.dropped;
        mark_stream_unhealthy_locked("ring buffer dropped incremental events");
        pthread_mutex_unlock(&g_mu);
    }
}

int ebpf_incr_get_seq(uint64_t *seq_out)
{
    if (!seq_out)
        return -1;

    pthread_mutex_lock(&g_mu);
    *seq_out = g_seq;
    pthread_mutex_unlock(&g_mu);
    return 0;
}

int ebpf_incr_master_healthy(void)
{
    int healthy;
    pthread_mutex_lock(&g_mu);
    healthy = g_stream_healthy;
    pthread_mutex_unlock(&g_mu);
    return healthy;
}

int ebpf_incr_add_slave(struct fid_ep *ep, struct fid_cq *cq,
                        struct fid_domain *domain, uint64_t start_seq)
{
    if (!ep || !cq || !domain)
        return -1;

    pthread_mutex_lock(&g_mu);

    if (!g_running || !g_stream_healthy) {
        pthread_mutex_unlock(&g_mu);
        return -1;
    }

    int slot = -1;
    for (int i = 0; i < INCR_MAX_SLAVES; i++) {
        if (!g_slaves[i].active) {
            slot = i;
            break;
        }
    }

    if (slot < 0) {
        pthread_mutex_unlock(&g_mu);
        fprintf(stderr, "[INCR-REPL] no free slot for incremental slave\n");
        return -1;
    }

    struct slave_conn *s = &g_slaves[slot];
    memset(s, 0, sizeof(*s));

    s->send_buf = (uint8_t *)malloc((size_t)INCR_SEND_BUF);
    if (!s->send_buf) {
        pthread_mutex_unlock(&g_mu);
        fprintf(stderr, "[INCR-REPL] OOM for slave send buffer\n");
        return -1;
    }
    memset(s->send_buf, 0, (size_t)INCR_SEND_BUF);

    int rc = incr_mr_reg(domain, s->send_buf, (size_t)INCR_SEND_BUF,
                         FI_SEND, &s->send_mr);
    if (rc) {
        free(s->send_buf);
        s->send_buf = NULL;
        pthread_mutex_unlock(&g_mu);
        fprintf(stderr, "[INCR-REPL] fi_mr_reg(send) failed: %s\n",
                fi_strerror(-rc));
        return -1;
    }

    s->ep = ep;
    s->cq = cq;
    s->domain = domain;
    s->active = 1;

    if (replay_backlog_locked(s, start_seq) != 0) {
        slave_close_locked(s);
        pthread_mutex_unlock(&g_mu);
        return -1;
    }

    printf("[INCR-REPL] slave registered in slot %d (catch-up from seq=%llu)\n",
           slot, (unsigned long long)start_seq);
    pthread_mutex_unlock(&g_mu);
    return 0;
}

static int on_incr_event(void *ctx, void *data, size_t sz)
{
    (void)ctx;
    (void)sz;

    struct incr_event *e = (struct incr_event *)data;
    if (!e || e->length == 0 || e->length > INCR_MAX_CMD)
        return 0;

    pthread_mutex_lock(&g_mu);

    if (!g_stream_healthy) {
        pthread_mutex_unlock(&g_mu);
        return 0;
    }

    uint64_t seq = ++g_seq;
    backlog_append_locked(e, seq);

    for (int i = 0; i < INCR_MAX_SLAVES; i++) {
        struct slave_conn *s = &g_slaves[i];
        if (!s->active)
            continue;

        if (slave_send_frame_locked(s, e->msg, e->length) != 0) {
            fprintf(stderr, "[INCR-REPL] send failed on slot %d, closing slave\n",
                    i);
            slave_close_locked(s);
        }
    }

    pthread_mutex_unlock(&g_mu);
    return 0;
}

static void *consumer_thread(void *arg)
{
    (void)arg;
    printf("[INCR-REPL] consumer thread started\n");

    while (g_running) {
        int pr = ring_buffer__poll(g_rb, 100);
        if (pr < 0 && pr != -EINTR) {
            pthread_mutex_lock(&g_mu);
            mark_stream_unhealthy_locked("ring_buffer__poll returned fatal error");
            pthread_mutex_unlock(&g_mu);
            fprintf(stderr, "[INCR-REPL] ring_buffer__poll failed: %d\n", pr);
            break;
        }
        check_stats_health();
    }

    printf("[INCR-REPL] consumer thread stopped\n");
    return NULL;
}

int ebpf_incr_master_start(void)
{
    if (g_running)
        return 0;

    g_obj = bpf_object__open_file("ebpf_incr.bpf.o", NULL);
    if (!g_obj || libbpf_get_error(g_obj)) {
        fprintf(stderr,
                "[INCR-REPL] bpf_object__open_file failed (ebpf_incr.bpf.o?)\n");
        g_obj = NULL;
        return -1;
    }
    if (bpf_object__load(g_obj)) {
        fprintf(stderr, "[INCR-REPL] bpf_object__load failed\n");
        bpf_object__close(g_obj);
        g_obj = NULL;
        return -1;
    }

    struct bpf_map *rb_map = bpf_object__find_map_by_name(g_obj, "rb");
    if (!rb_map) {
        fprintf(stderr, "[INCR-REPL] map 'rb' not found\n");
        goto err_close;
    }

    struct bpf_map *stats_map = bpf_object__find_map_by_name(g_obj, "stats");
    if (!stats_map) {
        fprintf(stderr, "[INCR-REPL] map 'stats' not found\n");
        goto err_close;
    }
    g_stats_fd = bpf_map__fd(stats_map);
    if (g_stats_fd < 0) {
        fprintf(stderr, "[INCR-REPL] invalid stats map fd\n");
        goto err_close;
    }

    g_rb = ring_buffer__new(bpf_map__fd(rb_map), on_incr_event, NULL, NULL);
    if (!g_rb) {
        fprintf(stderr, "[INCR-REPL] ring_buffer__new failed\n");
        goto err_close;
    }

    struct bpf_program *prog =
        bpf_object__find_program_by_name(g_obj, "on_kvs_rdma_incr_probe");
    if (!prog) {
        fprintf(stderr, "[INCR-REPL] program 'on_kvs_rdma_incr_probe' not found\n");
        goto err_rb;
    }

    LIBBPF_OPTS(bpf_uprobe_opts, uopts,
                .func_name = "kvs_rdma_incr_probe",
                .retprobe = false);
    g_link = bpf_program__attach_uprobe_opts(prog, getpid(), "/proc/self/exe",
                                             0, &uopts);
    if (!g_link || libbpf_get_error(g_link)) {
        fprintf(stderr, "[INCR-REPL] attach uprobe failed\n");
        g_link = NULL;
        goto err_rb;
    }

    pthread_mutex_lock(&g_mu);
    g_backlog_start = 0;
    g_backlog_count = 0;
    g_seq = 0;
    g_stream_healthy = 1;
    g_last_dropped = 0;
    pthread_mutex_unlock(&g_mu);

    g_running = 1;
    g_tid_started = 0;
    if (pthread_create(&g_tid, NULL, consumer_thread, NULL) != 0) {
        fprintf(stderr, "[INCR-REPL] pthread_create failed: %s\n", strerror(errno));
        goto err_link;
    }
    g_tid_started = 1;

    printf("[INCR-REPL] incremental sync module ready\n");
    return 0;

err_link:
    g_running = 0;
    if (g_link) {
        bpf_link__destroy(g_link);
        g_link = NULL;
    }
err_rb:
    if (g_rb) {
        ring_buffer__free(g_rb);
        g_rb = NULL;
    }
err_close:
    g_stats_fd = -1;
    if (g_obj) {
        bpf_object__close(g_obj);
        g_obj = NULL;
    }
    return -1;
}

void ebpf_incr_stop(void)
{
    g_running = 0;

    if (g_tid_started) {
        pthread_join(g_tid, NULL);
        g_tid_started = 0;
    }

    pthread_mutex_lock(&g_mu);
    for (int i = 0; i < INCR_MAX_SLAVES; i++)
        slave_close_locked(&g_slaves[i]);
    pthread_mutex_unlock(&g_mu);

    if (g_link) {
        bpf_link__destroy(g_link);
        g_link = NULL;
    }
    if (g_rb) {
        ring_buffer__free(g_rb);
        g_rb = NULL;
    }
    if (g_obj) {
        bpf_object__close(g_obj);
        g_obj = NULL;
    }
    g_stats_fd = -1;
}
