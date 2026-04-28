/* =======================================================================
 * ebpf_incr_repl.c  —  User-space eBPF loader + TCP incremental sender
 *
 * Changes from RDMA version:
 *   - Slave connections use TCP sockets instead of RDMA (libfabric)
 *   - No RDMA memory registration, CQ polling, or completion handling
 *   - TCP listener thread accepts incoming incr connections
 *   - Wire format unchanged: [uint64_t cmd_len][cmd_len bytes]
 *     (cmd_len = 0 is the graceful-disconnect sentinel)
 * ======================================================================= */

#include "ebpf_incr_repl.h"
#include "config.h"

#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <bpf/bpf.h>
#include <bpf/libbpf.h>

#define INCR_BACKLOG_CAP 4096

struct slave_conn {
    int fd;         /* TCP socket fd, -1 if inactive */
    int active;
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

/* TCP listener */
static int                  g_listen_fd = -1;
static pthread_t            g_listen_tid;
static int                  g_listen_tid_started = 0;

static void slave_close_locked(struct slave_conn *s)
{
    if (!s || !s->active)
        return;
    if (s->fd >= 0) {
        close(s->fd);
        s->fd = -1;
    }
    s->active = 0;
}

/* Send one frame over TCP: [uint64_t len][len bytes].
 * Returns 0 on success, -1 on failure (caller should close slave). */
static int slave_send_frame_locked(struct slave_conn *s, const char *msg,
                                   uint32_t len)
{
    if (!s || !s->active || s->fd < 0)
        return -1;
    if (!msg || len == 0 || len > INCR_MAX_CMD)
        return -1;

    uint64_t cmd_len = (uint64_t)len;

    /* Send length header (8 bytes, host byte order — same machine arch) */
    ssize_t n = send(s->fd, &cmd_len, sizeof(cmd_len), 0);
    if (n != (ssize_t)sizeof(cmd_len))
        return -1;

    /* Send payload */
    size_t remain = (size_t)len;
    const char *p = msg;
    while (remain > 0) {
        n = send(s->fd, p, remain, 0);
        if (n <= 0)
            return -1;
        p += n;
        remain -= (size_t)n;
    }
    return 0;
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

/* Replay all backlog records (start_seq=0 replay everything). */
static int replay_backlog_locked(struct slave_conn *s, uint64_t start_seq)
{
    if (g_backlog_count == 0)
        return 0;

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

int ebpf_incr_master_healthy(void)
{
    int healthy;
    pthread_mutex_lock(&g_mu);
    healthy = g_stream_healthy;
    pthread_mutex_unlock(&g_mu);
    return healthy;
}

int ebpf_incr_add_slave(int tcp_fd, uint64_t start_seq)
{
    if (tcp_fd < 0)
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
    s->fd = tcp_fd;
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

/* TCP listener thread: accepts incr connections from slaves.
 * Each accepted connection is registered via ebpf_incr_add_slave(fd, 0)
 * which replays the entire backlog before entering live streaming. */
static void *tcp_listener_thread(void *arg)
{
    (void)arg;
    printf("[INCR-REPL] TCP incr listener started on port %d\n",
           kvs_config.incr_port);

    while (g_running) {
        struct sockaddr_in addr;
        socklen_t addrlen = sizeof(addr);
        int fd = accept(g_listen_fd, (struct sockaddr *)&addr, &addrlen);
        if (fd < 0) {
            if (errno == EINTR)
                continue;
            if (!g_running)
                break;
            fprintf(stderr, "[INCR-REPL] accept failed: %s\n", strerror(errno));
            continue;
        }

        char peer[64];
        inet_ntop(AF_INET, &addr.sin_addr, peer, sizeof(peer));
        printf("[INCR-REPL] incr connection from %s:%d\n", peer,
               ntohs(addr.sin_port));

        /* Register with start_seq=0: replay entire backlog */
        if (ebpf_incr_add_slave(fd, 0) != 0) {
            fprintf(stderr, "[INCR-REPL] failed to register incr slave from %s\n",
                    peer);
            close(fd);
        }
    }

    printf("[INCR-REPL] TCP incr listener stopped\n");
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

    /* Initialize state */
    pthread_mutex_lock(&g_mu);
    g_backlog_start = 0;
    g_backlog_count = 0;
    g_seq = 0;
    g_stream_healthy = 1;
    g_last_dropped = 0;
    pthread_mutex_unlock(&g_mu);

    /* Start consumer thread */
    g_running = 1;
    g_tid_started = 0;
    if (pthread_create(&g_tid, NULL, consumer_thread, NULL) != 0) {
        fprintf(stderr, "[INCR-REPL] pthread_create failed: %s\n", strerror(errno));
        goto err_link;
    }
    g_tid_started = 1;

    /* Start TCP incr listener */
    g_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_listen_fd < 0) {
        fprintf(stderr, "[INCR-REPL] socket() failed: %s\n", strerror(errno));
        /* non-fatal: BPF/consumer still works, but incr connections disabled */
        goto done;
    }

    int opt = 1;
    setsockopt(g_listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)kvs_config.incr_port);

    if (bind(g_listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "[INCR-REPL] bind(port=%d) failed: %s\n",
                kvs_config.incr_port, strerror(errno));
        close(g_listen_fd);
        g_listen_fd = -1;
        goto done;
    }

    listen(g_listen_fd, 8);

    g_listen_tid_started = 0;
    if (pthread_create(&g_listen_tid, NULL, tcp_listener_thread, NULL) != 0) {
        fprintf(stderr, "[INCR-REPL] pthread_create(tcp listener) failed: %s\n",
                strerror(errno));
        close(g_listen_fd);
        g_listen_fd = -1;
    } else {
        g_listen_tid_started = 1;
    }

done:
    printf("[INCR-REPL] incremental sync module ready (TCP transport)\n");
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

    /* Stop TCP listener first so no new connections arrive */
    if (g_listen_fd >= 0) {
        close(g_listen_fd);
        g_listen_fd = -1;
    }
    if (g_listen_tid_started) {
        pthread_join(g_listen_tid, NULL);
        g_listen_tid_started = 0;
    }

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
