/* =======================================================================
 * ebpf_incr_repl.c  —  User-space eBPF loader + kernel-level sequencing
 *                       + lock-free SPSC fanout + Fullsync coordinator
 *
 * Architecture (refactored):
 *   - eBPF uprobe: captures write commands, assigns atomic seq in kernel,
 *     writes dynamically-sized events into ring buffer
 *   - Consumer thread: polls ring buffer, appends to pre-allocated backlog,
 *     pushes to per-slave lock-free SPSC queues
 *   - Per-slave send threads: drain SPSC queues, TCP sendmsg(writev)
 *   - Fullsync state machine: IDLE -> INPROGRESS -> CATCHUP -> IDLE
 *   - During fullsync (INPROGRESS/CATCHUP): events go to backlog only
 *   - Normal mode (IDLE): events pushed to SPSC queues (lock-free)
 *   - TCP listener accepts slave connections with INCR_SYNC handshake
 *
 * Copy path (3 user-space copies + kernel TCP):
 *   uprobe capture -> ringbuf (mmap'd, zero-copy to userspace)
 *   -> backlog pre-allocated slot (memcpy) + SPSC slot (memcpy)
 *   -> TCP sendmsg(writev) (kernel copy)
 *
 * Contrast with old design: removed g_mu, DynBuf concatenation,
 * per-event malloc, byte-at-a-time TCP read.
 * ======================================================================= */

#include "ebpf_incr_repl.h"
#include "server.h"
#include "config.h"

#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/eventfd.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <bpf/bpf.h>
#include <bpf/libbpf.h>

/* ================================================================
 * Global state
 * ================================================================ */

/* Per-slave SPSC queues (lock-free producer/consumer) */
static slave_spsc_t       g_slaves[INCR_MAX_SLAVES];

/* Pre-allocated backlog ring */
static backlog_slot_t      g_backlog[INCR_BACKLOG_CAP];
static size_t              g_backlog_start = 0;
static size_t              g_backlog_count = 0;

/* Narrow mutex: protects backlog metadata (start/count/valid).
 * Held by consumer thread (backlog_append) and slave/replay paths.
 * NOT held during SPSC push or TCP send — only for backlog bookkeeping. */
static pthread_mutex_t     g_backlog_mu = PTHREAD_MUTEX_INITIALIZER;

/* Tracking (single writer: consumer thread, no lock needed) */
static uint64_t            g_seq           = 0;
static int                 g_stream_healthy = 1;
static uint64_t            g_last_dropped  = 0;

/* Fullsync state machine (written by RDMA thread, read by consumer) */
static volatile int        g_fullsync_status    = FULLSYNC_IDLE;
static volatile uint64_t   g_fullsync_base_seq  = 0;

/* eBPF resources */
static struct bpf_object  *g_obj = NULL;
static struct bpf_link    *g_link = NULL;
static struct ring_buffer *g_rb = NULL;
static pthread_t           g_tid;
static volatile int        g_running = 0;
static int                 g_tid_started = 0;
static int                 g_stats_fd = -1;
static int                 g_global_seq_fd = -1;

/* TCP listener */
static int                 g_listen_fd = -1;
static pthread_t           g_listen_tid;
static int                 g_listen_tid_started = 0;

/* Phase 2: kernel-level multicast fanout plugin (future) */
static incr_fanout_fn      g_fanout_fn = NULL;

/* ================================================================
 * SPSC slot helpers (lazy allocation, AOF pattern)
 * ================================================================ */

static void spsc_slot_ensure(spsc_slot_t *s, int need) {
    if (s->msg_cap >= need)
        return;
    int new_cap = s->msg_cap ? s->msg_cap : SPSC_SLOT_INIT_CAP;
    while (new_cap < need)
        new_cap *= 2;
    char *nb = (char *)realloc(s->msg, (size_t)new_cap);
    if (!nb) return;
    s->msg     = nb;
    s->msg_cap = new_cap;
}

/* ================================================================
 * Lock-free SPSC push (producer: consumer thread)
 *
 * Pattern: AOF ring buffer from persistence.c lines 313-342
 * ================================================================ */

static int spsc_push(slave_spsc_t *q, uint64_t seq,
                     const char *msg, uint32_t len)
{
    if (!q || !q->active || len > INCR_MAX_CMD)
        return -1;

    uint32_t h = atomic_load_explicit(&q->head, memory_order_relaxed);
    uint32_t t;

    /* Spin if queue full (slave consumer is slow) */
    for (int spin = 0; ; spin++) {
        t = atomic_load_explicit(&q->tail, memory_order_acquire);
        if (h - t < SLAVE_SPSC_SIZE)
            break;
        if (spin < 100) {
            /* short busy-wait */
        } else {
            usleep(100);
        }
    }

    spsc_slot_t *s = &q->slots[h & SLAVE_SPSC_MASK];
    spsc_slot_ensure(s, (int)len);
    s->seq    = seq;
    s->length = len;
    memcpy(s->msg, msg, (size_t)len);

    atomic_store_explicit(&q->head, h + 1, memory_order_release);

    /* Wake per-slave send thread */
    uint64_t one = 1;
    ssize_t wr = write(q->evt_fd, &one, sizeof(one));
    (void)wr;

    return 0;
}

/* ================================================================
 * Per-slave send thread (consumer side of SPSC)
 *
 * Pattern: AOF worker from persistence.c lines 169-240
 * ================================================================ */

static void *slave_send_thread(void *arg)
{
    slave_spsc_t *q = (slave_spsc_t *)arg;
    uint32_t t = atomic_load_explicit(&q->tail, memory_order_relaxed);

    while (atomic_load_explicit(&q->running, memory_order_relaxed)) {
        uint32_t h = atomic_load_explicit(&q->head, memory_order_acquire);

        while (t != h) {
            spsc_slot_t *s = &q->slots[t & SLAVE_SPSC_MASK];

            /* Scatter/gather: send [len:8][payload] in one syscall.
             * No DynBuf concatenation needed. */
            uint64_t cmd_len = (uint64_t)s->length;
            struct iovec iov[2] = {
                { .iov_base = &cmd_len, .iov_len = sizeof(cmd_len) },
                { .iov_base = s->msg,   .iov_len = s->length }
            };
            struct msghdr mh = {
                .msg_iov    = iov,
                .msg_iovlen = 2
            };

            ssize_t sent = sendmsg(q->fd, &mh, MSG_NOSIGNAL);
            if (sent <= 0) {
                q->active = 0;
                goto done;
            }
            t++;
        }

        atomic_store_explicit(&q->tail, t, memory_order_release);

        /* Block on eventfd until producer signals new data */
        h = atomic_load_explicit(&q->head, memory_order_acquire);
        if (t != h)
            continue;

        uint64_t cnt;
        ssize_t nr = read(q->evt_fd, &cnt, sizeof(cnt));
        if (nr < 0 && errno != EAGAIN && errno != EINTR)
            break;
    }

done:
    atomic_store_explicit(&q->tail, t, memory_order_release);
    if (q->fd >= 0) {
        close(q->fd);
        q->fd = -1;
    }
    q->active = 0;
    return NULL;
}

/* ================================================================
 * Slave lifecycle
 * ================================================================ */

static void slave_close(slave_spsc_t *q)
{
    if (!q || !q->active)
        return;
    q->running = 0;
    /* Wake send thread if blocked on eventfd */
    uint64_t one = 1;
    ssize_t wr = write(q->evt_fd, &one, sizeof(one));
    (void)wr;
    if (q->send_tid) {
        pthread_join(q->send_tid, NULL);
        q->send_tid = 0;
    }
    if (q->fd >= 0) {
        close(q->fd);
        q->fd = -1;
    }
    if (q->evt_fd >= 0) {
        close(q->evt_fd);
        q->evt_fd = -1;
    }
    q->active = 0;
}

static int slave_start(slave_spsc_t *q, int fd, uint64_t start_seq)
{
    if (!q || fd < 0)
        return -1;

    memset(q, 0, sizeof(*q));
    q->fd      = fd;
    q->active  = 1;
    q->running = 1;

    q->evt_fd = eventfd(0, EFD_NONBLOCK);
    if (q->evt_fd < 0) {
        close(fd);
        return -1;
    }

    /* Replay backlog entries with seq >= start_seq */
    pthread_mutex_lock(&g_backlog_mu);
    for (size_t i = 0; i < g_backlog_count; i++) {
        size_t idx = (g_backlog_start + i) % INCR_BACKLOG_CAP;
        backlog_slot_t *rec = &g_backlog[idx];
        if (!rec->valid || rec->seq < start_seq)
            continue;
        if (rec->msg &&
            spsc_push(q, rec->seq, rec->msg, rec->length) != 0) {
            pthread_mutex_unlock(&g_backlog_mu);
            slave_close(q);
            return -1;
        }
    }
    pthread_mutex_unlock(&g_backlog_mu);

    /* Start per-slave send thread */
    if (pthread_create(&q->send_tid, NULL, slave_send_thread, q) != 0) {
        slave_close(q);
        return -1;
    }

    return 0;
}

/* ================================================================
 * Pre-allocated backlog append (no malloc, narrow lock)
 * ================================================================ */

static void backlog_append(uint64_t seq, const char *payload,
                           uint32_t length)
{
    pthread_mutex_lock(&g_backlog_mu);

    size_t idx = (g_backlog_start + g_backlog_count) % INCR_BACKLOG_CAP;

    if (g_backlog_count == INCR_BACKLOG_CAP) {
        /* Evict oldest: mark invalid, reuse slot buffer */
        g_backlog[g_backlog_start].valid = 0;
        g_backlog_start = (g_backlog_start + 1) % INCR_BACKLOG_CAP;
    } else {
        g_backlog_count++;
    }

    g_backlog[idx].seq    = seq;
    g_backlog[idx].length = length;
    g_backlog[idx].valid  = 1;
    if (g_backlog[idx].msg && length > 0)
        memcpy(g_backlog[idx].msg, payload, (size_t)length);

    pthread_mutex_unlock(&g_backlog_mu);
}

/* ================================================================
 * Health check (no lock needed — single writer: consumer thread)
 * ================================================================ */

static void check_stats_health(void)
{
    if (g_stats_fd < 0 || !g_running)
        return;

    uint32_t key = 0;
    struct incr_stats {
        uint64_t emitted;
        uint64_t dropped;
    } st = {0};

    if (bpf_map_lookup_elem(g_stats_fd, &key, &st) != 0)
        return;

    if (st.dropped > g_last_dropped) {
        g_last_dropped = st.dropped;
        g_stream_healthy = 0;
        fprintf(stderr, "[INCR-REPL] stream unhealthy: ring buffer dropped "
                "incremental events\n");

        /* Close all slaves — no lock needed, send threads exit independently */
        for (int i = 0; i < INCR_MAX_SLAVES; i++)
            slave_close(&g_slaves[i]);
    }
}

/* ================================================================
 * Fullsync coordination API (called from RDMA master thread)
 * ================================================================ */

uint64_t ebpf_incr_read_global_seq(void)
{
    if (g_global_seq_fd < 0)
        return g_seq;
    uint32_t zero = 0;
    uint64_t val  = 0;
    if (bpf_map_lookup_elem(g_global_seq_fd, &zero, &val) != 0)
        return g_seq;
    return val;
}

uint64_t ebpf_incr_fullsync_begin(void)
{
    g_fullsync_status   = FULLSYNC_INPROGRESS;
    uint64_t base = ebpf_incr_read_global_seq();
    g_fullsync_base_seq = base;
    printf("[INCR-REPL] fullsync begin: base_seq=%llu, status=INPROGRESS\n",
           (unsigned long long)base);
    return base;
}

void ebpf_incr_fullsync_send_done(void)
{
    pthread_mutex_lock(&g_backlog_mu);

    int waiting_slaves = 0;
    for (int i = 0; i < INCR_MAX_SLAVES; i++) {
        slave_spsc_t *q = &g_slaves[i];
        if (!q->active)
            continue;
        waiting_slaves++;

        /* If slave was deferred (INPROGRESS), init and replay backlog now */
        if (q->evt_fd < 0) {
            int fd = q->fd;
            if (slave_start(q, fd, g_fullsync_base_seq) != 0)
                continue;
        } else {
            /* Replay any backlog entries not yet sent */
            for (size_t j = 0; j < g_backlog_count; j++) {
                size_t idx = (g_backlog_start + j) % INCR_BACKLOG_CAP;
                backlog_slot_t *rec = &g_backlog[idx];
                if (!rec->valid || rec->seq < g_fullsync_base_seq)
                    continue;
                if (rec->msg && spsc_push(q, rec->seq, rec->msg,
                                          rec->length) != 0)
                    slave_close(q);
            }
        }
    }

    if (waiting_slaves > 0) {
        g_fullsync_status = FULLSYNC_IDLE;
        printf("[INCR-REPL] fullsync send done: status=IDLE "
               "(base_seq=%llu, %d slave(s) already connected)\n",
               (unsigned long long)g_fullsync_base_seq, waiting_slaves);
    } else {
        g_fullsync_status = FULLSYNC_CATCHUP;
        printf("[INCR-REPL] fullsync send done: status=CATCHUP "
               "(base_seq=%llu, waiting for slave)\n",
               (unsigned long long)g_fullsync_base_seq);
    }

    pthread_mutex_unlock(&g_backlog_mu);
}

void ebpf_incr_fullsync_finish(void)
{
    g_fullsync_status = FULLSYNC_IDLE;
    printf("[INCR-REPL] fullsync finish: status=IDLE\n");
}

int ebpf_incr_fullsync_status(void)
{
    return g_fullsync_status;
}

uint64_t ebpf_incr_fullsync_base_seq(void)
{
    return g_fullsync_base_seq;
}

void ebpf_incr_fullsync_set_base_seq(uint64_t seq)
{
    g_fullsync_base_seq = seq;
}

/* ================================================================
 * Health / slave management
 * ================================================================ */

int ebpf_incr_master_healthy(void)
{
    return g_stream_healthy;
}

int ebpf_incr_add_slave(int tcp_fd, uint64_t start_seq)
{
    if (tcp_fd < 0)
        return -1;

    if (!g_running || !g_stream_healthy) {
        close(tcp_fd);
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
        close(tcp_fd);
        fprintf(stderr, "[INCR-REPL] no free slot for incremental slave\n");
        return -1;
    }

    int deferred = 0;
    if (g_fullsync_status == FULLSYNC_INPROGRESS) {
        deferred = 1;
        /* Set up shell structure — full init deferred to send_done */
        slave_spsc_t *q = &g_slaves[slot];
        memset(q, 0, sizeof(*q));
        q->fd      = tcp_fd;
        q->active  = 1;
        q->evt_fd  = -1;    /* marker: not yet fully initialized */
    } else {
        if (slave_start(&g_slaves[slot], tcp_fd, start_seq) != 0)
            return -1;
    }

    printf("[INCR-REPL] slave registered slot=%d, catch-up from seq=%llu%s\n",
           slot, (unsigned long long)start_seq,
           deferred ? " (deferred to send_done)" : "");

    return 0;
}

/* ================================================================
 * Consumer: ring buffer -> backlog -> SPSC push -> slaves
 * ================================================================ */

static int on_incr_event(void *ctx, void *data, size_t sz)
{
    (void)ctx;

    /* Ringbuf record is the full struct incr_event (matches BPF side).
     * Fixed-size reservation: sizeof(incr_event) = 16 + INCR_MAX_CMD. */
    if (sz < sizeof(incr_event))
        return 0;

    incr_event *e = (incr_event *)data;

    if (e->length == 0 || e->length > INCR_MAX_CMD)
        return 0;

    uint64_t seq = e->seq;

    /* ── Hot path: NO global mutex ── */

    if (!g_stream_healthy)
        return 0;

    /* Update tracking (single writer: consumer thread) */
    if (seq > g_seq)
        g_seq = seq;

    /* Append to pre-allocated backlog (narrow lock, no malloc) */
    backlog_append(seq, e->msg, e->length);

    /* Phase 2: if kernel-level fanout is registered, use it */
    if (g_fanout_fn) {
        g_fanout_fn(NULL, seq, e->msg, e->length);
        return 0;
    }

    /* Normal mode: lock-free SPSC push to each active slave.
     * Per-slave send threads handle TCP independently. */
    if (g_fullsync_status == FULLSYNC_IDLE) {
        for (int i = 0; i < INCR_MAX_SLAVES; i++) {
            slave_spsc_t *q = &g_slaves[i];
            if (!q->active)
                continue;
            if (spsc_push(q, seq, e->msg, e->length) != 0) {
                fprintf(stderr, "[INCR-REPL] SPSC full slot %d, closing\n", i);
                slave_close(q);
            }
        }
    }
    /* INPROGRESS / CATCHUP: backlog only, no direct push to slaves */

    return 0;
}

static void *consumer_thread(void *arg)
{
    (void)arg;
    printf("[INCR-REPL] consumer thread started\n");

    while (g_running) {
        /* ring_buffer__poll returns immediately when data available.
         * 100ms timeout only matters when idle — during busy periods,
         * it returns as fast as events arrive. */
        int pr = ring_buffer__poll(g_rb, 100);
        if (pr < 0 && pr != -EINTR) {
            fprintf(stderr, "[INCR-REPL] ring_buffer__poll failed: %d\n", pr);
            g_stream_healthy = 0;
            for (int i = 0; i < INCR_MAX_SLAVES; i++)
                slave_close(&g_slaves[i]);
            break;
        }
        check_stats_health();
    }

    printf("[INCR-REPL] consumer thread stopped\n");
    return NULL;
}

/* ================================================================
 * TCP incr listener
 * ================================================================ */

/* Buffered TCP line reader.
 * Reads in chunks of up to 64 bytes, scans for \n.
 * Replaces the old byte-at-a-time recv() that caused ~30 syscalls
 * per handshake ("INCR_SYNC 18446744073709551615\r\n" = ~37 bytes). */
static int tcp_read_line(int fd, char *line, int max_len)
{
    static char peek[64];
    static int  peek_len = 0;
    static int  peek_off = 0;

    int pos = 0;
    while (pos < max_len - 1) {
        if (peek_off >= peek_len) {
            ssize_t n = recv(fd, peek, sizeof(peek), 0);
            if (n <= 0)
                return -1;
            peek_len = (int)n;
            peek_off = 0;
        }

        char c = peek[peek_off++];
        if (c == '\n') {
            if (pos > 0 && line[pos - 1] == '\r')
                pos--;
            line[pos] = '\0';
            return pos;
        }
        line[pos++] = c;
    }
    return -1; /* line too long */
}

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

        /* Read INCR_SYNC <base_seq> handshake line */
        char line[256];
        int line_len = tcp_read_line(fd, line, (int)sizeof(line));
        if (line_len <= 0) {
            fprintf(stderr, "[INCR-REPL] failed to read INCR_SYNC handshake "
                    "from %s\n", peer);
            close(fd);
            continue;
        }

        uint64_t slave_seq = 0;
        int is_sync = 0;
        if (strncmp(line, INCR_SYNC_CMD, strlen(INCR_SYNC_CMD)) == 0) {
            is_sync = 1;
            slave_seq = strtoull(line + strlen(INCR_SYNC_CMD), NULL, 10);
        }

        printf("[INCR-REPL] slave %s handshake: %s seq=%llu\n",
               peer, is_sync ? "INCR_SYNC" : "LEGACY",
               (unsigned long long)slave_seq);

        /* Register slave with seq-based catchup */
        if (ebpf_incr_add_slave(fd, slave_seq) != 0) {
            fprintf(stderr, "[INCR-REPL] failed to register slave from %s\n",
                    peer);
            close(fd);
            continue;
        }

        /* If we were in CATCHUP state and the slave sent INCR_SYNC
         * with our base_seq, the catchup is complete. */
        if (is_sync &&
            g_fullsync_status == FULLSYNC_CATCHUP &&
            slave_seq == g_fullsync_base_seq) {
            ebpf_incr_fullsync_finish();
        }
    }

    printf("[INCR-REPL] TCP incr listener stopped\n");
    return NULL;
}

/* ================================================================
 * Master start / stop
 * ================================================================ */

int ebpf_incr_master_start(void)
{
    if (g_running)
        return 0;

    char exe_path[PATH_MAX];
    char bpf_path[PATH_MAX + 32];
    ssize_t n = readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1);
    if (n > 0) {
        exe_path[n] = '\0';
        char *slash = strrchr(exe_path, '/');
        if (slash) {
            *slash = '\0';
            snprintf(bpf_path, sizeof(bpf_path), "%s/ebpf_incr.bpf.o", exe_path);
        } else {
            snprintf(bpf_path, sizeof(bpf_path), "ebpf_incr.bpf.o");
        }
    } else {
        snprintf(bpf_path, sizeof(bpf_path), "ebpf_incr.bpf.o");
    }

    g_obj = bpf_object__open_file(bpf_path, NULL);
    if (!g_obj || libbpf_get_error(g_obj)) {
        fprintf(stderr,
                "[INCR-REPL] bpf_object__open_file failed (%s)\n", bpf_path);
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

    /* Open global_seq map fd for reading current seq from userspace */
    struct bpf_map *seq_map = bpf_object__find_map_by_name(g_obj, "global_seq");
    if (seq_map) {
        g_global_seq_fd = bpf_map__fd(seq_map);
    } else {
        fprintf(stderr, "[INCR-REPL] map 'global_seq' not found, "
                "using user-space fallback\n");
        g_global_seq_fd = -1;
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
    g_backlog_start    = 0;
    g_backlog_count    = 0;
    g_seq              = 0;
    g_stream_healthy   = 1;
    g_last_dropped     = 0;
    g_fullsync_status  = FULLSYNC_IDLE;
    g_fullsync_base_seq = 0;
    g_fanout_fn        = NULL;

    /* Pre-allocate backlog slot buffers (no malloc in hot path) */
    for (size_t i = 0; i < INCR_BACKLOG_CAP; i++) {
        g_backlog[i].msg     = (char *)malloc(INCR_MAX_CMD);
        g_backlog[i].msg_cap = g_backlog[i].msg ? INCR_MAX_CMD : 0;
        g_backlog[i].valid   = 0;
    }

    /* Initialize slave SPSC queues */
    for (int i = 0; i < INCR_MAX_SLAVES; i++) {
        memset(&g_slaves[i], 0, sizeof(g_slaves[i]));
        g_slaves[i].fd     = -1;
        g_slaves[i].evt_fd = -1;
        g_slaves[i].active = 0;
    }

    /* Start consumer thread */
    g_running = 1;
    g_tid_started = 0;
    if (pthread_create(&g_tid, NULL, consumer_thread, NULL) != 0) {
        fprintf(stderr, "[INCR-REPL] pthread_create failed: %s\n",
                strerror(errno));
        goto err_link;
    }
    g_tid_started = 1;

    /* Start TCP incr listener */
    g_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_listen_fd < 0) {
        fprintf(stderr, "[INCR-REPL] socket() failed: %s\n",
                strerror(errno));
        goto err_listen;
    }

    int opt = 1;
    setsockopt(g_listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons((uint16_t)kvs_config.incr_port);

    if (bind(g_listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "[INCR-REPL] bind(port=%d) failed: %s\n",
                kvs_config.incr_port, strerror(errno));
        close(g_listen_fd);
        g_listen_fd = -1;
        goto err_listen;
    }

    listen(g_listen_fd, 8);

    g_listen_tid_started = 0;
    if (pthread_create(&g_listen_tid, NULL, tcp_listener_thread, NULL) != 0) {
        fprintf(stderr, "[INCR-REPL] pthread_create(tcp listener) failed: %s\n",
                strerror(errno));
        close(g_listen_fd);
        g_listen_fd = -1;
        goto err_listen;
    } else {
        g_listen_tid_started = 1;
    }

    printf("[INCR-REPL] incremental sync module ready (seq in eBPF, "
           "SPSC fanout active, backlog=%d slots pre-allocated)\n",
           INCR_BACKLOG_CAP);
    return 0;

err_listen:
    g_running = 0;
    if (g_tid_started) {
        pthread_join(g_tid, NULL);
        g_tid_started = 0;
    }
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
    g_stats_fd       = -1;
    g_global_seq_fd  = -1;
    if (g_obj) {
        bpf_object__close(g_obj);
        g_obj = NULL;
    }
    /* Free pre-allocated backlog buffers on error */
    for (size_t i = 0; i < INCR_BACKLOG_CAP; i++) {
        free(g_backlog[i].msg);
        g_backlog[i].msg = NULL;
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

    g_fullsync_status = FULLSYNC_IDLE;

    /* Close all per-slave send threads and free SPSC slot buffers */
    for (int i = 0; i < INCR_MAX_SLAVES; i++) {
        if (g_slaves[i].active)
            slave_close(&g_slaves[i]);
        for (int j = 0; j < SLAVE_SPSC_SIZE; j++)
            free(g_slaves[i].slots[j].msg);
    }

    /* Free pre-allocated backlog buffers */
    for (size_t i = 0; i < INCR_BACKLOG_CAP; i++) {
        free(g_backlog[i].msg);
        g_backlog[i].msg   = NULL;
        g_backlog[i].valid = 0;
    }
    g_backlog_start = 0;
    g_backlog_count = 0;

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
    g_stats_fd       = -1;
    g_global_seq_fd  = -1;
}

/* ================================================================
 * Phase 2: kernel-level multicast fanout plugin (future)
 * ================================================================ */

void ebpf_incr_set_fanout(incr_fanout_fn fn)
{
    g_fanout_fn = fn;
}
