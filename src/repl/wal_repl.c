/* wal_repl.c — WAL-based incremental replication
 *
 * Replaces ebpf_incr_repl.c. Zero BPF dependencies.
 *
 * Data flow (write path):
 *   kvs_rdma_incr_probe()
 *     └─▶ wal_repl_append()            [main thread, lock-free]
 *           ├─▶ journal ring slot      [in-process, atomic seq]
 *           └─▶ wakes consumer thread
 *
 *   consumer thread:
 *     ├─▶ io_uring_prep_write → WAL file   [crash recovery + reconnect]
 *     ├─▶ backlog_append()                 [fast in-memory catchup]
 *     └─▶ spsc_push() per active slave     [live streaming]
 *
 *   Per-slave send thread drains SPSC via TCP (Phase 3: replace with
 *   single io_uring dispatch thread for N-slave zero-copy batch send).
 */

#define _GNU_SOURCE
#include "wal_repl.h"
#include "config.h"
#include "persistence.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdatomic.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/eventfd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <liburing.h>
#include <zlib.h>

/* ── Journal ring (in-memory, producer: main thread, consumer: consumer thread) ── */
/* 131072 slots: at 300K QPS each slot is filled in ~3.3us; the ring can absorb
 * ~437ms of consumer stall (e.g. fdatasync on slow VM disk ~272ms) without
 * overflowing.  Memory cost: 131072 * ~32 bytes = ~4 MB (BSS). */
#define JOURNAL_RING_SIZE  131072
#define JOURNAL_RING_MASK  (JOURNAL_RING_SIZE - 1)

typedef struct {
    uint64_t  seq;
    uint32_t  length;    /* payload length */
    uint32_t  wire_len;  /* 8 + length: [uint64_t len][payload] */
    char     *buf;       /* wire frame buffer                   */
    int       cap;
} journal_slot_t;

static journal_slot_t    g_jslots[JOURNAL_RING_SIZE];
static _Atomic uint32_t  g_jhead = 0;   /* producer writes */
static uint32_t          g_jtail = 0;   /* consumer owns   */
static int               g_jevt  = -1;  /* eventfd wakeup  */

static _Atomic uint64_t  g_journal_seq     = 0;  /* global seq counter */
static _Atomic uint64_t  g_dropped         = 0;  /* overrun counter    */
static _Atomic int       g_consumer_sleeping = 0; /* 1 = blocked on eventfd read */

/* Phase 4B.3 — the journal ring was originally designed for a single
 * producer (the main proactor thread).  Phase 4 introduced multiple
 * proactor workers that all funnel writes through wal_repl_append();
 * concurrent producers would race on (g_jhead load, slot write,
 * g_jhead store), losing entries and corrupting slots.
 *
 * The simplest correct fix is a producer mutex.  This path only runs
 * when persistence != none OR rdma_sync is enabled (g_running == 1),
 * so default-config benches are unaffected.  A lock-free MPSC ring
 * could replace this in Phase 7 if persistence-on benches need it. */
static pthread_mutex_t   g_journal_producer_mu = PTHREAD_MUTEX_INITIALIZER;

/* ── WAL file state ── */
static int              g_wal_fd       = -1;

/* ── Slave state (io_uring dispatch, no per-slave threads) ── */
typedef struct { int fd; int active; } slave_state_t;
static slave_state_t  g_slaves[INCR_MAX_SLAVES];

/* ── Dispatch io_uring ring (single ring for all slave sends) ── */
#define DISPATCH_URING_DEPTH  256
static struct io_uring  g_dispatch_ring;
static int              g_dispatch_uring_ok = 0;
static int              g_dispatch_pending  = 0;
static uint32_t         g_wal_sync_cnt      = 0;  /* fdatasync batch counter */

/* ── In-memory backlog ── */
static backlog_slot_t   g_backlog[INCR_BACKLOG_CAP];
static uint64_t         g_backlog_start = 0;
static uint64_t         g_backlog_count = 0;
static pthread_mutex_t  g_backlog_mu    = PTHREAD_MUTEX_INITIALIZER;

/* ── Module state ── */
static volatile int  g_running        = 0;
static pthread_t     g_tid;
static int           g_tid_started    = 0;
static int           g_listen_fd      = -1;
static pthread_t     g_listen_tid;
static int           g_listen_started = 0;

/* ── Replication state ── */
static char             g_replid[INCR_REPL_ID_LEN + 1];
static uint64_t         g_seq           = 0;
static int              g_stream_healthy= 1;
static int              g_fullsync_status = FULLSYNC_IDLE;
static uint64_t         g_fullsync_base_seq = 0;
static incr_fanout_fn   g_fanout_fn     = NULL;

/* ======================================================================
 * Slave management (io_uring dispatch, no per-slave threads)
 * ====================================================================== */

/* Counts active slaves; allows backlog_append to be skipped when zero. */
static _Atomic int g_n_active_slaves = 0;

static void slave_close(int i)
{
    if (!g_slaves[i].active) return;
    g_slaves[i].active = 0;
    if (g_slaves[i].fd >= 0) { close(g_slaves[i].fd); g_slaves[i].fd = -1; }
    atomic_fetch_sub_explicit(&g_n_active_slaves, 1, memory_order_relaxed);
}

/* Drain completed dispatch CQEs; blocks until at least n_wait are reaped. */
static void dispatch_reap_cqes(int n_wait)
{
    for (int k = 0; k < n_wait; k++) {
        struct io_uring_cqe *cqe;
        if (io_uring_wait_cqe(&g_dispatch_ring, &cqe) == 0) {
            if (cqe->res < 0) {
                int idx = (int)io_uring_cqe_get_data64(cqe);
                if (idx >= 0 && idx < INCR_MAX_SLAVES) {
                    fprintf(stderr, "[WAL-REPL] slave %d send error: %s\n",
                            idx, strerror(-cqe->res));
                    slave_close(idx);
                }
            }
            io_uring_cqe_seen(&g_dispatch_ring, cqe);
            if (g_dispatch_pending > 0) g_dispatch_pending--;
        }
    }
    /* Drain any additional completions without blocking */
    struct io_uring_cqe *cqe;
    while (io_uring_peek_cqe(&g_dispatch_ring, &cqe) == 0) {
        if (cqe->res < 0) {
            int idx = (int)io_uring_cqe_get_data64(cqe);
            if (idx >= 0 && idx < INCR_MAX_SLAVES) {
                fprintf(stderr, "[WAL-REPL] slave %d send error: %s\n",
                        idx, strerror(-cqe->res));
                slave_close(idx);
            }
        }
        io_uring_cqe_seen(&g_dispatch_ring, cqe);
        if (g_dispatch_pending > 0) g_dispatch_pending--;
    }
}

/* Register a new slave. Sends backlog catchup synchronously, then marks
 * the slot active so the consumer dispatch loop will include it. */
static int slave_start(int i, int fd, uint64_t start_seq)
{
    g_slaves[i].fd     = fd;
    g_slaves[i].active = 0;

    pthread_mutex_lock(&g_backlog_mu);
    for (uint64_t j = 0; j < g_backlog_count; j++) {
        backlog_slot_t *bs =
            &g_backlog[(g_backlog_start + j) & INCR_BACKLOG_MASK];
        if (bs->seq < start_seq) continue;
        uint64_t plen = (uint64_t)bs->length;
        struct iovec iov[2] = {
            { .iov_base = &plen,    .iov_len = sizeof(plen) },
            { .iov_base = bs->msg,  .iov_len = bs->length   },
        };
        struct msghdr mh = { .msg_iov = iov, .msg_iovlen = 2 };
        if (sendmsg(fd, &mh, MSG_NOSIGNAL) < 0) {
            pthread_mutex_unlock(&g_backlog_mu);
            close(fd); g_slaves[i].fd = -1;
            return -1;
        }
    }
    pthread_mutex_unlock(&g_backlog_mu);

    g_slaves[i].active = 1;
    atomic_fetch_add_explicit(&g_n_active_slaves, 1, memory_order_relaxed);
    return 0;
}

/* ======================================================================
 * In-memory backlog
 * ====================================================================== */

static void backlog_slot_ensure(backlog_slot_t *s, int need)
{
    if (s->msg_cap >= need) return;
    /* Same rationale as journal_slot_ensure: 128 bytes covers typical commands.
     * 16384*4096=64MB was excessive heap; 16384*128=2MB is L3-friendly. */
    int nc = s->msg_cap ? s->msg_cap : 128;
    while (nc < need) nc *= 2;
    free(s->msg);
    s->msg = (char *)malloc((size_t)nc);
    s->msg_cap = s->msg ? nc : 0;
}

static void backlog_append(uint64_t seq, const char *msg, uint32_t len)
{
    /* Fast path: no slaves connected → backlog serves no purpose.
     * Skips mutex + memcpy, which at 300K QPS is a meaningful saving. */
    if (atomic_load_explicit(&g_n_active_slaves, memory_order_relaxed) == 0)
        return;

    pthread_mutex_lock(&g_backlog_mu);
    uint64_t idx = (g_backlog_start + g_backlog_count) & INCR_BACKLOG_MASK;
    backlog_slot_t *s = &g_backlog[idx];
    backlog_slot_ensure(s, (int)len);
    if (!s->msg) { pthread_mutex_unlock(&g_backlog_mu); return; }
    s->seq = seq; s->length = len;
    memcpy(s->msg, msg, len);
    if (g_backlog_count < INCR_BACKLOG_CAP) {
        g_backlog_count++;
    } else {
        g_backlog_start = (g_backlog_start + 1) & INCR_BACKLOG_MASK;
    }
    pthread_mutex_unlock(&g_backlog_mu);
}

static int backlog_has_seq_locked(uint64_t seq)
{
    if (g_backlog_count == 0) return 1;   /* empty = fresh master */
    uint64_t oldest = g_backlog[g_backlog_start & INCR_BACKLOG_MASK].seq;
    uint64_t newest = g_backlog[
        (g_backlog_start + g_backlog_count - 1) & INCR_BACKLOG_MASK].seq;
    return (seq >= oldest && seq <= newest + 1);
}

/* ======================================================================
 * Journal ring — producer side (main thread, no locks)
 * ====================================================================== */

static void journal_slot_ensure(journal_slot_t *s, int need)
{
    if (s->cap >= need) return;
    /* Start at 128 bytes (covers typical SET/GET ~65 bytes + 8-byte plen header).
     * Previous 4096-byte minimum caused 131072*4096=512MB heap, exceeding L3 cache
     * and causing DRAM misses on every hot-path memcpy. */
    int nc = s->cap ? s->cap : 128;
    while (nc < need) nc *= 2;
    free(s->buf);
    s->buf = (char *)malloc((size_t)nc);
    s->cap = s->buf ? nc : 0;
}

void wal_repl_append(const char *msg, int length)
{
    /* Fast path: WAL/repl module not running.  In default config
     * (persistence=none, rdma_sync=off) g_running stays 0, so this
     * function is a single-load + branch and adds zero contention to
     * multi-worker SET/GET.  No mutex needed for the fast-path check. */
    if (!g_running || !g_stream_healthy || length <= 0) return;

    /* Phase 4B.3: producer mutex — see comment at g_journal_producer_mu.
     * Only contended when WAL/repl is actually enabled, which is the
     * AOF / replication code path, not the perf-bench path. */
    pthread_mutex_lock(&g_journal_producer_mu);

    /* Re-check g_running under the lock — shutdown may have flipped it
     * between the early-return check and the lock acquisition. */
    if (!g_running || !g_stream_healthy) {
        pthread_mutex_unlock(&g_journal_producer_mu);
        return;
    }

    uint64_t seq = atomic_fetch_add_explicit(&g_journal_seq, 1,
                                             memory_order_relaxed);
    uint32_t h = atomic_load_explicit(&g_jhead, memory_order_relaxed);
    uint32_t t;
    for (int spin = 0; ; spin++) {
        t = atomic_load_explicit((_Atomic uint32_t *)&g_jtail,
                                 memory_order_acquire);
        if (h - t < JOURNAL_RING_SIZE) break;
        if (spin < 200) { /* busy wait */ }
        else {
            atomic_fetch_add(&g_dropped, 1);
            pthread_mutex_unlock(&g_journal_producer_mu);
            return;
        }
    }

    journal_slot_t *s = &g_jslots[h & JOURNAL_RING_MASK];
    journal_slot_ensure(s, (int)(length + 8));
    if (!s->buf) {
        pthread_mutex_unlock(&g_journal_producer_mu);
        return;
    }
    s->seq      = seq;
    s->length   = (uint32_t)length;
    s->wire_len = (uint32_t)(length + 8);
    uint64_t plen = (uint64_t)length;
    memcpy(s->buf,     &plen, 8);
    memcpy(s->buf + 8, msg,  (size_t)length);
    atomic_store_explicit(&g_jhead, h + 1, memory_order_release);

    pthread_mutex_unlock(&g_journal_producer_mu);

    /* Only write eventfd when consumer is actually sleeping — avoids one
     * syscall per write command when consumer is actively draining.
     * Safe outside the producer mutex: eventfd writes are themselves
     * serialised by the kernel; the only effect of multiple workers
     * writing here is at most a few extra wakeup events, which the
     * consumer collapses on the next read. */
    if (atomic_load_explicit(&g_consumer_sleeping, memory_order_acquire)) {
        uint64_t one = 1;
        ssize_t wr = write(g_jevt, &one, sizeof(one));
        (void)wr;
    }
}

/* ======================================================================
 * WAL file helpers (writev — simple, correct, no buffer lifetime races)
 * ====================================================================== */

/* Write one WAL record synchronously.  Uses writev(2) to issue header
 * + payload in a single syscall.  The data is fully copied to page cache
 * before we return, so the caller may immediately reuse its buffers.
 *
 * The consumer thread is a dedicated background thread — sync I/O here
 * does not block any worker thread.  io_uring is kept for the slave
 * dispatch path where N-slave batching truly matters. */
static void wal_write_record(uint64_t seq, const char *msg, uint32_t len)
{
    if (g_wal_fd < 0) return;

    wal_record_hdr_t hdr;
    hdr.magic  = WAL_MAGIC;
    hdr.seq    = seq;
    hdr.length = len;
    hdr.crc32  = (uint32_t)crc32(0, (const Bytef *)msg, (uInt)len);

    struct iovec iov[2] = {
        { .iov_base = &hdr,         .iov_len = WAL_HDR_SIZE },
        { .iov_base = (void *)msg,  .iov_len = len           },
    };
    ssize_t wr = writev(g_wal_fd, iov, 2);
    if (wr < 0)
        fprintf(stderr, "[WAL-REPL] WAL writev error: %s\n",
                strerror(errno));
}

static void wal_flush(void)
{
    if (g_wal_fd >= 0) fdatasync(g_wal_fd);
}

static int wal_open(void)
{
    const char *wal_path = persist_path(WAL_REL);
    /* Create parent directory (e.g. {basedir}/wal/) */
    char dir_buf[1024];
    strncpy(dir_buf, wal_path, sizeof(dir_buf) - 1);
    dir_buf[sizeof(dir_buf) - 1] = '\0';
    char *slash = strrchr(dir_buf, '/');
    if (slash && slash != dir_buf) { *slash = '\0'; mkdir(dir_buf, 0755); }
    g_wal_fd = open(wal_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (g_wal_fd < 0) {
        fprintf(stderr, "[WAL-REPL] open %s: %s\n", wal_path, strerror(errno));
        return -1;
    }
    printf("[WAL-REPL] WAL file opened: %s\n", wal_path);
    return 0;
}

/* ======================================================================
 * Consumer thread: journal ring → WAL file + backlog + SPSCs
 * ====================================================================== */

static void check_dropped_health(void)
{
    uint64_t d = atomic_load_explicit(&g_dropped, memory_order_relaxed);
    if (d > 0) {
        /* Ring overflow: log and reset counter, but keep writing future
         * records.  AOF persistence is best-effort — a gap is logged so
         * the operator knows the WAL may be incomplete.
         * Replication slaves cannot tolerate gaps, so we close them. */
        fprintf(stderr, "[WAL-REPL] WARNING: journal ring overflowed, dropped "
                "%" PRIu64 " records — WAL incomplete; closing slaves\n", d);
        atomic_store_explicit(&g_dropped, 0, memory_order_relaxed);
        for (int i = 0; i < INCR_MAX_SLAVES; i++)
            slave_close(i);
        /* Do NOT set g_stream_healthy = 0: AOF writes continue after overflow */
    }
}

/* ── WAL batch write buffers ─────────────────────────────────────────────
 * Consumer accumulates up to WAL_BATCH_MAX records before issuing a single
 * writev(), reducing syscall overhead from O(records) to O(records/128).
 * Headers are built in g_wal_hdrs[] because ring slots have no room for them.
 * Both arrays are file-scope (not stack) to avoid 16KB stack pressure. */
#define WAL_BATCH_MAX  128
static wal_record_hdr_t g_wal_hdrs[WAL_BATCH_MAX];
static struct iovec     g_wal_iov [WAL_BATCH_MAX * 2];

/* Periodic fdatasync: flush WAL at most once per second.
 * Calling fdatasync on every ring-empty event blocks the consumer for
 * ~272ms on VM disks, during which the ring can overflow at 300K QPS.
 * One-second batching (similar to Redis appendfsync everysec) is safe
 * for AOF persistence and avoids blocking the consumer. */
#define WAL_SYNC_INTERVAL_NS  1000000000LL   /* 1 second */

static struct timespec g_last_sync;   /* zero-initialised at program start */

static void wal_maybe_sync(void)
{
    if (g_wal_fd < 0) return;
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    long long elapsed = (long long)(now.tv_sec  - g_last_sync.tv_sec)  * 1000000000LL
                      + (long long)(now.tv_nsec - g_last_sync.tv_nsec);
    if (elapsed >= WAL_SYNC_INTERVAL_NS) {
        fdatasync(g_wal_fd);
        g_last_sync = now;
    }
}

static void *consumer_thread(void *arg)
{
    (void)arg;
    /* Block shutdown signals — only the main thread handles them */
    sigset_t ss; sigemptyset(&ss);
    sigaddset(&ss, SIGTERM); sigaddset(&ss, SIGINT);
    pthread_sigmask(SIG_BLOCK, &ss, NULL);
    printf("[WAL-REPL] consumer thread started\n");
    clock_gettime(CLOCK_MONOTONIC, &g_last_sync);   /* initialise sync timer */

    while (g_running) {
        uint32_t h = atomic_load_explicit(&g_jhead, memory_order_acquire);
        if (g_jtail == h) {
            /* Ring empty: periodic fdatasync (at most 1/s), then sleep.
             * We do NOT call fdatasync unconditionally here — on slow VM
             * disks it takes ~272ms and the ring can overflow while we wait.
             * Set sleeping flag BEFORE re-checking head to prevent a
             * lost-wakeup race. */
            wal_maybe_sync();
            atomic_store_explicit(&g_consumer_sleeping, 1, memory_order_release);
            h = atomic_load_explicit(&g_jhead, memory_order_acquire);
            if (g_jtail == h) {
                uint64_t cnt;
                ssize_t nr = read(g_jevt, &cnt, sizeof(cnt));
                if (nr < 0 && errno != EAGAIN && errno != EINTR) {
                    atomic_store_explicit(&g_consumer_sleeping, 0, memory_order_relaxed);
                    break;
                }
            }
            atomic_store_explicit(&g_consumer_sleeping, 0, memory_order_relaxed);
            continue;
        }

        /* Drain ring in batches: one writev per WAL_BATCH_MAX records. */
        while (g_jtail != h) {
            /* ── Pass 1: build WAL iov batch (g_jtail NOT advanced yet).
             * Slots [g_jtail, scan) are owned by consumer — producer
             * cannot overwrite them until we advance g_jtail. */
            int nbatch = 0;
            uint32_t scan = g_jtail;
            while (scan != h && nbatch < WAL_BATCH_MAX) {
                journal_slot_t *s = &g_jslots[scan & JOURNAL_RING_MASK];
                wal_record_hdr_t *hdr = &g_wal_hdrs[nbatch];
                hdr->magic  = WAL_MAGIC;
                hdr->seq    = s->seq;
                hdr->length = s->length;
                hdr->crc32  = (uint32_t)crc32(0,
                                  (const Bytef *)(s->buf + 8), (uInt)s->length);
                g_wal_iov[nbatch * 2 + 0] = (struct iovec){
                    .iov_base = hdr,         .iov_len = WAL_HDR_SIZE };
                g_wal_iov[nbatch * 2 + 1] = (struct iovec){
                    .iov_base = s->buf + 8,  .iov_len = s->length    };
                scan++;
                nbatch++;
            }

            /* Single writev for all nbatch record headers + payloads */
            if (g_wal_fd >= 0 && nbatch > 0)
                writev(g_wal_fd, g_wal_iov, nbatch * 2);

            /* ── Pass 2: backlog + slave dispatch; advance g_jtail ── */
            for (int bi = 0; bi < nbatch; bi++) {
                journal_slot_t *s = &g_jslots[g_jtail & JOURNAL_RING_MASK];

                if (s->seq > g_seq) g_seq = s->seq;

                /* 3. Append to in-memory backlog */
                backlog_append(s->seq, s->buf + 8, s->length);

                /* 4. Batch-dispatch to all slaves via io_uring */
                if (g_fullsync_status == FULLSYNC_IDLE && g_dispatch_uring_ok) {
                    int nsq = 0;
                    for (int i = 0; i < INCR_MAX_SLAVES; i++) {
                        if (!g_slaves[i].active) continue;
                        struct io_uring_sqe *sqe =
                            io_uring_get_sqe(&g_dispatch_ring);
                        if (!sqe) {
                            io_uring_submit(&g_dispatch_ring);
                            dispatch_reap_cqes(g_dispatch_pending);
                            sqe = io_uring_get_sqe(&g_dispatch_ring);
                        }
                        if (sqe) {
                            io_uring_prep_send(sqe, g_slaves[i].fd,
                                               s->buf, s->wire_len,
                                               MSG_NOSIGNAL);
                            io_uring_sqe_set_data64(sqe, (uint64_t)i);
                            nsq++;
                            g_dispatch_pending++;
                        } else {
                            slave_close(i);
                        }
                    }
                    if (nsq > 0) {
                        io_uring_submit(&g_dispatch_ring);
                        dispatch_reap_cqes(nsq);
                    }
                    if (g_fanout_fn)
                        g_fanout_fn(NULL, s->seq, s->buf + 8, s->length);
                }

                g_jtail++;
            }
        }

        check_dropped_health();
        wal_maybe_sync();   /* flush WAL at most once per second */
        h = atomic_load_explicit(&g_jhead, memory_order_acquire);
    }

    wal_flush();   /* final fdatasync on exit — ensures last records hit disk */
    printf("[WAL-REPL] consumer thread stopped\n");
    return NULL;
}

/* ======================================================================
 * TCP incr listener (handshake protocol unchanged)
 * ====================================================================== */

static int tcp_read_line(int fd, char *line, int max_len)
{
    int pos = 0;
    while (pos < max_len - 1) {
        char c;
        ssize_t n = recv(fd, &c, 1, 0);
        if (n <= 0) return -1;
        if (c == '\n') {
            if (pos > 0 && line[pos - 1] == '\r') pos--;
            line[pos] = '\0';
            return pos;
        }
        line[pos++] = c;
    }
    return -1;
}

static void *tcp_listener_thread(void *arg)
{
    (void)arg;
    sigset_t ss; sigemptyset(&ss);
    sigaddset(&ss, SIGTERM); sigaddset(&ss, SIGINT);
    pthread_sigmask(SIG_BLOCK, &ss, NULL);
    printf("[WAL-REPL] TCP incr listener on port %d\n", kvs_config.incr_port);

    while (g_running) {
        struct sockaddr_in addr;
        socklen_t addrlen = sizeof(addr);
        int fd = accept(g_listen_fd, (struct sockaddr *)&addr, &addrlen);
        if (fd < 0) {
            if (errno == EINTR) continue;
            if (!g_running)    break;
            fprintf(stderr, "[WAL-REPL] accept: %s\n", strerror(errno));
            continue;
        }

        char peer[64];
        inet_ntop(AF_INET, &addr.sin_addr, peer, sizeof(peer));

        char line[512];
        if (tcp_read_line(fd, line, (int)sizeof(line)) <= 0) {
            close(fd); continue;
        }

        uint64_t slave_seq   = 0;
        char  slave_replid[INCR_REPL_ID_LEN + 1] = {0};
        int   is_sync   = 0;
        int   has_replid = 0;

        if (strncmp(line, INCR_SYNC_CMD, strlen(INCR_SYNC_CMD)) == 0) {
            is_sync = 1;
            const char *p = line + strlen(INCR_SYNC_CMD);
            if (*p == ' ') p++;
            const char *after = strchr(p, ' ');
            if (after) {
                size_t tlen = (size_t)(after - p);
                int is_num = 1;
                for (size_t k = 0; k < tlen; k++)
                    if (p[k] < '0' || p[k] > '9') { is_num = 0; break; }
                if (!is_num && tlen <= INCR_REPL_ID_LEN) {
                    memcpy(slave_replid, p, tlen);
                    has_replid = 1;
                    p = after + 1;
                }
            }
            slave_seq = strtoull(p, NULL, 10);
        }

        /* Replid mismatch → full resync */
        if (has_replid && strcmp(slave_replid, g_replid) != 0) {
            char resp[256];
            int rlen = snprintf(resp, sizeof(resp),
                                INCR_FULLRESYNC " %s\r\n", g_replid);
            send(fd, resp, (size_t)rlen, MSG_NOSIGNAL);
            close(fd); continue;
        }

        /* Backlog eviction check → full resync */
        if (is_sync && slave_seq > 0) {
            pthread_mutex_lock(&g_backlog_mu);
            int in_backlog = backlog_has_seq_locked(slave_seq);
            pthread_mutex_unlock(&g_backlog_mu);
            if (!in_backlog) {
                char resp[256];
                int rlen = snprintf(resp, sizeof(resp),
                                    INCR_FULLRESYNC " %s\r\n", g_replid);
                send(fd, resp, (size_t)rlen, MSG_NOSIGNAL);
                close(fd); continue;
            }
        }

        /* Send +CONTINUE */
        if (is_sync) {
            char resp[256];
            int rlen = snprintf(resp, sizeof(resp),
                                INCR_CONTINUE " %s\r\n", g_replid);
            if (send(fd, resp, (size_t)rlen, MSG_NOSIGNAL) != rlen) {
                close(fd); continue;
            }
        }

        if (ebpf_incr_add_slave(fd, slave_seq) != 0) {
            close(fd); continue;
        }

        printf("[WAL-REPL] slave %s registered (seq=%" PRIu64 ")\n",
               peer, slave_seq);

        if (is_sync &&
            g_fullsync_status == FULLSYNC_CATCHUP &&
            slave_seq == g_fullsync_base_seq)
            ebpf_incr_fullsync_finish();
    }

    printf("[WAL-REPL] TCP incr listener stopped\n");
    return NULL;
}

/* ======================================================================
 * Public API — same names as ebpf_incr_repl for zero-diff on callers
 * ====================================================================== */

int ebpf_incr_master_start(void)
{
    if (g_running) return 0;

    /* Initialize journal ring eventfd */
    g_jevt = eventfd(0, EFD_NONBLOCK);
    if (g_jevt < 0) {
        fprintf(stderr, "[WAL-REPL] eventfd: %s\n", strerror(errno));
        return -1;
    }

    /* Open WAL file */
    wal_open();   /* non-fatal — replication works without WAL file */
    if (io_uring_queue_init(DISPATCH_URING_DEPTH, &g_dispatch_ring, 0) == 0) {
        g_dispatch_uring_ok = 1;
    } else {
        fprintf(stderr, "[WAL-REPL] io_uring_queue_init dispatch failed\n");
    }

    /* Init module state */
    g_seq             = 0;
    g_stream_healthy  = 1;
    g_fullsync_status = FULLSYNC_IDLE;
    g_fullsync_base_seq = 0;
    g_fanout_fn       = NULL;
    g_backlog_start   = 0;
    g_backlog_count   = 0;
    memset(g_backlog, 0, sizeof(g_backlog));

    snprintf(g_replid, sizeof(g_replid), "%d:%lx",
             getpid(), (unsigned long)time(NULL));

    for (int i = 0; i < INCR_MAX_SLAVES; i++) {
        g_slaves[i].fd     = -1;
        g_slaves[i].active = 0;
    }

    /* Start consumer thread */
    g_running     = 1;
    g_tid_started = 0;
    if (pthread_create(&g_tid, NULL, consumer_thread, NULL) != 0) {
        fprintf(stderr, "[WAL-REPL] pthread_create consumer: %s\n",
                strerror(errno));
        goto err_jevt;
    }
    g_tid_started = 1;

    /* Start TCP incr listener */
    g_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_listen_fd < 0) goto err_tid;
    int opt = 1;
    setsockopt(g_listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in laddr;
    memset(&laddr, 0, sizeof(laddr));
    laddr.sin_family      = AF_INET;
    laddr.sin_addr.s_addr = htonl(INADDR_ANY);
    laddr.sin_port        = htons((uint16_t)kvs_config.incr_port);
    if (bind(g_listen_fd, (struct sockaddr *)&laddr, sizeof(laddr)) < 0) {
        fprintf(stderr, "[WAL-REPL] bind port %d: %s\n",
                kvs_config.incr_port, strerror(errno));
        close(g_listen_fd); g_listen_fd = -1;
        goto err_tid;
    }
    listen(g_listen_fd, 8);

    g_listen_started = 0;
    if (pthread_create(&g_listen_tid, NULL, tcp_listener_thread, NULL) != 0) {
        fprintf(stderr, "[WAL-REPL] pthread_create listener: %s\n",
                strerror(errno));
        close(g_listen_fd); g_listen_fd = -1;
        goto err_tid;
    }
    g_listen_started = 1;

    printf("[WAL-REPL] ready — WAL + io_uring batch dispatch, "
           "backlog=%d slots, replid=%s\n",
           INCR_BACKLOG_CAP, g_replid);
    return 0;

err_tid:
    g_running = 0;
    uint64_t one = 1;
    ssize_t wr = write(g_jevt, &one, sizeof(one));
    (void)wr;
    if (g_tid_started) { pthread_join(g_tid, NULL); g_tid_started = 0; }
err_jevt:
    close(g_jevt); g_jevt = -1;
    if (g_wal_fd >= 0) { close(g_wal_fd); g_wal_fd = -1; }
    if (g_dispatch_uring_ok) { io_uring_queue_exit(&g_dispatch_ring); g_dispatch_uring_ok = 0; }
    return -1;
}

void ebpf_incr_stop(void)
{
    if (!g_running) return;
    g_running = 0;

    /* Wake consumer */
    if (g_jevt >= 0) {
        uint64_t one = 1;
        ssize_t wr = write(g_jevt, &one, sizeof(one));
        (void)wr;
    }
    /* Stop listener — shutdown forces accept() in the listener thread
     * to fail reliably on all kernels, then close the fd. */
    if (g_listen_fd >= 0) {
        shutdown(g_listen_fd, SHUT_RDWR);
        close(g_listen_fd);
        g_listen_fd = -1;
    }

    if (g_tid_started)    { pthread_join(g_tid, NULL);        g_tid_started    = 0; }
    if (g_listen_started) { pthread_join(g_listen_tid, NULL); g_listen_started = 0; }

    if (g_jevt >= 0) { close(g_jevt); g_jevt = -1; }
    if (g_wal_fd >= 0)  { close(g_wal_fd); g_wal_fd = -1; }
    if (g_dispatch_uring_ok) {
        dispatch_reap_cqes(g_dispatch_pending);
        io_uring_queue_exit(&g_dispatch_ring);
        g_dispatch_uring_ok = 0;
    }

    for (int i = 0; i < INCR_MAX_SLAVES; i++)
        slave_close(i);
    for (int i = 0; i < INCR_BACKLOG_CAP; i++) {
        free(g_backlog[i].msg); g_backlog[i].msg = NULL;
    }
    for (int i = 0; i < JOURNAL_RING_SIZE; i++) {
        free(g_jslots[i].buf); g_jslots[i].buf = NULL;
    }
}

int ebpf_incr_add_slave(int tcp_fd, uint64_t start_seq)
{
    for (int i = 0; i < INCR_MAX_SLAVES; i++) {
        if (g_slaves[i].active) continue;
        return slave_start(i, tcp_fd, start_seq);
    }
    fprintf(stderr, "[WAL-REPL] no free slave slot\n");
    return -1;
}

int ebpf_incr_master_healthy(void)
{
    return g_stream_healthy;
}

uint64_t ebpf_incr_read_global_seq(void)
{
    return atomic_load_explicit(&g_journal_seq, memory_order_relaxed);
}

const char *ebpf_incr_get_replid(void)
{
    return g_replid;
}

/* ── Fullsync coordination ── */

uint64_t ebpf_incr_fullsync_begin(void)
{
    g_fullsync_status = FULLSYNC_INPROGRESS;
    return ebpf_incr_read_global_seq();
}

void ebpf_incr_fullsync_send_done(void)
{
    if (g_fullsync_status == FULLSYNC_INPROGRESS)
        g_fullsync_status = FULLSYNC_CATCHUP;
}

void ebpf_incr_fullsync_finish(void)
{
    g_fullsync_status = FULLSYNC_IDLE;
    g_fullsync_base_seq = 0;
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

void ebpf_incr_set_fanout(incr_fanout_fn fn)
{
    g_fanout_fn = fn;
}

int ebpf_incr_is_started(void)
{
    return g_running;
}
