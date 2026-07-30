/* =========================================================
 * wal_persist.c — WAL 增量持久化（后台线程 + io_uring 落盘）
 *
 * 主线程通过 kvs_wal_persist_append_raw 入队，
 * 后台消费者线程批量 io_uring 写入 + fsync。
 * ========================================================= */

#include "wal_persist.h"
#include "persistence.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <stdatomic.h>
#include <liburing.h>

/* ── Configuration ── */
#define WAL_BATCH_MAX          1024
#define WAL_FLUSH_INTERVAL_MS  1
#define WAL_URING_DEPTH        64
#define WAL_WRITE_BUF_INIT     (256 * 1024)

/* ── Pending record (heap-allocated, payload follows header) ── */
typedef struct {
    uint64_t seq;
    uint16_t kind;
    uint16_t flags;
    uint32_t len;
    uint32_t crc32;
    char     payload[];
} wal_pending_t;

/* ── State ── */
static pthread_mutex_t g_wal_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_wal_cond = PTHREAD_COND_INITIALIZER;
static pthread_t       g_wal_thread;
static int             g_wal_running = 0;
static int             g_wal_fd = -1;

/* Pending queue: array of pointers, protected by g_wal_mu */
static wal_pending_t *g_queue[WAL_BATCH_MAX];
static int             g_queue_count = 0;

/* io_uring */
static struct io_uring g_wal_ring;
static int             g_wal_ring_ok = 0;

/* Flatten buffer for io_uring writes */
static char  *g_write_buf = NULL;
static size_t g_write_buf_cap = 0;

/* fsync policy */
static int   g_fsync_always = 0;
static time_t g_last_fsync = 0;

/* ── Helpers ── */

static int wal_open(void) {
    if (g_wal_fd >= 0) return 0;
    const char *path = persist_path(WAL_REL);
    g_wal_fd = open(path, O_CREAT | O_APPEND | O_WRONLY | O_CLOEXEC, 0644);
    if (g_wal_fd < 0) {
        fprintf(stderr, "[WAL] open %s: %s\n", path, strerror(errno));
        return -1;
    }
    printf("[WAL] file opened: %s\n", path);
    return 0;
}

static void wal_fsync_if_needed(int force) {
    if (g_wal_fd < 0) return;

    time_t now = time(NULL);
    if (!force && !g_fsync_always && now <= g_last_fsync)
        return;

    if (g_wal_ring_ok) {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&g_wal_ring);
        if (sqe) {
            io_uring_prep_fsync(sqe, g_wal_fd, IORING_FSYNC_DATASYNC);
            int ret = io_uring_submit(&g_wal_ring);
            if (ret >= 0) {
                struct io_uring_cqe *cqe = NULL;
                io_uring_wait_cqe(&g_wal_ring, &cqe);
                if (cqe) {
                    if (cqe->res < 0)
                        fprintf(stderr, "[WAL] fsync: %s\n", strerror(-cqe->res));
                    io_uring_cqe_seen(&g_wal_ring, cqe);
                }
            }
        }
    } else {
        fdatasync(g_wal_fd);
    }
    g_last_fsync = now;
}

/* Write one batch via io_uring.
 * Flattens hdr+payload for each record into g_write_buf, submits one write SQE. */
static int wal_flush_batch(wal_pending_t **batch, int n) {
    if (n <= 0 || g_wal_fd < 0) return 0;

    /* Calculate total bytes */
    size_t total = 0;
    for (int i = 0; i < n; i++)
        total += sizeof(kvs_wal2_record_hdr_t) + batch[i]->len;

    /* Grow write buffer if needed */
    if (total > g_write_buf_cap) {
        size_t nc = total + (total >> 2);
        char *nb = realloc(g_write_buf, nc);
        if (!nb) return -1;
        g_write_buf = nb;
        g_write_buf_cap = nc;
    }

    /* Flatten */
    char *p = g_write_buf;
    for (int i = 0; i < n; i++) {
        wal_pending_t *r = batch[i];
        kvs_wal2_record_hdr_t hdr;
        hdr.magic    = KVS_WAL2_MAGIC;
        hdr.version  = KVS_WAL2_VERSION;
        hdr.kind     = r->kind;
        hdr.flags    = r->flags;
        hdr.seq      = r->seq;
        hdr.len      = r->len;
        hdr.crc32    = r->crc32;
        hdr.reserved = 0;
        memcpy(p, &hdr, sizeof(hdr));
        p += sizeof(hdr);
        memcpy(p, r->payload, r->len);
        p += r->len;
    }

    /* Submit via io_uring or fallback to write() */
    if (g_wal_ring_ok) {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&g_wal_ring);
        if (sqe) {
            io_uring_prep_write(sqe, g_wal_fd, g_write_buf, total, -1);
            int ret = io_uring_submit(&g_wal_ring);
            if (ret >= 0) {
                struct io_uring_cqe *cqe = NULL;
                io_uring_wait_cqe(&g_wal_ring, &cqe);
                if (cqe) {
                    if (cqe->res < 0) {
                        fprintf(stderr, "[WAL] write: %s\n", strerror(-cqe->res));
                        io_uring_cqe_seen(&g_wal_ring, cqe);
                        return -1;
                    }
                    io_uring_cqe_seen(&g_wal_ring, cqe);
                }
            }
        } else {
            /* ring full, fallback */
            ssize_t w = write(g_wal_fd, g_write_buf, total);
            if (w < 0) return -1;
        }
    } else {
        ssize_t w = write(g_wal_fd, g_write_buf, total);
        if (w < 0) return -1;
    }

    return 0;
}

/* ── Consumer thread ── */

static void *wal_consumer(void *arg) {
    (void)arg;

    while (g_wal_running) {
        pthread_mutex_lock(&g_wal_mu);

        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += WAL_FLUSH_INTERVAL_MS * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }

        while (g_queue_count == 0 && g_wal_running)
            pthread_cond_timedwait(&g_wal_cond, &g_wal_mu, &ts);

        if (!g_wal_running && g_queue_count == 0) {
            pthread_mutex_unlock(&g_wal_mu);
            break;
        }

        /* Detach all pending */
        int n = g_queue_count;
        wal_pending_t *batch[WAL_BATCH_MAX];
        memcpy(batch, g_queue, (size_t)n * sizeof(wal_pending_t *));
        g_queue_count = 0;

        pthread_mutex_unlock(&g_wal_mu);

        if (n > 0) {
            wal_flush_batch(batch, n);
            wal_fsync_if_needed(0);
            for (int i = 0; i < n; i++) free(batch[i]);
        }
    }
    return NULL;
}

/* ── Public API ── */

int kvs_wal_persist_start(void) {
    pthread_mutex_lock(&g_wal_mu);

    if (wal_open() != 0) {
        pthread_mutex_unlock(&g_wal_mu);
        return -1;
    }

    /* Init io_uring */
    if (!g_wal_ring_ok) {
        int rc = io_uring_queue_init(WAL_URING_DEPTH, &g_wal_ring, 0);
        if (rc < 0) {
            fprintf(stderr, "[WAL] io_uring init: %s, using write() fallback\n",
                    strerror(-rc));
        } else {
            g_wal_ring_ok = 1;
            printf("[WAL] io_uring ready (depth=%d)\n", WAL_URING_DEPTH);
        }
    }

    /* Alloc initial write buffer */
    if (!g_write_buf) {
        g_write_buf = malloc(WAL_WRITE_BUF_INIT);
        g_write_buf_cap = g_write_buf ? WAL_WRITE_BUF_INIT : 0;
    }

    g_wal_running = 1;
    g_queue_count = 0;
    g_last_fsync = time(NULL);

    int rc = pthread_create(&g_wal_thread, NULL, wal_consumer, NULL);
    if (rc != 0) {
        fprintf(stderr, "[WAL] thread create: %s\n", strerror(rc));
        g_wal_running = 0;
        pthread_mutex_unlock(&g_wal_mu);
        return -1;
    }

    pthread_mutex_unlock(&g_wal_mu);
    printf("[WAL] consumer thread started (batch=%d)\n", WAL_BATCH_MAX);
    return 0;
}

int kvs_wal_persist_append_raw(uint64_t seq, uint16_t kind, uint16_t flags,
                                const char *payload, uint32_t len, uint32_t crc) {
    if (!payload || len == 0 || len > KVS_JOURNAL_MAX_PAYLOAD) return -1;

    wal_pending_t *r = malloc(sizeof(wal_pending_t) + len);
    if (!r) return -1;
    r->seq   = seq;
    r->kind  = kind;
    r->flags = flags;
    r->len   = len;
    r->crc32 = crc;
    memcpy(r->payload, payload, len);

    pthread_mutex_lock(&g_wal_mu);
    if (g_queue_count < WAL_BATCH_MAX) {
        g_queue[g_queue_count++] = r;
        r = NULL; /* ownership transferred */
    }
    /* If queue full, signal consumer to drain; retry next call */
    if (g_queue_count >= (WAL_BATCH_MAX >> 1))
        pthread_cond_signal(&g_wal_cond);
    pthread_mutex_unlock(&g_wal_mu);

    free(r); /* NULL if queued, else dropped (should not happen) */
    return 0;
}

void kvs_wal_persist_stop(void) {
    if (g_wal_running) {
        pthread_mutex_lock(&g_wal_mu);
        g_wal_running = 0;
        pthread_cond_signal(&g_wal_cond);
        pthread_mutex_unlock(&g_wal_mu);
        pthread_join(g_wal_thread, NULL);
        printf("[WAL] consumer stopped\n");
    }

    /* Drain remaining */
    pthread_mutex_lock(&g_wal_mu);
    if (g_queue_count > 0) {
        wal_flush_batch(g_queue, g_queue_count);
        for (int i = 0; i < g_queue_count; i++) free(g_queue[i]);
        g_queue_count = 0;
    }
    wal_fsync_if_needed(1);

    if (g_wal_fd >= 0) {
        close(g_wal_fd);
        g_wal_fd = -1;
    }

    if (g_wal_ring_ok) {
        io_uring_queue_exit(&g_wal_ring);
        g_wal_ring_ok = 0;
    }

    free(g_write_buf);
    g_write_buf = NULL;
    g_write_buf_cap = 0;

    pthread_mutex_unlock(&g_wal_mu);
}

void kvs_wal_set_fsync_policy(const char *policy) {
    if (!policy) return;
    if (strcmp(policy, "always") == 0) {
        g_fsync_always = 1;
        printf("[WAL] fsync: always\n");
    } else if (strcmp(policy, "everysec") == 0) {
        g_fsync_always = 0;
        printf("[WAL] fsync: everysec\n");
    } else {
        g_fsync_always = 0;
        printf("[WAL] fsync: no\n");
    }
}
