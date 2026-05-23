#include "wal_persist.h"
#include "persistence.h"

#include <errno.h>
#include <fcntl.h>
#include <liburing.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>
#include <time.h>
#include <stdatomic.h>

/* ── WAL batch writer configuration ── */
#define WAL_BATCH_SIZE 64           /* max records per batch */
#define WAL_FLUSH_INTERVAL_MS 10    /* flush every 10ms if not full */

/* ── WAL writer state ── */
static pthread_mutex_t g_wal_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_wal_cond = PTHREAD_COND_INITIALIZER;
static pthread_t g_wal_thread;
static int g_wal_thread_running = 0;
static int g_wal_fd = -1;
static struct io_uring g_wal_ring;
static int g_wal_ring_ready = 0;

/* ── Pending write queue ── */
typedef struct {
    kvs_journal_record_t rec;
    int valid;
} wal_pending_t;

static wal_pending_t g_wal_pending[WAL_BATCH_SIZE];
static int g_wal_pending_count = 0;
static _Atomic uint64_t g_wal_last_synced_seq = 0;

/* ── fsync policy ── */
typedef enum {
    WAL_FSYNC_ALWAYS,   /* fsync after every batch (safest, slowest) */
    WAL_FSYNC_EVERYSEC, /* fsync every second (balanced) */
    WAL_FSYNC_NO        /* never fsync, rely on OS (fastest, least safe) */
} wal_fsync_policy_t;

static wal_fsync_policy_t g_wal_fsync_policy = WAL_FSYNC_EVERYSEC;
static time_t g_wal_last_fsync_time = 0;

static int wal_open_locked(void)
{
    if (g_wal_fd >= 0) return 0;
    const char *path = persist_path(WAL_REL);
    g_wal_fd = open(path, O_CREAT | O_APPEND | O_WRONLY | O_CLOEXEC, 0644);
    if (g_wal_fd < 0) {
        fprintf(stderr, "[WAL-PERSIST] open %s failed: %s\n", path, strerror(errno));
        return -1;
    }
    printf("[WAL-PERSIST] WAL file opened: %s\n", path);
    return 0;
}

static int wal_ring_init_locked(void)
{
    if (g_wal_ring_ready) return 0;
    int rc = io_uring_queue_init(128, &g_wal_ring, 0);
    if (rc < 0) {
        fprintf(stderr, "[WAL-PERSIST] io_uring_queue_init failed: %s\n", strerror(-rc));
        return -1;
    }
    g_wal_ring_ready = 1;
    return 0;
}

/* Batch write pending records using io_uring writev */
static int wal_flush_batch_locked(void)
{
    if (g_wal_pending_count == 0) return 0;
    if (!g_wal_ring_ready || g_wal_fd < 0) return -1;

    /* Prepare iovec array: each record = header + payload */
    struct iovec *iovs = malloc(sizeof(struct iovec) * g_wal_pending_count * 2);
    if (!iovs) return -1;

    kvs_wal2_record_hdr_t *hdrs = malloc(sizeof(kvs_wal2_record_hdr_t) * g_wal_pending_count);
    if (!hdrs) {
        free(iovs);
        return -1;
    }

    int iov_idx = 0;
    size_t total_bytes = 0;
    for (int i = 0; i < g_wal_pending_count; i++) {
        if (!g_wal_pending[i].valid) continue;
        kvs_journal_record_t *rec = &g_wal_pending[i].rec;

        hdrs[i].magic = KVS_WAL2_MAGIC;
        hdrs[i].version = KVS_WAL2_VERSION;
        hdrs[i].kind = rec->kind;
        hdrs[i].flags = rec->flags;
        hdrs[i].seq = rec->seq;
        hdrs[i].len = rec->len;
        hdrs[i].crc32 = rec->crc32;

        iovs[iov_idx].iov_base = &hdrs[i];
        iovs[iov_idx].iov_len = sizeof(kvs_wal2_record_hdr_t);
        total_bytes += sizeof(kvs_wal2_record_hdr_t);
        iov_idx++;

        iovs[iov_idx].iov_base = rec->payload;
        iovs[iov_idx].iov_len = rec->len;
        total_bytes += rec->len;
        iov_idx++;
    }

    /* Submit batch write */
    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_wal_ring);
    if (!sqe) {
        free(iovs);
        free(hdrs);
        return -1;
    }

    io_uring_prep_writev(sqe, g_wal_fd, iovs, iov_idx, -1);
    sqe->user_data = (uint64_t)iovs;

    int rc = io_uring_submit(&g_wal_ring);
    if (rc < 0) {
        fprintf(stderr, "[WAL-PERSIST] io_uring_submit failed: %s\n", strerror(-rc));
        free(iovs);
        free(hdrs);
        return -1;
    }

    /* Wait for completion */
    struct io_uring_cqe *cqe = NULL;
    rc = io_uring_wait_cqe(&g_wal_ring, &cqe);
    if (rc < 0) {
        fprintf(stderr, "[WAL-PERSIST] wait_cqe failed: %s\n", strerror(-rc));
        free(iovs);
        free(hdrs);
        return -1;
    }

    int written = cqe->res;
    io_uring_cqe_seen(&g_wal_ring, cqe);

    free(iovs);
    free(hdrs);

    if (written < 0) {
        fprintf(stderr, "[WAL-PERSIST] batch write failed: %s\n", strerror(-written));
        return -1;
    }

    if ((size_t)written != total_bytes) {
        fprintf(stderr, "[WAL-PERSIST] short write: %d/%zu bytes\n", written, total_bytes);
        return -1;
    }

    /* Update last synced seq */
    uint64_t max_seq = 0;
    for (int i = 0; i < g_wal_pending_count; i++) {
        if (g_wal_pending[i].valid && g_wal_pending[i].rec.seq > max_seq) {
            max_seq = g_wal_pending[i].rec.seq;
        }
    }
    atomic_store_explicit(&g_wal_last_synced_seq, max_seq, memory_order_release);

    /* Free payloads and clear pending */
    for (int i = 0; i < g_wal_pending_count; i++) {
        if (g_wal_pending[i].valid) {
            free(g_wal_pending[i].rec.payload);
            g_wal_pending[i].valid = 0;
        }
    }
    g_wal_pending_count = 0;

    return 0;
}

/* Perform fsync based on policy */
static int wal_fsync_if_needed_locked(int force)
{
    if (g_wal_fsync_policy == WAL_FSYNC_NO && !force) return 0;
    if (g_wal_fd < 0 || !g_wal_ring_ready) return -1;

    time_t now = time(NULL);
    if (g_wal_fsync_policy == WAL_FSYNC_ALWAYS || force ||
        (g_wal_fsync_policy == WAL_FSYNC_EVERYSEC && now > g_wal_last_fsync_time)) {

        struct io_uring_sqe *sqe = io_uring_get_sqe(&g_wal_ring);
        if (sqe) {
            io_uring_prep_fsync(sqe, g_wal_fd, IORING_FSYNC_DATASYNC);
            if (io_uring_submit(&g_wal_ring) >= 0) {
                struct io_uring_cqe *cqe = NULL;
                if (io_uring_wait_cqe(&g_wal_ring, &cqe) == 0) {
                    io_uring_cqe_seen(&g_wal_ring, cqe);
                    g_wal_last_fsync_time = now;
                    return 0;
                }
            }
        }
        /* Fallback to blocking fsync */
        if (fdatasync(g_wal_fd) == 0) {
            g_wal_last_fsync_time = now;
            return 0;
        }
        return -1;
    }
    return 0;
}

/* WAL consumer thread - batches writes and handles fsync */
static void *wal_consumer_thread(void *arg)
{
    (void)arg;
    struct timespec ts;

    while (g_wal_thread_running) {
        pthread_mutex_lock(&g_wal_mu);

        /* Wait for data or timeout */
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += WAL_FLUSH_INTERVAL_MS * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec += 1;
            ts.tv_nsec -= 1000000000L;
        }

        int wait_result = 0;
        while (g_wal_pending_count == 0 && g_wal_thread_running && wait_result == 0) {
            wait_result = pthread_cond_timedwait(&g_wal_cond, &g_wal_mu, &ts);
        }

        /* Flush batch if we have data */
        if (g_wal_pending_count > 0) {
            wal_flush_batch_locked();
            wal_fsync_if_needed_locked(0);
        }

        pthread_mutex_unlock(&g_wal_mu);
    }

    /* Final flush on shutdown */
    pthread_mutex_lock(&g_wal_mu);
    if (g_wal_pending_count > 0) {
        wal_flush_batch_locked();
        wal_fsync_if_needed_locked(1);
    }
    pthread_mutex_unlock(&g_wal_mu);

    return NULL;
}

int kvs_wal_persist_start(void)
{
    pthread_mutex_lock(&g_wal_mu);
    int rc = wal_open_locked();
    if (rc == 0) rc = wal_ring_init_locked();

    if (rc == 0 && !g_wal_thread_running) {
        g_wal_thread_running = 1;
        memset(g_wal_pending, 0, sizeof(g_wal_pending));
        g_wal_pending_count = 0;
        g_wal_last_fsync_time = time(NULL);

        rc = pthread_create(&g_wal_thread, NULL, wal_consumer_thread, NULL);
        if (rc != 0) {
            fprintf(stderr, "[WAL-PERSIST] pthread_create failed: %s\n", strerror(rc));
            g_wal_thread_running = 0;
            pthread_mutex_unlock(&g_wal_mu);
            return -1;
        }
        printf("[WAL-PERSIST] batch consumer thread started\n");
    }

    pthread_mutex_unlock(&g_wal_mu);
    return rc;
}

int kvs_wal_persist_append(const kvs_journal_record_t *rec)
{
    if (!rec || !rec->payload || rec->len == 0) return -1;

    pthread_mutex_lock(&g_wal_mu);

    /* Check if batch is full, flush if needed */
    if (g_wal_pending_count >= WAL_BATCH_SIZE) {
        wal_flush_batch_locked();
        wal_fsync_if_needed_locked(0);
    }

    /* Copy record to pending queue */
    wal_pending_t *slot = &g_wal_pending[g_wal_pending_count];
    slot->rec.seq = rec->seq;
    slot->rec.kind = rec->kind;
    slot->rec.flags = rec->flags;
    slot->rec.len = rec->len;
    slot->rec.crc32 = rec->crc32;
    slot->rec.payload = malloc(rec->len);
    if (!slot->rec.payload) {
        pthread_mutex_unlock(&g_wal_mu);
        return -1;
    }
    memcpy(slot->rec.payload, rec->payload, rec->len);
    slot->valid = 1;
    g_wal_pending_count++;

    /* Signal consumer thread */
    pthread_cond_signal(&g_wal_cond);

    pthread_mutex_unlock(&g_wal_mu);
    return 0;
}

void kvs_wal_persist_stop(void)
{
    /* Stop consumer thread */
    if (g_wal_thread_running) {
        pthread_mutex_lock(&g_wal_mu);
        g_wal_thread_running = 0;
        pthread_cond_signal(&g_wal_cond);
        pthread_mutex_unlock(&g_wal_mu);

        pthread_join(g_wal_thread, NULL);
        printf("[WAL-PERSIST] consumer thread stopped\n");
    }

    pthread_mutex_lock(&g_wal_mu);

    /* Close file and cleanup */
    if (g_wal_fd >= 0) {
        if (g_wal_ring_ready) {
            struct io_uring_sqe *sqe = io_uring_get_sqe(&g_wal_ring);
            if (sqe) {
                io_uring_prep_fsync(sqe, g_wal_fd, IORING_FSYNC_DATASYNC);
                if (io_uring_submit(&g_wal_ring) >= 0) {
                    struct io_uring_cqe *cqe = NULL;
                    if (io_uring_wait_cqe(&g_wal_ring, &cqe) == 0) {
                        io_uring_cqe_seen(&g_wal_ring, cqe);
                    }
                }
            }
        } else {
            fdatasync(g_wal_fd);
        }
        close(g_wal_fd);
        g_wal_fd = -1;
    }

    if (g_wal_ring_ready) {
        io_uring_queue_exit(&g_wal_ring);
        g_wal_ring_ready = 0;
    }

    pthread_mutex_unlock(&g_wal_mu);
}

/* Set fsync policy (call before kvs_wal_persist_start) */
void kvs_wal_set_fsync_policy(const char *policy)
{
    if (!policy) return;

    if (strcmp(policy, "always") == 0) {
        g_wal_fsync_policy = WAL_FSYNC_ALWAYS;
        printf("[WAL-PERSIST] fsync policy: always\n");
    } else if (strcmp(policy, "everysec") == 0) {
        g_wal_fsync_policy = WAL_FSYNC_EVERYSEC;
        printf("[WAL-PERSIST] fsync policy: everysec\n");
    } else if (strcmp(policy, "no") == 0) {
        g_wal_fsync_policy = WAL_FSYNC_NO;
        printf("[WAL-PERSIST] fsync policy: no\n");
    } else {
        fprintf(stderr, "[WAL-PERSIST] unknown fsync policy '%s', using everysec\n", policy);
        g_wal_fsync_policy = WAL_FSYNC_EVERYSEC;
    }
}

/* Get last synced sequence number */
uint64_t kvs_wal_last_synced_seq(void)
{
    return atomic_load_explicit(&g_wal_last_synced_seq, memory_order_acquire);
}
