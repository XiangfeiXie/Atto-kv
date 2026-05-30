#include "wal_persist.h"
#include "persistence.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>
#include <unistd.h>
#include <time.h>
#include <stdatomic.h>
#include <liburing.h>

/* ── WAL batch writer configuration ── */
#define WAL_BATCH_SIZE 8192         /* max records per buffer */
#define WAL_FLUSH_INTERVAL_MS 1     /* flush every 1ms (reduced from 10ms) */
#define WAL_INLINE_PAYLOAD 128      /* payloads <= this are stored inline */
#define WAL_URING_DEPTH 256         /* io_uring queue depth */
#define WAL_URING_CHUNK_SIZE (1024 * 1024)  /* 1MB per SQE */

/* ── WAL writer state ── */
static pthread_mutex_t g_wal_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_wal_cond = PTHREAD_COND_INITIALIZER;
static pthread_t g_wal_thread;
static int g_wal_thread_running = 0;
static int g_wal_fd = -1;
static _Atomic uint64_t g_wal_last_synced_seq = 0;
static struct io_uring g_wal_ring;
static int g_wal_ring_initialized = 0;

/* ── Pending record with inline payload for small records ── */
typedef struct {
    uint64_t seq;
    uint16_t kind;
    uint16_t flags;
    uint32_t len;
    uint32_t crc32;
    int      valid;
    char    *payload;                    /* &inline_payload[0] or heap */
    char     inline_payload[WAL_INLINE_PAYLOAD];
} wal_pending_t;

/* ── Double-buffer pending write queue ── */
static wal_pending_t g_wal_buf[2][WAL_BATCH_SIZE];
static int g_wal_buf_count[2];
static int g_wal_active;  /* which buffer producers write to (0 or 1) */

/* ── Pre-allocated flush buffers (consumer thread only, no lock needed) ── */
static struct iovec g_wal_iovs[WAL_BATCH_SIZE * 2];
static kvs_wal2_record_hdr_t g_wal_hdrs[WAL_BATCH_SIZE];

/* ── fsync policy ── */
typedef enum {
    WAL_FSYNC_ALWAYS,
    WAL_FSYNC_EVERYSEC,
    WAL_FSYNC_NO
} wal_fsync_policy_t;

static wal_fsync_policy_t g_wal_fsync_policy = WAL_FSYNC_EVERYSEC;
static time_t g_wal_last_fsync_time;

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

/* io_uring-based fsync: submit fsync SQE and wait for completion */
static void wal_fsync_if_needed(int force)
{
    if (g_wal_fsync_policy == WAL_FSYNC_NO && !force) return;
    if (g_wal_fd < 0) return;
    if (!g_wal_ring_initialized) {
        fdatasync(g_wal_fd);
        return;
    }

    time_t now = time(NULL);
    if (g_wal_fsync_policy == WAL_FSYNC_ALWAYS || force ||
        (g_wal_fsync_policy == WAL_FSYNC_EVERYSEC && now > g_wal_last_fsync_time)) {

        struct io_uring_sqe *sqe = io_uring_get_sqe(&g_wal_ring);
        if (sqe) {
            io_uring_prep_fsync(sqe, g_wal_fd, IORING_FSYNC_DATASYNC);
            int ret = io_uring_submit(&g_wal_ring);
            if (ret < 0) {
                fprintf(stderr, "[WAL-PERSIST] fsync submit failed: %s\n", strerror(-ret));
                fdatasync(g_wal_fd);
                g_wal_last_fsync_time = now;
                return;
            }

            struct io_uring_cqe *cqe = NULL;
            ret = io_uring_wait_cqe(&g_wal_ring, &cqe);
            if (ret < 0) {
                fprintf(stderr, "[WAL-PERSIST] fsync wait failed: %s\n", strerror(-ret));
            } else if (cqe) {
                if (cqe->res < 0) {
                    fprintf(stderr, "[WAL-PERSIST] fsync failed: %s\n", strerror(-cqe->res));
                }
                io_uring_cqe_seen(&g_wal_ring, cqe);
            }
        } else {
            fdatasync(g_wal_fd);
        }
        g_wal_last_fsync_time = now;
    }
}

/* io_uring-based flush: batch submit write SQEs, then reap CQEs
 *
 * Fix: Use a temporary buffer to flatten iovec arrays before writing.
 * This avoids the "Invalid argument" error that occurs when using
 * io_uring writev with offset=-1 (O_APPEND mode) and large iovec arrays.
 * The kernel has limits on iovec count (typically UIO_MAXIOV=1024), and
 * combining writev with O_APPEND via io_uring can trigger edge cases.
 */
static int wal_flush_records(wal_pending_t *records, int count)
{
    if (count == 0) return 0;
    if (g_wal_fd < 0) return -1;
    if (!g_wal_ring_initialized) return -1;

    /* Step 1: Prepare iovec array for all records */
    int iov_idx = 0;
    for (int i = 0; i < count; i++) {
        if (!records[i].valid) continue;

        g_wal_hdrs[i].magic    = KVS_WAL2_MAGIC;
        g_wal_hdrs[i].version  = KVS_WAL2_VERSION;
        g_wal_hdrs[i].kind     = records[i].kind;
        g_wal_hdrs[i].flags    = records[i].flags;
        g_wal_hdrs[i].seq      = records[i].seq;
        g_wal_hdrs[i].len      = records[i].len;
        g_wal_hdrs[i].crc32    = records[i].crc32;
        g_wal_hdrs[i].reserved = 0;

        g_wal_iovs[iov_idx].iov_base = &g_wal_hdrs[i];
        g_wal_iovs[iov_idx].iov_len  = sizeof(kvs_wal2_record_hdr_t);
        iov_idx++;

        g_wal_iovs[iov_idx].iov_base = records[i].payload;
        g_wal_iovs[iov_idx].iov_len  = records[i].len;
        iov_idx++;
    }

    /* Step 2: Flatten iovecs into a contiguous buffer and submit via io_uring.
     * This avoids kernel limitations with writev + O_APPEND + io_uring.
     * We allocate a temporary buffer, copy all iovec data into it, then
     * submit a single write operation per chunk. */
    static __thread char *write_buf = NULL;
    static __thread size_t write_buf_cap = 0;

    int inflight = 0;
    int iov_pos = 0;

    while (iov_pos < iov_idx || inflight > 0) {
        /* Submit as many SQEs as possible */
        while (iov_pos < iov_idx && inflight < WAL_URING_DEPTH) {
            struct io_uring_sqe *sqe = io_uring_get_sqe(&g_wal_ring);
            if (!sqe) break;

            /* Calculate how many iovecs fit in this chunk */
            size_t chunk_bytes = 0;
            int chunk_iovs = 0;
            while (iov_pos + chunk_iovs < iov_idx &&
                   chunk_bytes + g_wal_iovs[iov_pos + chunk_iovs].iov_len <= WAL_URING_CHUNK_SIZE) {
                chunk_bytes += g_wal_iovs[iov_pos + chunk_iovs].iov_len;
                chunk_iovs++;
            }

            if (chunk_iovs == 0) {
                /* Single iovec exceeds chunk size, submit it alone */
                chunk_iovs = 1;
                chunk_bytes = g_wal_iovs[iov_pos].iov_len;
            }

            /* Ensure write buffer is large enough */
            if (chunk_bytes > write_buf_cap) {
                size_t new_cap = chunk_bytes + (chunk_bytes >> 2);  /* +25% headroom */
                char *new_buf = (char *)realloc(write_buf, new_cap);
                if (!new_buf) {
                    fprintf(stderr, "[WAL-PERSIST] failed to allocate write buffer (%zu bytes)\n", new_cap);
                    return -1;
                }
                write_buf = new_buf;
                write_buf_cap = new_cap;
            }

            /* Flatten iovecs into contiguous buffer */
            char *p = write_buf;
            for (int j = 0; j < chunk_iovs; j++) {
                memcpy(p, g_wal_iovs[iov_pos + j].iov_base, g_wal_iovs[iov_pos + j].iov_len);
                p += g_wal_iovs[iov_pos + j].iov_len;
            }

            /* Submit single write operation with flattened buffer */
            io_uring_prep_write(sqe, g_wal_fd, write_buf, chunk_bytes, -1);
            sqe->user_data = chunk_bytes;  /* Store bytes for verification */

            iov_pos += chunk_iovs;
            inflight++;
        }

        if (inflight > 0) {
            int submitted = io_uring_submit(&g_wal_ring);
            if (submitted < 0) {
                fprintf(stderr, "[WAL-PERSIST] io_uring_submit failed: %s\n", strerror(-submitted));
                return -1;
            }

            /* Reap completed CQEs */
            while (inflight > 0) {
                struct io_uring_cqe *cqe = NULL;
                int ret = io_uring_peek_cqe(&g_wal_ring, &cqe);

                if (ret == -EAGAIN) {
                    /* No more completed CQEs, wait for at least one */
                    ret = io_uring_wait_cqe(&g_wal_ring, &cqe);
                    if (ret < 0) {
                        fprintf(stderr, "[WAL-PERSIST] io_uring_wait_cqe failed: %s\n", strerror(-ret));
                        return -1;
                    }
                }

                if (cqe) {
                    if (cqe->res < 0) {
                        fprintf(stderr, "[WAL-PERSIST] write failed: %s\n", strerror(-cqe->res));
                        io_uring_cqe_seen(&g_wal_ring, cqe);
                        return -1;
                    }

                    size_t expected = cqe->user_data;
                    if ((size_t)cqe->res != expected) {
                        fprintf(stderr, "[WAL-PERSIST] short write: %d/%zu bytes\n", cqe->res, expected);
                        io_uring_cqe_seen(&g_wal_ring, cqe);
                        return -1;
                    }

                    io_uring_cqe_seen(&g_wal_ring, cqe);
                    inflight--;
                } else {
                    break;
                }
            }
        }
    }

    /* Step 3: Update synced sequence and free payloads */
    uint64_t max_seq = 0;
    for (int i = 0; i < count; i++) {
        if (!records[i].valid) continue;
        if (records[i].seq > max_seq)
            max_seq = records[i].seq;
        if (records[i].payload != records[i].inline_payload)
            free(records[i].payload);
        records[i].valid = 0;
    }
    atomic_store_explicit(&g_wal_last_synced_seq, max_seq, memory_order_release);

    return 0;
}

/* WAL consumer thread — drains buffers and writes to disk.
 *
 * - On signal from producer (buffer swapped because full): flush the INACTIVE
 *   buffer that the producer just swapped out.
 * - On timeout (10 ms): flush the ACTIVE buffer in-place (no swap) so
 *   producers keep writing to it after we release the lock. */
static void *wal_consumer_thread(void *arg)
{
    (void)arg;
    struct timespec ts;

    while (g_wal_thread_running) {
        pthread_mutex_lock(&g_wal_mu);

        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += WAL_FLUSH_INTERVAL_MS * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec += 1;
            ts.tv_nsec -= 1000000000L;
        }

        int flush_idx = -1;
        int timed_out = 0;

        while (!timed_out && flush_idx < 0 && g_wal_thread_running) {
            /* Inactive buffer has data (producer swapped on full) */
            if (g_wal_buf_count[1 - g_wal_active] > 0) {
                flush_idx = 1 - g_wal_active;
                break;
            }
            /* Active buffer is full but producer hasn't swapped yet */
            if (g_wal_buf_count[g_wal_active] >= WAL_BATCH_SIZE) {
                flush_idx = g_wal_active;
                g_wal_active = 1 - g_wal_active;
                break;
            }

            int rc = pthread_cond_timedwait(&g_wal_cond, &g_wal_mu, &ts);
            if (rc == ETIMEDOUT) {
                timed_out = 1;
                if (g_wal_buf_count[g_wal_active] > 0)
                    flush_idx = g_wal_active;  /* no swap on timeout */
            }
        }

        if (!g_wal_thread_running) {
            pthread_mutex_unlock(&g_wal_mu);
            break;
        }
        if (flush_idx < 0) {
            pthread_mutex_unlock(&g_wal_mu);
            continue;
        }

        int count = g_wal_buf_count[flush_idx];
        g_wal_buf_count[flush_idx] = 0;

        wal_pending_t *copy = malloc(sizeof(wal_pending_t) * (size_t)count);
        if (copy) {
            memcpy(copy, g_wal_buf[flush_idx],
                   sizeof(wal_pending_t) * (size_t)count);
            /* Fix up inline payload pointers: after memcpy the pointer
             * still references the original buffer; redirect to the copy. */
            for (int i = 0; i < count; i++) {
                if (copy[i].len <= WAL_INLINE_PAYLOAD)
                    copy[i].payload = copy[i].inline_payload;
            }
        }

        pthread_cond_broadcast(&g_wal_cond);
        pthread_mutex_unlock(&g_wal_mu);

        if (copy) {
            wal_flush_records(copy, count);
            free(copy);
        }
        wal_fsync_if_needed(0);
    }

    /* Final flush on shutdown */
    pthread_mutex_lock(&g_wal_mu);
    for (int b = 0; b < 2; b++) {
        int n = g_wal_buf_count[b];
        if (n > 0) {
            wal_pending_t *copy = malloc(sizeof(wal_pending_t) * (size_t)n);
            if (copy) {
                memcpy(copy, g_wal_buf[b], sizeof(wal_pending_t) * (size_t)n);
                for (int i = 0; i < n; i++) {
                    if (copy[i].len <= WAL_INLINE_PAYLOAD)
                        copy[i].payload = copy[i].inline_payload;
                }
                g_wal_buf_count[b] = 0;
                pthread_mutex_unlock(&g_wal_mu);
                wal_flush_records(copy, n);
                free(copy);
                wal_fsync_if_needed(1);
                pthread_mutex_lock(&g_wal_mu);
            }
        }
    }
    pthread_mutex_unlock(&g_wal_mu);

    return NULL;
}

int kvs_wal_persist_start(void)
{
    pthread_mutex_lock(&g_wal_mu);
    int rc = wal_open_locked();

    if (rc == 0 && !g_wal_thread_running) {
        /* Initialize io_uring */
        if (!g_wal_ring_initialized) {
            rc = io_uring_queue_init(WAL_URING_DEPTH, &g_wal_ring, 0);
            if (rc < 0) {
                fprintf(stderr, "[WAL-PERSIST] io_uring_queue_init failed: %s\n", strerror(-rc));
                pthread_mutex_unlock(&g_wal_mu);
                return -1;
            }
            g_wal_ring_initialized = 1;
            printf("[WAL-PERSIST] io_uring initialized (depth=%d)\n", WAL_URING_DEPTH);
        }

        g_wal_thread_running = 1;
        memset(g_wal_buf, 0, sizeof(g_wal_buf));
        g_wal_buf_count[0] = 0;
        g_wal_buf_count[1] = 0;
        g_wal_active = 0;
        g_wal_last_fsync_time = time(NULL);

        rc = pthread_create(&g_wal_thread, NULL, wal_consumer_thread, NULL);
        if (rc != 0) {
            fprintf(stderr, "[WAL-PERSIST] pthread_create failed: %s\n",
                    strerror(rc));
            g_wal_thread_running = 0;
            pthread_mutex_unlock(&g_wal_mu);
            return -1;
        }
        printf("[WAL-PERSIST] consumer started (batch=%d, inline=%d, io_uring)\n",
               WAL_BATCH_SIZE, WAL_INLINE_PAYLOAD);
    }

    pthread_mutex_unlock(&g_wal_mu);
    return rc;
}

static int wal_append_to_active(uint64_t seq, uint16_t kind, uint16_t flags,
                                 const char *payload, uint32_t len, uint32_t crc)
{
    pthread_mutex_lock(&g_wal_mu);

    if (g_wal_buf_count[g_wal_active] >= WAL_BATCH_SIZE) {
        int other = 1 - g_wal_active;
        while (g_wal_buf_count[other] > 0)
            pthread_cond_wait(&g_wal_cond, &g_wal_mu);
        g_wal_active = other;
        pthread_cond_signal(&g_wal_cond);
    }

    wal_pending_t *slot = &g_wal_buf[g_wal_active][g_wal_buf_count[g_wal_active]];
    slot->seq   = seq;
    slot->kind  = kind;
    slot->flags = flags;
    slot->len   = len;
    slot->crc32 = crc;

    if (len <= WAL_INLINE_PAYLOAD) {
        memcpy(slot->inline_payload, payload, len);
        slot->payload = slot->inline_payload;
    } else {
        slot->payload = malloc(len);
        if (!slot->payload) {
            pthread_mutex_unlock(&g_wal_mu);
            return -1;
        }
        memcpy(slot->payload, payload, len);
    }
    slot->valid = 1;
    g_wal_buf_count[g_wal_active]++;

    pthread_mutex_unlock(&g_wal_mu);
    return 0;
}

int kvs_wal_persist_append(const kvs_journal_record_t *rec)
{
    if (!rec || !rec->payload || rec->len == 0) return -1;
    return wal_append_to_active(rec->seq, rec->kind, rec->flags,
                                 rec->payload, rec->len, rec->crc32);
}

int kvs_wal_persist_append_raw(uint64_t seq, uint16_t kind, uint16_t flags,
                                const char *payload, uint32_t len, uint32_t crc)
{
    if (!payload || len == 0 || len > KVS_JOURNAL_MAX_PAYLOAD) return -1;
    return wal_append_to_active(seq, kind, flags, payload, len, crc);
}

void kvs_wal_persist_stop(void)
{
    if (g_wal_thread_running) {
        pthread_mutex_lock(&g_wal_mu);
        g_wal_thread_running = 0;
        pthread_cond_signal(&g_wal_cond);
        pthread_mutex_unlock(&g_wal_mu);

        pthread_join(g_wal_thread, NULL);
        printf("[WAL-PERSIST] consumer thread stopped\n");
    }

    pthread_mutex_lock(&g_wal_mu);

    for (int b = 0; b < 2; b++) {
        int n = g_wal_buf_count[b];
        if (n > 0) {
            wal_pending_t *copy = malloc(sizeof(wal_pending_t) * (size_t)n);
            if (copy) {
                memcpy(copy, g_wal_buf[b], sizeof(wal_pending_t) * (size_t)n);
                for (int i = 0; i < n; i++) {
                    if (copy[i].len <= WAL_INLINE_PAYLOAD)
                        copy[i].payload = copy[i].inline_payload;
                }
                g_wal_buf_count[b] = 0;
                wal_flush_records(copy, n);
                free(copy);
            }
        }
    }

    if (g_wal_fd >= 0) {
        wal_fsync_if_needed(1);
        close(g_wal_fd);
        g_wal_fd = -1;
    }

    /* Cleanup io_uring */
    if (g_wal_ring_initialized) {
        io_uring_queue_exit(&g_wal_ring);
        g_wal_ring_initialized = 0;
        printf("[WAL-PERSIST] io_uring cleaned up\n");
    }

    pthread_mutex_unlock(&g_wal_mu);
}

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
        fprintf(stderr, "[WAL-PERSIST] unknown fsync policy '%s', using everysec\n",
                policy);
        g_wal_fsync_policy = WAL_FSYNC_EVERYSEC;
    }
}

uint64_t kvs_wal_last_synced_seq(void)
{
    return atomic_load_explicit(&g_wal_last_synced_seq, memory_order_acquire);
}
