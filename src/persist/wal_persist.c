/* =========================================================
 * wal_persist.c — WAL 增量持久化（单线程无锁异步 io_uring 落盘）
 *
 * 设计要点（对齐 kvs_persist.c 的单线程模型）:
 *   - 静态预分配环形缓冲，无锁访问（proactor 单线程保证）
 *   - 阈值驱动提交（512KB），生产者直接提交 io_uring write SQE
 *   - 异步 reap：wal_flush_current_slot 时顺便收割已完成 CQE
 *   - 异步 fsync：仅提交 SQE 不等待，下次 reap 时检查完成
 *   - 绝对偏移写入，避免 O_APPEND 竞态
 *   - 零同步开销：无 mutex、无 condvar、无后台线程
 *
 * Public API:
 *   kvs_wal_persist_start / stop / append_raw / set_fsync_policy
 *   kvs_wal_periodic          — call from event loop (idle maintenance)
 *   kvs_wal_reap_completions  — reap in-flight write CQEs
 * ========================================================= */

#include "wal_persist.h"
#include "persistence.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <liburing.h>

/* ── Tunables ── */
#define AOF_RING_SIZE           16
#define AOF_SLOT_SIZE           (512 * 1024 + 4096)
#define AOF_FLUSH_THRESHOLD     (512 * 1024)
#define WAL_URING_DEPTH         256

/* ── Ring slot ── */
typedef struct {
    char      data[AOF_SLOT_SIZE];
    uint32_t  len;          /* valid bytes in this slot                    */
    int       in_flight;    /* write submitted, not yet completed by reap */
} aof_slot_t;

/* ── Global state (single-threaded — no mutex needed) ── */
static aof_slot_t   g_ring[AOF_RING_SIZE];
static int          g_prod_idx       = 0;   /* current write slot          */
static uint32_t     g_prod_pos       = 0;   /* bytes used in current slot  */
static int          g_inflight_count = 0;   /* slots with in_flight=1      */

static struct io_uring  g_wal_ring;
static int              g_wal_ring_ok = 0;
static int              g_wal_fd      = -1;
static off_t            g_aof_offset  = 0;

/* fsync state */
static int      g_fsync_always    = 0;
static int      g_fsync_in_flight = 0;
static time_t   g_last_fsync      = 0;

/* idle flush tracking — counter-based (single-threaded, no atomic needed) */
static int g_append_count = 0;

/* ── Internal prototypes ── */
static void wal_flush_current_slot(void);
static void wal_submit_fsync(void);

/* =========================================================================
 * io_uring helpers
 * ========================================================================= */

/* Reap all ready CQEs.  Called from wal_flush_current_slot (threshold flush)
 * and from kvs_wal_periodic (idle maintenance). */
void kvs_wal_reap_completions(void) {
    if (!g_wal_ring_ok) return;

    struct io_uring_cqe *cqe;
    unsigned head;
    unsigned count = 0;

    io_uring_for_each_cqe(&g_wal_ring, head, cqe) {
        int slot_idx = (int)(uintptr_t)io_uring_cqe_get_data(cqe);
        if (slot_idx == -1) {
            /* fsync completion */
            g_fsync_in_flight = 0;
            if (cqe->res < 0 && cqe->res != -ECANCELED)
                fprintf(stderr, "[WAL] fsync error: %s\n", strerror(-cqe->res));
        } else if (slot_idx >= 0 && slot_idx < AOF_RING_SIZE) {
            if (g_ring[slot_idx].in_flight) {
                g_ring[slot_idx].in_flight = 0;
                g_inflight_count--;
            }
            if (cqe->res < 0 && cqe->res != -ECANCELED) {
                fprintf(stderr, "[WAL] write error slot=%d: %s\n",
                        slot_idx, strerror(-cqe->res));
            }
        }
        count++;
    }

    if (count > 0)
        io_uring_cq_advance(&g_wal_ring, count);
}

/* Acquire an SQE with fallback: submit+reap, then block-wait if needed. */
static struct io_uring_sqe *wal_get_sqe(void) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_wal_ring);
    if (sqe) return sqe;

    /* Layer 1: submit pending + reap, retry */
    io_uring_submit(&g_wal_ring);
    kvs_wal_reap_completions();
    sqe = io_uring_get_sqe(&g_wal_ring);
    if (sqe) return sqe;

    /* Layer 2: block-wait for one CQE */
    struct io_uring_cqe *cqe = NULL;
    if (io_uring_wait_cqe(&g_wal_ring, &cqe) == 0 && cqe) {
        int slot_idx = (int)(uintptr_t)io_uring_cqe_get_data(cqe);
        if (slot_idx == -1) {
            g_fsync_in_flight = 0;
        } else if (slot_idx >= 0 && slot_idx < AOF_RING_SIZE) {
            if (g_ring[slot_idx].in_flight) {
                g_ring[slot_idx].in_flight = 0;
                g_inflight_count--;
            }
        }
        io_uring_cqe_seen(&g_wal_ring, cqe);
    }
    sqe = io_uring_get_sqe(&g_wal_ring);
    if (!sqe) {
        kvs_wal_reap_completions();
        sqe = io_uring_get_sqe(&g_wal_ring);
    }
    if (!sqe) {
        fprintf(stderr, "[WAL] FATAL: cannot acquire SQE after drain\n");
    }
    return sqe;
}

/* =========================================================================
 * File & fsync
 * ========================================================================= */

static int wal_open(void) {
    if (g_wal_fd >= 0) return 0;
    const char *path = persist_path(WAL_REL);
    g_wal_fd = open(path, O_CREAT | O_WRONLY | O_CLOEXEC, 0644);
    if (g_wal_fd < 0) {
        fprintf(stderr, "[WAL] open %s: %s\n", path, strerror(errno));
        return -1;
    }
    g_aof_offset = lseek(g_wal_fd, 0, SEEK_END);
    printf("[WAL] file opened: %s (offset=%ld)\n", path, (long)g_aof_offset);
    return 0;
}

/* Submit async fsync (never blocks).  Completion reaped later. */
static void wal_submit_fsync(void) {
    if (g_wal_fd < 0 || !g_wal_ring_ok || g_fsync_in_flight) return;

    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_wal_ring);
    if (sqe) {
        io_uring_prep_fsync(sqe, g_wal_fd, IORING_FSYNC_DATASYNC);
        io_uring_sqe_set_data(sqe, (void *)(uintptr_t)-1);
        io_uring_submit(&g_wal_ring);
        g_fsync_in_flight = 1;
        g_last_fsync = time(NULL);
    }
}

/* Check and possibly submit async fsync (non-blocking). */
static void wal_fsync_check(void) {
    if (g_fsync_always) {
        /* always mode: submit fsync after every slot flush */
        wal_submit_fsync();
        return;
    }

    time_t now = time(NULL);
    if (now > g_last_fsync && !g_fsync_in_flight)
        wal_submit_fsync();
}

/* =========================================================================
 * Slot flush (called when current slot reaches threshold)
 * ========================================================================= */

/* Submit current slot via async io_uring write, then roll to next slot.
 * Must be called from the proactor event-loop thread (single-threaded). */
static void wal_flush_current_slot(void) {
    if (g_prod_pos == 0) return;

    aof_slot_t *slot = &g_ring[g_prod_idx];
    slot->len = g_prod_pos;

    /* If slot is still in-flight from a previous round, reap and retry.
     * This should be extremely rare with 16 slots unless disk is very slow. */
    if (slot->in_flight) {
        kvs_wal_reap_completions();
        if (slot->in_flight) {
            static _Atomic int stall_counter = 0;
            int n = atomic_fetch_add(&stall_counter, 1);
            if (n % 100 == 0) {
                fprintf(stderr, "[WAL] I/O stall: all %d slots in flight, "
                        "waiting for disk (count=%d)\n",
                        AOF_RING_SIZE, n + 1);
            }
            /* Spin-reap until slot frees up */
            while (slot->in_flight) {
                struct io_uring_cqe *cqe = NULL;
                if (io_uring_wait_cqe(&g_wal_ring, &cqe) == 0 && cqe) {
                    int idx = (int)(uintptr_t)io_uring_cqe_get_data(cqe);
                    if (idx == -1) g_fsync_in_flight = 0;
                    else if (idx >= 0 && idx < AOF_RING_SIZE) {
                        if (g_ring[idx].in_flight) {
                            g_ring[idx].in_flight = 0;
                            g_inflight_count--;
                        }
                    }
                    io_uring_cqe_seen(&g_wal_ring, cqe);
                }
            }
        }
    }

    /* Acquire SQE and submit async write */
    struct io_uring_sqe *sqe = wal_get_sqe();
    if (sqe) {
        io_uring_prep_write(sqe, g_wal_fd, slot->data, slot->len, g_aof_offset);
        io_uring_sqe_set_data(sqe, (void *)(uintptr_t)(long)g_prod_idx);
        g_aof_offset += slot->len;
        slot->in_flight = 1;
        g_inflight_count++;
        io_uring_submit(&g_wal_ring);

        /* Reap any ready completions while we're here */
        kvs_wal_reap_completions();
    } else {
        /* Catastrophic: fallback to synchronous write */
        fprintf(stderr, "[WAL] FATAL: no SQE, using sync write fallback\n");
        ssize_t w = write(g_wal_fd, slot->data, slot->len);
        if (w < 0)
            fprintf(stderr, "[WAL] sync write error: %s\n", strerror(errno));
        /* slot not marked in_flight, can be reused immediately */
    }

    /* Roll to next slot */
    g_prod_idx = (g_prod_idx + 1) % AOF_RING_SIZE;
    g_prod_pos = 0;
}

/* =========================================================================
 * Public API
 * ========================================================================= */

/* Periodic maintenance — call from event loop (update_cached_time).
 * Handles: reap completions, partial flush of stale data, async fsync. */
void kvs_wal_periodic(void) {
    if (!g_wal_ring_ok) return;

    /* Reap any completions (frees in-flight slots) */
    kvs_wal_reap_completions();

    /* Partial flush: if data has been sitting > 1 second in current slot.
     * Check via append counter — if counter hasn't changed since last check,
     * data is stale.  Event loop runs at ~10K-100K iters/s so 4096 ≈ idle. */
    {
        static int last_count = 0;
        static int idle_ticks = 0;
        int cur = g_append_count;
        if (cur == last_count && g_prod_pos > 0) {
            idle_ticks++;
            if (idle_ticks > 100) { /* ~1s at 100Hz event loop */
                wal_flush_current_slot();
                idle_ticks = 0;
            }
        } else {
            idle_ticks = 0;
        }
        last_count = cur;
    }

    /* Check fsync timer */
    wal_fsync_check();
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

int kvs_wal_persist_start(void) {
    if (wal_open() != 0)
        return -1;

    if (!g_wal_ring_ok) {
        int rc = io_uring_queue_init(WAL_URING_DEPTH, &g_wal_ring, 0);
        if (rc < 0) {
            fprintf(stderr, "[WAL] io_uring init: %s\n", strerror(-rc));
            return -1;
        }
        g_wal_ring_ok = 1;
        printf("[WAL] io_uring ready (depth=%d)\n", WAL_URING_DEPTH);
    }

    memset(g_ring, 0, sizeof(g_ring));
    g_prod_idx        = 0;
    g_prod_pos        = 0;
    g_inflight_count  = 0;
    g_fsync_in_flight = 0;
    g_last_fsync      = time(NULL);
    g_append_count = 0;

    printf("[WAL] lock-free ring ready (%d slots x %d KB)\n",
           AOF_RING_SIZE, AOF_SLOT_SIZE / 1024);
    return 0;
}

/* Hot-path: append raw RESP bytes to ring buffer — lock-free, single-threaded.
 * Mirror of kvs_persist.c:kvs_aof_append — threshold check + single memcpy.
 * Called from kvs_write_journal_append (proactor event-loop thread). */
int kvs_wal_persist_append_raw(const char *payload, uint32_t len) {
    if (!payload || len == 0 || len > KVS_JOURNAL_MAX_PAYLOAD) return -1;

    /* Threshold check: slot nearly full?  Flush before writing new data
     * so the new record always fits in the current slot. */
    if (g_prod_pos + len > AOF_SLOT_SIZE - 4096) {
        wal_flush_current_slot();
    }

    /* Single memcpy — no header, no CRC, no time() syscall */
    memcpy(g_ring[g_prod_idx].data + g_prod_pos, payload, len);
    g_prod_pos += len;

    /* Update append counter for idle detection (every N appends) */
    g_append_count++;

    return 0;
}

void kvs_wal_persist_stop(void) {
    /* Flush remaining partial slot */
    if (g_prod_pos > 0)
        wal_flush_current_slot();

    /* Wait for all in-flight writes to complete */
    if (g_inflight_count > 0) {
        printf("[WAL] draining %d in-flight writes...\n", g_inflight_count);
        while (g_inflight_count > 0) {
            struct io_uring_cqe *cqe = NULL;
            int ret = io_uring_wait_cqe(&g_wal_ring, &cqe);
            if (ret < 0) break;
            if (cqe) {
                int idx = (int)(uintptr_t)io_uring_cqe_get_data(cqe);
                if (idx == -1) g_fsync_in_flight = 0;
                else if (idx >= 0 && idx < AOF_RING_SIZE) {
                    if (g_ring[idx].in_flight) {
                        g_ring[idx].in_flight = 0;
                        g_inflight_count--;
                    }
                }
                io_uring_cqe_seen(&g_wal_ring, cqe);
            }
        }
    }

    /* Final fsync */
    if (g_wal_fd >= 0) {
        if (g_wal_ring_ok) {
            struct io_uring_sqe *sqe = io_uring_get_sqe(&g_wal_ring);
            if (sqe) {
                io_uring_prep_fsync(sqe, g_wal_fd, IORING_FSYNC_DATASYNC);
                io_uring_submit(&g_wal_ring);
                struct io_uring_cqe *cqe = NULL;
                io_uring_wait_cqe(&g_wal_ring, &cqe);
                if (cqe) io_uring_cqe_seen(&g_wal_ring, cqe);
            }
        } else {
            fdatasync(g_wal_fd);
        }
        close(g_wal_fd);
        g_wal_fd = -1;
    }

    if (g_wal_ring_ok) {
        io_uring_queue_exit(&g_wal_ring);
        g_wal_ring_ok = 0;
    }

    printf("[WAL] stopped safely\n");
}
