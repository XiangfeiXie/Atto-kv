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
#include "kvstore.h"     /* get_time_now (cached clock) */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/wait.h>
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

/* ── AOF rewrite state ── */
static pid_t  g_aof_rewrite_pid   = 0;
static off_t  g_aof_base_size     = 0;
static time_t g_last_rewrite_time = 0;

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
 * AOF rewrite — compact WAL file from in-memory data (BGREWRITEAOF)
 * ========================================================================= */

#define AOF_REWRITE_BUF_CAP  (4 * 1024 * 1024)   /* 4 MB user-space buffer */

typedef struct {
    char   *buf;
    size_t  len;
    size_t  cap;
    int     fd;
} rewrite_ctx_t;

/* Fast uint32 → decimal, returns length */
static int rw_u2a(char *dst, uint32_t v) {
    if (v == 0) { dst[0] = '0'; return 1; }
    char tmp[12], *p = tmp;
    while (v) { *p++ = (char)('0' + (v % 10)); v /= 10; }
    int n = (int)(p - tmp);
    for (int i = 0; i < n; i++) dst[i] = tmp[n - 1 - i];
    return n;
}

/* Little-endian uint32 read (safe for unaligned) */
static inline uint32_t rw_pk_u32(const char *p) {
    uint32_t v;
    memcpy(&v, p, sizeof(v));
    return v;
}

static void rw_flush(rewrite_ctx_t *ctx) {
    if (ctx->len == 0) return;
    size_t off = 0;
    while (off < ctx->len) {
        ssize_t n = write(ctx->fd, ctx->buf + off, ctx->len - off);
        if (n <= 0) break;
        off += (size_t)n;
    }
    ctx->len = 0;
}

static void rw_reserve(rewrite_ctx_t *ctx, size_t need) {
    if (ctx->len + need > ctx->cap) rw_flush(ctx);
}

static void rw_append(rewrite_ctx_t *ctx, const void *data, size_t len) {
    rw_reserve(ctx, len);
    memcpy(ctx->buf + ctx->len, data, len);
    ctx->len += len;
}

/* Append RESP bulk string: $<len>\r\n<data>\r\n */
static void rw_bulk(rewrite_ctx_t *ctx, const char *data, uint32_t len) {
    char hdr[32], *p = hdr;
    *p++ = '$';
    p += rw_u2a(p, len);
    *p++ = '\r'; *p++ = '\n';
    rw_append(ctx, hdr, (size_t)(p - hdr));
    rw_append(ctx, data, (size_t)len);
    rw_append(ctx, "\r\n", 2);
}

/* Start a RESP array: *<argc>\r\n */
static void rw_cmd_start(rewrite_ctx_t *ctx, uint32_t argc) {
    char hdr[16], *p = hdr;
    *p++ = '*';
    p += rw_u2a(p, argc);
    *p++ = '\r'; *p++ = '\n';
    rw_append(ctx, hdr, (size_t)(p - hdr));
}

/* Write int64 as decimal bulk string */
static void rw_bulk_int(rewrite_ctx_t *ctx, int64_t v) {
    char num[24];
    int n = snprintf(num, sizeof(num), "%lld", (long long)v);
    rw_bulk(ctx, num, (uint32_t)n);
}

/* Encode one STRING entry as SET command */
static void rw_encode_string(rewrite_ctx_t *ctx, kv_entry_t *e) {
    const char *key = entry_key(e);
    uint32_t   klen = e->key_len;
    uint8_t     enc = entry_encoding(e);
    const char *val = entry_payload(e);
    uint32_t    vlen = entry_payload_len(e);

    rw_cmd_start(ctx, 3);
    rw_bulk(ctx, "SET", 3);
    rw_bulk(ctx, key, klen);
    if (enc == KV_ENCODING_INT) {
        int64_t iv;
        memcpy(&iv, val, sizeof(int64_t));
        rw_bulk_int(ctx, iv);
    } else {
        rw_bulk(ctx, val, vlen);
    }
}

/* Encode one HASH entry (PACKED encoding) as HSET command */
static void rw_encode_hash_packed(rewrite_ctx_t *ctx, kv_entry_t *e) {
    const char *key  = entry_key(e);
    uint32_t    klen = e->key_len;
    const char *data = e->data + 2;          /* skip type + encoding      */
    const char *end  = e->data + e->val_len;

    uint32_t count = rw_pk_u32(data);
    data += 4;
    if (count == 0) return;

    rw_cmd_start(ctx, 2 + 2 * count);
    rw_bulk(ctx, "HSET", 4);
    rw_bulk(ctx, key, klen);

    for (uint32_t i = 0; i < count && data < end; i++) {
        uint32_t flen = rw_pk_u32(data); data += 4;
        if (data + flen > end) break;
        rw_bulk(ctx, data, flen);
        data += flen;

        if (data >= end) break;
        uint8_t fenc = (uint8_t)*data++;

        if (fenc == 1 /* INT inline: [enc:1][i64:8], no length prefix */) {
            int64_t iv;
            memcpy(&iv, data, sizeof(int64_t));
            rw_bulk_int(ctx, iv);
            data += 8;
        } else /* RAW: [enc:1=0][val_len:4][val_bytes] */ {
            uint32_t fvlen = rw_pk_u32(data); data += 4;
            if (data + fvlen > end) break;
            rw_bulk(ctx, data, fvlen);
            data += fvlen;
        }
    }
}

/* Encode one HASH entry (DICT encoding) as HSET command */
static void rw_encode_hash_dict(rewrite_ctx_t *ctx, kv_entry_t *e) {
    const char *key  = entry_key(e);
    uint32_t    klen = e->key_len;
    kv_dict_t  *d    = (kv_dict_t *)(e->data + 2);

    uint32_t count = dict_count(d);
    if (count == 0) return;

    rw_cmd_start(ctx, 2 + 2 * count);
    rw_bulk(ctx, "HSET", 4);
    rw_bulk(ctx, key, klen);

    kv_iter_t *it = dict_iter_new(d, 1);
    kv_entry_t *de;
    while ((de = dict_iter_next(it)) != NULL) {
        const char *field = entry_key(de);
        uint32_t    flen  = de->key_len;
        uint8_t     fenc  = entry_encoding(de);
        const char *fval  = entry_payload(de);
        uint32_t    fvlen = entry_payload_len(de);

        rw_bulk(ctx, field, flen);
        if (fenc == KV_ENCODING_INT) {
            int64_t iv;
            memcpy(&iv, fval, sizeof(int64_t));
            rw_bulk_int(ctx, iv);
        } else {
            rw_bulk(ctx, fval, fvlen);
        }
    }
    dict_iter_free(it);
}

/* Traverse all in-memory data and write compact RESP to temp file */
static int aof_rewrite_traverse_and_write(int fd) {
    rewrite_ctx_t ctx;
    ctx.buf = malloc(AOF_REWRITE_BUF_CAP);
    if (!ctx.buf) { fprintf(stderr, "[REWRITE] OOM\n"); return -1; }
    ctx.len = 0;
    ctx.cap = AOF_REWRITE_BUF_CAP;
    ctx.fd  = fd;

    long long now = (long long)time(NULL) * 1000;

    kv_store_iter_t it;
    kv_store_iter_init(&it);
    kv_entry_t *e;
    long long written = 0;

    while ((e = kv_store_iter_next(&it)) != NULL) {
        /* Skip expired keys */
        if (e->expire && e->expire < now) continue;

        uint8_t type = entry_type(e);

        if (type == KV_TYPE_STRING) {
            rw_encode_string(&ctx, e);
            written++;
        } else if (type == KV_TYPE_HASH) {
            uint8_t enc = entry_encoding(e);
            if (enc == KV_ENCODING_PACKED) {
                rw_encode_hash_packed(&ctx, e);
                written++;
            } else if (enc == KV_ENCODING_DICT) {
                rw_encode_hash_dict(&ctx, e);
                written++;
            }
        }
    }
    kv_store_iter_free(&it);

    rw_flush(&ctx);
    if (fdatasync(fd) < 0)
        fprintf(stderr, "[REWRITE] fdatasync error: %s\n", strerror(errno));
    free(ctx.buf);
    printf("[REWRITE] compacted %lld keys to temp file\n", written);
    return 0;
}

/* ── Public rewrite API ── */

int kvs_aof_rewrite_bg(void) {
    if (g_aof_rewrite_pid != 0) return -1;

    const char *tmp_path = persist_path("build/incr.wal.tmp");
    const char *dst_path = persist_path(WAL_REL);

    pid_t pid = fork();
    if (pid < 0) {
        fprintf(stderr, "[REWRITE] fork failed: %s\n", strerror(errno));
        return -1;
    }

    if (pid == 0) {
        /* Child: destroy parent's io_uring, close parent's WAL fd */
        if (g_wal_ring_ok) { io_uring_queue_exit(&g_wal_ring); g_wal_ring_ok = 0; }
        if (g_wal_fd >= 0)  { close(g_wal_fd); g_wal_fd = -1; }

        int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            fprintf(stderr, "[REWRITE] open tmp failed: %s\n", strerror(errno));
            _exit(1);
        }

        /* Best-effort pre-allocate to reduce fragmentation */
        if (g_aof_offset > 0)
            posix_fallocate(fd, 0, (off_t)(g_aof_offset * 9 / 10));

        int rc = aof_rewrite_traverse_and_write(fd);
        close(fd);

        if (rc != 0) {
            unlink(tmp_path);
            _exit(1);
        }

        /* Atomic rename */
        if (rename(tmp_path, dst_path) < 0) {
            fprintf(stderr, "[REWRITE] rename failed: %s\n", strerror(errno));
            unlink(tmp_path);
            _exit(1);
        }

        printf("[REWRITE] child done, new AOF written\n");
        fflush(stdout);
        _exit(0);
    }

    /* Parent */
    g_aof_rewrite_pid = pid;
    printf("[REWRITE] started, child pid=%d\n", (int)pid);
    return 0;
}

void kvs_aof_rewrite_done(int status) {
    if (g_aof_rewrite_pid == 0) return;

    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
        /* Close old WAL fd and re-open the compacted file */
        if (g_wal_fd >= 0) {
            close(g_wal_fd);
            g_wal_fd = -1;
        }

        const char *path = persist_path(WAL_REL);
        g_wal_fd = open(path, O_CREAT | O_WRONLY | O_CLOEXEC, 0644);
        if (g_wal_fd < 0) {
            fprintf(stderr, "[REWRITE] re-open after rewrite failed: %s\n",
                    strerror(errno));
        }
        g_aof_offset = lseek(g_wal_fd, 0, SEEK_END);
        g_aof_base_size = g_aof_offset;
        g_last_rewrite_time = time(NULL);
        printf("[REWRITE] completed, new AOF size=%ld bytes\n",
               (long)g_aof_offset);
    } else {
        fprintf(stderr, "[REWRITE] child failed, status=%d\n", status);
    }

    g_aof_rewrite_pid = 0;
}

void kvs_aof_auto_rewrite_check(void) {
    /* Not yet started */
    if (!g_wal_ring_ok || g_wal_fd < 0) return;
    /* Already in progress */
    if (g_aof_rewrite_pid != 0) return;
    /* Cooldown */
    time_t now = time(NULL);
    if (now - g_last_rewrite_time < AOF_REWRITE_COOLDOWN_SEC) return;
    /* Min size gate */
    if (g_aof_offset < AOF_REWRITE_MIN_SIZE) return;

    /* First rewrite: set baseline */
    if (g_aof_base_size == 0) {
        g_aof_base_size = g_aof_offset;
        return;
    }

    /* Growth check */
    if (g_aof_base_size > 0) {
        long long growth = (g_aof_offset - g_aof_base_size) * 100
                         / g_aof_base_size;
        if (growth >= AOF_REWRITE_PERC_DEF) {
            printf("[REWRITE] auto-trigger: %lld%% growth "
                   "(%ld → %ld bytes)\n",
                   growth, (long)g_aof_base_size, (long)g_aof_offset);
            kvs_aof_rewrite_bg();
        }
    }
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

    /* Partial flush: flush the current slot once its data has been
     * buffered for ≥ 1 second (everysec semantics).  Time-based, NOT
     * tick-based: the event-loop frequency varies wildly between engines
     * (reactor idle ≈ 10 Hz, proactor idle ≈ 10 kHz), so a tick counter
     * was an unreliable proxy for wall-clock time — it delayed the flush
     * by up to 10 s on the reactor, silently widening the crash window
     * well past the advertised 1 s. */
    {
        static long long last_flush_ms = 0;
        long long now_ms = get_time_now();
        if (g_prod_pos > 0) {
            if (last_flush_ms == 0)
                last_flush_ms = now_ms;
            else if (now_ms - last_flush_ms >= 1000) {
                wal_flush_current_slot();
                last_flush_ms = now_ms;
            }
        } else {
            last_flush_ms = 0;
        }
    }

    /* Check fsync timer */
    wal_fsync_check();

    /* Poll AOF rewrite child completion */
    if (g_aof_rewrite_pid > 0) {
        int status;
        pid_t r = waitpid(g_aof_rewrite_pid, &status, WNOHANG);
        if (r == g_aof_rewrite_pid)
            kvs_aof_rewrite_done(status);
    }
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
