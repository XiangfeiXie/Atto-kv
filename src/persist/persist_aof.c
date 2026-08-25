/* persist_aof.c — AOF 追加日志（io_uring 异步落盘）+ 重写 + 启动重放
 *
 * 职责：
 *   - 追加写：单线程无锁 io_uring 环形缓冲落盘（appendonly.aof）
 *   - AOF 重写：fork 子进程压缩全量数据（BGREWRITEAOF）
 *   - 启动重放：aof_reload_mmap 用 mmap 重放 raw RESP 命令
 *
 * 对外接口见 persist_aof.h。 */

#include "persist_aof.h"
#include "persistence.h"
#include "kvstore.h"     /* get_time_now (cached clock) */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <liburing.h>

/* ── Tunables ── */
#define AOF_RING_SIZE           16
#define AOF_SLOT_SIZE           (512 * 1024 + 4096)
#define AOF_URING_DEPTH         256

/* AOF rewrite (BGREWRITEAOF) tunables */
#define AOF_REWRITE_MIN_SIZE     ((off_t)64 * 1024 * 1024)   /* 64 MB */
#define AOF_REWRITE_PERC_DEF     100                          /* 100%  */

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

static struct io_uring  g_aof_ring;
static int              g_aof_ring_ok = 0;
static int              g_aof_fd      = -1;
static off_t            g_aof_offset  = 0;

/* fsync policy — FSYNC_ALWAYS / FSYNC_EVERYSEC / FSYNC_NO */
typedef enum {
    FSYNC_NO = 0,
    FSYNC_EVERYSEC = 1,
    FSYNC_ALWAYS = 2,
} fsync_mode_t;

static fsync_mode_t g_fsync_mode      = FSYNC_EVERYSEC;
static int          g_fsync_in_flight = 0;
static time_t       g_last_fsync      = 0;

/* ── AOF rewrite state ── */
static pid_t  g_aof_rewrite_pid = 0;
static off_t  g_aof_base_size   = 0;

/* ── Internal prototypes ── */
static void aof_flush_current_slot(void);
static void aof_submit_fsync(void);

/* =========================================================================
 * io_uring helpers
 * ========================================================================= */

/* Update state for one completed CQE.  The CQE's data carries either a
 * slot index (>= 0) or -1 for an fsync completion. */
static void aof_handle_cqe(struct io_uring_cqe *cqe) {
    int idx = (int)(uintptr_t)io_uring_cqe_get_data(cqe);
    if (idx == -1) {
        g_fsync_in_flight = 0;
    } else if (idx >= 0 && idx < AOF_RING_SIZE) {
        if (g_ring[idx].in_flight) {
            g_ring[idx].in_flight = 0;
            g_inflight_count--;
        }
    }
}

/* Reap all ready CQEs.  Called from aof_flush_current_slot (threshold flush)
 * and from kvs_aof_periodic (idle maintenance). */
static void aof_reap_completions(void) {
    if (!g_aof_ring_ok) return;

    struct io_uring_cqe *cqe;
    unsigned head;
    unsigned count = 0;

    io_uring_for_each_cqe(&g_aof_ring, head, cqe) {
        int slot_idx = (int)(uintptr_t)io_uring_cqe_get_data(cqe);
        aof_handle_cqe(cqe);
        if (slot_idx == -1) {
            if (cqe->res < 0 && cqe->res != -ECANCELED)
                fprintf(stderr, "[AOF] fsync error: %s\n", strerror(-cqe->res));
        } else if (slot_idx >= 0 && slot_idx < AOF_RING_SIZE) {
            if (cqe->res < 0 && cqe->res != -ECANCELED) {
                fprintf(stderr, "[AOF] write error slot=%d: %s\n",
                        slot_idx, strerror(-cqe->res));
            }
        }
        count++;
    }

    if (count > 0)
        io_uring_cq_advance(&g_aof_ring, count);
}

/* Acquire an SQE with fallback: submit+reap, then block-wait if needed. */
static struct io_uring_sqe *aof_get_sqe(void) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_aof_ring);
    if (sqe) return sqe;

    /* Submit pending + reap, retry */
    io_uring_submit(&g_aof_ring);
    aof_reap_completions();
    sqe = io_uring_get_sqe(&g_aof_ring);
    if (sqe) return sqe;

    /* Block-wait for one CQE */
    struct io_uring_cqe *cqe = NULL;
    if (io_uring_wait_cqe(&g_aof_ring, &cqe) == 0 && cqe) {
        aof_handle_cqe(cqe);
        io_uring_cqe_seen(&g_aof_ring, cqe);
    }
    sqe = io_uring_get_sqe(&g_aof_ring);
    if (!sqe) {
        aof_reap_completions();
        sqe = io_uring_get_sqe(&g_aof_ring);
    }
    if (!sqe) {
        fprintf(stderr, "[AOF] FATAL: cannot acquire SQE after drain\n");
    }
    return sqe;
}

/* =========================================================================
 * File & fsync
 * ========================================================================= */

static int aof_open(void) {
    if (g_aof_fd >= 0) return 0;
    const char *path = persist_path(AOF_REL);
    g_aof_fd = open(path, O_CREAT | O_WRONLY | O_CLOEXEC, 0644);
    if (g_aof_fd < 0) {
        fprintf(stderr, "[AOF] open %s: %s\n", path, strerror(errno));
        return -1;
    }
    g_aof_offset = lseek(g_aof_fd, 0, SEEK_END);
    printf("[AOF] file opened: %s (offset=%ld)\n", path, (long)g_aof_offset);
    return 0;
}

/* Submit async fsync (never blocks).  Completion reaped later. */
static void aof_submit_fsync(void) {
    if (g_aof_fd < 0 || !g_aof_ring_ok || g_fsync_in_flight) return;

    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_aof_ring);
    if (sqe) {
        io_uring_prep_fsync(sqe, g_aof_fd, IORING_FSYNC_DATASYNC);
        io_uring_sqe_set_data(sqe, (void *)(uintptr_t)-1);
        io_uring_submit(&g_aof_ring);
        g_fsync_in_flight = 1;
        g_last_fsync = time(NULL);
    }
}

/* Check and possibly submit async fsync (non-blocking). */
static void aof_fsync_check(void) {
    switch (g_fsync_mode) {
    case FSYNC_ALWAYS:
        /* always mode: fsync on every periodic tick */
        aof_submit_fsync();
        break;
    case FSYNC_EVERYSEC: {
        time_t now = time(NULL);
        if (now > g_last_fsync && !g_fsync_in_flight)
            aof_submit_fsync();
        break;
    }
    case FSYNC_NO:
    default:
        /* no runtime fsync — rely on the OS page cache */
        break;
    }
}

/* =========================================================================
 * Slot flush (called when current slot reaches threshold)
 * ========================================================================= */

/* Submit current slot via async io_uring write, then roll to next slot.
 * Must be called from the proactor event-loop thread (single-threaded). */
static void aof_flush_current_slot(void) {
    if (g_prod_pos == 0) return;

    aof_slot_t *slot = &g_ring[g_prod_idx];
    slot->len = g_prod_pos;

    /* If slot is still in-flight from a previous round, reap and wait for it
     * to free (only happens when disk is slower than the ring). */
    if (slot->in_flight) {
        aof_reap_completions();
        while (slot->in_flight) {
            struct io_uring_cqe *cqe = NULL;
            if (io_uring_wait_cqe(&g_aof_ring, &cqe) == 0 && cqe) {
                aof_handle_cqe(cqe);
                io_uring_cqe_seen(&g_aof_ring, cqe);
            }
        }
    }

    /* Acquire SQE and submit async write */
    struct io_uring_sqe *sqe = aof_get_sqe();
    if (sqe) {
        io_uring_prep_write(sqe, g_aof_fd, slot->data, slot->len, g_aof_offset);
        io_uring_sqe_set_data(sqe, (void *)(uintptr_t)(long)g_prod_idx);
        g_aof_offset += slot->len;
        slot->in_flight = 1;
        g_inflight_count++;
        io_uring_submit(&g_aof_ring);

        /* Reap any ready completions while we're here */
        aof_reap_completions();
    } else {
        /* Catastrophic: fallback to synchronous write */
        fprintf(stderr, "[AOF] FATAL: no SQE, using sync write fallback\n");
        ssize_t w = write(g_aof_fd, slot->data, slot->len);
        if (w < 0)
            fprintf(stderr, "[AOF] sync write error: %s\n", strerror(errno));
        /* slot not marked in_flight, can be reused immediately */
    }

    /* Roll to next slot */
    g_prod_idx = (g_prod_idx + 1) % AOF_RING_SIZE;
    g_prod_pos = 0;
}

/* =========================================================================
 * AOF rewrite — compact AOF file from in-memory data (BGREWRITEAOF)
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

/* Encode one STRING entry as SET command */
static void rw_encode_string(rewrite_ctx_t *ctx, kv_entry_t *e) {
    const char *key = entry_key(e);
    uint32_t   klen = e->key_len;
    const char *val = entry_payload(e);
    uint32_t    vlen = entry_payload_len(e);

    rw_cmd_start(ctx, 3);
    rw_bulk(ctx, "SET", 3);
    rw_bulk(ctx, key, klen);
    rw_bulk(ctx, val, vlen);
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

    kv_iter_t *iter;
    iter = dict_iter_new(&g_dict, 1);
    kv_entry_t *e;
    long long written = 0;

    while ((e = dict_iter_next(iter)) != NULL) {
        /* Skip expired keys */
        if (e->expire && e->expire < now) continue;

        rw_encode_string(&ctx, e);
        written++;
    }
    dict_iter_free(iter);

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

    const char *tmp_path = persist_path("build/appendonly.aof.tmp");
    const char *dst_path = persist_path(AOF_REL);

    pid_t pid = fork();
    if (pid < 0) {
        fprintf(stderr, "[REWRITE] fork failed: %s\n", strerror(errno));
        return -1;
    }

    if (pid == 0) {
        /* Child: destroy parent's io_uring, close parent's AOF fd */
        if (g_aof_ring_ok) { io_uring_queue_exit(&g_aof_ring); g_aof_ring_ok = 0; }
        if (g_aof_fd >= 0)  { close(g_aof_fd); g_aof_fd = -1; }

        int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            fprintf(stderr, "[REWRITE] open tmp failed: %s\n", strerror(errno));
            _exit(1);
        }

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

static void aof_rewrite_done(int status) {
    if (g_aof_rewrite_pid == 0) return;

    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
        /* Close old AOF fd and re-open the compacted file */
        if (g_aof_fd >= 0) {
            close(g_aof_fd);
            g_aof_fd = -1;
        }

        const char *path = persist_path(AOF_REL);
        g_aof_fd = open(path, O_CREAT | O_WRONLY | O_CLOEXEC, 0644);
        if (g_aof_fd < 0) {
            fprintf(stderr, "[REWRITE] re-open after rewrite failed: %s\n",
                    strerror(errno));
        }
        g_aof_offset = lseek(g_aof_fd, 0, SEEK_END);
        g_aof_base_size = g_aof_offset;
        printf("[REWRITE] completed, new AOF size=%ld bytes\n",
               (long)g_aof_offset);
    } else {
        fprintf(stderr, "[REWRITE] child failed, status=%d\n", status);
    }

    g_aof_rewrite_pid = 0;
}

void kvs_aof_auto_rewrite_check(void) {
    /* Not yet started */
    if (!g_aof_ring_ok || g_aof_fd < 0) return;
    /* Already in progress */
    if (g_aof_rewrite_pid != 0) return;
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
void kvs_aof_periodic(void) {
    if (!g_aof_ring_ok) return;

    /* Reap any completions (frees in-flight slots) */
    aof_reap_completions();

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
                aof_flush_current_slot();
                last_flush_ms = now_ms;
            }
        } else {
            last_flush_ms = 0;
        }
    }

    /* Check fsync timer */
    aof_fsync_check();

    /* Poll AOF rewrite child completion */
    if (g_aof_rewrite_pid > 0) {
        int status;
        pid_t r = waitpid(g_aof_rewrite_pid, &status, WNOHANG);
        if (r == g_aof_rewrite_pid)
            aof_rewrite_done(status);
    }
}

void kvs_aof_set_fsync_policy(const char *policy) {
    if (!policy) return;
    if (strcmp(policy, "always") == 0) {
        g_fsync_mode = FSYNC_ALWAYS;
        printf("[AOF] fsync: always\n");
    } else if (strcmp(policy, "no") == 0) {
        g_fsync_mode = FSYNC_NO;
        printf("[AOF] fsync: no\n");
    } else {
        g_fsync_mode = FSYNC_EVERYSEC;
        printf("[AOF] fsync: everysec\n");
    }
}

int kvs_aof_persist_start(void) {
    if (aof_open() != 0)
        return -1;

    if (!g_aof_ring_ok) {
        int rc = io_uring_queue_init(AOF_URING_DEPTH, &g_aof_ring, 0);
        if (rc < 0) {
            fprintf(stderr, "[AOF] io_uring init: %s\n", strerror(-rc));
            return -1;
        }
        g_aof_ring_ok = 1;
        printf("[AOF] io_uring ready (depth=%d)\n", AOF_URING_DEPTH);
    }

    memset(g_ring, 0, sizeof(g_ring));
    g_prod_idx        = 0;
    g_prod_pos        = 0;
    g_inflight_count  = 0;
    g_fsync_in_flight = 0;
    g_last_fsync      = time(NULL);

    printf("[AOF] lock-free ring ready (%d slots x %d KB)\n",
           AOF_RING_SIZE, AOF_SLOT_SIZE / 1024);
    return 0;
}

/* Hot-path: append raw RESP bytes to the ring buffer — lock-free, single-threaded. */
int kvs_aof_persist_append_raw(const char *payload, uint32_t len) {
    if (!payload || len == 0 || len > KVS_JOURNAL_MAX_PAYLOAD) return -1;

    /* Threshold check: slot nearly full?  Flush before writing new data
     * so the new record always fits in the current slot. */
    if (g_prod_pos + len > AOF_SLOT_SIZE - 4096) {
        aof_flush_current_slot();
    }

    /* Single memcpy — no header, no CRC, no time() syscall */
    memcpy(g_ring[g_prod_idx].data + g_prod_pos, payload, len);
    g_prod_pos += len;

    return 0;
}

void kvs_aof_persist_stop(void) {
    /* Flush remaining partial slot */
    if (g_prod_pos > 0)
        aof_flush_current_slot();

    /* Wait for all in-flight writes to complete */
    if (g_inflight_count > 0) {
        printf("[AOF] draining %d in-flight writes...\n", g_inflight_count);
        while (g_inflight_count > 0) {
            struct io_uring_cqe *cqe = NULL;
            int ret = io_uring_wait_cqe(&g_aof_ring, &cqe);
            if (ret < 0) break;
            if (cqe) {
                aof_handle_cqe(cqe);
                io_uring_cqe_seen(&g_aof_ring, cqe);
            }
        }
    }

    /* Final fsync */
    if (g_aof_fd >= 0) {
        if (g_aof_ring_ok) {
            struct io_uring_sqe *sqe = io_uring_get_sqe(&g_aof_ring);
            if (sqe) {
                io_uring_prep_fsync(sqe, g_aof_fd, IORING_FSYNC_DATASYNC);
                io_uring_submit(&g_aof_ring);
                struct io_uring_cqe *cqe = NULL;
                io_uring_wait_cqe(&g_aof_ring, &cqe);
                if (cqe) io_uring_cqe_seen(&g_aof_ring, cqe);
            }
        } else {
            fdatasync(g_aof_fd);
        }
        close(g_aof_fd);
        g_aof_fd = -1;
    }

    if (g_aof_ring_ok) {
        io_uring_queue_exit(&g_aof_ring);
        g_aof_ring_ok = 0;
    }

    printf("[AOF] stopped safely\n");
}

/* =========================================================================
 * AOF reload — startup replay of raw RESP commands (mmap + read fallback)
 * ========================================================================= */

void aof_reload_mmap(void) {
    char *base = NULL;
    size_t fsz = 0;
    int use_mmap = 0;

    int rc = file_read_full(persist_path(AOF_REL), &base, &fsz, &use_mmap, "AOF");
    if (rc != FILE_READ_OK) return;

    printf("[AOF] load start, size=%zu\n", fsz);

    g_in_reload = 1;
    DynBuf wb;
    dynbuf_init(&wb, BUF_INIT_CAP);
    kvs_token_t tokens[KVS_MAX_TOKENS];
    int loaded = 0;

    const char *p = base;
    long remaining = (long)fsz;
    while (remaining > 0) {
        int argc = 0;
        int cmd_len = resp_parse_slices(p, (int)remaining,
                                        tokens, KVS_MAX_TOKENS, &argc);
        if (cmd_len <= 0) break;
        dynbuf_clear(&wb);
        kvs_dispatch(tokens, argc, p, cmd_len, &wb);
        p += cmd_len;
        remaining -= cmd_len;
        loaded++;
    }

    dynbuf_free(&wb);

    if (use_mmap) {
        munmap(base, fsz);
    } else {
        free(base);
    }

    g_in_reload = 0;
    printf("[AOF] load finished, %d commands replayed\n", loaded);
}
