/* =========================================================
 * persist_rdb.c — RDB save (fork + io_uring) and load (mmap),
 *                dict scan helpers, and BGSAVE state machine.
 *
 * Public API (declared in persistence.h):
 *   rdb_save_sync / rdb_save_async_uring / rdb_bgsave_in_progress
 *   rdb_load_mmap / rdb_load_from_buf / rdb_load_stream_*
 *   rdb_serialize_to_buf / rdb_serialize_begin/next/end
 *   rdb_rebuild_all_encodings
 * ========================================================= */

#include "persistence.h"
#include "kvstore.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <liburing.h>
#include <sys/mman.h>
#include <sys/wait.h>

/* ── RDB tmp path ── */
#define RDB_TMP  "build/dump.rdb.tmp"

/* ── Dynamic buffer for RDB serialization ── */
typedef struct {
    char  *buf;
    size_t len;
    size_t cap;
} rdb_buf_t;

/* ── Forward declarations ── */
static void rdb_buf_reserve(rdb_buf_t *b, size_t need);
static void store_scan(void (*cb)(kv_entry_t *e, void *priv), void *priv);
static void rdb_write_uring(int fd, const char *data, size_t total);
static void rdb_sigchld(int signo);
static const char *mread(const char *p, const char *end, void *out, size_t n);

/* ── BGSAVE child PID (access via rdb_bgsave_in_progress()) ── */
static volatile pid_t g_rdb_child_pid    = 0;

/* ── BGSAVE three-layer defense ── */
#define BGSAVE_COOLDOWN_MULT     5   /* cooldown = max(MIN, duration * MULT) */

static int      g_bgsave_pending             = 0;
static time_t   g_last_save_time             = 0;
static time_t   g_last_bgsave_start          = 0;
static int      g_last_bgsave_duration_sec   = 0;
static uint64_t g_bgsave_skipped             = 0;
static uint64_t g_bgsave_total               = 0;

static inline time_t bgsave_cooldown(void) {
    time_t dur = (time_t)g_last_bgsave_duration_sec * BGSAVE_COOLDOWN_MULT;
    return dur > BGSAVE_COOLDOWN_MIN_SEC ? dur : BGSAVE_COOLDOWN_MIN_SEC;
}

/* ── Changes since last save (incremented on hot path) ── */
uint64_t g_rdb_changes = 0;

/* =========================================================================
 * RDB buffer helpers (moved from persistence.c)
 * ========================================================================= */

static void rdb_buf_reserve(rdb_buf_t *b, size_t need) {
    if (b->len + need <= b->cap) return;

    size_t ncap = b->cap ? b->cap : 64 * 1024;

    if (b->len + need > ncap * 2) {
        ncap = b->len + need + (need >> 2);
    } else {
        while (ncap < b->len + need)
            ncap *= 2;
    }

    b->buf = (char *)realloc(b->buf, ncap);
    b->cap = ncap;
}

/* ── RDB on-disk format constants ── */
#define RDB_MAGIC    0x524442
#define RDB_VERSION  2

/* Write the 16-byte RDB header (magic, version, db_num, key_count). */
static void rdb_write_header(char *dst, int key_count) {
    int magic   = RDB_MAGIC;
    int version = RDB_VERSION;
    int db_num  = 0;
    memcpy(dst,      &magic,     sizeof(int));
    memcpy(dst + 4,  &version,   sizeof(int));
    memcpy(dst + 8,  &db_num,    sizeof(int));
    memcpy(dst + 12, &key_count, sizeof(int));
}

/* =========================================================================
 * Dict full scan (used by RDB save and serialize)
 * ========================================================================= */

static void store_scan(void (*cb)(kv_entry_t *e, void *priv), void *priv) {
    kv_iter_t *iter;
    iter = dict_iter_new(&g_dict, 1);
    kv_entry_t *e;
    while ((e = dict_iter_next(iter)) != NULL)
        cb(e, priv);
    dict_iter_free(iter);
}

/* =========================================================================
 * io_uring chunked write + fdatasync (RDB save)
 * ========================================================================= */

static void rdb_write_uring(int fd, const char *data, size_t total) {
    const size_t CHUNK = 1 * 1024 * 1024;
    const int    DEPTH = 128;

    struct io_uring ring;
    if (io_uring_queue_init(DEPTH, &ring, 0) < 0) {
        size_t off = 0;
        while (off < total) {
            ssize_t w = write(fd, data + off, total - off);
            if (w <= 0) {
                fprintf(stderr, "[RDB] fallback write failed at offset %zu: %s\n",
                        off, strerror(errno));
                return;
            }
            off += (size_t)w;
        }
        return;
    }

    size_t off = 0;
    int    inflight = 0;
    int    write_error = 0;

    while (off < total || inflight > 0) {
        while (off < total && inflight < DEPTH && !write_error) {
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
            if (!sqe) break;
            size_t n = total - off;
            if (n > CHUNK) n = CHUNK;
            io_uring_prep_write(sqe, fd, data + off, n, off);
            sqe->user_data = n;
            off += n;
            inflight++;
        }

        if (inflight > 0) {
            io_uring_submit(&ring);

            while (inflight > 0) {
                struct io_uring_cqe *cqe = NULL;
                int ret = io_uring_peek_cqe(&ring, &cqe);

                if (ret == -EAGAIN) {
                    ret = io_uring_wait_cqe(&ring, &cqe);
                    if (ret < 0) {
                        fprintf(stderr, "[RDB] io_uring_wait_cqe failed: %s\n", strerror(-ret));
                        write_error = 1;
                        break;
                    }
                }

                if (cqe) {
                    if (cqe->res < 0) {
                        fprintf(stderr, "[RDB] uring write error: %s\n", strerror(-cqe->res));
                        write_error = 1;
                        io_uring_cqe_seen(&ring, cqe);
                        inflight--;
                        break;
                    }

                    size_t expected = (size_t)cqe->user_data;
                    if ((size_t)cqe->res != expected) {
                        fprintf(stderr, "[RDB] short write: %d/%zu bytes\n", cqe->res, expected);
                        write_error = 1;
                        io_uring_cqe_seen(&ring, cqe);
                        inflight--;
                        break;
                    }

                    io_uring_cqe_seen(&ring, cqe);
                    inflight--;
                }
            }

            if (write_error) {
                io_uring_queue_exit(&ring);
                return;
            }
        }
    }

    {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        if (sqe) {
            io_uring_prep_fsync(sqe, fd, IORING_FSYNC_DATASYNC);
            sqe->flags |= IOSQE_IO_DRAIN;
            io_uring_submit(&ring);
            struct io_uring_cqe *cqe = NULL;
            if (io_uring_wait_cqe(&ring, &cqe) == 0) {
                if (cqe->res < 0) {
                    fprintf(stderr, "[RDB] fsync failed: %s\n", strerror(-cqe->res));
                }
                io_uring_cqe_seen(&ring, cqe);
            }
        } else {
            fdatasync(fd);
        }
    }
    io_uring_queue_exit(&ring);
}

/* =========================================================================
 * BGSAVE state machine — three-layer defense
 *
 *   Layer 1  Coalesce  : one pending retry queued when child is running
 *   Layer 2  Cooldown  : minimum interval between successive BGSAVE starts
 *   Layer 3  Drop      : redundant request when pending already exists
 * ========================================================================= */

int rdb_bgsave_in_progress(void) {
    return (g_rdb_child_pid > 0) ? 1 : 0;
}

static void rdb_bgsave_done(void) {
    time_t now = time(NULL);
    g_rdb_child_pid              = 0;
    g_bgsave_total++;
    g_last_save_time             = now;
    g_last_bgsave_duration_sec   = (int)(now - g_last_bgsave_start);
    g_rdb_changes                = 0;
}

static void rdb_sigchld(int signo) {
    (void)signo;
    pid_t pid;
    int   status;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (pid == g_rdb_child_pid)
            rdb_bgsave_done();
    }
}

/* Called from event loop — retries pending BGSAVE when cooldown expires */
void rdb_periodic(void) {
    if (g_rdb_child_pid > 0)   return;
    if (!g_bgsave_pending)      return;

    time_t now = time(NULL);
    if (now - g_last_bgsave_start >= bgsave_cooldown()) {
        g_bgsave_pending = 0;
        rdb_save_async_uring();
    }
}

/* ── Stat accessors ── */
uint64_t rdb_save_time(void)            { return (uint64_t)g_last_save_time; }
uint64_t rdb_bgsave_total(void)         { return g_bgsave_total; }
uint64_t rdb_bgsave_skipped(void)       { return g_bgsave_skipped; }
int     rdb_bgsave_cooldown_sec(void)   { return (int)bgsave_cooldown(); }
int     rdb_bgsave_last_duration(void)  { return g_last_bgsave_duration_sec; }

/* =========================================================================
 * Binary-safe read helper (RDB load)
 * ========================================================================= */

static const char *mread(const char *p, const char *end, void *out, size_t n) {
    if (p + n > end) return NULL;
    memcpy(out, p, n);
    return p + n;
}

/* ── RDB entry serialization helpers ── */

/* Extract serializable data from an entry into out-params.
 * Returns 0 on success. */
static int rdb_entry_extract(kv_entry_t *e,
                             const char **key, int *klen,
                             const char **data, int *dlen,
                             long long *expire) {
    if (!e) return -1;
    *key    = entry_key(e);
    *klen   = (int)e->key_len;
    *data   = entry_payload(e);
    *dlen   = (int)entry_payload_len(e);
    *expire = e->expire;
    return 0;
}

/* Serialised size of one entry (RDB per-entry format):
 *   [klen:4][key:klen][dlen:4][data:dlen][expire:8] */
static inline size_t entry_serialized_size(kv_entry_t *e) {
    return sizeof(int) + (size_t)e->key_len
         + sizeof(int) + (size_t)entry_payload_len(e)
         + sizeof(long long);
}

/* Write one entry into a raw buffer pointer (no container overhead).
 * Returns the number of bytes written. */
static size_t serialize_entry_to_ptr(char *dst, kv_entry_t *e) {
    const char *key, *data;
    int klen, dlen;
    long long expire;

    if (rdb_entry_extract(e, &key, &klen, &data, &dlen, &expire) != 0)
        return 0;

    size_t total = entry_serialized_size(e);
    char *p = dst;
    memcpy(p, &klen,   sizeof(int));   p += sizeof(int);
    memcpy(p, key,     (size_t)klen);  p += klen;
    memcpy(p, &dlen,   sizeof(int));   p += sizeof(int);
    memcpy(p, data,    (size_t)dlen);  p += dlen;
    memcpy(p, &expire, sizeof(long long));
    return total;
}

/* Single-entry serialize into DynBuf (RDB per-entry format). */
static void serialize_entry_to_buf(DynBuf *out, kv_entry_t *e) {
    if (!e) return;
    size_t total = entry_serialized_size(e);
    dynbuf_reserve(out, out->len + (int)total);
    serialize_entry_to_ptr(out->buf + out->len, e);
    out->len += (int)total;
}

/* Single-entry serialize into rdb_buf_t (used by rdb_save_sync).
 * Same binary format as serialize_entry_to_buf. */
static void rdb_save_callback_buf(kv_entry_t *e, void *priv) {
    if (!e) return;
    rdb_buf_t *b = (rdb_buf_t *)priv;
    size_t total = entry_serialized_size(e);
    rdb_buf_reserve(b, total);
    serialize_entry_to_ptr(b->buf + b->len, e);
    b->len += total;
}

/* DynBuf serialization callback (for RDMA fullsync). */
static void rdb_serialize_cb_dynbuf(kv_entry_t *e, void *priv) {
    if (!e) return;
    serialize_entry_to_buf((DynBuf *)priv, e);
}

void rdb_serialize_to_buf(DynBuf *out) {
    int key_count = (int)dict_count(&g_dict);

    dynbuf_reserve(out, 16 + key_count * 64);

    char hdr[16];
    rdb_write_header(hdr, key_count);
    dynbuf_append(out, hdr, 16);

    store_scan(rdb_serialize_cb_dynbuf, out);
}

/* ── Incremental RDB serialiser (RDMA fullsync master) ────────────────── */

void rdb_serialize_begin(rdb_serializer_t *rs) {
    rs->iter        = dict_iter_new(&g_dict, 1);
    rs->header_sent = 0;
    /* 空库也必须发出 RDB header（key_count=0），否则 RDMA 全量同步会因
     * exhausted 提前置位而 0 batch 收尾，slave 收不到 IMM_DONE 卡死。 */
    rs->exhausted   = 0;
}

size_t rdb_serialize_next(rdb_serializer_t *rs, char *dst,
                          size_t max_bytes, int *done) {
    size_t written = 0;
    *done = 0;

    if (rs->exhausted) {
        *done = 1;
        return 0;
    }

    /* RDB header (16 bytes) on first call */
    if (!rs->header_sent) {
        if (max_bytes < 16) return 0;  /* need at least header room */

        int key_count = (int)dict_count(&g_dict);
        rdb_write_header(dst, key_count);
        written    += 16;
        dst        += 16;
        max_bytes  -= 16;
        rs->header_sent = 1;
    }

    /* Serialize entries until max_bytes exhausted or dict done */
    while (max_bytes >= 16) {  /* minimum one entry: 4+0+4+0+8 = 16 */
        kv_entry_t *e = dict_iter_peek(rs->iter);
        if (!e) {
            rs->exhausted = 1;
            break;
        }

        size_t sz = entry_serialized_size(e);
        if (sz > max_bytes) break;         /* entry doesn't fit → next batch */

        dict_iter_next(rs->iter);          /* consume */
        size_t n = serialize_entry_to_ptr(dst, e);
        written   += n;
        dst       += n;
        max_bytes -= n;
    }

    if (rs->exhausted) *done = 1;
    return written;
}

void rdb_serialize_end(rdb_serializer_t *rs) {
    if (rs->iter) {
        dict_iter_free(rs->iter);
        rs->iter = NULL;
    }
    rs->exhausted = 1;
}


/* =============================================================================
 * RDB synchronous save (single-pass: serialize → io_uring write → rename)
 * ============================================================================= */

void rdb_save_sync(void) {
    rdb_buf_t b  = {0};
    int key_count = (int)dict_count(&g_dict);

    /* Reserve estimate: 16-byte header + ~64 bytes per key */
    rdb_buf_reserve(&b, 16 + (size_t)key_count * 64);

    /* Write 16-byte header */
    rdb_write_header(b.buf + b.len, key_count);
    b.len += 16;

    /* Single-pass: serialize all entries */
    store_scan(rdb_save_callback_buf, &b);

    /* Write to file */
    char tmp_path[1024], dst_path[1024];
    strncpy(tmp_path, persist_path(RDB_TMP), sizeof(tmp_path) - 1);
    tmp_path[sizeof(tmp_path) - 1] = '\0';
    strncpy(dst_path, persist_path(RDB_REL), sizeof(dst_path) - 1);
    dst_path[sizeof(dst_path) - 1] = '\0';

    int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        fprintf(stderr, "[RDB] open tmp failed: %s\n", strerror(errno));
        free(b.buf);
        return;
    }

    rdb_write_uring(fd, b.buf, b.len);
    close(fd);
    rename(tmp_path, dst_path);

    free(b.buf);
    printf("[RDB] saved via io_uring, %zu bytes (%d keys)\n", b.len, key_count);
}


/* =============================================================================
 * RDB asynchronous save (fork + child writes via io_uring)
 *
 * Return codes:
 *    0  BGSAVE started normally
 *   -1  drop — child already running + pending slot occupied
 *   -2  scheduled — child running, coalesced as pending
 *   -3  cooldown — too soon after last BGSAVE start, request rejected
 * ============================================================================= */

int rdb_save_async_uring(void) {
    static int installed = 0;
    if (!installed) {
        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = rdb_sigchld;
        sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
        sigaction(SIGCHLD, &sa, NULL);
        installed = 1;
    }

    time_t now = time(NULL);

    /* ── Layer 2: adaptive cooldown (applies whether or not child is running) ── */
    if (now - g_last_bgsave_start < bgsave_cooldown()) {
        g_bgsave_skipped++;
        return -3;
    }

    /* ── Child already running ── */
    if (g_rdb_child_pid > 0) {
        /* Layer 3: already have a pending retry → drop */
        if (g_bgsave_pending) {
            g_bgsave_skipped++;
            fprintf(stderr, "[RDB] BGSAVE dropped: child running + pending full "
                    "(pid=%d, skipped=%llu)\n",
                    (int)g_rdb_child_pid,
                    (unsigned long long)g_bgsave_skipped);
            return -1;
        }
        /* Layer 1: coalesce — queue one pending retry */
        g_bgsave_pending = 1;
        g_bgsave_skipped++;
        return -2;
    }

    /* ── Normal path: no child running ── */
    g_last_bgsave_start = now;
    g_bgsave_pending    = 0;

    long long t_lock = get_time_now_real();

    pid_t pid = fork();
    if (pid < 0) {
        fprintf(stderr, "[RDB] fork failed (%s), falling back to sync save\n",
                strerror(errno));
        rdb_save_sync();
        rdb_bgsave_done();
        return 0;
    }
    if (pid == 0) {
        rdb_save_sync();
        fflush(stdout);
        fflush(stderr);
        _exit(0);
    }
    long long t_done = get_time_now_real();
    g_rdb_child_pid = pid;
    printf("[RDB] BGSAVE started, child pid=%d "
           "(fork stall: %lld ms)\n",
           (int)pid, t_done - t_lock);
    return 0;
}


/* =============================================================================
 * RDB load (mmap + streaming)
 * ============================================================================= */

int rdb_load_stream_begin(const char *buf, size_t len) {
    const char *p   = buf;
    const char *end = buf + len;

    int magic = 0, version = 0, db_num = 0, key_count = 0;
    if (!(p = mread(p, end, &magic,     sizeof(int)))) return -1;
    if (!(p = mread(p, end, &version,   sizeof(int)))) return -1;
    if (!(p = mread(p, end, &db_num,    sizeof(int)))) return -1;
    if (!(p = mread(p, end, &key_count, sizeof(int)))) return -1;

    if (magic != RDB_MAGIC) {
        printf("[RDB] stream load: magic mismatch (0x%x)\n", magic);
        return -1;
    }

    kvs_flush_all();
    printf("[RDB] stream begin: version=%d, expected keys=%d\n", version, key_count);
    return key_count;
}

int rdb_load_stream_append(const char *buf, size_t len) {
    const char *p   = buf;
    const char *end = buf + len;
    int loaded = 0;

    while (p < end) {
        int klen = 0, dlen = 0;
        long long expire = 0;

        if (!(p = mread(p, end, &klen, sizeof(int)))) break;
        if (p + klen > end) break;
        const char *ks = p;
        p += klen;

        if (!(p = mread(p, end, &dlen, sizeof(int)))) break;
        if (p + dlen > end) break;

        uint64_t h = kv_hash((const uint8_t *)ks, (uint32_t)klen);
        kv_entry_t *e = dict_add_raw(&g_dict, ks, (uint32_t)klen,
                                     KV_TYPE_STRING, KV_ENCODING_RAW,
                                     p, (uint32_t)dlen, h);
        p += dlen;

        if (!(p = mread(p, end, &expire, sizeof(long long)))) break;
        if (e && expire) e->expire = expire;
        loaded++;
    }

    return loaded;
}

void rdb_rebuild_all_encodings(void) {
    /* After RDB load via dict_add_raw(), all entries are KV_ENCODING_RAW.
     * Saved INT values were serialised as their 8-byte int64_t binary;
     * genuine RAW strings are ASCII text.  Distinguish by checking
     * whether the 8-byte payload consists entirely of digit characters:
     *   - all ASCII digits → genuine RAW string, leave as-is
     *   - contains non-digit bytes → binary int64 blob, upgrade to INT */
    kv_iter_t *iter = dict_iter_new(&g_dict, 1);
    kv_entry_t *e;
    while ((e = dict_iter_next(iter)) != NULL) {
        if (entry_type(e) != KV_TYPE_STRING) continue;
        if (entry_encoding(e) != KV_ENCODING_RAW) continue;

        const char *payload = entry_payload(e);
        uint32_t plen = entry_payload_len(e);

        /* Only 8-byte payloads can be saved INT blobs (int64_t binary). */
        if (plen != 8) continue;

        /* If all 8 bytes are ASCII digits, this is a genuine 8-char
         * numeric string — keep it as RAW. */
        int all_digits = 1;
        for (uint32_t i = 0; i < 8; i++) {
            unsigned char c = (unsigned char)payload[i];
            if (c < '0' || c > '9') { all_digits = 0; break; }
        }
        if (all_digits) continue;

        /* Contains non-digit bytes → saved int64_t binary blob.
         * Upgrade to KV_ENCODING_INT so GET/INCR use the fast path. */
        e->data[1] = (char)KV_ENCODING_INT;
    }
    dict_iter_free(iter);
}

int rdb_load_from_buf(const char *buf, size_t len) {
    if (len < 16) return -1;

    int key_count = rdb_load_stream_begin(buf, 16);
    if (key_count < 0) return -1;

    int loaded = rdb_load_stream_append(buf + 16, len - 16);
    if (loaded < 0) { dict_clear(&g_dict); return -1; }

    rdb_rebuild_all_encodings();

    printf("[RDB] buf load: %d keys, %zu bytes\n", loaded, len);
    return loaded;
}

void rdb_load_mmap(void) {
    char *base = NULL;
    size_t fsz = 0;
    int use_mmap = 0;

    int rc = file_read_full(persist_path(RDB_REL), &base, &fsz, &use_mmap, "RDB");
    if (rc == FILE_READ_OPEN_FAIL) {
        printf("[RDB] open failed\n");
        return;
    }
    if (rc != FILE_READ_OK) return;

    rdb_load_from_buf(base, fsz);

    if (use_mmap) {
        munmap(base, fsz);
    } else {
        free(base);
    }
}
