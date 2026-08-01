/* =========================================================
 * persist_rdb.c — RDB save (fork + io_uring) and load (mmap),
 *                dict scan helpers, and BGSAVE state machine.
 *
 * Public API (declared in persistence.h):
 *   rdb_save_sync / rdb_save_async_uring / rdb_bgsave_in_progress
 *   rdb_load_mmap / rdb_load_from_buf / rdb_load_stream_*
 *   rdb_serialize_to_buf / serialize_entry_to_buf
 *   rdb_rebuild_all_encodings
 * ========================================================= */

#include "persistence.h"
#include "kvstore.h"
#include "kv_type_hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <inttypes.h>
#include <liburing.h>
#include <sys/mman.h>
#include <sys/stat.h>
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
static volatile pid_t g_rdb_child_pid = 0;

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

/* =========================================================================
 * Dict full scan (used by RDB save and serialize)
 * ========================================================================= */

static void store_scan(void (*cb)(kv_entry_t *e, void *priv), void *priv) {
    kv_store_iter_t it;
    kv_store_iter_init(&it);
    kv_entry_t *e;
    while ((e = kv_store_iter_next(&it)) != NULL)
        cb(e, priv);
    kv_store_iter_free(&it);
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
 * BGSAVE state machine
 * ========================================================================= */

int rdb_bgsave_in_progress(void) {
    return (g_rdb_child_pid > 0) ? 1 : 0;
}

static void rdb_sigchld(int signo) {
    (void)signo;
    pid_t pid;
    int   status;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (pid == g_rdb_child_pid)
            g_rdb_child_pid = 0;
    }
}

/* =========================================================================
 * Binary-safe read helper (RDB load)
 * ========================================================================= */

static const char *mread(const char *p, const char *end, void *out, size_t n) {
    if (p + n > end) return NULL;
    memcpy(out, p, n);
    return p + n;
}

/* ── RDB entry payload accessor ── */
int rdb_entry_payload(kv_entry_t *e, const char **data, int *dlen) {
    if (!e) return -1;
    if (entry_type(e) == KV_TYPE_HASH && entry_encoding(e) == KV_ENCODING_DICT)
        return -1;
    *data = e->data;
    *dlen = (int)e->val_len;
    return 0;
}

/* Extract serializable data from an entry. Handles hash dict flattening.
 * Returns 0 on success; *owned is set when the caller must free it. */
static int rdb_entry_extract(kv_entry_t *e,
                              const char **key, int *klen,
                              const char **data, int *dlen,
                              long long *expire,
                              char **owned) {
    *owned = NULL;
    if (!e) return -1;

    uint32_t owned_len = 0;
    if (entry_type(e) == KV_TYPE_HASH && entry_encoding(e) == KV_ENCODING_DICT) {
        if (hash_flatten_to_packed(e, owned, &owned_len) != 0) return -1;
        *data = *owned;
        *dlen = (int)owned_len;
    } else if (rdb_entry_payload(e, data, dlen) != 0) {
        return -1;
    }

    *key    = entry_key(e);
    *klen   = (int)e->key_len;
    *expire = e->expire;
    return 0;
}

/* ── Serialization callbacks ── */

/* Single-entry serialize into rdb_buf_t (used by rdb_save_sync).
 * Format: [klen:4][key:klen][dlen:4][data:dlen][expire:8] */
static void rdb_save_callback_buf(kv_entry_t *e, void *priv) {
    const char *key, *data;
    int klen, dlen;
    long long expire;
    char *owned = NULL;

    if (rdb_entry_extract(e, &key, &klen, &data, &dlen, &expire, &owned) != 0)
        return;

    rdb_buf_t *b = (rdb_buf_t *)priv;
    size_t total = sizeof(int) + (size_t)klen + sizeof(int)
                 + (size_t)dlen + sizeof(long long);
    rdb_buf_reserve(b, total);
    char *dst = b->buf + b->len;
    memcpy(dst, &klen,   sizeof(int));   dst += sizeof(int);
    memcpy(dst, key,     (size_t)klen);  dst += klen;
    memcpy(dst, &dlen,   sizeof(int));   dst += sizeof(int);
    memcpy(dst, data,    (size_t)dlen);  dst += dlen;
    memcpy(dst, &expire, sizeof(long long));
    b->len += total;

    free(owned);
}

/* Single-entry serialize into DynBuf (used by RDMA fullsync).
 * Same binary format as rdb_save_callback_buf. */
void serialize_entry_to_buf(DynBuf *out, kv_entry_t *e) {
    const char *key, *data;
    int klen, dlen;
    long long expire;
    char *owned = NULL;

    if (rdb_entry_extract(e, &key, &klen, &data, &dlen, &expire, &owned) != 0)
        return;

    size_t total = sizeof(int) + (size_t)klen + sizeof(int)
                 + (size_t)dlen + sizeof(long long);
    dynbuf_reserve(out, out->len + (int)total);
    char *dst = out->buf + out->len;
    memcpy(dst, &klen,   sizeof(int));   dst += sizeof(int);
    memcpy(dst, key,     (size_t)klen);  dst += klen;
    memcpy(dst, &dlen,   sizeof(int));   dst += sizeof(int);
    memcpy(dst, data,    (size_t)dlen);  dst += dlen;
    memcpy(dst, &expire, sizeof(long long));
    out->len += (int)total;

    free(owned);
}

/* DynBuf serialization callback (for RDMA fullsync). */
static void rdb_serialize_cb_dynbuf(kv_entry_t *e, void *priv) {
    if (!e) return;
    serialize_entry_to_buf((DynBuf *)priv, e);
}

void rdb_serialize_to_buf(DynBuf *out) {
    int magic     = 0x524442;
    int version   = 2;
    int db_num    = 0;
    int key_count = (int)kv_store_count();

    dynbuf_reserve(out, 16 + key_count * 64);

    dynbuf_append(out, (const char *)&magic,     sizeof(int));
    dynbuf_append(out, (const char *)&version,   sizeof(int));
    dynbuf_append(out, (const char *)&db_num,    sizeof(int));
    dynbuf_append(out, (const char *)&key_count, sizeof(int));

    store_scan(rdb_serialize_cb_dynbuf, out);
}


/* =============================================================================
 * RDB synchronous save (single-pass: serialize → io_uring write → rename)
 * ============================================================================= */

void rdb_save_sync(void) {
    rdb_buf_t b  = {0};
    int key_count = (int)kv_store_count();

    /* Reserve estimate: 16-byte header + ~64 bytes per key */
    rdb_buf_reserve(&b, 16 + (size_t)key_count * 64);

    /* Write header */
    int magic   = 0x524442;
    int version = 2;
    int db_num  = 0;
    {
        size_t hl  = sizeof(int) * 4;
        rdb_buf_reserve(&b, hl);
        char *dst = b.buf + b.len;
        memcpy(dst, &magic,     sizeof(int)); dst += sizeof(int);
        memcpy(dst, &version,   sizeof(int)); dst += sizeof(int);
        memcpy(dst, &db_num,    sizeof(int)); dst += sizeof(int);
        memcpy(dst, &key_count, sizeof(int));
        b.len += hl;
    }

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
 * ============================================================================= */

void rdb_save_async_uring(void) {
    static int installed = 0;
    if (!installed) {
        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = rdb_sigchld;
        sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
        sigaction(SIGCHLD, &sa, NULL);
        installed = 1;
    }

    if (g_rdb_child_pid > 0) {
        printf("[RDB] BGSAVE already in progress (pid=%d), skipping\n",
               (int)g_rdb_child_pid);
        return;
    }

    long long t_lock = get_time_now_real();
    kv_store_lock_for_snapshot();
    long long t_locked = get_time_now_real();

    pid_t pid = fork();
    if (pid < 0) {
        fprintf(stderr, "[RDB] fork failed (%s), falling back to sync save\n",
                strerror(errno));
        kv_store_unlock_snapshot();
        rdb_save_sync();
        return;
    }
    if (pid == 0) {
        rdb_save_sync();
        fflush(stdout);
        fflush(stderr);
        _exit(0);
    }
    kv_store_unlock_snapshot();
    long long t_done = get_time_now_real();
    g_rdb_child_pid = pid;
    printf("[RDB] BGSAVE started, child pid=%d (snapshot stall: %lld ms acquire, %lld ms total)\n",
           (int)pid, t_locked - t_lock, t_done - t_lock);
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

    if (magic != 0x524442) {
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
        kv_entry_t *e = dict_add(kv_dict_for(h), ks, (uint32_t)klen, p, (uint32_t)dlen, h);
        p += dlen;

        if (!(p = mread(p, end, &expire, sizeof(long long)))) break;
        if (e && expire) e->expire = expire;
        if (e) hash_rebuild_encoding(e);
        loaded++;
    }

    return loaded;
}

void rdb_rebuild_all_encodings(void) {
    kv_store_iter_t it;
    kv_store_iter_init(&it);
    kv_entry_t *e;
    while ((e = kv_store_iter_next(&it)) != NULL)
        hash_rebuild_encoding(e);
    kv_store_iter_free(&it);
}

int rdb_load_from_buf(const char *buf, size_t len) {
    if (len < 16) return -1;

    int key_count = rdb_load_stream_begin(buf, 16);
    if (key_count < 0) return -1;

    int loaded = rdb_load_stream_append(buf + 16, len - 16);
    if (loaded < 0) { kv_store_clear(); return -1; }

    rdb_rebuild_all_encodings();

    printf("[RDB] buf load: %d keys, %zu bytes\n", loaded, len);
    return loaded;
}

void rdb_load_mmap(void) {
    int fd = open(persist_path(RDB_REL), O_RDONLY);
    if (fd < 0) {
        printf("[RDB] open failed\n");
        return;
    }
    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size <= 0) {
        close(fd);
        return;
    }
    size_t fsz = (size_t)st.st_size;
    char *base = (char *)mmap(NULL, fsz, PROT_READ, MAP_PRIVATE, fd, 0);

    if (base == MAP_FAILED) {
        printf("[RDB] mmap failed: %s, falling back to read()\n", strerror(errno));

        char *buf = malloc(fsz);
        if (!buf) { close(fd); return; }

        lseek(fd, 0, SEEK_SET);
        size_t total_read = 0;
        while (total_read < fsz) {
            ssize_t n = read(fd, buf + total_read, fsz - total_read);
            if (n <= 0) break;
            total_read += n;
        }
        close(fd);

        if (total_read == fsz) {
            rdb_load_from_buf(buf, fsz);
        } else {
            printf("[RDB] read failed: only read %zu/%zu bytes\n", total_read, fsz);
        }
        free(buf);
        return;
    }

    close(fd);
    madvise(base, fsz, MADV_SEQUENTIAL);

    rdb_load_from_buf(base, fsz);

    munmap(base, fsz);
}
