/* persist_rdb.c — RDB save (fork + io_uring write) and load (mmap). */

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

#define RDB_TMP "build/dump.rdb.tmp"


/* Dynamic buffer for RDB serialization */
typedef struct {
    char  *buf;
    size_t len;
    size_t cap;
} rdb_buf_t;

/* BGSAVE child pid, 0 = not running. */
static volatile pid_t g_rdb_child_pid = 0;
static time_t        g_last_save_time = 0;

/* Changes since last save (incremented on the write path). */
uint64_t g_rdb_changes = 0;

static void rdb_buf_reserve(rdb_buf_t *b, size_t need) {
    if (b->len + need <= b->cap) return;
    size_t ncap = b->cap ? b->cap : 64 * 1024;
    while (ncap < b->len + need)
        ncap *= 2;
    b->buf = (char *)realloc(b->buf, ncap);
    b->cap = ncap;
}

/* Write the 16-byte RDB header (magic, version, db_num, key_count). */
static void rdb_write_header(char *dst, int key_count) {
    memcpy(dst, &key_count, sizeof(int));
}

/* Full dict scan — used by RDB save. */
static void store_scan(void (*cb)(kv_entry_t *e, void *priv), void *priv) {
    kv_iter_t *iter = dict_iter_new(&g_dict, 1);
    kv_entry_t *e;
    while ((e = dict_iter_next(iter)) != NULL)
        cb(e, priv);
    dict_iter_free(iter);
}

/* Chunked io_uring write + fdatasync, with a plain write() fallback. */
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

/* Serialised size of one entry (RDB per-entry format):
 *   [klen:4][key:klen][dlen:4][data:dlen][expire:8] */
static inline size_t entry_serialized_size(kv_entry_t *e) {
    return sizeof(int) + (size_t)e->key_len
         + sizeof(int) + (size_t)entry_payload_len(e)
         + sizeof(long long);
}

/* Serialize one entry into a raw buffer pointer. */
static size_t serialize_entry_to_ptr(char *dst, kv_entry_t *e) {
    const char *key  = entry_key(e);
    const char *data = entry_payload(e);
    int  klen = (int)e->key_len;
    int  dlen = (int)entry_payload_len(e);
    long long expire = e->expire;

    char *p = dst;
    memcpy(p, &klen,   sizeof(int));   p += sizeof(int);
    memcpy(p, key,     (size_t)klen);  p += klen;
    memcpy(p, &dlen,   sizeof(int));   p += sizeof(int);
    memcpy(p, data,    (size_t)dlen);  p += dlen;
    memcpy(p, &expire, sizeof(long long));
    return entry_serialized_size(e);
}

/* Single-entry serialize into rdb_buf_t (used by rdb_save_sync). */
static void rdb_save_callback_buf(kv_entry_t *e, void *priv) {
    if (!e) return;
    rdb_buf_t *b = (rdb_buf_t *)priv;
    size_t total = entry_serialized_size(e);
    rdb_buf_reserve(b, total);
    serialize_entry_to_ptr(b->buf + b->len, e);
    b->len += total;
}

/* ── Incremental RDB serialiser (RDMA fullsync master) ── */

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

// ✅ 修正
    if (!rs->header_sent) {
        if (max_bytes < 4) return 0;  // 只需要 4 字节
        
        int key_count = (int)dict_count(&g_dict);
        rdb_write_header(dst, key_count);
        written += 4;   // 只加 4
        dst += 4;       // 偏移 4
        max_bytes -= 4;
        rs->header_sent = 1;
    }

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

/* ── Synchronous save: serialize → io_uring write → rename ── */

void rdb_save_sync(void) {
    rdb_buf_t b  = {0};
    int key_count = (int)dict_count(&g_dict);

    rdb_buf_reserve(&b, 4 + (size_t)key_count * 64);

    rdb_write_header(b.buf + b.len, key_count);
    b.len += 4;

    store_scan(rdb_save_callback_buf, &b);

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

/* ── Asynchronous save (fork + child writes via io_uring) ──
 * Redis-style: only one BGSAVE at a time.  Returns 0 on success,
 * -1 if a save is already in progress. */

static void rdb_mark_saved(void) {
    g_last_save_time = time(NULL);
    g_rdb_changes    = 0;
}

uint64_t rdb_save_time(void) {
    return (uint64_t)g_last_save_time;
}

static void rdb_sigchld(int signo) {
    (void)signo;
    pid_t pid;
    int   status;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (pid == g_rdb_child_pid) {
            g_rdb_child_pid = 0;
            rdb_mark_saved();
        }
    }
}

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

    if (g_rdb_child_pid > 0)
        return -1;   /* BGSAVE already in progress */

    pid_t pid = fork();
    if (pid < 0) {
        fprintf(stderr, "[RDB] fork failed (%s), falling back to sync save\n",
                strerror(errno));
        rdb_save_sync();
        rdb_mark_saved();
        return 0;
    }
    if (pid == 0) {
        rdb_save_sync();
        fflush(stdout);
        fflush(stderr);
        _exit(0);
    }
    g_rdb_child_pid = pid;
    printf("[RDB] BGSAVE started, child pid=%d\n", (int)pid);
    return 0;
}

/* ── Load ── */

static const char *mread(const char *p, const char *end, void *out, size_t n) {
    if (p + n > end) return NULL;
    memcpy(out, p, n);
    return p + n;
}

int rdb_load_stream_begin(const char *buf, size_t len) {
    if (len < 4) return -1;  // 至少要有 key_count
    
    int key_count = 0;
    memcpy(&key_count, buf, sizeof(int));
    
    // ✅ 合理性检查
    if (key_count < 0 || key_count > 100000000) {
        fprintf(stderr, "[RDB] invalid key_count: %d\n", key_count);
        return -1;
    }
    
    kvs_flush_all();
    printf("[RDB] loading %d keys\n", key_count);
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
        kv_entry_t *e = dict_add(&g_dict, ks, (uint32_t)klen,
                                 p, (uint32_t)dlen, h);
        p += dlen;

        if (!(p = mread(p, end, &expire, sizeof(long long)))) break;
        if (e && expire) e->expire = expire;
        loaded++;
    }

    return loaded;
}

int rdb_load_from_buf(const char *buf, size_t len) {
    if (len < 4) return -1;
    
    int expected = rdb_load_stream_begin(buf, 4);
    if (expected < 0) return -1;
    
    int loaded = rdb_load_stream_append(buf + 4, len - 4);
    if (loaded < 0) {
        dict_clear(&g_dict);
        return -1;
    }
    
    // ✅ 核心校验
    if (loaded != expected) {
        fprintf(stderr, "[RDB] corruption: expected %d keys, got %d\n", 
                expected, loaded);
        dict_clear(&g_dict);
        return -1;
    }
    
    printf("[RDB] loaded %d keys\n", loaded);
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
