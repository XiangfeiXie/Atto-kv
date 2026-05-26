/* =========================================================
 * persistence.c
 *
 *  持久化层当前职责：
 *  1) WAL：写命令进入 journal 内存 backlog 后，由 wal_persist 使用
 *     io_uring 追加到 build/incr.wal；WAL record 格式独立于复制协议。
 *  2) WAL replay：启动时 mmap 读取 incr.wal，校验 JWAL header + CRC，
 *     然后按 RESP 命令回放到 KV 引擎。
 *  3) RDB：保留 fork/io_uring 异步保存、mmap 加载，以及需要的纯内存
 *     序列化/流式加载接口。RDB/WAL 与主从同步不相互依赖，只通过
 *     KV 引擎和 journal record 这种通用数据结构衔接。
 * ========================================================= */

#define _GNU_SOURCE
#include "persistence.h"
#include "kvstore.h"
#include "wal_persist.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <libgen.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/eventfd.h>
#include <stdatomic.h>
#include <liburing.h>
#include <inttypes.h>


/* ── 项目根目录：由 persist_set_basedir 在启动时设置 ── */
static char  g_basedir[1024] = ".";
static int   g_basedir_set = 0;

#define RDB_TMP  "build/dump.rdb.tmp"

/* 将相对路径拼接到项目根目录上，返回静态 buffer */
const char *persist_path(const char *rel) {
    static char buf[1024];
    int n = snprintf(buf, sizeof(buf), "%s/%s", g_basedir, rel);
    if (n < 0 || (size_t)n >= sizeof(buf)) return rel; /* 截断时回退到相对路径 */
    return buf;
}

void persist_set_basedir(const char *argv0) {
    if (g_basedir_set) return;

    /* 1) 解析二进制真实路径 */
    char binpath[1024] = {0};
    if (argv0 && argv0[0] == '/') {
        /* 绝对路径：直接使用 */
        strncpy(binpath, argv0, sizeof(binpath) - 1);
    } else if (argv0) {
        /* 相对路径：用 /proc/self/exe 兜底 */
        ssize_t n = readlink("/proc/self/exe", binpath, sizeof(binpath) - 1);
        if (n <= 0 && realpath(argv0, binpath) == NULL) {
            /* 最后尝试：getcwd + argv0 */
            if (getcwd(binpath, sizeof(binpath) - 1)) {
                size_t blen = strlen(binpath);
                snprintf(binpath + blen, sizeof(binpath) - blen - 1, "/%s", argv0);
            }
        }
    } else {
        ssize_t n = readlink("/proc/self/exe", binpath, sizeof(binpath) - 1);
        if (n > 0) binpath[n] = '\0';
    }

    /* 2) 取二进制所在目录 */
    char *d = dirname(binpath);

    /* 3) 如果所在目录名为 build，则项目根目录为上一级；
     *    否则项目根目录就是二进制所在目录（就地构建）。 */
    char *dname = basename(d); /* 注意：basename 可能修改 d */
    /* dirname 可能修改 binpath，所以重新构造 */
    char dircopy[1024];
    strncpy(dircopy, binpath, sizeof(dircopy) - 1);
    dircopy[sizeof(dircopy) - 1] = '\0';
    char *dd = dirname(dircopy);
    char *dn = strrchr(dd, '/');
    if (dn) dn++; else dn = dd;

    if (strcmp(dn, "build") == 0) {
        /* project_root = parent of build/ */
        char *last = strrchr(dd, '/');
        if (last) *last = '\0';
        strncpy(g_basedir, dd, sizeof(g_basedir) - 1);
    } else {
        strncpy(g_basedir, dd, sizeof(g_basedir) - 1);
    }

    g_basedir[sizeof(g_basedir) - 1] = '\0';
    g_basedir_set = 1;
}

/* ========================= 引擎数据访问（与 kvstore.c 保持一致） ========================= */
/* g_dict is declared in kvstore.h, defined in kvstore.c */

/* 由 kvstore.c 提供 */
KVItem* create_kvitem(char *value_str, char *expire_str);

/* =============================================================================
 * Part 1. AOF — unified with WAL replication
 *
 * The legacy AOF async writer (ring buffer + io_uring worker) has been
 * retired.  Every write command flows through the current replication
 * journal path, which fans out to XDP/AF_XDP via repl_stream.
 * feeds the unified journal ring.  The consumer thread serialises records
 * to wal/incr.wal (binary WAL + CRC32) for crash recovery.
 *
 * aof_async_append  →  current journal path      (journal the command)
 * aof_reload_mmap   →  reads wal/incr.wal        (crash recovery)
 * ============================================================================= */

int aof_async_init(void) {
    printf("[AOF] unified with journal persistence — writes go to %s\n", persist_path(WAL_REL));
    return 0;
}

void aof_async_shutdown(void) {
}

void aof_async_append(const char *buf, int len) {
    (void)buf;
    (void)len;
}

/* =============================================================================
 * Part 2. RDB 异步保存：fork + 子进程使用 io_uring 批量写
 * ============================================================================= */

/* 一块动态增长的内存 buffer（子进程内序列化用） */
typedef struct {
    char *buf;
    size_t len;
    size_t cap;
} rdb_buf_t;

static void rdb_buf_reserve(rdb_buf_t *b, size_t need) {
    if (b->len + need <= b->cap) return;
    size_t ncap = b->cap ? b->cap : 64 * 1024;
    while (ncap < b->len + need) ncap *= 2;
    b->buf = (char *)realloc(b->buf, ncap);
    b->cap = ncap;
}
static void rdb_buf_put(rdb_buf_t *b, const void *p, size_t n) {
    rdb_buf_reserve(b, n);
    memcpy(b->buf + b->len, p, n);
    b->len += n;
}

#include "kv_dict.h"
#include "kv_type_hash.h"

/* RDB now persists only the STRING exact-key cache. */
static inline int rdb_entry_payload(kv_entry_t *e, const char **data, int *dlen) {
    if (!e) return -1;
    if (entry_type(e) == KV_TYPE_HASH && entry_encoding(e) == KV_ENCODING_DICT) return -1;
    *data = e->data;
    *dlen = (int)e->val_len;
    return 0;
}

/* Serialize one STRING dict entry to RDB binary format. */
static void rdb_save_callback_buf(kv_entry_t *e, void *priv) {
    char *owned = NULL;
    const char *data_ptr = NULL;
    int dlen = 0;
    uint32_t owned_len = 0;
    if (e && entry_type(e) == KV_TYPE_HASH && entry_encoding(e) == KV_ENCODING_DICT) {
        if (hash_flatten_to_packed(e, &owned, &owned_len) != 0) return;
        data_ptr = owned;
        dlen = (int)owned_len;
    } else if (rdb_entry_payload(e, &data_ptr, &dlen) != 0) {
        return;
    }

    rdb_buf_t *b = (rdb_buf_t *)priv;
    const char *key = entry_key(e);
    int klen = (int)e->key_len;
    long long expire = e->expire;

    rdb_buf_put(b, &klen, sizeof(int));
    rdb_buf_put(b, key, (size_t)klen);
    rdb_buf_put(b, &dlen, sizeof(int));
    rdb_buf_put(b, data_ptr, (size_t)dlen);
    rdb_buf_put(b, &expire, sizeof(long long));
    free(owned);
}

void serialize_entry_to_buf(DynBuf *out, kv_entry_t *e) {
    char *owned = NULL;
    const char *data_ptr = NULL;
    int dlen = 0;
    uint32_t owned_len = 0;
    if (e && entry_type(e) == KV_TYPE_HASH && entry_encoding(e) == KV_ENCODING_DICT) {
        if (hash_flatten_to_packed(e, &owned, &owned_len) != 0) return;
        data_ptr = owned;
        dlen = (int)owned_len;
    } else if (rdb_entry_payload(e, &data_ptr, &dlen) != 0) {
        return;
    }

    const char *key = entry_key(e);
    int klen = (int)e->key_len;
    long long expire = e->expire;

    dynbuf_append(out, (const char *)&klen, sizeof(int));
    dynbuf_append(out, key, klen);
    dynbuf_append(out, (const char *)&dlen, sizeof(int));
    dynbuf_append(out, data_ptr, dlen);
    dynbuf_append(out, (const char *)&expire, sizeof(long long));
    free(owned);
}

int serialize_entry_inline(char *dst, size_t max_space, kv_entry_t *e) {
    char *owned = NULL;
    const char *data_ptr = NULL;
    int dlen = 0;
    uint32_t owned_len = 0;
    if (e && entry_type(e) == KV_TYPE_HASH && entry_encoding(e) == KV_ENCODING_DICT) {
        if (hash_flatten_to_packed(e, &owned, &owned_len) != 0) return 0;
        data_ptr = owned;
        dlen = (int)owned_len;
    } else if (rdb_entry_payload(e, &data_ptr, &dlen) != 0) {
        return 0;
    }

    const char *key = entry_key(e);
    int klen = (int)e->key_len;
    long long expire = e->expire;
    int total = (int)(sizeof(int) + klen + sizeof(int) + dlen + sizeof(long long));
    if (total > (int)max_space) { free(owned); return -1; }

    char *q = dst;
    memcpy(q, &klen, sizeof(int)); q += sizeof(int);
    memcpy(q, key, klen);          q += klen;
    memcpy(q, &dlen, sizeof(int)); q += sizeof(int);
    memcpy(q, data_ptr, dlen);     q += dlen;
    memcpy(q, &expire, sizeof(long long));
    free(owned);
    return total;
}

/* Same RDB binary format, appends to a DynBuf (for RDMA fullsync). */
static void rdb_serialize_cb_dynbuf(kv_entry_t *e, void *priv) {
    if (!e) return;
    serialize_entry_to_buf((DynBuf *)priv, e);
}

/* Set to 1 inside the BGSAVE fork child so shards_scan() switches to
 * the no-lock iterator.  Crucial for MT mode: at fork time some worker
 * may have held a shard wrlock; the child inherits the locked state
 * with no thread to release it, so the regular rdlock-walking iterator
 * would deadlock. */
static int g_rdb_in_fork_child = 0;

/* Iterate every entry across all shards.  In the BGSAVE fork child the
 * caller has already serialised access via kv_shards_lock_all_for_snapshot
 * before fork; the child sees a frozen COW image and walks it without
 * touching any rwlocks.  In the parent (sync save / replication
 * full-sync) the iterator takes a per-shard rdlock as it goes. */
static void shards_scan(void (*cb)(kv_entry_t *e, void *priv), void *priv) {
    kv_shards_iter_t it;
    if (g_rdb_in_fork_child)
        kv_shards_iter_init_nolock(&it);
    else
        kv_shards_iter_init(&it);
    kv_entry_t *e;
    while ((e = kv_shards_iter_next(&it)) != NULL)
        cb(e, priv);
    kv_shards_iter_free(&it);
}

void rdb_serialize_to_buf(DynBuf *out) {
    int magic = 0x524442;
    int version = 2;
    int db_num = 0;
    int key_count = (int)kv_shards_count();

    /* Reserve: header (16) + per-entry estimate (key + value + overhead ~= 256) */
    dynbuf_reserve(out, 16 + key_count * 256);

    dynbuf_append(out, (const char *)&magic,     sizeof(int));
    dynbuf_append(out, (const char *)&version,   sizeof(int));
    dynbuf_append(out, (const char *)&db_num,    sizeof(int));
    dynbuf_append(out, (const char *)&key_count, sizeof(int));

    shards_scan(rdb_serialize_cb_dynbuf, out);
}

/* 子进程：使用 io_uring 将整个 buffer 分块写入，最后 fsync */
static void rdb_write_uring(int fd, const char *data, size_t total) {
    const size_t CHUNK = 1 * 1024 * 1024; /* 1MB/SQE */
    const int    DEPTH = 32;

    struct io_uring ring;
    if (io_uring_queue_init(DEPTH, &ring, 0) < 0) {
        /* 回退：普通 write */
        size_t off = 0;
        while (off < total) {
            ssize_t w = write(fd, data + off, total - off);
            if (w <= 0) break;
            off += (size_t)w;
        }
        return;
    }

    size_t off = 0;
    int    inflight = 0;
    while (off < total || inflight > 0) {
        /* 尽量把 SQ 填满 */
        while (off < total && inflight < DEPTH) {
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
            if (!sqe) break;
            size_t n = total - off;
            if (n > CHUNK) n = CHUNK;
            io_uring_prep_write(sqe, fd, data + off, n, off);
            off += n;
            inflight++;
        }
        if (inflight > 0) {
            io_uring_submit(&ring);

            /* 批量收割所有已完成的 CQE */
            int reaped = 0;
            while (inflight > 0) {
                struct io_uring_cqe *cqe = NULL;
                int ret = io_uring_peek_cqe(&ring, &cqe);
                if (ret == -EAGAIN) {
                    /* 没有更多完成的 CQE，等待至少一个 */
                    if (reaped == 0 && io_uring_wait_cqe(&ring, &cqe) == 0) {
                        if (cqe->res < 0) {
                            fprintf(stderr, "[RDB] uring write error: %s\n", strerror(-cqe->res));
                        }
                        io_uring_cqe_seen(&ring, cqe);
                        inflight--;
                        reaped++;
                    }
                    break;
                }
                if (ret == 0 && cqe) {
                    if (cqe->res < 0) {
                        fprintf(stderr, "[RDB] uring write error: %s\n", strerror(-cqe->res));
                    }
                    io_uring_cqe_seen(&ring, cqe);
                    inflight--;
                    reaped++;
                }
            }
        }
    }

    /* 用 io_uring fdatasync（IOSQE_IO_DRAIN 保证所有写入完成后才执行）
     * 比阻塞 fsync() 少一次内核切换，且只刷数据不刷文件元数据 */
    {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        if (sqe) {
            io_uring_prep_fsync(sqe, fd, IORING_FSYNC_DATASYNC);
            sqe->flags |= IOSQE_IO_DRAIN;   /* 等前面所有写 SQE 完成 */
            io_uring_submit(&ring);
            struct io_uring_cqe *cqe = NULL;
            if (io_uring_wait_cqe(&ring, &cqe) == 0)
                io_uring_cqe_seen(&ring, cqe);
        } else {
            fdatasync(fd);   /* SQ 满时回退 */
        }
    }
    io_uring_queue_exit(&ring);
}

void rdb_save_sync(void) {
    /* 1) 序列化到内存 buffer */
    rdb_buf_t b = {0};

    int magic = 0x524442;
    int version = 2;
    int db_num = 0;
    int key_count = (int)kv_shards_count();

    rdb_buf_put(&b, &magic, sizeof(int));
    rdb_buf_put(&b, &version, sizeof(int));
    rdb_buf_put(&b, &db_num, sizeof(int));
    rdb_buf_put(&b, &key_count, sizeof(int));

    shards_scan(rdb_save_callback_buf, &b);

    /* 2) 写临时文件 + rename，保证原子性 */
    /* persist_path() 返回同一 static buffer，必须先拷贝再调用第二次，
     * 否则 tmp 和 dst 均指向同一内存，rename 变成 no-op。 */
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
    printf("[RDB] saved via io_uring, %zu bytes\n", b.len);
}

/* Track the currently running BGSAVE child so we never run two at once. */
static volatile pid_t g_rdb_child_pid = 0;

/* Track the currently running BGREWRITEAOF child */
static volatile pid_t g_aof_rewrite_child_pid = 0;
static int g_aof_rewrite_in_progress = 0;

/* Rewrite buffer: captures writes during AOF rewrite */
typedef struct {
    char *data;
    size_t len;
    size_t cap;
} aof_rewrite_buf_t;

static aof_rewrite_buf_t g_aof_rewrite_buf = {0};
static pthread_mutex_t g_aof_rewrite_mu = PTHREAD_MUTEX_INITIALIZER;

/* SIGCHLD 处理：回收已完成的 BGSAVE 子进程，避免僵尸 */
static void rdb_sigchld(int signo) {
    (void)signo;
    pid_t pid;
    int   status;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (pid == g_rdb_child_pid)
            g_rdb_child_pid = 0;  /* slot is free for next BGSAVE */
        if (pid == g_aof_rewrite_child_pid) {
            g_aof_rewrite_child_pid = 0;
            if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
                printf("[AOF] BGREWRITEAOF child completed successfully\n");
            } else {
                fprintf(stderr, "[AOF] BGREWRITEAOF child failed (status=%d)\n", status);
                pthread_mutex_lock(&g_aof_rewrite_mu);
                g_aof_rewrite_in_progress = 0;
                free(g_aof_rewrite_buf.data);
                g_aof_rewrite_buf.data = NULL;
                g_aof_rewrite_buf.len = 0;
                g_aof_rewrite_buf.cap = 0;
                pthread_mutex_unlock(&g_aof_rewrite_mu);
            }
        }
    }
}

/* Append to rewrite buffer (called during rewrite) */
static void aof_rewrite_buf_append(const char *data, size_t len) {
    pthread_mutex_lock(&g_aof_rewrite_mu);
    if (!g_aof_rewrite_in_progress) {
        pthread_mutex_unlock(&g_aof_rewrite_mu);
        return;
    }

    if (g_aof_rewrite_buf.len + len > g_aof_rewrite_buf.cap) {
        size_t new_cap = g_aof_rewrite_buf.cap ? g_aof_rewrite_buf.cap * 2 : 64 * 1024;
        while (new_cap < g_aof_rewrite_buf.len + len) new_cap *= 2;
        char *new_data = realloc(g_aof_rewrite_buf.data, new_cap);
        if (!new_data) {
            pthread_mutex_unlock(&g_aof_rewrite_mu);
            return;
        }
        g_aof_rewrite_buf.data = new_data;
        g_aof_rewrite_buf.cap = new_cap;
    }

    memcpy(g_aof_rewrite_buf.data + g_aof_rewrite_buf.len, data, len);
    g_aof_rewrite_buf.len += len;
    pthread_mutex_unlock(&g_aof_rewrite_mu);
}

/* Generate canonical RESP command for an entry */
static void aof_rewrite_entry(kv_entry_t *e, void *priv) {
    if (!e) return;
    rdb_buf_t *b = (rdb_buf_t *)priv;

    const char *key = entry_key(e);
    int klen = (int)e->key_len;
    long long expire = e->expire;

    /* Generate SET command for STRING type */
    if (entry_type(e) == KV_TYPE_STRING) {
        const char *val = e->data;
        int vlen = (int)e->val_len;

        /* *3\r\n$3\r\nSET\r\n$<klen>\r\n<key>\r\n$<vlen>\r\n<value>\r\n */
        char header[256];
        int hlen = snprintf(header, sizeof(header),
                           "*3\r\n$3\r\nSET\r\n$%d\r\n", klen);
        rdb_buf_put(b, header, hlen);
        rdb_buf_put(b, key, klen);
        rdb_buf_put(b, "\r\n", 2);

        hlen = snprintf(header, sizeof(header), "$%d\r\n", vlen);
        rdb_buf_put(b, header, hlen);
        rdb_buf_put(b, val, vlen);
        rdb_buf_put(b, "\r\n", 2);

        /* Add EXPIREAT if TTL is set */
        if (expire > 0) {
            hlen = snprintf(header, sizeof(header),
                           "*3\r\n$8\r\nEXPIREAT\r\n$%d\r\n", klen);
            rdb_buf_put(b, header, hlen);
            rdb_buf_put(b, key, klen);
            rdb_buf_put(b, "\r\n", 2);

            hlen = snprintf(header, sizeof(header), "$%lld\r\n%lld\r\n",
                           (long long)snprintf(NULL, 0, "%lld", expire), expire);
            rdb_buf_put(b, header, hlen);
        }
    }
    /* For other types (HASH, LIST, SET, ZSET), we'd generate HSET, RPUSH, SADD, ZADD commands.
     * For simplicity in this求职项目, we only handle STRING type. */
}

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

    /* Reject concurrent saves: one child at a time. */
    if (g_rdb_child_pid > 0) {
        printf("[RDB] BGSAVE already in progress (pid=%d), skipping\n",
               (int)g_rdb_child_pid);
        return;
    }

    /* Phase 4B.4: take all shard wrlocks before fork so the child
     * inherits a point-in-time consistent snapshot.  Workers stall
     * here for the duration of: 32 wrlock acquisitions + fork() +
     * 32 wrlock releases (a few hundred microseconds typically). */
    long long t_lock = get_time_now_real();
    kv_shards_lock_all_for_snapshot();
    long long t_locked = get_time_now_real();

    pid_t pid = fork();
    if (pid < 0) {
        fprintf(stderr, "[RDB] fork failed (%s), falling back to sync save\n",
                strerror(errno));
        kv_shards_unlock_all_snapshot();
        /* Fallback runs in the parent without the snapshot guarantee;
         * acceptable because fork-fail is rare and we still produce a
         * per-shard-consistent dump. */
        rdb_save_sync();
        return;
    }
    if (pid == 0) {
        /* Child: locks are "held" in our COW memory image, but no
         * thread will ever release them.  Switch shards_scan to the
         * no-lock iterator and serialise. */
        g_rdb_in_fork_child = 1;
        rdb_save_sync();
        fflush(stdout);
        fflush(stderr);
        _exit(0);
    }
    /* Parent: release the snapshot locks immediately so workers resume.
     * The child continues against its frozen COW image. */
    kv_shards_unlock_all_snapshot();
    long long t_done = get_time_now_real();
    g_rdb_child_pid = pid;
    printf("[RDB] BGSAVE started, child pid=%d (snapshot stall: %lld ms acquire, %lld ms total)\n",
           (int)pid, t_locked - t_lock, t_done - t_lock);
}

/* =============================================================================
 * Part 2b. AOF rewrite — Phase 7.1.0 stub.
 *
 * Status: NOT YET IMPLEMENTED.  The BGREWRITEAOF command currently
 * returns -ERR, and aof_rewrite_bg() returns -1.  AOF on this dev
 * box never grows beyond ~10 MB during normal smoke / dev_quick
 * runs, so rewrite is a no-op feature for current workloads.  When
 * a real workload pushes AOF past ~100 MB the implementation
 * outlined below should be slotted in — every piece of infrastructure
 * already exists in this file, so the wiring is mostly mechanical.
 *
 * ─── Design when we DO implement it (V2 plan) ───
 *
 * 1. Trigger:
 *    aof_rewrite_bg() is called by BGREWRITEAOF or by an
 *    auto-trigger (e.g. AOF doubled in size since last rewrite).
 *    Reject if g_aof_rewrite_pid != 0 (already running, like the
 *    existing g_rdb_child_pid guard does for BGSAVE).
 *
 * 2. Snapshot:
 *    Mirror rdb_save_async_uring exactly:
 *      kv_shards_lock_all_for_snapshot();   (Phase 4B.4)
 *      pid_t pid = fork();                  (parent unlocks immediately)
 *    Child gets a frozen COW image just like RDB; safe to walk
 *    via kv_shards_iter_init_nolock + g_rdb_in_fork_child=1.  We
 *    can reuse the same fork-child flag because BGSAVE and
 *    BGREWRITEAOF will refuse to overlap (one child slot).
 *
 * 3. Child output:
 *    For each entry, emit ONE canonical RESP command that recreates
 *    it (SET / HSET / RPUSH / SADD / ZADD plus EXPIREAT if TTL set).
 *    Use io_uring just like rdb_save_async_uring does:
 *    chunk-write to build/appendonly.aof.tmp, then fsync, then exit.
 *
 * 4. The rewrite buffer (the actually hard part):
 *    Between the fork and the child's exit, the parent keeps
 *    serving writes that mutate the live dict.  Those writes
 *    are NOT in the child's snapshot.  We tee them into an
 *    in-memory "rewrite buffer" (DynBuf protected by an existing
 *    producer mu, cf. wal_repl.c's g_journal_producer_mu).  Hook:
 *    aof_async_append() checks g_aof_rewrite_in_progress and, if
 *    set, also pushes a copy into the rewrite buffer.
 *
 * 5. Finalize (on SIGCHLD for the rewrite child):
 *    SIGCHLD handler is NOT a safe place for fsync/rename.  Set a
 *    flag g_aof_rewrite_done, polled by the proactor worker 0 tick:
 *      a) open appendonly.aof.tmp O_APPEND
 *      b) write out the rewrite buffer (under producer mu)
 *      c) fsync, close
 *      d) atomic rename appendonly.aof.tmp → appendonly.aof
 *      e) clear g_aof_rewrite_in_progress, free rewrite buffer
 *
 * 6. Failure modes:
 *    - fork fails: free buffer, fall through, return -1.
 *    - child crashes: SIGCHLD reaper sees nonzero exit, drops
 *      buffer, leaves old AOF in place.
 *    - rename fails (disk full): same — old AOF preserved.
 *
 * The whole thing is ~200 lines mirroring rdb_save_async_uring's
 * shape.  Do NOT inline the rewrite buffer copy on the dispatcher
 * hot path; aof_async_append already holds the producer mu, so the
 * extra dynbuf_cat is essentially free there.
 * ============================================================================= */

int aof_rewrite_bg(void) {
    static int installed = 0;
    if (!installed) {
        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = rdb_sigchld;
        sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
        sigaction(SIGCHLD, &sa, NULL);
        installed = 1;
    }

    /* Reject concurrent rewrites */
    if (g_aof_rewrite_child_pid > 0) {
        printf("[AOF] BGREWRITEAOF already in progress (pid=%d), skipping\n",
               (int)g_aof_rewrite_child_pid);
        return 0;
    }

    /* Also reject if BGSAVE is running (share the same fork-child slot) */
    if (g_rdb_child_pid > 0) {
        printf("[AOF] BGSAVE in progress, cannot start BGREWRITEAOF\n");
        return -1;
    }

    /* Initialize rewrite buffer */
    pthread_mutex_lock(&g_aof_rewrite_mu);
    g_aof_rewrite_in_progress = 1;
    g_aof_rewrite_buf.len = 0;
    if (!g_aof_rewrite_buf.data) {
        g_aof_rewrite_buf.cap = 64 * 1024;
        g_aof_rewrite_buf.data = malloc(g_aof_rewrite_buf.cap);
    }
    pthread_mutex_unlock(&g_aof_rewrite_mu);

    /* Take snapshot locks before fork */
    kv_shards_lock_all_for_snapshot();

    pid_t pid = fork();
    if (pid < 0) {
        fprintf(stderr, "[AOF] fork failed (%s)\n", strerror(errno));
        kv_shards_unlock_all_snapshot();
        pthread_mutex_lock(&g_aof_rewrite_mu);
        g_aof_rewrite_in_progress = 0;
        pthread_mutex_unlock(&g_aof_rewrite_mu);
        return -1;
    }

    if (pid == 0) {
        /* Child process: generate compacted AOF */
        g_rdb_in_fork_child = 1;

        /* Serialize all entries to RESP commands */
        rdb_buf_t b = {0};
        shards_scan(aof_rewrite_entry, &b);

        /* Write to temp file using io_uring */
        const char *tmp_path = persist_path("build/appendonly.aof.tmp");
        int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            fprintf(stderr, "[AOF] child: open tmp failed: %s\n", strerror(errno));
            free(b.buf);
            _exit(1);
        }

        rdb_write_uring(fd, b.buf, b.len);
        close(fd);
        free(b.buf);

        printf("[AOF] child: wrote %zu bytes to temp AOF\n", b.len);
        fflush(stdout);
        fflush(stderr);
        _exit(0);
    }

    /* Parent: release locks immediately */
    kv_shards_unlock_all_snapshot();
    g_aof_rewrite_child_pid = pid;
    printf("[AOF] BGREWRITEAOF started, child pid=%d\n", (int)pid);

    return 0;
}

/* Finalize AOF rewrite: append rewrite buffer and rename */
int aof_rewrite_finalize(void) {
    pthread_mutex_lock(&g_aof_rewrite_mu);

    if (!g_aof_rewrite_in_progress) {
        pthread_mutex_unlock(&g_aof_rewrite_mu);
        return -1;
    }

    /* Open temp file and append rewrite buffer */
    const char *tmp_path = persist_path("build/appendonly.aof.tmp");
    int fd = open(tmp_path, O_WRONLY | O_APPEND);
    if (fd < 0) {
        fprintf(stderr, "[AOF] finalize: open tmp failed: %s\n", strerror(errno));
        g_aof_rewrite_in_progress = 0;
        free(g_aof_rewrite_buf.data);
        g_aof_rewrite_buf.data = NULL;
        g_aof_rewrite_buf.len = 0;
        g_aof_rewrite_buf.cap = 0;
        pthread_mutex_unlock(&g_aof_rewrite_mu);
        return -1;
    }

    /* Write rewrite buffer */
    if (g_aof_rewrite_buf.len > 0) {
        size_t written = 0;
        while (written < g_aof_rewrite_buf.len) {
            ssize_t n = write(fd, g_aof_rewrite_buf.data + written,
                            g_aof_rewrite_buf.len - written);
            if (n <= 0) break;
            written += n;
        }
        printf("[AOF] finalize: appended %zu bytes from rewrite buffer\n", written);
    }

    fdatasync(fd);
    close(fd);

    /* Atomic rename */
    const char *aof_path = persist_path(WAL_REL);
    if (rename(tmp_path, aof_path) < 0) {
        fprintf(stderr, "[AOF] finalize: rename failed: %s\n", strerror(errno));
        g_aof_rewrite_in_progress = 0;
        free(g_aof_rewrite_buf.data);
        g_aof_rewrite_buf.data = NULL;
        g_aof_rewrite_buf.len = 0;
        g_aof_rewrite_buf.cap = 0;
        pthread_mutex_unlock(&g_aof_rewrite_mu);
        return -1;
    }

    /* Cleanup */
    g_aof_rewrite_in_progress = 0;
    free(g_aof_rewrite_buf.data);
    g_aof_rewrite_buf.data = NULL;
    g_aof_rewrite_buf.len = 0;
    g_aof_rewrite_buf.cap = 0;

    pthread_mutex_unlock(&g_aof_rewrite_mu);
    printf("[AOF] BGREWRITEAOF completed successfully\n");
    return 0;
}

/* =============================================================================
 * Part 3. mmap 加载：RDB / AOF
 * ============================================================================= */

/* 将 (addr, size) 处的定长字节拷贝到 out，返回推进后的指针，失败返回 NULL */
static const char *mread(const char *p, const char *end, void *out, size_t n) {
    if (p + n > end) return NULL;
    memcpy(out, p, n);
    return p + n;
}

/* Streaming RDB load: parse only the header, clear engine, return key_count.
 * Buffer format: [magic:4][version:4][db_num:4][key_count:4]
 * Returns key_count on success, -1 on format error. */
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

    /* Clean up any existing collection-type entries before clearing.
     * kvs_flush_all iterates the dict, calls per-type cleanup for nested
     * structures (quicklist nodes, internal dicts, skiplist nodes), then
     * clears the dict.  Necessary when a slave receives a second fullsync. */
    kvs_flush_all();
    printf("[RDB] stream begin: version=%d, expected keys=%d\n", version, key_count);
    return key_count;
}

/* Streaming RDB load: parse entries from a chunk buffer.
 * Buffer format: [klen:4][key:klen][dlen:4][data:dlen][expire:8]...
 * Returns number of entries loaded, -1 on parse error. */
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
    kv_shards_iter_t it;
    kv_shards_iter_init(&it);
    kv_entry_t *e;
    while ((e = kv_shards_iter_next(&it)) != NULL)
        hash_rebuild_encoding(e);
    kv_shards_iter_free(&it);
}

/* In-memory RDB loader: parses the same binary format as rdb_load_mmap()
 * but operates on a buffer already in memory (no file, no mmap).
 * Used by the slave fullsync path — data arrives via RDMA and is loaded
 * directly without any disk I/O or RESP text parsing. */
int rdb_load_from_buf(const char *buf, size_t len) {
    if (len < 16) return -1;

    int key_count = rdb_load_stream_begin(buf, 16);
    if (key_count < 0) return -1;

    int loaded = rdb_load_stream_append(buf + 16, len - 16);
    if (loaded < 0) { kv_shards_clear(); return -1; }

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

        /* Fallback: use regular read() */
        char *buf = malloc(fsz);
        if (!buf) {
            close(fd);
            return;
        }

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

void aof_reload_mmap(void) {
    /* Phase 4: try WAL file first; fall back to legacy RESP AOF if absent */
    int fd = open(persist_path(WAL_REL), O_RDONLY);
    int wal_format = (fd >= 0);
    if (!wal_format)
        fd = open(persist_path(AOF_REL), O_RDONLY);
    if (fd < 0) return;

    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size <= 0) { close(fd); return; }
    size_t fsz = (size_t)st.st_size;

    char *base = (char *)mmap(NULL, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
    int use_mmap = (base != MAP_FAILED);

    if (!use_mmap) {
        printf("[AOF] mmap failed: %s, falling back to read()\n", strerror(errno));

        /* Fallback: use regular read() */
        base = malloc(fsz);
        if (!base) {
            close(fd);
            return;
        }

        lseek(fd, 0, SEEK_SET);
        size_t total_read = 0;
        while (total_read < fsz) {
            ssize_t n = read(fd, base + total_read, fsz - total_read);
            if (n <= 0) break;
            total_read += n;
        }
        close(fd);

        if (total_read != fsz) {
            printf("[AOF] read failed: only read %zu/%zu bytes\n", total_read, fsz);
            free(base);
            return;
        }
    } else {
        close(fd);
        madvise(base, fsz, MADV_SEQUENTIAL);
    }

    printf("[AOF] %s load start, size=%zu\n",
           wal_format ? "WAL" : "legacy AOF", fsz);

    g_in_reload = 1;
    DynBuf wb;
    dynbuf_init(&wb, BUF_INIT_CAP);
    kvs_token_t tokens[KVS_MAX_TOKENS];
    int loaded = 0;

    if (wal_format) {
        const char *p   = base;
        const char *end = base + fsz;
        while (p + sizeof(kvs_wal2_record_hdr_t) <= end) {
            kvs_wal2_record_hdr_t hdr;
            memcpy(&hdr, p, sizeof(hdr));
            if (hdr.magic != KVS_WAL2_MAGIC) break;
            if (p + sizeof(kvs_wal2_record_hdr_t) + hdr.len > end) break;
            const char *payload = p + sizeof(kvs_wal2_record_hdr_t);
            uint32_t computed = kvs_crc32c(0, &hdr.seq, sizeof(hdr.seq));
            computed = kvs_crc32c(computed, &hdr.kind, sizeof(hdr.kind));
            computed = kvs_crc32c(computed, &hdr.flags, sizeof(hdr.flags));
            computed = kvs_crc32c(computed, &hdr.len, sizeof(hdr.len));
            computed = kvs_crc32c(computed, payload, (size_t)hdr.len);
            if (hdr.crc32 != computed) {
                fprintf(stderr, "[AOF] WAL CRC32 mismatch seq=%" PRIu64
                        ", stopping replay\n", hdr.seq);
                break;
            }
            int argc = 0;
            int cmd_len = resp_parse_slices(payload, (int)hdr.len,
                                            tokens, KVS_MAX_TOKENS, &argc);
            if (cmd_len > 0) {
                dynbuf_clear(&wb);
                kvs_dispatch(tokens, argc, payload, cmd_len, &wb, NULL);
                loaded++;
            }
            p += sizeof(kvs_wal2_record_hdr_t) + hdr.len;
        }
    } else {
        const char *p = base;
        long remaining = (long)fsz;
        while (remaining > 0) {
            int argc = 0;
            int cmd_len = resp_parse_slices(p, (int)remaining,
                                            tokens, KVS_MAX_TOKENS, &argc);
            if (cmd_len <= 0) break;
            dynbuf_clear(&wb);
            kvs_dispatch(tokens, argc, p, cmd_len, &wb, NULL);
            p += cmd_len;
            remaining -= cmd_len;
            loaded++;
        }
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
