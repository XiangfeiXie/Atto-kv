/* =========================================================
 * persistence.c
 *
 *  AOF/RDB 异步化 + io_uring 写入加速 + mmap 加载加速
 *
 *  设计要点：
 *  1) AOF：主线程仅做 memcpy + 入队（O(1)），一个后台线程用
 *     io_uring 批量 submit 写请求，一次 io_uring_submit 可以
 *     同时提交多条 write SQE，避免逐次 syscall 开销，主线程
 *     完全不阻塞在磁盘 IO 上。
 *  2) RDB：保持 fork(BGSAVE) 语义不阻塞主线程；子进程内
 *     使用 io_uring 将大 buffer 切成若干块并发提交写入，
 *     最后一次性 fsync。
 *  3) 加载：使用 mmap 直接映射整个 dump.rdb / appendonly.aof
 *     到虚拟内存，解析时只做指针算术 + 数据读取，避免 fread
 *     的用户态/内核态拷贝与多次 syscall。
 * ========================================================= */

#define _GNU_SOURCE
#include "persistence.h"
#include "kvstore.h"

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
#include <liburing.h>

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
 * Part 1. AOF 异步写（pthread + io_uring）
 * ============================================================================= */

#define AOF_URING_DEPTH       64         /* io_uring SQ 深度 */
#define AOF_BATCH_MAX         32         /* 一次批量提交的最大 entry */

typedef struct aof_entry_s {
    char                *buf;
    int                  len;
    struct aof_entry_s  *next;
} aof_entry_t;

static struct {
    int                  fd;              /* aof 文件 fd */
    pthread_t            tid;
    pthread_mutex_t      mtx;
    pthread_cond_t       cond;
    aof_entry_t         *head;
    aof_entry_t         *tail;
    int                  running;         /* 1: 运行中 0: 退出 */
    struct io_uring      ring;
    int                  ring_inited;
} g_aof = {
    .fd = -1,
    .mtx = PTHREAD_MUTEX_INITIALIZER,
    .cond = PTHREAD_COND_INITIALIZER,
};

/* pthread_atfork handlers: ensure child never inherits a locked g_aof.mtx
 * and that the child's AOF resources are cleaned up before RDB scan. */
static void aof_atfork_prepare(void) {
    /* Lock before fork so the AOF worker is not mid-append. */
    pthread_mutex_lock(&g_aof.mtx);
}
static void aof_atfork_parent(void) {
    pthread_mutex_unlock(&g_aof.mtx);
}
static void aof_atfork_child(void) {
    /* Unlock the mutex we inherited from the prepare handler.
     * The AOF worker thread does not exist in the child, so close
     * the AOF fd to prevent accidental double-writes.  We intentionally
     * do NOT call io_uring_queue_exit here: the ring may have in-flight
     * kernel operations from the parent's worker thread; trying to drain
     * or close the ring in the child can hang.  The ring_fd and mmaped
     * memory are released automatically when the child exits. */
    pthread_mutex_unlock(&g_aof.mtx);
    if (g_aof.fd >= 0) { close(g_aof.fd); g_aof.fd = -1; }
    g_aof.ring_inited = 0;  /* mark uninitialized so child won't use it */
    g_aof.head    = NULL;
    g_aof.tail    = NULL;
    g_aof.running = 0;
}

/* 后台线程：消费队列 + io_uring 批量写 */
static void *aof_worker(void *arg) {
    (void)arg;
    /* 忽略 SIGPIPE，避免写管道类异常导致整个进程挂掉（文件不会触发，但兜底） */
    signal(SIGPIPE, SIG_IGN);

    while (1) {
        aof_entry_t *batch = NULL;
        aof_entry_t *batch_tail = NULL;
        int          batch_n = 0;

        /* 1) 等待并取出一批待写 entry */
        pthread_mutex_lock(&g_aof.mtx);
        while (g_aof.running && g_aof.head == NULL) {
            pthread_cond_wait(&g_aof.cond, &g_aof.mtx);
        }
        if (!g_aof.running && g_aof.head == NULL) {
            pthread_mutex_unlock(&g_aof.mtx);
            break;
        }
        /* 从队头截取最多 AOF_BATCH_MAX 个 */
        batch = g_aof.head;
        batch_tail = batch;
        batch_n = 1;
        while (batch_tail->next && batch_n < AOF_BATCH_MAX) {
            batch_tail = batch_tail->next;
            batch_n++;
        }
        g_aof.head = batch_tail->next;
        if (g_aof.head == NULL) g_aof.tail = NULL;
        batch_tail->next = NULL;
        pthread_mutex_unlock(&g_aof.mtx);

        /* 2) 使用 io_uring 批量提交 write SQE —— 追加模式（O_APPEND），offset=-1 */
        int submitted = 0;
        for (aof_entry_t *e = batch; e != NULL; e = e->next) {
            struct io_uring_sqe *sqe = io_uring_get_sqe(&g_aof.ring);
            if (!sqe) {
                /* 队列满，先 submit 一次释放 SQE */
                io_uring_submit(&g_aof.ring);
                /* 等待已提交的完成 */
                for (int i = 0; i < submitted; i++) {
                    struct io_uring_cqe *cqe;
                    if (io_uring_wait_cqe(&g_aof.ring, &cqe) == 0) {
                        io_uring_cqe_seen(&g_aof.ring, cqe);
                    }
                }
                submitted = 0;
                sqe = io_uring_get_sqe(&g_aof.ring);
                if (!sqe) continue; /* 异常情况，丢弃 */
            }
            io_uring_prep_write(sqe, g_aof.fd, e->buf, e->len, (uint64_t)-1);
            io_uring_sqe_set_data(sqe, e);
            submitted++;
        }
        if (submitted > 0) {
            io_uring_submit(&g_aof.ring);
            /* 等待全部完成，确保持久化顺序 */
            for (int i = 0; i < submitted; i++) {
                struct io_uring_cqe *cqe = NULL;
                if (io_uring_wait_cqe(&g_aof.ring, &cqe) == 0) {
                    if (cqe->res < 0) {
                        fprintf(stderr, "[AOF] io_uring write error: %s\n", strerror(-cqe->res));
                    }
                    io_uring_cqe_seen(&g_aof.ring, cqe);
                }
            }
        }

        /* 3) 释放这批 entry */
        aof_entry_t *e = batch;
        while (e) {
            aof_entry_t *nxt = e->next;
            free(e->buf);
            free(e);
            e = nxt;
        }
    }

    /* 退出前 flush：fsync */
    if (g_aof.fd >= 0) fsync(g_aof.fd);
    return NULL;
}

int aof_async_init(void) {
    if (g_aof.fd >= 0) return 0; /* 已初始化 */

    const char *path = persist_path(AOF_REL);
    g_aof.fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (g_aof.fd < 0) {
        fprintf(stderr, "[AOF] open %s failed: %s\n", path, strerror(errno));
        return -1;
    }

    if (io_uring_queue_init(AOF_URING_DEPTH, &g_aof.ring, 0) < 0) {
        fprintf(stderr, "[AOF] io_uring_queue_init failed\n");
        close(g_aof.fd);
        g_aof.fd = -1;
        return -1;
    }
    g_aof.ring_inited = 1;
    g_aof.running = 1;

    if (pthread_create(&g_aof.tid, NULL, aof_worker, NULL) != 0) {
        fprintf(stderr, "[AOF] pthread_create failed\n");
        io_uring_queue_exit(&g_aof.ring);
        g_aof.ring_inited = 0;
        close(g_aof.fd);
        g_aof.fd = -1;
        g_aof.running = 0;
        return -1;
    }

    /* Register fork handlers once: lock AOF queue in parent before fork so
     * the child never inherits g_aof.mtx in a locked state. */
    pthread_atfork(aof_atfork_prepare, aof_atfork_parent, aof_atfork_child);

    printf("[AOF] async writer started (uring depth=%d)\n", AOF_URING_DEPTH);
    return 0;
}

void aof_async_shutdown(void) {
    if (g_aof.fd < 0) return;

    pthread_mutex_lock(&g_aof.mtx);
    g_aof.running = 0;
    pthread_cond_broadcast(&g_aof.cond);
    pthread_mutex_unlock(&g_aof.mtx);

    pthread_join(g_aof.tid, NULL);

    if (g_aof.ring_inited) {
        io_uring_queue_exit(&g_aof.ring);
        g_aof.ring_inited = 0;
    }
    close(g_aof.fd);
    g_aof.fd = -1;
    printf("[AOF] async writer stopped\n");
}

void aof_async_append(const char *buf, int len) {
    if (g_aof.fd < 0 || !buf || len <= 0) return;

    aof_entry_t *e = (aof_entry_t *)malloc(sizeof(*e));
    if (!e) return;
    e->buf = (char *)malloc(len);
    if (!e->buf) { free(e); return; }
    memcpy(e->buf, buf, len);
    e->len = len;
    e->next = NULL;

    pthread_mutex_lock(&g_aof.mtx);
    if (g_aof.tail) g_aof.tail->next = e;
    else            g_aof.head = e;
    g_aof.tail = e;
    pthread_cond_signal(&g_aof.cond);
    pthread_mutex_unlock(&g_aof.mtx);
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

/* Forward declarations — flattening helpers from type modules */
extern int list_flatten_to_packed(kv_entry_t *e, char **out_data, uint32_t *out_len);
extern int set_flatten_to_packed(kv_entry_t *e, char **out_data, uint32_t *out_len);
extern int hash_flatten_to_packed(kv_entry_t *e, char **out_data, uint32_t *out_len);
extern int zset_flatten_to_packed(kv_entry_t *e, char **out_data, uint32_t *out_len);

/* Forward declarations — encoding rebuild after RDB load */
extern void set_rebuild_encoding(kv_entry_t *e);
extern void hash_rebuild_encoding(kv_entry_t *e);
extern void zset_rebuild_encoding(kv_entry_t *e);

/* Serialize one dict entry to RDB binary format (appends to rdb_buf_t). */
static void rdb_save_callback_buf(kv_entry_t *e, void *priv) {
    if (!e) return;
    rdb_buf_t *b = (rdb_buf_t *)priv;
    const char *key = entry_key(e);
    int klen = (int)e->key_len;
    int dlen;
    const char *data_ptr;
    char *flat_buf = NULL;

    /* Flatten encodings that contain heap pointers to packed format */
    switch (entry_type(e)) {
    case KV_TYPE_LIST:
        if (entry_encoding(e) == KV_ENCODING_QUICKLIST) {
            uint32_t flat_len = 0;
            if (list_flatten_to_packed(e, &flat_buf, &flat_len) == 0) {
                dlen     = (int)flat_len;
                data_ptr = flat_buf;
            } else {
                dlen     = (int)e->val_len;
                data_ptr = e->data;
            }
        } else {
            dlen     = (int)e->val_len;
            data_ptr = e->data;
        }
        break;
    case KV_TYPE_SET:
        if (entry_encoding(e) == KV_ENCODING_DICT) {
            uint32_t flat_len = 0;
            if (set_flatten_to_packed(e, &flat_buf, &flat_len) == 0) {
                dlen     = (int)flat_len;
                data_ptr = flat_buf;
            } else {
                dlen     = (int)e->val_len;
                data_ptr = e->data;
            }
        } else {
            dlen     = (int)e->val_len;
            data_ptr = e->data;
        }
        break;
    case KV_TYPE_HASH:
        if (entry_encoding(e) == KV_ENCODING_DICT) {
            uint32_t flat_len = 0;
            if (hash_flatten_to_packed(e, &flat_buf, &flat_len) == 0) {
                dlen     = (int)flat_len;
                data_ptr = flat_buf;
            } else {
                dlen     = (int)e->val_len;
                data_ptr = e->data;
            }
        } else {
            dlen     = (int)e->val_len;
            data_ptr = e->data;
        }
        break;
    case KV_TYPE_ZSET:
        if (entry_encoding(e) == KV_ENCODING_SKIPLIST) {
            uint32_t flat_len = 0;
            if (zset_flatten_to_packed(e, &flat_buf, &flat_len) == 0) {
                dlen     = (int)flat_len;
                data_ptr = flat_buf;
            } else {
                dlen     = (int)e->val_len;
                data_ptr = e->data;
            }
        } else {
            dlen     = (int)e->val_len;
            data_ptr = e->data;
        }
        break;
    default:
        dlen     = (int)e->val_len;
        data_ptr = e->data;
        break;
    }

    long long expire = e->expire;
    rdb_buf_put(b, &klen, sizeof(int));
    rdb_buf_put(b, key, (size_t)klen);
    rdb_buf_put(b, &dlen, sizeof(int));
    rdb_buf_put(b, data_ptr, (size_t)dlen);

    free(flat_buf);

    rdb_buf_put(b, &expire, sizeof(long long));
}

/* Single-entry serialization to DynBuf (reusable, handles quicklist flattening).
   Appends [klen:4][key:klen][dlen:4][data:dlen][expire:8]. */
void serialize_entry_to_buf(DynBuf *out, kv_entry_t *e) {
    const char *key = entry_key(e);
    int klen = (int)e->key_len;
    int dlen;
    const char *data_ptr;
    char *flat_buf = NULL;

    switch (entry_type(e)) {
    case KV_TYPE_LIST:
        if (entry_encoding(e) == KV_ENCODING_QUICKLIST) {
            uint32_t flat_len = 0;
            if (list_flatten_to_packed(e, &flat_buf, &flat_len) == 0) {
                dlen     = (int)flat_len;
                data_ptr = flat_buf;
            } else {
                dlen     = (int)e->val_len;
                data_ptr = e->data;
            }
        } else {
            dlen     = (int)e->val_len;
            data_ptr = e->data;
        }
        break;
    case KV_TYPE_SET:
        if (entry_encoding(e) == KV_ENCODING_DICT) {
            uint32_t flat_len = 0;
            if (set_flatten_to_packed(e, &flat_buf, &flat_len) == 0) {
                dlen     = (int)flat_len;
                data_ptr = flat_buf;
            } else {
                dlen     = (int)e->val_len;
                data_ptr = e->data;
            }
        } else {
            dlen     = (int)e->val_len;
            data_ptr = e->data;
        }
        break;
    case KV_TYPE_HASH:
        if (entry_encoding(e) == KV_ENCODING_DICT) {
            uint32_t flat_len = 0;
            if (hash_flatten_to_packed(e, &flat_buf, &flat_len) == 0) {
                dlen     = (int)flat_len;
                data_ptr = flat_buf;
            } else {
                dlen     = (int)e->val_len;
                data_ptr = e->data;
            }
        } else {
            dlen     = (int)e->val_len;
            data_ptr = e->data;
        }
        break;
    case KV_TYPE_ZSET:
        if (entry_encoding(e) == KV_ENCODING_SKIPLIST) {
            uint32_t flat_len = 0;
            if (zset_flatten_to_packed(e, &flat_buf, &flat_len) == 0) {
                dlen     = (int)flat_len;
                data_ptr = flat_buf;
            } else {
                dlen     = (int)e->val_len;
                data_ptr = e->data;
            }
        } else {
            dlen     = (int)e->val_len;
            data_ptr = e->data;
        }
        break;
    default:
        dlen     = (int)e->val_len;
        data_ptr = e->data;
        break;
    }

    long long expire = e->expire;
    dynbuf_append(out, (const char *)&klen, sizeof(int));
    dynbuf_append(out, key, klen);
    dynbuf_append(out, (const char *)&dlen, sizeof(int));
    dynbuf_append(out, data_ptr, dlen);
    dynbuf_append(out, (const char *)&expire, sizeof(long long));

    free(flat_buf);
}

/* Inline variant: writes directly to dst, returns bytes written.
 * Returns -1 if the serialized entry would exceed max_space. */
int serialize_entry_inline(char *dst, size_t max_space, kv_entry_t *e) {
    const char *key = entry_key(e);
    int klen = (int)e->key_len;
    int dlen;
    const char *data_ptr;
    char *flat_buf = NULL;

    switch (entry_type(e)) {
    case KV_TYPE_LIST:
        if (entry_encoding(e) == KV_ENCODING_QUICKLIST) {
            uint32_t flat_len = 0;
            if (list_flatten_to_packed(e, &flat_buf, &flat_len) == 0) {
                dlen     = (int)flat_len;
                data_ptr = flat_buf;
            } else {
                dlen     = (int)e->val_len;
                data_ptr = e->data;
            }
        } else {
            dlen     = (int)e->val_len;
            data_ptr = e->data;
        }
        break;
    case KV_TYPE_SET:
        if (entry_encoding(e) == KV_ENCODING_DICT) {
            uint32_t flat_len = 0;
            if (set_flatten_to_packed(e, &flat_buf, &flat_len) == 0) {
                dlen     = (int)flat_len;
                data_ptr = flat_buf;
            } else {
                dlen     = (int)e->val_len;
                data_ptr = e->data;
            }
        } else {
            dlen     = (int)e->val_len;
            data_ptr = e->data;
        }
        break;
    case KV_TYPE_HASH:
        if (entry_encoding(e) == KV_ENCODING_DICT) {
            uint32_t flat_len = 0;
            if (hash_flatten_to_packed(e, &flat_buf, &flat_len) == 0) {
                dlen     = (int)flat_len;
                data_ptr = flat_buf;
            } else {
                dlen     = (int)e->val_len;
                data_ptr = e->data;
            }
        } else {
            dlen     = (int)e->val_len;
            data_ptr = e->data;
        }
        break;
    case KV_TYPE_ZSET:
        if (entry_encoding(e) == KV_ENCODING_SKIPLIST) {
            uint32_t flat_len = 0;
            if (zset_flatten_to_packed(e, &flat_buf, &flat_len) == 0) {
                dlen     = (int)flat_len;
                data_ptr = flat_buf;
            } else {
                dlen     = (int)e->val_len;
                data_ptr = e->data;
            }
        } else {
            dlen     = (int)e->val_len;
            data_ptr = e->data;
        }
        break;
    default:
        dlen     = (int)e->val_len;
        data_ptr = e->data;
        break;
    }

    long long expire = e->expire;
    int total = (int)(sizeof(int) + klen + sizeof(int) + dlen + sizeof(long long));
    if (total > (int)max_space) {
        free(flat_buf);
        return -1;
    }

    char *p = dst;
    memcpy(p, &klen, sizeof(int)); p += sizeof(int);
    memcpy(p, key, klen);          p += klen;
    memcpy(p, &dlen, sizeof(int)); p += sizeof(int);
    memcpy(p, data_ptr, dlen);     p += dlen;
    memcpy(p, &expire, sizeof(long long));

    free(flat_buf);
    return total;
}

/* Same RDB binary format, appends to a DynBuf (for RDMA fullsync). */
static void rdb_serialize_cb_dynbuf(kv_entry_t *e, void *priv) {
    if (!e) return;
    serialize_entry_to_buf((DynBuf *)priv, e);
}

/* Iterate all dict entries, calling cb for each. */
static void dict_scan(kv_dict_t *d, void (*cb)(kv_entry_t *e, void *priv), void *priv) {
    kv_iter_t *it = dict_iter_new(d, 1);
    if (!it) return;
    kv_entry_t *e;
    while ((e = dict_iter_next(it)) != NULL)
        cb(e, priv);
    dict_iter_free(it);
}

void rdb_serialize_to_buf(DynBuf *out) {
    int magic = 0x524442;
    int version = 2;
    int db_num = 0;
    int key_count = (int)dict_count(&g_dict);

    /* Reserve: header (16) + per-entry estimate (key + value + overhead ~= 256) */
    dynbuf_reserve(out, 16 + key_count * 256);

    dynbuf_append(out, (const char *)&magic,     sizeof(int));
    dynbuf_append(out, (const char *)&version,   sizeof(int));
    dynbuf_append(out, (const char *)&db_num,    sizeof(int));
    dynbuf_append(out, (const char *)&key_count, sizeof(int));

    dict_scan(&g_dict, rdb_serialize_cb_dynbuf, out);
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
            /* 收割一个 CQE，腾出 SQ 空间 */
            struct io_uring_cqe *cqe = NULL;
            if (io_uring_wait_cqe(&ring, &cqe) == 0) {
                if (cqe->res < 0) {
                    fprintf(stderr, "[RDB] uring write error: %s\n", strerror(-cqe->res));
                }
                io_uring_cqe_seen(&ring, cqe);
                inflight--;
            }
        }
    }

    io_uring_queue_exit(&ring);
    fsync(fd);
}

void rdb_save_sync(void) {
    /* 1) 序列化到内存 buffer */
    rdb_buf_t b = {0};

    int magic = 0x524442;
    int version = 2;
    int db_num = 0;
    int key_count = (int)dict_count(&g_dict);

    rdb_buf_put(&b, &magic, sizeof(int));
    rdb_buf_put(&b, &version, sizeof(int));
    rdb_buf_put(&b, &db_num, sizeof(int));
    rdb_buf_put(&b, &key_count, sizeof(int));

    dict_scan(&g_dict, rdb_save_callback_buf, &b);

    /* 2) 写临时文件 + rename，保证原子性 */
    const char *tmp = persist_path(RDB_TMP);
    const char *dst = persist_path(RDB_REL);
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        fprintf(stderr, "[RDB] open tmp failed: %s\n", strerror(errno));
        free(b.buf);
        return;
    }

    rdb_write_uring(fd, b.buf, b.len);
    close(fd);
    rename(tmp, dst);

    free(b.buf);
    printf("[RDB] saved via io_uring, %zu bytes\n", b.len);
}

/* Track the currently running BGSAVE child so we never run two at once. */
static volatile pid_t g_rdb_child_pid = 0;

/* SIGCHLD 处理：回收已完成的 BGSAVE 子进程，避免僵尸 */
static void rdb_sigchld(int signo) {
    (void)signo;
    pid_t pid;
    int   status;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (pid == g_rdb_child_pid)
            g_rdb_child_pid = 0;  /* slot is free for next BGSAVE */
    }
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

    pid_t pid = fork();
    if (pid < 0) {
        fprintf(stderr, "[RDB] fork failed, fallback sync\n");
        rdb_save_sync();
        return;
    }
    if (pid == 0) {
        /* 子进程: flush stdio so printf output is not lost with _exit */
        rdb_save_sync();
        fflush(stdout);
        fflush(stderr);
        _exit(0);
    }
    /* 父进程立即返回 —— 主线程零阻塞 */
    g_rdb_child_pid = pid;
    printf("[RDB] BGSAVE started, child pid=%d\n", (int)pid);
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

    dict_clear(&g_dict);
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
        kv_entry_t *e = dict_add(&g_dict, ks, (uint32_t)klen, p, (uint32_t)dlen, h);
        p += dlen;

        if (!(p = mread(p, end, &expire, sizeof(long long)))) break;
        if (e && expire) e->expire = expire;
        loaded++;
    }

    return loaded;
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
    if (loaded < 0) { dict_clear(&g_dict); return -1; }

    /* Rebuild encodings for entries loaded as PACKED that should be richer */
    {
        kv_iter_t *it = dict_iter_new(&g_dict, 1);
        if (it) {
            kv_entry_t *e;
            while ((e = dict_iter_next(it)) != NULL) {
                switch (entry_type(e)) {
                case KV_TYPE_SET:  set_rebuild_encoding(e);  break;
                case KV_TYPE_HASH: hash_rebuild_encoding(e); break;
                case KV_TYPE_ZSET: zset_rebuild_encoding(e); break;
                default: break;
                }
            }
            dict_iter_free(it);
        }
    }

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
    close(fd);
    if (base == MAP_FAILED) {
        printf("[RDB] mmap failed: %s\n", strerror(errno));
        return;
    }
    madvise(base, fsz, MADV_SEQUENTIAL);

    rdb_load_from_buf(base, fsz);

    munmap(base, fsz);
}

void aof_reload_mmap(void) {
    int fd = open(persist_path(AOF_REL), O_RDONLY);
    if (fd < 0) return;
    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size <= 0) { close(fd); return; }
    size_t fsz = (size_t)st.st_size;

    char *base = (char *)mmap(NULL, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (base == MAP_FAILED) return;
    madvise(base, fsz, MADV_SEQUENTIAL);

    printf("[AOF] mmap load start, size=%zu\n", fsz);

    g_in_reload = 1;

    const char *p = base;
    long remaining = (long)fsz;
    DynBuf wb;
    dynbuf_init(&wb, BUF_INIT_CAP);

    kvs_token_t tokens[KVS_MAX_TOKENS];

    while (remaining > 0) {
        int argc = 0;
        int cmd_len = resp_parse_slices(p, (int)remaining,
                                        tokens, KVS_MAX_TOKENS, &argc);
        if (cmd_len <= 0) break;

        dynbuf_clear(&wb);
        /* 零拷贝：直接传递 mmap 指针和 token slice，无需 malloc+memcpy */
        kvs_dispatch(tokens, argc, p, cmd_len, &wb, NULL);

        p += cmd_len;
        remaining -= cmd_len;
    }

    dynbuf_free(&wb);
    munmap(base, fsz);
    g_in_reload = 0;
    printf("[AOF] mmap load finished\n");
}
