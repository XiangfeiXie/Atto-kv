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
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <liburing.h>

/* ========================= 引擎数据访问（与 kvstore.c 保持一致） ========================= */
extern kvs_array_t    global_array;
extern kvs_rbtree_t   global_rbtree;
extern kvs_hash_t     global_hash;
extern kvs_skiptable_t global_skiptable;

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

int aof_async_init(const char *path) {
    if (g_aof.fd >= 0) return 0; /* 已初始化 */

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

static void rdb_save_callback_buf(const char *key, KVItem *item, void *priv) {
    if (!key || !item) return;
    rdb_buf_t *b = (rdb_buf_t *)priv;
    int klen = (int)strlen(key);
    int vlen = (int)item->len;
    rdb_buf_put(b, &klen, sizeof(int));
    rdb_buf_put(b, key, klen);
    rdb_buf_put(b, &vlen, sizeof(int));
    rdb_buf_put(b, item->value, vlen);
    rdb_buf_put(b, &item->expire, sizeof(long long));
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

static void rdb_save_child(void) {
    /* 1) 序列化到内存 buffer */
    rdb_buf_t b = {0};

    int magic = 0x524442;
    int version = 1;
    int db_num = 0;
    int key_count = 0;
    if      (kvs_config.engine == ENGINE_ARRAY)     key_count = global_array.total;
    else if (kvs_config.engine == ENGINE_RBTREE)    key_count = global_rbtree.total;
    else if (kvs_config.engine == ENGINE_HASH)      key_count = global_hash.count;
    else if (kvs_config.engine == ENGINE_SKIPTABLE) key_count = global_skiptable.skl->total;

    rdb_buf_put(&b, &magic, sizeof(int));
    rdb_buf_put(&b, &version, sizeof(int));
    rdb_buf_put(&b, &db_num, sizeof(int));
    rdb_buf_put(&b, &key_count, sizeof(int));

    if      (kvs_config.engine == ENGINE_ARRAY)     kvs_array_scan(&global_array, rdb_save_callback_buf, &b);
    else if (kvs_config.engine == ENGINE_RBTREE)    kvs_rbtree_scan(&global_rbtree, rdb_save_callback_buf, &b);
    else if (kvs_config.engine == ENGINE_HASH)      kvs_hash_scan(&global_hash, rdb_save_callback_buf, &b);
    else if (kvs_config.engine == ENGINE_SKIPTABLE) kvs_skiptable_scan(&global_skiptable, rdb_save_callback_buf, &b);

    /* 2) 写临时文件 + rename，保证原子性 */
    const char *tmp = "dump.rdb.tmp";
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        fprintf(stderr, "[RDB] open tmp failed: %s\n", strerror(errno));
        free(b.buf);
        return;
    }

    rdb_write_uring(fd, b.buf, b.len);
    close(fd);
    rename(tmp, "dump.rdb");

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
        rdb_save_child();
        return;
    }
    if (pid == 0) {
        /* 子进程: flush stdio so printf output is not lost with _exit */
        rdb_save_child();
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

void rdb_load_mmap(void) {
    int fd = open("dump.rdb", O_RDONLY);
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
    /* 提示内核顺序读 —— 预读加速 */
    madvise(base, fsz, MADV_SEQUENTIAL);

    const char *p = base;
    const char *end = base + fsz;

    int magic = 0, version = 0, db_num = 0, key_count = 0;
    if (!(p = mread(p, end, &magic,     sizeof(int)))) goto out;
    if (!(p = mread(p, end, &version,   sizeof(int)))) goto out;
    if (!(p = mread(p, end, &db_num,    sizeof(int)))) goto out;
    if (!(p = mread(p, end, &key_count, sizeof(int)))) goto out;

    if (magic != 0x524442) {
        printf("[RDB] magic mismatch\n");
        goto out;
    }

    if      (kvs_config.engine == ENGINE_ARRAY)     kvs_array_clear(&global_array);
    else if (kvs_config.engine == ENGINE_RBTREE)    kvs_rbtree_clear(&global_rbtree);
    else if (kvs_config.engine == ENGINE_HASH)      kvs_hash_clear(&global_hash);
    else if (kvs_config.engine == ENGINE_SKIPTABLE) kvs_skiptable_clear(&global_skiptable);

    for (int i = 0; i < key_count; i++) {
        int klen = 0, vlen = 0;
        long long expire = 0;
        if (!(p = mread(p, end, &klen, sizeof(int)))) break;
        if (p + klen > end) break;

        char *key = (char *)kvs_malloc(klen + 1);
        memcpy(key, p, klen); key[klen] = 0; p += klen;

        if (!(p = mread(p, end, &vlen, sizeof(int)))) { kvs_free(key); break; }
        if (p + vlen > end) { kvs_free(key); break; }

        char *val = (char *)kvs_malloc(vlen + 1);
        memcpy(val, p, vlen); val[vlen] = 0; p += vlen;

        if (!(p = mread(p, end, &expire, sizeof(long long)))) { kvs_free(key); kvs_free(val); break; }

        KVItem *item = create_kvitem(val, NULL);
        if (item) item->expire = expire;

        if      (kvs_config.engine == ENGINE_ARRAY)     kvs_array_set(&global_array, key, item);
        else if (kvs_config.engine == ENGINE_RBTREE)    kvs_rbtree_set(&global_rbtree, key, item);
        else if (kvs_config.engine == ENGINE_HASH)      kvs_hash_set(&global_hash, key, item);
        else if (kvs_config.engine == ENGINE_SKIPTABLE) kvs_skiptable_set(&global_skiptable, key, item);
        kvs_free(key);
        kvs_free(val);
    }

    printf("[RDB] mmap load done: %d keys, %zu bytes\n", key_count, fsz);
out:
    munmap(base, fsz);
}

void aof_reload_mmap(void) {
    int fd = open("appendonly.aof", O_RDONLY);
    if (fd < 0) return;
    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size <= 0) { close(fd); return; }
    size_t fsz = (size_t)st.st_size;

    char *base = (char *)mmap(NULL, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (base == MAP_FAILED) return;
    madvise(base, fsz, MADV_SEQUENTIAL);

    printf("[AOF] mmap load start, size=%zu\n", fsz);

    const char *p = base;
    long remaining = (long)fsz;
    DynBuf wb;
    dynbuf_init(&wb, BUF_INIT_CAP);

    while (remaining > 0) {
        int cmd_len = resp_try_parse_full_cmd(p, remaining);
        if (cmd_len <= 0) break;
        /* 解析时 kvs_protocol 需要可写（strtok 等），这里拷贝一份小 buffer */
        char *cmd = (char *)malloc(cmd_len);
        if (!cmd) break;
        memcpy(cmd, p, cmd_len);
        dynbuf_clear(&wb);
        kvs_protocol(cmd, cmd_len, &wb);
        free(cmd);

        p += cmd_len;
        remaining -= cmd_len;
    }

    dynbuf_free(&wb);
    munmap(base, fsz);
    printf("[AOF] mmap load finished\n");
}
