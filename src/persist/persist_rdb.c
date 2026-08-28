/*=============================================================================
 *  persist_rdb.c — RDB 持久化（fork + io_uring 写入）和加载（mmap）
 *  
 *  职责：
 *  1. RDB 保存：同步（rdb_save_sync）和异步 fork（rdb_save_async_uring）
 *  2. RDB 加载：mmap 加载和流式加载（全量同步用）
 *  3. 流式序列化器：RDMA 全量同步 Master 端使用
 *  4. 子进程回收管理：统一 SIGCHLD 处理
 *============================================================================*/

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

/*=============================================================================
 *  常量定义
 *============================================================================*/

#define RDB_TMP "build/dump.rdb.tmp"  /* 临时文件路径 */

/**
 * 加载时 key_count 的合理性上限
 * 真实库远小于此，防垃圾/损坏输入触发 32 位计数异常
 * 1 亿 key × 64B ≈ 6.4GB RDB，远超演示场景
 */
#define RDB_MAX_KEY_COUNT 100000000

/*=============================================================================
 *  动态缓冲区（RDB 序列化用）
 *============================================================================*/

typedef struct {
    char  *buf;      /* 缓冲区指针 */
    size_t len;      /* 当前已用长度 */
    size_t cap;      /* 缓冲区容量 */
} rdb_buf_t;

/*=============================================================================
 *  全局状态
 *============================================================================*/

static volatile pid_t g_rdb_child_pid = 0;  /* BGSAVE 子进程 PID，0=未运行 */

/*=============================================================================
 *  动态缓冲区辅助函数
 *============================================================================*/

/**
 * 确保缓冲区有足够空间
 * 容量不足时翻倍扩容
 */
static void rdb_buf_reserve(rdb_buf_t *b, size_t need) {
    if (b->len + need <= b->cap) return;
    size_t ncap = b->cap ? b->cap : 64 * 1024;  /* 初始 64KB */
    while (ncap < b->len + need)
        ncap *= 2;
    b->buf = (char *)realloc(b->buf, ncap);
    b->cap = ncap;
}

/*=============================================================================
 *  RDB 格式辅助
 *============================================================================*/

/**
 * 写入 RDB 头部：单个 4 字节 key_count
 * 无 magic/version，文件路径与 RDMA 流路径共用
 * 解析端按 4 字节读回
 */
static void rdb_write_header(char *dst, int key_count) {
    memcpy(dst, &key_count, sizeof(int));
}

/**
 * 单个条目的序列化大小
 * 格式：[klen:4][key:klen][dlen:4][data:dlen][expire:8]
 */
static inline size_t entry_serialized_size(kv_entry_t *e) {
    return sizeof(int) + (size_t)e->key_len
         + sizeof(int) + (size_t)entry_payload_len(e)
         + sizeof(long long);
}

/**
 * 序列化单个条目到缓冲区
 * @param dst 目标缓冲区
 * @param e 要序列化的 entry
 * @return 写入的字节数
 */
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

/*=============================================================================
 *  io_uring 写入
 * 分块异步写入 + fdatasync，带 read fallback
 *============================================================================*/

/**
 * 使用 io_uring 分块写入文件
 * @param fd 目标文件 fd
 * @param data 数据指针
 * @param total 数据大小
 * @return 0 成功，-1 失败（调用者不得 rename 残缺文件）
 */
static int rdb_write_uring(int fd, const char *data, size_t total) {
    const size_t CHUNK = 1 * 1024 * 1024;  /* 每块 1MB */
    const int    DEPTH = 128;              /* io_uring 队列深度 */

    struct io_uring ring;
    if (io_uring_queue_init(DEPTH, &ring, 0) < 0) {
        /* fallback：阻塞 write() + fdatasync */
        size_t off = 0;
        while (off < total) {
            ssize_t w = write(fd, data + off, total - off);
            if (w < 0 && errno == EINTR) continue;
            if (w <= 0) {
                fprintf(stderr, "[RDB] fallback write failed at offset %zu: %s\n",
                        off, strerror(errno));
                return -1;
            }
            off += (size_t)w;
        }
        if (fdatasync(fd) < 0) {
            fprintf(stderr, "[RDB] fallback fdatasync failed: %s\n", strerror(errno));
            return -1;
        }
        return 0;
    }

    size_t off = 0;
    int    inflight = 0;
    int    failed = 0;

    /* 提交所有写请求 */
    while (off < total || inflight > 0) {
        /* 尽可能多地提交 SQE */
        while (off < total && inflight < DEPTH && !failed) {
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
            if (!sqe) break;
            size_t n = total - off;
            if (n > CHUNK) n = CHUNK;
            io_uring_prep_write(sqe, fd, data + off, n, off);
            sqe->user_data = n;
            off += n;
            inflight++;
        }

        /* 收割已完成的请求 */
        if (inflight > 0) {
            io_uring_submit(&ring);

            while (inflight > 0) {
                struct io_uring_cqe *cqe = NULL;
                int ret = io_uring_peek_cqe(&ring, &cqe);

                if (ret == -EAGAIN) {
                    ret = io_uring_wait_cqe(&ring, &cqe);
                    if (ret < 0) {
                        fprintf(stderr, "[RDB] io_uring_wait_cqe failed: %s\n", strerror(-ret));
                        failed = 1;
                        break;
                    }
                }

                if (cqe) {
                    if (cqe->res < 0) {
                        fprintf(stderr, "[RDB] uring write error: %s\n", strerror(-cqe->res));
                        failed = 1;
                        io_uring_cqe_seen(&ring, cqe);
                        inflight--;
                        break;
                    }

                    size_t expected = (size_t)cqe->user_data;
                    if ((size_t)cqe->res != expected) {
                        fprintf(stderr, "[RDB] short write: %d/%zu bytes\n", cqe->res, expected);
                        failed = 1;
                        io_uring_cqe_seen(&ring, cqe);
                        inflight--;
                        break;
                    }

                    io_uring_cqe_seen(&ring, cqe);
                    inflight--;
                }
            }

            if (failed) {
                io_uring_queue_exit(&ring);
                return -1;
            }
        }
    }

    /* 最终 fsync：用 IOSQE_IO_DRAIN 保证先于所有写完成 */
    int ok = 0;
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (sqe) {
        io_uring_prep_fsync(sqe, fd, IORING_FSYNC_DATASYNC);
        sqe->flags |= IOSQE_IO_DRAIN;  /* 等待所有之前提交的请求完成 */
        io_uring_submit(&ring);
        struct io_uring_cqe *cqe = NULL;
        if (io_uring_wait_cqe(&ring, &cqe) == 0) {
            if (cqe->res == 0) ok = 1;
            else fprintf(stderr, "[RDB] fsync failed: %s\n", strerror(-cqe->res));
            io_uring_cqe_seen(&ring, cqe);
        } else {
            fprintf(stderr, "[RDB] fsync wait failed: %s\n", strerror(errno));
        }
    } else {
        if (fdatasync(fd) == 0) ok = 1;
        else fprintf(stderr, "[RDB] fdatasync failed: %s\n", strerror(errno));
    }
    io_uring_queue_exit(&ring);
    return ok ? 0 : -1;
}

/*=============================================================================
 *  增量 RDB 序列化器（RDMA 全量同步 Master 端）
 *  
 *  直接流式输出到 RDMA MR 缓冲区，避免中间 DynBuf 拷贝
 *============================================================================*/

/**
 * 开始 RDB 序列化
 */
void rdb_serialize_begin(rdb_serializer_t *rs) {
    rs->iter        = dict_iter_new(&g_dict, 1);
    rs->header_sent = 0;
    /* 空库也必须发出 RDB header（key_count=0）
     * 否则 RDMA 全量同步会因 exhausted 提前置位而 0 batch 收尾
     * slave 收不到 IMM_DONE 卡死 */
    rs->exhausted   = 0;
}

/**
 * 获取下一批 RDB 数据
 */
size_t rdb_serialize_next(rdb_serializer_t *rs, char *dst,
                          size_t max_bytes, int *done) {
    size_t written = 0;
    *done = 0;

    if (rs->exhausted) {
        *done = 1;
        return 0;
    }

    /* 发送头部：4 字节 key_count（未发送时） */
    if (!rs->header_sent) {
        if (max_bytes < 4) return 0;  /* 空间不足，等待下一轮 */
        
        int key_count = (int)dict_count(&g_dict);
        rdb_write_header(dst, key_count);
        written += 4;
        dst += 4;
        max_bytes -= 4;
        rs->header_sent = 1;
    }

    /* 逐个序列化条目，直到缓冲区满或迭代结束 */
    while (max_bytes >= 16) {  /* 最小一个条目：4+0+4+0+8=16 */
        kv_entry_t *e = dict_iter_peek(rs->iter);
        if (!e) {
            rs->exhausted = 1;
            break;
        }

        size_t sz = entry_serialized_size(e);
        if (sz > max_bytes) break;         /* 条目放不下 → 下一批 */

        dict_iter_next(rs->iter);          /* 消费条目 */
        size_t n = serialize_entry_to_ptr(dst, e);
        written   += n;
        dst       += n;
        max_bytes -= n;
    }

    if (rs->exhausted) *done = 1;
    return written;
}

/**
 * 结束 RDB 序列化
 */
void rdb_serialize_end(rdb_serializer_t *rs) {
    if (rs->iter) {
        dict_iter_free(rs->iter);
        rs->iter = NULL;
    }
    rs->exhausted = 1;
}

/*=============================================================================
 *  同步保存
 *  序列化 → io_uring 写入 → rename
 *============================================================================*/

void rdb_save_sync(void) {
    rdb_buf_t b  = {0};
    int key_count = (int)dict_count(&g_dict);

    /* 使用流式序列化器序列化所有数据 */
    rdb_buf_reserve(&b, 64 * 1024);
    rdb_serializer_t rs;
    rdb_serialize_begin(&rs);
    int done = 0;
    while (!done) {
        size_t n = rdb_serialize_next(&rs, b.buf + b.len, b.cap - b.len, &done);
        if (n == 0 && !done) {
            /* 放不下下一条 entry，扩容再试 */
            rdb_buf_reserve(&b, b.len + 64 * 1024);
            continue;
        }
        b.len += n;
    }
    rdb_serialize_end(&rs);

    /* 写入临时文件 */
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

    /* 写失败绝不能把残缺文件 rename 成 dump.rdb
     * 否则下次启动会拿损坏快照清空整库 */
    if (rdb_write_uring(fd, b.buf, b.len) < 0) {
        close(fd);
        unlink(tmp_path);
        free(b.buf);
        return;
    }
    close(fd);
    rename(tmp_path, dst_path);

    free(b.buf);
    printf("[RDB] saved via io_uring, %zu bytes (%d keys)\n", b.len, key_count);
}

/*=============================================================================
 *  子进程回收管理（统一 SIGCHLD 处理）
 *  
 *  问题：SIGCHLD 每进程只能有一个 handler
 *  解决方案：统一 handler + 回调注册机制
 *  
 *  BGSAVE 子进程与 repl 全量同步传输子进程各自注册回调
 *  互不覆盖，解决此前两个模块各自装 SIGCHLD 互相覆盖的问题
 *============================================================================*/

#define MAX_CHILD_REAP_CBS 3   /* BGSAVE + repl 传输 + AOF 重写 */

static kvs_child_reap_fn g_reap_cbs[MAX_CHILD_REAP_CBS];
static int               g_reap_n = 0;

/**
 * 统一 SIGCHLD 信号处理器
 * 用 waitpid(-1) 回收所有子进程，逐个通知注册的回调
 */
static void kvs_sigchld_handler(int signo) {
    (void)signo;
    int status; pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0)
        for (int i = 0; i < g_reap_n; i++)
            g_reap_cbs[i](pid, status);
}

/**
 * 初始化子进程回收机制（幂等）
 */
void kvs_child_reap_init(void) {
    static int installed = 0;
    if (installed) return;
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = kvs_sigchld_handler;
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);
    installed = 1;
}

/**
 * 注册子进程回收回调（幂等，去重）
 */
void kvs_child_reap_register(kvs_child_reap_fn fn) {
    for (int i = 0; i < g_reap_n; i++)
        if (g_reap_cbs[i] == fn) return;   /* 已注册，跳过 */
    if (g_reap_n < MAX_CHILD_REAP_CBS)
        g_reap_cbs[g_reap_n++] = fn;
}

/*=============================================================================
 *  异步保存（fork + 子进程 io_uring 写入）
 *  Redis 风格：同时只运行一个 BGSAVE
 *============================================================================*/

/**
 * BGSAVE 子进程退出回调
 * 清 PID（子进程已被统一 SIGCHLD 回收）
 */
static void rdb_reap_child(pid_t pid, int status) {
    (void)status;
    if (pid == g_rdb_child_pid)
        g_rdb_child_pid = 0;
}

/**
 * 异步 RDB 保存（BGSAVE）
 * @return 0 成功（或已触发），-1 已在运行
 */
int rdb_save_async_uring(void) {
    kvs_child_reap_init();
    kvs_child_reap_register(rdb_reap_child);

    if (g_rdb_child_pid > 0)
        return -1;   /* BGSAVE 已在运行 */

    pid_t pid = fork();
    if (pid < 0) {
        /* fork 失败：fallback 到同步保存 */
        fprintf(stderr, "[RDB] fork failed (%s), falling back to sync save\n",
                strerror(errno));
        rdb_save_sync();
        return 0;
    }
    
    if (pid == 0) {
        /* 子进程：执行同步保存后退出 */
        rdb_save_sync();
        fflush(stdout);
        fflush(stderr);
        _exit(0);
    }
    
    g_rdb_child_pid = pid;
    printf("[RDB] BGSAVE started, child pid=%d\n", (int)pid);
    return 0;
}

/*=============================================================================
 *  RDB 加载
 *  支持：mmap 文件加载、流式加载（全量同步）
 *============================================================================*/

/**
 * 从流中读取数据（边界检查）
 */
static const char *mread(const char *p, const char *end, void *out, size_t n) {
    if (p + n > end) return NULL;
    memcpy(out, p, n);
    return p + n;
}

/**
 * 开始流式 RDB 加载
 * 读取 key_count 头部，清空现有数据
 * @return key_count（成功），-1（失败）
 */
int rdb_load_stream_begin(const char *buf, size_t len) {
    if (len < 4) return -1;  /* 至少要有 key_count */
    
    int key_count = 0;
    memcpy(&key_count, buf, sizeof(int));
    
    /* 合理性检查：防垃圾/损坏输入 */
    if (key_count < 0 || key_count > RDB_MAX_KEY_COUNT) {
        fprintf(stderr, "[RDB] invalid key_count: %d\n", key_count);
        return -1;
    }
    
    kvs_flush_all();  /* 清空现有数据 */
    printf("[RDB] loading %d keys\n", key_count);
    return key_count;
}

/**
 * 追加流式 RDB 数据（到缓冲末尾）
 * @return 成功加载的条目数
 */
int rdb_load_stream_append(const char *buf, size_t len) {
    return rdb_load_stream_append_n(buf, len, -1, NULL);
}

/**
 * 追加流式 RDB 数据（限条数版）
 * 最多解析 limit 条（-1 = 不限，读到缓冲末尾）。
 * 单块全量传输专用：数据长度由流头 key_count 自描述，
 * 解析到第 key_count 条即停止，无需外部字节数（见 rdma.c）。
 *
 * @param limit 最多解析条目数（-1 = 不限）
 * @param consumed 输出：实际消费字节数（可为 NULL）
 * @return 成功加载的条目数
 */
int rdb_load_stream_append_n(const char *buf, size_t len, int limit,
                             size_t *consumed) {
    const char *p   = buf;
    const char *end = buf + len;
    int loaded = 0;

    while (p < end) {
        if (limit >= 0 && loaded >= limit) break;

        int klen = 0, dlen = 0;
        long long expire = 0;

        /* 读取 key 长度 */
        if (!(p = mread(p, end, &klen, sizeof(int)))) break;
        if (p + klen > end) break;
        const char *ks = p;
        p += klen;

        /* 读取 value 长度 */
        if (!(p = mread(p, end, &dlen, sizeof(int)))) break;
        if (p + dlen > end) break;

        /* 添加条目到字典 */
        uint64_t h = kv_hash((const uint8_t *)ks, (uint32_t)klen);
        kv_entry_t *e = dict_add(&g_dict, ks, (uint32_t)klen,
                                 p, (uint32_t)dlen, h);
        p += dlen;

        /* 读取过期时间 */
        if (!(p = mread(p, end, &expire, sizeof(long long)))) break;
        if (e && expire) e->expire = expire;
        loaded++;
    }

    if (consumed) *consumed = (size_t)(p - buf);
    return loaded;
}

/**
 * 从内存缓冲区加载 RDB（全量同步用）
 * @return 加载的条目数（成功），-1（数据损坏）
 */
int rdb_load_from_buf(const char *buf, size_t len) {
    if (len < 4) return -1;
    
    int expected = rdb_load_stream_begin(buf, 4);
    if (expected < 0) return -1;
    
    int loaded = rdb_load_stream_append(buf + 4, len - 4);
    if (loaded < 0) {
        dict_clear(&g_dict);
        return -1;
    }
    
    /* 核心校验：加载数量必须等于头部声明的数量 */
    if (loaded != expected) {
        fprintf(stderr, "[RDB] corruption: expected %d keys, got %d\n", 
                expected, loaded);
        dict_clear(&g_dict);
        return -1;
    }
    
    printf("[RDB] loaded %d keys\n", loaded);
    return loaded;
}

/**
 * mmap 加载 RDB 文件（启动时调用）
 */
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