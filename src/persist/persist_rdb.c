/*=============================================================================
 *  persist_rdb.c — RDB 持久化（fork + io_uring 写入）和加载（mmap）
 *  
 *  职责：
 *  1. RDB 保存：同步（rdb_save_sync）和异步 fork（rdb_save_async_uring）
 *  2. RDB 加载：mmap 加载和流式加载（全量同步用）
 *  3. 流式序列化器：RDMA 全量同步 Master 端使用
 *  4. 子进程回收：BGSAVE 入口 waitpid 探活（ECHILD=全局 handler 已回收）
 *============================================================================*/

#define _GNU_SOURCE   /* close_range: 保存子进程关掉继承的服务端 fd */
#include "persistence.h"
#include "kvstore.h"
#include "dynbuf.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
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
 *  全局状态
 *============================================================================*/

static pid_t g_rdb_child_pid = 0;  /* BGSAVE 子进程 PID，0=未运行 */

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
 * 分块异步写入 + fsync（io_uring 是项目基石，无阻塞写回退）
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
        fprintf(stderr, "[RDB] io_uring_queue_init failed: %s\n", strerror(errno));
        return -1;
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

    /* 最终 fsync：用 IOSQE_IO_DRAIN 保证先于所有写完成
     * 写循环结束后 SQE 必然空闲，get_sqe 失败即按保存失败处理（fail-closed） */
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
    DynBuf b;
    dynbuf_init(&b, KVS_NET_BUF_INIT);
    int key_count = (int)dict_count(&g_dict);

    /* 使用流式序列化器序列化所有数据到 DynBuf */
    rdb_serializer_t rs;
    rdb_serialize_begin(&rs);
    int done = 0;
    while (!done) {
        size_t n = rdb_serialize_next(&rs, b.buf + b.len, (size_t)(b.cap - b.len), &done);
        if (n == 0 && !done) {
            /* 放不下下一条 entry，扩容再试 */
            if (dynbuf_reserve(&b, b.len + 64 * 1024) < 0) {
                fprintf(stderr, "[RDB] serialization buffer grow failed\n");
                rdb_serialize_end(&rs);
                dynbuf_free(&b);
                return;
            }
            continue;
        }
        b.len += (int)n;
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
        dynbuf_free(&b);
        return;
    }

    /* 写失败绝不能把残缺文件 rename 成 dump.rdb
     * 否则下次启动会拿损坏快照清空整库 */
    if (rdb_write_uring(fd, b.buf, (size_t)b.len) < 0) {
        close(fd);
        unlink(tmp_path);
        dynbuf_free(&b);
        return;
    }
    close(fd);
    rename(tmp_path, dst_path);

    size_t nbytes = (size_t)b.len;  /* dynbuf_free 会清零 len，先取字节数 */
    dynbuf_free(&b);
    printf("[RDB] saved via io_uring, %zu bytes (%d keys)\n", nbytes, key_count);
}

/*=============================================================================
 *  异步保存（fork + 子进程 io_uring 写入）
 *  Redis 风格：同时只运行一个 BGSAVE
 *  子进程回收：全局 SIGCHLD handler 已回收，此处仅探活确认
 *============================================================================*/

/**
 * 异步 RDB 保存（BGSAVE）
 * @return 0 成功（或已触发），-1 已在运行
 */
int rdb_save_async_uring(void) {
    /* 探活上一轮子进程：已退出（SIGCHLD handler 已回收 → ECHILD）则放行，
     * 仍存活则拒绝 */
    if (g_rdb_child_pid > 0) {
        if (waitpid(g_rdb_child_pid, NULL, WNOHANG) == 0)
            return -1;   /* BGSAVE 仍在运行 */
        g_rdb_child_pid = 0;   /* 已结束 */
    }

    pid_t pid = fork();
    if (pid < 0) {
        /* fork 失败：fallback 到同步保存 */
        fprintf(stderr, "[RDB] fork failed (%s), falling back to sync save\n",
                strerror(errno));
        rdb_save_sync();
        return 0;
    }
    
    if (pid == 0) {
        /* 子进程：执行同步保存后退出
         * 先关掉继承的服务端 fd(监听 socket/io_uring/连接 fd):
         * 否则保存子进程把监听端口一直钉在 LISTEN 态, 父进程被 SIGTERM
         * 退出后端口仍被占用, 快速重启会 bind: Address already in use (实测踩坑) */
        close_range(3, ~0U, 0);
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
 * 追加流式 RDB 数据（限条数版）
 * 最多解析 limit 条（-1 = 不限，读到缓冲末尾）。
 *
 * RDMA 数据面专用（见 rdma.c 调用）：接收块是预分配大缓冲区，
 * 真实数据后是未用空间，limit=key_count 让解析在第 key_count 条即停，
 * 防止把尾部未用空间误当成条目解析。
 * 文件加载路径（rdb_load_from_buf）用 limit=-1 读到缓冲末尾。
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
    
    int loaded = rdb_load_stream_append_n(buf + 4, len - 4, -1, NULL);
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