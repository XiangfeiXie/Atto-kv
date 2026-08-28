/*=============================================================================
 *  persist_aof.c — AOF 追加日志（io_uring 异步落盘）+ 启动重放
 *
 *  职责：
 *  1. 追加写：单线程无锁 io_uring 环形缓冲落盘（appendonly.aof）
 *  2. 启动重放：aof_reload_mmap 用 mmap 重放 raw RESP 命令
 *
 *  对外接口见 persistence.h（AOF 接口已并入）
 *============================================================================*/

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
#include <liburing.h>

/*=============================================================================
 *  可调参数
 *============================================================================*/

#define AOF_RING_SIZE           16                     /* 环形缓冲区槽数 */
#define AOF_SLOT_SIZE           (KVS_JOURNAL_MAX_PAYLOAD)  /* 每个槽大小 = 512KB */
#define AOF_URING_DEPTH         256                    /* io_uring 队列深度 */

/*=============================================================================
 *  环形缓冲区槽位
 *  每个槽可存储一条完整的 RESP 命令
 *============================================================================*/

typedef struct {
    char      data[AOF_SLOT_SIZE];   /* 命令数据 */
    uint32_t  len;                   /* 本槽有效数据长度 */
    int       in_flight;             /* 是否已提交写请求，等待完成 */
} aof_slot_t;

/*=============================================================================
 *  全局状态（单线程，无需锁）
 *============================================================================*/

/* 环形缓冲区 */
static aof_slot_t   g_ring[AOF_RING_SIZE];
static int          g_prod_idx       = 0;   /* 当前写入槽位索引 */
static uint32_t     g_prod_pos       = 0;   /* 当前槽位已用字节数 */
static int          g_inflight_count = 0;   /* in_flight=1 的槽数 */

/* io_uring 实例 */
static struct io_uring  g_aof_ring;
static int              g_aof_ring_ok = 0;
static int              g_aof_fd      = -1;
static off_t            g_aof_offset  = 0;

/* fsync 策略 */
typedef enum {
    FSYNC_NO = 0,        /* 不主动 fsync */
    FSYNC_EVERYSEC = 1,  /* 每秒一次（默认） */
    FSYNC_ALWAYS = 2,    /* 每条命令后 fsync */
} fsync_mode_t;

static fsync_mode_t g_fsync_mode      = FSYNC_EVERYSEC;
static int          g_fsync_in_flight = 0;   /* fsync 请求是否在飞 */
static time_t       g_last_fsync      = 0;   /* 上次 fsync 时间 */

/*=============================================================================
 *  io_uring 辅助函数
 *============================================================================*/

/**
 * 处理一个 CQE 完成事件
 * 数据携带：槽位索引（>= 0）或 -1（fsync 完成）
 */
static void aof_handle_cqe(struct io_uring_cqe *cqe) {
    int idx = (int)(uintptr_t)io_uring_cqe_get_data(cqe);
    if (idx == -1) {
        /* fsync 完成 */
        g_fsync_in_flight = 0;
    } else if (idx >= 0 && idx < AOF_RING_SIZE) {
        /* 写入完成 */
        if (g_ring[idx].in_flight) {
            g_ring[idx].in_flight = 0;
            g_inflight_count--;
        }
    }
}

/**
 * 收割所有已完成的 CQE
 * 调用时机：
 * - aof_flush_current_slot（阈值 flush）
 * - kvs_aof_periodic（空闲维护）
 */
static void aof_reap_completions(void) {
    if (!g_aof_ring_ok) return;

    struct io_uring_cqe *cqe;
    unsigned head;
    unsigned count = 0;

    io_uring_for_each_cqe(&g_aof_ring, head, cqe) {
        int slot_idx = (int)(uintptr_t)io_uring_cqe_get_data(cqe);
        aof_handle_cqe(cqe);
        
        /* 记录错误（但继续处理） */
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

/**
 * 获取一个 SQE
 * SQ 满时先 submit 已提交请求并收割完成，腾出 SQE 再试
 * 仍拿不到返回 NULL，由调用方同步写兜底
 */
static struct io_uring_sqe *aof_get_sqe(void) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_aof_ring);
    if (sqe) return sqe;

    /* 尝试腾出空间 */
    io_uring_submit(&g_aof_ring);
    aof_reap_completions();
    return io_uring_get_sqe(&g_aof_ring);
}

/*=============================================================================
 *  文件操作 & fsync
 *============================================================================*/

/**
 * 打开 AOF 文件（追加模式）
 */
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

/**
 * 提交异步 fsync（非阻塞）
 * 完成后由 aof_handle_cqe 标记 g_fsync_in_flight = 0
 */
static void aof_submit_fsync(void) {
    if (g_aof_fd < 0 || !g_aof_ring_ok || g_fsync_in_flight) return;

    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_aof_ring);
    if (sqe) {
        io_uring_prep_fsync(sqe, g_aof_fd, IORING_FSYNC_DATASYNC);
        io_uring_sqe_set_data(sqe, (void *)(uintptr_t)-1);  /* -1 表示 fsync */
        io_uring_submit(&g_aof_ring);
        g_fsync_in_flight = 1;
        g_last_fsync = time(NULL);
    }
}

/**
 * 检查并可能提交 fsync（非阻塞）
 * 根据策略决定是否提交
 */
static void aof_fsync_check(void) {
    switch (g_fsync_mode) {
    case FSYNC_ALWAYS:
        /* always 模式：每个周期都提交 fsync */
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
        /* 不主动 fsync，依赖 OS 页缓存 */
        break;
    }
}

/*=============================================================================
 *  槽位刷新
 *  将当前槽位通过 io_uring 异步写入
 *============================================================================*/

/**
 * 提交当前槽位写入，然后滚动到下一个槽位
 * 必须在单线程事件循环中调用
 */
static void aof_flush_current_slot(void) {
    if (g_prod_pos == 0) return;

    aof_slot_t *slot = &g_ring[g_prod_idx];
    slot->len = g_prod_pos;

    /* 如果槽位还在飞，等待完成（仅当磁盘慢于环形缓冲区时发生） */
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

    /* 获取 SQE 并提交异步写入 */
    struct io_uring_sqe *sqe = aof_get_sqe();
    if (sqe) {
        io_uring_prep_write(sqe, g_aof_fd, slot->data, slot->len, g_aof_offset);
        io_uring_sqe_set_data(sqe, (void *)(uintptr_t)(long)g_prod_idx);
        g_aof_offset += slot->len;
        slot->in_flight = 1;
        g_inflight_count++;
        io_uring_submit(&g_aof_ring);

        /* 顺便收割已完成的请求 */
        aof_reap_completions();
    } else {
        /* 灾难性情况：fallback 到同步写入 */
        fprintf(stderr, "[AOF] FATAL: no SQE, using sync write fallback\n");
        ssize_t w = write(g_aof_fd, slot->data, slot->len);
        if (w < 0)
            fprintf(stderr, "[AOF] sync write error: %s\n", strerror(errno));
        /* slot 不标记 in_flight，可立即复用 */
    }

    /* 滚动到下一个槽位 */
    g_prod_idx = (g_prod_idx + 1) % AOF_RING_SIZE;
    g_prod_pos = 0;
}

/*=============================================================================
 *  公开 API
 *============================================================================*/

/**
 * 周期性维护函数
 * 由事件循环（update_cached_time）每轮调用
 * 
 * 职责：
 * 1. 收割已完成的 CQE
 * 2. 部分刷新（每 1 秒刷新一次）
 * 3. 检查 fsync 定时器
 */
void kvs_aof_periodic(void) {
    if (!g_aof_ring_ok) return;

    /* 收割已完成的 CQE（释放 in_flight 槽位） */
    aof_reap_completions();

    /*-------------------------------------------------------------------------
     *  部分刷新：时间驱动，非 tick 驱动
     *  
     *  为什么不用 tick 计数器？
     *  - reactor 空闲约 10 Hz
     *  - proactor 空闲约 10 kHz
     *  使用 tick 计数器会导致 reactor 延迟 flush 最多 10 秒
     *  因此改用 wall-clock 时间判断
     *-------------------------------------------------------------------------*/
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

    /* 检查 fsync 定时器 */
    aof_fsync_check();
}

/**
 * 设置 fsync 策略
 */
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

/**
 * 启动 AOF 持久化
 */
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

/**
 * 热路径：追加原始 RESP 字节到环形缓冲区
 * 无锁，单线程调用
 */
int kvs_aof_persist_append_raw(const char *payload, uint32_t len) {
    if (!payload || len == 0 || len > KVS_JOURNAL_MAX_PAYLOAD) return -1;

    /* 当前槽放不下新记录就 flush
     * flush 后槽空，AOF_SLOT_SIZE == KVS_JOURNAL_MAX_PAYLOAD
     * 保证任何合法记录都能装下，不会越界 */
    if (g_prod_pos + len > AOF_SLOT_SIZE) {
        aof_flush_current_slot();
    }

    /* 单次 memcpy — 无 header、无 CRC、无 time() 系统调用 */
    memcpy(g_ring[g_prod_idx].data + g_prod_pos, payload, len);
    g_prod_pos += len;

    return 0;
}

/**
 * 停止 AOF 持久化
 * 刷新所有待写入数据，等待完成，最终 fsync
 */
void kvs_aof_persist_stop(void) {
    /* 刷新剩余部分槽位 */
    if (g_prod_pos > 0)
        aof_flush_current_slot();

    /* 等待所有在飞写入完成 */
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

    /* 最终 fsync */
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

/*=============================================================================
 *  AOF 重放（启动时）
 *  使用 mmap 快速加载并重放所有命令
 *============================================================================*/

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