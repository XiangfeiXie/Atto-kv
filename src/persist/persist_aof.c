/*=============================================================================
 *  persist_aof.c — AOF 追加日志（io_uring 异步落盘）+ 启动重放
 *
 *  职责：
 *  1. 追加写：主线程把 RESP 命令 memcpy 进共享块池的当前块，块满立即移交、
 *     半块到时（年龄节拍）移交；整块入队给专用落盘线程；落盘线程用 io_uring
 *     异步写 appendonly.aof。只存 RESP 格式——网络层已拒绝内联命令，此处原样落盘
 *  2. 启动重放：aof_reload_mmap 用 mmap 重放 RESP 命令（文件内单一格式）
 *
 *  对外接口见 persistence.h（AOF 接口已并入）
 *
 *  线程模型与职责划分（主线程生产者 + 落盘线程消费者，无请求-授权）：
 *    - 主线程（生产者，唯一能碰当前块）：
 *        * 热路径逐命令 memcpy 无锁进当前块；块满立即封入队
 *        * 半块由自身周期节拍按年龄封：kvs_aof_periodic（事件循环每轮调，
 *          引擎空闲 100ms 级必醒）见当前块首字节后攒够 AOF_SEAL_MS 即封
 *        —— 发布节奏归主线程：谁持有块谁决定何时交，无需 worker 来催
 *    - 落盘线程（消费者，io_uring 单属主 + fsync 属主）：
 *        * 等 ready 链非空 → 整批提交 io_uring 写 → 收割 CQE → 块回 free 链
 *        * fsync 节奏完全按自身单调钟：always 每批 / everysec 距上次 fsync
 *          满 AOF_FSYNC_INTERVAL_MS（无脏数据不空发）—— 不需向主线程要任何东西
 *    - 块 = 任务 = io_uring 写源（同一内存，无第二层缓冲）。
 *
 *  为什么发布节奏归主线程、fsync 节奏归落盘线程：
 *    - 当前块归主线程独占（memcpy 无锁的前提），封块动作只能主线程做——
 *      故「何时停止攒批、交出去」由主线程自己的钟定，worker 若想封半块必须
 *      跨线程请求（旧版请求-授权，已删）；
 *    - fsync 只由 worker 提交（io_uring 单属主）——故「多久刷一次盘」由
 *      worker 自己的钟定，主线程若想刷盘也必须跨线程请求。
 *    两钟各归其主、零跨线程握手；相位可错开 → 耐久窗口相加：
 *    半块攒批(AOF_SEAL_MS) + 引擎帧(~100ms) + everysec fsync 周期(~1s) ≈ 1.35s
 *    （稀疏写时上次 fsync 早已到期 → 实际刷新间隔更短）。
 *
 *  背压唯一来源：free 链空时封块点阻塞（有界，落盘线程落后于生产才触发）。
 *============================================================================*/

#include "persistence.h"
#include "kvstore.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/mman.h>
#include <liburing.h>

/*=============================================================================
 *  可调参数（宏；默认内存 = 旧版环形槽一致：16 x 512KB = 8MB）
 *============================================================================*/

#define AOF_POOL_BLOCKS        16                         /* 块池块数 = in-flight 上限 */
#define AOF_BLOCK_SIZE         (KVS_JOURNAL_MAX_PAYLOAD)  /* 每块容量 = 512KB */
#define AOF_URING_DEPTH        256                        /* io_uring 队列深度 */
#define AOF_WORKER_POLL_MS     100                        /* worker 空闲自醒粒度（查停机/fsync 钟） */
#define AOF_SEAL_MS            250                        /* 主线程封半块节拍：首字节后攒 250ms 交 */
#define AOF_FSYNC_INTERVAL_MS  1000                       /* everysec：fsync 间隔（worker 单调钟） */

/*=============================================================================
 *  块池
 *  块状态轮转：FREE →(主线程 memcpy)FILL →(封块入队)READY →(worker 提交)WRITING →FREE
 *============================================================================*/

typedef struct aof_block {
    char      data[AOF_BLOCK_SIZE];   /* 命令数据（io_uring 写源，须存活到 CQE 完成） */
    uint32_t  len;                    /* 本块有效数据长度 */
    struct aof_block *next;           /* free/ready 链指针 */
} aof_block_t;

static aof_block_t g_blocks[AOF_POOL_BLOCKS];   /* 静态池：热路径零分配 */

/*=============================================================================
 *  主线程与落盘线程共享（一把锁 + 两条链）
 *============================================================================*/

static pthread_mutex_t g_aof_lock;
static pthread_cond_t  g_ready_cv;    /* worker 等：ready 链非空（主线程封块时 signal） */
static pthread_cond_t  g_free_cv;     /* 主线程等：free 链非空（背压） */
static aof_block_t    *g_free_head;   /* free 链（主线程取，落盘线程还） */
static aof_block_t    *g_ready_head;  /* ready 链 FIFO（主线程入，落盘线程出） */
static aof_block_t    *g_ready_tail;
static int             g_shutdown;    /* 停机标记（持锁读写） */

/*=============================================================================
 *  主线程独占
 *============================================================================*/

static aof_block_t *g_cur;            /* 当前正被填写的块（NULL=未启动/已停机） */
static long long    g_cur_first_ms;   /* 当前块首字节时刻：periodic 封半块节拍起点（主线程独占） */

/*=============================================================================
 *  落盘线程独占（io_uring 单属主）
 *============================================================================*/

static struct io_uring g_aof_ring;
static int          g_aof_ring_ok     = 0;
static int          g_aof_fd          = -1;
static off_t        g_aof_offset      = 0;
static int          g_outstanding     = 0;  /* 在飞写请求数 */
static int          g_dirty           = 0;  /* 存在尚未被 fsync 覆盖的已提交写 */
static int          g_fsync_in_flight = 0;
static long long    g_last_fsync_ms   = 0;  /* 上次 fsync 提交时刻：everysec 节拍起点（worker 独占） */
static pthread_t    g_aof_thread;
static int          g_thread_started  = 0;

/* fsync 策略（启动时由 kvs_aof_set_fsync_policy 设置，落盘线程只读） */
typedef enum {
    FSYNC_NO = 0,        /* 不主动 fsync */
    FSYNC_EVERYSEC = 1,  /* 每拍 fsync（默认） */
    FSYNC_ALWAYS = 2,    /* 每批落盘后 fsync */
} fsync_mode_t;

static fsync_mode_t g_fsync_mode = FSYNC_EVERYSEC;

/*=============================================================================
 *  小工具
 *============================================================================*/

/** 落盘线程用单调时钟毫秒（不受主循环节拍约束） */
static long long aof_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/*=============================================================================
 *  io_uring CQE 处理（落盘线程）
 *  约定 user_data：块指针 = 写完成；NULL = fsync 完成
 *============================================================================*/

static void aof_process_cqe(struct io_uring_cqe *cqe) {
    void *ud = io_uring_cqe_get_data(cqe);
    if (ud == NULL) {
        /* fsync 完成 */
        g_fsync_in_flight = 0;
        if (cqe->res < 0 && cqe->res != -ECANCELED)
            fprintf(stderr, "[AOF] fsync error: %s\n", strerror(-cqe->res));
        return;
    }
    /* 写完成：源缓冲已可复用 → 块放回 free 链 */
    aof_block_t *b = (aof_block_t *)ud;
    if (cqe->res < 0 && cqe->res != -ECANCELED)
        fprintf(stderr, "[AOF] write error: %s\n", strerror(-cqe->res));
    g_outstanding--;
    pthread_mutex_lock(&g_aof_lock);
    b->next = g_free_head;
    g_free_head = b;
    pthread_cond_signal(&g_free_cv);
    pthread_mutex_unlock(&g_aof_lock);
}

/** 收割所有已完成的 CQE（非阻塞） */
static void aof_reap_cqes(void) {
    struct io_uring_cqe *cqe;
    unsigned head;
    unsigned count = 0;
    io_uring_for_each_cqe(&g_aof_ring, head, cqe) {
        aof_process_cqe(cqe);
        count++;
    }
    if (count)
        io_uring_cq_advance(&g_aof_ring, count);
}

/** 阻塞等待在飞写全部完成（fsync 前保证顺序 / 停机排空用） */
static void aof_wait_drain(void) {
    while (g_outstanding > 0) {
        struct io_uring_cqe *cqe = NULL;
        int rc = io_uring_wait_cqe(&g_aof_ring, &cqe);
        if (rc != 0) break;
        if (cqe) {
            aof_process_cqe(cqe);
            io_uring_cqe_seen(&g_aof_ring, cqe);
        }
    }
}

/** 提交异步 fsync。前置：outstanding==0（所有写已入页缓存），顺序正确 */
static void aof_submit_fsync(void) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_aof_ring);
    if (!sqe) return;
    io_uring_prep_fsync(sqe, g_aof_fd, IORING_FSYNC_DATASYNC);
    io_uring_sqe_set_data(sqe, NULL);   /* NULL 标记 = fsync */
    io_uring_submit(&g_aof_ring);
    g_fsync_in_flight = 1;
    g_dirty = 0;                        /* 本次 fsync 覆盖到此刻所有写 */
    g_last_fsync_ms = aof_now_ms();     /* everysec 节拍起点（worker 独占） */
}

/** 取走整条 ready 链并批量提交（整批一次 io_uring_submit） */
static int aof_submit_ready_all(void) {
    pthread_mutex_lock(&g_aof_lock);
    aof_block_t *head = g_ready_head;
    g_ready_head = NULL;
    g_ready_tail = NULL;
    pthread_mutex_unlock(&g_aof_lock);

    int n = 0;
    aof_block_t *b = head;
    while (b) {
        aof_block_t *nxt = b->next;
        struct io_uring_sqe *sqe = io_uring_get_sqe(&g_aof_ring);
        if (!sqe) {
            /* SQ 满：submit+reap 腾位后重试（深度 256 >> 在飞块数 16，正常不触发） */
            io_uring_submit(&g_aof_ring);
            aof_reap_cqes();
            sqe = io_uring_get_sqe(&g_aof_ring);
            if (!sqe) {
                fprintf(stderr, "[AOF] FATAL: no SQE, block lost\n");
                break;
            }
        }
        io_uring_prep_write(sqe, g_aof_fd, b->data, b->len, g_aof_offset);
        g_aof_offset += b->len;
        io_uring_sqe_set_data(sqe, b);
        g_outstanding++;
        n++;
        b = nxt;
    }
    if (n) {
        io_uring_submit(&g_aof_ring);
        g_dirty = 1;
    }
    return n;
}

/*=============================================================================
 *  主线程侧：封块（生产者；发布节奏归主线程）
 *============================================================================*/

/**
 * 持锁：把主线程当前块（非空）封入 ready 尾，并取新 free 块作 g_cur。
 * free 链空则等 g_free_cv（cond_wait 释放锁）——唯一的有界背压点。
 * 停机时 g_cur 置 NULL。
 */
static void aof_seal_current_locked(void) {
    g_cur->next = NULL;
    if (g_ready_tail) g_ready_tail->next = g_cur;
    else              g_ready_head = g_cur;
    g_ready_tail = g_cur;
    pthread_cond_signal(&g_ready_cv);   /* 先入队唤醒 worker，再等 free（更早让出） */

    while (!g_free_head && !g_shutdown)
        pthread_cond_wait(&g_free_cv, &g_aof_lock);
    if (g_shutdown) {
        g_cur = NULL;                   /* 旧块已入队归 worker，主线程不再持有 */
        return;
    }
    g_cur = g_free_head;
    g_free_head = g_cur->next;
    g_cur->len = 0;
}

/** 主线程：封当前块入队并取新 free 块（可能阻塞 = 背压）。
 *  @return 0 成功（含空块无需封）；-1 停机/未启动 */
static int aof_seal_get_next(void) {
    if (!g_cur) return -1;
    if (g_cur->len == 0) return 0;      /* 空块：无需封，继续复用 */
    pthread_mutex_lock(&g_aof_lock);
    aof_seal_current_locked();
    pthread_mutex_unlock(&g_aof_lock);
    return g_cur ? 0 : -1;
}

/*=============================================================================
 *  落盘线程主循环（消费者 + fsync 属主；fsync 节奏归本线程单调钟）
 *============================================================================*/

static void *aof_worker_main(void *arg) {
    (void)arg;

    /* 屏蔽主线程专属信号，避免异步处理落在落盘线程 */
    sigset_t ss;
    sigemptyset(&ss);
    sigaddset(&ss, SIGINT);
    sigaddset(&ss, SIGTERM);
    sigaddset(&ss, SIGCHLD);
    pthread_sigmask(SIG_BLOCK, &ss, NULL);

    g_last_fsync_ms = aof_now_ms();     /* 启动即起 fsync 节拍（everysec 满间隔首发） */

    for (;;) {
        /* 1. 等 ready 链非空；100ms 空闲量子到期即 break（每轮自醒查 fsync 钟/停机） */
        pthread_mutex_lock(&g_aof_lock);
        while (!g_ready_head && !g_shutdown) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += AOF_WORKER_POLL_MS / 1000;
            ts.tv_nsec += (long)(AOF_WORKER_POLL_MS % 1000) * 1000000L;
            if (ts.tv_nsec >= 1000000000L) {
                ts.tv_sec++;
                ts.tv_nsec -= 1000000000L;
            }
            if (pthread_cond_timedwait(&g_ready_cv, &g_aof_lock, &ts) == ETIMEDOUT)
                break;      /* 空闲量子到期 → 出循环检查 fsync 钟 */
        }
        int shutting = g_shutdown;
        pthread_mutex_unlock(&g_aof_lock);

        /* 2. 提交 ready 整批 + 收割（CQE 完成 → 块回 free 链，唤主线程背压点） */
        aof_submit_ready_all();
        aof_reap_cqes();

        /* 3. 停机：排空在飞写 → 最终 fsync → 等 fsync 完成 → 退出 */
        if (shutting) {
            aof_wait_drain();
            if (g_dirty && !g_fsync_in_flight)
                aof_submit_fsync();
            while (g_fsync_in_flight) {
                struct io_uring_cqe *cqe = NULL;
                if (io_uring_wait_cqe(&g_aof_ring, &cqe) == 0 && cqe) {
                    aof_process_cqe(cqe);
                    io_uring_cqe_seen(&g_aof_ring, cqe);
                }
            }
            break;
        }

        /* 4. fsync 决策（半块何时封出归主线程，不在此催；本线程只管何时刷盘）：
         *   always   — 每批落盘后（有写排空即发）
         *   everysec — 距上次 fsync ≥ AOF_FSYNC_INTERVAL_MS 且脏（无数据不空发）
         *   no       — 不发
         * 一律先排空在飞写：fsync 只覆盖已完成的写，顺序正确性 */
        if (g_dirty && !g_fsync_in_flight) {
            int due = (g_fsync_mode == FSYNC_ALWAYS);
            if (!due && g_fsync_mode == FSYNC_EVERYSEC)
                due = (aof_now_ms() - g_last_fsync_ms >= AOF_FSYNC_INTERVAL_MS);
            if (due) {
                aof_wait_drain();
                if (g_dirty && !g_fsync_in_flight)
                    aof_submit_fsync();
            }
        }
    }
    return NULL;
}

/*=============================================================================
 *  文件操作 & 公开 API
 *============================================================================*/

/** 打开 AOF 文件（追加模式）。主线程调用（start），落盘线程接管后续 io_uring */
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
 * 周期性维护函数（主线程事件循环每轮调用）
 * 主线程自己的封半块节拍：当前块非空且首字节后攒够 AOF_SEAL_MS 即封入队。
 * 满块已在 append_raw 封；这里是半块兜底定时（引擎空闲 100ms 级必醒，
 * 见 kvstore.c update_cached_time → 本函数，节拍可靠）。free 链空时阻塞 = 背压。
 */
void kvs_aof_periodic(void) {
    if (!g_cur || g_cur->len == 0) return;                   /* 空块不封 */
    if (aof_now_ms() - g_cur_first_ms < AOF_SEAL_MS) return; /* 首字节后未攒够节拍 */
    aof_seal_get_next();                                     /* 半块封入队 + 取新 free 块 */
}

/**
 * 设置 fsync 策略（启动时调用一次，落盘线程启动前，无锁）
 *   - "always"：每批落盘后 fsync（最安全）
 *   - "everysec"：每秒 fsync（平衡，默认）
 *   - "no"：由操作系统决定（最快，可能丢失数据）
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
 * 启动 AOF 持久化（主线程）
 * 打开文件 + 初始化 io_uring + 组块池 free 链 + 启动落盘线程
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
    }

    pthread_mutex_init(&g_aof_lock, NULL);
    pthread_cond_init(&g_ready_cv, NULL);
    pthread_cond_init(&g_free_cv, NULL);

    /* 组 free 链 */
    g_free_head = NULL;
    for (int i = 0; i < AOF_POOL_BLOCKS; i++) {
        g_blocks[i].len  = 0;
        g_blocks[i].next = g_free_head;
        g_free_head      = &g_blocks[i];
    }
    g_ready_head = NULL;
    g_ready_tail = NULL;
    g_shutdown   = 0;

    g_cur = g_free_head;
    g_free_head = g_cur->next;
    g_cur->len  = 0;
    g_cur_first_ms = 0;               /* periodic 半块节拍起点：首字节 append 时才置 */

    if (pthread_create(&g_aof_thread, NULL, aof_worker_main, NULL) != 0) {
        fprintf(stderr, "[AOF] pthread_create: %s\n", strerror(errno));
        return -1;
    }
    g_thread_started = 1;

    printf("[AOF] worker thread started (%d blocks x %d KB, depth=%d)\n",
           AOF_POOL_BLOCKS, AOF_BLOCK_SIZE / 1024, AOF_URING_DEPTH);
    return 0;
}

/**
 * 热路径：追加原始 RESP 命令到当前块（主线程调用）
 * 逐命令无锁 memcpy；块满才封块入队（加锁移交，每 ~1.2 万命令一次）。
 * 半块封出不在此判断——归 kvs_aof_periodic 的年龄节拍（此处只开节拍起点）。
 * @return 0 成功；-1 未启动/超限/停机（命令仍执行成功，只是不落 AOF）
 */
int kvs_aof_persist_append_raw(const char *payload, uint32_t len) {
    if (!payload || len == 0 || len > AOF_BLOCK_SIZE) return -1;
    if (!g_cur) return -1;      /* 未启动（重放期不重复记）或已停机 */

    if (g_cur->len + len > AOF_BLOCK_SIZE) {
        if (aof_seal_get_next() < 0) return -1;  /* 满块封入队 + 取新块（可能阻塞 = 背压） */
    }
    if (g_cur->len == 0)
        g_cur_first_ms = aof_now_ms();           /* 新块首字节：periodic 封半块节拍起点 */
    memcpy(g_cur->data + g_cur->len, payload, len);
    g_cur->len += len;
    return 0;
}

/**
 * 停止 AOF 持久化（主线程，appends 已停止后）
 * 封残余块 → 置停机 → 唤醒落盘线程排空（写全部完成 + 最终 fsync）→ join → 关 io_uring/fd
 */
void kvs_aof_persist_stop(void) {
    if (!g_thread_started) {
        /* worker 未启动（start 失败等）：直接收尾已打开的 fd */
        if (g_aof_fd >= 0) { close(g_aof_fd); g_aof_fd = -1; }
        if (g_aof_ring_ok) { io_uring_queue_exit(&g_aof_ring); g_aof_ring_ok = 0; }
        return;
    }

    /* 封残余块入 ready，置停机，唤醒落盘线程 */
    pthread_mutex_lock(&g_aof_lock);
    if (g_cur && g_cur->len > 0) {
        g_cur->next = NULL;
        if (g_ready_tail) g_ready_tail->next = g_cur;
        else              g_ready_head = g_cur;
        g_ready_tail = g_cur;
    }
    g_cur = NULL;
    g_shutdown = 1;
    pthread_cond_broadcast(&g_ready_cv);
    pthread_cond_broadcast(&g_free_cv);
    pthread_mutex_unlock(&g_aof_lock);

    /* join：落盘线程内部排空在飞写 + 最终 fsync 后自行退出 */
    pthread_join(g_aof_thread, NULL);
    g_thread_started = 0;

    if (g_aof_fd >= 0) { close(g_aof_fd); g_aof_fd = -1; }
    if (g_aof_ring_ok) { io_uring_queue_exit(&g_aof_ring); g_aof_ring_ok = 0; }
    pthread_mutex_destroy(&g_aof_lock);
    pthread_cond_destroy(&g_ready_cv);
    pthread_cond_destroy(&g_free_cv);

    printf("[AOF] stopped safely\n");
}

/*=============================================================================
 *  AOF 重放（启动时）
 *  使用 mmap 快速加载并重放所有命令
 *  文件只含 RESP 格式（网络层只收 RESP，AOF 原样落盘），
 *  解析失败即文件损坏/截断，停止重放
 *  注：重放期 persist_start 尚未调用 → append_raw 返回 -1 丢弃，
 *      与旧版「重放后 memset 槽位」等效，避免重放命令重复记入新文件
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

    int journaled = 0;   /* 重放期丢弃：回放不落新 AOF/复制（g_in_reload 下 kvs_journal_effect 也短路） */
    const char *p = base;
    long remaining = (long)fsz;
    while (remaining > 0) {
        int argc = 0;
        int cmd_len = resp_parse_slices(p, (int)remaining,
                                        tokens, KVS_MAX_TOKENS, &argc);
        /* AOF 只存 RESP（网络层已拒绝非 RESP），非 RESP 即损坏/截断 */
        if (cmd_len <= 0) break;
        dynbuf_clear(&wb);
        kvs_dispatch(tokens, argc, &wb, &journaled);
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
