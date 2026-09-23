/*=============================================================================
 *  persist_rdb.c — RDB 落盘 + RDMA 全量同步的 RDB 字节定义
 *
 *  文件布局（自上而下，落盘主体在上、RDMA 辅助在尾，便于阅读）：
 *
 *  ① RDB 格式与 CRC 公共基础
 *     条目磁盘字节序 + crc32：磁盘落盘（②）与 RDMA 无盘流（④）共用同一
 *     字节序；加载端（③）按同一布局解析两种流。
 *
 *  ② RDB 磁盘落盘（SAVE / BGSAVE）——本文件主体
 *     逐条目 io_uring gather 直写 dump.rdb：无内存块、零拷贝，窗口流水线
 *     异步落盘；忠实全量快照（任意长度条目都完整保存，无丢弃/无尺寸上限），
 *     一遍扫描 key_count O(1) 预知，末尾一次性补写 8B 头 + 一次 fsync +
 *     tmp→rename 原子发布。落盘格式带 crc 槽（防篡改/损坏仍被加载）。
 *     SAVE（主线程阻塞）/ BGSAVE（fork 子进程 COW）共用 rdb_save_sync。
 *
 *  ③ RDB 加载（磁盘启动 / sendfile 对端 / RDMA 流共用）
 *     磁盘文件校验 crc（防篡改/损坏）；RDMA 无盘流无 crc，直接流式解析。
 *
 *  ④ RDMA 全量同步序列化器（Master 端无盘流，文件尾部辅助段）
 *     RDMA 全量传输对标 Redis 无盘同步：RDMA 只是本项目的一种传输方式，
 *     本序列化器即为其服务——Master（rdma.c）用它把整份快照序列化进 MR
 *     缓冲直传，Slave（rdma.c）用 ③ 的流式加载消费同一流。磁盘落盘（②）
 *     已不再走本序列化器（磁盘格式的 crc 分支随之删除，RDMA 侧零改动）。
 *
 *  子进程回收：BGSAVE fork 子进程 waitpid 探活（ECHILD=全局 handler 已回收）
 *============================================================================*/

#define _GNU_SOURCE   /* close_range: 保存子进程关掉继承的服务端 fd */
#include "persistence.h"
#include "kv_engine.h"    /* dict_* / entry_* / kv_iter_t / kv_node_t */
#include "kvstore.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <liburing.h>
#include <zlib.h>       /* crc32: RDB 流头防篡改校验（工程已链 zlib） */
#include <sys/mman.h>
#include <sys/wait.h>

/*=============================================================================
 *  常量定义
 *============================================================================*/

#define RDB_TMP "build/dump.rdb.tmp"  /* 临时文件路径 */

/* 逐条目 io_uring 流水线参数（无 staging 块）：
 * - RDB_SAVE_WINDOW：在飞 SQE 上限 = iov 槽数（槽只装 5 段 iov，~数百 B）
 * - RDB_SUBMIT_BATCH：攒满多少条 SQE 才一次 io_uring_submit（批提交降系统调用）
 * - RDB_URING_DEPTH：io_uring 队列深度（≥ 在飞窗口 + 排队批）
 * 窗口按 SQE 条数计而非字节数 → 巨型条目与微小条目共用同一窗口，无特判 */
#define RDB_SAVE_WINDOW   64
#define RDB_SUBMIT_BATCH  32
#define RDB_URING_DEPTH   128

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
 *  ① RDB 格式与 CRC 公共基础
 *
 *  条目磁盘字节序（② 落盘与 ④ RDMA 流共用；③ 加载端按此解析）：
 *  [klen:4][key:klen][dlen:4][data:dlen][expire:8]
 *
 *  crc32：② 落盘时对 key_count + 每条目逐段实时续算、末尾随 8B 头落槽；
 *  ③ 磁盘加载用同一函数重算比对（防篡改/损坏）。④ RDMA 无盘流不含 crc。
 *============================================================================*/

/**
 * 计算/续算 crc32（zlib），按 ~1MB 分块喂入避免 uInt 上限
 * 传入上一次的返回值即可对不连续内存续算
 * @param crc 上次 crc（首段传 0）
 * @param p 数据
 * @param n 字节数
 * @return 更新后的 crc32
 */
static uint32_t rdb_crc32(uint32_t crc, const void *p, size_t n) {
    const unsigned char *b = (const unsigned char *)p;
    while (n) {
        size_t k = n > 0xfffff0u ? 0xfffff0u : n;   /* ~1MB 一块 */
        crc = (uint32_t)crc32(crc, b, (uInt)k);
        b += k;
        n -= k;
    }
    return crc;
}

/*=============================================================================
 *  ② RDB 磁盘落盘（SAVE / BGSAVE）
 *
 *  本大节是 persist_rdb.c 的主体：把 dict 快照以"逐条目 io_uring 流水线"
 *  忠实直写 dump.rdb（详细语义见各小节）。大节顺序：
 *    a. io_uring 同步写辅助（排空后尾部 8B 头补写 + 一次 fsync）
 *    b. 逐条目写入器（5 段 writev gather，无块零拷贝，窗口流水线）
 *    c. rdb_save_sync：一遍全量扫描 + 末尾补头 → SAVE / BGSAVE 共用
 *    d. 异步保存 fork（BGSAVE）+ 子进程探活守卫
 *============================================================================*/

/*---------------------------------------------------------------------------
 *  ②a io_uring 同步写辅助（供逐条目流水线尾部使用）
 *  主体流水线见 ②b：每条 entry 5 段 writev 异步 gather dict 内存，零拷贝
 *  无 staging 块，槽空才等 CQE（背压闸门），排空后本小节的同步辅助补写
 *  尾部 8B 文件头并 fsync。io_uring 是项目基石，无阻塞写回退。
 *---------------------------------------------------------------------------*/

/**
 * 把 buf[0..len) 写到 fd 的 off 处，并等该写完成
 * 只在收尾路径用（唯一调用者），故不设 user_data 标记：本函数只认自己这条
 * CQE，且只读 cqe->res，标记了也没人看。
 * @return 0 成功（调用方可安全复用 buf），-1 失败
 */
static int rdb_save_flush(int fd, struct io_uring *ring,
                          const char *buf, size_t len, off_t off) {
    if (len == 0) return 0;

    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) return -1;
    io_uring_prep_write(sqe, fd, buf, len, off);
    io_uring_submit(ring);

    struct io_uring_cqe *cqe = NULL;
    if (io_uring_wait_cqe(ring, &cqe) < 0) {
        fprintf(stderr, "[RDB] io_uring_wait_cqe failed: %s\n", strerror(errno));
        return -1;
    }
    int res = cqe->res;
    io_uring_cqe_seen(ring, cqe);

    if (res < 0 || (size_t)res != len) {
        fprintf(stderr, "[RDB] uring write error: %s\n",
                res < 0 ? strerror(-res) : "short write");
        return -1;
    }
    return 0;
}

/**
 * 所有条目写完后落盘一次（fsync 须覆盖之前所有写）
 * 走到这里必经 rdb_writer_drain（在飞清零）+ rdb_save_flush（等自己 CQE）
 * 两步，ring 里已无先前请求，故不需要 IOSQE_IO_DRAIN 之类的排空标志。
 */
static int rdb_save_fsync(int fd, struct io_uring *ring) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) return -1;
    io_uring_prep_fsync(sqe, fd, IORING_FSYNC_DATASYNC);
    io_uring_submit(ring);

    struct io_uring_cqe *cqe = NULL;
    if (io_uring_wait_cqe(ring, &cqe) < 0) {
        fprintf(stderr, "[RDB] fsync wait failed: %s\n", strerror(errno));
        return -1;
    }
    int res = cqe->res;
    io_uring_cqe_seen(ring, cqe);
    if (res != 0) {
        fprintf(stderr, "[RDB] fsync failed: %s\n", strerror(-res));
        return -1;
    }
    return 0;
}

/*---------------------------------------------------------------------------
 *  ②b 逐条目写入器（rdb_save_sync 专用）
 *
 *  无块一条一写：每条 entry 拼成 5 段 iov 直接 gather dict 内存发 writev，
 *  无 staging 块、无拷贝。槽 = 每 SQE 在飞的 5 段 iov（固定池），CQE user_data
 *  指向槽 → 收割即归还（乱序完成安全）；槽空才等一个 CQE = 流水线背压闸门。
 *  在飞 ≤ RDB_SAVE_WINDOW。iov 只须存活到对应 CQE 完成 → drain 在函数内排空，
 *  槽可放自动存储（进程内单保存者，SAVE/BGSAVE 互斥）。
 *
 *  磁盘字节顺序（= ① 公共字节序，与加载端一致）：
 *  [klen:4][key:klen][dlen:4][data:dlen][expire:8]
 *  crc 按同一顺序逐段续算（种子已含 key_count），全部排空后与 key_count
 *  一起作为 8B 文件头一次性补写。
 *---------------------------------------------------------------------------*/

typedef struct {
    struct iovec v[5];               /* 单条 entry 的 5 段 gather */
    /* 头部 3 个标量的暂存区。
     * 旧版直接取 &e->key_len / &e->val_len / &e->expire 摊进 iovec；记录类型
     * 各引擎不同后取字段地址不再可能（访问器只能给值），于是先落到槽里。
     * 写盘与 crc 都读这份副本 ⇒ 两者读的是同一块字节，比旧版「各读一次字段」
     * 更严格（旧版一致只是靠约定）。载荷（v[1]/v[3]）仍然零拷贝。
     * 生命周期：槽在本条写对应的 CQE 完成后才归还，旧版要求 e 活到 drain 之后，
     * 现在只要槽不归还即可 —— 是变短，不是变长。 */
    int32_t klen;
    int32_t vlen;
    int64_t expire;
} rdb_iov_slot_t;

typedef struct {
    int              fd;
    struct io_uring  ring;
    off_t            off;            /* 下一写文件偏移，从 RDB_FILE_HDR_LEN 起 */
    rdb_iov_slot_t   slots[RDB_SAVE_WINDOW];
    int              free_idx[RDB_SAVE_WINDOW];  /* 空闲槽栈 */
    int              free_n;         /* 空闲槽数 */
    int              queued;         /* 已入 SQ 未 submit 的 SQE 数 */
    int              inflight;       /* 在飞未收割 SQE 数（含 SQ 未 submit） */
    uint32_t         crc;            /* 运行 crc（已含 key_count 种子） */
    int              err;            /* 任一写失败置位，收尾统一判定 */
} rdb_writer_t;

/**
 * 收割一个 CQE：校验结果、把 user_data 指向的槽归还空闲栈。
 * 只对纯写 CQE 调用（fsync/头部补写走 rdb_save_flush/fsync 同步路径），
 * user_data 恒为槽指针。
 */
static void rdb_writer_reap_one(rdb_writer_t *w) {
    struct io_uring_cqe *cqe = NULL;
    if (io_uring_wait_cqe(&w->ring, &cqe) < 0 || !cqe) {
        fprintf(stderr, "[RDB] io_uring_wait_cqe failed: %s\n", strerror(errno));
        w->err = 1;
        return;
    }
    rdb_iov_slot_t *s = io_uring_cqe_get_data(cqe);
    ssize_t want = 0;
    for (int i = 0; i < 5; i++) want += s->v[i].iov_len;
    if (cqe->res < 0 || cqe->res != want) {
        fprintf(stderr, "[RDB] %s: %s\n",
                cqe->res < 0 ? "uring write error" : "uring short write",
                cqe->res < 0 ? strerror(-cqe->res) : "iov bytes mismatch");
        w->err = 1;   /* 记录失败；drain 仍收割完全部 CQE（槽全部归还）后统一收尾 */
    }
    io_uring_cqe_seen(&w->ring, cqe);
    w->free_idx[w->free_n++] = (int)(s - w->slots);  /* 槽归还（该槽的写已完成） */
    w->inflight--;
}

/**
 * 发出一条 entry：5 段 gather + 同序 crc 续算 + 窗口/批管理。
 * 全量快照语义：不做任何长度判断/丢弃，任意长度条目都完整落盘。
 * @param e 要落盘的 entry（SAVE 主线程阻塞期冻结 / BGSAVE 子进程 COW，
 *          源内存稳定到该条的 CQE 完成 —— key/payload 仍是零拷贝，头部 3 个
 *          标量已暂存进槽，见 rdb_iov_slot_t）
 */
static void rdb_writer_emit(rdb_writer_t *w, kv_node_t *e) {
    const char *key = entry_key(e);
    char       *val = entry_payload(e);
    size_t      klen = (size_t)entry_key_len(e);
    size_t      vlen = (size_t)entry_payload_len(e);

    /* 无空闲槽：把排队批 submit 出去，再收割一个归还槽（背压闸门） */
    if (w->free_n == 0) {
        io_uring_submit(&w->ring);
        rdb_writer_reap_one(w);
        if (w->err || w->free_n == 0) return;
    }

    rdb_iov_slot_t *s = &w->slots[w->free_idx[--w->free_n]];

    /* 头部 3 个标量落槽暂存，写盘与 crc 都从槽里读（同一块字节） */
    s->klen   = (int32_t)klen;
    s->vlen   = (int32_t)vlen;
    s->expire = entry_expire(e);

    s->v[0].iov_base = &s->klen;   s->v[0].iov_len = sizeof(int);
    s->v[1].iov_base = (void *)key; s->v[1].iov_len = klen;
    s->v[2].iov_base = &s->vlen;   s->v[2].iov_len = sizeof(int);
    s->v[3].iov_base = val;        s->v[3].iov_len = vlen;
    s->v[4].iov_base = &s->expire; s->v[4].iov_len = sizeof(long long);

    /* crc 与写盘同序逐段续算：种子 = key_count（rdb_save_sync 已算入） */
    w->crc = rdb_crc32(w->crc, &s->klen, sizeof(int));
    w->crc = rdb_crc32(w->crc, key, klen);
    w->crc = rdb_crc32(w->crc, &s->vlen, sizeof(int));
    w->crc = rdb_crc32(w->crc, val, vlen);
    w->crc = rdb_crc32(w->crc, &s->expire, sizeof(long long));

    /* 取不到 SQE 是不可能的：走到这里 free_n ≥ 1，而恒等式
     * free_n + inflight == RDB_SAVE_WINDOW 给出 inflight ≤ 63；queued 攒满
     * RDB_SUBMIT_BATCH 就 submit 清环，故 SQ 待处理 ≤ 31，深度 128 绰绰有余。
     * 真取空也不必重试：置 err 即整库作废、不 rename（fail-closed） */
    struct io_uring_sqe *sqe = io_uring_get_sqe(&w->ring);
    if (!sqe) { w->err = 1; return; }
    io_uring_prep_writev(sqe, w->fd, s->v, 5, w->off);
    io_uring_sqe_set_data(sqe, s);      /* user_data = 槽指针 → 完成时安全归还 */
    w->inflight++;
    w->queued++;
    w->off += (off_t)(sizeof(int) + klen + sizeof(int) + vlen + sizeof(long long));
    if (w->queued >= RDB_SUBMIT_BATCH) {   /* 批提交降系统调用 */
        io_uring_submit(&w->ring);
        w->queued = 0;
    }
}

/**
 * 排空：submit 尾批 → 收割至全部在飞完成。写失败只置 err，不在中途 abort，
 * 保证全部 CQE 都被收割（槽全部归还），收尾统一判定。
 */
static void rdb_writer_drain(rdb_writer_t *w) {
    if (w->queued > 0) {
        io_uring_submit(&w->ring);
        w->queued = 0;
    }
    while (w->inflight > 0)
        rdb_writer_reap_one(w);
}

/*---------------------------------------------------------------------------
 *  ②c rdb_save_sync（一遍全量 + 末尾补头）
 *  一遍扫描：key_count = dict_count O(1) 预知（全量快照，无丢弃）→ crc 种子确定；
 *  每条目 gather 直写 → 排空 → 末尾一次性补写 8B 头（key_count + crc）→
 *  一次 fsync → tmp→rename 原子发布。SAVE（主线程）与 BGSAVE（fork 子进程）共用。
 *  任意长度 entry 都完整保存（无超块缝/无丢弃）：旧块模型"单条 >1MB 中止整库
 *  保存"的缺陷随块移除。
 *---------------------------------------------------------------------------*/

void rdb_save_sync(void) {
    int key_count = (int)dict_count();

    /* 写入临时文件 */
    char tmp_path[1024], dst_path[1024];
    strncpy(tmp_path, persist_path(RDB_TMP), sizeof(tmp_path) - 1);
    tmp_path[sizeof(tmp_path) - 1] = '\0';
    strncpy(dst_path, persist_path(RDB_REL), sizeof(dst_path) - 1);
    dst_path[sizeof(dst_path) - 1] = '\0';

    int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        fprintf(stderr, "[RDB] open tmp failed: %s\n", strerror(errno));
        return;
    }

    rdb_writer_t w;
    memset(&w, 0, sizeof(w));
    if (io_uring_queue_init(RDB_URING_DEPTH, &w.ring, 0) < 0) {
        fprintf(stderr, "[RDB] io_uring_queue_init failed: %s\n", strerror(errno));
        close(fd);
        return;
    }
    w.fd  = fd;
    w.off = RDB_FILE_HDR_LEN;          /* 条目从文件头后写起 */
    w.free_n = RDB_SAVE_WINDOW;
    for (int i = 0; i < RDB_SAVE_WINDOW; i++) w.free_idx[i] = i;
    /* crc 种子 = key_count：加载端校验顺序是 key_count → 条目区 */
    w.crc = rdb_crc32(0, &key_count, sizeof(int));

    /* 一遍全量扫描：无尺寸判断，忠实保存每条 entry */
    kv_iter_t *it = dict_iter_new();
    for (kv_node_t *e; (e = dict_iter_next(it)) != NULL && !w.err; )
        rdb_writer_emit(&w, e);
    dict_iter_free(it);

    rdb_writer_drain(&w);              /* 排空在飞（写失败已置 w.err） */
    int ok = !w.err;

    if (ok) {
        /* 末尾一次性补写 8B 文件头 @offset0：key_count + 最终 crc。
         * 排空后才写 → crc 已收敛；覆盖全部条目（含 crc 校验必须的 key_count 前缀） */
        char hdr[RDB_FILE_HDR_LEN];
        memcpy(hdr, &key_count, sizeof(int));
        memcpy(hdr + RDB_FILE_CRC_OFF, &w.crc, sizeof(uint32_t));
        ok = (rdb_save_flush(fd, &w.ring, hdr, RDB_FILE_HDR_LEN, 0) == 0);
    }
    if (ok)                            /* 一次 fsync 覆盖头部 + 全部数据后才可 rename */
        ok = (rdb_save_fsync(fd, &w.ring) == 0);
    io_uring_queue_exit(&w.ring);
    close(fd);

    if (!ok) {
        /* 写失败绝不能把残缺文件 rename 成 dump.rdb
         * 否则下次启动会拿损坏快照清空整库（fail-closed） */
        unlink(tmp_path);
        fprintf(stderr, "[RDB] save failed\n");
        return;
    }
    rename(tmp_path, dst_path);
    printf("[RDB] saved via io_uring, %zu bytes (%d keys)\n",
           (size_t)w.off, key_count);
}

/*---------------------------------------------------------------------------
 *  ②d 异步保存（fork + 子进程 io_uring 写入）
 *  Redis 风格：同时只运行一个 BGSAVE
 *  子进程回收：全局 SIGCHLD handler 已回收，此处仅探活确认
 *---------------------------------------------------------------------------*/

/**
 * BGSAVE 子进程是否仍在运行（waitpid 探活）
 * 已退出（全局 SIGCHLD handler 已回收 → ECHILD）则顺手清内部状态
 * 供 BGSAVE / SAVE 命令共用守卫：有 BGSAVE 在跑时两者都拒绝，
 * 避免两路同时写同一 dump.rdb.tmp + rename 的竞态。
 * @return 1 在运行，0 无
 */
int rdb_bgsave_running(void) {
    if (g_rdb_child_pid <= 0)
        return 0;
    if (waitpid(g_rdb_child_pid, NULL, WNOHANG) == 0)
        return 1;                 /* 仍在运行 */
    g_rdb_child_pid = 0;          /* 已结束 */
    return 0;
}

/**
 * 异步 RDB 保存（BGSAVE）
 * @return 0 成功（或已触发），-1 已在运行
 */
int rdb_save_async_uring(void) {
    /* BGSAVE 子进程仍在运行则拒绝 */
    if (rdb_bgsave_running())
        return -1;

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
 *  ③ RDB 加载（磁盘启动 / sendfile 对端 / RDMA 流共用）
 *  - 磁盘文件带 crc 槽 [key_count:4][crc:4]：rdb_load_from_buf 先核数量再核 crc
 *    （防篡改/损坏仍被加载），sendfile 对端与启动 mmap 都走它
 *  - RDMA 无盘流 [key_count:4] 无 crc：rdb_load_stream_begin/append_n 由
 *    rdma.c Slave 按 limit 消费，纯流式解析
 *============================================================================*/

/**
 * 校验磁盘 RDB 文件头 crc（防篡改：dump.rdb 被篡改/损坏即拒绝加载）
 * 文件布局：[key_count:4][crc 槽:4][条目...]
 * crc 槽在 key_count 后（offset RDB_FILE_CRC_OFF），覆盖 key_count + 条目区。
 * 仅磁盘文件格式含 crc 槽；RDMA 无盘流不经过本函数。
 * @param stream RDB 文件头起点
 * @param entry_bytes 条目区实际字节数（= 文件大小 - RDB_FILE_HDR_LEN）
 * @return 1 通过，0 不通过
 */
static int rdb_stream_crc_verify(const char *stream, size_t entry_bytes) {
    uint32_t stored = 0;
    memcpy(&stored, stream + RDB_FILE_CRC_OFF, sizeof(stored));

    uint32_t calc = rdb_crc32(0, stream, RDB_FILE_CRC_OFF);             /* key_count */
    calc = rdb_crc32(calc, stream + RDB_FILE_HDR_LEN, entry_bytes);     /* 条目区 */
    return calc == stored;
}

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
 * 只读 buf[0..4) 的 key_count，两种布局共用：
 * RDMA 无盘流 / 磁盘文件（crc 槽在其后，由调用方跳过）。
 * @return key_count（成功），-1（失败）
 */
int rdb_load_stream_begin(const char *buf) {
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
 * @return 成功加载的条目数
 */
int rdb_load_stream_append_n(const char *buf, size_t len, int limit) {
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

        /* 添加条目到字典：走不查重的 dict_add —— 上面 rdb_load_stream_begin
         * 刚清过表，这批数据自身无重复键，逐条查重会把加载拖成 O(N²) */
        kv_node_t *e = dict_add(ks, (uint32_t)klen, p, (uint32_t)dlen);
        p += dlen;

        /* 读取过期时间 */
        if (!(p = mread(p, end, &expire, sizeof(long long)))) break;
        if (e && expire) entry_set_expire(e, expire);
        loaded++;
    }

    return loaded;
}

/**
 * 从内存缓冲区加载磁盘 RDB 文件（dump.rdb 启动加载 / sendfile 对端加载）
 * 文件格式带 crc 槽（RDB_FILE_HDR_LEN=8），crc 校验失败即拒绝加载
 * @return 加载的条目数（成功），-1（数据损坏/篡改）
 */
int rdb_load_from_buf(const char *buf, size_t len) {
    if (len < RDB_FILE_HDR_LEN) return -1;  /* 至少要有文件头：key_count + crc 槽 */

    int expected = rdb_load_stream_begin(buf);
    if (expected < 0) return -1;

    /* append_n 的返回只增不减，不会为负 */
    int loaded = rdb_load_stream_append_n(buf + RDB_FILE_HDR_LEN,
                                          len - RDB_FILE_HDR_LEN, -1);

    /* 核心校验 1：加载数量必须等于头部声明的数量 */
    if (loaded != expected) {
        fprintf(stderr, "[RDB] corruption: expected %d keys, got %d\n",
                expected, loaded);
        dict_clear();
        return -1;
    }

    /* 核心校验 2：crc 覆盖 key_count + 条目区，篡改/损坏即拒绝加载 */
    if (!rdb_stream_crc_verify(buf, len - RDB_FILE_HDR_LEN)) {
        fprintf(stderr, "[RDB] crc mismatch, refuse to load (tampered/corrupt)\n");
        dict_clear();
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

/*=============================================================================
 *  ④ RDMA 全量同步序列化（Master 端无盘流；文件尾部辅助段）
 *
 *  RDMA 全量传输对标 Redis 无盘同步：RDMA 只是本项目的一种传输方式，本段
 *  即为该传输服务——Master（rdma.c）把整份快照按 [key_count:4][条目...]
 *  一次性序列化进它那块暂存内存，再由 rdma_transport.c 一个 WR 直传；
 *  Slave（rdma.c）用 ③ 的 rdb_load_stream_* 消费同一流。
 *
 *  磁盘落盘（②）已改逐条目 io_uring 直写，其 crc 由 rdb_writer_t 自己累计。
 *  本段与 ② 是两个独立编码器：当前字节序完全一致，改动须两侧镜像
 *  （详见 persistence.h 的 RDB 序列化文档）。
 *============================================================================*/

/**
 * 单个条目的序列化大小
 * 格式：[klen:4][key:klen][dlen:4][data:dlen][expire:8]
 */
static inline size_t entry_serialized_size(kv_node_t *e) {
    return sizeof(int) + (size_t)entry_key_len(e)
         + sizeof(int) + (size_t)entry_payload_len(e)
         + sizeof(long long);
}

/**
 * 序列化单个条目到缓冲区
 * 写入长度由调用方用 entry_serialized_size 预算（放不下要整份作废），
 * 故本函数不再重复算一遍。
 * @param dst 目标缓冲区
 * @param e 要序列化的 entry
 */
static void serialize_entry_to_ptr(char *dst, kv_node_t *e) {
    const char *key  = entry_key(e);
    const char *data = entry_payload(e);
    int  klen = (int)entry_key_len(e);
    int  dlen = (int)entry_payload_len(e);
    long long expire = entry_expire(e);

    char *p = dst;
    memcpy(p, &klen,   sizeof(int));   p += sizeof(int);
    memcpy(p, key,     (size_t)klen);  p += klen;
    memcpy(p, &dlen,   sizeof(int));   p += sizeof(int);
    memcpy(p, data,    (size_t)dlen);  p += dlen;
    memcpy(p, &expire, sizeof(long long));
}

/**
 * 把整份快照序列化进 dst（一次性装满）
 *
 * 流头 [key_count:4] 无条件先写：空库也要发出 key_count=0，否则对端一个字节
 * 都收不到，等不到尾部 IMM 而卡死。无 magic/version，解析端按 4 字节读回。
 *
 * 条目先消费再判断装不装得下 —— 一次性装满的模型下，装不下就整份作废，
 * 「先看一眼、放不下就留给下一批」的语义不存在，所以迭代器不必带前瞻。
 */
size_t rdb_serialize_all(char *dst, size_t cap) {
    if (cap < sizeof(int)) return 0;

    int key_count = (int)dict_count();   /* O(1)：一遍扫描前先预知条数 */
    memcpy(dst, &key_count, sizeof(int));

    size_t off = sizeof(int);
    kv_iter_t *it = dict_iter_new();
    if (!it) return 0;

    kv_node_t *e;
    while ((e = dict_iter_next(it)) != NULL) {
        size_t sz = entry_serialized_size(e);
        if (off + sz > cap) { off = 0; break; }   /* 装不下 → 整份作废 */
        serialize_entry_to_ptr(dst + off, e);
        off += sz;
    }
    dict_iter_free(it);
    return off;
}
