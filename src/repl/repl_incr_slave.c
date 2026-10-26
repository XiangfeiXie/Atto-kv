/*=============================================================================
 *  repl_incr_slave.c — Slave 端增量同步
 *
 *  职责：
 *  1. 管理 Slave 与 Master 的增量同步连接
 *  2. 处理 PSYNC 握手（+CONTINUE / +FULLRESYNC）
 *  3. 流式回放 Master 发送的增量命令
 *  4. 断线重连和全量同步交接
 *
 *  流程（Redis 模型，PSYNC 驱动）：
 *  1. 启动：记录 Master 地址，tick 统一推进
 *  2. 连接 Master：发送 PSYNC <offset>（首次 offset=0）
 *  3. 握手：+CONTINUE → 流式回放；+FULLRESYNC → 走数据面全量
 *  4. 全量加载后复位 offset，重新 PSYNC 续连
 *  5. 断线：offset 保留，tick 周期重连
 *
 *  控制面状态（链路存在性 + 标志位，无独立枚举）：
 *
 *    链路存在性由 g_repl_fd 表达，子状态由标志位区分：
 *      fd>=0 + !handshake_done  → 待握手（等 +CONTINUE / +FULLRESYNC）
 *      fd>=0 +  handshake_done  → 流式回放中
 *      fd<0  +  !sync_done      → 无链路，tick 连接/重连
 *      g_sync_done=1            → 终结（仅全量完成 / 全量后 rejudge 失败）
 *
 *    转移：
 *      无链路       --tick: 连接成功并 adopt--> 待握手
 *      无链路       --tick: 连接失败--> 无链路（1s 后重试）
 *      待握手       --+CONTINUE--> 流式回放（fullsync_done 清零）
 *      待握手       --+FULLRESYNC（fullsync_done=0）--> 摘链 + 内联数据面全量
 *                     ├─ 全量成功，incr on --> 无链路（fullsync_done=1，tick 重连 rejudge）
 *                     ├─ 全量成功，incr off--> g_sync_done 终结（仅全量模式，不同步增量）
 *                     └─ 全量失败--> 无链路（fullsync_done 保持 0，1s 后重试）
 *      待握手       --+FULLRESYNC（fullsync_done=1，全量后 rejudge 仍失败）
 *                     --> g_sync_done 终结（增量同步失败，断开）
 *      待握手       --意外响应--> 无链路（1s 后重试）
 *      流式回放     --on_close: 断开--> 无链路（offset 保留，1s 后重试）
 *
 *    g_fullsync_done 守卫语义：同一同步周期内最多一次全量；达成 +CONTINUE
 *    （增量基线建立）即清零，允许后续断线过久时再次全量（自愈）。
 *
 *    统一重试时间戳（g_next_retry，tick 唯一行动门槛 now < g_next_retry）：
 *      - 0 或过去时间 → 立即可行动（初始 / 全量后 rejudge 续连）
 *      - now + 1000   → 1s 后重试（连接失败 / 全量传输失败 / 断线）
 *    终结由 g_sync_done 标志表达（仅全量模式完成 / 全量后 rejudge 失败，
 *    tick 入口直接返回，不再连接）
 *
 *    触发入口（引擎三个回调，控制面所有跳转都汇聚于此）：
 *      数据到达 → repl_incr_slave_link_rbuf_handler（握手 / 流式回放）
 *      每轮循环 → repl_incr_slave_tick（连接 / 重连）
 *      连接释放 → repl_incr_slave_on_close（断开 → 安排重连）
 *============================================================================*/

#include "repl.h"
#include "kvstore.h"
#include "protocol.h"
#include "rbuf.h"       /* Master 链路读缓冲与普通连接一致：rbuf 节点队列 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

/*=============================================================================
 *  模块状态
 *  链路存在性由 g_repl_fd 表达，子状态由 g_handshake_done / g_fullsync_done /
 *  g_sync_done 区分（见文件头状态机图）；handler / tick / on_close 三个回调共同推进
 *============================================================================*/

static int          g_repl_fd         = -1;  /* >=0 链路 fd；-1 无链路 */
static int          g_handshake_done  = 0;   /* 1=握手响应已收（+CONTINUE→流式） */
static int          g_fullsync_done   = 0;   /* 1=本同步周期已做过全量（守卫：禁止连续两次全量） */
static int          g_sync_done       = 0;   /* 1=终结：仅全量模式完成 / 全量后 rejudge 失败 */
static size_t       g_slave_offset    = 0;   /* 已消费到的 master 绝对 offset（命令边界） */
static char         g_master_ip[64]   = {0}; /* Master IP 地址 */
static int          g_master_port     = 0;   /* Master 端口 */

static int          g_incr_enabled    = 1;   /* incr_sync off → 只做一次全量 */
static long long    g_next_retry      = 0;   /* 下一次允许 tick 行动的时间戳（退避语义见文件头） */
static int          g_fullsync_in_progress = 0; /* 1=全量进行中：tick 禁止并发 PSYNC */

/*=============================================================================
 *  Master 链路 Handler（slave 出站链路，引擎挂载）
 *
 *  Slave 的 Master 链路读缓冲与普通连接一致（rbuf 节点队列，
 *  reactor/proactor 同构）；数据到达按状态分派，状态机见文件头图：
 *    - 握手（fd>=0 + !handshake_done）：在 rbuf 逻辑流上按行
 *      （memchr '\n'）解析 PSYNC 响应第一行 → slave_handshake_rbuf
 *    - 流式回放（handshake_done）：复用协议层 rbuf drain
 *      （kvs_drain_rbuf_commands）整命令消费 → slave_replay_rbuf
 *
 *  拷贝粒度与 AOF / 普通客户端一致（整命令），offset 推进天然只计完整
 *  命令边界（drain 消费字节）；协议错误由 drain 置 want_close → 引擎断链
 *  → on_close 安排重连（全程用 want_close 表达失败，无 -1 返回值遗留）。
 *
 *  M2 清理：原 DynBuf 三件套（slave_replay_stream / slave_handle_handshake /
 *  repl_incr_slave_master_link_handler）已删——全引擎读缓冲均为 rbuf，
 *  状态机只保留一份 rbuf 实现，无形态分叉。
 *============================================================================*/

/** 在 rbuf 逻辑流 [0, len) 中找第一个 '\n' 的下标；找不到返回 0 */
static int rbuf_find_newline(const struct rbuf *rb, uint32_t *idx) {
    uint32_t off = 0, len = rbuf_len(rb);
    while (off < len) {
        const char *base = NULL;
        uint32_t n = rbuf_contig(rb, off, &base);   /* 段内连续可读长度 */
        const char *nl = memchr(base, '\n', n);
        if (nl) { *idx = off + (uint32_t)(nl - base); return 1; }
        off += n;                                    /* 未中：换下一连续段 */
    }
    return 0;
}

/**
 * 流式回放：复用普通命令 drain 消费完整命令
 *
 * g_in_reload=1 罩住 drain → 其 want_raw（!g_in_reload）在回放期不物化、不回传
 * journal 到 kvs_journal_effect——从 master 收到的写不落本机 AOF/backlog；
 * kvs_journal_effect 自身的 g_in_reload 短路是第二道保险。
 * offset 推进 = drain 消费字节（before − after），只到完整命令边界。
 */
static int slave_replay_rbuf(struct rbuf *rb, int *want_close) {
    uint32_t before = rbuf_len(rb);
    DynBuf dummy;
    dynbuf_init(&dummy, 256);

    g_in_reload = 1;
    int rc = kvs_drain_rbuf_commands(-1, rb, &dummy, want_close);
    g_in_reload = 0;
    dynbuf_free(&dummy);

    g_slave_offset += (size_t)(before - rbuf_len(rb));
    return rc < 0 ? -1 : 0;
}

/**
 * 处理 PSYNC 握手响应：按 rbuf 逻辑流找第一行 '\n'，前缀判定只需行首 64B
 * （+FULLRESYNC 行很短，超长行的其余字节无关紧要）。
 */
static int slave_handshake_rbuf(struct rbuf *rb, int *want_close) {
    uint32_t nl_idx;
    if (!rbuf_find_newline(rb, &nl_idx))
        return 0;   /* 第一行不完整，等待 */

    uint32_t linelen = nl_idx + 1;
    char line[64];
    uint32_t got = rbuf_copy(rb, 0, line, sizeof(line));

    /* +CONTINUE：增量续传成功，进入流式（行后可能已夹带 backfill） */
    if (linelen >= 10 && strncmp(line, "+CONTINUE", 9) == 0) {
        rbuf_consume(rb, linelen);
        g_handshake_done = 1;
        g_fullsync_done  = 0;   /* 达成增量基线，清除全量守卫 */
        g_next_retry     = 0;
        printf("[INCR-SLAVE] +CONTINUE — 流式接收 (offset=%zu)\n",
               g_slave_offset);
        if (rbuf_len(rb) > 0)
            return slave_replay_rbuf(rb, want_close);   /* 夹带 backfill：同批回放 */
        return 0;
    }

    /* +FULLRESYNC：offset 过期，走数据面全量（内联阻塞） */
    if (linelen >= 12 && strncmp(line, "+FULLRESYNC", 11) == 0) {
        if (g_fullsync_done) {
            fprintf(stderr,
                    "[INCR-SLAVE] 全量后 offset 仍超出 backlog，增量同步失败\n");
            g_repl_fd   = -1;
            g_sync_done = 1;   /* 终结：tick 入口永久返回 */
            if (want_close) *want_close = 1;
            return 0;
        }

        printf("[INCR-SLAVE] +FULLRESYNC — 执行全量同步\n");
        g_repl_fd = -1;        /* 提前摘链：on_close 将因 fd 不匹配而早退 */
        if (want_close) *want_close = 1;

        /* 全量期间控制面已摘链（g_repl_fd < 0），而 ntyco 的 ticker 是独立
         * 协程：全量客户端在 recv/poll 上让出时它会跑起来，看到“无链路”
         * 就再发 PSYNC，master 随即 kill 掉正在传输的子进程。用标志把 tick
         * 挡在全量之外（reactor/proactor 下无此并发，标志同样无害）。 */
        g_fullsync_in_progress = 1;
        int fs_ok = (repl_fullsync_slave_start() == 0);
        g_fullsync_in_progress = 0;

        if (fs_ok) {
            g_fullsync_done = 1;
            repl_incr_slave_reset_offset(); /* 对齐到快照 offset */
            if (!g_incr_enabled) {
                printf("[INCR-SLAVE] 仅全量模式，同步完成\n");
                g_sync_done = 1;
            } else {
                g_next_retry = 0;           /* tick 立即重连 PSYNC（rejudge） */
            }
        } else {
            g_next_retry = get_time_now() + 1000;   /* 传输失败：1s 后重试 */
        }
        return 0;
    }

    /* 意外的响应 */
    fprintf(stderr, "[INCR-SLAVE] 意外 PSYNC 响应: %.*s\n", (int)got, line);
    g_repl_fd    = -1;   /* 摘链 */
    g_next_retry = get_time_now() + 1000;
    if (want_close) *want_close = 1;
    return 0;
}

/**
 * Slave 端 Master 链路 handler（slave_adopt 注册给引擎）
 * 握手 / 回放按 g_handshake_done 分派；对 reactor/proactor 通用
 */
int repl_incr_slave_link_rbuf_handler(int fd, struct rbuf *rb, DynBuf *wb,
                                      int *want_close) {
    (void)wb;
    if (fd != g_repl_fd) return -1;

    if (!g_handshake_done)
        return slave_handshake_rbuf(rb, want_close);
    return slave_replay_rbuf(rb, want_close);
}

/*=============================================================================
 *  连接工具
 *  非阻塞 TCP 连接（带超时）
 *============================================================================*/

/**
 * 单次 TCP 连接（阻塞式，带超时护栏）
 *
 * SO_SNDTIMEO 在 Linux 上同样约束阻塞 connect（超时返回 EAGAIN），
 * 避免链路收养前 socket 挂在内核 SYN 重试里。
 * 引擎收养后会把 fd 置为非阻塞，届时超时项自动失效。
 *
 * @param ip Master IP
 * @param port Master 端口
 * @return socket fd，失败返回 -1
 */
static int slave_connect_once(const char *ip, int port) {
    struct sockaddr_in addr = { .sin_family = AF_INET,
                                .sin_port   = htons((uint16_t)port) };
    if (!ip || inet_pton(AF_INET, ip, &addr.sin_addr) != 1)
        return -1;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }

    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    return fd;
}

/*=============================================================================
 *  连接收养（无链路 → 待握手）
 *  将已连接的 fd 交给引擎，并发送 PSYNC 命令
 *============================================================================*/

/**
 * 收养已连接的 fd 进引擎，发送 PSYNC 命令
 * 握手由 repl_incr_slave_link_rbuf_handler 推进
 */
static void slave_adopt(int fd) {
    g_repl_fd        = fd;
    g_handshake_done = 0;   /* 新链路重新握手 */
    g_next_retry     = 0;

    /* 注册 handler（引擎接管 fd）。全引擎读缓冲均为 rbuf，只挂 rbuf 版
     * link handler；DynBuf 版 repl_engine_register 已删（M2），两引擎的
     * start() 只赋 repl_engine_register_rbuf。 */
    if (repl_engine_register_rbuf)
        repl_engine_register_rbuf(fd, repl_incr_slave_link_rbuf_handler);

    /* 发送 PSYNC <offset> 命令 */
    if (repl_engine_append) {
        char cmd[128], offbuf[32];
        int offlen = snprintf(offbuf, sizeof(offbuf), "%zu", g_slave_offset);
        int clen = snprintf(cmd, sizeof(cmd),
                            "*2\r\n$5\r\nPSYNC\r\n$%d\r\n%s\r\n",
                            offlen, offbuf);
        repl_engine_append(fd, cmd, clen);
    }
}

/*=============================================================================
 *  事件循环 Tick（引擎每轮调用）
 *  无链路时推进：连接 / 重连。全量在握手内联完成；ntyco 的 ticker 是独立
 *  协程，全量客户端让出时它仍会跑，必须用 g_fullsync_in_progress 挡住并发
 *  PSYNC（否则 master 会 kill 掉正在传输的子进程）。
 *============================================================================*/

/**
 * Slave 端 Tick（引擎每轮事件循环调用）
 *
 * 行动门槛：非终结（!g_sync_done）、非全量进行中、无链路（g_repl_fd < 0）
 * 且过了重试时间戳才推进；链路已建立（g_repl_fd >= 0）时 tick 不做事，等引擎事件。
 */
void repl_incr_slave_tick(void) {
    if (g_sync_done || g_master_ip[0] == '\0') return;  /* 终结 / 未配置 Master */
    if (g_fullsync_in_progress) return;                 /* 全量进行中：禁止并发 PSYNC */
    if (g_repl_fd >= 0) return;                         /* 链路已建立，等引擎事件 */

    long long now = get_time_now();
    if (now < g_next_retry) return;                     /* 退避中 */

    /*-------------------------------------------------------------------------
     *  IDLE：连接 Master 并发送 PSYNC
     *  首次发送 PSYNC 0；全量后以快照 offset 续连（rejudge）
     *  全量在握手内联完成，tick 不感知（见 slave_handshake_rbuf）
     *-------------------------------------------------------------------------*/
    int fd = slave_connect_once(g_master_ip, g_master_port);
    if (fd < 0) {
        g_next_retry = now + 1000;
        return;
    }
    printf("[INCR-SLAVE] connected %s:%d, PSYNC %zu\n",
           g_master_ip, g_master_port, g_slave_offset);
    slave_adopt(fd);
}

/*=============================================================================
 *  连接关闭回调（链路断开 → 无链路，安排重连）
 *============================================================================*/

/**
 * Slave 端连接关闭回调
 * Master 链路断开时安排重连
 *
 * 调用者：引擎的 conn_release（通过 repl_on_close 函数指针）
 */
void repl_incr_slave_on_close(int fd) {
    if (fd != g_repl_fd) return;
    printf("[INCR-SLAVE] master link fd=%d closed\n", fd);
    g_repl_fd         = -1;
    g_handshake_done  = 0;
    g_next_retry      = get_time_now() + 1000;   /* 1s 后重连（offset 保留，只到命令边界） */
}

/*=============================================================================
 *  启动和状态查询
 *============================================================================*/

/**
 * 启动 Slave 增量同步
 *
 * 功能：
 * 1. 记录 Master 地址
 * 2. 检查 incr_sync 配置
 * 3. 后续由 tick 统一推进：首次连 Master 发 PSYNC 0，
 *    全量与否由 +FULLRESYNC/+CONTINUE 握手决定（Redis 模型）
 *
 * 调用时机：kvstore.c main 中
 *
 * @param master_ip Master IP
 * @param master_port Master 端口
 * @return 0 成功
 */
int repl_incr_slave_start(const char *master_ip, int master_port) {
    snprintf(g_master_ip, sizeof(g_master_ip), "%s", master_ip);
    g_master_port        = master_port;
    g_incr_enabled       = kvs_config.incr_sync;  /* off → 一次全量后终结 */
    g_repl_fd            = -1;           /* 无链路 */
    g_handshake_done     = 0;
    g_fullsync_done      = 0;            /* 守卫清零：允许首次全量 */
    g_sync_done          = 0;            /* 终结清零 */
    g_next_retry         = 0;
    return 0;
}

/**
 * 重置 Slave 偏移量
 * 全量同步完成后调用，对齐到快照 offset
 */
void repl_incr_slave_reset_offset(void) {
    g_slave_offset = g_repl_snapshot_offset;
}
