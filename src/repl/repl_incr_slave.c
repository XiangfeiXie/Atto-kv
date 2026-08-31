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
 *      数据到达 → repl_incr_slave_master_link_handler（握手 / 流式回放）
 *      每轮循环 → repl_incr_slave_tick（连接 / 重连）
 *      连接释放 → repl_incr_slave_on_close（断开 → 安排重连）
 *============================================================================*/

#include "repl.h"
#include "kvstore.h"
#include "protocol.h"
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

/*=============================================================================
 *  流式回放
 *  从读缓冲中解析并执行 RESP 命令
 *  只推进到命令边界（offset 只计完整命令）
 *============================================================================*/

/**
 * 回放流式数据
 *
 * 功能：
 * 1. 从读缓冲中解析完整的 RESP 命令
 * 2. 执行命令（写入本地字典）
 * 3. 推进 offset（只计完整命令边界）
 *
 * @param fd 连接 fd
 * @param rb 读缓冲
 * @param r_off 读偏移
 * @return 0 成功，-1 协议错误
 *
 * 设计要点：
 * - master 转发的只会是标准 RESP，不做其他格式兜底
 * - g_in_reload=1 抑制日志和复制传播
 * - offset 只推进到命令边界（断线重续安全）
 */
static int slave_replay_stream(int fd, DynBuf *rb, int *r_off) {
    (void)fd;
    int off = *r_off, remaining = rb->len;

    DynBuf dummy;
    dynbuf_init(&dummy, 256);

    while (remaining > 0) {
        kvs_token_t tokens[KVS_MAX_TOKENS];
        int argc = 0;
        int cmd_len = resp_parse_slices(rb->buf + off, remaining,
                                        tokens, KVS_MAX_TOKENS, &argc);

        if (cmd_len < 0) {
            dynbuf_free(&dummy);
            return -1;          /* 协议错误：交给引擎断线走重连 */
        }
        if (cmd_len == 0)
            break;              /* 命令不完整，等更多数据 */

        /* 执行命令（g_in_reload 抑制日志/复制/AOF） */
        dummy.len = 0;
        g_in_reload = 1;
        kvs_dispatch(tokens, argc, rb->buf + off, cmd_len, &dummy);
        g_in_reload = 0;

        off += cmd_len;
        remaining -= cmd_len;
    }

    dynbuf_free(&dummy);

    /* 只计完整命令边界的字节数 → 断线重连续传对齐 */
    g_slave_offset += (size_t)(off - *r_off);

    /* 缓冲压缩（与 kvs_drain 相同逻辑） */
    if (off > rb->cap / KVS_RB_COMPACT_RATIO && remaining > 0 &&
        remaining < off) {
        memmove(rb->buf, rb->buf + off, (size_t)remaining);
        off = 0;
    }
    *r_off = off;
    rb->len = remaining;
    return 0;
}

/*=============================================================================
 *  握手处理（待握手：fd>=0 + !handshake_done）
 *  解析 PSYNC 响应，决定进入流式 / 全量 / 重连
 *============================================================================*/

/**
 * 处理 PSYNC 握手响应（第一行）
 *
 * @param rb 读缓冲
 * @param r_off 读偏移
 * @param want_close 关闭标志
 * @return 0 成功（或等待更多数据）
 *
 * 转移：
 * - +CONTINUE  → g_handshake_done=1（流式，行后可能夹带 backfill，直接回放）
 * - +FULLRESYNC→ 摘链 + 内联数据面全量；全量后 rejudge 仍 +FULLRESYNC → 终结失败
 * - 意外响应   → 无链路（1s 后重试）
 */
static int slave_handle_handshake(DynBuf *rb, int *r_off, int *want_close) {
    const char *b = rb->buf + *r_off;
    int rem = rb->len;
    const char *nl = memchr(b, '\n', (size_t)rem);
    if (!nl) return 0;   /* 第一行不完整，等待 */

    int linelen = (int)(nl + 1 - b);

    /*-----------------------------------------------------------------
     *  +CONTINUE：增量续传成功，进入流式模式
     *-----------------------------------------------------------------*/
    if (linelen >= 10 && strncmp(b, "+CONTINUE", 9) == 0) {
        *r_off += linelen;
        rb->len -= linelen;   /* 维护不变量：len 为 r_off 起未消费尾部 */
        g_handshake_done = 1;
        g_fullsync_done  = 0;   /* 达成增量基线，清除全量守卫（允许后续再全量） */
        g_next_retry = 0;
        printf("[INCR-SLAVE] +CONTINUE — 流式接收 (offset=%zu)\n",
               g_slave_offset);
        return slave_replay_stream(g_repl_fd, rb, r_off);  /* 行后可能已夹带 backfill */
    }

    /*-----------------------------------------------------------------
     *  +FULLRESYNC：offset 过期，需要全量同步
     *  全量在握手中内联执行（阻塞）；全量成功后以快照 offset 重新
     *  PSYNC rejudge——仍失败则终结（同一周期最多一次全量）
     *-----------------------------------------------------------------*/
    if (linelen >= 12 && strncmp(b, "+FULLRESYNC", 11) == 0) {
        /* 全量后的 rejudge 仍失败：backlog 已回绕，全量也追不上 → 终结 */
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

        /* 内联执行数据面全量（阻塞，等价原 tick 内的阻塞调用）。
         * 传输失败可重试（fullsync_done 保持 0）；成功后重新 PSYNC rejudge。 */
        if (repl_fullsync_slave_start() == 0) {
            g_fullsync_done = 1;            /* 守卫：本同步周期已做全量 */
            repl_incr_slave_reset_offset(); /* 对齐到快照 offset */
            if (!g_incr_enabled) {
                printf("[INCR-SLAVE] 仅全量模式，同步完成\n");
                g_sync_done = 1;            /* 终结：只全量不增量 */
            } else {
                g_next_retry = 0;           /* tick 立即重连 PSYNC（rejudge） */
            }
        } else {
            g_next_retry = get_time_now() + 1000;  /* 传输失败：1s 后重试 */
        }
        return 0;
    }

    /* 意外的响应 */
    fprintf(stderr, "[INCR-SLAVE] 意外 PSYNC 响应: %.*s\n", linelen, b);
    g_repl_fd    = -1;   /* 摘链（两标志位保持 0 → 无链路态） */
    g_next_retry = get_time_now() + 1000;
    if (want_close) *want_close = 1;
    return 0;
}

/*=============================================================================
 *  Master 链路 Handler（引擎调用）
 *  按当前状态分派：握手 or 流式回放
 *============================================================================*/

/**
 * Slave 端 Master 链路 handler
 *
 * 功能：
 * 1. 握手未完成（!g_handshake_done）：解析 +CONTINUE / +FULLRESYNC
 * 2. 握手已完成（g_handshake_done）：回放增量命令
 *
 * @param fd 连接 fd
 * @param rb 读缓冲
 * @param r_off 读偏移
 * @param wb 写缓冲
 * @param want_close 关闭标志
 * @return 处理的命令数
 */
int repl_incr_slave_master_link_handler(int fd, DynBuf *rb, int *r_off,
                                        DynBuf *wb, int *want_close) {
    (void)wb;
    if (fd != g_repl_fd) return -1;

    if (!g_handshake_done)
        return slave_handle_handshake(rb, r_off, want_close);
    return slave_replay_stream(fd, rb, r_off);
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
 * 握手由 repl_incr_slave_master_link_handler 推进
 */
static void slave_adopt(int fd) {
    g_repl_fd        = fd;
    g_handshake_done = 0;   /* 新链路重新握手 */
    g_next_retry     = 0;

    /* 注册 handler（引擎接管 fd） */
    if (repl_engine_register)
        repl_engine_register(fd, repl_incr_slave_master_link_handler);

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
 *  无链路时推进：连接 / 重连（全量在握手内联完成，tick 不感知）
 *============================================================================*/

/**
 * Slave 端 Tick（引擎每轮事件循环调用）
 *
 * 行动门槛：非终结（!g_sync_done）、无链路（g_repl_fd < 0）且过了重试时间戳才推进；
 * 链路已建立（g_repl_fd >= 0）时 tick 不做事，等引擎事件。
 */
void repl_incr_slave_tick(void) {
    if (g_sync_done || g_master_ip[0] == '\0') return;  /* 终结 / 未配置 Master */
    if (g_repl_fd >= 0) return;                         /* 链路已建立，等引擎事件 */

    long long now = get_time_now();
    if (now < g_next_retry) return;                     /* 退避中 */

    /*-------------------------------------------------------------------------
     *  IDLE：连接 Master 并发送 PSYNC
     *  首次发送 PSYNC 0；全量后以快照 offset 续连（rejudge）
     *  全量在握手内联完成，tick 不感知（见 slave_handle_handshake）
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
