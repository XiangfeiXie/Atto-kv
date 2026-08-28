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
 *  控制面状态机（4 态）：
 *
 *    状态：
 *      SLAVE_IDLE      — 无链路，等待 tick 连接/重连
 *      SLAVE_HANDSHAKE — 已连接，等待 +CONTINUE / +FULLRESYNC
 *      SLAVE_STREAMING — 增量流式回放中
 *      SLAVE_FULLSYNC  — +FULLRESYNC 交接：tick 走数据面全量；
 *                        仅全量模式（incr off）完成一次后即终结
 *
 *    转移：
 *      IDLE      --tick: 连接成功并 adopt--> HANDSHAKE
 *      IDLE      --tick: 连接失败--> IDLE（1s 后重试）
 *      HANDSHAKE --+CONTINUE--> STREAMING
 *      HANDSHAKE --+FULLRESYNC--> FULLSYNC（关链路，fd 置 -1）
 *      HANDSHAKE --意外响应--> IDLE（1s 后重试）
 *      STREAMING --on_close: 断开--> IDLE（offset 保留，1s 后重试）
 *      FULLSYNC  --tick: 全量成功，incr on--> IDLE（以快照 offset 重连）
 *      FULLSYNC  --tick: 全量成功，incr off--> FULLSYNC（retry=NEVER 终结）
 *      FULLSYNC  --tick: 全量失败--> FULLSYNC（1s 后重试）
 *
 *    统一重试时间戳（g_next_retry，tick 唯一行动门槛 now < g_next_retry）：
 *      - 0 或过去时间      → 立即可行动（初始 / 增量续连）
 *      - now + 1000        → 1s 后重试（连接失败 / 全量失败 / 断线）
 *      - REPL_RETRY_NEVER  → 永不行动（仅全量模式完成，同步终结）
 *
 *    触发入口（引擎三个回调，控制面所有跳转都汇聚于此）：
 *      数据到达 → repl_incr_slave_master_link_handler（HANDSHAKE 握手 / STREAMING 回放）
 *      每轮循环 → repl_incr_slave_tick（连接 / 重连 / FULLSYNC 数据面交接）
 *      连接释放 → repl_incr_slave_on_close（断开 → 安排重连）
 *============================================================================*/

#include "repl.h"
#include "kvstore.h"
#include "protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>   /* LLONG_MAX：REPL_RETRY_NEVER */
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

/*=============================================================================
 *  模块状态
 *============================================================================*/

/**
 * Slave 控制面状态（见文件头状态机图）
 * 由 handler / tick / on_close 三个引擎回调共同推进
 */
typedef enum {
    SLAVE_IDLE      = 0,   /* 无链路，等待 tick 连接/重连 */
    SLAVE_HANDSHAKE = 1,   /* 已连接，等待 +CONTINUE / +FULLRESYNC */
    SLAVE_STREAMING = 2,   /* 增量流式回放中 */
    SLAVE_FULLSYNC  = 3,   /* +FULLRESYNC 交接：数据面全量；仅全量模式完成即终结 */
} slave_state_t;

/**
 * 重试时间戳"永不"哨兵
 * 统一重试语义见文件头：tick 门槛 now < g_next_retry 在达到此值时永远为真，
 * 即同步终结（仅全量模式完成一次全量后不再有任何动作）
 */
static const long long REPL_RETRY_NEVER = LLONG_MAX;

static int          g_repl_fd       = -1;   /* master 链路 fd（HANDSHAKE/STREAMING 时有效，其余 -1） */
static slave_state_t g_state        = SLAVE_IDLE; /* 控制面当前状态 */
static size_t       g_slave_offset  = 0;    /* 已消费到的 master 绝对 offset（命令边界） */
static char         g_master_ip[64] = {0};  /* Master IP 地址 */
static int          g_master_port   = 0;    /* Master 端口 */

static int          g_incr_enabled  = 1;    /* incr_sync off → 只做一次全量 */
static long long    g_next_retry    = 0;    /* 下一次允许 tick 行动的时间戳（统一语义见文件头） */

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
 *  握手处理（SLAVE_HANDSHAKE 状态）
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
 * - +CONTINUE  → SLAVE_STREAMING（行后可能夹带 backfill，直接回放）
 * - +FULLRESYNC→ SLAVE_FULLSYNC（关链路，fd 置 -1，tick 走数据面全量）
 * - 意外响应   → SLAVE_IDLE（1s 后重试）
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
        g_state = SLAVE_STREAMING;
        g_next_retry = 0;
        printf("[INCR-SLAVE] +CONTINUE — 流式接收 (offset=%zu)\n",
               g_slave_offset);
        return slave_replay_stream(g_repl_fd, rb, r_off);  /* 行后可能已夹带 backfill */
    }

    /*-----------------------------------------------------------------
     *  +FULLRESYNC：offset 过期，需要全量同步
     *  续传 offset 以数据面快照 token（g_repl_snapshot_offset）为准，
     *  全量完成后由 repl_incr_slave_reset_offset 对齐
     *-----------------------------------------------------------------*/
    if (linelen >= 12 && strncmp(b, "+FULLRESYNC", 11) == 0) {
        printf("[INCR-SLAVE] +FULLRESYNC — 转全量同步\n");
        g_state      = SLAVE_FULLSYNC;
        g_repl_fd    = -1;         /* 提前摘链：on_close 将因 fd 不匹配而早退，保留 FULLSYNC */
        g_next_retry = 0;          /* 立即可行动：tick 立刻走数据面全量 */
        if (want_close) *want_close = 1;
        return 0;
    }

    /* 意外的响应 */
    fprintf(stderr, "[INCR-SLAVE] 意外 PSYNC 响应: %.*s\n", linelen, b);
    g_state      = SLAVE_IDLE;
    g_repl_fd    = -1;
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
 * 1. 握手阶段（SLAVE_HANDSHAKE）：解析 +CONTINUE / +FULLRESYNC
 * 2. 流式阶段（SLAVE_STREAMING）：回放增量命令
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

    switch (g_state) {
    case SLAVE_HANDSHAKE:
        return slave_handle_handshake(rb, r_off, want_close);
    case SLAVE_STREAMING:
        return slave_replay_stream(fd, rb, r_off);
    default:
        return 0;   /* IDLE/FULLSYNC 态 fd 已置 -1，数据不会到达 */
    }
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
 *  连接收养（IDLE → HANDSHAKE）
 *  将已连接的 fd 交给引擎，并发送 PSYNC 命令
 *============================================================================*/

/**
 * 收养已连接的 fd 进引擎，发送 PSYNC 命令
 * 握手由 repl_incr_slave_master_link_handler 推进
 */
static void slave_adopt(int fd) {
    g_repl_fd    = fd;
    g_state      = SLAVE_HANDSHAKE;
    g_next_retry = 0;

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
 *  按状态推进：FULLSYNC 数据面交接 / IDLE 连接与重连
 *============================================================================*/

/**
 * Slave 端 Tick（引擎每轮事件循环调用）
 *
 * 行动门槛：只有无链路（IDLE/FULLSYNC）且过了重试时间戳才推进；
 * 链路已建立（HANDSHAKE/STREAMING）时 tick 不做事，等引擎事件。
 */
void repl_incr_slave_tick(void) {
    if (g_master_ip[0] == '\0') return;   /* 未配置 Master */

    switch (g_state) {
    case SLAVE_HANDSHAKE:
    case SLAVE_STREAMING:
        return;                          /* 链路已建立，等引擎事件 */
    case SLAVE_IDLE:
    case SLAVE_FULLSYNC:
        break;                           /* 无链路：往下推进 */
    }

    long long now = get_time_now();
    if (now < g_next_retry) return;      /* 退避中 / 终结（REPL_RETRY_NEVER） */

    /*-------------------------------------------------------------------------
     *  FULLSYNC 交接：由 +FULLRESYNC 握手置位，tick 走数据面全量
     *  incr_sync off 时 master 无 backlog → 一律 +FULLRESYNC → 一次全量后终结
     *-------------------------------------------------------------------------*/
    if (g_state == SLAVE_FULLSYNC) {
        fprintf(stderr, "[INCR-SLAVE] 全量同步...\n");

        /* 数据面端口接收并加载快照（阻塞式，见 repl_fullsync.c） */
        if (repl_fullsync_slave_start() == 0) {
            repl_incr_slave_reset_offset();  /* 对齐到快照 offset */
            /* 仅全量模式（incr off）：一次全量后终结，不建增量链路 */
            if (!g_incr_enabled) {
                printf("[INCR-SLAVE] 仅全量模式，同步完成\n");
                g_next_retry = REPL_RETRY_NEVER;  /* 终结：tick 永久退避 */
                return;
            }
            /* 增量模式：以快照 offset 重新 PSYNC 续连 */
            g_state = SLAVE_IDLE;
            g_next_retry = 0;
        } else {
            g_next_retry = now + 1000;   /* 失败：保留 FULLSYNC，1s 后重试 */
            return;
        }
    }

    /*-------------------------------------------------------------------------
     *  IDLE：连接 Master 并发送 PSYNC
     *  首次发送 PSYNC 0；全量完成后以快照 offset 续连
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
 *  连接关闭回调（STREAMING/HANDSHAKE → IDLE）
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
    g_repl_fd    = -1;
    g_state      = SLAVE_IDLE;
    g_next_retry = get_time_now() + 1000;   /* 1s 后重连（offset 保留，只到命令边界） */
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
    g_state              = SLAVE_IDLE;   /* 全量一律由 +FULLRESYNC 握手触发（Redis 模型） */
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
