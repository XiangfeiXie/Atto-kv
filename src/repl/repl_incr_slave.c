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
 *============================================================================*/

#include "repl_incr.h"
#include "repl_fullsync.h"
#include "kvstore.h"
#include "protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <arpa/inet.h>

/*=============================================================================
 *  模块状态
 *============================================================================*/

static int    g_repl_fd       = -1;   /* 引擎内 master 链路 fd */
static int    g_link_state    = 0;    /* 0=握手阶段，+CONTINUE 后 1=流式 */
static size_t g_slave_offset  = 0;    /* 已消费到的 master 绝对 offset（命令边界） */
static char   g_master_ip[64] = {0};  /* Master IP 地址 */
static int    g_master_port   = 0;    /* Master 端口 */

static int    g_fullsync_pending  = 0;/* 待做全量（由 +FULLRESYNC 握手置位） */
static int    g_fullsync_retries  = 0;/* 全量同步重试次数 */
static int    g_give_up           = 0;/* 是否放弃同步 */
static int    g_incr_enabled      = 1;/* incr_sync off → 只做一次全量 */
static long long g_next_retry     = 0;/* 下次重试时间戳 */

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
 * @return 0 成功
 * 
 * 设计要点：
 * - 支持 RESP 数组和内联命令两种格式
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
        
        /*-----------------------------------------------------------------
         *  内联命令 fallback（与 kvs_drain_resp_commands_off 一致）
         *  backlog 原样保存客户端字节，RESP 数组和 "SET k v\r\n" 都可能有
         *-----------------------------------------------------------------*/
        if (cmd_len == -1) {
            const char *p = rb->buf + off;
            const char *end = p + remaining;
            const char *ln = memchr(p, '\n', (size_t)(end - p));
            if (!ln || p == ln) { break; }   /* 行不完整，等更多数据 */

            char line[256];
            int linelen = (int)(ln - p);
            if (linelen > 255) linelen = 255;
            if (linelen > 0 && p[linelen - 1] == '\r') linelen--;
            memcpy(line, p, (size_t)linelen);
            line[linelen] = '\0';

            argc = 0;
            char *saveptr = NULL;
            char *tok = strtok_r(line, " ", &saveptr);
            while (tok && argc < KVS_MAX_TOKENS) {
                tokens[argc].ptr = tok;
                tokens[argc].len = (int)strlen(tok);
                argc++;
                tok = strtok_r(NULL, " ", &saveptr);
            }
            if (argc == 0) {
                /* 空行（redis-cli --pipe 的同步标记）：消费但不推进 offset */
                int consumed = (int)(ln + 1 - p);
                off       += consumed;
                remaining -= consumed;
                continue;
            }
            cmd_len = (int)(ln + 1 - p);   /* 含换行，offset 到命令边界 */
        }

        if (cmd_len <= 0)
            break;   /* 命令不完整，等更多数据 */

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
 *  Master 链路 Handler（引擎调用）
 *  处理 PSYNC 握手 → 流式回放
 *============================================================================*/

/**
 * Slave 端 Master 链路 handler
 * 
 * 功能：
 * 1. 握手阶段：解析 +CONTINUE / +FULLRESYNC
 * 2. 流式阶段：回放增量命令
 * 
 * 状态机：
 * - g_link_state == 0：等待握手响应
 * - g_link_state == 1：流式回放
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

    if (g_link_state == 0) {
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
            g_link_state = 1;
            g_fullsync_retries = 0;
            g_next_retry = 0;
            printf("[INCR-SLAVE] +CONTINUE — 流式接收 (offset=%zu)\n",
                   g_slave_offset);
            return slave_replay_stream(fd, rb, r_off);  /* 行后可能已夹带 backfill */
        }

        /*-----------------------------------------------------------------
         *  +FULLRESYNC：offset 过期，需要全量同步
         *  续传 offset 以数据面快照 token（g_repl_snapshot_offset）为准，
         *  全量完成后由 repl_incr_slave_reset_offset 对齐
         *-----------------------------------------------------------------*/
        if (linelen >= 12 && strncmp(b, "+FULLRESYNC", 11) == 0) {
            printf("[INCR-SLAVE] +FULLRESYNC — 转全量同步\n");
            g_fullsync_pending = 1;
            g_repl_fd    = -1;
            g_next_retry = -1;    /* 立即进入全量交接 */
            if (want_close) *want_close = 1;
            return 0;
        }

        /* 意外的响应 */
        fprintf(stderr, "[INCR-SLAVE] 意外 PSYNC 响应: %.*s\n", linelen, b);
        g_repl_fd    = -1;
        g_next_retry = get_time_now() + 1000;
        if (want_close) *want_close = 1;
        return 0;
    }

    /* 流式模式：回放增量命令 */
    return slave_replay_stream(fd, rb, r_off);
}

/*=============================================================================
 *  连接工具
 *  非阻塞 TCP 连接（带超时）
 *============================================================================*/

/**
 * 单次非阻塞 TCP 连接
 * @param ip Master IP
 * @param port Master 端口
 * @param timeout_ms 超时时间（毫秒）
 * @return socket fd，失败返回 -1
 */
static int slave_connect_once(const char *ip, int port, int timeout_ms) {
    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", port);

    struct addrinfo hints = {0}, *ai;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(ip, port_str, &hints, &ai) != 0)
        return -1;

    int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) { freeaddrinfo(ai); return -1; }

    /* 设置非阻塞 */
    int fl = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, fl | O_NONBLOCK);

    int rc = connect(fd, ai->ai_addr, ai->ai_addrlen);
    freeaddrinfo(ai);

    /* 非阻塞 connect 需要 poll 等待完成 */
    if (rc != 0) {
        if (errno != EINPROGRESS) {
            close(fd);
            return -1;
        }
        struct pollfd pfd = { .fd = fd, .events = POLLOUT };
        if (poll(&pfd, 1, timeout_ms) <= 0) {
            close(fd);
            return -1;
        }
        int err = 0;
        socklen_t elen = sizeof(err);
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &elen) < 0 || err != 0) {
            close(fd);
            return -1;
        }
    }

    /* 恢复阻塞模式（交给引擎后由引擎设置非阻塞） */
    fcntl(fd, F_SETFL, fl);

    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    return fd;
}

/*=============================================================================
 *  连接收养
 *  将已连接的 fd 交给引擎，并发送 PSYNC 命令
 *============================================================================*/

/**
 * 收养已连接的 fd 进引擎，发送 PSYNC 命令
 * 握手由 repl_incr_slave_master_link_handler 推进
 */
static void slave_adopt(int fd) {
    g_repl_fd    = fd;
    g_link_state = 0;
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
 *  断线重连 / 首次全量 / FULLRESYNC 交接
 *  由引擎事件循环周期性调用
 *============================================================================*/

/**
 * Slave 端 Tick（引擎每轮事件循环调用）
 * 
 * 功能：
 * 1. 检查是否需要全量同步（首次启动或 +FULLRESYNC）
 * 2. 执行全量同步（transport 层）
 * 3. 连接 Master 并发送 PSYNC
 * 4. 断线重连
 */
void repl_incr_slave_tick(void) {
    if (g_repl_fd >= 0 || g_master_ip[0] == '\0' || g_give_up) return;

    long long now = get_time_now();
    if (now < g_next_retry) return;

    /*-------------------------------------------------------------------------
     *  全量同步：由 +FULLRESYNC 握手置位触发（Redis 模型）
     *  incr_sync off 时 master 无 backlog → 一律 +FULLRESYNC → 一次全量后结束
     *-------------------------------------------------------------------------*/
    if (g_fullsync_pending) {
        g_fullsync_pending = 0;
        if (++g_fullsync_retries > REPL_FULLSYNC_MAX_RETRY) {
            fprintf(stderr, "[INCR-SLAVE] 全量同步重试超限，放弃同步\n");
            g_give_up = 1;
            return;
        }
        fprintf(stderr, "[INCR-SLAVE] 全量同步 (%d/%d)...\n",
                g_fullsync_retries, REPL_FULLSYNC_MAX_RETRY);

        /* 执行全量同步（数据面端口，通过 transport 层） */
        if (repl_fullsync_slave_start() == 0) {
            repl_incr_slave_reset_offset();  /* 对齐到快照 offset */
            /* 仅全量模式（incr_sync off）：一次全量后结束，不建增量链路 */
            if (!g_incr_enabled) {
                g_give_up = 1;
                return;
            }
        } else {
            g_fullsync_pending = 1;
            g_next_retry = now + 1000;
            return;
        }
    }

    /*-------------------------------------------------------------------------
     *  连接 Master 并发送 PSYNC
     *  首次发送 PSYNC 0；全量完成后以快照 offset 续连
     *-------------------------------------------------------------------------*/
    int fd = slave_connect_once(g_master_ip, g_master_port, 200);
    if (fd < 0) {
        g_next_retry = now + 1000;
        return;
    }
    printf("[INCR-SLAVE] connected %s:%d, PSYNC %zu\n",
           g_master_ip, g_master_port, g_slave_offset);
    slave_adopt(fd);
}

/*=============================================================================
 *  连接关闭回调
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
    g_link_state = 0;
    /* offset 保留（只到命令边界） */
    if (g_next_retry == 0)
        g_next_retry = get_time_now() + 1000;
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
    g_incr_enabled       = kvs_config.incr_sync;  /* off → 一次全量后结束 */
    g_give_up            = 0;
    g_fullsync_retries   = 0;
    g_fullsync_pending   = 0;   /* 全量一律由 +FULLRESYNC 握手触发（Redis 模型） */
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

/**
 * 获取 Slave 当前偏移量
 * 用途：INFO replication 命令展示
 */
size_t repl_incr_slave_offset(void) {
    return g_slave_offset;
}