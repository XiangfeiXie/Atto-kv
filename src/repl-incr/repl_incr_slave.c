/* repl_incr_slave.c — Slave 侧增量同步
 *
 *  - TCP 连 master 的 incr_repl_port
 *  - 发 PSYNC <offset>，处理 +CONTINUE / +FULLRESYNC
 *  - 命令流（引擎驱动）经 kvs_dispatch 回放
 *  - 断线后由 repl_incr_slave_tick 周期重连，offset 追踪到命令边界续传
 */

#include "repl_incr.h"
#include "kvstore.h"
#include "protocol.h"
#include "kvstore_internal.h"
#include "rdma_repl.h"
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

/* ── Module state ────────────────────────────────────────────────────── */

static int    g_repl_fd   = -1;   /* 与 master 的持久 TCP 连接 */
static int    g_streaming = 0;    /* 1 = 已收 +CONTINUE，正在收命令流 */
static DynBuf g_slave_rb  = {0};  /* 命令流读缓冲 */

static size_t     g_slave_offset = 0;   /* 已消费到的 master 绝对 offset（命令边界） */
static char       g_master_ip[64] = {0}; /* 重连用 */
static int        g_master_port   = 0;
static int        g_give_up       = 0;   /* offset 过期且无全量同步 → 放弃重连 */
static long long  g_next_retry    = 0;   /* 下次重连时间（ms，get_time_now） */

/* ── 回放缓冲里的完整 RESP 命令 ────────────────────────────────────────
 * 消费掉的字节（只到完整命令边界）计入 g_slave_offset，供断线重连续传。 */

static void slave_replay_pending(void) {
    if (g_slave_rb.len <= 0) return;

    DynBuf dummy_wb;
    dynbuf_init(&dummy_wb, 256);

    int off = 0;
    while (g_slave_rb.len - off > 0) {
        kvs_token_t tokens[KVS_MAX_TOKENS];
        int argc = 0;
        int cmd_len = resp_parse_slices(g_slave_rb.buf + off,
                                         g_slave_rb.len - off,
                                         tokens, KVS_MAX_TOKENS, &argc);
        if (cmd_len <= 0)
            break;  /* 命令不完整，等更多数据 */

        dummy_wb.len = 0;
        g_in_reload = 1;
        kvs_dispatch(tokens, argc, g_slave_rb.buf + off, cmd_len, &dummy_wb);
        g_in_reload = 0;

        off += cmd_len;
    }

    dynbuf_free(&dummy_wb);

    if (off > 0) {
        g_slave_offset += (size_t)off;
        int remaining = g_slave_rb.len - off;
        if (remaining > 0)
            memmove(g_slave_rb.buf, g_slave_rb.buf + off, (size_t)remaining);
        g_slave_rb.len = remaining;
    }
}

/* ── 单次非阻塞 connect（timeout_ms 内未完成则失败）────────────────────
 * 返回一个阻塞 socket：握手用阻塞 recv，注册进引擎后由引擎置非阻塞。 */

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

    /* 临时置非阻塞，用 poll 给 connect 加超时；完成后恢复阻塞。 */
    int fl = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, fl | O_NONBLOCK);

    int rc = connect(fd, ai->ai_addr, ai->ai_addrlen);
    freeaddrinfo(ai);

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

    fcntl(fd, F_SETFL, fl);   /* 恢复阻塞 */

    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    return fd;
}

/* ── 启动期连接：最多重试 10 次，每次间隔 1s（阻塞）────────────────── */

static int slave_connect(const char *ip, int port) {
    for (int i = 0; i < 10; i++) {
        int fd = slave_connect_once(ip, port, 1000);
        if (fd >= 0)
            return fd;
        fprintf(stderr, "[INCR-SLAVE] connect retry %d/10...\n", i + 1);
        sleep(1);
    }
    return -1;
}

/* ── PSYNC 握手：发 g_slave_offset、读响应。失败时 fd 由本函数关闭 ────
 * 返回 REPL_INCR_CONTINUE / REPL_INCR_ERROR / REPL_INCR_FULLRESYNC。 */

static int slave_handshake(int fd) {
    char cmd[128], offbuf[32];
    int offlen = snprintf(offbuf, sizeof(offbuf), "%zu", g_slave_offset);
    int clen = snprintf(cmd, sizeof(cmd),
                        "*2\r\n$5\r\nPSYNC\r\n$%d\r\n%s\r\n", offlen, offbuf);

    if (send(fd, cmd, (size_t)clen, MSG_NOSIGNAL) != clen) {
        perror("[INCR-SLAVE] send PSYNC");
        close(fd);
        return REPL_INCR_ERROR;
    }

    char resp[128] = {0};
    ssize_t n = recv(fd, resp, sizeof(resp) - 1, 0);
    if (n <= 0) {
        perror("[INCR-SLAVE] recv PSYNC response");
        close(fd);
        return REPL_INCR_ERROR;
    }

    if (strncmp(resp, "+CONTINUE", 9) == 0) {
        printf("[INCR-SLAVE] +CONTINUE — 进入流式接收 (offset=%zu)\n",
               g_slave_offset);
        /* "+CONTINUE\r\n" 之后可能已夹带 backfill，先入缓冲 */
        if (n > 11) {
            if (!g_slave_rb.buf)
                dynbuf_init(&g_slave_rb, 65536);
            dynbuf_reserve(&g_slave_rb, g_slave_rb.len + (int)(n - 11));
            memcpy(g_slave_rb.buf + g_slave_rb.len, resp + 11, (size_t)(n - 11));
            g_slave_rb.len += (int)(n - 11);
        }
        g_repl_fd   = fd;
        g_streaming = 1;
        slave_replay_pending();
        return REPL_INCR_CONTINUE;
    }

    if (strncmp(resp, "+FULLRESYNC", 11) == 0) {
        printf("[INCR-SLAVE] +FULLRESYNC — 快照 offset 已过期，需全量同步\n");
        close(fd);
        return REPL_INCR_FULLRESYNC;
    }

    fprintf(stderr, "[INCR-SLAVE] unexpected PSYNC response: %.*s\n", (int)n, resp);
    close(fd);
    return REPL_INCR_ERROR;
}

/* ── Setup（启动期一次性）───────────────────────────────────────────── */

int repl_incr_slave_start(const char *master_ip, int master_port) {
    snprintf(g_master_ip, sizeof(g_master_ip), "%s", master_ip);
    g_master_port = master_port;

    int fd = slave_connect(master_ip, master_port);
    if (fd < 0)
        return REPL_INCR_ERROR;

    printf("[INCR-SLAVE] connected to %s:%d\n", master_ip, master_port);
    return slave_handshake(fd);
}

/* ── 全量同步后对齐续传 offset ──────────────────────────────────────── */

void repl_incr_slave_reset_offset(void) {
    g_slave_offset = g_repl_snapshot_offset;
}

/* ── 断线重连（引擎主循环周期调用，带退避）─────────────────────────── */

void repl_incr_slave_tick(void) {
    if (g_repl_fd >= 0 || g_master_ip[0] == '\0' || g_give_up)
        return;

    long long now = get_time_now();
    if (now < g_next_retry)
        return;

    int fd = slave_connect_once(g_master_ip, g_master_port, 200);
    if (fd < 0) {
        g_next_retry = now + 1000;
        return;
    }

    int r = slave_handshake(fd);
    if (r == REPL_INCR_CONTINUE) {
        g_next_retry = 0;
        if (repl_engine_register)
            repl_engine_register(g_repl_fd, CONN_KIND_REPL_MASTER);
    } else if (r == REPL_INCR_FULLRESYNC) {
        if (kvs_config.rdma_sync_enabled) {
            fprintf(stderr, "[INCR-SLAVE] 重连 offset 过期，重做全量同步\n");
            rdma_repl_slave_start();        /* 全量同步（阻塞）→ 更新快照 offset */
            repl_incr_slave_reset_offset();
        } else {
            g_give_up = 1;
            fprintf(stderr, "[INCR-SLAVE] 重连 offset 过期且无全量同步，放弃重连\n");
        }
        g_next_retry = now + 1000;
    } else {
        g_next_retry = now + 1000;
    }
}

/* ── 读回调：收命令流并回放 ─────────────────────────────────────────── */

int repl_incr_slave_read(int fd) {
    if (fd < 0 || fd != g_repl_fd || !g_streaming) return -1;

    if (!g_slave_rb.buf)
        dynbuf_init(&g_slave_rb, 65536);

    if (g_slave_rb.len + 4096 > g_slave_rb.cap)
        dynbuf_reserve(&g_slave_rb, g_slave_rb.cap * 2);

    ssize_t n = recv(fd,
                     g_slave_rb.buf + g_slave_rb.len,
                     (size_t)(g_slave_rb.cap - g_slave_rb.len),
                     MSG_DONTWAIT);
    if (n > 0) {
        g_slave_rb.len += (int)n;
    } else if (n == 0) {
        printf("[INCR-SLAVE] master disconnected\n");
        repl_incr_slave_close(fd);
        return 0;
    } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
        printf("[INCR-SLAVE] recv error: %s\n", strerror(errno));
        repl_incr_slave_close(fd);
        return 0;
    }

    slave_replay_pending();
    return 0;
}

void repl_incr_slave_close(int fd) {
    if (fd != g_repl_fd) return;

    if (fd >= 0) {
        if (repl_engine_close) repl_engine_close(fd);
        else close(fd);
    }
    dynbuf_free(&g_slave_rb);
    g_repl_fd   = -1;
    g_streaming = 0;
    /* g_slave_offset 保留（只推进到完整命令边界），等 tick 重连续传 */
    g_next_retry = get_time_now() + 1000;
}

int repl_incr_slave_link_fd(void) {
    return g_repl_fd;
}
