/* repl_incr_master.c — Master 侧增量同步
 *
 *  建独立 TCP 监听（默认 2001），接受从库连接，处理 PSYNC 握手，
 *  向 streaming 从库推送 backlog + 实时命令流。
 *
 *  fd 归属: 监听 fd 与从库 fd 注册进引擎事件循环，引擎就绪时回调下面
 *  的 accept/read/write。本模块除 *_start 的一次性 setup 外不自轮询。
 *  设计上只有一台从机。 */

#define _GNU_SOURCE    /* accept4 */

#include "repl_incr.h"
#include "protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>

/* ── Module state ────────────────────────────────────────────────────── */

static int          g_listen_fd = -1;
static repl_slave_t g_slave;      /* 唯一从机 */

/* ── Helpers ─────────────────────────────────────────────────────────── */

static void slave_close(void) {
    int fd = g_slave.fd;
    dynbuf_free(&g_slave.write_buf);
    memset(&g_slave, 0, sizeof(g_slave));
    g_slave.fd = -1;

    if (fd >= 0) {
        if (repl_engine_close) repl_engine_close(fd);
        else close(fd);
    }
}

/* ── 向 streaming 从库推送写命令 ─────────────────────────────────────── */

void repl_incr_propagate(const char *resp_data, int len) {
    if (!g_slave.active || g_slave.state != REPL_STATE_STREAMING)
        return;

    if (g_slave.write_buf.len + len > g_slave.write_buf.cap)
        dynbuf_reserve(&g_slave.write_buf, g_slave.write_buf.len + len + 4096);
    memcpy(g_slave.write_buf.buf + g_slave.write_buf.len, resp_data, (size_t)len);
    g_slave.write_buf.len += len;

    /* 唤醒引擎，冲刷待发字节 */
    if (repl_engine_writable)
        repl_engine_writable(g_slave.fd, 1);
}

/* ── 冲刷从库 write_buf（非阻塞）────────────────────────────────────── */

int repl_incr_master_write(int fd) {
    repl_slave_t *s = &g_slave;
    if (!s->active || s->fd != fd) return -1;
    if (s->write_buf.len <= 0) return 0;

    ssize_t n = send(fd, s->write_buf.buf,
                     (size_t)s->write_buf.len, MSG_NOSIGNAL | MSG_DONTWAIT);
    if (n > 0) {
        size_t unsent = (size_t)s->write_buf.len - (size_t)n;
        if (unsent > 0)
            memmove(s->write_buf.buf, s->write_buf.buf + n, unsent);
        s->write_buf.len = (int)unsent;
    } else if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        printf("[INCR-MASTER] slave fd=%d disconnected\n", fd);
        slave_close();
        return 0;
    }

    if (repl_engine_writable)
        repl_engine_writable(fd, s->write_buf.len > 0);
    return 0;
}

/* ── 从指定 offset 把 backlog 塞进从库 write_buf ────────────────────────
 * 返回下一个续传 offset；追平（或 offset 无效）时返回当前 head。 */

static size_t slave_backfill(size_t offset) {
    char buf[65536];
    size_t next_off;

    int n = repl_backlog_fetch(offset, buf, (int)sizeof(buf), &next_off);
    if (n <= 0)
        return repl_backlog_offset();  /* 追平或无效 offset */

    if (g_slave.write_buf.len + n > g_slave.write_buf.cap)
        dynbuf_reserve(&g_slave.write_buf, g_slave.write_buf.len + n + 4096);
    memcpy(g_slave.write_buf.buf + g_slave.write_buf.len, buf, (size_t)n);
    g_slave.write_buf.len += n;

    return next_off;
}

/* ── 解析新连接从库发来的 PSYNC（*2\r\n$5\r\nPSYNC\r\n$<n>\r\n<offset>\r\n）── */

static void slave_handle_psync(void) {
    char buf[256];
    ssize_t n = recv(g_slave.fd, buf, sizeof(buf) - 1, MSG_DONTWAIT);
    if (n <= 0) {
        if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
            printf("[INCR-MASTER] slave fd=%d disconnected\n", g_slave.fd);
            slave_close();
        }
        return;
    }

    kvs_token_t tokens[3];
    int argc = 0;
    if (resp_parse_slices(buf, (int)n, tokens, 3, &argc) <= 0 ||
        argc != 2 || !TOKEN_EQ(tokens[0], "PSYNC")) {
        fprintf(stderr, "[INCR-MASTER] bad handshake from fd=%d\n", g_slave.fd);
        slave_close();
        return;
    }

    /* tokens[1] 是十进制 offset */
    size_t psync_offset = 0;
    for (int i = 0; i < tokens[1].len; i++) {
        char c = tokens[1].ptr[i];
        if (c < '0' || c > '9') {
            fprintf(stderr, "[INCR-MASTER] bad offset from fd=%d\n", g_slave.fd);
            slave_close();
            return;
        }
        psync_offset = psync_offset * 10 + (size_t)(c - '0');
    }

    printf("[INCR-MASTER] PSYNC %zu from fd=%d (current=%zu)\n",
           psync_offset, g_slave.fd, repl_backlog_offset());

    if (repl_backlog_valid(psync_offset)) {
        /* 增量续传: 先回 +CONTINUE，再从 backlog 补发 */
        dynbuf_cat(&g_slave.write_buf, "+CONTINUE\r\n", 11);
        g_slave.state = REPL_STATE_STREAMING;

        size_t off = psync_offset;
        size_t target = repl_backlog_offset();
        while (off < target) {
            size_t next = slave_backfill(off);
            if (next <= off) break;  /* no progress */
            off = next;
        }
        printf("[INCR-MASTER] slave fd=%d streaming (backfill %zu -> %zu)\n",
               g_slave.fd, psync_offset, off);

        if (repl_engine_writable)
            repl_engine_writable(g_slave.fd, 1);
    } else {
        /* offset 过期 → 需全量同步，回 FULLRESYNC 后拆链 */
        char resp[64];
        int rlen = snprintf(resp, sizeof(resp),
                            "+FULLRESYNC %zu\r\n", repl_backlog_offset());
        send(g_slave.fd, resp, (size_t)rlen, MSG_NOSIGNAL);
        printf("[INCR-MASTER] slave fd=%d needs FULLRESYNC (asked=%zu)\n",
               g_slave.fd, psync_offset);
        slave_close();
    }
}

/* ── 接受从库连接 ────────────────────────────────────────────────────── */

int repl_incr_master_accept(int listen_fd) {
    (void)listen_fd;
    if (g_listen_fd < 0) return 0;

    while (1) {
        int cfd = accept4(g_listen_fd, NULL, NULL, SOCK_NONBLOCK);
        if (cfd < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK)
                perror("[INCR-MASTER] accept");
            break;
        }

        int opt = 1;
        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

        /* 单从机：新连接到来时，先拆掉仍挂着的旧连接 */
        if (g_slave.active)
            slave_close();

        memset(&g_slave, 0, sizeof(g_slave));
        g_slave.fd     = cfd;
        g_slave.state  = REPL_STATE_CONNECTED;
        g_slave.active = 1;
        dynbuf_init(&g_slave.write_buf, 65536);
        printf("[INCR-MASTER] slave connected fd=%d\n", cfd);

        if (repl_engine_register)
            repl_engine_register(cfd, CONN_KIND_REPL_SLAVE);
    }
    return 0;
}

/* ── 读回调：PSYNC 握手，或 streaming 时检测断开 ─────────────────────── */

int repl_incr_master_read(int fd) {
    if (!g_slave.active || g_slave.fd != fd) return -1;

    if (g_slave.state == REPL_STATE_CONNECTED) {
        slave_handle_psync();
    } else {
        /* Streaming: 从库不发数据，任何字节/EOF/错误都视为断开 */
        char tmp[64];
        ssize_t n = recv(fd, tmp, sizeof(tmp), MSG_DONTWAIT);
        if (n > 0 || n == 0 ||
            (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
            printf("[INCR-MASTER] slave fd=%d disconnected\n", fd);
            slave_close();
        }
    }
    return 0;
}

void repl_incr_master_close(int fd) {
    if (g_slave.active && g_slave.fd == fd)
        slave_close();
}

/* ── Setup / teardown ─────────────────────────────────────────────────── */

int repl_incr_master_start(int port) {
    g_listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (g_listen_fd < 0) {
        perror("[INCR-MASTER] socket");
        return -1;
    }

    int opt = 1;
    setsockopt(g_listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {0};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons((uint16_t)port);

    if (bind(g_listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("[INCR-MASTER] bind");
        close(g_listen_fd);
        g_listen_fd = -1;
        return -1;
    }

    if (listen(g_listen_fd, SOMAXCONN) < 0) {
        perror("[INCR-MASTER] listen");
        close(g_listen_fd);
        g_listen_fd = -1;
        return -1;
    }

    printf("[INCR-MASTER] listening on 0.0.0.0:%d\n", port);
    return 0;
}

int repl_incr_master_listen_fd(void) {
    return g_listen_fd;
}

/* ── 引擎查询该 fd 应等待的就绪事件（据此 arm 精确的 IORING_OP_POLL_ADD）──
 *   CONNECTED           -> POLLIN   等待 PSYNC 握手
 *   STREAMING + 有数据  -> POLLOUT  冲刷 write_buf
 *   STREAMING + 空闲    -> 0        无需等待（断开在下一次写时惰性发现）
 * 未知/未激活 fd 返回 0。 */

int repl_incr_master_poll_mask(int fd) {
    if (!g_slave.active || g_slave.fd != fd) return 0;

    if (g_slave.state == REPL_STATE_CONNECTED)
        return POLLIN;

    if (g_slave.state == REPL_STATE_STREAMING && g_slave.write_buf.len > 0)
        return POLLOUT;

    return 0;
}
