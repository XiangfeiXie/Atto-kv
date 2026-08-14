/* repl_incr_master.c — Master-side incremental replication
 *
 *  - Creates a dedicated TCP listen socket (default port 2001)
 *  - Accepts slave connections, handles PSYNC handshake
 *  - Pushes backlog + live command stream to streaming slaves
 *
 *  fd ownership: the listen socket and each accepted slave fd are registered
 *  into the network engine's event loop.  The engine calls the accept/read/
 *  write callbacks below when the fd becomes ready; this module performs no
 *  I/O outside those callbacks (except the one-shot setup in *_start).
 *
 * Licensed under GPL-2.0 or later */

#define _GNU_SOURCE    /* accept4 */

#include "repl_incr.h"
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
static repl_slave_t g_slaves[REPL_MAX_SLAVES];

/* ── Helpers ─────────────────────────────────────────────────────────── */

static repl_slave_t *slave_by_fd(int fd) {
    for (int i = 0; i < REPL_MAX_SLAVES; i++)
        if (g_slaves[i].active && g_slaves[i].fd == fd)
            return &g_slaves[i];
    return NULL;
}

static void slave_close(repl_slave_t *s) {
    int fd = s->fd;
    dynbuf_free(&s->write_buf);
    memset(s, 0, sizeof(*s));
    s->fd = -1;

    if (fd >= 0) {
        if (repl_engine_close) repl_engine_close(fd);
        else close(fd);
    }
}

static repl_slave_t *slave_alloc(void) {
    for (int i = 0; i < REPL_MAX_SLAVES; i++) {
        if (!g_slaves[i].active) {
            memset(&g_slaves[i], 0, sizeof(repl_slave_t));
            g_slaves[i].fd     = -1;
            g_slaves[i].active = 1;
            return &g_slaves[i];
        }
    }
    return NULL;
}

/* ── Propagate to streaming slaves ───────────────────────────────────── */

void repl_incr_propagate(const char *resp_data, int len) {
    for (int i = 0; i < REPL_MAX_SLAVES; i++) {
        repl_slave_t *s = &g_slaves[i];
        if (!s->active || s->state != REPL_STATE_STREAMING)
            continue;
        if (s->write_buf.len + len > s->write_buf.cap)
            dynbuf_reserve(&s->write_buf, s->write_buf.len + len + 4096);
        memcpy(s->write_buf.buf + s->write_buf.len, resp_data, (size_t)len);
        s->write_buf.len += len;

        /* Wake the engine so the pending bytes are flushed. */
        if (repl_engine_writable)
            repl_engine_writable(s->fd, 1);
    }
}

/* ── Flush slave write buffer (non-blocking) ──────────────────────────── */

int repl_incr_master_write(int fd) {
    repl_slave_t *s = slave_by_fd(fd);
    if (!s || !s->active) return -1;
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
        slave_close(s);
        return 0;
    }

    /* Re-arm EPOLLOUT only while bytes remain. */
    if (repl_engine_writable)
        repl_engine_writable(fd, s->write_buf.len > 0);
    return 0;
}

/* ── Send backlog from a given offset to slave ─────────────────────────
 *
 * Packs commands into the slave's write_buf.  Returns the next offset to
 * resume from, or the current head if caught up.                        */

static size_t slave_backfill(repl_slave_t *s, size_t offset) {
    char buf[65536];
    size_t next_off;

    int n = repl_backlog_fetch(offset, buf, (int)sizeof(buf), &next_off);
    if (n < 0 || n == 0)
        return repl_backlog_offset();  /* caught up or error */

    if (s->write_buf.len + n > s->write_buf.cap)
        dynbuf_reserve(&s->write_buf, s->write_buf.len + n + 4096);
    memcpy(s->write_buf.buf + s->write_buf.len, buf, (size_t)n);
    s->write_buf.len += n;

    return next_off;
}

/* ── Parse PSYNC command from newly connected slave ────────────────────
 *
 * Expects: *2\r\n$5\r\nPSYNC\r\n$<n>\r\n<offset>\r\n                    */

static void slave_handle_psync(repl_slave_t *s) {
    char buf[256];
    ssize_t n = recv(s->fd, buf, sizeof(buf) - 1, MSG_DONTWAIT);
    if (n <= 0) {
        if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
            printf("[INCR-MASTER] slave fd=%d disconnected\n", s->fd);
            slave_close(s);
        }
        return;
    }
    buf[n] = '\0';

    /* Quick sanity checks */
    if (n < 10 || buf[0] != '*' || !strstr(buf, "PSYNC")) {
        fprintf(stderr, "[INCR-MASTER] bad handshake from fd=%d\n", s->fd);
        slave_close(s);
        return;
    }

    /* Parse offset from the last bulk string: $<digits>\r\n<digits>\r\n */
    const char *dollar = strrchr(buf, '$');
    if (!dollar) { slave_close(s); return; }

    /* Skip bulk-string length: "$<n>\r\n" */
    const char *p = dollar + 1;
    while (*p >= '0' && *p <= '9') p++;
    if (*p == '\r') p++;
    if (*p == '\n') p++;

    /* Now parse the actual offset value */
    size_t psync_offset = 0;
    while (*p >= '0' && *p <= '9') {
        psync_offset = psync_offset * 10 + (size_t)(*p - '0');
        p++;
    }

    printf("[INCR-MASTER] PSYNC %zu from fd=%d (current=%zu)\n",
           psync_offset, s->fd, repl_backlog_offset());

    if (repl_backlog_valid(psync_offset)) {
        /* Incremental: queue +CONTINUE, then catch up from backlog */
        dynbuf_append(&s->write_buf, "+CONTINUE\r\n", 11);
        s->state      = REPL_STATE_STREAMING;
        s->repl_offset = psync_offset;

        size_t off = psync_offset;
        size_t target = repl_backlog_offset();
        while (off < target) {
            size_t next = slave_backfill(s, off);
            if (next <= off) break;  /* no progress */
            off = next;
        }
        printf("[INCR-MASTER] slave fd=%d streaming (backfill %zu -> %zu)\n",
               s->fd, psync_offset, off);

        if (repl_engine_writable)
            repl_engine_writable(s->fd, 1);
    } else {
        /* Need full sync — send the error inline, then drop the link. */
        char resp[64];
        int rlen = snprintf(resp, sizeof(resp),
                            "+FULLRESYNC %zu\r\n", repl_backlog_offset());
        send(s->fd, resp, (size_t)rlen, MSG_NOSIGNAL);
        printf("[INCR-MASTER] slave fd=%d needs FULLRESYNC (asked=%zu window=[%zu,%zu))\n",
               s->fd, psync_offset,
               repl_backlog_offset() > REPL_BACKLOG_SIZE
                   ? repl_backlog_offset() - REPL_BACKLOG_SIZE : 0,
               repl_backlog_offset());
        slave_close(s);
    }
}

/* ── Accept new slave connections ─────────────────────────────────────── */

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

        repl_slave_t *s = slave_alloc();
        if (!s) {
            fprintf(stderr, "[INCR-MASTER] slave table full\n");
            close(cfd);
            continue;
        }
        s->fd     = cfd;
        s->state  = REPL_STATE_CONNECTED;
        s->active = 1;
        dynbuf_init(&s->write_buf, 65536);
        printf("[INCR-MASTER] slave connected fd=%d\n", cfd);

        if (repl_engine_register)
            repl_engine_register(cfd, CONN_KIND_REPL_SLAVE);
    }
    return 0;
}

/* ── Read callback: PSYNC handshake, or drop detection while streaming ── */

int repl_incr_master_read(int fd) {
    repl_slave_t *s = slave_by_fd(fd);
    if (!s || !s->active) return -1;

    if (s->state == REPL_STATE_CONNECTED) {
        slave_handle_psync(s);
    } else {
        /* Streaming: a slave never sends.  Any bytes, EOF, or error = drop. */
        char tmp[64];
        ssize_t n = recv(fd, tmp, sizeof(tmp), MSG_DONTWAIT);
        if (n > 0 || n == 0 ||
            (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
            printf("[INCR-MASTER] slave fd=%d disconnected\n", fd);
            slave_close(s);
        }
    }
    return 0;
}

void repl_incr_master_close(int fd) {
    repl_slave_t *s = slave_by_fd(fd);
    if (s) slave_close(s);
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

/* ── Readiness query for the engine ─────────────────────────────────────
 *
 * Returns the poll mask the engine should wait on for a slave fd, so the
 * proactor can arm IORING_OP_POLL_ADD for just the readiness it needs:
 *
 *   CONNECTED             -> POLLIN   (await the PSYNC handshake)
 *   STREAMING + pending   -> POLLOUT  (flush write_buf)
 *   STREAMING + idle      -> 0        (nothing to wait on; a dropped slave
 *                                      is detected lazily on the next write)
 *
 * An unknown/inactive fd yields 0. */

int repl_incr_master_poll_mask(int fd) {
    repl_slave_t *s = slave_by_fd(fd);
    if (!s || !s->active) return 0;

    if (s->state == REPL_STATE_CONNECTED)
        return POLLIN;

    if (s->state == REPL_STATE_STREAMING && s->write_buf.len > 0)
        return POLLOUT;

    return 0;
}
