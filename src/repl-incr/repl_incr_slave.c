/* repl_incr_slave.c — Slave-side incremental replication
 *
 *  - TCP connect to master's incr_repl_port
 *  - Send PSYNC <offset>, handle +CONTINUE / +FULLRESYNC
 *  - Read the command stream (driven by the engine) and replay via kvs_dispatch
 */

#include "repl_incr.h"
#include "kvstore.h"
#include "protocol.h"
#include "kvstore_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <arpa/inet.h>

/* ── Module state ────────────────────────────────────────────────────── */

static int    g_repl_fd   = -1;   /* persistent TCP connection to master */
static int    g_streaming = 0;    /* 1 = +CONTINUE received, receiving commands */
static DynBuf g_slave_rb  = {0}; /* read buffer for command stream */

/* ── Replay complete RESP commands buffered in g_slave_rb ──────────────
 *
 * Drains g_slave_rb of complete commands and replays each via
 * kvs_dispatch() with g_in_reload=1 (suppress WAL + backlog).  Incomplete
 * trailing bytes are compacted to buf[0] and retained. */
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
            break;  /* incomplete command, wait for more data */

        dummy_wb.len = 0;
        g_in_reload = 1;
        kvs_dispatch(tokens, argc, g_slave_rb.buf + off, cmd_len,
                     &dummy_wb, NULL);
        g_in_reload = 0;

        off += cmd_len;
    }

    dynbuf_free(&dummy_wb);

    if (off > 0) {
        int remaining = g_slave_rb.len - off;
        if (remaining > 0)
            memmove(g_slave_rb.buf, g_slave_rb.buf + off, (size_t)remaining);
        g_slave_rb.len = remaining;
    }
}

/* ── Connect to master ────────────────────────────────────────────────── */

static int slave_connect(const char *ip, int port) {
    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", port);

    struct addrinfo hints = {0}, *ai;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(ip, port_str, &hints, &ai) != 0) {
        perror("[INCR-SLAVE] getaddrinfo");
        return -1;
    }

    int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) {
        perror("[INCR-SLAVE] socket");
        freeaddrinfo(ai);
        return -1;
    }

    int retry;
    for (retry = 0; retry < 10; retry++) {
        if (connect(fd, ai->ai_addr, ai->ai_addrlen) == 0)
            break;
        fprintf(stderr, "[INCR-SLAVE] connect retry %d/10...\n", retry + 1);
        sleep(1);
    }
    freeaddrinfo(ai);

    if (retry == 10) {
        perror("[INCR-SLAVE] connect");
        close(fd);
        return -1;
    }

    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    return fd;
}

/* ── Setup: connect + PSYNC handshake (blocking, one-shot) ───────────── */

int repl_incr_slave_start(const char *master_ip, int master_port) {
    int fd = slave_connect(master_ip, master_port);
    if (fd < 0)
        return -1;

    printf("[INCR-SLAVE] connected to %s:%d\n", master_ip, master_port);

    /* Build PSYNC command */
    size_t off = g_repl_snapshot_offset;
    char cmd[128], offbuf[32];
    int offlen = snprintf(offbuf, sizeof(offbuf), "%zu", off);
    int clen = snprintf(cmd, sizeof(cmd),
                        "*2\r\n$5\r\nPSYNC\r\n$%d\r\n%s\r\n",
                        offlen, offbuf);

    if (send(fd, cmd, (size_t)clen, MSG_NOSIGNAL) != clen) {
        perror("[INCR-SLAVE] send PSYNC");
        close(fd);
        return -1;
    }

    /* Read response */
    char resp[128] = {0};
    ssize_t n = recv(fd, resp, sizeof(resp) - 1, 0);
    if (n <= 0) {
        perror("[INCR-SLAVE] recv PSYNC response");
        close(fd);
        return -1;
    }

    if (strncmp(resp, "+CONTINUE", 9) == 0) {
        printf("[INCR-SLAVE] +CONTINUE — entering streaming mode\n");
        /* Preserve any extra data (backfill) beyond "+CONTINUE\r\n" */
        if (n > 11) {
            if (!g_slave_rb.buf)
                dynbuf_init(&g_slave_rb, 65536);
            dynbuf_reserve(&g_slave_rb, g_slave_rb.len + (int)(n - 11));
            memcpy(g_slave_rb.buf + g_slave_rb.len, resp + 11, (size_t)(n - 11));
            g_slave_rb.len += (int)(n - 11);
        }
        g_repl_fd   = fd;
        g_streaming = 1;
        /* Replay any backfill that arrived together with +CONTINUE — it
         * would otherwise sit buffered until the next command arrives. */
        slave_replay_pending();
        return 0;
    }

    if (strncmp(resp, "+FULLRESYNC", 11) == 0) {
        printf("[INCR-SLAVE] +FULLRESYNC — full sync required\n");
        close(fd);
        return -2;  /* caller checks: if rdma_sync is off, this is fatal */
    }

    fprintf(stderr, "[INCR-SLAVE] unexpected PSYNC response: %.*s\n",
            (int)n, resp);
    close(fd);
    return -1;
}

/* ── Read callback: receive and replay commands ────────────────────────
 *
 * Called by the engine when the master link is readable.  Accumulates data
 * in a static read buffer, extracts complete RESP commands, and replays
 * each via kvs_dispatch().  Returns 0 on success.                        */

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

    /* Extract and replay any complete commands now buffered */
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
}

int repl_incr_slave_link_fd(void) {
    return g_repl_fd;
}
