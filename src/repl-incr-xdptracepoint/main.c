/* kvstore-incr-sync — standalone incremental sync daemon
 *
 * Single-threaded pipeline (Plan A):
 *   XDP BPF → ringbuf → xdp_reasm (lock-free TCP reassembly + RESP extract)
 *          → on_write() → direct append to slave write buffer
 *          → post-poll flush → non-blocking send() to slave
 *
 * No command queue, no writer thread, no mutexes.
 *
 * Configuration via config.h macros (edit and recompile):
 *   SLAVE_IP, SLAVE_PORT, KVS_FILTER_PORT, XDP_IFACE
 *   SLAVE_WB_HIGH_WATER, CONN_MAX, CONN_HASH_BITS
 */

#define _GNU_SOURCE
#include "config.h"
#include "xdp_loader.h"    /* xdp_load_bpf, xdp_cleanup, xdp_attach_t */
#include "xdp_reasm.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* ── Minimal dynamic write buffer ─────────────────────────────────────── */

typedef struct {
    char *buf;
    int   len;
    int   cap;
} slave_wb_t;

static void slave_wb_init(slave_wb_t *wb, int init_cap) {
    wb->buf = (char *)malloc((size_t)init_cap);
    wb->len = 0;
    wb->cap = wb->buf ? init_cap : 0;
}

static int slave_wb_append(slave_wb_t *wb, const char *data, int data_len) {
    int need = wb->len + data_len;
    if (unlikely(need > wb->cap)) {
        int new_cap = wb->cap ? wb->cap : 4096;
        while (new_cap < need)
            new_cap *= 2;
        if (new_cap > SLAVE_WB_HIGH_WATER)
            new_cap = SLAVE_WB_HIGH_WATER;
        if (new_cap < need)
            return -1;  /* would exceed high water mark */
        char *nb = (char *)realloc(wb->buf, (size_t)new_cap);
        if (unlikely(!nb)) return -1;
        wb->buf = nb;
        wb->cap = new_cap;
    }
    memcpy(wb->buf + wb->len, data, (size_t)data_len);
    wb->len = need;
    return 0;
}

static void slave_wb_free(slave_wb_t *wb) {
    free(wb->buf);
    wb->buf = NULL;
    wb->cap = 0;
    wb->len = 0;
}

/* ── Global state ─────────────────────────────────────────────────────── */

static volatile int  g_running           = 1;
static int           g_slave_fd          = -1;
static int           g_slave_connecting  = 0;
static time_t        g_slave_reconnect_after;
static slave_wb_t    g_slave_wb;
static uint64_t      g_events_last;

/* ── Slave send (non-blocking) ──────────────────────────────────────────
 *
 * Drains as much as the kernel TCP buffer accepts.  Unsent data stays
 * in g_slave_wb and will be retried on the next loop iteration.
 *
 * Returns:  1 = fully sent
 *           0 = partial send (EAGAIN, data remains in wb)
 *          -1 = hard error / disconnect                                */

static int slave_flush(void) {
    if (g_slave_fd < 0 || g_slave_wb.len == 0)
        return 1;

    while (g_slave_wb.len > 0) {
        ssize_t n = send(g_slave_fd,
                         g_slave_wb.buf, (size_t)g_slave_wb.len,
                         MSG_NOSIGNAL | MSG_DONTWAIT);
        if (likely(n > 0)) {
            size_t unsent = (size_t)g_slave_wb.len - (size_t)n;
            if (unsent > 0)
                memmove(g_slave_wb.buf, g_slave_wb.buf + n, unsent);
            g_slave_wb.len = (int)unsent;
        } else if (n == 0) {
            fprintf(stderr, "[SLAVE] peer closed connection\n");
            close(g_slave_fd);
            g_slave_fd = -1;
            g_slave_connecting = 0;
            g_slave_reconnect_after = time(NULL) + SLAVE_RECONNECT_S;
            return -1;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return 0;
            perror("[SLAVE] send");
            close(g_slave_fd);
            g_slave_fd = -1;
            g_slave_connecting = 0;
            g_slave_reconnect_after = time(NULL) + SLAVE_RECONNECT_S;
            return -1;
        }
    }

    /* Fully drained — release large buffer if idle */
    if (unlikely(g_slave_wb.cap > 65536)) {
        free(g_slave_wb.buf);
        g_slave_wb.buf = NULL;
        g_slave_wb.cap = 0;
    }
    g_slave_wb.len = 0;
    return 1;
}

/* ── Slave connect (non-blocking) ─────────────────────────────────────── */

static int slave_connect_start(void) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (unlikely(fd < 0)) {
        perror("[SLAVE] socket");
        return -1;
    }

    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    struct sockaddr_in addr = {.sin_family = AF_INET,
                               .sin_port   = htons((uint16_t)SLAVE_PORT)};
    if (unlikely(inet_pton(AF_INET, SLAVE_IP, &addr.sin_addr) != 1)) {
        fprintf(stderr, "[SLAVE] invalid IP: %s\n", SLAVE_IP);
        close(fd);
        return -1;
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
        return fd;  /* immediate success (localhost) */
    }
    if (errno == EINPROGRESS) {
        g_slave_connecting = 1;
        return fd;  /* async connect in progress */
    }

    perror("[SLAVE] connect");
    close(fd);
    return -1;
}

/* Poll the socket for writability after EINPROGRESS.
 * Returns: 0 = connected, -1 = still in progress / error. */
static int slave_connect_poll(int fd) {
    int err = 0;
    socklen_t len = sizeof(err);
    if (unlikely(getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0)) {
        perror("[SLAVE] getsockopt SO_ERROR");
        return -1;
    }
    if (err == 0) {
        g_slave_connecting = 0;
        return 0;  /* connected */
    }
    if (err == EINPROGRESS)
        return -1;  /* still connecting */

    fprintf(stderr, "[SLAVE] connect failed: %s\n", strerror(err));
    return -1;
}

static void slave_try_reconnect(void) {
    time_t now = time(NULL);

    /* Async connect still in progress — check it */
    if (g_slave_connecting && g_slave_fd >= 0) {
        if (slave_connect_poll(g_slave_fd) == 0) {
            printf("[SLAVE] connected to %s:%u\n",
                   SLAVE_IP, (unsigned)SLAVE_PORT);
        } else {
            close(g_slave_fd);
            g_slave_fd = -1;
            g_slave_connecting = 0;
            g_slave_reconnect_after = now + SLAVE_RECONNECT_S;
        }
        return;
    }

    /* Disconnected and backoff elapsed — start new connect */
    if (g_slave_fd < 0 && now >= g_slave_reconnect_after) {
        g_slave_fd = slave_connect_start();
        if (g_slave_fd >= 0 && !g_slave_connecting) {
            printf("[SLAVE] connected to %s:%u\n",
                   SLAVE_IP, (unsigned)SLAVE_PORT);
        } else if (g_slave_fd >= 0) {
            /* EINPROGRESS — poll in next iteration */
        } else {
            g_slave_reconnect_after = now + SLAVE_RECONNECT_S;
        }
    }
}

/* ── on_write callback ───────────────────────────────────────────────────
 *
 * Called from xdp_reasm_on_event (inside ring_buffer__poll context).
 * Writes directly into g_slave_wb.  Returns -1 on backpressure
 * (slave disconnected or write buffer full).                         */

static int on_write_command(const uint8_t *cmd, uint32_t cmd_len) {
    if (unlikely(g_slave_fd < 0 || g_slave_connecting))
        return -1;

    if (unlikely(g_slave_wb.len + (int)cmd_len > SLAVE_WB_HIGH_WATER))
        return -1;

    slave_wb_append(&g_slave_wb, (const char *)cmd, (int)cmd_len);
    return 0;
}

/* ── Signal handler ───────────────────────────────────────────────────── */

static void sig_handler(int sig) {
    (void)sig;
    g_running = 0;
}

/* ── Main ─────────────────────────────────────────────────────────────── */

int main(void) {
    setlinebuf(stdout);
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    printf("[MAIN] kvstore-incr-sync starting (single-threaded)\n");
    printf("[MAIN] slave=%s:%u filter_port=%u iface=%s\n",
           SLAVE_IP, (unsigned)SLAVE_PORT,
           (unsigned)KVS_FILTER_PORT, XDP_IFACE);
    printf("[MAIN] slave wb high=%u low=%u\n",
           (unsigned)SLAVE_WB_HIGH_WATER, (unsigned)SLAVE_WB_LOW_WATER);

    /* 1. Init slave write buffer */
    slave_wb_init(&g_slave_wb, 65536);

    /* 2. Init XDP reassembly (lock-free, single-threaded) */
    if (unlikely(xdp_reasm_init(on_write_command) != 0)) {
        fprintf(stderr, "[MAIN] xdp_reasm_init failed\n");
        slave_wb_free(&g_slave_wb);
        return 1;
    }

    /* 3. Load + attach XDP BPF */
    struct bpf_object  *bpf_obj = NULL;
    struct ring_buffer *ringbuf = NULL;
    xdp_attach_t        xdp_att;

    if (unlikely(xdp_load_bpf(KVS_FILTER_PORT, XDP_IFACE,
                               xdp_reasm_on_event,
                               &bpf_obj, &ringbuf, &xdp_att) != 0)) {
        fprintf(stderr, "[MAIN] xdp_load_bpf failed\n");
        xdp_reasm_shutdown();
        slave_wb_free(&g_slave_wb);
        return 1;
    }

    /* 4. Initiate first slave connection (non-blocking) */
    g_slave_reconnect_after = 0;
    slave_try_reconnect();

    printf("[MAIN] incremental sync active\n");

    /* 5. Main loop */
    time_t last_gc    = time(NULL);
    time_t last_stats = last_gc;
    g_events_last     = 0;

    while (likely(g_running)) {
        /* Adaptive poll timeout: fast when backlog exists, idle otherwise */
        int timeout_ms = (g_slave_wb.len > 0)
                         ? SLAVE_POLL_FAST_MS : SLAVE_POLL_IDLE_MS;

        /* Poll XDP ringbuf — on_write_command appends directly to g_slave_wb */
        ring_buffer__poll(ringbuf, timeout_ms);

        /* Flush slave write buffer (non-blocking) */
        slave_flush();

        /* Reconnect if needed (non-blocking, with backoff) */
        if (unlikely(g_slave_fd < 0 || g_slave_connecting))
            slave_try_reconnect();

        /* Periodic maintenance */
        time_t now = time(NULL);

        if (unlikely(now - last_gc >= GC_INTERVAL_S)) {
            xdp_reasm_gc();
            last_gc = now;
        }

        /* Periodic stats (~10 s) */
        if (unlikely(now - last_stats >= 10)) {
            struct xdp_reasm_stats st;
            xdp_reasm_get_stats(&st);
            uint64_t rate = (st.events_total - g_events_last)
                          / (uint64_t)(now - last_stats);
            printf("[MAIN] events=%lu (%.0f/s) written=%lu conns=%u "
                   "ooo=%lu wb=%d send_drop=%lu "
                   "buf_reset=%lu parse_err=%lu\n",
                   (unsigned long)st.events_total,
                   (double)rate,
                   (unsigned long)st.cmds_written,
                   (unsigned)st.conns_active,
                   (unsigned long)st.ooo_segments,
                   g_slave_wb.len,
                   (unsigned long)st.send_dropped,
                   (unsigned long)st.buf_resets,
                   (unsigned long)st.parse_errors);
            g_events_last = st.events_total;
            last_stats    = now;
        }
    }

    /* ── Shutdown ──────────────────────────────────────────────────── */

    /* Print final stats for diagnostics */
    {
        struct xdp_reasm_stats st;
        xdp_reasm_get_stats(&st);
        printf("[MAIN] FINAL: events=%lu written=%lu conns=%u "
               "ooo=%lu wb=%d send_drop=%lu "
               "buf_reset=%lu parse_err=%lu\n",
               (unsigned long)st.events_total,
               (unsigned long)st.cmds_written,
               (unsigned)st.conns_active,
               (unsigned long)st.ooo_segments,
               g_slave_wb.len,
               (unsigned long)st.send_dropped,
               (unsigned long)st.buf_resets,
               (unsigned long)st.parse_errors);
    }

    printf("[MAIN] shutting down...\n");

    /* Print BPF-side stats (ringbuf drops etc.) */
    xdp_print_bpf_stats(bpf_obj);

    /* Detach XDP first — stop producing new events */
    xdp_cleanup(&xdp_att, bpf_obj, ringbuf);

    /* Final blocking flush of slave write buffer */
    if (g_slave_fd >= 0 && g_slave_wb.len > 0) {
        printf("[MAIN] flushing final %d bytes to slave...\n",
               g_slave_wb.len);
        while (g_slave_wb.len > 0) {
            ssize_t n = send(g_slave_fd, g_slave_wb.buf,
                             (size_t)g_slave_wb.len, MSG_NOSIGNAL);
            if (n <= 0) break;
            size_t unsent = (size_t)g_slave_wb.len - (size_t)n;
            if (unsent > 0)
                memmove(g_slave_wb.buf, g_slave_wb.buf + n, unsent);
            g_slave_wb.len = (int)unsent;
        }
    }

    if (g_slave_fd >= 0)
        close(g_slave_fd);

    slave_wb_free(&g_slave_wb);
    xdp_reasm_shutdown();

    printf("[MAIN] done\n");
    return 0;
}
