/* kvstore-incr-sync — standalone incremental sync daemon
 *
 * Pipeline:
 *   XDP BPF → ringbuf → xdp_reasm (TCP reassembly + RESP extract)
 *          → is_write_command() filter → tcp_send_all() → slave
 *
 * Configuration via #define (edit and recompile):
 *   SLAVE_IP          slave kvstore IP
 *   SLAVE_PORT        slave kvstore port (default 6379)
 *   KVS_FILTER_PORT   XDP capture filter port (must match kvstore's port)
 *   XDP_IFACE         network interface to attach XDP to
 *   XDP_MODE          "auto" (default), "generic" (SKB), "native" (DRV)
 */

#define _GNU_SOURCE
#include "xdp_reasm.h"
#include "tcp_sender.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* ── Configuration ─────────────────────────────────────────────────── */

#ifndef SLAVE_IP
#define SLAVE_IP  "127.0.0.1"
#endif
#ifndef SLAVE_PORT
#define SLAVE_PORT  6379
#endif
#ifndef KVS_FILTER_PORT
#define KVS_FILTER_PORT  6379
#endif
#ifndef XDP_IFACE
#define XDP_IFACE  "lo"
#endif

/* ── Global state ──────────────────────────────────────────────────── */

static volatile int g_running = 1;
static int g_slave_fd = -1;

/* ── Callback: forward write command to slave ──────────────────────── */

static void forward_to_slave(const uint8_t *cmd, uint32_t cmd_len)
{
    if (g_slave_fd < 0) return;

    if (tcp_send_all(g_slave_fd, cmd, (size_t)cmd_len) < 0) {
        fprintf(stderr, "[MAIN] send to slave failed, exiting\n");
        g_running = 0;
    }
}

/* ── Signal handler ────────────────────────────────────────────────── */

static void sig_handler(int sig)
{
    (void)sig;
    g_running = 0;
}

/* ── Main ──────────────────────────────────────────────────────────── */

int main(void)
{
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    printf("[MAIN] kvstore-incr-sync starting\n");
    printf("[MAIN] config: slave=%s:%u filter_port=%u iface=%s\n",
           SLAVE_IP, (unsigned)SLAVE_PORT,
           (unsigned)KVS_FILTER_PORT, XDP_IFACE);

    /* 1. Init XDP reassembly */
    if (xdp_reasm_init(forward_to_slave) != 0) {
        fprintf(stderr, "[MAIN] xdp_reasm_init failed\n");
        return 1;
    }

    /* 2. Load + attach XDP BPF */
    struct bpf_object *bpf_obj = NULL;
    struct ring_buffer *ringbuf = NULL;
    xdp_attach_t xdp_att;

    if (xdp_load_bpf(KVS_FILTER_PORT, XDP_IFACE,
                     xdp_reasm_on_event,
                     &bpf_obj, &ringbuf, &xdp_att) != 0) {
        fprintf(stderr, "[MAIN] xdp_load_bpf failed\n");
        xdp_reasm_shutdown();
        return 1;
    }

    /* 3. Connect to slave — loop until connected */
    while (g_running && g_slave_fd < 0) {
        g_slave_fd = tcp_connect_slave(SLAVE_IP, SLAVE_PORT);
        if (g_slave_fd < 0) {
            fprintf(stderr, "[MAIN] slave connect failed, retrying in 3s...\n");
            sleep(3);
        }
    }

    if (!g_running) {
        xdp_cleanup(&xdp_att, bpf_obj, ringbuf);
        xdp_reasm_shutdown();
        return 1;
    }

    printf("[MAIN] incremental sync active\n");

    /* 4. Main loop */
    time_t last_gc = time(NULL);

    while (g_running) {
        /* Poll XDP ringbuf — drain all available events */
        ring_buffer__poll(ringbuf, 1);

        /* Drain slave responses */
        tcp_drain(g_slave_fd);

        /* Periodic GC of stale connections (~5s) */
        time_t now = time(NULL);
        if (now - last_gc >= 5) {
            xdp_reasm_gc();
            last_gc = now;
        }
    }

    /* 5. Cleanup */
    printf("[MAIN] shutting down...\n");

    if (g_slave_fd >= 0) close(g_slave_fd);
    xdp_cleanup(&xdp_att, bpf_obj, ringbuf);
    xdp_reasm_shutdown();

    printf("[MAIN] done\n");
    return 0;
}
