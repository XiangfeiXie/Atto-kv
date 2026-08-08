/* kvstore-incr-sync — standalone incremental sync daemon
 *
 * Pipeline:
 *   XDP BPF → ringbuf → xdp_reasm (TCP reassembly + RESP extract)
 *          → is_write_command() filter → cmd_queue → writer thread → slave
 *
 * Configuration via config.h macros (edit and recompile):
 *   SLAVE_IP, SLAVE_PORT, KVS_FILTER_PORT, XDP_IFACE
 *   CONN_HASH_BITS, CONN_MAX, CMD_QUEUE_SIZE, WRITER_BATCH_SIZE
 */

#define _GNU_SOURCE
#include "config.h"
#include "xdp_reasm.h"
#include "tcp_sender.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* --Global state ──────────────────────────────────────────────────── */

static volatile int g_running = 1;
static cmd_queue_t   g_cmdq;
static tcp_writer_t  g_writer;
static uint64_t      g_queue_dropped;  /* counter, updated from callback */

/* Callback: called from xdp_reasm (inside bucket lock) */

static void on_write_command(const uint8_t *cmd, uint32_t cmd_len)
{
    if (cmdq_push(&g_cmdq, cmd, cmd_len) < 0)
        __atomic_add_fetch(&g_queue_dropped, 1, __ATOMIC_RELAXED);
}

/* --Signal handler ────────────────────────────────────────────────── */

static void sig_handler(int sig)
{
    (void)sig;
    g_running = 0;
}

/* --Stats printer ─────────────────────────────────────────────────── */

static void print_stats(void)
{
    struct xdp_reasm_stats st;
    xdp_reasm_get_stats(&st);

    uint64_t dropped = __atomic_load_n(&g_queue_dropped, __ATOMIC_RELAXED);

    printf("[STATS] events=%lu data=%lu syn=%lu fin_rst=%lu "
           "ordered=%lu cmds=%lu written=%lu "
           "ooo=%lu dup=%lu queue_drop=%lu frag=%lu conns=%u\n",
           (unsigned long)st.events_total,
           (unsigned long)st.events_data,
           (unsigned long)st.events_syn,
           (unsigned long)st.events_fin_rst,
           (unsigned long)st.bytes_ordered,
           (unsigned long)st.cmds_extracted,
           (unsigned long)st.cmds_written,
           (unsigned long)st.ooo_segments,
           (unsigned long)st.dup_discarded,
           (unsigned long)dropped,
           (unsigned long)st.fragment_events,
           (unsigned)st.conns_active);
}

/* --Main ──────────────────────────────────────────────────────────── */

int main(void)
{
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    printf("[MAIN] kvstore-incr-sync starting\n");
    printf("[MAIN] slave=%s:%u filter_port=%u iface=%s\n",
           SLAVE_IP, (unsigned)SLAVE_PORT,
           (unsigned)KVS_FILTER_PORT, XDP_IFACE);

    /* 1. Init command queue */
    cmdq_init(&g_cmdq, CMD_QUEUE_SIZE);

    /* 2. Init XDP reassembly */
    if (xdp_reasm_init(on_write_command) != 0) {
        fprintf(stderr, "[MAIN] xdp_reasm_init failed\n");
        return 1;
    }

    /* 3. Load + attach XDP BPF */
    struct bpf_object *bpf_obj = NULL;
    struct ring_buffer *ringbuf = NULL;
    xdp_attach_t        xdp_att;

    if (xdp_load_bpf(KVS_FILTER_PORT, XDP_IFACE,
                     xdp_reasm_on_event,
                     &bpf_obj, &ringbuf, &xdp_att) != 0) {
        fprintf(stderr, "[MAIN] xdp_load_bpf failed\n");
        xdp_reasm_shutdown();
        cmdq_drain(&g_cmdq);
        return 1;
    }

    /* 4. Start writer thread */
    if (tcp_writer_start(&g_writer, &g_cmdq, SLAVE_IP, SLAVE_PORT) != 0) {
        fprintf(stderr, "[MAIN] writer start failed\n");
        xdp_cleanup(&xdp_att, bpf_obj, ringbuf);
        xdp_reasm_shutdown();
        cmdq_drain(&g_cmdq);
        return 1;
    }

    printf("[MAIN] incremental sync active\n");

    /* 5. Main loop */
    time_t last_gc    = time(NULL);
    time_t last_stats = last_gc;
    uint64_t last_events = 0;

    while (g_running) {
        /* Poll XDP ringbuf */
        ring_buffer__poll(ringbuf, RINGBUF_POLL_MS);

        time_t now = time(NULL);

        /* Periodic GC */
        if (now - last_gc >= GC_INTERVAL_S) {
            xdp_reasm_gc();
            last_gc = now;
        }

        /* Periodic stats (~10s) */
        if (now - last_stats >= 10) {
            struct xdp_reasm_stats st;
            xdp_reasm_get_stats(&st);
            uint64_t rate = (st.events_total - last_events) / (uint64_t)(now - last_stats);
            printf("[MAIN] events=%lu (%.0f/s) written=%lu conns=%u ooo=%lu\n",
                   (unsigned long)st.events_total,
                   (double)rate,
                   (unsigned long)st.cmds_written,
                   (unsigned)st.conns_active,
                   (unsigned long)st.ooo_segments);
            last_events = st.events_total;
            last_stats  = now;
        }
    }

    /* 6. Shutdown */
    printf("[MAIN] shutting down...\n");

    /* Detach XDP first — stop producing new events */
    xdp_cleanup(&xdp_att, bpf_obj, ringbuf);

    /* Signal writer and drain */
    tcp_writer_stop(&g_writer);
    cmdq_shutdown(&g_cmdq);

    /* Give writer a moment to flush */
    usleep(200000);

    cmdq_drain(&g_cmdq);
    xdp_reasm_shutdown();

    /* Final stats */
    print_stats();
    printf("[MAIN] done\n");
    return 0;
}
