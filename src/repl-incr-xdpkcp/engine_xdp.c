/* engine_xdp.c — see engine_xdp.h.
 *
 * The slave replay path mirrors repl_incr_slave.c's replay loop: accumulate
 * ordered bytes, extract complete RESP commands with resp_parse_slices(),
 * and replay each via kvs_dispatch() under g_in_reload=1 (which suppresses
 * WAL append + incremental-backlog + eBPF on the replayed writes).
 */

#include "engine_xdp.h"
#include "shm_ipc.h"

#include "kvstore.h"     /* kvs_dispatch, g_in_reload, kvs_config */
#include "dynbuf.h"      /* DynBuf */

#include <stdio.h>
#include <string.h>
#include <sched.h>

static shm_region_t *g_shm = NULL;
static int           g_slave_active = 0;
static DynBuf        g_rx_buf = {0};     /* slave: reassembly buffer */

/* Master batching: coalesce many small write commands into one SHM slot so the
 * lock-free publish (and its cross-core cache-line ping-pong) is amortised over
 * ~SHM_MAX_PAYLOAD bytes instead of per command.  Single-threaded, so no lock. */
static char          g_tx_buf[SHM_MAX_PAYLOAD];
static int           g_tx_len = 0;

/* Shutdown diagnostics.  These are the only counters that make the engine's
 * copy of the pipeline visible (the gateway's own counters are printed in
 * gateway.c). */
static uint64_t      g_enqueue_total = 0; /* master: commands handed to gateway */
static uint64_t      g_rx_bytes      = 0; /* slave:  bytes drained from the gateway */
static uint64_t      g_replayed      = 0; /* slave:  commands replayed via kvs_dispatch */
static int           g_warned_full   = 0; /* one-shot "ring full" backpressure notice */

/* ── Slave: replay complete commands buffered in g_rx_buf ────────────── */

static void engine_replay_pending(void)
{
    if (g_rx_buf.len <= 0)
        return;

    DynBuf dummy_wb;
    dynbuf_init(&dummy_wb, 256);

    int off = 0;
    while (g_rx_buf.len - off > 0) {
        kvs_token_t tokens[KVS_MAX_TOKENS];
        int argc = 0;
        int cmd_len = resp_parse_slices(g_rx_buf.buf + off,
                                        g_rx_buf.len - off,
                                        tokens, KVS_MAX_TOKENS, &argc);
        if (cmd_len <= 0)
            break;                       /* incomplete command, wait for more */

        dummy_wb.len = 0;
        g_in_reload = 1;
        kvs_dispatch(tokens, argc, g_rx_buf.buf + off, cmd_len,
                     &dummy_wb, NULL);
        g_in_reload = 0;
        g_replayed++;

        off += cmd_len;
    }

    dynbuf_free(&dummy_wb);

    if (off > 0) {
        int remaining = g_rx_buf.len - off;
        if (remaining > 0)
            memmove(g_rx_buf.buf, g_rx_buf.buf + off, (size_t)remaining);
        g_rx_buf.len = remaining;
    }
}

/* ── Public API ──────────────────────────────────────────────────────── */

int xdp_engine_master_init(void)
{
    g_shm = shm_region_create();
    if (!g_shm)
        return -1;
    printf("[XDP-ENGINE] master SHM region created\n");
    return 0;
}

int xdp_engine_slave_init(void)
{
    g_shm = shm_region_create();
    if (!g_shm)
        return -1;
    dynbuf_init(&g_rx_buf, 65536);
    g_slave_active = 1;
    printf("[XDP-ENGINE] slave SHM region created\n");
    return 0;
}

/* Blocking publish of one ≤SHM_MAX_PAYLOAD chunk.  Lossless backpressure: the
 * ring is full only while the gateway is slower than us, so spin/yield rather
 * than drop — a replication path must never silently lose commands. */
static void enqueue_chunk(const char *data, uint32_t len)
{
    while (spsc_enqueue(&g_shm->q, data, len) != 0) {
        if (!g_warned_full) {
            printf("[XDP-ENGINE] SHM ring full, waiting for gateway...\n");
            g_warned_full = 1;
        }
        sched_yield();
    }
}

void xdp_engine_enqueue(const char *data, int len)
{
    if (!g_shm || !data || len <= 0)
        return;

    /* Oversized command: flush the pending batch, then chunk it directly. */
    if (len > SHM_MAX_PAYLOAD) {
        xdp_engine_flush();
        int off = 0;
        while (off < len) {
            int chunk = len - off;
            if (chunk > SHM_MAX_PAYLOAD)
                chunk = SHM_MAX_PAYLOAD;
            enqueue_chunk(data + off, (uint32_t)chunk);
            off += chunk;
        }
        g_enqueue_total++;
        return;
    }

    /* Coalesce small commands into one slot (the slave reassembles the byte
     * stream, so command boundaries are irrelevant to the transport). */
    if (g_tx_len + len > SHM_MAX_PAYLOAD)
        xdp_engine_flush();
    memcpy(g_tx_buf + g_tx_len, data, (size_t)len);
    g_tx_len += len;
    g_enqueue_total++;
}

void xdp_engine_flush(void)
{
    if (!g_shm || g_tx_len <= 0)
        return;
    enqueue_chunk(g_tx_buf, (uint32_t)g_tx_len);
    g_tx_len = 0;
}

void xdp_engine_poll(void)
{
    if (!g_shm || !g_slave_active)
        return;

    char buf[SHM_MAX_PAYLOAD];
    uint32_t len;
    int drained = 0;
    while (drained < 256 && spsc_dequeue(&g_shm->q, buf, &len) == 0) {
        dynbuf_reserve(&g_rx_buf, g_rx_buf.len + (int)len);
        memcpy(g_rx_buf.buf + g_rx_buf.len, buf, len);
        g_rx_buf.len += (int)len;
        g_rx_bytes += len;
        drained++;
    }

    if (g_rx_buf.len > 0)
        engine_replay_pending();
}

int xdp_engine_slave_active(void)
{
    return g_slave_active;
}

void xdp_engine_shutdown(void)
{
    if (!g_shm)
        return;

    /* Drain any commands still sitting in the master batch buffer. */
    xdp_engine_flush();

    /* One-line shutdown summary for diagnosing replication data loss.
     *   master: enqueued must equal the gateway's shm_in.
     *   slave:  rx_bytes must equal the gateway's shm_out; replayed must equal
     *           the number of commands written; rx_left > 0 means the replay
     *           stalled on an unparseable command and the rest never applied. */
    printf("[XDP-ENGINE] shutdown: enqueued=%llu rx_bytes=%llu "
           "replayed=%llu rx_left=%d\n",
           (unsigned long long)g_enqueue_total,
           (unsigned long long)g_rx_bytes,
           (unsigned long long)g_replayed,
           g_rx_buf.len);

    dynbuf_free(&g_rx_buf);
    shm_region_destroy(g_shm, 1);
    g_shm = NULL;
    g_slave_active = 0;
}
