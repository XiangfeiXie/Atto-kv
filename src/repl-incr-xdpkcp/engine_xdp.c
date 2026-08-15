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

static shm_region_t *g_shm = NULL;
static int           g_slave_active = 0;
static DynBuf        g_rx_buf = {0};     /* slave: reassembly buffer */
static uint64_t      g_enqueue_drops = 0;

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

void xdp_engine_enqueue(const char *data, int len)
{
    if (!g_shm || !data || len <= 0)
        return;
    if (len > SHM_MAX_PAYLOAD) {
        g_enqueue_drops++;
        return;
    }
    if (spsc_enqueue(&g_shm->q, data, (uint32_t)len) != 0)
        g_enqueue_drops++;               /* gateway fell behind — drop (hot path) */
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
    dynbuf_free(&g_rx_buf);
    shm_region_destroy(g_shm, 1);
    g_shm = NULL;
    g_slave_active = 0;
}
