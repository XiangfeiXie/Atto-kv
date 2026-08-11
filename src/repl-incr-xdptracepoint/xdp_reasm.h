/* xdp_reasm.h — TCP reassembly + RESP extraction API
 *
 * Single-threaded, lock-free architecture (Plan A):
 *   ringbuf poll → xdp_reasm_on_event() → on_write callback
 *
 * Responsibilities:
 *   - Connection tracking (saddr+sport → hash bucket, lock-free)
 *   - ISN extraction from SYN packets
 *   - TCP sequence-number-based dedup & ordering
 *   - OOO (out-of-order) segment linked list with merge
 *   - Circular buffer for ordered byte stream
 *   - RESP protocol command extraction + write-command filtering
 *   - Forward write commands via callback (returns 0=ok, -1=backpressure drop)
 *
 * Usage (single-threaded):
 *   xdp_reasm_init(on_write);
 *   // In main loop:
 *   //   ring_buffer__poll(rb, timeout)  → xdp_reasm_on_event invoked
 *   //   xdp_reasm_gc();                 → periodic
 *   // Shutdown:
 *   //   xdp_reasm_shutdown();
 */

#ifndef XDP_REASM_H
#define XDP_REASM_H

#include <stdint.h>
#include <stddef.h>
#include <bpf/libbpf.h>    /* ring_buffer_sample_fn */

/* ── Callback type ───────────────────────────────────────────────────────
 *
 * Called for each parsed write command.  Returns:
 *   0 = accepted
 *  -1 = dropped (backpressure: slave write buffer full or disconnected)  */

typedef int (*xdp_reasm_write_cb)(const uint8_t *cmd, uint32_t cmd_len);

/* ── Statistics (plain counters — single-threaded, no atomics needed) ─── */

struct xdp_reasm_stats {
    uint64_t events_total;       /* ringbuf events received */
    uint64_t events_data;        /* data-payload events */
    uint64_t events_syn;         /* SYN events */
    uint64_t events_fin_rst;     /* FIN/RST events */
    uint64_t bytes_ordered;      /* bytes appended to circular buffer */
    uint64_t cmds_extracted;     /* complete RESP commands parsed */
    uint64_t cmds_written;       /* write commands forwarded */
    uint64_t ooo_segments;       /* current OOO segment count */
    uint64_t dup_discarded;      /* duplicate bytes discarded */
    uint64_t send_dropped;       /* commands dropped (slave wb full) */
    uint64_t fragment_events;    /* IP fragment events */
    uint32_t conns_active;       /* current connection count */
};

/* ── API ──────────────────────────────────────────────────────────────── */

/* Initialize reassembly subsystem.
 * on_write is called for each extracted write command. */
int  xdp_reasm_init(xdp_reasm_write_cb on_write);

/* Ringbuf event callback — matches ring_buffer_sample_fn signature.
 * Single-threaded: no internal locking. */
int  xdp_reasm_on_event(void *ctx, void *data, size_t data_sz);

/* Garbage-collect stale connections (call periodically, e.g. every 5 s). */
void xdp_reasm_gc(void);

/* Shutdown, free all connection state. Does NOT detach XDP or free ringbuf. */
void xdp_reasm_shutdown(void);

/* Copy current statistics snapshot. */
void xdp_reasm_get_stats(struct xdp_reasm_stats *st);

#endif /* XDP_REASM_H */
