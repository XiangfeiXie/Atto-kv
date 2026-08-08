/* xdp_reasm.h — XDP-based replication: TCP reassembly + RESP extraction
 *
 * Responsibilities:
 *   - Per-bucket-locked connection tracking (saddr+sport)
 *   - ISN extraction from SYN packets
 *   - TCP sequence-number-based dedup & ordering
 *   - Circular buffer for ordered byte stream
 *   - RESP command extraction + write-command filtering
 *   - Forward write commands via callback
 *   - XDP BPF loader (libbpf)
 *
 * Usage:
 *   xdp_reasm_init(on_write);
 *   xdp_load_bpf(port, iface, xdp_reasm_on_event, &obj, &rb, &att);
 *   // poll loop: ring_buffer__poll(rb, timeout);
 *   // periodic:  xdp_reasm_gc();
 *   // shutdown:  xdp_reasm_shutdown(); xdp_cleanup(&att, obj, rb);
 */

#ifndef XDP_REASM_H
#define XDP_REASM_H

#include <stdint.h>
#include <stddef.h>
#include <bpf/libbpf.h>    /* ring_buffer_sample_fn, struct bpf_object */

/* ── Callback (called from within bucket lock) ────────────────────────── */

typedef void (*xdp_reasm_write_cb)(const uint8_t *cmd, uint32_t cmd_len);

/* ── XDP attach tracking ──────────────────────────────────────────────── */

typedef struct {
    int             use_netlink;   /* 0 = bpf_link, 1 = netlink */
    struct bpf_link *link;
    int             ifindex;
    int             prog_fd;
} xdp_attach_t;

/* ── Statistics (atomic counters, read via xdp_reasm_get_stats) ──────── */

struct xdp_reasm_stats {
    uint64_t events_total;       /* ringbuf events received */
    uint64_t events_data;        /* data-payload events */
    uint64_t events_syn;         /* SYN events */
    uint64_t events_fin_rst;     /* FIN/RST events */
    uint64_t bytes_ordered;      /* bytes appended to circ buffer */
    uint64_t cmds_extracted;     /* complete RESP commands parsed */
    uint64_t cmds_written;       /* write commands forwarded */
    uint64_t ooo_segments;       /* current OOO segment count */
    uint64_t dup_discarded;      /* duplicate bytes discarded */
    uint64_t queue_dropped;      /* commands dropped (queue full) */
    uint64_t fragment_events;    /* IP fragment events */
    uint32_t conns_active;       /* current connection count */
};

/* ── API ──────────────────────────────────────────────────────────────── */

/* Init reassembly subsystem. on_write will be called for each write cmd. */
int  xdp_reasm_init(xdp_reasm_write_cb on_write);

/* Ringbuf callback — matches ring_buffer_sample_fn signature.
 * Thread-safe via per-bucket locking. */
int  xdp_reasm_on_event(void *ctx, void *data, size_t data_sz);

/* Garbage-collect stale connections (call every ~5s from main loop). */
void xdp_reasm_gc(void);

/* Shutdown, free all connection state. Does NOT detach XDP or free ringbuf. */
void xdp_reasm_shutdown(void);

/* Copy current statistics. */
void xdp_reasm_get_stats(struct xdp_reasm_stats *st);

/* ── XDP loader ──────────────────────────────────────────────────────── */

/* Load XDP BPF, attach to iface, create ringbuf.
 *
 * kv_port:    kvstore TCP port to filter on (host byte order)
 * iface_name: e.g. "lo", "eth0" (NULL → $XDP_IFACE → "lo")
 * ringbuf_cb: xdp_reasm_on_event (or custom handler)
 *
 * Returns: 0 on success, -1 on error.
 * On success, caller owns out_obj/out_rb/out_att and must clean up. */
int  xdp_load_bpf(uint16_t kv_port, const char *iface_name,
                  ring_buffer_sample_fn ringbuf_cb,
                  struct bpf_object **out_obj,
                  struct ring_buffer **out_rb,
                  xdp_attach_t *out_att);

/* Detach XDP program (link or netlink). Idempotent. */
void xdp_detach(xdp_attach_t *a);

/* Convenience: detach + free ringbuf + close bpf_object. */
static inline void xdp_cleanup(xdp_attach_t *a,
                               struct bpf_object *obj,
                               struct ring_buffer *rb)
{
    if (rb)  ring_buffer__free(rb);
    if (obj) bpf_object__close(obj);
    xdp_detach(a);
}

#endif /* XDP_REASM_H */
