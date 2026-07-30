/* repl_xdp_reasm.h — XDP-based replication reassembly API
 *
 * Responsibilities:
 *   - Connection tracking (saddr+sport → per-conn state)
 *   - ISN extraction from SYN packets
 *   - TCP sequence-number-based dedup & ordering
 *   - Feed ordered byte stream into RESP command extraction
 *   - Forward write commands via callback (reuses existing RESP parser)
 *   - XDP BPF loader
 *
 * Usage:
 *   xdp_attach_t att;
 *   xdp_reasm_init(on_write_command);
 *   xdp_load_bpf(kv_port, "lo", xdp_reasm_on_event, &obj, &rb, &att);
 *   // ... in main loop: ring_buffer__poll(rb, 1);
 *   // ... periodically: xdp_reasm_gc();
 *   // ... on shutdown: xdp_reasm_shutdown(); xdp_cleanup(&att, obj, rb);
 */

#ifndef XDP_REASM_H
#define XDP_REASM_H

#include <stdint.h>
#include <stddef.h>
#include <bpf/libbpf.h>    /* ring_buffer_sample_fn, struct bpf_object, etc. */

/* ── Callback ────────────────────────────────────────────────────────── */

/* Called when a complete write-command RESP frame is extracted. */
typedef void (*xdp_reasm_write_cb)(const uint8_t *cmd, uint32_t cmd_len);

/* ── Attach tracking ─────────────────────────────────────────────────── */

typedef struct {
    int             use_netlink;   /* 0 = bpf_link, 1 = netlink */
    struct bpf_link *link;
    int             ifindex;
    int             prog_fd;
} xdp_attach_t;

/* ── API ─────────────────────────────────────────────────────────────── */

/* Initialize reassembly subsystem. */
int  xdp_reasm_init(xdp_reasm_write_cb on_write);

/* Shutdown, free all connection state. Does NOT detach XDP or free ringbuf. */
void xdp_reasm_shutdown(void);

/* Ringbuf callback — matches ring_buffer_sample_fn signature. */
int  xdp_reasm_on_event(void *ctx, void *data, size_t data_sz);

/* Garbage-collect stale connections (call every ~5s). */
void xdp_reasm_gc(void);

/* Get statistics. */
struct xdp_reasm_stats {
    uint64_t events_total;
    uint64_t events_data;
    uint64_t events_syn;
    uint64_t events_fin_rst;
    uint64_t bytes_ordered;
    uint64_t cmds_extracted;
    uint64_t cmds_written;
    uint32_t conns_active;
    uint64_t ooo_segments;
    uint64_t dup_discarded;
};

void xdp_reasm_get_stats(struct xdp_reasm_stats *st);

/* ── XDP loader ──────────────────────────────────────────────────────── */

/* Load XDP BPF, attach to iface ($XDP_IFACE or "lo"), create ringbuf.
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

/* Detach XDP program (link or netlink). Safe to call multiple times. */
void xdp_detach(xdp_attach_t *a);

/* Convenience: detach + free ringbuf + close bpf_object. */
static inline void xdp_cleanup(xdp_attach_t *a,
                               struct bpf_object *obj,
                               struct ring_buffer *rb) {
    if (rb)  ring_buffer__free(rb);
    if (obj) bpf_object__close(obj);
    xdp_detach(a);
}

#endif /* XDP_REASM_H */
