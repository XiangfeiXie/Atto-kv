/* xdp_loader.h — XDP BPF program loader and attach/detach helpers
 *
 * Handles: finding the BPF .o file, loading BPF bytecode, attaching to
 * the target network interface via BPF link or netlink, and creating
 * the ringbuf consumer.
 *
 * Separated from xdp_reasm.h so the reassembly module is purely about
 * TCP stream reconstruction and RESP command extraction.
 */

#ifndef XDP_LOADER_H
#define XDP_LOADER_H

#include <stdint.h>
#include <bpf/libbpf.h>    /* struct bpf_object, struct ring_buffer,
                              ring_buffer_sample_fn, struct bpf_link */

/* ── XDP attach state ──────────────────────────────────────────────────── */

typedef struct {
    int              use_netlink;   /* 0 = bpf_link, 1 = netlink */
    struct bpf_link *link;
    int              ifindex;
    int              prog_fd;
} xdp_attach_t;

/* ── API ───────────────────────────────────────────────────────────────── */

/* Load XDP BPF object, attach to iface, create ringbuf consumer.
 *
 * kv_port:    kvstore TCP port to filter on (host byte order)
 * iface_name: e.g. "lo", "eth0" (NULL → XDP_IFACE from config.h)
 * ringbuf_cb: callback for each ringbuf event (xdp_reasm_on_event)
 *
 * On success returns 0; caller owns *out_obj, *out_rb, *out_att.
 * On failure returns -1 and cleans up any partially-created resources. */
int  xdp_load_bpf(uint16_t kv_port, const char *iface_name,
                  ring_buffer_sample_fn ringbuf_cb,
                  struct bpf_object **out_obj,
                  struct ring_buffer **out_rb,
                  xdp_attach_t *out_att);

/* Detach XDP program. Idempotent (safe to call multiple times). */
void xdp_detach(xdp_attach_t *a);

/* Read and print BPF-side statistics counters (ringbuf drops etc.) */
void xdp_print_bpf_stats(struct bpf_object *obj);

/* Convenience: detach XDP + free ringbuf + close bpf_object. */
static inline void xdp_cleanup(xdp_attach_t *a,
                               struct bpf_object *obj,
                               struct ring_buffer *rb)
{
    if (rb)  ring_buffer__free(rb);
    if (obj) bpf_object__close(obj);
    xdp_detach(a);
}

#endif /* XDP_LOADER_H */
