/* af_xdp.h — AF_XDP transport (libxdp xsk.h for UMEM/socket/rings).
 *
 * Implements the minimal kernel-bypass datapath the gateway needs:
 *   - UMEM + Fill/Completion/Rx/Tx ring setup via libxdp's xsk.h
 *     (xsk_umem__create / xsk_socket__create)
 *   - load & attach the XDP program (xdp_kcp.bpf.o) that redirects
 *     UDP:sync_port into the XSK via the XSKMAP (libbpf attach)
 *   - af_xdp_send_frame()   — copy a raw Ethernet frame into a UMEM frame and
 *                             submit it on the Tx ring
 *   - af_xdp_poll_rx()      — drain the Rx ring, hand each frame to a callback
 *   - af_xdp_poll_tx()      — recycle completed Tx frames back into the Fill ring
 *
 * Frame layout is the caller's responsibility (kcp_transport assembles
 * Eth+IPv4+UDP+KCP); this layer is byte-agnostic.
 */

#ifndef AF_XDP_H
#define AF_XDP_H

#include <stdint.h>
#include <stddef.h>

typedef struct af_xdp_ctx af_xdp_ctx_t;

/* Open, bind, and attach everything for one NIC queue (queue 0).  Returns
 * NULL with diagnostics on any failure. */
af_xdp_ctx_t *af_xdp_init(const char *ifname, const char *bpf_obj_path);

void af_xdp_destroy(af_xdp_ctx_t *ctx);

/* Send one pre-assembled Ethernet frame.  0 on success, -1 on failure (no
 * free Tx frame, oversized, or empty ring). */
int af_xdp_send_frame(af_xdp_ctx_t *ctx, const void *frame, size_t len);

/* Drain up to `max_frames` received frames, invoking cb(frame, len, arg) for
 * each.  Returns the number of frames delivered. */
int af_xdp_poll_rx(af_xdp_ctx_t *ctx,
                   void (*cb)(const void *frame, size_t len, void *arg),
                   void *arg, int max_frames);

/* Recycle completed Tx frames into the Fill ring (frees UMEM slots). */
void af_xdp_poll_tx(af_xdp_ctx_t *ctx);

int  af_xdp_local_mac(af_xdp_ctx_t *ctx, uint8_t out[6]);
int  af_xdp_ifindex(af_xdp_ctx_t *ctx);

/* Snapshot of datapath counters, for diagnostics / shutdown summary. */
typedef struct {
    uint64_t rx_pkts;          /* frames received from the kernel */
    uint64_t tx_pkts;          /* frames submitted to the kernel */
    uint64_t tx_drops;         /* frames dropped: no free Tx frame / full ring */
    uint64_t rx_refill_fail;   /* Rx frames not returned to the fill ring */
} af_xdp_stats_t;

void af_xdp_stats(const af_xdp_ctx_t *ctx, af_xdp_stats_t *out);

#endif /* AF_XDP_H */
