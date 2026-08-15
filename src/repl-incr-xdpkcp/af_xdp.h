/* af_xdp.h — raw AF_XDP transport (no libxdp / libbpf xsk.h dependency).
 *
 * Implements the minimal kernel-bypass datapath the gateway needs:
 *   - UMEM allocation + Fill/Completion/Rx/Tx ring setup via <linux/if_xdp.h>
 *   - load & attach the XDP program (xdp_kcp.bpf.o) that redirects
 *     UDP:sync_port into the XSK via the XSKMAP
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

#endif /* AF_XDP_H */
