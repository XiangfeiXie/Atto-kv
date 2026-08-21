/* kcp_transport.h — KCP reliable-stream glue over the AF_XDP datapath.
 *
 * Owns the ikcpcb and the frame assembly/parsing.  Presents a reliable byte
 * stream (KCP stream mode) on top of raw Eth+IPv4+UDP frames pushed through
 * af_xdp.  The ikcp_output callback is where a KCP datagram becomes an
 * Ethernet frame; the AF_XDP Rx callback is where a frame becomes a KCP
 * datagram fed back into ikcp_input.
 */

#ifndef KCP_TRANSPORT_H
#define KCP_TRANSPORT_H

#include <stdint.h>
#include <stddef.h>

#include "af_xdp.h"

typedef struct kcp_transport kcp_transport_t;

typedef struct {
    const char *iface;          /* NIC to bind AF_XDP on */
    const char *bpf_obj_path;   /* compiled xdp_kcp.bpf.o */
    uint8_t     peer_mac[6];
    uint32_t    local_ip;       /* network byte order */
    uint32_t    peer_ip;        /* network byte order */
    uint16_t    sync_port;      /* host byte order */
    int         is_master;      /* 1 = master (send side), 0 = slave */
} kcp_transport_cfg_t;

kcp_transport_t *kcp_transport_init(const kcp_transport_cfg_t *cfg);
void kcp_transport_destroy(kcp_transport_t *t);

/* Master: append `len` bytes to the KCP send queue (no flush yet).  Call
 * kcp_flush() once after a batch so adjacent commands coalesce into full MTU
 * segments instead of emitting one datagram per command.  Returns 0 on
 * success, -1 if KCP cannot accept the data (queue full / OOM). */
int kcp_send_queue(kcp_transport_t *t, const void *data, size_t len);

/* Master: flush the queued KCP data to the wire (ikcp_flush). */
void kcp_flush(kcp_transport_t *t);

/* Slave: drain up to `cap` bytes of the ordered stream into buf.
 * Returns bytes copied (>0), 0 if nothing ready, -1 on error. */
int kcp_recv_stream(kcp_transport_t *t, void *buf, size_t cap);

/* Both roles, every loop iteration: poll AF_XDP Rx → ikcp_input, then
 * ikcp_update (drives ACKs / retransmit via ikcp_output), then recycle Tx
 * completions.  `now_ms` is a monotonic millisecond clock. */
void kcp_poll(kcp_transport_t *t, uint64_t now_ms);

/* Snapshot of transport + datapath counters, for diagnostics. */
typedef struct {
    uint64_t kcp_sent;      /* bytes pushed into the KCP stream */
    uint64_t kcp_recv;      /* bytes pulled from the KCP stream */
    uint64_t tx_fail;       /* datagrams that failed to reach the wire */
    uint64_t rx_invalid;    /* frames that were not our UDP:sync_port */
    af_xdp_stats_t xdp;     /* underlying AF_XDP ring counters */
} kcp_transport_stats_t;

void kcp_transport_stats(const kcp_transport_t *t, kcp_transport_stats_t *out);

#endif /* KCP_TRANSPORT_H */
