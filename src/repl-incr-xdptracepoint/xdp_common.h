/* repl_xdp_common.h — Shared structures between XDP BPF and user-space
 *
 * This file defines the ringbuf event format and constants used by both
 * the BPF XDP program (repl_filter_xdp.bpf.c) and the user-space
 * reassembly module (repl_xdp_reasm.c).
 */

#ifndef REPL_XDP_COMMON_H
#define REPL_XDP_COMMON_H

/* Max TCP payload per XDP event.
 * Standard MTU 1500 → max TCP payload ~1460 bytes.
 * With GRO in generic XDP mode: up to 64KB coalesced.
 * We use 4096 as a safe middle ground — covers jumbo frames
 * and moderate GRO coalescing. */
#define XDP_MAX_PAYLOAD   4096

/* Total event size: header (20) + payload (4096) = 4116 */
#define XDP_EVENT_SIZE    (20 + XDP_MAX_PAYLOAD)

/* Ringbuf size — same as current strparser ringbuf (8 MB) */
#define XDP_RINGBUF_SZ    (8 * 1024 * 1024)

/* Flags for xdp_repl_event.flags */
#define XDP_FLAG_SYN      0x01
#define XDP_FLAG_FIN      0x02
#define XDP_FLAG_RST      0x04

/* ── Ringbuf event (pushed from XDP BPF, consumed by user-space) ──── */

#ifdef __BPF__

/* BPF side: use __u32 etc from bpf_helper_defs.h */
struct xdp_repl_event {
    __u32 saddr;        /* source IPv4 address  (network byte order) */
    __u32 daddr;        /* dest IPv4 address    (network byte order) */
    __u16 sport;        /* source TCP port      (network byte order) */
    __u16 dport;        /* dest TCP port        (network byte order) */
    __u32 seq;          /* TCP sequence number  (network byte order) */
    __u16 data_len;     /* actual payload length (0..XDP_MAX_PAYLOAD) */
    __u8  flags;        /* XDP_FLAG_SYN | XDP_FLAG_FIN | XDP_FLAG_RST */
    __u8  pad;          /* alignment */
    __u8  data[XDP_MAX_PAYLOAD];
};

#else

/* User-space side: use stdint types */
#include <stdint.h>

struct xdp_repl_event {
    uint32_t saddr;
    uint32_t daddr;
    uint16_t sport;
    uint16_t dport;
    uint32_t seq;
    uint16_t data_len;
    uint8_t  flags;
    uint8_t  pad;
    uint8_t  data[XDP_MAX_PAYLOAD];
};

#endif /* __BPF__ */

#endif /* REPL_XDP_COMMON_H */
