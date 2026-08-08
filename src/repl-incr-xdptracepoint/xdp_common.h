/* xdp_common.h — Shared structures between XDP BPF and user-space
 *
 * This file defines the ringbuf event format and constants used by both
 * the BPF XDP program (xdp_filter.bpf.c) and the user-space
 * reassembly module (xdp_reasm.c).
 *
 * The #ifdef __BPF__ guard ensures each side sees the correct types:
 *   BPF side:    __u32, __u16, __u8 from bpf_helper_defs.h
 *   User-space:  uint32_t, uint16_t, uint8_t from stdint.h
 */

#ifndef XDP_COMMON_H
#define XDP_COMMON_H

/* ── Constants ────────────────────────────────────────────────────────── */

/* Max TCP payload per XDP event.
 * Standard MTU 1500 → max TCP payload ~1460 bytes.
 * With GRO in generic XDP mode: up to 64KB coalesced.
 * 4096 covers jumbo frames and moderate GRO coalescing. */
#define XDP_MAX_PAYLOAD     4096

/* Total event size: header (20) + payload (4096) = 4116 */
#define XDP_EVENT_SIZE      (20 + XDP_MAX_PAYLOAD)

/* Ringbuf size — 8 MB */
#define XDP_RINGBUF_SZ      (8 * 1024 * 1024)

/* Flags for xdp_repl_event.flags */
#define XDP_FLAG_SYN        0x01
#define XDP_FLAG_FIN        0x02
#define XDP_FLAG_RST        0x04
#define XDP_FLAG_FRAG       0x08    /* IPv4 fragment (MF=1 or offset>0) */

/* ── Ringbuf event ────────────────────────────────────────────────────── */

#ifdef __BPF__

/* BPF side: use __u32 etc from bpf_helper_defs.h */
struct xdp_repl_event {
    __u32 saddr;        /* source IPv4 address  (network byte order) */
    __u32 daddr;        /* dest IPv4 address    (network byte order) */
    __u16 sport;        /* source TCP port      (network byte order) */
    __u16 dport;        /* dest TCP port        (network byte order) */
    __u32 seq;          /* TCP sequence number  (network byte order) */
    __u16 data_len;     /* actual payload length (0..XDP_MAX_PAYLOAD) */
    __u8  flags;        /* XDP_FLAG_SYN | FIN | RST | FRAG */
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

#endif /* XDP_COMMON_H */
