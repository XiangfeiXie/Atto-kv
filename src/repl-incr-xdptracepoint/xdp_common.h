/* xdp_common.h — Shared structures between XDP BPF and user-space
 *
 * This file defines the ringbuf event format and constants used by both
 * the BPF XDP program (xdp_filter.bpf.c) and the user-space daemon
 * (stream.c / replay.c).
 *
 * The event uses a flexible-array payload (`data[]`).  bpf_ringbuf_reserve()
 * requires a compile-time-constant size on this kernel, so the BPF program
 * reserves a fixed `sizeof(header) + XDP_MAX_PAYLOAD` slot per event and
 * only fills `data_len` bytes; the consumer reads `data_len` and ignores
 * the trailing padding.
 */

#ifndef XDP_COMMON_H
#define XDP_COMMON_H

/* ── Constants ────────────────────────────────────────────────────────── */

/* Absolute ceiling on the TCP payload we will ever capture in one event.
 * 65536 covers loopback's 64KB MTU and IPv4 GRO (which is capped at 64KB).
 4096 native */
#define XDP_MAX_PAYLOAD     65536

/* Bytes reserved per ringbuf event.  This — not XDP_MAX_PAYLOAD — is what
 * determines ringbuf capacity:
 *     XDP_RINGBUF_SZ / (sizeof(header) + XDP_RESERVE_PAYLOAD)
 *
 * bpf_ringbuf_reserve() requires a compile-time-constant size, so reserve
 * matches the deployment: on a real NIC in native-XDP mode (wire segments
 * ~1460 bytes) set this to 4096 to hold ~8x more events; on loopback /
 * generic-XDP the segments can reach 64KB so it must stay at 65536.
 * Must be <= XDP_MAX_PAYLOAD. */
#ifndef XDP_RESERVE_PAYLOAD
#define XDP_RESERVE_PAYLOAD XDP_MAX_PAYLOAD
#endif

/* Effective capture ceiling: we can never store more than we reserve. */
#define XDP_CAPTURE_PAYLOAD \
    ((XDP_MAX_PAYLOAD) < (XDP_RESERVE_PAYLOAD) ? (XDP_MAX_PAYLOAD) : (XDP_RESERVE_PAYLOAD))

/* Ringbuf size. */
#define XDP_RINGBUF_SZ      (16 * 1024 * 1024)

/* Flags for xdp_repl_event.flags */
#define XDP_FLAG_SYN        0x01
#define XDP_FLAG_FIN        0x02
#define XDP_FLAG_RST        0x04
#define XDP_FLAG_FRAG       0x08    /* IPv4 first-fragment (MF=1) */

/* BPF stats counter indices — shared between BPF program and userspace */
enum {
    XDP_CNT_SUBMITTED = 0,
    XDP_CNT_RINGBUF_FULL,
    XDP_CNT_PROBE_FAIL,
    XDP_CNT_TOTAL,
    XDP_CNT_NUM
};

/* ── Ringbuf event ────────────────────────────────────────────────────── */

#ifdef __BPF__

struct xdp_repl_event {
    __u32 saddr;        /* source IPv4 address  (network byte order) */
    __u32 daddr;        /* dest IPv4 address    (network byte order) */
    __u16 sport;        /* source TCP port      (network byte order) */
    __u16 dport;        /* dest TCP port        (network byte order) */
    __u32 seq;          /* TCP sequence number  (network byte order) */
    __u16 data_len;     /* actual payload length (0..XDP_MAX_PAYLOAD) */
    __u8  flags;        /* XDP_FLAG_SYN | FIN | RST | FRAG */
    __u8  pad;          /* alignment */
    __u8  data[];       /* variable-length payload (data_len bytes) */
};

#else

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
    uint8_t  data[];
};

#endif /* __BPF__ */

#endif /* XDP_COMMON_H */
