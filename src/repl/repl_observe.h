#ifndef __REPL_OBSERVE_H__
#define __REPL_OBSERVE_H__

/* Maximum number of sockets to track */
#define REPL_OBSERVE_MAX_SOCKS  1024

/* Ringbuf size for replication events (8MB for better buffer capacity) */
#define REPL_OBSERVE_RINGBUF_SZ (8 * 1024 * 1024)

/* Maximum data size per event (4KB) */
#define REPL_OBSERVE_MAX_DATA   4096

/* Maximum command length (4MB, covers 99.9% cases) */
#define REPL_OBSERVE_MAX_CMD    (4 * 1024 * 1024)

/* BPF programs use kernel types directly, userspace uses stdint.h */
#ifdef __BPF__

/* Replication event structure (BPF side)
 *
 * Each event holds one complete RESP command (no multi-segment reassembly).
 * Commands that don't fit in a single skb (>4KB or split across segments)
 * are handled by the main-process repl_notify path instead. */
struct repl_observe_event {
    __u64 sock_cookie;  /* Socket cookie for connection tracking */
    __u32 len;          /* Length of complete command data */
    __u32 pad;          /* Padding for 8-byte alignment */
    __u8  data[REPL_OBSERVE_MAX_DATA];  /* Complete command data */
};

#endif /* __BPF__ */

#endif /* __REPL_OBSERVE_H__ */
