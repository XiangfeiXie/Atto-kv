#ifndef __REPL_OBSERVE_H__
#define __REPL_OBSERVE_H__

/* Maximum number of sockets to track in sockmap */
#define REPL_OBSERVE_MAX_SOCKS  1024

/* Ringbuf size (8 MB) */
#define REPL_OBSERVE_RINGBUF_SZ (8 * 1024 * 1024)

/* Max data per ringbuf event = one TCP segment (MSS ≤ 1460, 4096 is safe) */
#define REPL_OBSERVE_MAX_DATA   4096

/* Max total command length (4 MB — covers any realistic Redis command) */
#define REPL_OBSERVE_MAX_CMD    (4 * 1024 * 1024)

#ifdef __BPF__

/* Minimal ringbuf event: just raw bytes from one TCP segment.
 * eBPF does NO protocol parsing — user-space handles reassembly + filtering. */
struct repl_observe_event {
    __u64 sock_cookie;
    __u32 len;
    __u32 pad;
    __u8  data[REPL_OBSERVE_MAX_DATA];
};

#endif /* __BPF__ */

#endif /* __REPL_OBSERVE_H__ */
