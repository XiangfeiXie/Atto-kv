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

/* Event flags */
#define REPL_FLAG_FIRST  0x01   /* First skb of a command */
#define REPL_FLAG_LAST   0x02   /* Last skb of a command */

/* BPF programs use kernel types directly, userspace uses stdint.h */
#ifdef __BPF__

/* Replication event structure (BPF side) */
struct repl_observe_event {
    __u64 sock_cookie;  /* Socket cookie for connection tracking */
    __u32 len;          /* Length of data in this segment */
    __u32 offset;       /* Offset in complete command */
    __u8  flags;        /* REPL_FLAG_FIRST | REPL_FLAG_LAST */
    __u8  padding[3];   /* Padding for alignment */
    __u8  data[REPL_OBSERVE_MAX_DATA];  /* Command data */
};

#endif /* __BPF__ */

#endif /* __REPL_OBSERVE_H__ */
