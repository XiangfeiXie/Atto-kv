#ifndef __REPL_OBSERVE_H__
#define __REPL_OBSERVE_H__

/* Use BPF-compatible types */
#ifdef __BPF__
typedef unsigned int   __u32;
typedef unsigned char  __u8;
#else
#include <stdint.h>
typedef uint32_t __u32;
typedef uint8_t  __u8;
#endif

/* Maximum number of sockets to track */
#define REPL_OBSERVE_MAX_SOCKS  1024

/* Ringbuf size for replication events (2MB) */
#define REPL_OBSERVE_RINGBUF_SZ (2 * 1024 * 1024)

/* Maximum data size per event (4KB) */
#define REPL_OBSERVE_MAX_DATA   4096

/* Replication event structure */
struct repl_observe_event {
    __u64 sock_cookie;  /* Socket cookie for connection tracking */
    __u32 len;          /* Length of data */
    __u32 padding;      /* Padding for alignment */
    __u8  data[REPL_OBSERVE_MAX_DATA];  /* Command data */
};

#endif /* __REPL_OBSERVE_H__ */
