/* repl_observe.h — shared between BPF kernel program and userspace loader */
#ifndef REPL_OBSERVE_H
#define REPL_OBSERVE_H

#include <linux/types.h>

#define REPL_OBSERVE_MAX_DATA  4080  /* fit ringbuf entry in 4K page */
#define REPL_OBSERVE_MAX_SOCKS 65536
#define REPL_OBSERVE_RINGBUF_SZ (512 * 1024)

/* ring buffer event: 8-byte header followed by raw data.
 * Reserve sizeof(event) + data_len bytes from ring buffer. */
struct repl_observe_event {
    __u32 len;
    __u32 padding;
    __u8  data[];
};

#endif
