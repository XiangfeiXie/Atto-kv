/* repl_observe.h — shared between BPF kernel program and userspace loader */
#ifndef REPL_OBSERVE_H
#define REPL_OBSERVE_H

#include <linux/types.h>

#define REPL_OBSERVE_MAX_DATA  512   /* reduced from 4080 to 512 for better performance */
#define REPL_OBSERVE_MAX_SOCKS 65536
#define REPL_OBSERVE_RINGBUF_SZ (2 * 1024 * 1024)  /* increased from 512KB to 2MB */

/* ring buffer event: fixed-size structure for eBPF verifier.
 * Total size: 8 + 4080 = 4088 bytes (fits in 4K page). */
struct repl_observe_event {
    __u32 len;
    __u32 padding;
    __u8  data[REPL_OBSERVE_MAX_DATA];
};

#endif
