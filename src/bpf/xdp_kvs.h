/* xdp_kvs.h — shared between XDP kernel program and userspace manager */
#ifndef XDP_KVS_H
#define XDP_KVS_H

/* Both BPF kernel code and userspace C code use <linux/types.h> for
 * __u8/__u16/__u32/__u64.  In BPF context these come in via bpf_helpers.h;
 * in userspace they are available directly from the kernel headers. */
#ifndef __bpf__
# include <linux/types.h>
#endif

#define XDP_PARSE_KEY_MAX 64

/* Stats map keys */
#define XDP_STAT_GET        0
#define XDP_STAT_PARSE_SKIP 1
#define XDP_STAT_BYPASS     2
#define XDP_STAT_RESERVED   3

#endif /* XDP_KVS_H */
