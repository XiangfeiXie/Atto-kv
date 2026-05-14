/* xdp_kvs.h — shared between XDP kernel program and userspace manager */
#ifndef XDP_KVS_H
#define XDP_KVS_H

/* Both BPF kernel code and userspace C code use <linux/types.h> for
 * __u8/__u16/__u32/__u64.  In BPF context these come in via bpf_helpers.h;
 * in userspace they are available directly from the kernel headers. */
#ifndef __bpf__
# include <linux/types.h>
#endif

/* Maximum key / value sizes cached in the BPF LRU map.
 * Keys longer than XDP_KEY_MAX or values longer than XDP_VAL_MAX
 * are NOT cached; those requests fall through to the kvstore process. */
#define XDP_KEY_MAX   64
#define XDP_VAL_MAX  256

/* BPF map capacity: number of hot-key slots.
 * LRU eviction handles overflow automatically. */
#define XDP_CACHE_MAX_ENTRIES  65536

/* Stats map keys */
#define XDP_STAT_HITS     0
#define XDP_STAT_MISSES   1
#define XDP_STAT_TX_DROP  2   /* cache hit but XDP_TX impossible (e.g. IPv4 opts) */
#define XDP_STAT_BYPASS   3   /* packets not targeting our port / not a GET */

struct xdp_kvs_key {
    __u16 len;
    __u8  data[XDP_KEY_MAX];
};

struct xdp_kvs_val {
    __u16 len;
    __u8  data[XDP_VAL_MAX];
};

#endif /* XDP_KVS_H */
