/* repl_observe.bpf.c — eBPF socket-layer observation for replication.
 *
 * Attach to a cgroup to intercept application data on server sockets.
 * Copies every skb to a ring buffer, always returns SK_PASS.
 * Master receives data normally, completely unaware of replication.
 */
#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>
#include "repl_observe.h"

struct {
    __uint(type,        BPF_MAP_TYPE_SOCKHASH);
    __uint(max_entries, REPL_OBSERVE_MAX_SOCKS);
    __type(key,         __u64);
    __type(value,       __u64);
} repl_sockmap SEC(".maps");

struct {
    __uint(type,        BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key,         __u32);
    __type(value,       __u32);
} repl_config SEC(".maps");

struct {
    __uint(type,        BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, REPL_OBSERVE_RINGBUF_SZ);
} repl_ringbuf SEC(".maps");

/* ── sockops: auto-insert passive-established sockets ──────────── */

SEC("sockops")
int repl_observe_sockops(struct bpf_sock_ops *skops)
{
    if (skops->family != 2)  /* AF_INET */
        return 0;

    __u32 key = 0;
    __u32 *cfg_port = bpf_map_lookup_elem(&repl_config, &key);
    if (!cfg_port || *cfg_port == 0)
        return 0;

    if (skops->op == BPF_SOCK_OPS_PASSIVE_ESTABLISHED_CB) {
        if (skops->local_port != *cfg_port)
            return 0;
        __u64 cookie = bpf_get_socket_cookie(skops);
        bpf_sock_hash_update(skops, &repl_sockmap, &cookie, BPF_ANY);
    }
    return 0;
}

/* ── stream_parser: simple passthrough parser ──────────────────── */

SEC("sk_skb/stream_parser")
int repl_observe_parser(struct __sk_buff *skb)
{
    return skb->len;
}

/* ── stream_verdict: copy data to ring buffer, pass through ────── */

SEC("sk_skb/stream_verdict")
int repl_observe_verdict(struct __sk_buff *skb)
{
    __u32 len = skb->len;
    if (len == 0 || len > REPL_OBSERVE_MAX_DATA)
        return SK_PASS;

    /* allocate entry — keep total <= 4096 for ringbuf page fit */
    struct repl_observe_event *e =
        bpf_ringbuf_reserve(&repl_ringbuf, sizeof(*e) + REPL_OBSERVE_MAX_DATA, 0);
    if (!e)
        return SK_PASS;

    e->len     = len;
    e->padding = 0;

    if (bpf_skb_load_bytes(skb, 0, e->data, len) != 0) {
        bpf_ringbuf_discard(e, 0);
        return SK_PASS;
    }

    bpf_ringbuf_submit(e, 0);
    return SK_PASS;
}

char _license[] SEC("license") = "GPL";
