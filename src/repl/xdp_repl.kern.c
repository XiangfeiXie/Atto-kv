#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/udp.h>

#define IPPROTO_UDP 17
#define KVS_REPL_FRAME_MAGIC 0x5852504cU

struct {
    __uint(type, BPF_MAP_TYPE_XSKMAP);
    __uint(max_entries, 64);
    __type(key, __u32);
    __type(value, __u32);
} kvs_repl_xsks SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u16);
} kvs_repl_port SEC(".maps");

SEC("xdp")
int xdp_kvs_repl(struct xdp_md *ctx)
{
    void *data = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end) return XDP_PASS;
    if (eth->h_proto != __builtin_bswap16(ETH_P_IP)) return XDP_PASS;

    struct iphdr *iph = (void *)(eth + 1);
    if ((void *)(iph + 1) > data_end) return XDP_PASS;
    if (iph->protocol != IPPROTO_UDP) return XDP_PASS;

    struct udphdr *udp = (void *)iph + iph->ihl * 4;
    if ((void *)(udp + 1) > data_end) return XDP_PASS;

    __u32 key = 0;
    __u16 *port = bpf_map_lookup_elem(&kvs_repl_port, &key);
    if (!port || udp->dest != __builtin_bswap16(*port)) return XDP_PASS;

    __u32 rxq = ctx->rx_queue_index;
    return bpf_redirect_map(&kvs_repl_xsks, rxq, XDP_PASS);
}

char _license[] SEC("license") = "GPL";
