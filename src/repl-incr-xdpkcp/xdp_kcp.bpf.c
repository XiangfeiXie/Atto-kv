/* xdp_kcp.bpf.c — XDP program: redirect UDP:sync_port frames into the AF_XDP
 * socket via the XSKMAP.  All other traffic is passed through untouched.
 *
 * This is the transport-side use of XDP (XDP_REDIRECT to XSK), in contrast
 * to repl-incr-xdptracepoint's observability-side XDP_PASS + ringbuf mirror.
 *
 * The XSKMAP is keyed by rx_queue_index; user space inserts the AF_XDP socket
 * fd under the queue it bound to (queue 0).  If no socket is bound for this
 * queue, bpf_redirect_map falls back to XDP_PASS (default action), so a
 * gateway that hasn't bound yet simply leaves the traffic on the normal path.
 */

#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/udp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

/* UDP destination port both gateways exchange KCP datagrams on.  Override at
 * compile time with -DKCP_SYNC_PORT=<n> if the default conflicts. */
#ifndef KCP_SYNC_PORT
#define KCP_SYNC_PORT 8888
#endif

struct {
    __uint(type, BPF_MAP_TYPE_XSKMAP);
    __uint(max_entries, 64);
    __uint(key_size, sizeof(__u32));
    __uint(value_size, sizeof(__u32));
} xsks_map SEC(".maps");

SEC("xdp")
int xdp_kcp_prog(struct xdp_md *ctx)
{
    void *data     = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;

    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end)
        return XDP_PASS;

    if (eth->h_proto != bpf_htons(ETH_P_IP))
        return XDP_PASS;

    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end)
        return XDP_PASS;
    if (ip->protocol != 17)          /* IPPROTO_UDP */
        return XDP_PASS;

    int ihl = ip->ihl * 4;
    if (ihl < (int)sizeof(struct iphdr))
        return XDP_PASS;

    struct udphdr *udp = (void *)((char *)ip + ihl);
    if ((void *)(udp + 1) > data_end)
        return XDP_PASS;

    if (udp->dest != bpf_htons(KCP_SYNC_PORT))
        return XDP_PASS;

    /* Route to the XSK bound to this NIC queue; XDP_PASS if none. */
    return bpf_redirect_map(&xsks_map, ctx->rx_queue_index, XDP_PASS);
}

char _license[] SEC("license") = "GPL";
