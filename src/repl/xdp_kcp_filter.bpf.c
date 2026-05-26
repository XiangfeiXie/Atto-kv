// SPDX-License-Identifier: GPL-2.0
/* XDP program for KCP replication packet filtering
 *
 * Purpose: Intercept KCP replication packets on slave node and redirect
 *          them to AF_XDP socket for zero-copy processing.
 *
 * Usage:
 *   - Loaded only on SLAVE nodes
 *   - Filters UDP packets destined to KCP replication port (6484)
 *   - Redirects matched packets to AF_XDP socket via xsks_map
 *   - Other traffic passes through normal kernel stack
 */

#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/udp.h>
#include <linux/in.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

/* BPF map for AF_XDP sockets
 * Key: queue_id (RX queue index)
 * Value: AF_XDP socket fd
 */
struct {
    __uint(type, BPF_MAP_TYPE_XSKMAP);
    __uint(max_entries, 64);  // Support up to 64 RX queues
    __uint(key_size, sizeof(__u32));
    __uint(value_size, sizeof(__u32));
} xsks_map SEC(".maps");

/* Helper function to parse Ethernet header */
static __always_inline struct ethhdr *
parse_ethhdr(void *data, void *data_end)
{
    struct ethhdr *eth = data;

    if ((void *)(eth + 1) > data_end)
        return NULL;

    return eth;
}

/* Helper function to parse IP header */
static __always_inline struct iphdr *
parse_iphdr(void *data, void *data_end, struct ethhdr *eth)
{
    struct iphdr *ip = (void *)(eth + 1);

    if ((void *)(ip + 1) > data_end)
        return NULL;

    // Only handle IPv4
    if (ip->version != 4)
        return NULL;

    return ip;
}

/* Helper function to parse UDP header */
static __always_inline struct udphdr *
parse_udphdr(void *data, void *data_end, struct iphdr *ip)
{
    // IP header length is variable (options)
    __u32 ip_hdr_len = ip->ihl * 4;
    struct udphdr *udp = (void *)ip + ip_hdr_len;

    if ((void *)(udp + 1) > data_end)
        return NULL;

    return udp;
}

/* Main XDP program
 *
 * Logic:
 *   1. Parse Ethernet/IP/UDP headers
 *   2. Check if UDP destination port is 6484 (slave KCP port)
 *   3. If matched: redirect to AF_XDP socket
 *   4. Otherwise: pass to kernel stack
 */
SEC("xdp")
int xdp_kcp_filter_prog(struct xdp_md *ctx)
{
    void *data_end = (void *)(long)ctx->data_end;
    void *data = (void *)(long)ctx->data;

    // Parse Ethernet header
    struct ethhdr *eth = parse_ethhdr(data, data_end);
    if (!eth)
        return XDP_PASS;

    // Only handle IP packets
    if (eth->h_proto != bpf_htons(ETH_P_IP))
        return XDP_PASS;

    // Parse IP header
    struct iphdr *ip = parse_iphdr(data, data_end, eth);
    if (!ip)
        return XDP_PASS;

    // Only handle UDP packets
    if (ip->protocol != IPPROTO_UDP)
        return XDP_PASS;

    // Parse UDP header
    struct udphdr *udp = parse_udphdr(data, data_end, ip);
    if (!udp)
        return XDP_PASS;

    // Check if this is a KCP replication packet
    // Port 6484 is the slave's listening port for replication data
    __u16 dest_port = bpf_ntohs(udp->dest);
    if (dest_port == 6484) {
        // Redirect to AF_XDP socket
        // Use rx_queue_index as the map key
        return bpf_redirect_map(&xsks_map, ctx->rx_queue_index, 0);
    }

    // Not a KCP replication packet, pass to kernel stack
    return XDP_PASS;
}

char _license[] SEC("license") = "GPL";
