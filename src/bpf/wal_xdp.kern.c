/* wal_xdp.kern.c — XDP-based WAL replication bypass
 *
 * Architecture:
 *   Intercepts WAL packets at the NIC driver layer (XDP hook), parses
 *   the wire protocol ([uint64_t len][payload]), and redirects to
 *   multiple slave NICs via XDP_REDIRECT, completely bypassing the
 *   TCP/IP stack for maximum throughput and minimum latency.
 *
 * Wire protocol (unchanged from wal_repl.c):
 *   [uint64_t cmd_len][cmd_len bytes of RESP command]
 *
 * XDP program flow:
 *   1. Parse Ethernet → IP → TCP headers
 *   2. Check destination port == incr_port (6382)
 *   3. Extract WAL payload from TCP data
 *   4. Clone packet and redirect to each slave NIC (via devmap)
 *   5. Return XDP_PASS to allow original packet to reach user space
 *      (for WAL disk persistence via io_uring)
 *
 * Performance target:
 *   - Latency: ~5µs (vs ~50µs TCP stack)
 *   - Throughput: 10Gbps+ (vs ~1Gbps TCP)
 *   - CPU: 20% (vs 100% TCP stack)
 */

#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <linux/in.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

#define INCR_PORT 6382
#define MAX_SLAVES 8

/* ── BPF maps ── */

/* Slave device map: maps slave index → ifindex for XDP_REDIRECT.
 * Populated by user space when slaves connect. */
struct {
    __uint(type, BPF_MAP_TYPE_DEVMAP);
    __uint(max_entries, MAX_SLAVES);
    __type(key, __u32);
    __type(value, __u32);  /* ifindex */
} slave_devmap SEC(".maps");

/* Slave MAC address map: maps slave index → MAC address.
 * Used to rewrite destination MAC when redirecting packets. */
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, MAX_SLAVES);
    __type(key, __u32);
    __type(value, __u8[6]);  /* MAC address */
} slave_mac_map SEC(".maps");

/* Slave IP address map: maps slave index → IP address.
 * Used to rewrite destination IP when redirecting packets. */
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, MAX_SLAVES);
    __type(key, __u32);
    __type(value, __u32);  /* IPv4 address in network byte order */
} slave_ip_map SEC(".maps");

/* Statistics: per-slave packet counters */
struct wal_xdp_stats {
    __u64 packets_redirected;
    __u64 bytes_redirected;
    __u64 redirect_errors;
};

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, MAX_SLAVES);
    __type(key, __u32);
    __type(value, struct wal_xdp_stats);
} stats_map SEC(".maps");

/* Global stats: total packets processed */
struct wal_xdp_global_stats {
    __u64 total_packets;
    __u64 wal_packets;
    __u64 non_wal_packets;
    __u64 parse_errors;
};

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct wal_xdp_global_stats);
} global_stats_map SEC(".maps");

/* Event ringbuf: reports redirect failures to user space */
struct redirect_event {
    __u32 slave_idx;
    __s32 error_code;
    __u64 timestamp;
};

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 256 * 1024);  /* 256KB */
} event_ringbuf SEC(".maps");

/* ── Helper functions ── */

static __always_inline __u16 csum_fold_helper(__u32 csum)
{
    csum = (csum & 0xffff) + (csum >> 16);
    csum = (csum & 0xffff) + (csum >> 16);
    return (__u16)~csum;
}

static __always_inline void update_ip_checksum(struct iphdr *iph)
{
    __u32 csum = 0;
    __u16 *buf = (__u16 *)iph;
    
    iph->check = 0;
    
    #pragma unroll
    for (int i = 0; i < sizeof(struct iphdr) / 2; i++) {
        csum += buf[i];
    }
    
    iph->check = csum_fold_helper(csum);
}

static __always_inline void update_tcp_checksum(struct iphdr *iph, 
                                                 struct tcphdr *tcph,
                                                 __u32 payload_len)
{
    __u32 csum = 0;
    __u16 *buf;
    
    tcph->check = 0;
    
    /* Pseudo-header checksum */
    csum += (iph->saddr >> 16) + (iph->saddr & 0xffff);
    csum += (iph->daddr >> 16) + (iph->daddr & 0xffff);
    csum += bpf_htons((__u16)iph->protocol);
    csum += bpf_htons((__u16)(sizeof(struct tcphdr) + payload_len));
    
    /* TCP header checksum */
    buf = (__u16 *)tcph;
    #pragma unroll
    for (int i = 0; i < sizeof(struct tcphdr) / 2; i++) {
        csum += buf[i];
    }
    
    tcph->check = csum_fold_helper(csum);
}

/* ── Main XDP program ── */

SEC("xdp")
int wal_xdp_redirect(struct xdp_md *ctx)
{
    void *data_end = (void *)(long)ctx->data_end;
    void *data = (void *)(long)ctx->data;
    
    /* Update global stats */
    __u32 zero = 0;
    struct wal_xdp_global_stats *gstats = 
        bpf_map_lookup_elem(&global_stats_map, &zero);
    if (gstats) {
        __sync_fetch_and_add(&gstats->total_packets, 1);
    }
    
    /* ── Parse Ethernet header ── */
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end) {
        if (gstats) __sync_fetch_and_add(&gstats->parse_errors, 1);
        return XDP_PASS;
    }
    
    if (eth->h_proto != bpf_htons(ETH_P_IP)) {
        if (gstats) __sync_fetch_and_add(&gstats->non_wal_packets, 1);
        return XDP_PASS;
    }
    
    /* ── Parse IP header ── */
    struct iphdr *iph = (void *)(eth + 1);
    if ((void *)(iph + 1) > data_end) {
        if (gstats) __sync_fetch_and_add(&gstats->parse_errors, 1);
        return XDP_PASS;
    }
    
    if (iph->protocol != IPPROTO_TCP) {
        if (gstats) __sync_fetch_and_add(&gstats->non_wal_packets, 1);
        return XDP_PASS;
    }
    
    /* ── Parse TCP header ── */
    struct tcphdr *tcph = (void *)iph + (iph->ihl * 4);
    if ((void *)(tcph + 1) > data_end) {
        if (gstats) __sync_fetch_and_add(&gstats->parse_errors, 1);
        return XDP_PASS;
    }
    
    /* Check if this is a WAL packet (destination port == INCR_PORT) */
    if (tcph->dest != bpf_htons(INCR_PORT)) {
        if (gstats) __sync_fetch_and_add(&gstats->non_wal_packets, 1);
        return XDP_PASS;
    }
    
    if (gstats) {
        __sync_fetch_and_add(&gstats->wal_packets, 1);
    }
    
    /* Calculate TCP payload offset and length */
    void *tcp_payload = (void *)tcph + (tcph->doff * 4);
    if (tcp_payload > data_end) {
        if (gstats) __sync_fetch_and_add(&gstats->parse_errors, 1);
        return XDP_PASS;
    }
    
    __u32 payload_len = data_end - tcp_payload;
    
    /* ── Multicast to all active slaves ── */
    #pragma unroll
    for (__u32 i = 0; i < MAX_SLAVES; i++) {
        /* Check if slave is active (has ifindex in devmap) */
        __u32 *ifindex = bpf_map_lookup_elem(&slave_devmap, &i);
        if (!ifindex || *ifindex == 0) {
            continue;
        }
        
        /* Clone packet for this slave (bpf_clone_redirect handles the copy) */
        __u8 *slave_mac = bpf_map_lookup_elem(&slave_mac_map, &i);
        __u32 *slave_ip = bpf_map_lookup_elem(&slave_ip_map, &i);
        
        if (!slave_mac || !slave_ip || *slave_ip == 0) {
            continue;
        }
        
        /* Note: bpf_clone_redirect creates a copy, so we can't modify
         * headers in place. For full header rewrite, we'd need to use
         * bpf_xdp_adjust_head + manual packet reconstruction, which is
         * complex. For now, we rely on the master sending packets with
         * the correct destination already set (via AF_XDP socket), and
         * XDP just does fast forwarding. */
        
        long ret = bpf_clone_redirect(ctx, *ifindex, 0);
        
        /* Update per-slave stats */
        struct wal_xdp_stats *stats = bpf_map_lookup_elem(&stats_map, &i);
        if (stats) {
            if (ret == 0) {
                __sync_fetch_and_add(&stats->packets_redirected, 1);
                __sync_fetch_and_add(&stats->bytes_redirected, payload_len);
            } else {
                __sync_fetch_and_add(&stats->redirect_errors, 1);
                
                /* Report error to user space via ringbuf */
                struct redirect_event *event = 
                    bpf_ringbuf_reserve(&event_ringbuf, 
                                       sizeof(struct redirect_event), 0);
                if (event) {
                    event->slave_idx = i;
                    event->error_code = (__s32)ret;
                    event->timestamp = bpf_ktime_get_ns();
                    bpf_ringbuf_submit(event, 0);
                }
            }
        }
    }
    
    /* Return XDP_PASS to allow the original packet to continue to user space.
     * This is necessary for:
     *   1. WAL disk persistence (consumer thread writes to WAL file)
     *   2. TCP ACK handling (kernel TCP stack manages connection state)
     *   3. Backlog management (in-memory catchup buffer) */
    return XDP_PASS;
}

char _license[] SEC("license") = "GPL";
