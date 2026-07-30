/* repl_filter_xdp.bpf.c — XDP packet-level mirror for replication
 *
 * Attach point: network interface (XDP)
 * Function:     mirror TCP payloads destined for the kvstore port to ringbuf
 * Data path:    XDP_PASS (kernel TCP stack processes normally)
 *
 * This is a PURE OBSERVER — it copies data, never drops or modifies.
 * The kernel TCP stack handles retransmission, reordering, and
 * reassembly as usual.  User-space does seq-based dedup + ordering
 * and RESP command extraction.
 *
 * Header layout:
 *   [Ethernet:14] [IP:20-60] [TCP:20-60] [payload:0-1460]
 */

#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/in.h>         /* IPPROTO_TCP */
#include <linux/ip.h>
#include <linux/tcp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

#include "xdp_common.h"

char _license[] SEC("license") = "GPL";

/* ── BPF maps ────────────────────────────────────────────────────────── */

/* Port filter: repl_config[0] = kvstore TCP port (host byte order) */
struct {
    __uint(type,        BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key,         __u32);
    __type(value,       __u32);
} repl_config SEC(".maps");

/* Ringbuf for pushing mirrored packets to user-space.
 * user-space reads via ring_buffer__new() + ring_buffer__poll(). */
struct {
    __uint(type,        BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, XDP_RINGBUF_SZ);
} repl_ringbuf SEC(".maps");

/* ── XDP program ─────────────────────────────────────────────────────── */

SEC("xdp")
int xdp_repl_sniff(struct xdp_md *ctx)
{
    void *data     = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;

    /* ── 1. Ethernet header ──────────────────────────────────────────── */
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end)
        return XDP_PASS;

    /* IPv4 only */
    if (eth->h_proto != __constant_htons(ETH_P_IP))
        return XDP_PASS;

    /* ── 2. IP header ────────────────────────────────────────────────── */
    struct iphdr *iph = (void *)(eth + 1);
    if ((void *)(iph + 1) > data_end)
        return XDP_PASS;

    /* TCP only */
    if (iph->protocol != IPPROTO_TCP)
        return XDP_PASS;

    __u32 ip_hdr_len = iph->ihl * 4;
    if (ip_hdr_len < sizeof(struct iphdr))
        return XDP_PASS;

    /* ── 3. TCP header ───────────────────────────────────────────────── */
    struct tcphdr *tcph = (void *)iph + ip_hdr_len;
    if ((void *)(tcph + 1) > data_end)
        return XDP_PASS;

    __u32 key = 0;
    __u32 *cfg_port = bpf_map_lookup_elem(&repl_config, &key);
    if (!cfg_port || *cfg_port == 0)
        return XDP_PASS;

    if (tcph->dest != __constant_htons((__u16)*cfg_port))
        return XDP_PASS;

    /* Extract TCP header length from the doff/reserved field.
     * On little-endian, the bitfield layout is:
     *   res1:4, doff:4, fin:1, syn:1, rst:1, psh:1, ack:1, urg:1, ece:1, cwr:1
     * doff is the second nibble (bits 4-7 of the little-endian u16). */
    __u32 tcp_hdr_len = tcph->doff * 4;
    if (tcp_hdr_len < sizeof(struct tcphdr))
        return XDP_PASS;

    /* ── 4. Compute payload ──────────────────────────────────────────── */
    void *payload     = (void *)tcph + tcp_hdr_len;
    int   payload_len = (__u8 *)data_end - (__u8 *)payload;

    if (payload_len <= 0)
        return XDP_PASS;   /* pure ACK / control — nothing to mirror */

    /* Cap to max event payload size */
    if (payload_len > XDP_MAX_PAYLOAD)
        payload_len = XDP_MAX_PAYLOAD;

    /* ── 5. Reserve ringbuf slot ─────────────────────────────────────── */
    struct xdp_repl_event *e =
        bpf_ringbuf_reserve(&repl_ringbuf, sizeof(*e), 0);
    if (!e)
        return XDP_PASS;   /* ringbuf full — silently drop (observer only) */

    /* Fill metadata (network byte order from packet headers) */
    e->saddr    = iph->saddr;
    e->daddr    = iph->daddr;
    e->sport    = tcph->source;
    e->dport    = tcph->dest;
    e->seq      = tcph->seq;
    e->data_len = (__u16)payload_len;
    e->flags    = 0;
    e->pad      = 0;

    if (tcph->syn) e->flags |= XDP_FLAG_SYN;
    if (tcph->fin) e->flags |= XDP_FLAG_FIN;
    if (tcph->rst) e->flags |= XDP_FLAG_RST;

    /* Copy payload via kernel helper (packet data is kernel memory).
     * payload_len is bounded to [1, XDP_MAX_PAYLOAD] — verifier-safe. */
    if (payload_len > 0) {
        if (bpf_probe_read_kernel(e->data, (__u32)payload_len, payload) != 0) {
            bpf_ringbuf_discard(e, 0);
            return XDP_PASS;
        }
    }

    bpf_ringbuf_submit(e, 0);
    return XDP_PASS;
}
