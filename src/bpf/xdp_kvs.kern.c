/* xdp_kvs.kern.c — XDP passive packet monitor for kvstore GET traffic
 *
 * Parse incoming TCP packets on the configured port, identify RESP GET
 * commands, and record GET/skip/bypass counters in BPF maps.  It always
 * returns XDP_PASS, so the kernel TCP stack and kvstore handle every request.
 *
 * Compile:
 *   clang -O2 -target bpf -I/usr/include -I/usr/include/x86_64-linux-gnu \
 *         -c xdp_kvs.kern.c -o xdp_kvs.kern.o
 */

#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <linux/in.h>
#include "xdp_kvs.h"

/* ── BPF Maps ─────────────────────────────────────────────────────────── */

/* Config: single-entry map so userspace can tell us which port to watch */
struct {
    __uint(type,        BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key,         __u32);
    __type(value,       __u32);   /* host-byte-order port number */
} config_map SEC(".maps");

struct {
    __uint(type,        BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 4);
    __type(key,         __u32);
    __type(value,       __u64);
} stats_map SEC(".maps");

/* ── Helpers ──────────────────────────────────────────────────────────── */

static __always_inline void stat_inc(__u32 idx) {
    __u64 *cnt = bpf_map_lookup_elem(&stats_map, &idx);
    if (cnt) __sync_fetch_and_add(cnt, 1);
}

/* ── RESP GET Parser ──────────────────────────────────────────────────── */
/*
 * Match: *2\r\n$3\r\nGET\r\n$<N>\r\n<key>\r\n
 */
static __always_inline int parse_resp_get(const char *p, const char *end)
{
    /* *2\r\n$3\r\nGET\r\n — 13 fixed bytes */
    if (p + 13 > end) return 0;
    if (p[0]  != '*' || p[1]  != '2' || p[2]  != '\r' || p[3]  != '\n') return 0;
    if (p[4]  != '$' || p[5]  != '3' || p[6]  != '\r' || p[7]  != '\n') return 0;
    if ((p[8]  != 'G' && p[8]  != 'g') ||
        (p[9]  != 'E' && p[9]  != 'e') ||
        (p[10] != 'T' && p[10] != 't') ||
        p[11] != '\r' || p[12] != '\n') return 0;
    p += 13;

    /* $<N>\r\n  — key length prefix (1 or 2 digit length) */
    if (p + 2 > end || p[0] != '$') return 0;
    p++;

    int klen = 0;
    if (p + 1 > end || p[0] < '0' || p[0] > '9') return 0;
    klen = p[0] - '0';
    p++;
    if (p + 1 > end) return 0;
    if (p[0] >= '0' && p[0] <= '9') { klen = klen * 10 + (p[0] - '0'); p++; }

    if (klen <= 0 || klen > XDP_PARSE_KEY_MAX) return 0;
    if (p + 2 > end || p[0] != '\r' || p[1] != '\n') return 0;
    p += 2;

    if (p + klen + 2 > end) return 0;

    if (p[klen] != '\r' || p[klen + 1] != '\n') return 0;
    return klen;
}

/* ── XDP Entry Point ──────────────────────────────────────────────────── */

SEC("xdp")
int xdp_kvs_handler(struct xdp_md *ctx)
{
    void *data     = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;

    /* ── L2: Ethernet ── */
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end) return XDP_PASS;
    if (eth->h_proto != bpf_htons(ETH_P_IP)) {
        stat_inc(XDP_STAT_BYPASS);
        return XDP_PASS;
    }

    /* ── L3: IPv4 ── */
    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end) return XDP_PASS;
    if (ip->protocol != IPPROTO_TCP || ip->ihl < 5) {
        stat_inc(XDP_STAT_BYPASS);
        return XDP_PASS;
    }

    /* ── L4: TCP ── */
    struct tcphdr *tcp = (void *)ip + ip->ihl * 4;
    if ((void *)(tcp + 1) > data_end) return XDP_PASS;
    if (tcp->doff < 5) {
        stat_inc(XDP_STAT_BYPASS);
        return XDP_PASS;
    }

    /* Filter to configured service port */
    __u32 cfg_key = 0;
    __u32 *cfg_port = bpf_map_lookup_elem(&config_map, &cfg_key);
    if (!cfg_port || *cfg_port == 0 ||
        bpf_ntohs(tcp->dest) != (__u16)*cfg_port) {
        stat_inc(XDP_STAT_BYPASS);
        return XDP_PASS;
    }

    /* Skip control segments with no payload */
    char *payload = (char *)tcp + tcp->doff * 4;
    if ((void *)payload >= data_end) return XDP_PASS;

    /* O(1) shortcut for pipelined batches: a single
     * "*2\r\n$3\r\nGET\r\n$<N>\r\n<key>\r\n" is at most
     * 19 + XDP_PARSE_KEY_MAX (=64) + 1 (extra digit) = 84 bytes.
     * Anything larger must be multi-command — skip the parse. */
    if ((char *)data_end - payload > (long)(20 + XDP_PARSE_KEY_MAX)) {
        stat_inc(XDP_STAT_BYPASS);
        return XDP_PASS;
    }

    /* Parse RESP GET */
    int klen = parse_resp_get(payload, (char *)data_end);
    if (klen == 0) {
        stat_inc(XDP_STAT_PARSE_SKIP);
        return XDP_PASS;
    }

    /* Detect pipelined GETs in this segment (consumed < segment size) */
    {
        __u32 consumed = 19u + (__u32)klen + ((__u32)klen >= 10u ? 1u : 0u);
        const char *next = payload + consumed;
        if ((void *)next < data_end) {
            stat_inc(XDP_STAT_PARSE_SKIP);
            return XDP_PASS;
        }
    }

    stat_inc(XDP_STAT_GET);
    return XDP_PASS;
}

char _license[] SEC("license") = "GPL";
