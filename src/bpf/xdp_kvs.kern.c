/* xdp_kvs.kern.c — XDP hot-path for kvstore GET commands
 *
 * Phase 1/2:
 *   Parse incoming TCP packets on the configured port, identify RESP GET
 *   commands, look up the key in hot_cache, and record hit/miss stats.
 *
 * Phase 3 (implemented):
 *   On hot-cache hit, craft the RESP bulk-string reply in-place and
 *   return XDP_TX, short-circuiting the kernel TCP stack (~2μs vs ~15μs).
 *   Constraints: standard IPv4 (ihl=5), any TCP header up to 60 bytes.
 *   Note: bypasses kernel TCP state machine — for demo/benchmark use;
 *   production deployments should pair with AF_XDP for full state mgmt.
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

/* Hot-key cache: populated/invalidated by userspace on every SET/DEL.
 * Phase 3 will use this for XDP_TX reply; Phase 1/2 uses it for stats. */
struct {
    __uint(type,        BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, XDP_CACHE_MAX_ENTRIES);
    __type(key,         struct xdp_kvs_key);
    __type(value,       struct xdp_kvs_val);
} hot_cache SEC(".maps");

/* Config: single-entry map so userspace can tell us which port to watch */
struct {
    __uint(type,        BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key,         __u32);
    __type(value,       __u32);   /* host-byte-order port number */
} config_map SEC(".maps");

/* Stats counters */
struct {
    __uint(type,        BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 4);
    __type(key,         __u32);
    __type(value,       __u64);
} stats_map SEC(".maps");

/* Per-CPU scratch buffer used to build the outbound RESP reply.
 * Sized for "$NNN\r\n" (≤6 B) + value (≤XDP_VAL_MAX) + "\r\n" (2 B). */
#define XDP_RESP_MAX  (8 + XDP_VAL_MAX + 2)
struct resp_buf {
    __u8  data[XDP_RESP_MAX];
    __u32 len;
};
struct {
    __uint(type,        BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key,         __u32);
    __type(value,       struct resp_buf);
} resp_scratch SEC(".maps");

/* ── Helpers ──────────────────────────────────────────────────────────── */

static __always_inline void stat_inc(__u32 idx) {
    __u64 *cnt = bpf_map_lookup_elem(&stats_map, &idx);
    if (cnt) __sync_fetch_and_add(cnt, 1);
}

/* One's-complement 32→16 fold for checksum computation */
static __always_inline __u16 csum_fold(__u32 s) {
    s = (s >> 16) + (s & 0xffff);
    s = (s >> 16) + (s & 0xffff);
    return ~(__u16)s;
}

/* IPv4 header checksum — caller must zero ip->check first; requires ihl=5 */
static __always_inline __u16 ip4_csum(struct iphdr *ip) {
    __u16 *h = (__u16 *)ip;
    __u32 s  = 0;
    s += h[0]; s += h[1]; s += h[2]; s += h[3]; s += h[4];
    s += h[5]; s += h[6]; s += h[7]; s += h[8]; s += h[9];
    return csum_fold(s);
}

/* ── RESP GET Parser ──────────────────────────────────────────────────── */
/*
 * Match: *2\r\n$3\r\nGET\r\n$<N>\r\n<key>\r\n
 * Returns key length on success (1..XDP_KEY_MAX), 0 otherwise.
 * Writes key bytes into dst (caller must zero it first).
 */
static __always_inline int parse_resp_get(const char *p, const char *end,
                                          __u8 *dst)
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
    /* Up to 2 digits (max key 64 bytes) */
    if (p + 1 > end || p[0] < '0' || p[0] > '9') return 0;
    klen = p[0] - '0';
    p++;
    if (p + 1 > end) return 0;
    if (p[0] >= '0' && p[0] <= '9') { klen = klen * 10 + (p[0] - '0'); p++; }

    if (klen <= 0 || klen > XDP_KEY_MAX) return 0;
    if (p + 2 > end || p[0] != '\r' || p[1] != '\n') return 0;
    p += 2;

    /* key bytes */
    if (p + klen + 2 > end) return 0;

    /* Direct bounded copy from packet data.
     * klen already validated: 1 <= klen <= XDP_KEY_MAX.
     * Use constant loop bound (XDP_KEY_MAX) with runtime break to satisfy
     * the BPF verifier, which cannot track `p + klen <= end` across
     * loop back-edges. */
    for (__u32 _i = 0; _i < XDP_KEY_MAX; _i++) {
        if (_i >= (__u32)klen) break;
        if (p + _i + 1 > end) break;
        dst[_i] = ((const __u8 *)p)[_i];
    }

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

    /* Check destination port against configured service port */
    __u32 cfg_key = 0;
    __u32 *cfg_port = bpf_map_lookup_elem(&config_map, &cfg_key);
    if (!cfg_port || *cfg_port == 0 ||
        bpf_ntohs(tcp->dest) != (__u16)*cfg_port) {
        stat_inc(XDP_STAT_BYPASS);
        return XDP_PASS;
    }

    /* Skip control packets (SYN/FIN/pure-ACK) with no payload */
    char *payload = (char *)tcp + tcp->doff * 4;
    if ((void *)payload >= data_end) return XDP_PASS;

    /* O(1) fast-bypass for pipelined requests:
     * a single "*2\r\n$3\r\nGET\r\n$<N>\r\n<key>\r\n" is at most
     * 19 + XDP_KEY_MAX (=64) + 1(2-digit) = 84 bytes.  If the TCP payload
     * exceeds this, it must contain multiple commands → bypass to kvstore
     * without paying the full parse cost. */
    if ((char *)data_end - payload > (long)(20 + XDP_KEY_MAX)) {
        stat_inc(XDP_STAT_BYPASS);
        return XDP_PASS;
    }

    /* ── Parse RESP GET ── */
    struct xdp_kvs_key map_key = {};
    int klen = parse_resp_get(payload, (char *)data_end, map_key.data);
    if (klen == 0) {
        /* Not a GET or key too large — bypass counter, pass normally */
        stat_inc(XDP_STAT_BYPASS);
        return XDP_PASS;
    }
    map_key.len = (__u16)klen;

    /* Detect pipelined requests (P>1): more than one RESP command in
     * this segment.  XDP can only emit one TX response per ingress
     * packet — if we serve only the first GET, the client hangs waiting
     * for the remaining responses.  Bypass to kvstore in this case.
     *
     * Bytes consumed by one "*2\r\n$3\r\nGET\r\n$<N>\r\n<key>\r\n":
     *   13 (fixed) + 3+ndigits (length prefix) + klen + 2 (\r\n)
     *   = 19+klen           (klen<10, ndigits=1)
     *   = 20+klen           (klen>=10, ndigits=2) */
    {
        __u32 consumed = 19u + (__u32)klen + ((__u32)klen >= 10u ? 1u : 0u);
        const char *next = payload + consumed;
        if ((void *)next < data_end) {
            /* Pipelined request — let kvstore handle the whole segment */
            stat_inc(XDP_STAT_BYPASS);
            return XDP_PASS;
        }
    }

    /* ── Hot-cache lookup ── */
    struct xdp_kvs_val *val = bpf_map_lookup_elem(&hot_cache, &map_key);
    if (!val) {
        stat_inc(XDP_STAT_MISSES);
        return XDP_PASS;
    }

    /* ── Phase 3: build RESP reply in-place and return XDP_TX ── */
    __u16 vlen = val->len;
    if (vlen == 0 || vlen > XDP_VAL_MAX ||
        ip->ihl != 5 || tcp->doff < 5 || tcp->doff > 15)
        goto tx_pass;

    {
        __u32 tcp_hdr_len = (__u32)tcp->doff * 4;

        /* Allocate per-CPU scratch to avoid large BPF stack usage */
        __u32 scratch_key = 0;
        struct resp_buf *scratch =
            bpf_map_lookup_elem(&resp_scratch, &scratch_key);
        if (!scratch) goto tx_pass;

        /* Build RESP bulk string "$<vlen>\r\n<value>\r\n" into scratch */
        int pfx;
        scratch->data[0] = '$';
        if (vlen >= 100) {
            scratch->data[1] = '0' + vlen / 100;
            scratch->data[2] = '0' + (vlen / 10) % 10;
            scratch->data[3] = '0' + vlen % 10;
            scratch->data[4] = '\r'; scratch->data[5] = '\n';
            pfx = 6;
        } else if (vlen >= 10) {
            scratch->data[1] = '0' + vlen / 10;
            scratch->data[2] = '0' + vlen % 10;
            scratch->data[3] = '\r'; scratch->data[4] = '\n';
            pfx = 5;
        } else {
            scratch->data[1] = '0' + vlen;
            scratch->data[2] = '\r'; scratch->data[3] = '\n';
            pfx = 4;
        }
        /* Copy value bytes: pfx ≤ 6, vi < XDP_VAL_MAX = 256,
         * so pfx+vi ≤ 261 < XDP_RESP_MAX = 266 — always within map value (vs=272). */
        for (__u32 vi = 0; vi < XDP_VAL_MAX; vi++) {
            if (vi >= vlen) break;
            scratch->data[(__u32)pfx + vi] = val->data[vi];
        }
        /* Re-read val->len from the map value after the copy loop to get a fresh
         * bounded range [0, u16_max].  The guard clamps it to [1, XDP_VAL_MAX] so
         * the verifier sees slen as a RANGE (not a precise constant), which lets it
         * use fixpoint analysis for STEP 3's copy loop instead of full unrolling
         * (full unrolling with a precise slen ≈ 264 exceeds the 1M insn limit). */
        __u16 vlen2 = val->len;
        if (vlen2 == 0 || vlen2 > XDP_VAL_MAX) goto tx_pass;
        __u32 trail = (__u32)pfx + (__u32)vlen2;
        if (trail + 1 < XDP_RESP_MAX) {
            scratch->data[trail]     = '\r';
            scratch->data[trail + 1] = '\n';
        }
        scratch->len = trail + 2;
        /* Save on stack BEFORE any BPF helper call — helpers reset
         * map-value register ranges in the verifier. */
        __u32 slen = scratch->len;

        int old_pkt_len = (int)((char *)data_end - (char *)data);
        int payload_off = (int)((char *)payload  - (char *)data);

        /* ── STEP 1: modify headers using Phase-1 pointers (still live) ──
         * Phase 1 established: tcp+1 <= data_end (r=54 for data pointer),
         * so tcp field accesses at offsets 0..19 are safe here. */

        /* Swap Ethernet src/dst */
        __u8 tmp_mac[ETH_ALEN];
        __builtin_memcpy(tmp_mac,       eth->h_dest,   ETH_ALEN);
        __builtin_memcpy(eth->h_dest,   eth->h_source, ETH_ALEN);
        __builtin_memcpy(eth->h_source, tmp_mac,       ETH_ALEN);

        /* Swap IP src/dst, update length, recompute checksum */
        __be32 tmp_ip = ip->saddr;
        ip->saddr    = ip->daddr;
        ip->daddr    = tmp_ip;
        ip->ttl      = 64;
        ip->tot_len  = bpf_htons((__u16)(20 + tcp_hdr_len + slen));
        ip->check    = 0;
        ip->check    = ip4_csum(ip);

        /* Swap TCP ports, fix seq/ack, set PSH+ACK */
        __be16 tmp_port = tcp->source;
        tcp->source     = tcp->dest;
        tcp->dest       = tmp_port;
        __u32 req_plen  = (__u32)(old_pkt_len - payload_off);
        __u32 old_seq_h = bpf_ntohl(tcp->seq);
        __u32 old_ack_h = bpf_ntohl(tcp->ack_seq);
        tcp->seq        = bpf_htonl(old_ack_h);
        tcp->ack_seq    = bpf_htonl(old_seq_h + req_plen);
        tcp->psh        = 1;
        tcp->ack        = 1;

        /* TCP checksum: pseudo-hdr + TCP hdr (original pkt) + RESP (scratch).
         * Computed before adjust_tail so original tcp pointer is usable. */
        tcp->check = 0;
        {
            __u32 tc = 0;
            __u16 seg_len = (__u16)(tcp_hdr_len + slen);

            /* IPv4 pseudo-header (12 bytes) */
            __u32 ph[3];
            ph[0] = (__u32)ip->saddr;
            ph[1] = (__u32)ip->daddr;
            ph[2] = bpf_htonl((__u32)(IPPROTO_TCP << 16) | seg_len);
            __u16 *q = (__u16 *)ph;
            tc += q[0]; tc += q[1]; tc += q[2];
            tc += q[3]; tc += q[4]; tc += q[5];

            /* TCP header (doff*4 bytes, at most 30 u16s) */
            q = (__u16 *)tcp;
            __u32 tcp_u16 = tcp_hdr_len >> 1;
            for (__u32 i = 0; i < 30; i++) {
                if (i >= tcp_u16) break;
                if ((void *)(q + i + 1) > data_end) break;
                tc += q[i];
            }

            /* RESP payload from scratch (only complete u16 pairs) */
            __u32 rlen = slen;
            q = (__u16 *)scratch->data;
            for (__u32 i = 0; i < XDP_RESP_MAX / 2; i++) {
                if (i * 2 + 2 > rlen) break;
                tc += q[i];
            }
            if (rlen & 1) {
                __u32 last = rlen - 1;
                if (last < XDP_RESP_MAX)
                    tc += (__u16)((__u8 *)scratch->data)[last];
            }

            tcp->check = csum_fold(tc);
        }

        /* ── STEP 2: resize packet tail ── */
        int delta = (payload_off + (int)slen) - old_pkt_len;
        /* On failure headers are already modified — drop, not pass */
        if (bpf_xdp_adjust_tail(ctx, delta)) return XDP_DROP;

        /* ── STEP 3: copy RESP payload into the (now-resized) packet ──
         * Do NOT reuse payload_off: pkt-pointer subtraction yields an
         * unbounded scalar in the verifier (scalar() with no range).
         * Re-position using constant + bounded-variable additions instead:
         *   data + 14 (eth) + 20 (ip, ihl=5) + tcp_hdr_len ([20..60]) */
        data     = (void *)(long)ctx->data;
        data_end = (void *)(long)ctx->data_end;
        {
            char *p  = (char *)data + 34;  /* eth(14) + ip(20); ihl==5 guaranteed */
            __u32 thl = tcp_hdr_len;        /* [20..60] — saved on stack before helper */
            if (thl < 20 || thl > 60) return XDP_DROP;  /* re-establish verifier bounds */
            p += thl;
            const __u8 *src = scratch->data;
            for (__u32 ri = 0; ri < XDP_RESP_MAX; ri++) {
                if (ri >= slen) break;
                if (p + 1 > (char *)data_end) break;
                *p = (char)*src;
                p++;
                src++;
            }
        }

        stat_inc(XDP_STAT_HITS);
        return XDP_TX;
    }

tx_pass:
    stat_inc(XDP_STAT_TX_DROP);
    return XDP_PASS;
}

char _license[] SEC("license") = "GPL";
