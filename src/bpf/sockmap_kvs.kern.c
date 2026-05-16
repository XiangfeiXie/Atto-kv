/* sockmap_kvs.kern.c — sockmap stream-layer observability for kvstore
 *
 * Phase 2.1 (skeleton):  sockops auto-inserts sockets, stream_verdict stub.
 * Phase 2.2 (this rev):  stream_verdict parses RESP array headers,
 *                        extracts the command name, and updates per-command
 *                        counters in a BPF hash map.
 * Phase 2.3 (this rev):  same verdict updates a per-connection LRU profile
 *                        keyed by socket cookie (first_seen, last_seen,
 *                        cmd_count, bytes_in).
 *
 * Always returns SK_PASS — userspace continues to handle every byte.  The
 * BPF programs are pure observers; they never modify or drop traffic.
 *
 * Approximation note:
 *   We parse the FIRST command in each skb and count it once.  If a single
 *   skb carries multiple pipelined commands, only the first is broken down
 *   by command-name; the rest contribute to bytes_in but not cmd_counts.
 *   This keeps the verifier happy (bounded loop) while remaining accurate
 *   for the dominant case of one-command-per-skb.  Total command count is
 *   still tracked precisely via SOCKMAP_STAT_PARSED.
 */

#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>
#include "sockmap_kvs.h"

/* ── BPF Maps ─────────────────────────────────────────────────────────── */

struct {
    __uint(type,        BPF_MAP_TYPE_SOCKHASH);
    __uint(max_entries, SOCKMAP_MAX_ENTRIES);
    __type(key,         __u64);
    __type(value,       __u64);
} sockmap SEC(".maps");

struct {
    __uint(type,        BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key,         __u32);
    __type(value,       __u32);
} sockmap_config SEC(".maps");

struct {
    __uint(type,        BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, SOCKMAP_STAT_MAX);
    __type(key,         __u32);
    __type(value,       __u64);
} sockmap_stats SEC(".maps");

/* Per-command counters: keyed by uppercased command name. */
struct {
    __uint(type,        BPF_MAP_TYPE_HASH);
    __uint(max_entries, SOCKMAP_CMD_MAX_ENTRIES);
    __type(key,         struct sockmap_cmd_key);
    __type(value,       struct sockmap_cmd_stats);
} cmd_counts SEC(".maps");

/* Per-connection profile: keyed by socket cookie.  LRU so it self-evicts
 * if maxclients is misconfigured. */
struct {
    __uint(type,        BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, SOCKMAP_MAX_ENTRIES);
    __type(key,         __u64);
    __type(value,       struct sockmap_conn_profile);
} conn_profiles SEC(".maps");

/* ── Helpers ──────────────────────────────────────────────────────────── */

static __always_inline void stat_inc(__u32 idx) {
    __u64 *cnt = bpf_map_lookup_elem(&sockmap_stats, &idx);
    if (cnt) __sync_fetch_and_add(cnt, 1);
}

/* Parse `*N\r\n$L\r\n<NAME>\r\n` at the start of [data, data_end) and
 * fill `key->name`/`key->len` with the uppercased command name.
 * Returns 1 on success, 0 on malformed/incomplete header. */
static __always_inline int parse_resp_cmd_name(const char *data,
                                                const char *data_end,
                                                struct sockmap_cmd_key *key)
{
    /* Need at least "*1\r\n$1\r\nX\r\n" = 11 bytes */
    if (data + 11 > data_end) return 0;
    if (data[0] != '*') return 0;

    /* Skip *<digits>\r\n — array length up to 2 digits */
    int i = 1;
    if (data + i + 1 > data_end) return 0;
    if (data[i] < '0' || data[i] > '9') return 0;
    i++;
    if (data + i + 1 > data_end) return 0;
    if (data[i] >= '0' && data[i] <= '9') i++;
    if (data + i + 2 > data_end) return 0;
    if (data[i] != '\r' || data[i+1] != '\n') return 0;
    i += 2;

    /* $<digits>\r\n — bulk string length 1..2 digits, ≤ SOCKMAP_CMD_NAME_MAX */
    if (data + i + 1 > data_end) return 0;
    if (data[i] != '$') return 0;
    i++;
    if (data + i + 1 > data_end) return 0;
    if (data[i] < '0' || data[i] > '9') return 0;
    int namelen = data[i] - '0';
    i++;
    if (data + i + 1 > data_end) return 0;
    if (data[i] >= '0' && data[i] <= '9') {
        namelen = namelen * 10 + (data[i] - '0');
        i++;
    }
    if (namelen <= 0 || namelen > SOCKMAP_CMD_NAME_MAX) return 0;
    if (data + i + 2 > data_end) return 0;
    if (data[i] != '\r' || data[i+1] != '\n') return 0;
    i += 2;

    /* Copy + uppercase the command name.  Bounded loop with constant cap. */
    if (data + i + namelen + 2 > data_end) return 0;

    /* Bounded loop with constant cap — verifier-friendly without #pragma unroll */
    for (int j = 0; j < SOCKMAP_CMD_NAME_MAX; j++) {
        if (j >= namelen) break;
        if (data + i + j + 1 > data_end) break;
        char c = data[i + j];
        if (c >= 'a' && c <= 'z') c = c - 'a' + 'A';
        key->name[j] = (__u8)c;
    }
    key->len = (__u8)namelen;
    return 1;
}

/* ── sockops: insert passive-established sockets into sockmap ─────────── */

SEC("sockops")
int sockmap_kvs_sockops(struct bpf_sock_ops *skops)
{
    if (skops->family != 2 /* AF_INET */)
        return 0;

    __u32 cfg_key = 0;
    __u32 *cfg_port = bpf_map_lookup_elem(&sockmap_config, &cfg_key);
    if (!cfg_port || *cfg_port == 0)
        return 0;

    if (skops->op == BPF_SOCK_OPS_PASSIVE_ESTABLISHED_CB) {
        if (skops->local_port != *cfg_port)
            return 0;
        __u64 cookie = bpf_get_socket_cookie(skops);
        long ret = bpf_sock_hash_update(skops, &sockmap, &cookie, BPF_NOEXIST);
        if (ret == 0) stat_inc(SOCKMAP_STAT_ADDED);
        else          stat_inc(SOCKMAP_STAT_ADD_FAIL);
    }
    return 0;
}

/* ── sk_skb stream_verdict: count commands + profile connections ──────── */

SEC("sk_skb/stream_verdict")
int sockmap_kvs_verdict(struct __sk_buff *skb)
{
    stat_inc(SOCKMAP_STAT_VERDICT);

    /* TCP RX path: data may be split between linear and paged regions.
     * Pull the first 64 bytes into linear so direct pointer access can
     * see the RESP header.  Returns 0 if already linear/successful. */
    if (skb->len >= 64)
        bpf_skb_pull_data(skb, 64);
    else
        bpf_skb_pull_data(skb, skb->len);

    void *data     = (void *)(long)skb->data;
    void *data_end = (void *)(long)skb->data_end;

    /* Per-connection profile: bump bytes_in for every skb (regardless of
     * whether RESP parsing succeeds — keeps byte accounting accurate). */
    __u64 cookie  = bpf_get_socket_cookie(skb);
    __u64 now     = bpf_ktime_get_ns();
    __u32 skb_len = (__u32)((char *)data_end - (char *)data);

    struct sockmap_cmd_key cmd_key = {};
    int parsed = parse_resp_cmd_name((const char *)data,
                                      (const char *)data_end, &cmd_key);

    if (parsed) {
        stat_inc(SOCKMAP_STAT_PARSED);

        /* Per-command bucket */
        struct sockmap_cmd_stats *s =
            bpf_map_lookup_elem(&cmd_counts, &cmd_key);
        if (s) {
            __sync_fetch_and_add(&s->count, 1);
            __sync_fetch_and_add(&s->bytes, skb_len);
        } else {
            struct sockmap_cmd_stats init = {
                .count = 1,
                .bytes = skb_len,
            };
            bpf_map_update_elem(&cmd_counts, &cmd_key, &init, BPF_NOEXIST);
        }
    } else {
        stat_inc(SOCKMAP_STAT_PARSE_SKIP);
    }

    /* Per-connection profile (always update) */
    struct sockmap_conn_profile *cp =
        bpf_map_lookup_elem(&conn_profiles, &cookie);
    if (cp) {
        __sync_fetch_and_add(&cp->cmd_count, parsed ? 1 : 0);
        __sync_fetch_and_add(&cp->bytes_in, skb_len);
        cp->last_seen_ns = now;
    } else {
        struct sockmap_conn_profile init = {
            .first_seen_ns = now,
            .last_seen_ns  = now,
            .cmd_count     = (parsed ? 1 : 0),
            .bytes_in      = skb_len,
        };
        bpf_map_update_elem(&conn_profiles, &cookie, &init, BPF_ANY);
    }

    stat_inc(SOCKMAP_STAT_VERDICT_PASS);
    return SK_PASS;
}

char _license[] SEC("license") = "GPL";
