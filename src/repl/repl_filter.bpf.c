/* repl_filter.bpf.c — Smart eBPF filter for replication with stream reassembly
 *
 * This eBPF program filters Redis commands at the socket layer:
 * - Only intercepts WRITE commands (SET, DEL, LPUSH, etc.)
 * - Ignores READ commands (GET, KEYS, INFO, etc.)
 * - Handles multi-segment commands via per-socket state tracking
 * - Copies write commands to ringbuf for replication
 * - Always returns SK_PASS (transparent to application)
 *
 * Key improvement: Solves the multi-segment command loss problem by tracking
 * command boundaries using RESP protocol length parsing.
 */

#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>
#include "repl_observe.h"

/* ── BPF Maps ────────────────────────────────────────────────────────── */

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

/* Per-CPU temporary buffer for skb data (avoids stack overflow) */
struct {
    __uint(type,        BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key,         __u32);
    __type(value,       __u8[REPL_OBSERVE_MAX_DATA]);
} repl_temp_buf SEC(".maps");

/* Per-socket state for command tracking */
struct repl_sock_state {
    __u32 expected;   /* Total bytes expected for current write command, 0 = not in command */
    __u32 seen;       /* Bytes captured so far */
};

struct {
    __uint(type,        BPF_MAP_TYPE_SK_STORAGE);
    __uint(map_flags,   BPF_F_NO_PREALLOC);
    __type(key,         int);
    __type(value,       struct repl_sock_state);
} repl_sock_state_map SEC(".maps");

/* ── Helper: Parse RESP total length ──────────────────────────────────── */

/*
 * Parse RESP command total length from protocol header.
 * Only needs first 256 bytes (all $L prefixes are in the header).
 * Returns 0 on parse failure or if command exceeds REPL_OBSERVE_MAX_CMD.
 * Supports up to 16 arguments (sufficient for KV commands).
 */
static __always_inline __u32 parse_resp_total_len(const __u8 *data, __u32 data_len)
{
    if (data_len < 4 || data[0] != '*')
        return 0;

    __u32 cursor = 1, N = 0;

    /* Parse *N\r\n - array count (max 6 digits) */
    #pragma unroll
    for (int j = 0; j < 6; j++) {
        if (cursor >= data_len) break;
        __u8 c = data[cursor];
        if (c >= '0' && c <= '9') {
            if (N > 100) return 0;  /* Overflow protection */
            N = N * 10 + (c - '0');
            cursor++;
        } else {
            break;
        }
    }
    if (N == 0 || N > 16)
        return 0;
    if (cursor + 1 >= data_len || data[cursor] != '\r' || data[cursor + 1] != '\n')
        return 0;
    cursor += 2;

    /* Initialize total bytes with *N\r\n header length */
    __u32 total = cursor;

    /* Parse each argument's length */
    #pragma unroll
    for (int i = 0; i < 16; i++) {
        if (i >= (int)N) break;

        __u32 arg_start = cursor;

        if (cursor >= data_len || data[cursor] != '$')
            return 0;
        cursor++;

        __u32 L = 0;
        /* Parse argument length (max 10 digits = 9GB limit) */
        #pragma unroll
        for (int j = 0; j < 10; j++) {
            if (cursor >= data_len) break;
            __u8 c = data[cursor];
            if (c >= '0' && c <= '9') {
                /* Overflow protection: max 1GB per argument */
                if (L > 100000000) return 0;
                L = L * 10 + (c - '0');
                cursor++;
            } else {
                break;
            }
        }
        if (cursor + 1 >= data_len)
            return 0;
        if (data[cursor] != '\r' || data[cursor + 1] != '\n')
            return 0;
        cursor += 2;

        /* Accumulate: $L\r\n prefix + L data bytes + trailing \r\n */
        __u32 segment_len = (cursor - arg_start) + L + 2;

        /* Prevent addition overflow */
        if (total > 0xFFFFFFFF - segment_len)
            return 0;

        total += segment_len;
    }

    /* Final check: don't exceed REPL_OBSERVE_MAX_CMD (4MB) */
    if (total > REPL_OBSERVE_MAX_CMD)
        return 0;

    return total;
}

/* ── Helper: Check if command is a write command ──────────────────────── */

static __always_inline int is_write_command(const char *data, __u32 len) {
    /* RESP protocol format: *N\r\n$L\r\nCOMMAND\r\n...
     * We need to parse to the command name and check if it's a write op.
     */

    if (len < 10 || data[0] != '*') return 0;

    /* Skip array count: *N\r\n */
    int i = 1;
    #pragma unroll
    for (int j = 0; j < 10; j++) {
        if (i >= len) return 0;
        if (data[i] == '\n') {
            i++;
            break;
        }
        i++;
    }

    /* Check bulk string marker: $ */
    if (i >= len || data[i] != '$') return 0;
    i++;

    /* Skip bulk string length: L\r\n */
    #pragma unroll
    for (int j = 0; j < 10; j++) {
        if (i >= len) return 0;
        if (data[i] == '\n') {
            i++;
            break;
        }
        i++;
    }

    /* Now we're at the command name */
    if (i + 3 >= len) return 0;

    /* Convert to uppercase for comparison */
    char c0 = data[i];
    char c1 = data[i+1];
    char c2 = data[i+2];

    if (c0 >= 'a' && c0 <= 'z') c0 -= 32;
    if (c1 >= 'a' && c1 <= 'z') c1 -= 32;
    if (c2 >= 'a' && c2 <= 'z') c2 -= 32;

    /* Check write commands (first 3 chars) */

    /* SET, SETEX, SETNX */
    if (c0 == 'S' && c1 == 'E' && c2 == 'T') return 1;

    /* DEL */
    if (c0 == 'D' && c1 == 'E' && c2 == 'L') return 1;

    /* LPUSH, LPOP */
    if (c0 == 'L' && c1 == 'P') return 1;

    /* RPUSH, RPOP */
    if (c0 == 'R' && c1 == 'P') return 1;

    /* HSET, HDEL */
    if (c0 == 'H' && (c1 == 'S' || c1 == 'D')) return 1;

    /* SADD, SREM */
    if (c0 == 'S' && (c1 == 'A' || c1 == 'R')) return 1;

    /* ZADD, ZREM */
    if (c0 == 'Z' && (c1 == 'A' || c1 == 'R')) return 1;

    /* INCR, INCRBY */
    if (c0 == 'I' && c1 == 'N' && c2 == 'C') return 1;

    /* DECR, DECRBY */
    if (c0 == 'D' && c1 == 'E' && c2 == 'C') return 1;

    /* APPEND */
    if (c0 == 'A' && c1 == 'P' && c2 == 'P') return 1;

    /* MSET */
    if (c0 == 'M' && c1 == 'S' && c2 == 'E') return 1;

    /* FLUSHALL, FLUSHDB */
    if (c0 == 'F' && c1 == 'L' && c2 == 'U') return 1;

    /* Not a write command */
    return 0;
}

/* ── sockops: Auto-insert passive-established sockets ──────────────────── */

SEC("sockops")
int repl_filter_sockops(struct bpf_sock_ops *skops) {
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

/* ── stream_parser: Simple passthrough parser ──────────────────────────── */

SEC("sk_skb/stream_parser")
int repl_filter_parser(struct __sk_buff *skb) {
    return skb->len;
}

/* ── stream_verdict: Smart filtering + stream reassembly ───────────────── */

SEC("sk_skb/stream_verdict")
int repl_filter_verdict(struct __sk_buff *skb) {
    __u32 skb_len = skb->len;
    if (skb_len == 0)
        return SK_PASS;

    /* Get per-socket state (create if not exists) */
    __u64 cookie = bpf_get_socket_cookie(skb);
    struct repl_sock_state *st =
        bpf_sk_storage_get(&repl_sock_state_map, skb, 0,
                           BPF_SK_STORAGE_GET_F_CREATE);
    if (!st)
        return SK_PASS;

    /* Check if we're in the middle of a write command */
    if (st->expected == 0) {
        /* Not in a command - check if this starts a new write command */
        __u8 first_byte = 0;
        if (bpf_skb_load_bytes(skb, 0, &first_byte, 1) != 0)
            return SK_PASS;
        if (first_byte != '*')
            return SK_PASS;

        /* Get temporary buffer for parsing */
        __u32 key = 0;
        __u8 *data = bpf_map_lookup_elem(&repl_temp_buf, &key);
        if (!data)
            return SK_PASS;

        /* Load header for parsing (256 bytes is enough for all $L prefixes) */
        __u32 parse_len = skb_len < 256 ? skb_len : 256;
        if (bpf_skb_load_bytes(skb, 0, data, parse_len) != 0)
            return SK_PASS;

        /* Parse RESP header to get total command length */
        __u32 total = parse_resp_total_len(data, parse_len);
        if (total == 0 || total > REPL_OBSERVE_MAX_CMD)
            return SK_PASS;

        /* Check if this is a write command */
        if (!is_write_command((const char *)data, parse_len))
            return SK_PASS;

        /* All commands (up to REPL_OBSERVE_MAX_CMD = 4MB) captured via
         * stream reassembly.  Each TCP segment (≤MSS=1460) fits in one
         * ringbuf event (4096 bytes).  User-space reassembles via
         * FIRST/LAST flags + offset. */

        /* Enter capture state */
        st->expected = total;
        st->seen = 0;
    }

    /* If expected==0, this is a non-write command or read — pass through */
    if (st->expected == 0)
        return SK_PASS;

    /* Allocate ringbuf event */
    struct repl_observe_event *e =
        bpf_ringbuf_reserve(&repl_ringbuf, sizeof(*e), 0);
    if (!e)
        return SK_PASS;  /* ringbuf full, drop (slave will catch up via resync) */

    __u32 copy_len = skb_len;
    if (copy_len > REPL_OBSERVE_MAX_DATA)
        copy_len = REPL_OBSERVE_MAX_DATA;

    if (bpf_skb_load_bytes(skb, 0, e->data, copy_len) != 0) {
        bpf_ringbuf_discard(e, 0);
        return SK_PASS;
    }

    e->sock_cookie = cookie;
    e->len = copy_len;
    e->offset = st->seen;
    e->flags = 0;
    e->padding[0] = 0;
    e->padding[1] = 0;
    e->padding[2] = 0;

    /* Set flags */
    if (st->seen == 0)
        e->flags |= REPL_FLAG_FIRST;

    st->seen += skb_len;
    if (st->seen >= st->expected) {
        e->flags |= REPL_FLAG_LAST;
        st->expected = 0;
        st->seen = 0;
    }

    bpf_ringbuf_submit(e, 0);

    /* Always pass through (transparent) */
    return SK_PASS;
}

char _license[] SEC("license") = "GPL";
