/* repl_filter.bpf.c — eBPF filter for replication
 *
 * Captures complete write commands from the socket layer:
 * - sockops: auto-inserts passive-established sockets into sockmap
 * - stream_parser: simple passthrough
 * - stream_verdict: inspects each skb, copies potential write-command data
 *   (≤4KB, single-segment) to ringbuf for replication
 *
 * Commands >4KB or split across segments pass through transparently
 * and are handled by the main-process repl_notify path.
 *
 * No per-socket state, no SK_STORAGE, no RESP full-length parsing in BPF.
 * User-space validates completeness via find_complete_resp_command().
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

/* Per-CPU temporary buffer (shared by verdict and parser) */
struct {
    __uint(type,        BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key,         __u32);
    __type(value,       __u8[REPL_OBSERVE_MAX_DATA]);
} repl_temp_buf SEC(".maps");

/* ── Helper: check first char of RESP command name ───────────────────── */

/*
 * Returns 1 if the command name's first character matches a known write
 * command prefix.  False positives (e.g. SUBSTR) are harmless — user-space
 * will filter them out; we only care about reducing ringbuf volume.
 *
 * Write-command first chars: S(ET/ADD/REM), D(EL/ECR), H(SET/DEL),
 *   L(PUSH/POP), R(PUSH/POP), I(NCR), Z(ADD/REM), A(PPEND), M(SET), F(LUSHALL)
 */
static __always_inline int is_write_cmd_first_char(char c)
{
    /* Uppercase */
    if (c >= 'a' && c <= 'z')
        c -= 32;
    return (c == 'S' || c == 'D' || c == 'H' || c == 'L' || c == 'R' ||
            c == 'I' || c == 'Z' || c == 'A' || c == 'M' || c == 'F');
}

/*
 * Locate the command name in a RESP header and check its first character.
 * RESP format: *N\r\n$L\r\n<COMMAND>...
 * We scan forward through the *N\r\n$L\r\n prefix to reach the command name.
 */
static __always_inline int is_write_command(const __u8 *data, __u32 data_len)
{
    if (data_len < 10 || data[0] != '*')
        return 0;

    __u32 i = 1;

    /* Skip *N\r\n — N is at most 2 digits for our supported commands */
#pragma unroll
    for (int j = 0; j < 4; j++) {
        if (i >= data_len) return 0;
        if (data[i] == '\r') { i += 2; break; }
        i++;
    }
    if (i >= data_len) return 0;

    /* Expect $L\r\n */
    if (data[i] != '$') return 0;
    i++;

    /* Skip L\r\n — L is at most 6 digits */
#pragma unroll
    for (int j = 0; j < 8; j++) {
        if (i >= data_len) return 0;
        if (data[i] == '\r') { i += 2; break; }
        i++;
    }
    if (i >= data_len) return 0;

    return is_write_cmd_first_char((char)data[i]);
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

/* ── stream_parser: Simple passthrough ────────────────────────────────── */

SEC("sk_skb/stream_parser")
int repl_filter_parser(struct __sk_buff *skb) {
    return skb->len;
}

/* ── stream_verdict: Capture complete write commands ──────────────────── */

SEC("sk_skb/stream_verdict")
int repl_filter_verdict(struct __sk_buff *skb) {
    __u32 skb_len = skb->len;
    if (skb_len == 0 || skb_len > REPL_OBSERVE_MAX_DATA)
        return SK_PASS;

    /* Quick check: first byte must be '*' (RESP array) */
    __u8 first_byte = 0;
    if (bpf_skb_load_bytes(skb, 0, &first_byte, 1) != 0)
        return SK_PASS;
    if (first_byte != '*')
        return SK_PASS;

    /* Load header for write-command check (32 bytes is enough to reach
     * the command name: *N\r\n$L\r\n<CMD> → typically 12-16 bytes) */
    __u32 key = 0;
    __u8 *hdr = bpf_map_lookup_elem(&repl_temp_buf, &key);
    if (!hdr)
        return SK_PASS;

    __u32 hdr_len = skb_len < 48 ? skb_len : 48;
    if (bpf_skb_load_bytes(skb, 0, hdr, hdr_len) != 0)
        return SK_PASS;

    if (!is_write_command(hdr, hdr_len))
        return SK_PASS;

    /* Allocate ringbuf event — copy entire skb payload.
     * User-space find_complete_resp_command() validates the command
     * is complete; partial or multi-command data is discarded there. */
    struct repl_observe_event *e =
        bpf_ringbuf_reserve(&repl_ringbuf, sizeof(*e), 0);
    if (!e)
        return SK_PASS;

    if (bpf_skb_load_bytes(skb, 0, e->data, skb_len) != 0) {
        bpf_ringbuf_discard(e, 0);
        return SK_PASS;
    }

    e->sock_cookie = bpf_get_socket_cookie(skb);
    e->len = skb_len;
    e->pad = 0;

    bpf_ringbuf_submit(e, 0);
    return SK_PASS;
}

char _license[] SEC("license") = "GPL";
