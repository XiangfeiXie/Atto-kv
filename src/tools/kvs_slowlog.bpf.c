/* SPDX-License-Identifier: GPL-2.0
 * kvs_slowlog.bpf.c — uprobe-based slow-command monitor for kvstore
 *
 * Attaches uprobes to kvs_dispatch() entry and uretprobe to its return.
 * At return, if the elapsed time exceeds cfg_map[0] (threshold_ns), a
 * slow_event containing the latency and first MAX_CMD_CAPTURE bytes of the
 * raw RESP command is submitted to slow_rb for userspace consumption.
 *
 * kvs_dispatch signature (x86-64 SysV ABI):
 *   dispatch_result_t kvs_dispatch(kvs_token_t *tokens,   ← rdi (PARM1)
 *                                  int argc,              ← rsi (PARM2)
 *                                  const char *msg_raw,   ← rdx (PARM3)
 *                                  int msg_len,           ← rcx (PARM4)
 *                                  DynBuf *wb,            ← r8  (PARM5)
 *                                  struct conn *c)        ← r9  (PARM6)
 */

#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

#define MAX_CMD_CAPTURE  128   /* bytes of RESP command captured per event */
#define START_MAP_SIZE   1024  /* max concurrent in-flight dispatch calls   */

/* ── Shared structures (must match kvs_slowlog.c) ── */

struct slow_event {
    __u64 latency_ns;                /* wall-clock latency of kvs_dispatch  */
    __u32 cmd_len;                   /* captured RESP bytes (≤ MAX_CMD_CAPTURE) */
    __u8  cmd[MAX_CMD_CAPTURE];      /* raw RESP command fragment            */
};

struct entry_ctx {
    __u64 ts;                        /* ktime_get_ns() at dispatch entry     */
    __u64 msg_raw;                   /* userspace ptr to raw RESP buffer     */
    __u32 msg_len;                   /* length of raw RESP buffer            */
};

/* ── BPF maps ── */

/* cfg_map[0] = threshold_ns; written by userspace after load */
struct {
    __uint(type,        BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key,         __u32);
    __type(value,       __u64);
} cfg_map SEC(".maps");

/* Keyed by tid; tracks timestamp + msg pointer for each in-flight call */
struct {
    __uint(type,        BPF_MAP_TYPE_HASH);
    __uint(max_entries, START_MAP_SIZE);
    __type(key,         __u32);
    __type(value,       struct entry_ctx);
} start_map SEC(".maps");

/* Output ring buffer consumed by the userspace tool */
struct {
    __uint(type,        BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 4 * 1024 * 1024);  /* 4 MB */
} slow_rb SEC(".maps");

/* ── uprobe: kvs_dispatch entry ── */

SEC("uprobe")
int on_dispatch_enter(struct pt_regs *ctx)
{
    __u32 tid = (__u32)bpf_get_current_pid_tgid();

    struct entry_ctx ec = {};
    ec.ts      = bpf_ktime_get_ns();
    ec.msg_raw = (__u64)PT_REGS_PARM3(ctx);  /* msg_raw: const char* */
    ec.msg_len = (__u32)(__u64)PT_REGS_PARM4(ctx); /* msg_len: int  */

    bpf_map_update_elem(&start_map, &tid, &ec, BPF_ANY);
    return 0;
}

/* ── uretprobe: kvs_dispatch return ── */

SEC("uretprobe")
int on_dispatch_exit(struct pt_regs *ctx)
{
    __u32 tid = (__u32)bpf_get_current_pid_tgid();

    struct entry_ctx *ec = bpf_map_lookup_elem(&start_map, &tid);
    if (!ec)
        return 0;

    __u64 now   = bpf_ktime_get_ns();
    __u64 delta = now - ec->ts;

    /* Save and clean up entry context before any conditional return */
    __u64 msg_raw = ec->msg_raw;
    __u32 msg_len = ec->msg_len;
    bpf_map_delete_elem(&start_map, &tid);

    /* Read threshold; default 1ms if map lookup fails */
    __u32 zero = 0;
    __u64 *thresh = bpf_map_lookup_elem(&cfg_map, &zero);
    __u64 threshold_ns = thresh ? *thresh : 1000000ULL;

    if (delta < threshold_ns)
        return 0;

    if (msg_raw == 0 || msg_len == 0)
        return 0;

    struct slow_event *e = bpf_ringbuf_reserve(&slow_rb, sizeof(*e), 0);
    if (!e)
        return 0;

    e->latency_ns = delta;

    /* Clamp to MAX_CMD_CAPTURE; the verifier requires a compile-time
     * constant for the size argument of bpf_probe_read_user.
     * We always read MAX_CMD_CAPTURE bytes and record the real length
     * separately — this is the standard verifier-safe pattern. */
    e->cmd_len = msg_len < MAX_CMD_CAPTURE ? msg_len : MAX_CMD_CAPTURE;
    __builtin_memset(e->cmd, 0, MAX_CMD_CAPTURE);
    bpf_probe_read_user(e->cmd, MAX_CMD_CAPTURE, (void *)msg_raw);

    bpf_ringbuf_submit(e, 0);
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
