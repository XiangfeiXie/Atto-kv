/* SPDX-License-Identifier: GPL-2.0
 * ebpf_incr.bpf.c  —  BPF kernel program for RDMA incremental replication
 *
 * Attaches as a uprobe on kvs_rdma_incr_probe().
 * That function is called by kvs_protocol() ONLY when a write command
 * (SET/MOD/DEL/FLUSHALL) has already been confirmed successful, so the
 * BPF program does NOT need to evaluate success/failure itself.
 *
 * Arguments captured (x86-64 ABI):
 *   PARM1 (rdi) = const char *msg    — raw RESP command bytes
 *   PARM2 (rsi) = int         length — byte count
 *
 * Output: ring-buffer entries of type struct incr_event.
 * User-space consumer reads these and sends them to slaves via RDMA.
 */

#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

#define INCR_MAX_CMD  8192
#define RINGBUF_SIZE  (2 * 1024 * 1024)   /* 2 MB */

struct incr_event {
    __u32 length;
    char  msg[INCR_MAX_CMD];
};

struct incr_stats {
    __u64 emitted;
    __u64 dropped;
};

struct {
    __uint(type,       BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, RINGBUF_SIZE);
} rb SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct incr_stats);
} stats SEC(".maps");

SEC("uprobe")
int on_kvs_rdma_incr_probe(struct pt_regs *ctx)
{
    const char *msg = (const char *)PT_REGS_PARM1(ctx);
    __u32 zero = 0;
    struct incr_stats *st = bpf_map_lookup_elem(&stats, &zero);

    /* Read length as a signed long so the verifier tracks the range
     * on the same register without an intermediate u32 copy.         */
    long raw_len = (long)PT_REGS_PARM2(ctx);

    /* Reject zero/negative and oversized commands */
    if (raw_len <= 0 || raw_len > INCR_MAX_CMD)
        return 0;

    /* Reserve a fixed-size slot from the ring buffer */
    struct incr_event *e = bpf_ringbuf_reserve(&rb, sizeof(*e), 0);
    if (!e) {
        if (st)
            __sync_fetch_and_add(&st->dropped, 1);
        return 0;
    }

    __u32 length = (__u32)raw_len;
    e->length    = length;

    /* Duplicate bound check immediately before bpf_probe_read_user so
     * the verifier can prove at *this exact call site* that length is
     * in [1, INCR_MAX_CMD] and fits inside e->msg[INCR_MAX_CMD].     */
    if (length == 0 || length > INCR_MAX_CMD) {
        bpf_ringbuf_discard(e, 0);
        return 0;
    }

    /* Copy RESP bytes from user-space into the ring buffer entry */
    if (bpf_probe_read_user(e->msg, length, msg) < 0) {
        bpf_ringbuf_discard(e, 0);
        if (st)
            __sync_fetch_and_add(&st->dropped, 1);
        return 0;
    }

    bpf_ringbuf_submit(e, 0);
    if (st)
        __sync_fetch_and_add(&st->emitted, 1);
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
