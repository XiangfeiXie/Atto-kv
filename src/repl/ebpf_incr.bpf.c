/* SPDX-License-Identifier: GPL-2.0
 * ebpf_incr.bpf.c  —  BPF kernel program for RDMA incremental replication
 *
 * Attaches as a uprobe on kvs_rdma_incr_probe().
 * Captures write commands and assigns a global monotonically-increasing
 * sequence number (global_seq) so the user-space fullsync coordinator
 * can precisely determine the cut point for fork-based snapshots.
 */

#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

#define INCR_MAX_CMD  65536
#define RINGBUF_SIZE  (8 * 1024 * 1024)   /* 8 MB */

struct incr_event {
	__u64    seq;      /* global sequence number, assigned atomically in-kernel */
	__u32    length;
	char     msg[INCR_MAX_CMD];
};

struct incr_stats {
	__u64 emitted;
	__u64 dropped;
};

struct {
	__uint(type,        BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, RINGBUF_SIZE);
} rb SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, struct incr_stats);
} stats SEC(".maps");

/* Global monotonically-increasing sequence number.
 * User-space reads this map before fork() to determine the
 * fullsync_base_seq cut point. */
struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, __u64);
} global_seq SEC(".maps");

SEC("uprobe")
int on_kvs_rdma_incr_probe(struct pt_regs *ctx)
{
	const char *msg = (const char *)PT_REGS_PARM1(ctx);
	__u32 zero = 0;
	struct incr_stats *st = bpf_map_lookup_elem(&stats, &zero);

	long raw_len = (long)PT_REGS_PARM2(ctx);

	if (raw_len <= 0)
		return 0;
	if (raw_len > INCR_MAX_CMD) {
		if (st)
			__sync_fetch_and_add(&st->dropped, 1);
		return 0;
	}

	/* Atomically assign a global sequence number */
	__u64 *seq_p = bpf_map_lookup_elem(&global_seq, &zero);
	__u64 cur_seq = seq_p ? __sync_fetch_and_add(seq_p, 1) : 0;

	struct incr_event *e = bpf_ringbuf_reserve(&rb, sizeof(*e), 0);
	if (!e) {
		if (st)
			__sync_fetch_and_add(&st->dropped, 1);
		return 0;
	}

	e->seq    = cur_seq;
	e->length = (__u32)raw_len;

	if (e->length == 0 || e->length > INCR_MAX_CMD) {
		bpf_ringbuf_discard(e, 0);
		return 0;
	}

	if (bpf_probe_read_user(e->msg, e->length, msg) < 0) {
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
