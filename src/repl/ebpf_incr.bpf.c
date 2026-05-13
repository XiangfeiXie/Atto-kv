/* SPDX-License-Identifier: GPL-2.0
 * ebpf_incr.bpf.c  —  BPF kernel program for incremental replication
 *
 * Attaches as a uprobe on kvs_rdma_incr_probe().
 * Captures write commands and assigns a global monotonically-increasing
 * sequence number (global_seq) so the user-space fullsync coordinator
 * can precisely determine the cut point for fork-based snapshots.
 *
 * Ring buffer: tiered constant-size reservations to satisfy kernel 6.8
 * verifier's compile-time-constant requirement while keeping records
 * proportional to actual command size.
 *   4 KB tier  —  commands   ≤ 4 KB   (~65K events in 256 MB ringbuf)
 *  64 KB tier  —  commands  ≤ 64 KB   (~4K events)
 * 256 KB tier  —  commands ≤ 256 KB   (~998 events, fallback)
 */

#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

#define INCR_MAX_CMD  262144               /* 256 KB — max RESP command */
#define RINGBUF_SIZE  (256 * 1024 * 1024)  /* 256 MB */

/* Tiered reservation sizes — each is a compile-time constant.
 * The verifier accepts these because each branch passes a literal. */
#define TIER_SMALL    4096                 /* 4 KB */
#define TIER_MEDIUM   65536                /* 64 KB */
#define TIER_LARGE    INCR_MAX_CMD         /* 256 KB */

struct incr_event_hdr {
	__u64    seq;
	__u32    length;
	__u32    _pad;
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

	/* Tiered constant-size reservation.
	 * Compare raw_len directly (not (u32)raw_len) so the verifier
	 * narrows the register that bpf_probe_read_user uses for size.
	 * Each branch contains the full lifecycle so the verifier checks
	 * each path independently without cross-branch state merging.  */
	struct incr_event_hdr *e;

	if (raw_len <= TIER_SMALL) {
		e = bpf_ringbuf_reserve(&rb, sizeof(*e) + TIER_SMALL, 0);
		if (!e) goto drop;
		e->seq    = cur_seq;
		e->length = (__u32)raw_len;
		if (bpf_probe_read_user((void *)(e + 1), e->length, msg) < 0) {
			bpf_ringbuf_discard(e, 0);
			goto drop;
		}
		bpf_ringbuf_submit(e, 0);
		if (st) __sync_fetch_and_add(&st->emitted, 1);
		return 0;
	}

	if (raw_len <= TIER_MEDIUM) {
		e = bpf_ringbuf_reserve(&rb, sizeof(*e) + TIER_MEDIUM, 0);
		if (!e) goto drop;
		e->seq    = cur_seq;
		e->length = (__u32)raw_len;
		if (bpf_probe_read_user((void *)(e + 1), e->length, msg) < 0) {
			bpf_ringbuf_discard(e, 0);
			goto drop;
		}
		bpf_ringbuf_submit(e, 0);
		if (st) __sync_fetch_and_add(&st->emitted, 1);
		return 0;
	}

	/* TIER_LARGE fallback */
	{
		e = bpf_ringbuf_reserve(&rb, sizeof(*e) + TIER_LARGE, 0);
		if (!e) goto drop;
		e->seq    = cur_seq;
		e->length = (__u32)raw_len;
		if (bpf_probe_read_user((void *)(e + 1), e->length, msg) < 0) {
			bpf_ringbuf_discard(e, 0);
			goto drop;
		}
		bpf_ringbuf_submit(e, 0);
		if (st) __sync_fetch_and_add(&st->emitted, 1);
		return 0;
	}

drop:
	if (st) __sync_fetch_and_add(&st->dropped, 1);
	return 0;
}

char LICENSE[] SEC("license") = "GPL";
