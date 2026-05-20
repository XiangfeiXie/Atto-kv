/*
 * fastpath_cache.h — Phase 10: Lock-free seqlock short-key hot cache
 *
 * A 512 KB direct-mapped cache (1024 buckets × 512B) layered ABOVE the
 * 32-shard rwlock dict.  Hot keys with payload ≤ 256B serve their
 * GET reply from here, completely skipping shard rdlock + dict_find +
 * reply_bulk formatting.  Reader path is lock-free; writer path uses
 * a per-bucket CAS-based single-writer protocol so writers on
 * different dict shards that happen to hash to the same fp bucket
 * remain mutually exclusive.
 *
 * Seqlock invariants:
 *   seq == 0          — bucket has never been written (empty)
 *   seq even (>0)     — bucket holds a stable record
 *   seq odd           — writer mid-update; readers must retry
 *
 *   Writer: CAS seq from even to (seq+1) [odd], publish fields,
 *           atomic_store(seq+2) with release.
 *   Reader: acquire-load seq → if odd or zero, miss.  Otherwise
 *           snapshot fields, re-load seq, verify unchanged.  Retry
 *           at most once.
 *
 * MT-safety contract:
 *   - fp_get may run from ANY worker thread, fully reentrant.
 *   - fp_put / fp_del are typically invoked from a dispatcher worker
 *     while holding the corresponding dict shard wrlock; the per-
 *     bucket CAS spin handles the rare cross-shard collision.
 *   - All stats counters use relaxed atomic adds (TSAN-clean).
 *
 * Eligibility (caller-enforced): STRING type only, key length 1..64,
 *   value length 1..256, encoding == RAW.
 */
#ifndef KV_FASTPATH_CACHE_H
#define KV_FASTPATH_CACHE_H

#include <stdint.h>
#include <stddef.h>
#include <stdatomic.h>

/* Phase 10 sizing rationale.
 *
 * 2048 buckets × 512B = 1 MB total table.  This fits inside the L2
 * cache of common modern x86-64 / aarch64 desktop and server CPU, so
 * even a *miss* on the seqlock acquire-load only pays an L2 hit
 * (~12 cycles) instead of an L3 round-trip (~30+ cycles).  Earlier
 * sizing (4096 × 512B = 2 MB) over-spilled L2 on uniform-random GET
 * workloads (e.g. redis-benchmark key:__rand_int__) and turned every
 * fast-path probe into a near-guaranteed L2 miss, regressing GET QPS.
 *
 * FP_VAL_MIN gates fp_put: values < 32B are already so cheap on the
 * shard-locked dict path that copying them through the seqlock buys
 * nothing, while still costing one cacheline per probe.  Skipping them
 * keeps the cache reserved for value sizes where the copy + reply
 * preformatting actually pays back.
 */
#define FP_BUCKETS        2048u
#define FP_BUCKETS_MASK   (FP_BUCKETS - 1u)
#define FP_KEY_MAX        64u
#define FP_VAL_MIN        32u
#define FP_VAL_MAX        256u
#define FP_REPLY_MAX      (16u + FP_VAL_MAX + 2u)  /* "$<len>\r\n" + val + "\r\n" */
#define FP_ENTRY_BYTES    512u   /* cacheline-aligned (8 × 64B) */
#define FP_STATS_SLOTS    64u

struct fp_entry {
    _Atomic uint64_t seq;        /* seqlock counter (see header comment) */
    uint64_t         key_hash;   /* full 64-bit kv_hash */
    uint32_t         key_len;    /* 1..FP_KEY_MAX while occupied, 0 = invalidated */
    uint32_t         reply_len;  /* preformatted RESP bulk length, 0 = invalidated */
    uint8_t          key[FP_KEY_MAX];
    uint8_t          reply[FP_REPLY_MAX];
    uint8_t          _pad[FP_ENTRY_BYTES
                          - sizeof(_Atomic uint64_t)
                          - sizeof(uint64_t)
                          - sizeof(uint32_t)
                          - sizeof(uint32_t)
                          - FP_KEY_MAX
                          - FP_REPLY_MAX];
} __attribute__((aligned(64)));

_Static_assert(sizeof(struct fp_entry) == FP_ENTRY_BYTES,
               "fp_entry must be exactly 512 bytes for cacheline isolation");

struct fp_stat_slot {
    _Atomic uint64_t hits;
    _Atomic uint64_t misses;
    uint8_t          _pad[64u - sizeof(_Atomic uint64_t) * 2u];
} __attribute__((aligned(64)));

_Static_assert(sizeof(struct fp_stat_slot) == 64u,
               "fp_stat_slot must be exactly one cacheline");

/* Phase 10.9 — demand-driven lazy fill.
 *
 * Each STRING SET previously did a forced fp_put: a CAS spin + 512B
 * memcpy + reply preformat, EVEN IF the key was never going to be GET'd
 * again.  In benchmark patterns where each key is set exactly once
 * (uniform random write loops) every fp_put was pure waste — the entry
 * gets evicted before any reader looks at the bucket.  Costs ~12% on
 * t=2 SET as measured against a build that skipped fp_put entirely.
 *
 * Fix: track a 1-bit "demand" per bucket.  Reader sets it on miss
 * (the bucket is hot enough that someone *wanted* a hit), writer
 * checks-and-clears it before doing the expensive seqlock dance.  A
 * SET to a key nobody has GET'd skips fp_put after a single relaxed
 * load.  Recently-read keys still get populated lazily on the *next*
 * write to that bucket.
 *
 * Demand + cached-hash metadata is ~10 KB total, stays L1-resident on
 * every worker.  Stored separately (not packed into fp_entry) so the
 * 512B cacheline-aligned writer payload stays untouched. */
#define FP_DEMAND_HOT  1u
#define FP_DEMAND_COLD 0u

struct fp_cache {
    struct fp_entry  buckets[FP_BUCKETS];      /* 512 KB */
    _Atomic uint8_t  demand[FP_BUCKETS];       /* 1 KB, L1-resident */
    _Atomic uint64_t cached_hash[FP_BUCKETS];  /* 8 KB metadata prefilter */
    _Atomic uint8_t  cached_valid[FP_BUCKETS]; /* 1 KB metadata prefilter */
    struct fp_stat_slot stat_slots[FP_STATS_SLOTS];
    _Atomic uint64_t invalidations;
    _Atomic uint64_t writer_collisions;        /* per-bucket CAS retry count */
    _Atomic uint64_t puts_skipped_cold;        /* fp_put short-circuited, no demand */
} __attribute__((aligned(64)));

/* Lifecycle */
struct fp_cache *fp_cache_new(void);
void             fp_cache_free(struct fp_cache *c);
void             fp_cache_reset(struct fp_cache *c);  /* zero all buckets + stats */

/* Lookup.  Returns 0 on hit (writes a complete RESP bulk reply into out,
 * sets *outlen to reply length).  Returns -1 on miss / unsuitable input /
 * out_cap too small.  Never blocks. */
int  fp_get(struct fp_cache *c,
            const void *key, uint32_t klen, uint64_t khash,
            void *out, uint32_t *outlen, uint32_t out_cap);

/* Insert or overwrite.  Caller must ensure 1 ≤ klen ≤ FP_KEY_MAX and
 * 1 ≤ vlen ≤ FP_VAL_MAX (else this is a silent no-op). */
void fp_put(struct fp_cache *c,
            const void *key, uint32_t klen, uint64_t khash,
            const void *val, uint32_t vlen);

/* Invalidate.  If the bucket currently holds an entry with matching
 * khash (and matching key under hash collision check), clear it.
 * No-op otherwise. */
void fp_del(struct fp_cache *c,
            const void *key, uint32_t klen, uint64_t khash);

/* Cheap prefilter for writer post-hooks.  Returns non-zero if fp_put/fp_del
 * may need to touch the seqlock bucket because either a reader recently
 * missed there (demand hot) or the bucket already holds this exact key. */
int fp_should_refresh(struct fp_cache *c,
                      const void *key, uint32_t klen, uint64_t khash);

int fp_may_hold_key(struct fp_cache *c,
                    const void *key, uint32_t klen, uint64_t khash);

/* Stats snapshot — relaxed loads, slightly skewed under heavy traffic
 * but adequate for INFO / kvs-top display. */
void fp_stats(const struct fp_cache *c,
              uint64_t *hits,
              uint64_t *misses,
              uint64_t *invalidations,
              uint64_t *writer_collisions);

/* Phase 10.9 — count of fp_put calls short-circuited by the demand
 * filter.  Useful for tuning: a high ratio relative to misses means
 * the workload is write-heavy with low read reuse (good — SET path
 * paid the optimization). */
uint64_t fp_puts_skipped_cold(const struct fp_cache *c);

/* Approximate occupancy (count of buckets with even seq > 0 AND
 * val_len > 0).  O(FP_BUCKETS) — use only for INFO / diagnostics. */
uint32_t fp_occupancy(const struct fp_cache *c);

#endif /* KV_FASTPATH_CACHE_H */
