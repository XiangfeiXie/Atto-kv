/*
 * fastpath_cache.h — Phase 10: Lock-free seqlock short-key hot cache
 *
 * A 2 MB direct-mapped cache (4096 buckets × 512B) layered ABOVE the
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
 * 1024 buckets × 512B = 512 KB total table.  This fits inside the L2
 * cache of every modern x86-64 / aarch64 desktop and server CPU, so
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
#define FP_BUCKETS        1024u
#define FP_BUCKETS_MASK   (FP_BUCKETS - 1u)
#define FP_KEY_MAX        64u
#define FP_VAL_MIN        32u
#define FP_VAL_MAX        256u
#define FP_REPLY_MAX      (16u + FP_VAL_MAX + 2u)  /* "$<len>\r\n" + val + "\r\n" */
#define FP_ENTRY_BYTES    512u   /* cacheline-aligned (8 × 64B) */

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

struct fp_cache {
    struct fp_entry  buckets[FP_BUCKETS];      /* 2 MB */
    _Atomic uint64_t hits;
    _Atomic uint64_t misses;
    _Atomic uint64_t invalidations;
    _Atomic uint64_t writer_collisions;        /* per-bucket CAS retry count */
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

/* Stats snapshot — relaxed loads, slightly skewed under heavy traffic
 * but adequate for INFO / kvs-top display. */
void fp_stats(const struct fp_cache *c,
              uint64_t *hits,
              uint64_t *misses,
              uint64_t *invalidations,
              uint64_t *writer_collisions);

/* Approximate occupancy (count of buckets with even seq > 0 AND
 * val_len > 0).  O(FP_BUCKETS) — use only for INFO / diagnostics. */
uint32_t fp_occupancy(const struct fp_cache *c);

#endif /* KV_FASTPATH_CACHE_H */
