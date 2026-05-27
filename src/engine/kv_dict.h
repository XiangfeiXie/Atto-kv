#ifndef __KV_DICT_H__
#define __KV_DICT_H__

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

/* =====================================================================
 * Unified Dict Engine — the single storage engine for KVStore.
 *
 * Features:
 *  - Single malloc per entry (header + key + type/encoding + payload)
 *  - Progressive rehashing (two hash tables, incremental bucket migration)
 *  - Cached hash value (no recomputation during rehash)
 *  - FNV-1a + Murmur3 finalizer for excellent distribution
 *  - Auto-grow (load factor > 2) and auto-shrink (load factor < 0.1)
 *
 * data[] layout:  [type:1][encoding:1][payload: val_len-2]
 * ===================================================================== */

/* --- Entry types (the "type" byte in data[0]) --- */
enum {
    KV_TYPE_STRING = 0,
    KV_TYPE_HASH = 1,
    KV_TYPE_COUNT
};

/* --- Entry encoding (the "encoding" byte in data[1]) --- */
enum {
    KV_ENCODING_RAW = 0,
    KV_ENCODING_INT = 1,
    KV_ENCODING_PACKED = 2,
    KV_ENCODING_DICT = 3,
};

/* --- Single heap allocation entry --- */
typedef struct kv_entry_s {
    struct kv_entry_s *next;    /* chain pointer              (8B) */
    uint64_t           hash;    /* cached hash                (8B) */
    long long          expire;  /* expiration timestamp (ms)  (8B) */
    uint32_t           key_len; /* key length                 (4B) */
    uint32_t           val_len; /* data[] total size          (4B) */
    char               data[];  /* [type:1][encoding:1][payload...] */
} kv_entry_t;

/* Inline accessors */
static inline uint8_t  entry_type(kv_entry_t *e)     { return (uint8_t)e->data[0]; }
static inline uint8_t  entry_encoding(kv_entry_t *e)  { return (uint8_t)e->data[1]; }
static inline char    *entry_payload(kv_entry_t *e)   { return e->data + 2; }
static inline uint32_t entry_payload_len(kv_entry_t *e){ return e->val_len - 2; }
static inline const char *entry_key(kv_entry_t *e)    { return e->data + e->val_len; }

/* --- Hash table sub-table --- */
typedef struct {
    kv_entry_t **buckets;   /* bucket array               */
    uint32_t     size;      /* number of buckets (pow 2)   */
    uint32_t     size_mask; /* size - 1                    */
    uint32_t     used;      /* entry count in this table   */
} kv_ht_t;

/* --- Dictionary (two tables for progressive rehash) --- */
typedef struct {
    kv_ht_t   ht[2];        /* ht[0] current, ht[1] rehash target */
    int64_t   rehash_idx;   /* -1 = not rehashing, >=0 = next bucket */
    uint32_t  iterators;    /* safe iterator count (pauses rehash)   */
} kv_dict_t;

/* --- Iterator (for RDB save, FLUSHALL, future SCAN) --- */
typedef struct {
    kv_dict_t  *dict;
    int         table;      /* which ht (0 or 1) */
    int64_t     bucket;     /* current bucket index */
    kv_entry_t *next;       /* next entry to yield */
    int         safe;       /* 1 = hold rehash during iteration */
} kv_iter_t;

/* --- Hash function --- */
static inline uint64_t kv_hash(const uint8_t *key, uint32_t len) {
    uint64_t h = 14695981039346656037ULL;  /* FNV-1a offset basis */
    uint32_t i = 0;
    /* Process 8 bytes at a time */
    for (; i + 8 <= len; i += 8) {
        uint64_t word;
        memcpy(&word, key + i, 8);
        h ^= word;
        h *= 1099511628211ULL;             /* FNV prime */
    }
    /* Process remaining bytes */
    for (; i < len; i++) {
        h ^= key[i];
        h *= 1099511628211ULL;
    }
    /* Murmur3 64-bit finalizer */
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return h;
}

/* --- Dict lifecycle --- */
void     dict_init(kv_dict_t *d);
void     dict_destroy(kv_dict_t *d);
void     dict_clear(kv_dict_t *d);

/* --- Core operations --- */
kv_entry_t *dict_find(kv_dict_t *d, const char *key, uint32_t key_len, uint64_t hash);

/* dict_find variant: entry key area stores a pointer to the actual key
 * (not the key bytes themselves). Used by ZSET skiplist encoding where
 * member strings are owned by skiplist nodes.
 *   entry data[]: [type][encoding][real_key_len:4][score:8]
 *   entry key area: [ptr:8] -> external key string
 *   entry->key_len = 8 (sizeof(void*))
 * Comparison dereferences the stored pointer before memcmp. */
kv_entry_t *dict_find_extkey(kv_dict_t *d, const char *key,
                              uint32_t key_len, uint64_t hash);
kv_entry_t *dict_add(kv_dict_t *d, const char *key, uint32_t key_len,
                     const char *data, uint32_t data_len, uint64_t hash);
/* Direct add: builds entry from raw components (no intermediate data buffer) */
kv_entry_t *dict_add_raw(kv_dict_t *d, const char *key, uint32_t key_len,
                         uint8_t type, uint8_t enc,
                         const char *payload, uint32_t payload_len,
                         uint64_t hash);
int         dict_delete(kv_dict_t *d, const char *key, uint32_t key_len, uint64_t hash);

/* --- In-place entry resize (for packed encodings that grow) --- */
kv_entry_t *dict_resize_entry(kv_dict_t *d, kv_entry_t *e, uint32_t new_data_len);

/* --- Iteration --- */
kv_iter_t  *dict_iter_new(kv_dict_t *d, int safe);
kv_entry_t *dict_iter_next(kv_iter_t *it);
kv_entry_t *dict_iter_peek(kv_iter_t *it);
void        dict_iter_free(kv_iter_t *it);

/* --- Stats --- */
uint32_t    dict_count(kv_dict_t *d);
kv_entry_t *dict_get_random_key(kv_dict_t *d);  /* get a random key for expire sampling */

/* =====================================================================
 * Phase 4: sharded dict wrapper.
 *
 * The keyspace is split into KV_NR_SHARDS independent kv_dict_t instances,
 * each protected by its own pthread_rwlock_t.  Callers compute the key
 * hash once, take the appropriate shard's lock (read or write), call the
 * regular dict_* helpers on `kv_dict_for(hash)`, then release the lock.
 *
 * The shard index is derived from the *upper* hash bits so it is
 * statistically independent of the bucket index inside the shard's
 * hashtable (which uses lower bits).
 * ===================================================================== */

#define KV_NR_SHARDS_LOG2  5
#define KV_NR_SHARDS       (1u << KV_NR_SHARDS_LOG2)
#define KV_SHARD_MASK      (KV_NR_SHARDS - 1u)

/* Phase 5.3 — cacheline-isolated shard.
 *
 *   pthread_rwlock_t       56 bytes
 *   kv_dict_t              64 bytes  (ht[0..1] + rehash_idx + iterators)
 *   tail pad                8 bytes
 *   ─────────────────────────────────
 *   total                 128 bytes,  aligned to 128.
 *
 * 128 bytes (2 × 64-byte cachelines) ensures adjacent shards never share
 * a line AND defeats the Intel L2 spatial prefetcher's 128-byte stride —
 * a writer on shard N+1 will not invalidate a reader on shard N.
 * Before Phase 5.3 the struct was 184 B / align-8, so two shards could
 * land in the same cacheline pair under hot multi-writer workloads. */
typedef struct kv_shard {
    pthread_rwlock_t lock;
    kv_dict_t        dict;
    char             _pad[8];
} __attribute__((aligned(128))) kv_shard_t;

_Static_assert(sizeof(struct kv_shard) == 128,
               "kv_shard_t must be exactly 128 bytes (2 cachelines) — "
               "see Phase 5.3 cacheline-isolation comment");
_Static_assert(_Alignof(struct kv_shard) == 128,
               "kv_shard_t must be 128-byte aligned for cacheline isolation");

typedef struct {
    kv_shard_t shards[KV_NR_SHARDS];
} kv_shards_t;

extern kv_shards_t g_shards;

static inline kv_shard_t *kv_shard_for(uint64_t hash) {
    return &g_shards.shards[(hash >> 32) & KV_SHARD_MASK];
}
static inline kv_dict_t *kv_dict_for(uint64_t hash) {
    return &kv_shard_for(hash)->dict;
}

static inline void kv_shard_rdlock_h(uint64_t hash) {
    pthread_rwlock_rdlock(&kv_shard_for(hash)->lock);
}
static inline void kv_shard_wrlock_h(uint64_t hash) {
    pthread_rwlock_wrlock(&kv_shard_for(hash)->lock);
}
static inline void kv_shard_unlock_h(uint64_t hash) {
    pthread_rwlock_unlock(&kv_shard_for(hash)->lock);
}

static inline uint32_t kv_shard_idx(uint64_t hash) {
    return (uint32_t)((hash >> 32) & KV_SHARD_MASK);
}

/* =====================================================================
 * Multi-shard locking helper for dual-key commands
 *  (SMOVE / LMOVE / RPOPLPUSH and similar src+dst patterns).
 *
 * Pre-condition:
 *   The caller's dispatcher already holds wrlock on `primary_hash` (via the
 *   SHARD_LOCK macro in kvstore.c) and only this lock is held by us.
 *
 * Post-condition:
 *   `primary_hash`'s shard wrlock is held (re-acquired if needed) AND
 *   `secondary_hash`'s shard wrlock is held (only when it's a different
 *   shard than primary; otherwise the primary lock already covers it).
 *
 * Strategy:
 *   - same shard: no extra work; *out_dst_locked = 0
 *   - secondary_idx > primary_idx: just take secondary; cannot deadlock
 *     because all callers add secondaries in ascending order vs primary
 *   - secondary_idx < primary_idx: must release primary first to honour
 *     the canonical (ascending) global lock order; then take secondary
 *     then primary. Returns 1 in this case so the caller knows to
 *     re-find the primary entry (another writer may have mutated during
 *     the release window).
 *
 * Cleanup:
 *   The dispatcher's __cleanup__ on _sg releases primary at scope exit.
 *   The caller MUST call kv_shard_release_dst_wr() before returning to
 *   release the secondary if *out_dst_locked was set.
 *
 * Returns 1 if primary was released and re-acquired (caller MUST re-find
 * any entry it had cached on primary), 0 otherwise.
 * ===================================================================== */
static inline int kv_shard_acquire_dst_wr(uint64_t primary_hash,
                                          uint64_t secondary_hash,
                                          int *out_dst_locked) {
    *out_dst_locked = 0;
    uint32_t pi = kv_shard_idx(primary_hash);
    uint32_t si = kv_shard_idx(secondary_hash);
    if (pi == si) return 0;
    if (si > pi) {
        kv_shard_wrlock_h(secondary_hash);
        *out_dst_locked = 1;
        return 0;
    }
    /* si < pi — release primary, take in canonical order */
    kv_shard_unlock_h(primary_hash);
    kv_shard_wrlock_h(secondary_hash);
    kv_shard_wrlock_h(primary_hash);
    *out_dst_locked = 1;
    return 1;
}

static inline void kv_shard_release_dst_wr(uint64_t secondary_hash,
                                           int dst_locked) {
    if (dst_locked) kv_shard_unlock_h(secondary_hash);
}

void     kv_shards_init(void);
void     kv_shards_destroy(void);
uint32_t kv_shards_count(void);     /* sum dict_count across all shards   */
void     kv_shards_clear(void);     /* FLUSHDB / FLUSHALL: wrlock+clear   */

/* Multi-shard iteration helper.
 *
 *   kv_shards_iter_t it; kv_shards_iter_init(&it);
 *   while ((e = kv_shards_iter_next(&it)) != NULL) { use e; }
 *
 * Each shard is rdlocked while being walked; the lock is released as
 * soon as iteration moves to the next shard.  Snapshots are *not*
 * globally consistent across shards (acceptable for KEYS / RDB save). */
typedef struct {
    uint32_t   shard_idx;        /* next shard to enter            */
    kv_iter_t *cur_iter;         /* current shard's iterator (or NULL) */
    int        no_lock;          /* if 1, skip per-shard rdlock — use only
                                    when caller already holds all shard
                                    wrlocks or runs single-threaded
                                    (e.g. fork child for RDB save) */
} kv_shards_iter_t;

void        kv_shards_iter_init(kv_shards_iter_t *it);
void        kv_shards_iter_init_nolock(kv_shards_iter_t *it);
kv_entry_t *kv_shards_iter_next(kv_shards_iter_t *it);
void        kv_shards_iter_free(kv_shards_iter_t *it);

/* Phase 4B.4 — point-in-time snapshot helpers.
 *
 *   kv_shards_lock_all_for_snapshot();
 *   pid_t pid = fork();
 *   if (pid == 0) {                     // child: locks frozen in COW image
 *       use kv_shards_iter_init_nolock(); read; _exit;
 *   }
 *   kv_shards_unlock_all_snapshot();    // parent: workers resume
 *
 * Locks are taken in shard-index order; dispatch only ever holds one
 * shard wrlock at a time (per the dispatcher contract), so this fixed
 * order cannot deadlock against the dispatcher. */
void kv_shards_lock_all_for_snapshot(void);
void kv_shards_unlock_all_snapshot(void);

#endif
