#ifndef __KV_DICT_H__
#define __KV_DICT_H__

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

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
void     dict_reserve(kv_dict_t *d, uint32_t count); /* pre-alloc for bulk load */

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
 * Single global dict
 * ===================================================================== */

typedef struct {
    kv_dict_t dict;
} kv_store_t;

extern kv_store_t g_store;

static inline kv_dict_t *kv_dict_global(void) {
    return &g_store.dict;
}

/* =====================================================================
 * Single global dict access helpers
 * ===================================================================== */
static inline kv_dict_t *kv_dict_for(uint64_t hash) {
    (void)hash;  /* unused in single-dict mode */
    return &g_store.dict;
}

void     kv_store_init(void);
void     kv_store_destroy(void);
uint32_t kv_store_count(void);
void     kv_store_clear(void);
void     kv_store_reserve(uint32_t count);

/* Single dict iteration helper */
typedef struct {
    kv_iter_t *iter;
} kv_store_iter_t;

void        kv_store_iter_init(kv_store_iter_t *it);
kv_entry_t *kv_store_iter_next(kv_store_iter_t *it);
void        kv_store_iter_free(kv_store_iter_t *it);

/* Locking helpers for concurrent access protection */
extern pthread_rwlock_t g_store_rwlock;  /* Global rwlock for dict access */
void kv_store_rdlock(void);    /* Acquire read lock (shared) */
void kv_store_wrlock(void);    /* Acquire write lock (exclusive) */
void kv_store_unlock(void);    /* Release lock */

/* Snapshot helpers (no-op in single-dict mode) */
static inline void kv_store_lock_for_snapshot(void) { }
static inline void kv_store_unlock_snapshot(void) { }

void     kv_store_init(void);
void     kv_store_destroy(void);
uint32_t kv_store_count(void);
void     kv_store_clear(void);

#endif
