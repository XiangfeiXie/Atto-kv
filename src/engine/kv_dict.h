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
 *  - Auto-grow (load factor > 1) and auto-shrink (load factor < 0.1)
 *
 * data[] layout:  [type:1][encoding:1][key: key_len][payload: val_len-2]
 *
 * Key is stored BEFORE payload (fixed offset data+2).  This makes
 * entry_key() a compile-time constant and avoids moving the key during
 * resize — only the trailing payload shifts.  dict_find's memcmp is
 * also more cache-friendly (key starts right after the header).
 * ===================================================================== */

/* --- Entry type (data[0]) — string only --- */
enum {
    KV_TYPE_STRING = 0,
};

/* --- Entry encoding (data[1]) — RAW or INT --- */
enum {
    KV_ENCODING_RAW = 0,
    KV_ENCODING_INT = 1,
};

/* --- Single heap allocation entry --- */
typedef struct kv_entry_s {
    struct kv_entry_s *next;    /* chain pointer              (8B) */
    uint64_t           hash;    /* cached hash                (8B) */
    long long          expire;  /* expiration timestamp (ms)  (8B) */
    uint32_t           key_len; /* key length                 (4B) */
    uint32_t           val_len; /* data[] total size          (4B) */
    char               data[];  /* [type:1][enc:1][key:key_len][payload:val_len-2] */
} kv_entry_t;

/* ── Inline accessors (Key-First layout) ── */
static inline uint8_t  entry_type(kv_entry_t *e)     { return (uint8_t)e->data[0]; }
static inline uint8_t  entry_encoding(kv_entry_t *e)  { return (uint8_t)e->data[1]; }
static inline const char *entry_key(kv_entry_t *e)    { return e->data + 2; }
static inline char    *entry_payload(kv_entry_t *e)   { return e->data + 2 + e->key_len; }
static inline uint32_t entry_payload_len(kv_entry_t *e){ return e->val_len - 2; }

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
    /* Arena allocator for bulk insert (fullsync slave) */
    char     *arena;        /* pre-allocated arena base */
    size_t    arena_cap;    /* arena total capacity */
    size_t    arena_used;   /* arena bump pointer offset */
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
void     dict_arena_init(kv_dict_t *d, size_t size); /* bulk alloc arena */

/* --- Core operations --- */
kv_entry_t *dict_find(kv_dict_t *d, const char *key, uint32_t key_len, uint64_t hash);

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
void        dict_iter_free(kv_iter_t *it);

/* --- Stats --- */
uint32_t    dict_count(kv_dict_t *d);
kv_entry_t *dict_get_random_key(kv_dict_t *d);  /* get a random key for expire sampling */

/* =====================================================================
 * Single global dict — single-threaded, no sharding (like Redis server.db[0])
 * ===================================================================== */

extern kv_dict_t g_dict;

#endif
