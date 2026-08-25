#ifndef __KV_DICT_H__
#define __KV_DICT_H__

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* =====================================================================
 * Hash-table storage engine (single global dict).
 *
 *  - One malloc per entry: header + key + payload
 *  - Progressive rehashing (two tables, incremental bucket migration)
 *  - Cached hash value (no recompute during rehash)
 *  - FNV-1a + Murmur3 finalizer
 *  - Auto-grow (load factor >= 1)
 *
 * Entry layout: [header][key: key_len][payload: val_len]
 *
 * The key sits at a fixed offset (start of data[]), so it never moves
 * during resize — only the trailing payload shifts.
 * ===================================================================== */

/* --- Single heap allocation entry --- */
typedef struct kv_entry_s {
    struct kv_entry_s *next;    /* chain pointer              (8B) */
    uint64_t           hash;    /* cached hash                (8B) */
    long long          expire;  /* expiration timestamp (ms)  (8B) */
    uint32_t           key_len; /* key length                 (4B) */
    uint32_t           val_len; /* payload length             (4B) */
    char               data[];  /* [key:key_len][payload:val_len] */
} kv_entry_t;

/* ── Inline accessors ── */
static inline const char *entry_key(kv_entry_t *e)     { return e->data; }
static inline char       *entry_payload(kv_entry_t *e) { return e->data + e->key_len; }
static inline uint32_t    entry_payload_len(kv_entry_t *e) { return e->val_len; }

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
                     const char *payload, uint32_t payload_len, uint64_t hash);
int         dict_delete(kv_dict_t *d, const char *key, uint32_t key_len, uint64_t hash);

/* --- In-place entry resize (payload grows or shrinks) --- */
kv_entry_t *dict_resize_entry(kv_dict_t *d, kv_entry_t *e, uint32_t new_payload_len);

/* --- Iteration --- */
kv_iter_t  *dict_iter_new(kv_dict_t *d, int safe);
kv_entry_t *dict_iter_next(kv_iter_t *it);
void        dict_iter_free(kv_iter_t *it);

/* Peek at the next entry without advancing the iterator.
 * Returns NULL when no more entries are available.
 * Useful for batch serialisation: check entry size before committing. */
static inline kv_entry_t *dict_iter_peek(kv_iter_t *it) {
    return it ? it->next : NULL;
}

/* --- Stats --- */
uint32_t    dict_count(kv_dict_t *d);
kv_entry_t *dict_get_random_key(kv_dict_t *d);  /* get a random key for expire sampling */

/* =====================================================================
 * Single global dict — single-threaded, no sharding (like Redis server.db[0])
 * ===================================================================== */

extern kv_dict_t g_dict;

#endif
