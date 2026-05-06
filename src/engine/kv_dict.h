#ifndef __KV_DICT_H__
#define __KV_DICT_H__

#include <stdint.h>
#include <stdlib.h>

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
    KV_TYPE_LIST   = 1,
    KV_TYPE_SET    = 2,
    KV_TYPE_ZSET   = 3,
    KV_TYPE_HASH   = 4,
    KV_TYPE_COUNT
};

/* --- Entry encoding (the "encoding" byte in data[1]) --- */
enum {
    KV_ENCODING_RAW      = 0,  /* STRING: raw bytes               */
    KV_ENCODING_PACKED   = 1,  /* LIST/HASH/ZSET: inline packed   */
    KV_ENCODING_QUICKLIST = 2, /* LIST: linked ziplist nodes      */
    KV_ENCODING_DICT     = 3,  /* SET/HASH: nested dict           */
    KV_ENCODING_SKIPLIST = 4,  /* ZSET: dict + skiplist dual idx  */
    KV_ENCODING_INT      = 5,  /* STRING: inline int64_t           */
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
uint64_t kv_hash(const uint8_t *key, uint32_t len);

/* --- Dict lifecycle --- */
void     dict_init(kv_dict_t *d);
void     dict_destroy(kv_dict_t *d);
void     dict_clear(kv_dict_t *d);

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

#endif
