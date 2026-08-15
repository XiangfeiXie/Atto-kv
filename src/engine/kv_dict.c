#include "kv_dict.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* =====================================================================
 * Hash function: FNV-1a 64-bit + Murmur3 64-bit finalizer
 *
 * FNV gives every input byte a contribution; Murmur3 finalizer
 * avalanches high bits so that "hash & size_mask" distributes evenly
 * even for small power-of-two tables.
 * ===================================================================== */

/* kv_hash is now static inline in kv_dict.h */

/* =====================================================================
 * Internal helpers
 * ===================================================================== */

#define DICT_INIT_SIZE  16          /* must be power of 2 */
#define DICT_MAX_SIZE   (1U << 24)  /* ~16M buckets max */

/* Rehash batch size — scales with ht[0].size so larger tables migrate
 * proportionally more buckets per operation, keeping rehash completion
 * roughly constant at ~60 operations regardless of table size. */
static inline int rehash_batch(kv_dict_t *d) {
    int n = (int)(d->ht[0].size >> 6);  /* d->ht[0].size / 64 */
    return n < 8 ? 8 : n;
}

static uint32_t next_pow2(uint32_t v) {
    if (v <= DICT_INIT_SIZE) return DICT_INIT_SIZE;
    v--;
    v |= v >> 1;  v |= v >> 2;
    v |= v >> 4;  v |= v >> 8;
    v |= v >> 16;
    return v + 1;
}

/* Allocate a new entry.  Key-First layout:
 *
 *   [kv_entry_t header][type:1][enc:1][key:key_len][payload:val_len-2]
 *
 * When dict has an arena, allocates from arena (bump pointer); falls back to malloc.
 *
 * The caller (dict_add) passes type+enc+payload as a contiguous buffer in `data`.
 * We split it: type+enc go at data[0..1], key at data[2..], payload at the tail.
 * This is transparent to all callers because only the accessor macros touch data[]
 * layout.                                                                */
static kv_entry_t *entry_alloc(kv_dict_t *d, const char *key, uint32_t key_len,
                                const char *data, uint32_t data_len,
                                uint64_t hash)
{
    uint32_t total = (uint32_t)sizeof(kv_entry_t) + data_len + key_len;
    kv_entry_t *e;
    if (d->arena && d->arena_used + total <= d->arena_cap) {
        e = (kv_entry_t *)(d->arena + d->arena_used);
        d->arena_used += total;
    } else {
        e = (kv_entry_t *)malloc((size_t)total);
    }
    if (!e) return NULL;

    e->next    = NULL;
    e->hash    = hash;
    e->expire  = 0;
    e->key_len = key_len;
    e->val_len = data_len;   /* = 2 + payload_len */

    /* type + encoding (always 2 bytes at data[0..1]) */
    if (data && data_len >= 2)
        memcpy(e->data, data, 2);

    /* key at data + 2 (compile-time constant offset) */
    memcpy(e->data + 2, key, key_len);

    /* payload after key (remaining data_len-2 bytes) */
    if (data && data_len > 2)
        memcpy(e->data + 2 + key_len, data + 2, (size_t)(data_len - 2));

    return e;
}

/* =====================================================================
 * Hashtable sub-operations
 * ===================================================================== */

static void ht_init(kv_ht_t *ht) {
    ht->buckets   = NULL;
    ht->size      = 0;
    ht->size_mask = 0;
    ht->used      = 0;
}

static int ht_alloc(kv_ht_t *ht, uint32_t size) {
    ht->size      = size;
    ht->size_mask = size - 1;
    ht->used      = 0;
    ht->buckets   = (kv_entry_t **)calloc((size_t)size, sizeof(kv_entry_t *));
    return ht->buckets ? 0 : -1;
}

static void ht_destroy(kv_ht_t *ht) {
    free(ht->buckets);
    ht_init(ht);
}

/* =====================================================================
 * Dict lifecycle
 * ===================================================================== */

void dict_init(kv_dict_t *d) {
    ht_init(&d->ht[0]);
    ht_init(&d->ht[1]);
    d->rehash_idx = -1;
    d->iterators  = 0;
    d->arena      = NULL;
    d->arena_cap  = 0;
    d->arena_used = 0;
    /* Start with a small table so the first adds trigger the lazy-init
       path inside dict_add rather than pre-allocating here. */
}

void dict_destroy(kv_dict_t *d) {
    dict_clear(d);
    ht_destroy(&d->ht[0]);
    ht_destroy(&d->ht[1]);
    d->rehash_idx = -1;
    d->iterators  = 0;
}

void dict_clear(kv_dict_t *d) {
    int has_arena = (d->arena != NULL);
    const char *arena_end = d->arena + d->arena_cap;

    for (int t = 0; t < 2; t++) {
        kv_ht_t *ht = &d->ht[t];
        if (!ht->buckets) continue;
        for (uint32_t i = 0; i < ht->size; i++) {
            kv_entry_t *e = ht->buckets[i];
            while (e) {
                kv_entry_t *next = e->next;
                /* Only free if NOT in arena (arena entries live in bulk block) */
                if (!has_arena ||
                    (char *)e < d->arena || (char *)e >= arena_end) {
                    free(e);
                }
                e = next;
            }
        }
        ht_init(ht);
    }
    d->rehash_idx = -1;

    if (has_arena) {
        free(d->arena);
        d->arena = NULL;
        d->arena_cap = 0;
        d->arena_used = 0;
    }
}

/* Pre-allocate hashtable for bulk load. Must be called on an empty dict
 * (after dict_clear). Eliminates progressive rehash overhead during
 * large batch inserts like fullsync. */
void dict_reserve(kv_dict_t *d, uint32_t count) {
    if (d->ht[0].buckets || d->ht[1].buckets || d->rehash_idx >= 0) {
        /* Not empty — caller should have cleared first */
        dict_clear(d);
    }
    uint32_t size = next_pow2(count);
    if (size < DICT_INIT_SIZE) size = DICT_INIT_SIZE;
    if (size > DICT_MAX_SIZE) size = DICT_MAX_SIZE;
    ht_alloc(&d->ht[0], size);
}

/* Pre-allocate entry arena for bulk insert (fullsync slave).
 * All entries inserted after this will be carved from the arena
 * instead of individual malloc() calls.  Arena persists until
 * the next dict_clear(), which frees the entire block at once.
 *
 * Call after dict_clear() + dict_reserve() on the empty dict.
 * Safe to call even if no arena is needed (size=0 is a no-op). */
void dict_arena_init(kv_dict_t *d, size_t size) {
    if (size == 0) return;
    d->arena = (char *)malloc(size);
    if (d->arena) {
        d->arena_cap  = size;
        d->arena_used = 0;
    }
}


/* =====================================================================
 * Progressive rehash: migrate up to 'steps' buckets from ht[0] to ht[1].
 * Called inside dict_add / dict_delete — amortised O(1).
 * ===================================================================== */

static void dict_rehash_step(kv_dict_t *d, int steps) {
    if (d->rehash_idx < 0 || d->iterators > 0) return;

    while (steps-- > 0) {
        /* Find next non-empty bucket */
        while (d->rehash_idx < (int64_t)d->ht[0].size &&
               d->ht[0].buckets[d->rehash_idx] == NULL)
        {
            d->rehash_idx++;
        }

        if (d->rehash_idx >= (int64_t)d->ht[0].size) {
            /* Rehash complete: swap tables */
            ht_destroy(&d->ht[0]);
            d->ht[0] = d->ht[1];
            ht_init(&d->ht[1]);
            d->rehash_idx = -1;
            return;
        }

        /* Migrate the entire chain at this bucket */
        kv_entry_t *chain = d->ht[0].buckets[d->rehash_idx];
        d->ht[0].buckets[d->rehash_idx] = NULL;

        while (chain) {
            kv_entry_t *next = chain->next;
            uint32_t idx = (uint32_t)(chain->hash & d->ht[1].size_mask);

            chain->next = d->ht[1].buckets[idx];
            d->ht[1].buckets[idx] = chain;

            d->ht[0].used--;
            d->ht[1].used++;
            chain = next;
        }
        d->rehash_idx++;
    }
}

/* =====================================================================
 * Resize trigger logic
 * ===================================================================== */

static int dict_maybe_expand(kv_dict_t *d) {
    /* Already rehashing? */
    if (d->rehash_idx >= 0) return 0;

    uint32_t used = d->ht[0].used;
    uint32_t size = d->ht[0].size;

    /* Lazy initialise */
    if (size == 0) return ht_alloc(&d->ht[0], DICT_INIT_SIZE);

    /* Expand when load factor >= 1 (Redis-style: grow before the table
     * fills, so new_size = next_pow2(used*2) = 2*size — a 2x jump, not the
     * 4x jump `used > size` produced). */
    if (size < DICT_MAX_SIZE && used >= size) {
        uint32_t new_size = next_pow2(used * 2);
        if (new_size > DICT_MAX_SIZE) new_size = DICT_MAX_SIZE;
        if (new_size <= size) return 0;  /* already at max */
        if (ht_alloc(&d->ht[1], new_size) < 0) return -1;
        d->rehash_idx = 0;
        return 1;
    }
    return 0;
}

static int dict_maybe_shrink(kv_dict_t *d) {
    if (d->rehash_idx >= 0) return 0;

    uint32_t used = d->ht[0].used;
    uint32_t size = d->ht[0].size;

    /* Shrink when load factor < 0.1 and above minimum */
    if (size > DICT_INIT_SIZE && used < size / 10) {
        uint32_t new_size = next_pow2(used * 2);
        if (new_size >= size) return 0;
        if (ht_alloc(&d->ht[1], new_size) < 0) return -1;
        d->rehash_idx = 0;
        return 1;
    }
    return 0;
}

/* =====================================================================
 * Core operations
 * ===================================================================== */

kv_entry_t *dict_find(kv_dict_t *d, const char *key, uint32_t key_len,
                      uint64_t hash)
{
    /* Fast return when dict is empty (no tables allocated yet) */
    if (d->ht[0].size == 0 && d->rehash_idx < 0)
        return NULL;


    {
        uint32_t idx = (uint32_t)(hash & d->ht[0].size_mask);
        kv_entry_t *e = d->ht[0].buckets[idx];
        while (e) {
            if (e->key_len == key_len &&
                memcmp(entry_key(e), key, key_len) == 0)
                return e;
            e = e->next;
        }
    }

    /* Search ht[1] only when rehash is actually in progress */
    if (d->rehash_idx >= 0 && d->ht[1].size > 0) {
        uint32_t idx = (uint32_t)(hash & d->ht[1].size_mask);
        kv_entry_t *e = d->ht[1].buckets[idx];
        while (e) {
            if (e->key_len == key_len &&
                memcmp(entry_key(e), key, key_len) == 0)
                return e;
            e = e->next;
        }
    }

    return NULL;
}


/* ── Insertion helper: expand, rehash, select target ht, lazy init ── */

static kv_ht_t *dict_prepare_insert(kv_dict_t *d) {
    dict_maybe_expand(d);

    if (d->rehash_idx >= 0) {
        dict_rehash_step(d, rehash_batch(d));
        if (d->ht[1].size == 0) goto use_ht0;
        return &d->ht[1];
    }

use_ht0:
    if (d->ht[0].size == 0) {
        if (ht_alloc(&d->ht[0], DICT_INIT_SIZE) < 0)
            return NULL;
    }
    return &d->ht[0];
}

kv_entry_t *dict_add(kv_dict_t *d, const char *key, uint32_t key_len,
                     const char *data, uint32_t data_len, uint64_t hash)
{
    kv_ht_t *ht = dict_prepare_insert(d);
    if (!ht) return NULL;

    kv_entry_t *e = entry_alloc(d, key, key_len, data, data_len, hash);
    if (!e) return NULL;

    uint32_t idx = (uint32_t)(hash & ht->size_mask);
    e->next = ht->buckets[idx];
    ht->buckets[idx] = e;
    ht->used++;
    return e;
}

kv_entry_t *dict_add_raw(kv_dict_t *d, const char *key, uint32_t key_len,
                         uint8_t type, uint8_t enc,
                         const char *payload, uint32_t payload_len,
                         uint64_t hash)
{
    kv_ht_t *ht = dict_prepare_insert(d);
    if (!ht) return NULL;

    uint32_t data_len = (uint32_t)(2 + payload_len);
    kv_entry_t *e = entry_alloc(d, key, key_len, NULL, data_len, hash);
    if (!e) return NULL;

    e->data[0] = (char)type;
    e->data[1] = (char)enc;
    if (payload && payload_len)
        memcpy(entry_payload(e), payload, payload_len);

    uint32_t idx = (uint32_t)(hash & ht->size_mask);
    e->next = ht->buckets[idx];
    ht->buckets[idx] = e;
    ht->used++;
    return e;
}

int dict_delete(kv_dict_t *d, const char *key, uint32_t key_len,
                uint64_t hash)
{
    kv_entry_t *prev = NULL;

    for (int t = 0; t < 2; t++) {
        kv_ht_t *ht = &d->ht[t];
        if (ht->size == 0) continue;

        uint32_t idx = (uint32_t)(hash & ht->size_mask);
        kv_entry_t *e = ht->buckets[idx];
        prev = NULL;

        while (e) {
            if (e->hash == hash && e->key_len == key_len &&
                memcmp(entry_key(e), key, key_len) == 0)
            {
                /* Unlink */
                if (prev)
                    prev->next = e->next;
                else
                    ht->buckets[idx] = e->next;

                /* Arena entries must not be freed individually —
                 * they belong to the bulk arena freed in dict_clear(). */
                int has_arena = (d->arena != NULL);
                const char *arena_end = d->arena + d->arena_cap;
                if (!has_arena ||
                    (char *)e < d->arena || (char *)e >= arena_end) {
                    free(e);
                }
                ht->used--;
                dict_rehash_step(d, rehash_batch(d));
                dict_maybe_shrink(d);
                return 0;
            }
            prev = e;
            e = e->next;
        }
    }

    dict_rehash_step(d, rehash_batch(d));
    return -1;  /* not found */
}

kv_entry_t *dict_resize_entry(kv_dict_t *d, kv_entry_t *e,
                               uint32_t new_data_len)
{
    /* Also advance rehash: SET-on-existing-key is the dominant path
     * under overwrite-heavy workloads and must not stall migration. */
    dict_rehash_step(d, rehash_batch(d));

    uint32_t old_val_len = e->val_len;

    if (new_data_len == old_val_len)
        return e;

    /* Key-First layout: key sits at data+2 (fixed), payload at data+2+key_len.
     * Shrinking or growing only affects the trailing payload — no key memmove
     * and no chain pointer fixup needed unless realloc moves the whole entry. */

    if (new_data_len < old_val_len) {
        /* Shrink: only the tail payload shrinks, key doesn't move */
        e->val_len = new_data_len;
        return e;
    }

    /* Grow: realloc to make room for larger payload at the tail */
    uint32_t old_total = (uint32_t)sizeof(kv_entry_t) + old_val_len + e->key_len;
    uint32_t new_total = (uint32_t)sizeof(kv_entry_t) + new_data_len + e->key_len;

    kv_entry_t *ne = (kv_entry_t *)realloc(e, (size_t)new_total);
    if (!ne) return NULL;

    ne->val_len = new_data_len;

    /* Fix up bucket chain pointers if realloc moved the entry */
    if (ne != e) {
        for (int t = 0; t < 2; t++) {
            kv_ht_t *ht = &d->ht[t];
            if (ht->size == 0) continue;
            uint32_t idx = (uint32_t)(ne->hash & ht->size_mask);
            kv_entry_t **pp = &ht->buckets[idx];
            while (*pp) {
                if (*pp == e) {
                    *pp = ne;
                    goto done;
                }
                pp = &(*pp)->next;
            }
        }
    }
done:
    return ne;
}

/* =====================================================================
 * Iteration
 * ===================================================================== */

kv_iter_t *dict_iter_new(kv_dict_t *d, int safe) {
    kv_iter_t *it = (kv_iter_t *)calloc(1, sizeof(kv_iter_t));
    if (!it) return NULL;

    it->dict   = d;
    it->table  = 0;
    it->bucket = -1;
    it->next   = NULL;
    it->safe   = safe;

    if (safe) d->iterators++;

    /* Find the first entry */
    dict_iter_next(it);
    return it;
}

kv_entry_t *dict_iter_next(kv_iter_t *it) {
    kv_entry_t *result = it->next;

    /* Advance */
    while (1) {
        if (it->next) {
            it->next = it->next->next;
            if (it->next) break;
        }

        /* Move to next bucket */
        it->bucket++;
        int max_table = (it->dict->rehash_idx >= 0) ? 2 : 1;

        while (it->table < max_table) {
            kv_ht_t *ht = &it->dict->ht[it->table];
            if (ht->buckets && it->bucket < (int64_t)ht->size) {
                while (it->bucket < (int64_t)ht->size) {
                    if (ht->buckets[it->bucket]) {
                        it->next = ht->buckets[it->bucket];
                        goto done;
                    }
                    it->bucket++;
                }
            }
            it->table++;
            it->bucket = 0;
        }
        it->next = NULL;
        break;
    }
done:
    return result;
}


void dict_iter_free(kv_iter_t *it) {
    if (!it) return;
    if (it->safe && it->dict->iterators > 0)
        it->dict->iterators--;
    free(it);
}

/* =====================================================================
 * Stats
 * ===================================================================== */

uint32_t dict_count(kv_dict_t *d) {
    uint32_t n = d->ht[0].used + d->ht[1].used;
    return n;
}

/* Get a random key from the dictionary for expire sampling.
 * Returns NULL if the dictionary is empty. */
kv_entry_t *dict_get_random_key(kv_dict_t *d) {
    if (d->ht[0].used == 0 && d->ht[1].used == 0) {
        return NULL;
    }

    /* Choose a non-empty hash table */
    int table = (d->ht[1].used > 0) ? 1 : 0;
    kv_ht_t *ht = &d->ht[table];

    if (ht->used == 0) return NULL;

    /* Random bucket index */
    uint32_t bucket_idx = (uint32_t)rand() % ht->size;

    /* Find first non-empty bucket (linear probing) */
    uint32_t max_steps = ht->size;
    while (max_steps-- > 0) {
        if (ht->buckets[bucket_idx] != NULL) {
            /* Count chain length */
            kv_entry_t *entry = ht->buckets[bucket_idx];
            int chain_len = 0;
            kv_entry_t *tmp = entry;
            while (tmp) {
                chain_len++;
                tmp = tmp->next;
            }

            /* Random entry in chain */
            int target = rand() % chain_len;
            for (int i = 0; i < target; i++) {
                entry = entry->next;
            }

            return entry;
        }
        bucket_idx = (bucket_idx + 1) % ht->size;
    }

    return NULL;
}

/* =====================================================================
 * Single global dict — single-threaded, no sharding
 * ===================================================================== */

kv_dict_t g_dict;
