#include "kv_type_zset.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <sys/time.h>

/* =====================================================================
 * Static RESP fragments
 * ===================================================================== */

#define S(id, str) \
    static const char id[] = str; \
    static const int  id##_LEN = (int)(sizeof(str) - 1)

S(REP_OK,        "+OK\r\n");
S(REP_NIL,       "$-1\r\n");
S(REP_ONE,       ":1\r\n");
S(REP_ZERO,      ":0\r\n");
S(REP_ERR_ARGS,  "-ERR wrong number of arguments\r\n");
S(REP_ERR_UNK,   "-ERR unknown command\r\n");
S(REP_WRONGTYPE, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");

#undef S

/* =====================================================================
 * Limits
 * ===================================================================== */

#define ZSET_PACKED_MAX_ELEMS  128
#define ZSET_PACKED_MAX_BYTES  4096
#define ZSET_SHRINK_THRESHOLD   64
#define ZSKIPLIST_MAXLEVEL      32

/* =====================================================================
 * Packed encoding helpers
 *
 * Format: [count:4][member1_len:4][member1_bytes...][score1:8]...
 * (sorted by score ascending)
 * ===================================================================== */

static inline uint32_t pk_read_u32(const char *p) {
    uint32_t v; memcpy(&v, p, 4); return v;
}
static inline void pk_write_u32(char *p, uint32_t v) {
    memcpy(p, &v, 4);
}
static inline double pk_read_score(const char *p) {
    double v; memcpy(&v, p, 8); return v;
}
static inline void pk_write_score(char *p, double v) {
    memcpy(p, &v, 8);
}

/* Size of one packed element: 4 + member_len + 8 */
#define PK_ELEM_SIZE(mlen) (4 + (uint32_t)(mlen) + 8)

/* Find member in packed zset, returns pointer to its member_len field or NULL */
static char *pk_find_member(char *data, uint32_t data_len,
                            const char *member, uint32_t member_len) {
    uint32_t count = pk_read_u32(data);
    char *p = data + 4;
    for (uint32_t i = 0; i < count; i++) {
        uint32_t mlen = pk_read_u32(p);
        if (mlen == member_len && memcmp(p + 4, member, mlen) == 0)
            return p;
        p += 4 + mlen + 8;  /* member_len + member + score */
    }
    return NULL;
}

/* Get score from element at pointer p (p points to member_len) */
static double pk_get_score(char *p) {
    uint32_t mlen = pk_read_u32(p);
    return pk_read_score(p + 4 + mlen);
}

/* =====================================================================
 * Score parsing
 * ===================================================================== */

static int parse_score(kvs_token_t t, double *out) {
    char buf[64];
    int n = t.len < 63 ? t.len : 63;
    memcpy(buf, t.ptr, (size_t)n);
    buf[n] = '\0';
    char *end = NULL;
    *out = strtod(buf, &end);
    if (end == buf) return -1;
    /* Accept inf, -inf, nan etc that strtod handles */
    if (*end != '\0' && *end != '\r' && *end != '\n') return -1;
    return 0;
}

/* =====================================================================
 * Engine access
 * ===================================================================== */

extern kv_dict_t g_dict;
extern long long get_time_now(void);

static const char *key_from_token(kvs_token_t t, char *buf, int sz) {
    int n = t.len < sz - 1 ? t.len : sz - 1;
    memcpy(buf, t.ptr, (size_t)n);
    buf[n] = '\0';
    return buf;
}

static int check_expiry(kv_entry_t *e, const char *key, uint64_t hash) {
    if (e && e->expire && get_time_now() > e->expire) {
        zset_cleanup_entry(e);
        dict_delete(&g_dict, key, (uint32_t)strlen(key), hash);
        return 1;
    }
    return 0;
}

/* =====================================================================
 * Skiplist node allocation
 * ===================================================================== */

static zskiplist_node_t *zsl_node_alloc(int level, double score,
                                         const char *member, uint32_t member_len) {
    size_t sz = sizeof(zskiplist_node_t) + (size_t)level * sizeof(struct {
        zskiplist_node_t *forward;
        uint32_t span;
    });
    zskiplist_node_t *n = (zskiplist_node_t *)malloc(sz);
    if (!n) return NULL;
    n->score = score;
    n->member = (char *)malloc(member_len);
    if (!n->member) { free(n); return NULL; }
    memcpy(n->member, member, member_len);
    n->member_len = member_len;
    n->backward = NULL;
    for (int i = 0; i < level; i++) {
        n->level[i].forward = NULL;
        n->level[i].span = 0;
    }
    return n;
}

static void zsl_node_free(zskiplist_node_t *n) {
    if (!n) return;
    free(n->member);
    free(n);
}

/* =====================================================================
 * Skiplist random level
 * ===================================================================== */

static int zsl_random_level(void) {
    int level = 1;
    while ((rand() & 3) == 0 && level < ZSKIPLIST_MAXLEVEL)
        level++;
    return level;
}

/* =====================================================================
 * Skiplist insert
 * ===================================================================== */

static zskiplist_node_t *zsl_insert(kv_entry_t *e, double score,
                                     const char *member, uint32_t member_len) {
    zskiplist_node_t *head = zset_head(e);
    zskiplist_node_t *update[ZSKIPLIST_MAXLEVEL];
    uint32_t rank[ZSKIPLIST_MAXLEVEL];
    int max_level = zset_level(e);

    /* Find insertion point for each level */
    zskiplist_node_t *x = head;
    for (int i = max_level - 1; i >= 0; i--) {
        rank[i] = (i == max_level - 1) ? 0 : rank[i + 1];
        while (x->level[i].forward &&
               (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                 memcmp(x->level[i].forward->member, member,
                        x->level[i].forward->member_len < member_len ?
                        x->level[i].forward->member_len : member_len) < 0))) {
            rank[i] += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    int level = zsl_random_level();
    if (level > max_level) {
        for (int i = max_level; i < level; i++) {
            rank[i] = 0;
            update[i] = head;
            update[i]->level[i].span = zset_count(e);
        }
        zset_set_level(e, level);
        max_level = level;
    }

    x = zsl_node_alloc(level, score, member, member_len);
    if (!x) return NULL;

    for (int i = 0; i < level; i++) {
        x->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = x;
        x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }
    /* Increment span for untouched levels */
    for (int i = level; i < max_level; i++)
        update[i]->level[i].span++;

    x->backward = (update[0] == head) ? NULL : update[0];
    if (x->level[0].forward)
        x->level[0].forward->backward = x;
    else
        zset_set_tail(e, x);

    zset_set_count(e, zset_count(e) + 1);
    return x;
}

/* =====================================================================
 * Skiplist delete (node must exist in the skiplist)
 * ===================================================================== */

static void zsl_delete(kv_entry_t *e, zskiplist_node_t *x,
                        zskiplist_node_t **update) {
    int max_level = zset_level(e);

    for (int i = 0; i < max_level; i++) {
        if (update[i]->level[i].forward == x) {
            update[i]->level[i].span += x->level[i].span - 1;
            update[i]->level[i].forward = x->level[i].forward;
        } else {
            update[i]->level[i].span--;
        }
    }
    if (x->level[0].forward)
        x->level[0].forward->backward = x->backward;
    else
        zset_set_tail(e, x->backward);

    /* Reduce level if top levels are empty */
    while (max_level > 1 && zset_head(e)->level[max_level - 1].forward == NULL) {
        max_level--;
        zset_set_level(e, max_level);
    }

    zset_set_count(e, zset_count(e) - 1);
    zsl_node_free(x);
}

/* =====================================================================
 * Skiplist: find node and collect update[] for deletion
 * Returns node if found, NULL otherwise.
 * ===================================================================== */

static zskiplist_node_t *zsl_find_with_update(kv_entry_t *e, double score,
                                               const char *member,
                                               uint32_t member_len,
                                               zskiplist_node_t **update) {
    zskiplist_node_t *head = zset_head(e);
    zskiplist_node_t *x = head;
    int max_level = zset_level(e);

    for (int i = max_level - 1; i >= 0; i--) {
        while (x->level[i].forward &&
               (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                 memcmp(x->level[i].forward->member, member,
                        x->level[i].forward->member_len < member_len ?
                        x->level[i].forward->member_len : member_len) < 0))) {
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    x = x->level[0].forward;
    if (x && x->score == score && x->member_len == member_len &&
        memcmp(x->member, member, member_len) == 0)
        return x;
    return NULL;
}

/* =====================================================================
 * Skiplist find by score only (for ZCOUNT/ZRANGEBYSCORE)
 * ===================================================================== */

static zskiplist_node_t *zsl_first_ge(kv_entry_t *e, double score) {
    zskiplist_node_t *head = zset_head(e);
    zskiplist_node_t *x = head;
    int max_level = zset_level(e);

    for (int i = max_level - 1; i >= 0; i--) {
        while (x->level[i].forward && x->level[i].forward->score < score)
            x = x->level[i].forward;
    }
    return x->level[0].forward;
}

/* =====================================================================
 * Skiplist: get rank of member (0-based), -1 if not found
 * ===================================================================== */

static int zsl_get_rank(kv_entry_t *e, double score, const char *member,
                         uint32_t member_len) {
    zskiplist_node_t *head = zset_head(e);
    zskiplist_node_t *x = head;
    uint32_t rank = 0;
    int max_level = zset_level(e);

    for (int i = max_level - 1; i >= 0; i--) {
        while (x->level[i].forward &&
               (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                 memcmp(x->level[i].forward->member, member,
                        x->level[i].forward->member_len < member_len ?
                        x->level[i].forward->member_len : member_len) <= 0))) {
            rank += x->level[i].span;
            x = x->level[i].forward;
        }
    }

    if (x && x->score == score && x->member_len == member_len &&
        memcmp(x->member, member, member_len) == 0)
        return (int)(rank - 1);
    return -1;
}

/* =====================================================================
 * Skiplist: get node by 0-based rank
 * ===================================================================== */

static zskiplist_node_t *zsl_get_by_rank(kv_entry_t *e, uint32_t rank) {
    zskiplist_node_t *head = zset_head(e);
    zskiplist_node_t *x = head;
    uint32_t traversed = 0;
    int max_level = zset_level(e);

    for (int i = max_level - 1; i >= 0; i--) {
        while (x->level[i].forward && traversed + x->level[i].span <= rank + 1) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        if (traversed == rank + 1) return x;
    }
    return NULL;
}

/* =====================================================================
 * Create sentinel nodes for a new skiplist
 * ===================================================================== */

static void zsl_create_sentinels(kv_entry_t *e) {
    zskiplist_node_t *head = zsl_node_alloc(ZSKIPLIST_MAXLEVEL, -HUGE_VAL,
                                             "__head__", 8);
    zskiplist_node_t *tail = zsl_node_alloc(1, HUGE_VAL, "__tail__", 8);
    if (head) {
        for (int i = 0; i < ZSKIPLIST_MAXLEVEL; i++) {
            head->level[i].forward = tail;
            head->level[i].span = 0;
        }
        head->backward = NULL;
    }
    if (tail) {
        tail->backward = head;
    }
    zset_set_head(e, head);
    zset_set_tail(e, tail);
    zset_set_level(e, 1);
    zset_set_count(e, 0);
}

/* =====================================================================
 * Build empty packed zset
 * ===================================================================== */

static void zset_build_empty(char *out_data) {
    out_data[0] = (char)KV_TYPE_ZSET;
    out_data[1] = (char)KV_ENCODING_PACKED;
    pk_write_u32(out_data + 2, 0);
}

#define ZSET_EMPTY_SIZE 6  /* type:1 + encoding:1 + count:4 */

/* =====================================================================
 * Zset cardinality
 * ===================================================================== */

static uint32_t zset_card(kv_entry_t *e) {
    if (entry_encoding(e) == KV_ENCODING_PACKED)
        return pk_read_u32(e->data + 2);
    return zset_count(e);
}

/* =====================================================================
 * PACKED: remove element at pointer p, compact
 * ===================================================================== */

static void pk_zset_remove_at(char *data, uint32_t *data_len, char *p) {
    uint32_t count = pk_read_u32(data);
    if (count == 0) return;
    uint32_t mlen = pk_read_u32(p);
    uint32_t consumed = 4 + mlen + 8;
    uint32_t offset = (uint32_t)(p - data);
    uint32_t remaining = *data_len - offset - consumed;

    if (remaining > 0)
        memmove(p, p + consumed, remaining);
    *data_len -= consumed;
    pk_write_u32(data, count - 1);
}

/* =====================================================================
 * PACKED: insert at position maintaining score order
 * ===================================================================== */

static int pk_zset_insert(char *data, uint32_t *data_len,
                          const char *member, uint32_t member_len,
                          double score) {
    uint32_t count = pk_read_u32(data);
    uint32_t need = PK_ELEM_SIZE(member_len);

    /* Find insertion position */
    uint32_t insert_off = 4;  /* after count */
    char *p = data + 4;
    for (uint32_t i = 0; i < count; i++) {
        uint32_t mlen = pk_read_u32(p);
        double s = pk_read_score(p + 4 + mlen);
        if (score < s || (score == s && memcmp(p + 4, member, member_len) < 0))
            break;
        insert_off = (uint32_t)(p - data) + 4 + mlen + 8;
        p += 4 + mlen + 8;
    }

    /* Shift data after insertion point */
    if (insert_off < *data_len)
        memmove(data + insert_off + need, data + insert_off, *data_len - insert_off);

    pk_write_u32(data + insert_off, member_len);
    memcpy(data + insert_off + 4, member, member_len);
    pk_write_score(data + insert_off + 4 + member_len, score);

    *data_len += need;
    pk_write_u32(data, count + 1);
    return 0;
}

/* =====================================================================
 * Convert packed zset to skiplist encoding
 * ===================================================================== */

static kv_entry_t *zset_convert_to_skiplist(kv_entry_t *e) {
    uint32_t old_count = pk_read_u32(e->data + 2);

    /* Save all members and scores before resize */
    char **members = NULL;
    double *scores = NULL;
    uint32_t *mlens = NULL;

    if (old_count > 0) {
        members = (char **)malloc((size_t)old_count * sizeof(char *));
        scores  = (double *)malloc((size_t)old_count * sizeof(double));
        mlens   = (uint32_t *)malloc((size_t)old_count * sizeof(uint32_t));
        if (!members || !scores || !mlens) {
            free(members); free(scores); free(mlens);
            return NULL;
        }

        const char *p = e->data + 6;
        for (uint32_t i = 0; i < old_count; i++) {
            uint32_t mlen = pk_read_u32(p);
            mlens[i] = mlen;
            members[i] = (char *)malloc(mlen);
            if (members[i]) memcpy(members[i], p + 4, mlen);
            p += 4 + mlen;
            scores[i] = pk_read_score(p);
            p += 8;
        }
    }

    /* Resize to skiplist header */
    kv_entry_t *ne = dict_resize_entry(&g_dict, e, ZSET_HEADER_SIZE);
    if (!ne) goto err;
    e = ne;

    e->data[0] = (char)KV_TYPE_ZSET;
    e->data[1] = (char)KV_ENCODING_SKIPLIST;
    kv_dict_t *d = zset_dict(e);
    dict_init(d);
    zsl_create_sentinels(e);

    /* Insert all elements */
    for (uint32_t i = 0; i < old_count; i++) {
        zskiplist_node_t *node = zsl_insert(e, scores[i], members[i], mlens[i]);
        if (!node) goto err_cleanup;

        /* Add to dict with pointer reference as key */
        uint64_t h = kv_hash((const uint8_t *)members[i], mlens[i]);
        /* Build dict entry data: [type][enc][member_len:4][score:8] */
        char payload[12];
        memcpy(payload, &mlens[i], 4);
        memcpy(payload + 4, &scores[i], 8);
        /* Key is the member string pointer */
        const char *mptr = node->member;
        kv_entry_t *de = dict_add_raw(d, (const char *)&mptr, sizeof(char *),
                                      KV_TYPE_STRING, KV_ENCODING_RAW,
                                      payload, 12, h);
        if (!de) {
            /* Rollback: this is best-effort, the entry exists */
        }
    }

    for (uint32_t i = 0; i < old_count; i++) free(members[i]);
    free(members); free(scores); free(mlens);
    return e;

err_cleanup:
    zset_cleanup_entry(e);
err:
    if (members) {
        for (uint32_t i = 0; i < old_count; i++) free(members[i]);
    }
    free(members); free(scores); free(mlens);
    return NULL;
}

/* =====================================================================
 * Convert skiplist to packed encoding (shrink)
 * ===================================================================== */

static kv_entry_t *zset_contract_to_packed(kv_entry_t *e) {
    uint32_t count = zset_count(e);
    uint32_t pk_size = 6;

    /* Compute size */
    zskiplist_node_t *node = zset_head(e)->level[0].forward;
    zskiplist_node_t *tail = zset_tail(e);
    while (node != tail) {
        pk_size += PK_ELEM_SIZE(node->member_len);
        node = node->level[0].forward;
    }

    /* Save all data before cleanup */
    typedef struct { char *m; double s; uint32_t ml; } elem_t;
    elem_t *elems = (elem_t *)malloc((size_t)count * sizeof(elem_t));
    if (!elems) return e;

    node = zset_head(e)->level[0].forward;
    uint32_t idx = 0;
    while (node != tail && idx < count) {
        elems[idx].m = (char *)malloc(node->member_len);
        if (elems[idx].m) memcpy(elems[idx].m, node->member, node->member_len);
        elems[idx].s = node->score;
        elems[idx].ml = node->member_len;
        idx++;
        node = node->level[0].forward;
    }

    /* Destroy skiplist and dict */
    /* Free all nodes */
    node = zset_head(e)->level[0].forward;
    while (node != tail) {
        zskiplist_node_t *next = node->level[0].forward;
        zsl_node_free(node);
        node = next;
    }
    zsl_node_free(zset_head(e));
    zsl_node_free(zset_tail(e));

    kv_dict_t *d = zset_dict(e);
    dict_clear(d);
    dict_destroy(d);

    /* Resize to packed */
    kv_entry_t *ne = dict_resize_entry(&g_dict, e, pk_size);
    if (!ne) {
        /* Restore skiplist */
        ne = e;
        dict_init(d);
        zsl_create_sentinels(ne);
        for (uint32_t i = 0; i < count; i++) {
            zskiplist_node_t *n = zsl_insert(ne, elems[i].s, elems[i].m, elems[i].ml);
            if (n) {
                const char *mptr = n->member;
                char payload[12];
                memcpy(payload, &elems[i].ml, 4);
                memcpy(payload + 4, &elems[i].s, 8);
                uint64_t h = kv_hash((const uint8_t *)elems[i].m, elems[i].ml);
                dict_add_raw(d, (const char *)&mptr, sizeof(char *),
                            KV_TYPE_STRING, KV_ENCODING_RAW, payload, 12, h);
            }
        }
        goto out;
    }
    e = ne;

    e->data[0] = (char)KV_TYPE_ZSET;
    e->data[1] = (char)KV_ENCODING_PACKED;
    pk_write_u32(e->data + 2, count);

    uint32_t off = 6;
    for (uint32_t i = 0; i < count; i++) {
        pk_write_u32(e->data + off, elems[i].ml);
        memcpy(e->data + off + 4, elems[i].m, elems[i].ml);
        pk_write_score(e->data + off + 4 + elems[i].ml, elems[i].s);
        off += PK_ELEM_SIZE(elems[i].ml);
    }
    e->val_len = off;

out:
    for (uint32_t i = 0; i < count; i++) free(elems[i].m);
    free(elems);
    return e;
}

static kv_entry_t *zset_maybe_shrink(kv_entry_t *e) {
    if (entry_encoding(e) != KV_ENCODING_SKIPLIST) return e;
    if (zset_count(e) < ZSET_SHRINK_THRESHOLD)
        return zset_contract_to_packed(e);
    return e;
}

/* =====================================================================
 * Cleanup
 * ===================================================================== */

void zset_cleanup_entry(kv_entry_t *e) {
    if (!e) return;
    if (entry_type(e) != KV_TYPE_ZSET) return;
    if (entry_encoding(e) != KV_ENCODING_SKIPLIST) return;

    zskiplist_node_t *head = zset_head(e);
    zskiplist_node_t *tail = zset_tail(e);
    zskiplist_node_t *node = head ? head->level[0].forward : NULL;
    zskiplist_node_t *safe_tail = tail;

    while (node && node != safe_tail) {
        zskiplist_node_t *next = node->level[0].forward;
        zsl_node_free(node);
        node = next;
    }
    if (head) zsl_node_free(head);
    if (tail) zsl_node_free(tail);

    kv_dict_t *d = zset_dict(e);
    dict_clear(d);
    dict_destroy(d);
}

/* =====================================================================
 * ZADD packed implementation
 * ===================================================================== */

static int zset_add_packed(kv_entry_t **pe, double score,
                           const char *member, uint32_t member_len,
                           int *added) {
    kv_entry_t *e = *pe;
    uint32_t pk_len = e->val_len - 2;
    int was_update = 0;

    char *fp = pk_find_member(e->data + 2, pk_len, member, member_len);
    if (fp) {
        /* Member exists — check score */
        double old_score = pk_get_score(fp);
        if (old_score == score) {
            *added = 0;
            return 0;
        }
        /* Remove old entry first */
        pk_zset_remove_at(e->data + 2, &pk_len, fp);
        was_update = 1;
    }

    /* Check if should convert */
    uint32_t count = pk_read_u32(e->data + 2);
    uint32_t need = PK_ELEM_SIZE(member_len);
    if (count + 1 > ZSET_PACKED_MAX_ELEMS ||
        pk_len + need > ZSET_PACKED_MAX_BYTES) {
        e = zset_convert_to_skiplist(e);
        if (!e) return -1;
        *pe = e;
        *added = was_update ? 0 : 1;
        return -2;  /* caller will retry in skiplist path */
    }

    /* Resize */
    uint32_t new_dlen = (uint32_t)(2 + pk_len + need);
    kv_entry_t *ne = dict_resize_entry(&g_dict, e, new_dlen);
    if (!ne) return -1;
    e = ne;
    *pe = e;

    pk_zset_insert(e->data + 2, &pk_len, member, member_len, score);
    e->val_len = pk_len + 2;
    *added = was_update ? 0 : 1;
    return 0;
}

/* =====================================================================
 * Flatten for serialization
 * ===================================================================== */

int zset_flatten_to_packed(kv_entry_t *e, char **out_data, uint32_t *out_len) {
    *out_data = NULL;
    *out_len  = 0;

    if (!e || entry_type(e) != KV_TYPE_ZSET ||
        entry_encoding(e) != KV_ENCODING_SKIPLIST)
        return -1;

    uint32_t count = zset_count(e);

    uint32_t buf_size = 6;
    zskiplist_node_t *node = zset_head(e)->level[0].forward;
    zskiplist_node_t *tail = zset_tail(e);
    while (node != tail) {
        buf_size += PK_ELEM_SIZE(node->member_len);
        node = node->level[0].forward;
    }

    char *buf = (char *)malloc(buf_size);
    if (!buf) return -1;

    buf[0] = (char)KV_TYPE_ZSET;
    buf[1] = (char)KV_ENCODING_PACKED;
    pk_write_u32(buf + 2, count);

    uint32_t off = 6;
    node = zset_head(e)->level[0].forward;
    while (node != tail) {
        pk_write_u32(buf + off, node->member_len);
        memcpy(buf + off + 4, node->member, node->member_len);
        pk_write_score(buf + off + 4 + node->member_len, node->score);
        off += PK_ELEM_SIZE(node->member_len);
        node = node->level[0].forward;
    }

    *out_data = buf;
    *out_len  = buf_size;
    return 0;
}

/* =====================================================================
 * Rebuild encoding after RDB load
 * ===================================================================== */

void zset_rebuild_encoding(kv_entry_t *e) {
    if (!e || entry_type(e) != KV_TYPE_ZSET) return;
    if (entry_encoding(e) != KV_ENCODING_PACKED) return;

    uint32_t count = pk_read_u32(e->data + 2);
    if (count > ZSET_PACKED_MAX_ELEMS) {
        (void)zset_convert_to_skiplist(e);
    }
}

/* =====================================================================
 * ZSET command dispatch
 * ===================================================================== */

dispatch_result_t zset_cmd(kvs_token_t cmd, kv_entry_t *entry,
                           kvs_token_t *args, int argc, DynBuf *wb,
                           const char *msg_raw, int msg_len,
                           client_t *client)
{
    (void)msg_raw;
    (void)msg_len;
    (void)client;

    char key_buf[256] = {0};
    const char *key = NULL;
    if (argc >= 1)
        key = key_from_token(args[0], key_buf, (int)sizeof(key_buf));

    char temp[64];

    switch (cmd.ptr[0]) {

    /* ── ZADD / ZREM / ZSCORE / ZRANGE / ZRANK / ZCARD / ZCOUNT / ZRANGEBYSCORE ── */
    case 'Z':
        if (TOKEN_EQ(cmd, "ZADD")) {
            if (argc < 3) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }

            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            kv_entry_t *e = dict_find(&g_dict, key, (uint32_t)strlen(key), hash);
            int existed = 0;

            if (!e) {
                char data[16];
                zset_build_empty(data);
                e = dict_add(&g_dict, key, (uint32_t)strlen(key), data, ZSET_EMPTY_SIZE, hash);
                if (!e) { dynbuf_cat(wb, "-ERR create zset failed\r\n", 24); return DISPATCH_ERROR; }
            } else {
                if (entry_type(e) != KV_TYPE_ZSET) {
                    dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                    return DISPATCH_ERROR;
                }
                if (check_expiry(e, key, hash)) existed = 1;
            }

            if (existed) {
                char data[16];
                zset_build_empty(data);
                e = dict_add(&g_dict, key, (uint32_t)strlen(key), data, ZSET_EMPTY_SIZE, hash);
                if (!e) { dynbuf_cat(wb, "-ERR create zset failed\r\n", 24); return DISPATCH_ERROR; }
            }

            int total_added = 0;
            for (int i = 1; i + 1 < argc; i += 2) {
                double score;
                if (parse_score(args[i], &score) < 0) {
                    dynbuf_cat(wb, "-ERR value is not a valid float\r\n", 34);
                    return DISPATCH_ERROR;
                }

                int added = 0;
                if (entry_encoding(e) == KV_ENCODING_PACKED) {
                    int ret = zset_add_packed(&e, score, args[i+1].ptr,
                                              (uint32_t)args[i+1].len, &added);
                    if (ret == -2) {
                        goto zadd_skiplist;
                    }
                    if (ret < 0) {
                        dynbuf_cat(wb, "-ERR zadd failed\r\n", 17);
                        return DISPATCH_ERROR;
                    }
                } else {
                zadd_skiplist:
                    if (entry_encoding(e) == KV_ENCODING_SKIPLIST) {
                        kv_dict_t *d = zset_dict(e);
                        uint64_t mhash = kv_hash((const uint8_t *)args[i+1].ptr,
                                                 (uint32_t)args[i+1].len);
                        kv_entry_t *de = dict_find_extkey(d, args[i+1].ptr,
                                                           (uint32_t)args[i+1].len, mhash);

                        if (de) {
                            double old_score;
                            memcpy(&old_score, de->data + 6, 8);
                            if (old_score == score) {
                                added = 0;
                            } else {
                                zskiplist_node_t *old_node =
                                    *(zskiplist_node_t **)entry_key(de);
                                zskiplist_node_t *update[ZSKIPLIST_MAXLEVEL];
                                zskiplist_node_t *found = zsl_find_with_update(
                                    e, old_score, args[i+1].ptr, (uint32_t)args[i+1].len, update);
                                if (found) {
                                    zsl_delete(e, found, update);
                                    zskiplist_node_t *new_node = zsl_insert(
                                        e, score, args[i+1].ptr, (uint32_t)args[i+1].len);
                                    if (new_node) {
                                        const char *mptr = new_node->member;
                                        uint32_t mlen = new_node->member_len;
                                        uint32_t new_data_len = 14;
                                        kv_entry_t *nde = dict_resize_entry(
                                            d, de, new_data_len);
                                        if (nde) {
                                            nde->data[0] = (char)KV_TYPE_STRING;
                                            nde->data[1] = (char)KV_ENCODING_RAW;
                                            memcpy(nde->data + 2, &mlen, 4);
                                            memcpy(nde->data + 6, &score, 8);
                                            memcpy((char *)entry_key(nde), &mptr, sizeof(char *));
                                        }
                                        added = 0;
                                    }
                                }
                            }
                    } else {
                            zskiplist_node_t *node = zsl_insert(
                                e, score, args[i+1].ptr, (uint32_t)args[i+1].len);
                            if (!node) {
                                dynbuf_cat(wb, "-ERR zadd failed\r\n", 17);
                                return DISPATCH_ERROR;
                            }
                            const char *mptr = node->member;
                            uint32_t mlen = node->member_len;
                            char payload[12];
                            memcpy(payload, &mlen, 4);
                            memcpy(payload + 4, &score, 8);
                            dict_add_raw(d, (const char *)&mptr, sizeof(char *),
                                        KV_TYPE_STRING, KV_ENCODING_RAW,
                                        payload, 12, mhash);
                            added = 1;
                        }
                    }
                }
                total_added += added;
            }

            int hl = snprintf(temp, sizeof(temp), ":%d\r\n", total_added);
            dynbuf_cat(wb, temp, hl);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "ZREM")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_ZSET) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, hash)) {
                dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN);
                return DISPATCH_OK;
            }

            int total_removed = 0;
            for (int i = 1; i < argc; i++) {
                if (entry_encoding(entry) == KV_ENCODING_PACKED) {
                    uint32_t pk_len = entry->val_len - 2;
                    char *fp = pk_find_member(entry->data + 2, pk_len,
                                              args[i].ptr, (uint32_t)args[i].len);
                    if (fp) {
                        pk_zset_remove_at(entry->data + 2, &pk_len, fp);
                        uint32_t new_dlen = pk_len + 2;
                        kv_entry_t *ne = dict_resize_entry(&g_dict, entry, new_dlen);
                        if (ne) { ne->val_len = new_dlen; entry = ne; }
                        else    { entry->val_len = new_dlen; }
                        total_removed++;
                    }
                } else {
                    kv_dict_t *d = zset_dict(entry);
                    uint64_t mhash = kv_hash((const uint8_t *)args[i].ptr,
                                              (uint32_t)args[i].len);
                    kv_entry_t *de = dict_find_extkey(d, args[i].ptr,
                                                       (uint32_t)args[i].len, mhash);
                    if (de) {
                        zskiplist_node_t *node = *(zskiplist_node_t **)entry_key(de);
                        zskiplist_node_t *update[ZSKIPLIST_MAXLEVEL];
                        zskiplist_node_t *found = zsl_find_with_update(
                            entry, node->score, args[i].ptr, (uint32_t)args[i].len,
                            update);
                        if (found) {
                            zsl_delete(entry, found, update);
                        }
                        dict_delete(d, (const char *)&node, sizeof(char *), mhash);
                        total_removed++;
                        entry = zset_maybe_shrink(entry);
                    }
                }
            }

            int hl = snprintf(temp, sizeof(temp), ":%d\r\n", total_removed);
            dynbuf_cat(wb, temp, hl);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "ZSCORE")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, REP_NIL, REP_NIL_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_ZSET) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, hash)) {
                dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                return DISPATCH_OK;
            }

            double score = 0;
            int found = 0;

            if (entry_encoding(entry) == KV_ENCODING_PACKED) {
                uint32_t pk_len = entry->val_len - 2;
                char *fp = pk_find_member(entry->data + 2, pk_len,
                                          args[1].ptr, (uint32_t)args[1].len);
                if (fp) { score = pk_get_score(fp); found = 1; }
            } else {
                kv_dict_t *d = zset_dict(entry);
                uint64_t mhash = kv_hash((const uint8_t *)args[1].ptr,
                                          (uint32_t)args[1].len);
                kv_entry_t *de = dict_find_extkey(d, args[1].ptr,
                                                   (uint32_t)args[1].len, mhash);
                if (de) {
                    memcpy(&score, de->data + 6, 8);
                    found = 1;
                }
            }

            if (!found) {
                dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
            } else {
                char sbuf[64];
                int slen = snprintf(sbuf, sizeof(sbuf), "%.17g", score);
                int hl = snprintf(temp, sizeof(temp), "$%d\r\n", slen);
                dynbuf_cat(wb, temp, hl);
                dynbuf_cat(wb, sbuf, slen);
                dynbuf_cat(wb, "\r\n", 2);
            }
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "ZCARD")) {
            if (!entry) { dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_ZSET) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, hash)) {
                dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN);
                return DISPATCH_OK;
            }
            int hl = snprintf(temp, sizeof(temp), ":%u\r\n", zset_card(entry));
            dynbuf_cat(wb, temp, hl);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "ZRANK")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, REP_NIL, REP_NIL_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_ZSET) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, hash)) {
                dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                return DISPATCH_OK;
            }

            int rank = -1;
            if (entry_encoding(entry) == KV_ENCODING_PACKED) {
                uint32_t pk_len = entry->val_len - 2;
                uint32_t count = pk_read_u32(entry->data + 2);
                char *p = entry->data + 6;
                for (uint32_t i = 0; i < count; i++) {
                    uint32_t mlen = pk_read_u32(p);
                    if (mlen == (uint32_t)args[1].len &&
                        memcmp(p + 4, args[1].ptr, mlen) == 0) {
                        rank = (int)i;
                        break;
                    }
                    p += 4 + mlen + 8;
                }
            } else {
                kv_dict_t *d = zset_dict(entry);
                uint64_t mhash = kv_hash((const uint8_t *)args[1].ptr,
                                          (uint32_t)args[1].len);
                kv_entry_t *de = dict_find_extkey(d, args[1].ptr,
                                                   (uint32_t)args[1].len, mhash);
                if (de) {
                    double score;
                    memcpy(&score, de->data + 6, 8);
                    rank = zsl_get_rank(entry, score, args[1].ptr,
                                         (uint32_t)args[1].len);
                }
            }

            if (rank < 0) {
                dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
            } else {
                int hl = snprintf(temp, sizeof(temp), ":%d\r\n", rank);
                dynbuf_cat(wb, temp, hl);
            }
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "ZRANGE")) {
            if (argc < 3) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, "*0\r\n", 4); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_ZSET) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, hash)) {
                dynbuf_cat(wb, "*0\r\n", 4);
                return DISPATCH_OK;
            }

            int start = 0, stop = 0;
            {
                int v;
                char tbuf[32];
                int nl = args[1].len < 31 ? args[1].len : 31;
                memcpy(tbuf, args[1].ptr, nl); tbuf[nl] = '\0';
                start = atoi(tbuf);
                nl = args[2].len < 31 ? args[2].len : 31;
                memcpy(tbuf, args[2].ptr, nl); tbuf[nl] = '\0';
                stop = atoi(tbuf);
            }

            int withscores = (argc >= 4 && args[3].len == 10 &&
                              memcmp(args[3].ptr, "WITHSCORES", 10) == 0);

            uint32_t len = zset_card(entry);
            if (start < 0) start = (int)len + start;
            if (stop < 0) stop = (int)len + stop;
            if (start < 0) start = 0;
            if (stop >= (int)len) stop = (int)len - 1;
            if (start > stop || len == 0) {
                dynbuf_cat(wb, "*0\r\n", 4);
                return DISPATCH_OK;
            }

            int range_count = stop - start + 1;
            int out_count = withscores ? range_count * 2 : range_count;
            char hdr[32];
            int hl = snprintf(hdr, sizeof(hdr), "*%d\r\n", out_count);
            dynbuf_cat(wb, hdr, hl);

            if (entry_encoding(entry) == KV_ENCODING_PACKED) {
                char *p = entry->data + 6;
                for (int i = 0; i <= stop; i++) {
                    uint32_t mlen = pk_read_u32(p);
                    if (i >= start) {
                        char bhdr[32];
                        int bhl = snprintf(bhdr, sizeof(bhdr), "$%u\r\n", mlen);
                        dynbuf_cat(wb, bhdr, bhl);
                        dynbuf_cat(wb, p + 4, mlen);
                        dynbuf_cat(wb, "\r\n", 2);
                        if (withscores) {
                            double sc = pk_read_score(p + 4 + mlen);
                            char sbuf[64];
                            int slen = snprintf(sbuf, sizeof(sbuf), "%.17g", sc);
                            bhl = snprintf(bhdr, sizeof(bhdr), "$%d\r\n", slen);
                            dynbuf_cat(wb, bhdr, bhl);
                            dynbuf_cat(wb, sbuf, slen);
                            dynbuf_cat(wb, "\r\n", 2);
                        }
                    }
                    p += 4 + mlen + 8;
                }
            } else {
                zskiplist_node_t *node = zsl_get_by_rank(entry, (uint32_t)start);
                for (int i = 0; i < range_count && node; i++) {
                    if (node == zset_tail(entry)) break;
                    char bhdr[32];
                    int bhl = snprintf(bhdr, sizeof(bhdr), "$%u\r\n", node->member_len);
                    dynbuf_cat(wb, bhdr, bhl);
                    dynbuf_cat(wb, node->member, node->member_len);
                    dynbuf_cat(wb, "\r\n", 2);
                    if (withscores) {
                        char sbuf[64];
                        int slen = snprintf(sbuf, sizeof(sbuf), "%.17g", node->score);
                        bhl = snprintf(bhdr, sizeof(bhdr), "$%d\r\n", slen);
                        dynbuf_cat(wb, bhdr, bhl);
                        dynbuf_cat(wb, sbuf, slen);
                        dynbuf_cat(wb, "\r\n", 2);
                    }
                    node = node->level[0].forward;
                }
            }
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "ZCOUNT")) {
            if (argc < 3) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_ZSET) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, hash)) {
                dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN);
                return DISPATCH_OK;
            }

            double min_score, max_score;
            if (parse_score(args[1], &min_score) < 0 ||
                parse_score(args[2], &max_score) < 0) {
                dynbuf_cat(wb, "-ERR value is not a valid float\r\n", 34);
                return DISPATCH_ERROR;
            }

            uint32_t cnt = 0;
            if (entry_encoding(entry) == KV_ENCODING_PACKED) {
                uint32_t pk_len = entry->val_len - 2;
                uint32_t count = pk_read_u32(entry->data + 2);
                char *p = entry->data + 6;
                for (uint32_t i = 0; i < count; i++) {
                    uint32_t mlen = pk_read_u32(p);
                    double sc = pk_read_score(p + 4 + mlen);
                    if (sc >= min_score && sc <= max_score) cnt++;
                    p += 4 + mlen + 8;
                }
            } else {
                zskiplist_node_t *node = zsl_first_ge(entry, min_score);
                zskiplist_node_t *tail = zset_tail(entry);
                while (node && node != tail && node->score <= max_score) {
                    cnt++;
                    node = node->level[0].forward;
                }
            }

            int hl = snprintf(temp, sizeof(temp), ":%u\r\n", cnt);
            dynbuf_cat(wb, temp, hl);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "ZRANGEBYSCORE")) {
            if (argc < 3) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, "*0\r\n", 4); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_ZSET) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, hash)) {
                dynbuf_cat(wb, "*0\r\n", 4);
                return DISPATCH_OK;
            }

            double min_score, max_score;
            if (parse_score(args[1], &min_score) < 0 ||
                parse_score(args[2], &max_score) < 0) {
                dynbuf_cat(wb, "-ERR value is not a valid float\r\n", 34);
                return DISPATCH_ERROR;
            }

            int withscores = (argc >= 4 && args[3].len == 10 &&
                              memcmp(args[3].ptr, "WITHSCORES", 10) == 0);

            /* Collect matching elements first (need count for array header) */
            typedef struct { const char *m; uint32_t ml; double s; } match_t;
            match_t matches[4096]; /* reasonable upper bound for a single response */
            uint32_t match_count = 0;
            int overflow = 0;

            if (entry_encoding(entry) == KV_ENCODING_PACKED) {
                uint32_t pk_len = entry->val_len - 2;
                uint32_t count = pk_read_u32(entry->data + 2);
                char *p = entry->data + 6;
                for (uint32_t i = 0; i < count; i++) {
                    uint32_t mlen = pk_read_u32(p);
                    double sc = pk_read_score(p + 4 + mlen);
                    if (sc >= min_score && sc <= max_score) {
                        if (match_count < 4096) {
                            matches[match_count].m  = p + 4;
                            matches[match_count].ml = mlen;
                            matches[match_count].s  = sc;
                            match_count++;
                        } else overflow = 1;
                    }
                    p += 4 + mlen + 8;
                }
            } else {
                zskiplist_node_t *node = zsl_first_ge(entry, min_score);
                zskiplist_node_t *tail = zset_tail(entry);
                while (node && node != tail && node->score <= max_score) {
                    if (match_count < 4096) {
                        matches[match_count].m  = node->member;
                        matches[match_count].ml = node->member_len;
                        matches[match_count].s  = node->score;
                        match_count++;
                    } else overflow = 1;
                    node = node->level[0].forward;
                }
            }

            (void)overflow;

            int out_count = withscores ? (int)match_count * 2 : (int)match_count;
            char hdr[32];
            int hl = snprintf(hdr, sizeof(hdr), "*%d\r\n", out_count);
            dynbuf_cat(wb, hdr, hl);

            for (uint32_t i = 0; i < match_count; i++) {
                char bhdr[32];
                int bhl = snprintf(bhdr, sizeof(bhdr), "$%u\r\n", matches[i].ml);
                dynbuf_cat(wb, bhdr, bhl);
                dynbuf_cat(wb, matches[i].m, (int)matches[i].ml);
                dynbuf_cat(wb, "\r\n", 2);
                if (withscores) {
                    char sbuf[64];
                    int slen = snprintf(sbuf, sizeof(sbuf), "%.17g", matches[i].s);
                    bhl = snprintf(bhdr, sizeof(bhdr), "$%d\r\n", slen);
                    dynbuf_cat(wb, bhdr, bhl);
                    dynbuf_cat(wb, sbuf, slen);
                    dynbuf_cat(wb, "\r\n", 2);
                }
            }
            return DISPATCH_OK;
        }
        break; /* 'Z' */

    /* ── DEL ── */
    case 'D':
        if (TOKEN_EQ(cmd, "DEL")) {
            if (!entry) { dynbuf_cat(wb, REP_NIL, REP_NIL_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_ZSET) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            {
                uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
                zset_cleanup_entry(entry);
                int ret = dict_delete(&g_dict, key, (uint32_t)strlen(key), hash);
                dynbuf_cat(wb, ret == 0 ? REP_OK : REP_NIL,
                                ret == 0 ? REP_OK_LEN : REP_NIL_LEN);
            }
            return DISPATCH_OK;
        }
        break;

    /* ── EXIST ── */
    case 'E':
        if (TOKEN_EQ(cmd, "EXIST")) {
            if (!entry) { dynbuf_cat(wb, REP_NIL, REP_NIL_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_ZSET) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            {
                uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
                if (check_expiry(entry, key, hash))
                    dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                else
                    dynbuf_cat(wb, REP_ONE, REP_ONE_LEN);
            }
            return DISPATCH_OK;
        }
        break;
    }

    dynbuf_cat(wb, REP_ERR_UNK, REP_ERR_UNK_LEN);
    return DISPATCH_ERROR;
}
