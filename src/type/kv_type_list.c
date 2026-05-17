#include "kv_type_list.h"
#include "block.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

/* =====================================================================
 * Static RESP fragments
 * ===================================================================== */

#define S(id, str) \
    static const char id[] = str; \
    static const int  id##_LEN = (int)(sizeof(str) - 1)

S(REP_OK,        "+OK\r\n");
S(REP_NIL,       "$-1\r\n");
S(REP_ERR_ARGS,  "-ERR wrong number of arguments\r\n");
S(REP_ERR_UNK,   "-ERR unknown command\r\n");
S(REP_WRONGTYPE, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");

#undef S

/* =====================================================================
 * Limits
 * ===================================================================== */

#define LIST_PACKED_MAX_ELEMS  128
#define LIST_PACKED_MAX_BYTES  4096
#define LIST_QL_NODE_MAX_ELEMS 64
#define LIST_QL_NODE_MAX_BYTES 4096

/* =====================================================================
 * Packed encoding helpers
 *
 * Format: [count:4][elem1_len:4][elem1_bytes...][elem2_len:4][elem2_bytes...]...
 * The 'data' pointer points at the count field (NOT the type byte).
 * ===================================================================== */

static inline uint32_t pk_read_u32(const char *p) {
    uint32_t v;
    memcpy(&v, p, 4);
    return v;
}

static inline void pk_write_u32(char *p, uint32_t v) {
    memcpy(p, &v, 4);
}

static inline uint32_t pk_count(const char *data) {
    return pk_read_u32(data);
}

/* Walk to the idx-th element, returns pointer to its length field */
static char *pk_elem_ptr(char *data, uint32_t data_len, uint32_t idx) {
    uint32_t count = pk_read_u32(data);
    if (idx >= count) return NULL;
    char *p = data + 4;
    for (uint32_t i = 0; i < idx; i++) {
        uint32_t elen = pk_read_u32(p);
        p += 4 + elen;
        if (p > data + data_len) return NULL;
    }
    return p;
}

static const char *pk_elem_get(const char *data, uint32_t data_len,
                               uint32_t idx, int *out_len) {
    const char *p = pk_elem_ptr((char *)data, data_len, idx);
    if (!p) { *out_len = -1; return NULL; }
    *out_len = (int)pk_read_u32(p);
    return p + 4;
}

/* Insert element at front. data must have enough capacity.
   *data_len tracks used bytes and is updated. */
static void pk_insert_front(char *data, uint32_t *data_len,
                            const char *elem, int elem_len) {
    uint32_t old_count = pk_read_u32(data);
    uint32_t old_data  = *data_len;
    uint32_t new_bytes = (uint32_t)(4 + elem_len);

    if (old_data > 4)
        memmove(data + 4 + new_bytes, data + 4, old_data - 4);
    pk_write_u32(data, old_count + 1);
    pk_write_u32(data + 4, (uint32_t)elem_len);
    memcpy(data + 8, elem, (size_t)elem_len);
    *data_len = old_data + new_bytes;
}

/* Append element at back */
static void pk_append(char *data, uint32_t *data_len,
                      const char *elem, int elem_len) {
    uint32_t old_count = pk_read_u32(data);
    char *p = data + *data_len;
    pk_write_u32(p, (uint32_t)elem_len);
    memcpy(p + 4, elem, (size_t)elem_len);
    *data_len += (uint32_t)(4 + elem_len);
    pk_write_u32(data, old_count + 1);
}

/* Pop element from front. Copies data to out_buf (memmove may overwrite source). */
static int pk_pop_front(char *data, uint32_t *data_len,
                        char *out_buf, int *out_len) {
    uint32_t count = pk_read_u32(data);
    if (count == 0) return -1;
    uint32_t elen = pk_read_u32(data + 4);

    /* Copy element data before memmove overwrites it */
    if (out_buf) memcpy(out_buf, data + 8, elen);
    *out_len = (int)elen;

    uint32_t consumed = 4 + elen;
    uint32_t remaining = *data_len - consumed;
    if (remaining > 4)
        memmove(data + 4, data + 4 + consumed, remaining - 4);
    else
        remaining = 4;
    *data_len = remaining;
    pk_write_u32(data, count - 1);
    return 0;
}

/* Pop element from back. Copies data to out_buf (safe, no memmove over source). */
static int pk_pop_back(char *data, uint32_t *data_len,
                       char *out_buf, int *out_len) {
    uint32_t count = pk_read_u32(data);
    if (count == 0) return -1;

    char *p = data + 4;
    for (uint32_t i = 0; i < count - 1; i++) {
        uint32_t elen = pk_read_u32(p);
        p += 4 + elen;
    }
    uint32_t last_len = pk_read_u32(p);
    if (out_buf) memcpy(out_buf, p + 4, last_len);
    *out_len  = (int)last_len;
    *data_len = (uint32_t)(p - data);
    pk_write_u32(data, count - 1);
    return 0;
}

/* =====================================================================
 * Quicklist node
 * ===================================================================== */

typedef struct kv_ql_node_s {
    struct kv_ql_node_s *prev;
    struct kv_ql_node_s *next;
    uint32_t count;
    uint32_t data_len;
    uint32_t data_cap;
    char     data[];       /* packed: [count:4][len:4][elem...]... */
} kv_ql_node_t;

static kv_ql_node_t *ql_node_alloc(void) {
    uint32_t cap = 256;
    kv_ql_node_t *n = (kv_ql_node_t *)malloc(sizeof(kv_ql_node_t) + cap);
    if (!n) return NULL;
    n->prev     = NULL;
    n->next     = NULL;
    n->count    = 0;
    n->data_len = 4;
    n->data_cap = cap;
    pk_write_u32(n->data, 0);
    return n;
}

static void ql_node_free(kv_ql_node_t *n) { free(n); }

static int ql_node_is_full(kv_ql_node_t *n) {
    return n->count >= LIST_QL_NODE_MAX_ELEMS ||
           n->data_len >= LIST_QL_NODE_MAX_BYTES;
}

/* =====================================================================
 * Quicklist header in entry data[]
 *
 * Layout: [type:1][encoding:1][total_count:4][node_max:4][head:8][tail:8][node_count:4]
 * Total: 26 bytes
 * ===================================================================== */

#define QL_OFF_TOTAL_COUNT 2
#define QL_OFF_NODE_MAX    6
#define QL_OFF_HEAD        10
#define QL_OFF_TAIL        18
#define QL_HEADER_SIZE     30  /* type+encoding+count+node_max+head+tail+node_count */

static inline uint32_t ql_total_count(kv_entry_t *e) {
    return pk_read_u32(e->data + QL_OFF_TOTAL_COUNT);
}
static inline void ql_set_total_count(kv_entry_t *e, uint32_t c) {
    pk_write_u32(e->data + QL_OFF_TOTAL_COUNT, c);
}
static inline uint32_t ql_node_count(kv_entry_t *e) {
    return pk_read_u32(e->data + QL_OFF_HEAD + 16);
}
static inline void ql_set_node_count(kv_entry_t *e, uint32_t c) {
    pk_write_u32(e->data + QL_OFF_HEAD + 16, c);
}

static kv_ql_node_t *ql_head(kv_entry_t *e) {
    uint64_t v;
    memcpy(&v, e->data + QL_OFF_HEAD, 8);
    return (kv_ql_node_t *)(uintptr_t)v;
}
static void ql_set_head(kv_entry_t *e, kv_ql_node_t *n) {
    uint64_t v = (uint64_t)(uintptr_t)n;
    memcpy(e->data + QL_OFF_HEAD, &v, 8);
}
static kv_ql_node_t *ql_tail(kv_entry_t *e) {
    uint64_t v;
    memcpy(&v, e->data + QL_OFF_TAIL, 8);
    return (kv_ql_node_t *)(uintptr_t)v;
}
static void ql_set_tail(kv_entry_t *e, kv_ql_node_t *n) {
    uint64_t v = (uint64_t)(uintptr_t)n;
    memcpy(e->data + QL_OFF_TAIL, &v, 8);
}

static void ql_init_header(kv_entry_t *e) {
    e->data[1] = (char)KV_ENCODING_QUICKLIST;
    ql_set_total_count(e, 0);
    pk_write_u32(e->data + QL_OFF_NODE_MAX, LIST_QL_NODE_MAX_ELEMS);
    ql_set_head(e, NULL);
    ql_set_tail(e, NULL);
    ql_set_node_count(e, 0);
}

/* =====================================================================
 * Quicklist node push (returns new node pointer if realloc'd)
 * ===================================================================== */

static kv_ql_node_t *ql_node_push_left(kv_ql_node_t *n,
                                       const char *elem, int elem_len) {
    uint32_t need = (uint32_t)(4 + elem_len);
    if (n->data_len + need > n->data_cap) {
        uint32_t new_cap = n->data_cap;
        while (new_cap < n->data_len + need) new_cap *= 2;
        kv_ql_node_t *nn = (kv_ql_node_t *)realloc(n, sizeof(*n) + new_cap);
        if (!nn) return NULL;
        nn->data_cap = new_cap;
        if (nn->prev) nn->prev->next = nn;
        if (nn->next) nn->next->prev = nn;
        n = nn;
    }
    pk_insert_front(n->data, &n->data_len, elem, elem_len);
    n->count++;
    return n;
}

static kv_ql_node_t *ql_node_push_right(kv_ql_node_t *n,
                                        const char *elem, int elem_len) {
    uint32_t need = (uint32_t)(4 + elem_len);
    if (n->data_len + need > n->data_cap) {
        uint32_t new_cap = n->data_cap;
        while (new_cap < n->data_len + need) new_cap *= 2;
        kv_ql_node_t *nn = (kv_ql_node_t *)realloc(n, sizeof(*n) + new_cap);
        if (!nn) return NULL;
        nn->data_cap = new_cap;
        if (nn->prev) nn->prev->next = nn;
        if (nn->next) nn->next->prev = nn;
        n = nn;
    }
    pk_append(n->data, &n->data_len, elem, elem_len);
    n->count++;
    return n;
}

/* =====================================================================
 * Quicklist push left/right (manage head/tail nodes)
 * ===================================================================== */

static int ql_push_left(kv_entry_t *e, const char *elem, int elem_len) {
    kv_ql_node_t *head = ql_head(e);

    if (!head || ql_node_is_full(head)) {
        kv_ql_node_t *node = ql_node_alloc();
        if (!node) return -1;
        node->next = head;
        if (head) head->prev = node;
        else      ql_set_tail(e, node);
        ql_set_head(e, node);
        ql_set_node_count(e, ql_node_count(e) + 1);
        head = node;
    }

    head = ql_node_push_left(head, elem, elem_len);
    if (!head) return -1;
    ql_set_head(e, head);  /* update in case realloc moved it */
    ql_set_total_count(e, ql_total_count(e) + 1);
    return 0;
}

static int ql_push_right(kv_entry_t *e, const char *elem, int elem_len) {
    kv_ql_node_t *tail = ql_tail(e);

    if (!tail || ql_node_is_full(tail)) {
        kv_ql_node_t *node = ql_node_alloc();
        if (!node) return -1;
        node->prev = tail;
        if (tail) tail->next = node;
        else      ql_set_head(e, node);
        ql_set_tail(e, node);
        ql_set_node_count(e, ql_node_count(e) + 1);
        tail = node;
    }

    tail = ql_node_push_right(tail, elem, elem_len);
    if (!tail) return -1;
    ql_set_tail(e, tail);  /* update in case realloc moved it */
    ql_set_total_count(e, ql_total_count(e) + 1);
    return 0;
}

/* =====================================================================
 * Quicklist pop left/right
 * ===================================================================== */

static int ql_pop_left(kv_entry_t *e, char *out_buf, int *out_len) {
    kv_ql_node_t *head = ql_head(e);
    if (!head || head->count == 0) return -1;

    pk_pop_front(head->data, &head->data_len, out_buf, out_len);
    head->count--;
    ql_set_total_count(e, ql_total_count(e) - 1);

    if (head->count == 0) {
        kv_ql_node_t *next = head->next;
        if (next) next->prev = NULL;
        else      ql_set_tail(e, NULL);
        ql_set_head(e, next);
        ql_set_node_count(e, ql_node_count(e) - 1);
        ql_node_free(head);
    }
    return 0;
}

static int ql_pop_right(kv_entry_t *e, char *out_buf, int *out_len) {
    kv_ql_node_t *tail = ql_tail(e);
    if (!tail || tail->count == 0) return -1;

    pk_pop_back(tail->data, &tail->data_len, out_buf, out_len);
    tail->count--;
    ql_set_total_count(e, ql_total_count(e) - 1);

    if (tail->count == 0) {
        kv_ql_node_t *prev = tail->prev;
        if (prev) prev->next = NULL;
        else      ql_set_head(e, NULL);
        ql_set_tail(e, prev);
        ql_set_node_count(e, ql_node_count(e) - 1);
        ql_node_free(tail);
    }
    return 0;
}

/* =====================================================================
 * List cleanup
 * ===================================================================== */

void list_cleanup_entry(kv_entry_t *e) {
    if (!e) return;
    if (entry_type(e) != KV_TYPE_LIST) return;
    if (entry_encoding(e) != KV_ENCODING_QUICKLIST) return;

    kv_ql_node_t *node = ql_head(e);
    while (node) {
        kv_ql_node_t *next = node->next;
        ql_node_free(node);
        node = next;
    }
}

/* =====================================================================
 * Engine access
 * ===================================================================== */

/* g_dict replaced by sharded g_shards (kv_dict.h) */
extern long long get_time_now(void);

static const char *key_from_token(kvs_token_t t, char *buf, int sz) {
    int n = t.len < sz - 1 ? t.len : sz - 1;
    memcpy(buf, t.ptr, (size_t)n);
    buf[n] = '\0';
    return buf;
}

static int check_expiry(kv_entry_t *e, const char *key, uint64_t hash) {
    if (e && e->expire && get_time_now() > e->expire) {
        list_cleanup_entry(e);
        dict_delete(kv_dict_for(hash), key, (uint32_t)strlen(key), hash);
        return 1;
    }
    return 0;
}

/* =====================================================================
 * Build empty packed list
 * ===================================================================== */

static void list_build_empty(char *out_data) {
    out_data[0] = (char)KV_TYPE_LIST;
    out_data[1] = (char)KV_ENCODING_PACKED;
    pk_write_u32(out_data + 2, 0);
}

#define LIST_EMPTY_SIZE 6  /* type:1 + encoding:1 + count:4 */

/* =====================================================================
 * Get list length
 * ===================================================================== */

static uint32_t list_len(kv_entry_t *e) {
    if (entry_encoding(e) == KV_ENCODING_PACKED)
        return pk_read_u32(e->data + 2);
    return ql_total_count(e);
}

/* =====================================================================
 * Get element at index (0-based, negative from end)
 * ===================================================================== */

static int list_index(kv_entry_t *e, int index, const char **out_elem) {
    uint32_t len = list_len(e);
    if (len == 0) return -1;
    if (index < 0) index = (int)len + index;
    if (index < 0 || (uint32_t)index >= len) return -1;

    if (entry_encoding(e) == KV_ENCODING_PACKED) {
        int elen;
        *out_elem = pk_elem_get(e->data + 2, e->val_len - 2, (uint32_t)index, &elen);
        return elen;
    }

    kv_ql_node_t *node = ql_head(e);
    uint32_t skipped = 0;
    while (node) {
        if ((uint32_t)index < skipped + node->count) {
            int elen;
            *out_elem = pk_elem_get(node->data, node->data_len,
                                    (uint32_t)index - skipped, &elen);
            return elen;
        }
        skipped += node->count;
        node = node->next;
    }
    return -1;
}

/* =====================================================================
 * Convert packed list to quicklist
 * ===================================================================== */

static kv_entry_t *list_convert_to_quicklist(kv_entry_t *e) {
    uint32_t old_count = pk_read_u32(e->data + 2);
    uint32_t pk_len    = e->val_len - 2;  /* packed section length */

    if (old_count == 0) {
        /* Empty list: just resize to quicklist header */
        kv_entry_t *ne = dict_resize_entry(kv_dict_for((e)->hash), e, QL_HEADER_SIZE);
        if (!ne) return NULL;
        ql_init_header(ne);
        pk_write_u32(ne->data + QL_OFF_NODE_MAX, LIST_QL_NODE_MAX_ELEMS);
        return ne;
    }

    /* Save all elements before resize — dict_resize_entry may realloc */
    char **saved_elems = (char **)malloc((size_t)old_count * sizeof(char *));
    int   *saved_lens  = (int *)malloc((size_t)old_count * sizeof(int));
    if (!saved_elems || !saved_lens) {
        free(saved_elems); free(saved_lens);
        return NULL;
    }

    {
        const char *p = e->data + 6;  /* skip type:1 + encoding:1 + count:4 */
        for (uint32_t i = 0; i < old_count; i++) {
            uint32_t elen = pk_read_u32(p);
            const char *edata = p + 4;
            saved_lens[i]  = (int)elen;
            saved_elems[i] = (char *)malloc((size_t)elen);
            if (saved_elems[i]) memcpy(saved_elems[i], edata, (size_t)elen);
            p += 4 + elen;
        }
    }

    /* Now safe to resize */
    kv_entry_t *ne = dict_resize_entry(kv_dict_for((e)->hash), e, QL_HEADER_SIZE);
    if (!ne) goto err;
    e = ne;

    ql_init_header(e);
    pk_write_u32(e->data + QL_OFF_NODE_MAX, LIST_QL_NODE_MAX_ELEMS);

    /* Build quicklist nodes from saved data */
    {
        kv_ql_node_t *first = NULL, *last = NULL;
        uint32_t node_cnt = 0;

        for (uint32_t i = 0; i < old_count; i++) {
            if (!last || last->count >= LIST_QL_NODE_MAX_ELEMS ||
                last->data_len + 4 + (uint32_t)saved_lens[i] > LIST_QL_NODE_MAX_BYTES) {
                kv_ql_node_t *node = ql_node_alloc();
                if (!node) {
                    /* Cleanup partially built nodes */
                    kv_ql_node_t *cn = first;
                    while (cn) { kv_ql_node_t *nx = cn->next; ql_node_free(cn); cn = nx; }
                    first = NULL;
                    goto err;
                }
                if (!first) first = node;
                if (last)  { last->next = node; node->prev = last; }
                last = node;
                node_cnt++;
            }

            last = ql_node_push_right(last, saved_elems[i], saved_lens[i]);
            if (!last) {
                kv_ql_node_t *cn = first;
                while (cn) { kv_ql_node_t *nx = cn->next; ql_node_free(cn); cn = nx; }
                first = NULL;
                goto err;
            }
            if (!last->prev) first = last;  /* first node may have been realloc'd */
        }

        ql_set_head(e, first);
        ql_set_tail(e, last);
        ql_set_total_count(e, old_count);
        ql_set_node_count(e, node_cnt);
    }

    for (uint32_t i = 0; i < old_count; i++) free(saved_elems[i]);
    free(saved_elems);
    free(saved_lens);
    return e;

err:
    {
        uint32_t i;
        for (i = 0; i < old_count; i++) free(saved_elems[i]);
    }
    free(saved_elems);
    free(saved_lens);
    return NULL;
}

/* =====================================================================
 * Generic push/pop (handles both encodings)
 * ===================================================================== */

static int list_push(kv_entry_t **pe, const char *key,
                     const char *elem, int elem_len, int left) {
    kv_entry_t *e = *pe;

    if (entry_encoding(e) == KV_ENCODING_PACKED) {
        uint32_t pk_len = e->val_len - 2;  /* packed section only */
        uint32_t cur_count = pk_read_u32(e->data + 2);
        uint32_t need = (uint32_t)(4 + elem_len);

        if (cur_count + 1 > LIST_PACKED_MAX_ELEMS ||
            pk_len + need > LIST_PACKED_MAX_BYTES) {
            e = list_convert_to_quicklist(e);
            if (!e) return -1;
            *pe = e;
            goto do_quicklist;
        }

        /* Resize entry to fit new element */
        uint32_t new_dlen = (uint32_t)(2 + pk_len + need);
        kv_entry_t *ne = dict_resize_entry(kv_dict_for((e)->hash), e, new_dlen);
        if (!ne) return -1;
        e = ne;
        *pe = e;

        /* pk_len tracks the packed section length, starting from current */
        uint32_t new_pk_len = pk_len;
        if (left)
            pk_insert_front(e->data + 2, &new_pk_len, elem, elem_len);
        else
            pk_append(e->data + 2, &new_pk_len, elem, elem_len);
        e->val_len = new_pk_len + 2;
        return 0;
    }

do_quicklist:
    if (left)
        return ql_push_left(e, elem, elem_len);
    else
        return ql_push_right(e, elem, elem_len);
}

/* Pop: caller provides out_buf (must be large enough).
   For packed encoding, updates e->val_len via dict_resize_entry. */
static int list_pop(kv_entry_t **pe, const char *key,
                    char *out_buf, int *out_len, int left) {
    kv_entry_t *e = *pe;

    if (entry_encoding(e) == KV_ENCODING_PACKED) {
        uint32_t pk_len = e->val_len - 2;
        if (left)
            pk_pop_front(e->data + 2, &pk_len, out_buf, out_len);
        else
            pk_pop_back(e->data + 2, &pk_len, out_buf, out_len);

        /* Shrink entry */
        uint32_t new_dlen = pk_len + 2;
        kv_entry_t *ne = dict_resize_entry(kv_dict_for((e)->hash), e, new_dlen);
        if (ne) { ne->val_len = new_dlen; *pe = ne; }
        else    { e->val_len = new_dlen; }
        return 0;
    }

    if (left)
        return ql_pop_left(e, out_buf, out_len);
    else
        return ql_pop_right(e, out_buf, out_len);
}

/* =====================================================================
 * LRANGE response
 * ===================================================================== */

static void list_range_response(kv_entry_t *e, int start, int stop, DynBuf *wb) {
    uint32_t len = list_len(e);
    if (len == 0) { dynbuf_cat(wb, "*0\r\n", 4); return; }

    if (start < 0) start = (int)len + start;
    if (stop  < 0) stop  = (int)len + stop;
    if (start < 0) start = 0;
    if (stop >= (int)len) stop = (int)len - 1;
    if (start > stop) { dynbuf_cat(wb, "*0\r\n", 4); return; }

    int count = stop - start + 1;
    char hdr[32];
    int hl = snprintf(hdr, sizeof(hdr), "*%d\r\n", count);
    dynbuf_cat(wb, hdr, hl);

    if (entry_encoding(e) == KV_ENCODING_PACKED) {
        const char *pk = e->data + 2;
        uint32_t pk_len = e->val_len - 2;
        for (int i = start; i <= stop; i++) {
            int elen;
            const char *edata = pk_elem_get(pk, pk_len, (uint32_t)i, &elen);
            if (elen < 0) break;
            char bhdr[32];
            int bhl = snprintf(bhdr, sizeof(bhdr), "$%d\r\n", elen);
            dynbuf_cat(wb, bhdr, bhl);
            dynbuf_cat(wb, edata, elen);
            dynbuf_cat(wb, "\r\n", 2);
        }
        return;
    }

    kv_ql_node_t *node = ql_head(e);
    int idx = 0;
    while (node && idx <= stop) {
        for (uint32_t i = 0; i < node->count && idx <= stop; i++, idx++) {
            if (idx >= start) {
                int elen;
                const char *edata = pk_elem_get(node->data, node->data_len, i, &elen);
                if (elen >= 0) {
                    char bhdr[32];
                    int bhl = snprintf(bhdr, sizeof(bhdr), "$%d\r\n", elen);
                    dynbuf_cat(wb, bhdr, bhl);
                    dynbuf_cat(wb, edata, elen);
                    dynbuf_cat(wb, "\r\n", 2);
                }
            }
        }
        node = node->next;
    }
}

/* =====================================================================
 * Parse integer from token
 * ===================================================================== */

static int token_to_int(kvs_token_t t, int *out) {
    int v = 0, neg = 0, i = 0;
    if (i < t.len && t.ptr[i] == '-') { neg = 1; i++; }
    for (; i < t.len; i++) {
        if (t.ptr[i] < '0' || t.ptr[i] > '9') return -1;
        v = v * 10 + (t.ptr[i] - '0');
    }
    *out = neg ? -v : v;
    return 0;
}

/* =====================================================================
 * Flatten quicklist to packed format (for RDB persistence).
 * Allocates a buffer containing: [type:1][encoding:PACKED:1][count:4][elements...]
 * Caller must free *out_data.
 * ===================================================================== */

int list_flatten_to_packed(kv_entry_t *e, char **out_data, uint32_t *out_len) {
    *out_data = NULL;
    *out_len  = 0;

    if (!e || entry_type(e) != KV_TYPE_LIST ||
        entry_encoding(e) != KV_ENCODING_QUICKLIST)
        return -1;

    uint32_t total = ql_total_count(e);

    /* Compute total buffer size */
    uint32_t buf_size = 6; /* type + encoding + count */
    kv_ql_node_t *node = ql_head(e);
    while (node) {
        /* Each element in node: 4 bytes len + data */
        const char *pd = node->data + 4; /* skip node's count header */
        for (uint32_t i = 0; i < node->count; i++) {
            uint32_t elen = pk_read_u32(pd);
            buf_size += 4 + elen;
            pd += 4 + elen;
        }
        node = node->next;
    }

    char *buf = (char *)malloc(buf_size);
    if (!buf) return -1;

    buf[0] = (char)KV_TYPE_LIST;
    buf[1] = (char)KV_ENCODING_PACKED;
    pk_write_u32(buf + 2, total);

    uint32_t off = 6;
    node = ql_head(e);
    while (node) {
        const char *pd = node->data + 4;
        for (uint32_t i = 0; i < node->count; i++) {
            uint32_t elen = pk_read_u32(pd);
            pk_write_u32(buf + off, elen);
            memcpy(buf + off + 4, pd + 4, elen);
            off += 4 + elen;
            pd += 4 + elen;
        }
        node = node->next;
    }

    *out_data = buf;
    *out_len  = buf_size;
    return 0;
}

/* =====================================================================
 * Slice collection + rewrite helpers
 *
 * For multi-step mutators (LSET / LREM / LTRIM / LINSERT) we materialize
 * the entire list into a heap buffer of (ptr, len) slices, mutate the
 * array, then rebuild the list via list_push. This is O(N) but greatly
 * simplifies correctness: a single code path handles both PACKED and
 * QUICKLIST encodings, and conversion direction (shrink → packed,
 * grow → quicklist) is automatic.
 * ===================================================================== */

typedef struct {
    const char *p;
    int         len;
} list_slice_t;

/* Walks the list and copies all elements into a single contiguous heap
   buffer. Returns 0 on success, -1 on OOM. Caller frees *out_slices and
   *out_buf even on success. */
static int list_collect(kv_entry_t *e,
                        list_slice_t **out_slices, int *out_count,
                        char **out_buf)
{
    *out_slices = NULL; *out_buf = NULL; *out_count = 0;

    uint32_t total_n = list_len(e);
    if (total_n == 0) return 0;

    /* Pass 1: compute total byte size */
    uint32_t total_bytes = 0;
    if (entry_encoding(e) == KV_ENCODING_PACKED) {
        const char *p = e->data + 6;
        for (uint32_t i = 0; i < total_n; i++) {
            uint32_t el = pk_read_u32(p);
            total_bytes += el;
            p += 4 + el;
        }
    } else {
        kv_ql_node_t *node = ql_head(e);
        while (node) {
            const char *p = node->data + 4;
            for (uint32_t i = 0; i < node->count; i++) {
                uint32_t el = pk_read_u32(p);
                total_bytes += el;
                p += 4 + el;
            }
            node = node->next;
        }
    }

    char         *buf = (char *)malloc(total_bytes > 0 ? total_bytes : 1);
    list_slice_t *arr = (list_slice_t *)malloc(sizeof(list_slice_t) * total_n);
    if (!buf || !arr) { free(buf); free(arr); return -1; }

    /* Pass 2: copy element bodies + populate slice pointers */
    char *bp = buf;
    int   idx = 0;
    if (entry_encoding(e) == KV_ENCODING_PACKED) {
        const char *p = e->data + 6;
        for (uint32_t i = 0; i < total_n; i++) {
            uint32_t el = pk_read_u32(p);
            memcpy(bp, p + 4, el);
            arr[idx].p = bp; arr[idx].len = (int)el;
            bp += el; idx++;
            p += 4 + el;
        }
    } else {
        kv_ql_node_t *node = ql_head(e);
        while (node) {
            const char *p = node->data + 4;
            for (uint32_t i = 0; i < node->count; i++) {
                uint32_t el = pk_read_u32(p);
                memcpy(bp, p + 4, el);
                arr[idx].p = bp; arr[idx].len = (int)el;
                bp += el; idx++;
                p += 4 + el;
            }
            node = node->next;
        }
    }

    *out_slices = arr;
    *out_count  = (int)total_n;
    *out_buf    = buf;
    return 0;
}

/* Replace the contents of *pe with the provided slice array. Preserves
   TTL. May reallocate *pe via list_push. */
static int list_rewrite(kv_entry_t **pe, const char *key,
                        const list_slice_t *slices, int count)
{
    kv_entry_t *e = *pe;
    long long saved_expire = e->expire;

    /* Free any external quicklist nodes; entry data is owned by the dict. */
    if (entry_encoding(e) == KV_ENCODING_QUICKLIST)
        list_cleanup_entry(e);

    /* Reset entry to empty PACKED list. */
    kv_entry_t *ne = dict_resize_entry(kv_dict_for((e)->hash), e, LIST_EMPTY_SIZE);
    if (!ne) return -1;
    list_build_empty(ne->data);
    ne->expire = saved_expire;
    *pe = ne;

    /* Push all slices to the tail; list_push auto-converts to quicklist
       once packed limits are exceeded. */
    for (int i = 0; i < count; i++) {
        if (list_push(pe, key, slices[i].p, slices[i].len, /*left=*/0) < 0)
            return -1;
    }
    return 0;
}

/* After a pop, if the list became empty Redis deletes the key. */
static void list_maybe_delete_empty(kv_entry_t *e, const char *key, uint64_t hash) {
    if (!e || list_len(e) != 0) return;
    list_cleanup_entry(e);
    dict_delete(kv_dict_for(hash), key, (uint32_t)strlen(key), hash);
}

/* Locate a list-typed entry by token; returns 0 if absent or wrong type
   (sets *out_e to NULL and writes WRONGTYPE on wrong-type). */
static int list_lookup(kvs_token_t key_tok, kv_entry_t **out_e,
                       uint64_t *out_hash, char *kbuf, int kbuf_sz,
                       DynBuf *wb)
{
    const char *k = key_from_token(key_tok, kbuf, kbuf_sz);
    uint32_t kl = (uint32_t)strlen(k);
    uint64_t h = kv_hash((const uint8_t *)k, kl);
    kv_entry_t *e = dict_find(kv_dict_for(h), k, kl, h);
    *out_hash = h;
    if (!e) { *out_e = NULL; return 0; }
    if (entry_type(e) != KV_TYPE_LIST) {
        if (wb) dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
        *out_e = NULL;
        return -1;
    }
    if (check_expiry(e, k, h)) { *out_e = NULL; return 0; }
    *out_e = e;
    return 0;
}

/* =====================================================================
 * LIST command dispatch
 * ===================================================================== */

dispatch_result_t list_cmd(kvs_token_t cmd, kv_entry_t *entry,
                           kvs_token_t *args, int argc, DynBuf *wb,
                           const char *msg_raw, int msg_len,
                           client_t *client)
{
    (void)msg_raw;
    (void)msg_len;

    char key_buf[256] = {0};
    const char *key = NULL;
    if (argc >= 1)
        key = key_from_token(args[0], key_buf, (int)sizeof(key_buf));

    char temp[64];
    int ret;

    switch (cmd.ptr[0]) {

    /* ── LPUSH / LPOP / LLEN / LRANGE / LINDEX ── */
    case 'L':
        if (TOKEN_EQ(cmd, "LPUSH")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }

            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            kv_entry_t *e = dict_find(kv_dict_for(hash), key, (uint32_t)strlen(key), hash);

            if (!e) {
                char data[16];
                list_build_empty(data);
                e = dict_add(kv_dict_for(hash), key, (uint32_t)strlen(key), data, LIST_EMPTY_SIZE, hash);
                if (!e) { dynbuf_cat(wb, "-ERR create list failed\r\n", 24); return DISPATCH_ERROR; }
            } else if (entry_type(e) != KV_TYPE_LIST) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }

            if (check_expiry(e, key, hash)) {
                char data[16];
                list_build_empty(data);
                e = dict_add(kv_dict_for(hash), key, (uint32_t)strlen(key), data, LIST_EMPTY_SIZE, hash);
                if (!e) { dynbuf_cat(wb, "-ERR create list failed\r\n", 24); return DISPATCH_ERROR; }
            }

            for (int i = 1; i < argc; i++) {
                if (list_push(&e, key, args[i].ptr, args[i].len, 1) < 0) {
                    dynbuf_cat(wb, "-ERR push failed\r\n", 18);
                    return DISPATCH_ERROR;
                }
            }

            uint32_t ll = list_len(e);
            int hl = snprintf(temp, sizeof(temp), ":%u\r\n", ll);
            dynbuf_cat(wb, temp, hl);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "LPOP")) {
            if (!entry) { dynbuf_cat(wb, REP_NIL, REP_NIL_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_LIST) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }

            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, hash)) {
                dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                return DISPATCH_OK;
            }

            int pop_count = 1;
            int has_count = (argc >= 2);
            if (has_count) { token_to_int(args[1], &pop_count); if (pop_count <= 0) pop_count = 1; }

            uint32_t avail = list_len(entry);
            int actual = (int)avail < pop_count ? (int)avail : pop_count;

            if (actual == 0) {
                dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                return DISPATCH_OK;
            }

            if (!has_count) {
                char out_buf[65536];
                int olen = 0;
                if (list_pop(&entry, key, out_buf, &olen, 1) < 0) {
                    dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                    return DISPATCH_ERROR;
                }
                int bh = snprintf(temp, sizeof(temp), "$%d\r\n", olen);
                dynbuf_cat(wb, temp, bh);
                dynbuf_cat(wb, out_buf, olen);
                dynbuf_cat(wb, "\r\n", 2);
                list_maybe_delete_empty(entry, key, hash);
                return DISPATCH_OK;
            }

            char hdr[32];
            int hl = snprintf(hdr, sizeof(hdr), "*%d\r\n", actual);
            dynbuf_cat(wb, hdr, hl);

            char out_buf[65536];
            for (int i = 0; i < actual; i++) {
                int olen = 0;
                if (list_pop(&entry, key, out_buf, &olen, 1) < 0) break;
                int bh = snprintf(hdr, sizeof(hdr), "$%d\r\n", olen);
                dynbuf_cat(wb, hdr, bh);
                dynbuf_cat(wb, out_buf, olen);
                dynbuf_cat(wb, "\r\n", 2);
            }
            list_maybe_delete_empty(entry, key, hash);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "LLEN")) {
            if (!entry) { dynbuf_cat(wb, ":0\r\n", 4); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_LIST) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, hash)) {
                dynbuf_cat(wb, ":0\r\n", 4);
                return DISPATCH_OK;
            }
            int hl = snprintf(temp, sizeof(temp), ":%u\r\n", list_len(entry));
            dynbuf_cat(wb, temp, hl);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "LRANGE")) {
            if (!entry) { dynbuf_cat(wb, "*0\r\n", 4); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_LIST) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            if (argc < 3) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }

            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, hash)) {
                dynbuf_cat(wb, "*0\r\n", 4);
                return DISPATCH_OK;
            }

            int start = 0, stop = 0;
            token_to_int(args[1], &start);
            token_to_int(args[2], &stop);

            list_range_response(entry, start, stop, wb);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "LINDEX")) {
            if (!entry) { dynbuf_cat(wb, REP_NIL, REP_NIL_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_LIST) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }

            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, hash)) {
                dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                return DISPATCH_OK;
            }

            int idx = 0;
            token_to_int(args[1], &idx);

            const char *edata;
            int elen = list_index(entry, idx, &edata);
            if (elen < 0) {
                dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
            } else {
                int hl = snprintf(temp, sizeof(temp), "$%d\r\n", elen);
                dynbuf_cat(wb, temp, hl);
                dynbuf_cat(wb, edata, elen);
                dynbuf_cat(wb, "\r\n", 2);
            }
            return DISPATCH_OK;
        }

        /* ── LPUSHX key val [val ...] — push only if key exists ── */
        if (TOKEN_EQ(cmd, "LPUSHX")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, ":0\r\n", 4); return DISPATCH_OK; }
            kv_entry_t *e = entry;
            for (int i = 1; i < argc; i++) {
                if (list_push(&e, key, args[i].ptr, args[i].len, /*left=*/1) < 0) {
                    dynbuf_cat(wb, "-ERR push failed\r\n", 18);
                    return DISPATCH_ERROR;
                }
            }
            int hl = snprintf(temp, sizeof(temp), ":%u\r\n", list_len(e));
            dynbuf_cat(wb, temp, hl);
            return DISPATCH_OK;
        }

        /* ── LSET key idx value ── */
        if (TOKEN_EQ(cmd, "LSET")) {
            if (argc < 3) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) {
                dynbuf_cat(wb, "-ERR no such key\r\n", 18);
                return DISPATCH_ERROR;
            }
            int idx = 0;
            if (token_to_int(args[1], &idx) < 0) {
                dynbuf_cat(wb, "-ERR value is not an integer or out of range\r\n", 46);
                return DISPATCH_ERROR;
            }
            uint32_t llen = list_len(entry);
            if (idx < 0) idx = (int)llen + idx;
            if (idx < 0 || (uint32_t)idx >= llen) {
                dynbuf_cat(wb, "-ERR index out of range\r\n", 25);
                return DISPATCH_ERROR;
            }

            list_slice_t *sl = NULL; int sc = 0; char *bb = NULL;
            if (list_collect(entry, &sl, &sc, &bb) < 0) {
                free(sl); free(bb);
                dynbuf_cat(wb, "-ERR collect failed\r\n", 21);
                return DISPATCH_ERROR;
            }
            sl[idx].p = args[2].ptr;
            sl[idx].len = args[2].len;
            kv_entry_t *e = entry;
            int rr = list_rewrite(&e, key, sl, sc);
            free(sl); free(bb);
            if (rr < 0) { dynbuf_cat(wb, "-ERR rewrite failed\r\n", 21);
                          return DISPATCH_ERROR; }
            dynbuf_cat(wb, REP_OK, REP_OK_LEN);
            return DISPATCH_OK;
        }

        /* ── LREM key count value ── */
        if (TOKEN_EQ(cmd, "LREM")) {
            if (argc < 3) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, ":0\r\n", 4); return DISPATCH_OK; }
            int count = 0;
            if (token_to_int(args[1], &count) < 0) {
                dynbuf_cat(wb, "-ERR value is not an integer or out of range\r\n", 46);
                return DISPATCH_ERROR;
            }
            kvs_token_t vtok = args[2];

            list_slice_t *sl = NULL; int sc = 0; char *bb = NULL;
            if (list_collect(entry, &sl, &sc, &bb) < 0) {
                free(sl); free(bb);
                dynbuf_cat(wb, "-ERR collect failed\r\n", 21);
                return DISPATCH_ERROR;
            }

            int removed = 0;
            /* keep[i] = 1 means keep, 0 means drop */
            char *keep = (char *)malloc((size_t)(sc > 0 ? sc : 1));
            if (!keep) { free(sl); free(bb);
                         dynbuf_cat(wb, "-ERR oom\r\n", 10);
                         return DISPATCH_ERROR; }
            for (int i = 0; i < sc; i++) keep[i] = 1;

            int limit = count == 0 ? sc : (count > 0 ? count : -count);
            if (count >= 0) {
                for (int i = 0; i < sc && removed < limit; i++) {
                    if (sl[i].len == vtok.len &&
                        memcmp(sl[i].p, vtok.ptr, (size_t)vtok.len) == 0) {
                        keep[i] = 0; removed++;
                    }
                }
            } else {
                for (int i = sc - 1; i >= 0 && removed < limit; i--) {
                    if (sl[i].len == vtok.len &&
                        memcmp(sl[i].p, vtok.ptr, (size_t)vtok.len) == 0) {
                        keep[i] = 0; removed++;
                    }
                }
            }

            /* Compact kept slices in place */
            int new_sc = 0;
            for (int i = 0; i < sc; i++) {
                if (keep[i]) sl[new_sc++] = sl[i];
            }

            kv_entry_t *e = entry;
            int rr = list_rewrite(&e, key, sl, new_sc);
            free(sl); free(bb); free(keep);
            if (rr < 0) { dynbuf_cat(wb, "-ERR rewrite failed\r\n", 21);
                          return DISPATCH_ERROR; }
            int hl = snprintf(temp, sizeof(temp), ":%d\r\n", removed);
            dynbuf_cat(wb, temp, hl);

            /* If we emptied the list, drop the key. */
            uint64_t h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            list_maybe_delete_empty(e, key, h);
            return DISPATCH_OK;
        }

        /* ── LTRIM key start stop ── */
        if (TOKEN_EQ(cmd, "LTRIM")) {
            if (argc < 3) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, REP_OK, REP_OK_LEN); return DISPATCH_OK; }

            int start = 0, stop = 0;
            if (token_to_int(args[1], &start) < 0 ||
                token_to_int(args[2], &stop)  < 0) {
                dynbuf_cat(wb, "-ERR value is not an integer or out of range\r\n", 46);
                return DISPATCH_ERROR;
            }

            list_slice_t *sl = NULL; int sc = 0; char *bb = NULL;
            if (list_collect(entry, &sl, &sc, &bb) < 0) {
                free(sl); free(bb);
                dynbuf_cat(wb, "-ERR collect failed\r\n", 21);
                return DISPATCH_ERROR;
            }
            if (start < 0) start += sc;
            if (stop  < 0) stop  += sc;
            if (start < 0) start = 0;
            if (stop  >= sc) stop = sc - 1;

            int new_sc;
            if (start > stop || sc == 0) {
                new_sc = 0;
            } else {
                new_sc = stop - start + 1;
                for (int i = 0; i < new_sc; i++) sl[i] = sl[start + i];
            }

            kv_entry_t *e = entry;
            int rr = list_rewrite(&e, key, sl, new_sc);
            free(sl); free(bb);
            if (rr < 0) { dynbuf_cat(wb, "-ERR rewrite failed\r\n", 21);
                          return DISPATCH_ERROR; }
            dynbuf_cat(wb, REP_OK, REP_OK_LEN);
            uint64_t h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            list_maybe_delete_empty(e, key, h);
            return DISPATCH_OK;
        }

        /* ── LINSERT key BEFORE|AFTER pivot value ── */
        if (TOKEN_EQ(cmd, "LINSERT")) {
            if (argc < 4) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, ":0\r\n", 4); return DISPATCH_OK; }

            int before;
            kvs_token_t pos = args[1];
            if (pos.len == 6 && (memcmp(pos.ptr, "BEFORE", 6) == 0 ||
                                  memcmp(pos.ptr, "before", 6) == 0)) before = 1;
            else if (pos.len == 5 && (memcmp(pos.ptr, "AFTER", 5) == 0 ||
                                       memcmp(pos.ptr, "after", 5) == 0)) before = 0;
            else { dynbuf_cat(wb, "-ERR syntax error\r\n", 19);
                   return DISPATCH_ERROR; }

            kvs_token_t pivot = args[2];
            kvs_token_t value = args[3];

            list_slice_t *sl = NULL; int sc = 0; char *bb = NULL;
            if (list_collect(entry, &sl, &sc, &bb) < 0) {
                free(sl); free(bb);
                dynbuf_cat(wb, "-ERR collect failed\r\n", 21);
                return DISPATCH_ERROR;
            }

            int pivot_idx = -1;
            for (int i = 0; i < sc; i++) {
                if (sl[i].len == pivot.len &&
                    memcmp(sl[i].p, pivot.ptr, (size_t)pivot.len) == 0) {
                    pivot_idx = i; break;
                }
            }

            if (pivot_idx < 0) {
                free(sl); free(bb);
                dynbuf_cat(wb, ":-1\r\n", 5);
                return DISPATCH_OK;
            }

            /* Build new array with one extra slot. */
            int new_sc = sc + 1;
            list_slice_t *nsl = (list_slice_t *)malloc(sizeof(list_slice_t) * (size_t)new_sc);
            if (!nsl) { free(sl); free(bb);
                        dynbuf_cat(wb, "-ERR oom\r\n", 10);
                        return DISPATCH_ERROR; }

            int insert_at = before ? pivot_idx : pivot_idx + 1;
            for (int i = 0; i < insert_at; i++) nsl[i] = sl[i];
            nsl[insert_at].p = value.ptr;
            nsl[insert_at].len = value.len;
            for (int i = insert_at; i < sc; i++) nsl[i + 1] = sl[i];

            kv_entry_t *e = entry;
            int rr = list_rewrite(&e, key, nsl, new_sc);
            free(sl); free(bb); free(nsl);
            if (rr < 0) { dynbuf_cat(wb, "-ERR rewrite failed\r\n", 21);
                          return DISPATCH_ERROR; }
            int hl = snprintf(temp, sizeof(temp), ":%u\r\n", list_len(e));
            dynbuf_cat(wb, temp, hl);
            return DISPATCH_OK;
        }

        /* ── LPOS key element [RANK r] [COUNT c] ──
             Simplified: supports COUNT only. Returns integer (first match)
             or array when COUNT given. */
        if (TOKEN_EQ(cmd, "LPOS")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            int want_count = -1;  /* -1 means single-match mode */
            for (int i = 2; i + 1 < argc; i += 2) {
                kvs_token_t opt = args[i];
                if (opt.len == 5 && (memcmp(opt.ptr, "COUNT", 5) == 0 ||
                                      memcmp(opt.ptr, "count", 5) == 0)) {
                    if (token_to_int(args[i + 1], &want_count) < 0 || want_count < 0) {
                        dynbuf_cat(wb, "-ERR COUNT must be >= 0\r\n", 25);
                        return DISPATCH_ERROR;
                    }
                } else {
                    dynbuf_cat(wb, "-ERR syntax error\r\n", 19);
                    return DISPATCH_ERROR;
                }
            }
            if (!entry) {
                dynbuf_cat(wb, want_count < 0 ? REP_NIL : "*0\r\n",
                           want_count < 0 ? REP_NIL_LEN : 4);
                return DISPATCH_OK;
            }
            kvs_token_t vtok = args[1];

            list_slice_t *sl = NULL; int sc = 0; char *bb = NULL;
            if (list_collect(entry, &sl, &sc, &bb) < 0) {
                free(sl); free(bb);
                dynbuf_cat(wb, "-ERR collect failed\r\n", 21);
                return DISPATCH_ERROR;
            }

            if (want_count < 0) {
                int found = -1;
                for (int i = 0; i < sc; i++) {
                    if (sl[i].len == vtok.len &&
                        memcmp(sl[i].p, vtok.ptr, (size_t)vtok.len) == 0) {
                        found = i; break;
                    }
                }
                free(sl); free(bb);
                if (found < 0) dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                else { int hl = snprintf(temp, sizeof(temp), ":%d\r\n", found);
                       dynbuf_cat(wb, temp, hl); }
                return DISPATCH_OK;
            }

            /* COUNT mode: gather up to want_count matches (0 = all). */
            int cap = sc > 0 ? sc : 1;
            int *idxs = (int *)malloc(sizeof(int) * (size_t)cap);
            int n = 0;
            if (!idxs) { free(sl); free(bb);
                         dynbuf_cat(wb, "-ERR oom\r\n", 10);
                         return DISPATCH_ERROR; }
            int limit = want_count == 0 ? sc : want_count;
            for (int i = 0; i < sc && n < limit; i++) {
                if (sl[i].len == vtok.len &&
                    memcmp(sl[i].p, vtok.ptr, (size_t)vtok.len) == 0)
                    idxs[n++] = i;
            }
            free(sl); free(bb);

            int hl = snprintf(temp, sizeof(temp), "*%d\r\n", n);
            dynbuf_cat(wb, temp, hl);
            for (int i = 0; i < n; i++) {
                int el = snprintf(temp, sizeof(temp), ":%d\r\n", idxs[i]);
                dynbuf_cat(wb, temp, el);
            }
            free(idxs);
            return DISPATCH_OK;
        }

        /* ── LMOVE src dst LEFT|RIGHT LEFT|RIGHT ── */
        if (TOKEN_EQ(cmd, "LMOVE")) {
            if (argc < 4) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, REP_NIL, REP_NIL_LEN); return DISPATCH_OK; }

            int src_left, dst_left;
            kvs_token_t sd = args[2], dd = args[3];
            if (sd.len == 4 && (memcmp(sd.ptr, "LEFT", 4) == 0 || memcmp(sd.ptr, "left", 4) == 0))      src_left = 1;
            else if (sd.len == 5 && (memcmp(sd.ptr, "RIGHT", 5) == 0 || memcmp(sd.ptr, "right", 5) == 0)) src_left = 0;
            else { dynbuf_cat(wb, "-ERR syntax error\r\n", 19); return DISPATCH_ERROR; }
            if (dd.len == 4 && (memcmp(dd.ptr, "LEFT", 4) == 0 || memcmp(dd.ptr, "left", 4) == 0))      dst_left = 1;
            else if (dd.len == 5 && (memcmp(dd.ptr, "RIGHT", 5) == 0 || memcmp(dd.ptr, "right", 5) == 0)) dst_left = 0;
            else { dynbuf_cat(wb, "-ERR syntax error\r\n", 19); return DISPATCH_ERROR; }

            char dst_kb[256];
            kv_entry_t *dst_e = NULL;
            uint64_t dst_h = 0;
            if (list_lookup(args[1], &dst_e, &dst_h, dst_kb, (int)sizeof(dst_kb), wb) < 0)
                return DISPATCH_ERROR;
            const char *dst_k = dst_kb;

            uint64_t src_h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));

            char out_buf[65536];
            int olen = 0;
            if (list_pop(&entry, key, out_buf, &olen, src_left) < 0) {
                dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                return DISPATCH_OK;
            }

            if (!dst_e) {
                char data[16]; list_build_empty(data);
                dst_e = dict_add(kv_dict_for(dst_h), dst_k, (uint32_t)strlen(dst_k), data, LIST_EMPTY_SIZE, dst_h);
                if (!dst_e) { dynbuf_cat(wb, "-ERR create list failed\r\n", 24);
                              return DISPATCH_ERROR; }
            }
            if (list_push(&dst_e, dst_k, out_buf, olen, dst_left) < 0) {
                dynbuf_cat(wb, "-ERR push failed\r\n", 18);
                return DISPATCH_ERROR;
            }

            list_maybe_delete_empty(entry, key, src_h);

            int bh = snprintf(temp, sizeof(temp), "$%d\r\n", olen);
            dynbuf_cat(wb, temp, bh);
            dynbuf_cat(wb, out_buf, olen);
            dynbuf_cat(wb, "\r\n", 2);
            return DISPATCH_OK;
        }
        break; /* 'L' */

    /* ── RPUSH / RPOP ── */
    case 'R':
        if (TOKEN_EQ(cmd, "RPUSH")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }

            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            kv_entry_t *e = dict_find(kv_dict_for(hash), key, (uint32_t)strlen(key), hash);

            if (!e) {
                char data[16];
                list_build_empty(data);
                e = dict_add(kv_dict_for(hash), key, (uint32_t)strlen(key), data, LIST_EMPTY_SIZE, hash);
                if (!e) { dynbuf_cat(wb, "-ERR create list failed\r\n", 24); return DISPATCH_ERROR; }
            } else if (entry_type(e) != KV_TYPE_LIST) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }

            if (check_expiry(e, key, hash)) {
                char data[16];
                list_build_empty(data);
                e = dict_add(kv_dict_for(hash), key, (uint32_t)strlen(key), data, LIST_EMPTY_SIZE, hash);
                if (!e) { dynbuf_cat(wb, "-ERR create list failed\r\n", 24); return DISPATCH_ERROR; }
            }

            for (int i = 1; i < argc; i++) {
                if (list_push(&e, key, args[i].ptr, args[i].len, 0) < 0) {
                    dynbuf_cat(wb, "-ERR push failed\r\n", 18);
                    return DISPATCH_ERROR;
                }
            }

            uint32_t ll = list_len(e);
            int hl = snprintf(temp, sizeof(temp), ":%u\r\n", ll);
            dynbuf_cat(wb, temp, hl);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "RPOP")) {
            if (!entry) { dynbuf_cat(wb, REP_NIL, REP_NIL_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_LIST) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }

            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, hash)) {
                dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                return DISPATCH_OK;
            }

            int pop_count = 1;
            int has_count = (argc >= 2);
            if (has_count) { token_to_int(args[1], &pop_count); if (pop_count <= 0) pop_count = 1; }

            uint32_t avail = list_len(entry);
            int actual = (int)avail < pop_count ? (int)avail : pop_count;

            if (actual == 0) {
                dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                return DISPATCH_OK;
            }

            if (!has_count) {
                char out_buf[65536];
                int olen = 0;
                if (list_pop(&entry, key, out_buf, &olen, 0) < 0) {
                    dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                    return DISPATCH_ERROR;
                }
                int bh = snprintf(temp, sizeof(temp), "$%d\r\n", olen);
                dynbuf_cat(wb, temp, bh);
                dynbuf_cat(wb, out_buf, olen);
                dynbuf_cat(wb, "\r\n", 2);
                list_maybe_delete_empty(entry, key, hash);
                return DISPATCH_OK;
            }

            char hdr[32];
            int hl = snprintf(hdr, sizeof(hdr), "*%d\r\n", actual);
            dynbuf_cat(wb, hdr, hl);

            char out_buf[65536];
            for (int i = 0; i < actual; i++) {
                int olen = 0;
                if (list_pop(&entry, key, out_buf, &olen, 0) < 0) break;
                int bh = snprintf(hdr, sizeof(hdr), "$%d\r\n", olen);
                dynbuf_cat(wb, hdr, bh);
                dynbuf_cat(wb, out_buf, olen);
                dynbuf_cat(wb, "\r\n", 2);
            }
            list_maybe_delete_empty(entry, key, hash);
            return DISPATCH_OK;
        }

        /* ── RPOPLPUSH src dst ── */
        if (TOKEN_EQ(cmd, "RPOPLPUSH")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, REP_NIL, REP_NIL_LEN); return DISPATCH_OK; }

            char dst_kb[256];
            kv_entry_t *dst_e = NULL;
            uint64_t dst_h = 0;
            if (list_lookup(args[1], &dst_e, &dst_h, dst_kb, (int)sizeof(dst_kb), wb) < 0)
                return DISPATCH_ERROR;
            const char *dst_k = dst_kb;

            uint64_t src_h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));

            char out_buf[65536];
            int olen = 0;
            if (list_pop(&entry, key, out_buf, &olen, /*left=*/0) < 0) {
                dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                return DISPATCH_OK;
            }

            /* Create dst if absent */
            if (!dst_e) {
                char data[16]; list_build_empty(data);
                dst_e = dict_add(kv_dict_for(dst_h), dst_k, (uint32_t)strlen(dst_k), data, LIST_EMPTY_SIZE, dst_h);
                if (!dst_e) { dynbuf_cat(wb, "-ERR create list failed\r\n", 24);
                              return DISPATCH_ERROR; }
            }
            if (list_push(&dst_e, dst_k, out_buf, olen, /*left=*/1) < 0) {
                dynbuf_cat(wb, "-ERR push failed\r\n", 18);
                return DISPATCH_ERROR;
            }

            /* If src == dst, src wasn't deleted (push refilled it). */
            list_maybe_delete_empty(entry, key, src_h);

            int bh = snprintf(temp, sizeof(temp), "$%d\r\n", olen);
            dynbuf_cat(wb, temp, bh);
            dynbuf_cat(wb, out_buf, olen);
            dynbuf_cat(wb, "\r\n", 2);
            return DISPATCH_OK;
        }

        /* ── RPUSHX key val [val ...] — push only if key exists ── */
        if (TOKEN_EQ(cmd, "RPUSHX")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, ":0\r\n", 4); return DISPATCH_OK; }
            kv_entry_t *e = entry;
            for (int i = 1; i < argc; i++) {
                if (list_push(&e, key, args[i].ptr, args[i].len, /*left=*/0) < 0) {
                    dynbuf_cat(wb, "-ERR push failed\r\n", 18);
                    return DISPATCH_ERROR;
                }
            }
            int hl = snprintf(temp, sizeof(temp), ":%u\r\n", list_len(e));
            dynbuf_cat(wb, temp, hl);
            return DISPATCH_OK;
        }
        break; /* 'R' */

    /* ── BLPOP / BRPOP ── */
    case 'B':
        if (TOKEN_EQ(cmd, "BLPOP") || TOKEN_EQ(cmd, "BRPOP")) {
            int is_left = TOKEN_EQ(cmd, "BLPOP");
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }

            int num_keys = argc - 1;
            long long timeout_sec = 0;
            {
                long long v = 0;
                kvs_token_t tt = args[argc - 1];
                for (int i = 0; i < tt.len; i++)
                    v = v * 10 + (tt.ptr[i] - '0');
                timeout_sec = v;
            }

            /* Try each key in order, pop from first non-empty list */
            for (int i = 0; i < num_keys; i++) {
                char kb[256];
                const char *k = key_from_token(args[i], kb, (int)sizeof(kb));
                uint64_t hash = kv_hash((const uint8_t *)k, (uint32_t)strlen(k));
                kv_entry_t *e = dict_find(kv_dict_for(hash), k, (uint32_t)strlen(k), hash);

                if (!e || entry_type(e) != KV_TYPE_LIST) continue;
                if (check_expiry(e, k, hash)) continue;

                uint32_t ll = list_len(e);
                if (ll == 0) continue;

                /* Found a non-empty list — pop and return [key, value] */
                char out_buf[65536];
                int olen = 0;
                list_pop(&e, k, out_buf, &olen, is_left);

                dynbuf_cat(wb, "*2\r\n", 4);
                int kl = (int)strlen(k);
                int hl = snprintf(temp, sizeof(temp), "$%d\r\n", kl);
                dynbuf_cat(wb, temp, hl);
                dynbuf_cat(wb, k, kl);
                dynbuf_cat(wb, "\r\n", 2);
                hl = snprintf(temp, sizeof(temp), "$%d\r\n", olen);
                dynbuf_cat(wb, temp, hl);
                dynbuf_cat(wb, out_buf, olen);
                dynbuf_cat(wb, "\r\n", 2);
                list_maybe_delete_empty(e, k, hash);
                return DISPATCH_OK;
            }

            /* All keys empty — block or return nil */
            if (!client) {
                dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                return DISPATCH_OK;
            }

            client->state = CLIENT_BLOCKED;
            client->block_reason = BLOCK_LIST;
            client->block_key_count = 0;

            for (int i = 0; i < num_keys && i < 4; i++) {
                char kb[256];
                const char *k = key_from_token(args[i], kb, (int)sizeof(kb));
                int klen = (int)strlen(k);
                if (klen < 64) {
                    memcpy(client->block_keys[i], k, (size_t)klen + 1);
                }
                block_add(k, (uint32_t)klen, client);
                client->block_key_count++;
            }

            /* Phase 4B.2: use real wall clock (not the cached ms — only
             * worker 0 updates the cache, so other workers may register
             * with a stale base, causing the timeout to fire early). */
            if (timeout_sec > 0)
                client->block_until_ms = get_time_now_real() + timeout_sec * 1000;
            else
                client->block_until_ms = 0;  /* 0 means no timeout */

            /* Phase 4B.2: ask worker 0 (the timeout sweeper) to recompute
             * its nearest-timeout SQE.  Without this, worker 0 may already
             * be parked in io_uring_submit_and_wait() with no timeout
             * armed and would only wake on the next data CQE. */
            if (timeout_sec > 0) proactor_kick_worker(0);

            if (msg_raw && msg_len > 0 && msg_len < (int)sizeof(client->block_cmd_buf)) {
                memcpy(client->block_cmd_buf, msg_raw, msg_len);
                client->block_cmd_len = msg_len;
            }

            return DISPATCH_BLOCKED;
        }
        break;

    }
    (void)ret;

    dynbuf_cat(wb, REP_ERR_UNK, REP_ERR_UNK_LEN);
    return DISPATCH_ERROR;
}
