#include "kv_type_set.h"

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
S(REP_ONE,       ":1\r\n");
S(REP_ZERO,      ":0\r\n");
S(REP_ERR_ARGS,  "-ERR wrong number of arguments\r\n");
S(REP_ERR_UNK,   "-ERR unknown command\r\n");
S(REP_WRONGTYPE, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");

#undef S

/* =====================================================================
 * Limits
 * ===================================================================== */

#define SET_PACKED_MAX_ELEMS  128
#define SET_PACKED_MAX_BYTES  4096
#define SET_SHRINK_THRESHOLD  64

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

/* Find element by index, returns pointer to its length field */
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

/* Search for element in packed set. Returns index or -1. */
static int pk_find(const char *data, uint32_t data_len,
                   const char *elem, uint32_t elem_len) {
    uint32_t count = pk_read_u32(data);
    const char *p = data + 4;
    for (uint32_t i = 0; i < count; i++) {
        uint32_t elen = pk_read_u32(p);
        if (elen == elem_len && memcmp(p + 4, elem, elen) == 0)
            return (int)i;
        p += 4 + elen;
    }
    return -1;
}

/* Append element at end */
static int pk_append(char *data, uint32_t *data_len,
                     const char *elem, int elem_len) {
    uint32_t old_count = pk_read_u32(data);
    char *p = data + *data_len;
    pk_write_u32(p, (uint32_t)elem_len);
    memcpy(p + 4, elem, (size_t)elem_len);
    *data_len += (uint32_t)(4 + elem_len);
    pk_write_u32(data, old_count + 1);
    return 0;
}

/* Remove element at index, compact remaining */
static void pk_remove_at(char *data, uint32_t *data_len, uint32_t idx) {
    uint32_t count = pk_read_u32(data);
    if (idx >= count) return;

    char *elem_p = pk_elem_ptr(data, *data_len, idx);
    if (!elem_p) return;
    uint32_t elen = pk_read_u32(elem_p);
    uint32_t consumed = 4 + elen;
    uint32_t offset = (uint32_t)(elem_p - data);
    uint32_t remaining = *data_len - offset - consumed;

    if (remaining > 0)
        memmove(elem_p, elem_p + consumed, remaining);
    *data_len -= consumed;
    pk_write_u32(data, count - 1);
}

/* Swap-remove last element to fill the gap at idx.
 * Save last element data, truncate last, overwrite idx with saved data. */
static void pk_swap_remove(char *data, uint32_t *data_len, uint32_t idx) {
    uint32_t count = pk_read_u32(data);
    if (idx >= count) return;
    uint32_t last = count - 1;
    if (idx == last) {
        pk_remove_at(data, data_len, last);
        return;
    }

    char *last_p = pk_elem_ptr(data, *data_len, last);
    char *idx_p  = pk_elem_ptr(data, *data_len, idx);
    if (!last_p || !idx_p) return;
    uint32_t last_len = pk_read_u32(last_p);
    uint32_t idx_len  = pk_read_u32(idx_p);

    /* Save last element data */
    char saved[4096];
    if (last_len > sizeof(saved)) return;
    memcpy(saved, last_p + 4, last_len);

    /* Truncate last element */
    *data_len -= (4 + last_len);
    pk_write_u32(data, count - 1);

    /* Shift trailing data to fill/resize gap at idx */
    uint32_t idx_off = (uint32_t)(idx_p - data);
    uint32_t move_start = idx_off + 4 + idx_len;
    uint32_t move_len = *data_len - move_start;

    if (last_len != idx_len) {
        uint32_t new_start = idx_off + 4 + last_len;
        memmove(data + new_start, data + move_start, move_len);
    }

    pk_write_u32(data + idx_off, last_len);
    memcpy(data + idx_off + 4, saved, last_len);
    *data_len = idx_off + 4 + last_len + move_len;
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
        set_cleanup_entry(e);
        dict_delete(&g_dict, key, (uint32_t)strlen(key), hash);
        return 1;
    }
    return 0;
}

/* =====================================================================
 * Build empty packed set
 * ===================================================================== */

static void set_build_empty(char *out_data) {
    out_data[0] = (char)KV_TYPE_SET;
    out_data[1] = (char)KV_ENCODING_PACKED;
    pk_write_u32(out_data + 2, 0);
}

#define SET_EMPTY_SIZE 6  /* type:1 + encoding:1 + count:4 */

/* =====================================================================
 * Get set cardinality
 * ===================================================================== */

static uint32_t set_card(kv_entry_t *e) {
    if (entry_encoding(e) == KV_ENCODING_PACKED)
        return pk_read_u32(e->data + 2);
    /* DICT encoding */
    kv_dict_t *d = (kv_dict_t *)(e->data + 2);
    return dict_count(d);
}

/* =====================================================================
 * Convert packed set to dict encoding
 * ===================================================================== */

#define SET_DICT_HEADER_SIZE 54  /* type:1 + encoding:1 + kv_dict_t:52 */

static kv_entry_t *set_convert_to_dict(kv_entry_t *e) {
    uint32_t old_count = pk_read_u32(e->data + 2);

    if (old_count == 0) {
        kv_entry_t *ne = dict_resize_entry(&g_dict, e, SET_DICT_HEADER_SIZE);
        if (!ne) return NULL;
        ne->data[0] = (char)KV_TYPE_SET;
        ne->data[1] = (char)KV_ENCODING_DICT;
        kv_dict_t *d = (kv_dict_t *)(ne->data + 2);
        dict_init(d);
        return ne;
    }

    /* Save all elements before resize */
    char **saved_elems = (char **)malloc((size_t)old_count * sizeof(char *));
    int   *saved_lens  = (int *)malloc((size_t)old_count * sizeof(int));
    if (!saved_elems || !saved_lens) {
        free(saved_elems); free(saved_lens);
        return NULL;
    }

    const char *p = e->data + 6;
    for (uint32_t i = 0; i < old_count; i++) {
        uint32_t elen = pk_read_u32(p);
        saved_lens[i]  = (int)elen;
        saved_elems[i] = (char *)malloc((size_t)elen);
        if (saved_elems[i]) memcpy(saved_elems[i], p + 4, elen);
        p += 4 + elen;
    }

    kv_entry_t *ne = dict_resize_entry(&g_dict, e, SET_DICT_HEADER_SIZE);
    if (!ne) goto err;
    e = ne;

    e->data[0] = (char)KV_TYPE_SET;
    e->data[1] = (char)KV_ENCODING_DICT;
    kv_dict_t *d = (kv_dict_t *)(e->data + 2);
    dict_init(d);

    for (uint32_t i = 0; i < old_count; i++) {
        uint64_t h = kv_hash((const uint8_t *)saved_elems[i],
                             (uint32_t)saved_lens[i]);
        /* value is a 1-byte placeholder */
        char placeholder[3] = {
            (char)KV_TYPE_STRING, (char)KV_ENCODING_RAW, 0
        };
        dict_add_raw(d, saved_elems[i], (uint32_t)saved_lens[i],
                     KV_TYPE_STRING, KV_ENCODING_RAW,
                     &placeholder[2], 1, h);
    }

    for (uint32_t i = 0; i < old_count; i++) free(saved_elems[i]);
    free(saved_elems);
    free(saved_lens);
    return e;

err:
    for (uint32_t i = 0; i < old_count; i++) free(saved_elems[i]);
    free(saved_elems);
    free(saved_lens);
    return NULL;
}

/* =====================================================================
 * Convert dict set to packed encoding (shrink)
 * ===================================================================== */

static kv_entry_t *set_contract_to_packed(kv_entry_t *e) {
    kv_dict_t *d = (kv_dict_t *)(e->data + 2);
    uint32_t count = dict_count(d);

    /* Compute total packed size */
    uint32_t pk_size = 6;  /* type + encoding + count */
    kv_iter_t *it = dict_iter_new(d, 1);
    if (!it) return e;
    kv_entry_t *de;
    while ((de = dict_iter_next(it)) != NULL)
        pk_size += 4 + de->key_len;
    dict_iter_free(it);

    /* Save all element data before resize */
    char **elems = (char **)malloc((size_t)count * sizeof(char *));
    uint32_t *lens = (uint32_t *)malloc((size_t)count * sizeof(uint32_t));
    if (!elems || !lens) { free(elems); free(lens); return e; }

    it = dict_iter_new(d, 1);
    uint32_t idx = 0;
    while ((de = dict_iter_next(it)) != NULL && idx < count) {
        const char *k = entry_key(de);
        elems[idx] = (char *)malloc(de->key_len);
        if (elems[idx]) memcpy(elems[idx], k, de->key_len);
        lens[idx] = de->key_len;
        idx++;
    }
    dict_iter_free(it);

    /* Clean up nested dict, then resize */
    dict_clear(d);
    dict_destroy(d);

    kv_entry_t *ne = dict_resize_entry(&g_dict, e, pk_size);
    if (!ne) {
        /* Can't shrink — reinit dict and restore */
        dict_init(d);
        for (uint32_t i = 0; i < count; i++) {
            uint64_t h = kv_hash((const uint8_t *)elems[i], lens[i]);
            char placeholder[3] = {(char)KV_TYPE_STRING, (char)KV_ENCODING_RAW, 0};
            dict_add_raw(d, elems[i], lens[i], KV_TYPE_STRING, KV_ENCODING_RAW,
                         &placeholder[2], 1, h);
        }
        goto out;
    }
    e = ne;

    e->data[0] = (char)KV_TYPE_SET;
    e->data[1] = (char)KV_ENCODING_PACKED;
    pk_write_u32(e->data + 2, count);

    uint32_t off = 6;
    for (uint32_t i = 0; i < count; i++) {
        pk_write_u32(e->data + off, lens[i]);
        memcpy(e->data + off + 4, elems[i], lens[i]);
        off += 4 + lens[i];
    }
    e->val_len = off;

out:
    for (uint32_t i = 0; i < count; i++) free(elems[i]);
    free(elems);
    free(lens);
    return e;
}

/* =====================================================================
 * DICT encoding: check and maybe shrink
 * ===================================================================== */

static kv_entry_t *set_maybe_shrink(kv_entry_t *e) {
    if (entry_encoding(e) != KV_ENCODING_DICT) return e;
    kv_dict_t *d = (kv_dict_t *)(e->data + 2);
    if (dict_count(d) < SET_SHRINK_THRESHOLD)
        return set_contract_to_packed(e);
    return e;
}

/* =====================================================================
 * Cleanup
 * ===================================================================== */

void set_cleanup_entry(kv_entry_t *e) {
    if (!e) return;
    if (entry_type(e) != KV_TYPE_SET) return;
    if (entry_encoding(e) != KV_ENCODING_DICT) return;

    kv_dict_t *d = (kv_dict_t *)(e->data + 2);
    dict_clear(d);
    dict_destroy(d);
}

/* =====================================================================
 * SADD implementation
 * ===================================================================== */

static int set_add(kv_entry_t **pe, const char *key,
                   const char *elem, int elem_len, int *added) {
    kv_entry_t *e = *pe;
    *added = 0;

    if (entry_encoding(e) == KV_ENCODING_PACKED) {
        uint32_t pk_len = e->val_len - 2;
        uint32_t cur_count = pk_read_u32(e->data + 2);

        /* Check if already exists */
        if (pk_find(e->data + 2, pk_len, elem, (uint32_t)elem_len) >= 0)
            return 0;

        uint32_t need = (uint32_t)(4 + elem_len);

        /* Check if should convert to dict */
        if (cur_count + 1 > SET_PACKED_MAX_ELEMS ||
            pk_len + need > SET_PACKED_MAX_BYTES) {
            e = set_convert_to_dict(e);
            if (!e) return -1;
            *pe = e;
            goto do_dict;
        }

        /* Resize entry */
        uint32_t new_dlen = (uint32_t)(2 + pk_len + need);
        kv_entry_t *ne = dict_resize_entry(&g_dict, e, new_dlen);
        if (!ne) return -1;
        e = ne;
        *pe = e;

        pk_append(e->data + 2, &pk_len, elem, elem_len);
        e->val_len = pk_len + 2;
        *added = 1;
        return 0;
    }

do_dict:
    {
        kv_dict_t *d = (kv_dict_t *)(e->data + 2);
        uint64_t h = kv_hash((const uint8_t *)elem, (uint32_t)elem_len);
        if (dict_find(d, elem, (uint32_t)elem_len, h))
            return 0;

        char placeholder[3] = {(char)KV_TYPE_STRING, (char)KV_ENCODING_RAW, 0};
        if (!dict_add_raw(d, elem, (uint32_t)elem_len,
                          KV_TYPE_STRING, KV_ENCODING_RAW,
                          &placeholder[2], 1, h))
            return -1;
        *added = 1;
    }
    return 0;
}

/* =====================================================================
 * SREM implementation
 * ===================================================================== */

static int set_remove(kv_entry_t **pe, const char *key,
                      const char *elem, int elem_len, int *removed) {
    kv_entry_t *e = *pe;
    *removed = 0;

    if (entry_encoding(e) == KV_ENCODING_PACKED) {
        uint32_t pk_len = e->val_len - 2;
        int idx = pk_find(e->data + 2, pk_len, elem, (uint32_t)elem_len);
        if (idx < 0) return 0;

        pk_remove_at(e->data + 2, &pk_len, (uint32_t)idx);

        uint32_t new_dlen = pk_len + 2;
        kv_entry_t *ne = dict_resize_entry(&g_dict, e, new_dlen);
        if (ne) { ne->val_len = new_dlen; *pe = ne; }
        else    { e->val_len = new_dlen; }
        *removed = 1;
        return 0;
    }

    {
        kv_dict_t *d = (kv_dict_t *)(e->data + 2);
        uint64_t h = kv_hash((const uint8_t *)elem, (uint32_t)elem_len);
        if (dict_delete(d, elem, (uint32_t)elem_len, h) == 0) {
            *removed = 1;
            *pe = set_maybe_shrink(e);
        }
    }
    return 0;
}

/* =====================================================================
 * SISMEMBER implementation
 * ===================================================================== */

static int set_ismember(kv_entry_t *e, const char *elem, int elem_len) {
    if (entry_encoding(e) == KV_ENCODING_PACKED) {
        uint32_t pk_len = e->val_len - 2;
        return pk_find(e->data + 2, pk_len, elem, (uint32_t)elem_len) >= 0;
    }

    kv_dict_t *d = (kv_dict_t *)(e->data + 2);
    uint64_t h = kv_hash((const uint8_t *)elem, (uint32_t)elem_len);
    return dict_find(d, elem, (uint32_t)elem_len, h) != NULL;
}

/* =====================================================================
 * SMEMBERS / SRANDMEMBER / SPOP response helpers
 * ===================================================================== */

static void set_send_all_elements(kv_entry_t *e, DynBuf *wb) {
    uint32_t count = set_card(e);
    char hdr[32];
    int hl = snprintf(hdr, sizeof(hdr), "*%u\r\n", count);
    dynbuf_cat(wb, hdr, hl);

    if (entry_encoding(e) == KV_ENCODING_PACKED) {
        uint32_t pk_len = e->val_len - 2;
        const char *p = e->data + 6;  /* points to first elem_len */
        for (uint32_t i = 0; i < count; i++) {
            uint32_t elen = pk_read_u32(p);
            p += 4;
            char bhdr[32];
            int bhl = snprintf(bhdr, sizeof(bhdr), "$%u\r\n", elen);
            dynbuf_cat(wb, bhdr, bhl);
            dynbuf_cat(wb, p, elen);
            dynbuf_cat(wb, "\r\n", 2);
            p += elen;
        }
        return;
    }

    kv_dict_t *d = (kv_dict_t *)(e->data + 2);
    kv_iter_t *it = dict_iter_new(d, 1);
    if (!it) return;
    kv_entry_t *de;
    char temp[32];
    while ((de = dict_iter_next(it)) != NULL) {
        const char *k = entry_key(de);
        int hl = snprintf(temp, sizeof(temp), "$%u\r\n", de->key_len);
        dynbuf_cat(wb, temp, hl);
        dynbuf_cat(wb, k, (int)de->key_len);
        dynbuf_cat(wb, "\r\n", 2);
    }
    dict_iter_free(it);
}

/* Send one random element */
static void set_send_random_one(kv_entry_t *e, DynBuf *wb) {
    uint32_t count = set_card(e);
    if (count == 0) {
        dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
        return;
    }
    uint32_t ridx = (uint32_t)(rand() % count);

    if (entry_encoding(e) == KV_ENCODING_PACKED) {
        uint32_t pk_len = e->val_len - 2;
        char *p = pk_elem_ptr(e->data + 2, pk_len, ridx);
        if (!p) { dynbuf_cat(wb, REP_NIL, REP_NIL_LEN); return; }
        uint32_t elen = pk_read_u32(p);
        char hdr[32];
        int hl = snprintf(hdr, sizeof(hdr), "$%u\r\n", elen);
        dynbuf_cat(wb, hdr, hl);
        dynbuf_cat(wb, p + 4, elen);
        dynbuf_cat(wb, "\r\n", 2);
    } else {
        kv_dict_t *d = (kv_dict_t *)(e->data + 2);
        kv_iter_t *it = dict_iter_new(d, 1);
        if (!it) { dynbuf_cat(wb, REP_NIL, REP_NIL_LEN); return; }
        kv_entry_t *de;
        uint32_t i = 0;
        while ((de = dict_iter_next(it)) != NULL && i < ridx) i++;
        if (!de) { dict_iter_free(it); dynbuf_cat(wb, REP_NIL, REP_NIL_LEN); return; }
        char hdr[32];
        int hl = snprintf(hdr, sizeof(hdr), "$%u\r\n", de->key_len);
        dynbuf_cat(wb, hdr, hl);
        dynbuf_cat(wb, entry_key(de), (int)de->key_len);
        dynbuf_cat(wb, "\r\n", 2);
        dict_iter_free(it);
    }
}

/* =====================================================================
 * Flatten DICT-encoded set to PACKED (for serialization)
 * ===================================================================== */

int set_flatten_to_packed(kv_entry_t *e, char **out_data, uint32_t *out_len) {
    *out_data = NULL;
    *out_len  = 0;

    if (!e || entry_type(e) != KV_TYPE_SET ||
        entry_encoding(e) != KV_ENCODING_DICT)
        return -1;

    kv_dict_t *d = (kv_dict_t *)(e->data + 2);
    uint32_t count = dict_count(d);

    uint32_t buf_size = 6; /* type + encoding + count */
    kv_iter_t *it = dict_iter_new(d, 1);
    if (!it) return -1;
    kv_entry_t *de;
    while ((de = dict_iter_next(it)) != NULL)
        buf_size += 4 + de->key_len;
    dict_iter_free(it);

    char *buf = (char *)malloc(buf_size);
    if (!buf) return -1;

    buf[0] = (char)KV_TYPE_SET;
    buf[1] = (char)KV_ENCODING_PACKED;
    pk_write_u32(buf + 2, count);

    uint32_t off = 6;
    it = dict_iter_new(d, 1);
    while ((de = dict_iter_next(it)) != NULL) {
        pk_write_u32(buf + off, de->key_len);
        memcpy(buf + off + 4, entry_key(de), de->key_len);
        off += 4 + de->key_len;
    }
    dict_iter_free(it);

    *out_data = buf;
    *out_len  = buf_size;
    return 0;
}

/* =====================================================================
 * Rebuild encoding after RDB load
 * ===================================================================== */

void set_rebuild_encoding(kv_entry_t *e) {
    if (!e || entry_type(e) != KV_TYPE_SET) return;
    if (entry_encoding(e) != KV_ENCODING_PACKED) return;

    uint32_t count = pk_read_u32(e->data + 2);
    if (count > SET_PACKED_MAX_ELEMS) {
        kv_entry_t *ne = set_convert_to_dict(e);
        /* Update the entry in the main dict — caller must do this.
         * Since we get e from an iterator, we can use dict_resize_entry
         * which fixes up bucket pointers. set_convert_to_dict already
         * uses dict_resize_entry internally. The caller just needs the
         * returned pointer, which we can't propagate here. So we use
         * a different approach: convert in-place via temporary.
         * Actually set_convert_to_dict uses dict_resize_entry which
         * handles bucket pointer fixup. The result is the entry is
         * now DICT encoded in the same bucket chain. So it's fine. */
        (void)ne;  /* entry already updated in-place via dict_resize_entry */
    }
}

/* =====================================================================
 * SET command dispatch
 * ===================================================================== */

dispatch_result_t set_cmd(kvs_token_t cmd, kv_entry_t *entry,
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

    /* ── SADD ── */
    case 'S':
        if (TOKEN_EQ(cmd, "SADD")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }

            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            kv_entry_t *e = dict_find(&g_dict, key, (uint32_t)strlen(key), hash);
            int existed = 0;

            if (!e) {
                char data[16];
                set_build_empty(data);
                e = dict_add(&g_dict, key, (uint32_t)strlen(key), data, SET_EMPTY_SIZE, hash);
                if (!e) { dynbuf_cat(wb, "-ERR create set failed\r\n", 23); return DISPATCH_ERROR; }
            } else {
                if (entry_type(e) != KV_TYPE_SET) {
                    dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                    return DISPATCH_ERROR;
                }
                if (check_expiry(e, key, hash)) existed = 1;
            }

            if (existed) {
                char data[16];
                set_build_empty(data);
                e = dict_add(&g_dict, key, (uint32_t)strlen(key), data, SET_EMPTY_SIZE, hash);
                if (!e) { dynbuf_cat(wb, "-ERR create set failed\r\n", 23); return DISPATCH_ERROR; }
            }

            int total_added = 0;
            for (int i = 1; i < argc; i++) {
                int added = 0;
                if (set_add(&e, key, args[i].ptr, args[i].len, &added) < 0) {
                    dynbuf_cat(wb, "-ERR sadd failed\r\n", 17);
                    return DISPATCH_ERROR;
                }
                total_added += added;
            }

            int hl = snprintf(temp, sizeof(temp), ":%d\r\n", total_added);
            dynbuf_cat(wb, temp, hl);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "SREM")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_SET) {
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
                int removed = 0;
                if (set_remove(&entry, key, args[i].ptr, args[i].len, &removed) < 0) {
                    dynbuf_cat(wb, "-ERR srem failed\r\n", 17);
                    return DISPATCH_ERROR;
                }
                total_removed += removed;
            }

            int hl = snprintf(temp, sizeof(temp), ":%d\r\n", total_removed);
            dynbuf_cat(wb, temp, hl);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "SMEMBERS")) {
            if (!entry) { dynbuf_cat(wb, "*0\r\n", 4); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_SET) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, hash)) {
                dynbuf_cat(wb, "*0\r\n", 4);
                return DISPATCH_OK;
            }
            set_send_all_elements(entry, wb);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "SCARD")) {
            if (!entry) { dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_SET) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, hash)) {
                dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN);
                return DISPATCH_OK;
            }
            int hl = snprintf(temp, sizeof(temp), ":%u\r\n", set_card(entry));
            dynbuf_cat(wb, temp, hl);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "SISMEMBER")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_SET) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, hash)) {
                dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN);
                return DISPATCH_OK;
            }
            int ismem = set_ismember(entry, args[1].ptr, args[1].len);
            dynbuf_cat(wb, ismem ? REP_ONE : REP_ZERO,
                           ismem ? REP_ONE_LEN : REP_ZERO_LEN);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "SPOP")) {
            if (!entry) { dynbuf_cat(wb, REP_NIL, REP_NIL_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_SET) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, hash)) {
                dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                return DISPATCH_OK;
            }

            uint32_t count = set_card(entry);
            if (count == 0) { dynbuf_cat(wb, REP_NIL, REP_NIL_LEN); return DISPATCH_OK; }

            /* Pick random element */
            uint32_t ridx = (uint32_t)(rand() % count);

            /* Get the element first (before remove destroys it) */
            char elem_buf[65536];
            uint32_t elem_len = 0;
            int found = 0;

            if (entry_encoding(entry) == KV_ENCODING_PACKED) {
                uint32_t pk_len = entry->val_len - 2;
                char *p = pk_elem_ptr(entry->data + 2, pk_len, ridx);
                if (p) {
                    elem_len = pk_read_u32(p);
                    if (elem_len < sizeof(elem_buf)) {
                        memcpy(elem_buf, p + 4, elem_len);
                        found = 1;
                    }
                }
            } else {
                kv_dict_t *d = (kv_dict_t *)(entry->data + 2);
                kv_iter_t *it = dict_iter_new(d, 1);
                if (it) {
                    kv_entry_t *de;
                    uint32_t i = 0;
                    while ((de = dict_iter_next(it)) != NULL && i < ridx) i++;
                    if (de && de->key_len < sizeof(elem_buf)) {
                        memcpy(elem_buf, entry_key(de), de->key_len);
                        elem_len = de->key_len;
                        found = 1;
                    }
                    dict_iter_free(it);
                }
            }

            if (!found) { dynbuf_cat(wb, REP_NIL, REP_NIL_LEN); return DISPATCH_OK; }

            /* Remove it */
            int removed = 0;
            set_remove(&entry, key, elem_buf, (int)elem_len, &removed);

            /* Send response */
            char hdr[32];
            int hl = snprintf(hdr, sizeof(hdr), "$%u\r\n", elem_len);
            dynbuf_cat(wb, hdr, hl);
            dynbuf_cat(wb, elem_buf, (int)elem_len);
            dynbuf_cat(wb, "\r\n", 2);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "SRANDMEMBER")) {
            if (!entry) { dynbuf_cat(wb, REP_NIL, REP_NIL_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_SET) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, hash)) {
                dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                return DISPATCH_OK;
            }
            set_send_random_one(entry, wb);
            return DISPATCH_OK;
        }
        break; /* 'S' */

    /* ── DEL ── */
    case 'D':
        if (TOKEN_EQ(cmd, "DEL")) {
            if (!entry) { dynbuf_cat(wb, REP_NIL, REP_NIL_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_SET) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            {
                uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
                set_cleanup_entry(entry);
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
            if (entry_type(entry) != KV_TYPE_SET) {
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
