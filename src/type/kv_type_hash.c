#include "kv_type_hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <inttypes.h>
#include <math.h>

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
S(REP_ERR_INT,   "-ERR value is not an integer or out of range\r\n");

#undef S

/* =====================================================================
 * Limits
 * ===================================================================== */

#define HASH_PACKED_MAX_FIELDS  128
#define HASH_PACKED_MAX_BYTES  4096
#define HASH_SHRINK_THRESHOLD   64

/* =====================================================================
 * Packed encoding helpers
 *
 * Format: [count:4][field1_len:4][field1_bytes...][val1_enc:1][val1_len:4][val1_bytes...]...
 *
 * value encoding byte (before the 4-byte len):
 *   0 = RAW string   (val_len = byte count of string)
 *   1 = INT inline   (val_len = 8, value is int64_t stored inline, big-endian-like)
 *
 * For backward compatibility with data that lacks the enc byte,
 * we detect old format by checking: every field starts at known offsets,
 * and the 4-byte val_len never exceeds remaining data.
 * ===================================================================== */

static inline uint32_t pk_read_u32(const char *p) {
    uint32_t v;
    memcpy(&v, p, 4);
    return v;
}

static inline void pk_write_u32(char *p, uint32_t v) {
    memcpy(p, &v, 4);
}

static inline int64_t pk_read_i64(const char *p) {
    int64_t v;
    memcpy(&v, p, 8);
    return v;
}

static inline void pk_write_i64(char *p, int64_t v) {
    memcpy(p, &v, 8);
}

static inline uint8_t pk_val_enc(const char *p) {
    return (uint8_t)p[0];
}

static inline uint32_t pk_raw_vlen(const char *p) {
    return pk_read_u32(p + 1);
}

/* Returns pointer past the value bytes, or NULL on corruption */
static inline const char *pk_skip_value(const char *p, const char *end) {
    uint8_t enc = (uint8_t)p[0];
    if (enc == 1) {
        return p + 1 + 8;  /* enc + 8 bytes int64 */
    }
    uint32_t vlen = pk_read_u32(p + 1);
    if (p + 1 + 4 + vlen > end) return NULL;
    return p + 1 + 4 + vlen;
}

static inline uint32_t pk_value_bytes(const char *p) {
    if ((uint8_t)p[0] == 1) return 1 + 8;
    return 1 + 4 + pk_read_u32(p + 1);
}

static inline uint32_t pk_count(const char *data) {
    return pk_read_u32(data);
}

/* Find a field, returns pointer to its field_len field or NULL */
static char *pk_find_field(char *data, uint32_t data_len,
                           const char *field, uint32_t field_len) {
    uint32_t count = pk_read_u32(data);
    char *p = data + 4;
    const char *end = data + data_len;
    for (uint32_t i = 0; i < count; i++) {
        if (p + 4 > end) return NULL;
        uint32_t flen = pk_read_u32(p);
        if (flen == field_len && p + 4 + flen <= end &&
            memcmp(p + 4, field, flen) == 0)
            return p;
        /* Skip field name + value */
        p += 4 + flen;
        if (p >= end) return NULL;
        uint32_t vbytes = pk_value_bytes(p);
        p += vbytes;
        if (p > end) return NULL;
    }
    return NULL;
}

/* Append field-value pair at end */
static int pk_append_field(char *data, uint32_t *data_len,
                           const char *field, int field_len,
                           uint8_t venc, const char *value, int value_len) {
    uint32_t old_count = pk_read_u32(data);
    uint32_t need;
    if (venc == 1) {
        need = 4 + (uint32_t)field_len + 1 + 8;  /* flen + field + enc + i64 */
    } else {
        need = 4 + (uint32_t)field_len + 1 + 4 + (uint32_t)value_len;
    }
    if (*data_len + need > HASH_PACKED_MAX_BYTES + 6) return -1;

    char *p = data + *data_len;
    pk_write_u32(p, (uint32_t)field_len);
    memcpy(p + 4, field, (size_t)field_len);
    p += 4 + field_len;
    p[0] = (char)venc;
    if (venc == 1) {
        pk_write_i64(p + 1, *(int64_t *)value);
    } else {
        pk_write_u32(p + 1, (uint32_t)value_len);
        memcpy(p + 1 + 4, value, (size_t)value_len);
    }
    *data_len += need;
    pk_write_u32(data, old_count + 1);
    return 0;
}

/* Remove field at pointer p (pointing to field_len). */
static void pk_remove_field(char *data, uint32_t *data_len, char *p) {
    uint32_t count = pk_read_u32(data);
    uint32_t flen = pk_read_u32(p);
    uint32_t vbytes = pk_value_bytes(p + 4 + flen);
    uint32_t consumed = 4 + flen + vbytes;
    uint32_t offset = (uint32_t)(p - data);
    uint32_t remaining = *data_len - offset - consumed;

    if (remaining > 0)
        memmove(p, p + consumed, remaining);
    *data_len -= consumed;
    pk_write_u32(data, count - 1);
}

/* Update value of field at pointer p. Returns 0 on success, -1 if resize needed. */
static int pk_update_value(kv_entry_t **pe, char *data, uint32_t *data_len,
                           char *p, uint8_t venc, const char *value, int value_len) {
    uint32_t flen = pk_read_u32(p);
    char *vp = p + 4 + flen;
    uint8_t old_enc = pk_val_enc(vp);
    uint32_t old_vbytes = pk_value_bytes(vp);

    uint32_t new_vbytes;
    if (venc == 1) {
        new_vbytes = 1 + 8;
    } else {
        new_vbytes = 1 + 4 + (uint32_t)value_len;
    }

    if (new_vbytes == old_vbytes && venc == old_enc) {
        /* In-place update */
        if (venc == 1) {
            pk_write_i64(vp + 1, *(int64_t *)value);
        } else {
            pk_write_u32(vp + 1, (uint32_t)value_len);
            memcpy(vp + 1 + 4, value, (size_t)value_len);
        }
        return 0;
    }

    /* Size/encoding changed — relocation order is critical because
       dict_resize_entry slides the key suffix to maintain
       e->data[val_len..val_len+key_len] invariant:
         shrink: resize moves key LEFT, overwriting the last |delta| bytes
                 of the old packed tail → we must memmove FIRST, resize after.
         grow:   the new bytes [old_val_len, new_val_len) are uninitialized,
                 so resize FIRST to gain space, then memmove the tail right. */
    kv_entry_t *e           = *pe;
    int32_t  delta          = (int32_t)new_vbytes - (int32_t)old_vbytes;
    uint32_t old_packed_len = *data_len;
    uint32_t move_start_off = (uint32_t)((p + 4 + flen + old_vbytes) - (e->data + 2));
    uint32_t move_len       = (old_packed_len > move_start_off)
                              ? (old_packed_len - move_start_off) : 0;

    if (delta < 0) {
        /* Shrink path: memmove FIRST (the source includes bytes that will
           later be overwritten by key relocation inside dict_resize_entry),
           then write new value in place, then call dict_resize_entry to
           contract val_len and slide the key suffix to its new home. */
        if (move_len > 0)
            memmove(e->data + 2 + move_start_off + delta,
                    e->data + 2 + move_start_off, move_len);

        vp = (e->data + 2) + (uint32_t)(p - (e->data + 2)) + 4 + flen;
        vp[0] = (char)venc;
        if (venc == 1) {
            pk_write_i64(vp + 1, *(int64_t *)value);
        } else {
            pk_write_u32(vp + 1, (uint32_t)value_len);
            memcpy(vp + 1 + 4, value, (size_t)value_len);
        }

        kv_entry_t *ne = dict_resize_entry(&g_dict, *pe,
                                           (*pe)->val_len + delta);
        if (!ne) return -1;
        *pe = ne;
        *data_len = ne->val_len - 2;
        return 0;
    }

    /* Grow path: must remember the field name across realloc; pk_find_field
       again afterwards because the buffer may have moved. */
    char *saved_field = (char *)malloc((size_t)flen);
    if (!saved_field) return -1;
    memcpy(saved_field, p + 4, flen);

    kv_entry_t *ne = dict_resize_entry(&g_dict, *pe,
                                       (*pe)->val_len + delta);
    if (!ne) { free(saved_field); return -1; }
    *pe = ne;

    data = ne->data + 2;
    *data_len = ne->val_len - 2;
    p = pk_find_field(data, *data_len, saved_field, flen);
    free(saved_field);
    if (!p) return -1;
    vp = p + 4 + flen;

    if (move_len > 0)
        memmove(data + move_start_off + delta,
                data + move_start_off, move_len);

    vp[0] = (char)venc;
    if (venc == 1) {
        pk_write_i64(vp + 1, *(int64_t *)value);
    } else {
        pk_write_u32(vp + 1, (uint32_t)value_len);
        memcpy(vp + 1 + 4, value, (size_t)value_len);
    }
    return 0;
}

/* Get value pointer and length for field at p.
 *
 * NOTE: enc_out is normalized to engine-level constants
 * (KV_ENCODING_RAW / KV_ENCODING_INT) so callers can compare directly
 * against KV_ENCODING_INT without having to know the on-disk byte
 * representation (0/1) used inside packed hash payload. */
static const char *pk_get_value(char *vp, int *vlen_out, uint8_t *enc_out) {
    uint8_t enc = pk_val_enc(vp);  /* raw on-disk byte: 0 = RAW, 1 = INT */
    if (enc == 1) {
        if (enc_out) *enc_out = KV_ENCODING_INT;
        if (vlen_out) *vlen_out = 8;
        return vp + 1;
    }
    if (enc_out) *enc_out = KV_ENCODING_RAW;
    uint32_t vlen = pk_read_u32(vp + 1);
    if (vlen_out) *vlen_out = (int)vlen;
    return vp + 1 + 4;
}

/* Parse token as int64 safely. Returns 1 on success. */
static int parse_int64(const char *p, int len, int64_t *out) {
    if (len <= 0 || len > 20) return 0;
    const char *end = p + len;
    int negative = 0;
    if (*p == '-') {
        if (len == 1) return 0;
        negative = 1;
        p++;
    }
    if (p >= end) return 0;
    if (*p == '0' && (end - p) > 1) return 0;
    int64_t v = 0;
    for (; p < end; p++) {
        if (*p < '0' || *p > '9') return 0;
        int64_t nv = v * 10 + (*p - '0');
        if (nv < v) return 0;  /* overflow */
        v = nv;
    }
    *out = negative ? -v : v;
    return 1;
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
        hash_cleanup_entry(e);
        dict_delete(&g_dict, key, (uint32_t)strlen(key), hash);
        return 1;
    }
    return 0;
}

/* =====================================================================
 * Build empty packed hash
 * ===================================================================== */

static void hash_build_empty(char *out_data) {
    out_data[0] = (char)KV_TYPE_HASH;
    out_data[1] = (char)KV_ENCODING_PACKED;
    pk_write_u32(out_data + 2, 0);
}

#define HASH_EMPTY_SIZE 6  /* type:1 + encoding:1 + count:4 */

/* =====================================================================
 * Get hash field count
 * ===================================================================== */

static uint32_t hash_len(kv_entry_t *e) {
    if (entry_encoding(e) == KV_ENCODING_PACKED)
        return pk_read_u32(e->data + 2);
    kv_dict_t *d = (kv_dict_t *)(e->data + 2);
    return dict_count(d);
}

/* =====================================================================
 * Convert packed hash to dict encoding
 * ===================================================================== */

#define HASH_DICT_HEADER_SIZE (2 + (uint32_t)sizeof(kv_dict_t))  /* type:1 + encoding:1 + kv_dict_t */

static kv_entry_t *hash_convert_to_dict(kv_entry_t *e) {
    uint32_t old_count = pk_read_u32(e->data + 2);

    if (old_count == 0) {
        kv_entry_t *ne = dict_resize_entry(&g_dict, e, HASH_DICT_HEADER_SIZE);
        if (!ne) return NULL;
        ne->data[0] = (char)KV_TYPE_HASH;
        ne->data[1] = (char)KV_ENCODING_DICT;
        kv_dict_t *d = (kv_dict_t *)(ne->data + 2);
        dict_init(d);
        return ne;
    }

    typedef struct { char *field; char *value; uint32_t flen; uint32_t vlen; uint8_t enc; } fv_t;
    fv_t *pairs = (fv_t *)malloc((size_t)old_count * sizeof(fv_t));
    if (!pairs) return NULL;
    memset(pairs, 0, (size_t)old_count * sizeof(fv_t));

    const char *p = e->data + 6;
    const char *end = e->data + e->val_len;
    for (uint32_t i = 0; i < old_count; i++) {
        uint32_t flen = pk_read_u32(p);
        pairs[i].flen = flen;
        pairs[i].field = (char *)malloc(flen);
        if (pairs[i].field) memcpy(pairs[i].field, p + 4, flen);
        p += 4 + flen;
        uint8_t enc = pk_val_enc(p);
        pairs[i].enc = enc;
        int vlen;
        const char *v = pk_get_value((char *)p, &vlen, NULL);
        pairs[i].vlen = (uint32_t)vlen;
        pairs[i].value = (char *)malloc((size_t)vlen);
        if (pairs[i].value) memcpy(pairs[i].value, v, (size_t)vlen);
        p += pk_value_bytes((char *)p);
        if (p > end) break;
    }

    kv_entry_t *ne = dict_resize_entry(&g_dict, e, HASH_DICT_HEADER_SIZE);
    if (!ne) goto err;
    e = ne;

    e->data[0] = (char)KV_TYPE_HASH;
    e->data[1] = (char)KV_ENCODING_DICT;
    kv_dict_t *d = (kv_dict_t *)(e->data + 2);
    dict_init(d);

    for (uint32_t i = 0; i < old_count; i++) {
        uint64_t h = kv_hash((const uint8_t *)pairs[i].field, pairs[i].flen);
        uint8_t sub_enc = (pairs[i].enc == 1) ? KV_ENCODING_INT : KV_ENCODING_RAW;
        dict_add_raw(d, pairs[i].field, pairs[i].flen,
                     KV_TYPE_STRING, sub_enc,
                     pairs[i].value, pairs[i].vlen, h);
    }

    for (uint32_t i = 0; i < old_count; i++) {
        free(pairs[i].field);
        free(pairs[i].value);
    }
    free(pairs);
    return e;

err:
    for (uint32_t i = 0; i < old_count; i++) {
        free(pairs[i].field);
        free(pairs[i].value);
    }
    free(pairs);
    return NULL;
}

/* =====================================================================
 * Convert dict hash to packed encoding (shrink)
 * ===================================================================== */

static kv_entry_t *hash_contract_to_packed(kv_entry_t *e) {
    kv_dict_t *d = (kv_dict_t *)(e->data + 2);
    uint32_t count = dict_count(d);

    uint32_t pk_size = 6;
    kv_iter_t *it = dict_iter_new(d, 1);
    if (!it) return e;
    kv_entry_t *de;
    while ((de = dict_iter_next(it)) != NULL) {
        if (entry_encoding(de) == KV_ENCODING_INT)
            pk_size += 4 + de->key_len + 1 + 8;
        else
            pk_size += 4 + de->key_len + 1 + 4 + entry_payload_len(de);
    }
    dict_iter_free(it);

    typedef struct { char *f; char *v; uint32_t fl; uint32_t vl; uint8_t enc; } fv_t;
    fv_t *pairs = (fv_t *)malloc((size_t)count * sizeof(fv_t));
    if (!pairs) return e;
    memset(pairs, 0, (size_t)count * sizeof(fv_t));

    it = dict_iter_new(d, 1);
    uint32_t idx = 0;
    while ((de = dict_iter_next(it)) != NULL && idx < count) {
        const char *k = entry_key(de);
        pairs[idx].fl = de->key_len;
        pairs[idx].f = (char *)malloc(de->key_len);
        if (pairs[idx].f) memcpy(pairs[idx].f, k, de->key_len);
        pairs[idx].enc = entry_encoding(de);
        pairs[idx].vl = entry_payload_len(de);
        pairs[idx].v = (char *)malloc(pairs[idx].vl);
        if (pairs[idx].v) memcpy(pairs[idx].v, entry_payload(de), pairs[idx].vl);
        idx++;
    }
    dict_iter_free(it);

    dict_clear(d);
    dict_destroy(d);

    kv_entry_t *ne = dict_resize_entry(&g_dict, e, pk_size);
    if (!ne) {
        dict_init(d);
        for (uint32_t i = 0; i < count; i++) {
            uint64_t h = kv_hash((const uint8_t *)pairs[i].f, pairs[i].fl);
            dict_add_raw(d, pairs[i].f, pairs[i].fl,
                         KV_TYPE_STRING, pairs[i].enc,
                         pairs[i].v, pairs[i].vl, h);
        }
        goto out;
    }
    e = ne;

    e->data[0] = (char)KV_TYPE_HASH;
    e->data[1] = (char)KV_ENCODING_PACKED;
    pk_write_u32(e->data + 2, count);

    uint32_t off = 6;
    for (uint32_t i = 0; i < count; i++) {
        pk_write_u32(e->data + off, pairs[i].fl);
        memcpy(e->data + off + 4, pairs[i].f, pairs[i].fl);
        off += 4 + pairs[i].fl;
        if (pairs[i].enc == KV_ENCODING_INT) {
            e->data[off] = 1;  /* INT encoding */
            pk_write_i64(e->data + off + 1, *(int64_t *)pairs[i].v);
            off += 1 + 8;
        } else {
            e->data[off] = 0;  /* RAW encoding */
            pk_write_u32(e->data + off + 1, pairs[i].vl);
            memcpy(e->data + off + 1 + 4, pairs[i].v, pairs[i].vl);
            off += 1 + 4 + pairs[i].vl;
        }
    }
    e->val_len = off;

out:
    for (uint32_t i = 0; i < count; i++) { free(pairs[i].f); free(pairs[i].v); }
    free(pairs);
    return e;
}

static kv_entry_t *hash_maybe_shrink(kv_entry_t *e) {
    if (entry_encoding(e) != KV_ENCODING_DICT) return e;
    kv_dict_t *d = (kv_dict_t *)(e->data + 2);
    if (dict_count(d) < HASH_SHRINK_THRESHOLD)
        return hash_contract_to_packed(e);
    return e;
}

/* =====================================================================
 * Cleanup
 * ===================================================================== */

void hash_cleanup_entry(kv_entry_t *e) {
    if (!e) return;
    if (entry_type(e) != KV_TYPE_HASH) return;
    if (entry_encoding(e) != KV_ENCODING_DICT) return;

    kv_dict_t *d = (kv_dict_t *)(e->data + 2);
    dict_clear(d);
    dict_destroy(d);
}

/* =====================================================================
 * HSET / HSETNX implementation
 * ===================================================================== */

static int hash_set(kv_entry_t **pe, const char *field, int field_len,
                    const char *value, int value_len,
                    int *added, int nx_only) {
    kv_entry_t *e = *pe;
    *added = 0;

    /* Try to encode value as integer if it fits */
    int64_t iv;
    uint8_t venc = KV_ENCODING_RAW;
    const char *vptr = value;
    int vlen_stored = value_len;
    if (parse_int64(value, value_len, &iv)) {
        venc = 1;  /* INT encoding in packed format */
        vptr = (const char *)&iv;
        vlen_stored = 8;
    }

    if (entry_encoding(e) == KV_ENCODING_PACKED) {
        uint32_t pk_len = e->val_len - 2;
        uint32_t cur_count = pk_read_u32(e->data + 2);

        char *fp = pk_find_field(e->data + 2, pk_len,
                                 field, (uint32_t)field_len);

        if (fp) {
            if (nx_only) return 0;  /* HSETNX: field exists, do nothing */
            return pk_update_value(pe, e->data + 2, &pk_len, fp,
                                   venc, vptr, vlen_stored);
        }

        /* New field */
        uint32_t need;
        if (venc == 1)
            need = 4 + (uint32_t)field_len + 1 + 8;
        else
            need = 4 + (uint32_t)field_len + 1 + 4 + (uint32_t)value_len;

        if (cur_count + 1 > HASH_PACKED_MAX_FIELDS ||
            pk_len + need > HASH_PACKED_MAX_BYTES) {
            e = hash_convert_to_dict(e);
            if (!e) return -1;
            *pe = e;
            goto do_dict;
        }

        uint32_t new_dlen = (uint32_t)(2 + pk_len + need);
        kv_entry_t *ne = dict_resize_entry(&g_dict, e, new_dlen);
        if (!ne) return -1;
        e = ne;
        *pe = e;

        pk_append_field(e->data + 2, &pk_len, field, field_len,
                        venc, vptr, vlen_stored);
        e->val_len = pk_len + 2;
        *added = 1;
        return 0;
    }

do_dict:
    {
        kv_dict_t *d = (kv_dict_t *)(e->data + 2);
        uint64_t h = kv_hash((const uint8_t *)field, (uint32_t)field_len);
        kv_entry_t *de = dict_find(d, field, (uint32_t)field_len, h);

        if (de) {
            if (nx_only) return 0;
            uint8_t sub_enc;
            const char *payload;
            uint32_t plen;
            if (venc == 1) {
                sub_enc = KV_ENCODING_INT;
                payload = vptr;
                plen = 8;
            } else {
                sub_enc = KV_ENCODING_RAW;
                payload = value;
                plen = (uint32_t)value_len;
            }
            uint32_t new_data_len = 2 + plen;
            de = dict_resize_entry(d, de, new_data_len);
            if (!de) return -1;
            de->data[0] = (char)KV_TYPE_STRING;
            de->data[1] = (char)sub_enc;
            memcpy(de->data + 2, payload, plen);
            return 0;
        }

        uint8_t sub_enc = (venc == 1) ? KV_ENCODING_INT : KV_ENCODING_RAW;
        uint32_t plen = (venc == 1) ? 8 : (uint32_t)value_len;
        const char *payload = (venc == 1) ? vptr : value;
        if (!dict_add_raw(d, field, (uint32_t)field_len,
                          KV_TYPE_STRING, sub_enc,
                          payload, plen, h))
            return -1;
        *added = 1;
    }
    return 0;
}

/* =====================================================================
 * HGET implementation (also used by HMGET)
 * Returns pointer to value bytes, sets vlen_out and enc_out.
 * ===================================================================== */

static const char *hash_get(kv_entry_t *e, const char *field, int field_len,
                            int *vlen_out, uint8_t *enc_out) {
    if (entry_encoding(e) == KV_ENCODING_PACKED) {
        uint32_t pk_len = e->val_len - 2;
        char *fp = pk_find_field(e->data + 2, pk_len, field, (uint32_t)field_len);
        if (!fp) return NULL;
        char *vp = fp + 4 + pk_read_u32(fp);
        return pk_get_value(vp, vlen_out, enc_out);
    }

    kv_dict_t *d = (kv_dict_t *)(e->data + 2);
    uint64_t h = kv_hash((const uint8_t *)field, (uint32_t)field_len);
    kv_entry_t *de = dict_find(d, field, (uint32_t)field_len, h);
    if (!de) return NULL;
    if (vlen_out) *vlen_out = (int)entry_payload_len(de);
    if (enc_out)  *enc_out  = entry_encoding(de);
    return entry_payload(de);
}

/* =====================================================================
 * HDEL implementation
 * ===================================================================== */

static int hash_del(kv_entry_t **pe, const char *field, int field_len,
                    int *removed) {
    kv_entry_t *e = *pe;
    *removed = 0;

    if (entry_encoding(e) == KV_ENCODING_PACKED) {
        uint32_t pk_len = e->val_len - 2;
        char *fp = pk_find_field(e->data + 2, pk_len, field, (uint32_t)field_len);
        if (!fp) return 0;

        pk_remove_field(e->data + 2, &pk_len, fp);

        uint32_t new_dlen = pk_len + 2;
        kv_entry_t *ne = dict_resize_entry(&g_dict, e, new_dlen);
        if (ne) { ne->val_len = new_dlen; *pe = ne; }
        else    { e->val_len = new_dlen; }
        *removed = 1;
        return 0;
    }

    {
        kv_dict_t *d = (kv_dict_t *)(e->data + 2);
        uint64_t h = kv_hash((const uint8_t *)field, (uint32_t)field_len);
        if (dict_delete(d, field, (uint32_t)field_len, h) == 0) {
            *removed = 1;
            *pe = hash_maybe_shrink(e);
        }
    }
    return 0;
}

/* =====================================================================
 * Flatten DICT-encoded hash to PACKED (for serialization)
 * ===================================================================== */

int hash_flatten_to_packed(kv_entry_t *e, char **out_data, uint32_t *out_len) {
    *out_data = NULL;
    *out_len  = 0;

    if (!e || entry_type(e) != KV_TYPE_HASH ||
        entry_encoding(e) != KV_ENCODING_DICT)
        return -1;

    kv_dict_t *d = (kv_dict_t *)(e->data + 2);
    uint32_t count = dict_count(d);

    uint32_t buf_size = 6;
    kv_iter_t *it = dict_iter_new(d, 1);
    if (!it) return -1;
    kv_entry_t *de;
    while ((de = dict_iter_next(it)) != NULL) {
        if (entry_encoding(de) == KV_ENCODING_INT)
            buf_size += 4 + de->key_len + 1 + 8;
        else
            buf_size += 4 + de->key_len + 1 + 4 + entry_payload_len(de);
    }
    dict_iter_free(it);

    char *buf = (char *)malloc(buf_size);
    if (!buf) return -1;

    buf[0] = (char)KV_TYPE_HASH;
    buf[1] = (char)KV_ENCODING_PACKED;
    pk_write_u32(buf + 2, count);

    uint32_t off = 6;
    it = dict_iter_new(d, 1);
    while ((de = dict_iter_next(it)) != NULL) {
        pk_write_u32(buf + off, de->key_len);
        memcpy(buf + off + 4, entry_key(de), de->key_len);
        off += 4 + de->key_len;
        if (entry_encoding(de) == KV_ENCODING_INT) {
            buf[off] = 1;
            pk_write_i64(buf + off + 1, *(int64_t *)entry_payload(de));
            off += 1 + 8;
        } else {
            uint32_t vlen = entry_payload_len(de);
            buf[off] = 0;
            pk_write_u32(buf + off + 1, vlen);
            memcpy(buf + off + 1 + 4, entry_payload(de), vlen);
            off += 1 + 4 + vlen;
        }
    }
    dict_iter_free(it);

    *out_data = buf;
    *out_len  = buf_size;
    return 0;
}

/* =====================================================================
 * Rebuild encoding after RDB load
 * ===================================================================== */

void hash_rebuild_encoding(kv_entry_t *e) {
    if (!e || entry_type(e) != KV_TYPE_HASH) return;
    if (entry_encoding(e) != KV_ENCODING_PACKED) return;

    uint32_t count = pk_read_u32(e->data + 2);
    if (count > HASH_PACKED_MAX_FIELDS) {
        (void)hash_convert_to_dict(e);
    }
}

/* =====================================================================
 * HINCRBY implementation
 *   - Parse existing value as int64, increment, store back.
 *   - If field does not exist, treat as 0 + increment.
 *   - Returns the new value as integer RESP.
 * ===================================================================== */

static int hash_incrby(kv_entry_t **pe, const char *field, int field_len,
                       int64_t delta, int64_t *out_val) {
    kv_entry_t *e = *pe;

    /* Check if field exists */
    int vlen;
    uint8_t enc;
    const char *val = hash_get(e, field, field_len, &vlen, &enc);

    int64_t cur = 0;
    if (val) {
        if (enc == KV_ENCODING_INT) {
            cur = *(int64_t *)val;
        } else if (enc == KV_ENCODING_RAW || enc == 0) {
            /* Try to parse string as integer */
            char tmp[32];
            int n = vlen < 31 ? vlen : 31;
            memcpy(tmp, val, (size_t)n);
            tmp[n] = '\0';
            char *endp;
            cur = (int64_t)strtoll(tmp, &endp, 10);
            if (*endp != '\0') return -2;  /* not an integer */
        } else {
            return -2;
        }
    }

    int64_t new_val = cur + delta;
    /* Check overflow */
    if ((delta > 0 && new_val < cur) || (delta < 0 && new_val > cur))
        return -2;

    /* Store the new value as an int64 in our packed format */
    char sbuf[32];
    int slen = snprintf(sbuf, sizeof(sbuf), "%" PRId64, new_val);
    int added;

    int rc = hash_set(pe, field, field_len, sbuf, slen, &added, 0);
    if (rc < 0) return -1;
    *out_val = new_val;
    return 0;
}

/* =====================================================================
 * Write a single hash field-value pair as RESP bulk strings
 * ===================================================================== */

static void write_field_value_resp(DynBuf *wb, const char *field,
                                   uint32_t flen, const char *val,
                                   uint32_t vlen, uint8_t enc) {
    char hdr[32];
    int hl;
    if (enc == KV_ENCODING_INT) {
        int64_t iv;
        memcpy(&iv, val, 8);
        char istr[24];
        int il = snprintf(istr, sizeof(istr), "%" PRId64, iv);
        hl = snprintf(hdr, sizeof(hdr), "$%u\r\n", flen);
        dynbuf_cat(wb, hdr, hl);
        dynbuf_cat(wb, field, (int)flen);
        dynbuf_cat(wb, "\r\n", 2);
        hl = snprintf(hdr, sizeof(hdr), "$%d\r\n", il);
        dynbuf_cat(wb, hdr, hl);
        dynbuf_cat(wb, istr, il);
        dynbuf_cat(wb, "\r\n", 2);
    } else {
        hl = snprintf(hdr, sizeof(hdr), "$%u\r\n", flen);
        dynbuf_cat(wb, hdr, hl);
        dynbuf_cat(wb, field, (int)flen);
        dynbuf_cat(wb, "\r\n", 2);
        hl = snprintf(hdr, sizeof(hdr), "$%u\r\n", vlen);
        dynbuf_cat(wb, hdr, hl);
        dynbuf_cat(wb, val, (int)vlen);
        dynbuf_cat(wb, "\r\n", 2);
    }
}

/* =====================================================================
 * Write a RESP bulk string for a HASH value (used by HMGET)
 * ===================================================================== */

static void write_value_resp(DynBuf *wb, const char *val,
                             uint32_t vlen, uint8_t enc) {
    char hdr[32];
    int hl;
    if (!val) {
        dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
    } else if (enc == KV_ENCODING_INT) {
        int64_t iv;
        memcpy(&iv, val, 8);
        char istr[24];
        int il = snprintf(istr, sizeof(istr), "%" PRId64, iv);
        hl = snprintf(hdr, sizeof(hdr), "$%d\r\n", il);
        dynbuf_cat(wb, hdr, hl);
        dynbuf_cat(wb, istr, il);
        dynbuf_cat(wb, "\r\n", 2);
    } else {
        hl = snprintf(hdr, sizeof(hdr), "$%u\r\n", vlen);
        dynbuf_cat(wb, hdr, hl);
        dynbuf_cat(wb, val, (int)vlen);
        dynbuf_cat(wb, "\r\n", 2);
    }
}

/* =====================================================================
 * Iterate and output all fields/values for HGETALL/HKEYS/HVALS
 * ===================================================================== */

static void write_all_fields(DynBuf *wb, kv_entry_t *entry, int keys_only,
                             int vals_only) {
    uint32_t count = hash_len(entry);
    char hdr[32];
    int hl;

    if (keys_only || vals_only) {
        hl = snprintf(hdr, sizeof(hdr), "*%u\r\n", count);
    } else {
        hl = snprintf(hdr, sizeof(hdr), "*%u\r\n", count * 2);
    }
    dynbuf_cat(wb, hdr, hl);

    char temp[64];  /* reused for headers */
    int temp_hl;

    if (entry_encoding(entry) == KV_ENCODING_PACKED) {
        uint32_t pk_len = entry->val_len - 2;
        const char *p = entry->data + 6;
        const char *end = entry->data + entry->val_len;
        for (uint32_t i = 0; i < count; i++) {
            if (p + 4 > end) break;
            uint32_t flen = pk_read_u32(p);
            p += 4;

            /* Emit the field name for HGETALL (both flags 0) and HKEYS,
             * but NOT for HVALS. The previous condition `!keys_only` was
             * wrong for HVALS (vals_only=1, keys_only=0), causing HVALS to
             * emit field+value pairs while the array header announced only
             * `count` items, truncating the response. */
            if (!vals_only) {
                temp_hl = snprintf(temp, sizeof(temp), "$%u\r\n", flen);
                dynbuf_cat(wb, temp, temp_hl);
                dynbuf_cat(wb, p, (int)flen);
                dynbuf_cat(wb, "\r\n", 2);
            }
            p += flen;
            if (p >= end) break;

            uint8_t enc = pk_val_enc((char *)p);
            int vlen;
            const char *val = pk_get_value((char *)p, &vlen, NULL);
            p += pk_value_bytes((char *)p);

            if (vals_only || !keys_only) {
                if (enc == 1) {
                    int64_t iv;
                    memcpy(&iv, val, 8);
                    char istr[24];
                    int il = snprintf(istr, sizeof(istr), "%" PRId64, iv);
                    temp_hl = snprintf(temp, sizeof(temp), "$%d\r\n", il);
                    dynbuf_cat(wb, temp, temp_hl);
                    dynbuf_cat(wb, istr, il);
                    dynbuf_cat(wb, "\r\n", 2);
                } else {
                    temp_hl = snprintf(temp, sizeof(temp), "$%u\r\n", vlen);
                    dynbuf_cat(wb, temp, temp_hl);
                    dynbuf_cat(wb, val, (int)vlen);
                    dynbuf_cat(wb, "\r\n", 2);
                }
            }
            if (p > end) break;
        }
        return;
    }

    kv_dict_t *d = (kv_dict_t *)(entry->data + 2);
    kv_iter_t *it = dict_iter_new(d, 1);
    if (!it) return;
    kv_entry_t *de;
    while ((de = dict_iter_next(it)) != NULL) {
        uint32_t vlen = entry_payload_len(de);
        uint8_t enc = entry_encoding(de);

        if (!vals_only) {
            /* Write field name */
            temp_hl = snprintf(temp, sizeof(temp), "$%u\r\n", de->key_len);
            dynbuf_cat(wb, temp, temp_hl);
            dynbuf_cat(wb, entry_key(de), (int)de->key_len);
            dynbuf_cat(wb, "\r\n", 2);
        }
        if (!keys_only) {
            if (enc == KV_ENCODING_INT) {
                int64_t iv;
                memcpy(&iv, entry_payload(de), 8);
                char istr[24];
                int il = snprintf(istr, sizeof(istr), "%" PRId64, iv);
                temp_hl = snprintf(temp, sizeof(temp), "$%d\r\n", il);
                dynbuf_cat(wb, temp, temp_hl);
                dynbuf_cat(wb, istr, il);
                dynbuf_cat(wb, "\r\n", 2);
            } else {
                temp_hl = snprintf(temp, sizeof(temp), "$%u\r\n", vlen);
                dynbuf_cat(wb, temp, temp_hl);
                dynbuf_cat(wb, entry_payload(de), (int)vlen);
                dynbuf_cat(wb, "\r\n", 2);
            }
        }
    }
    dict_iter_free(it);
}

/* =====================================================================
 * HASH command dispatch
 * ===================================================================== */

dispatch_result_t hash_cmd(kvs_token_t cmd, kv_entry_t *entry,
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

    /* ── HSET / HGET / HDEL / HGETALL / HKEYS / HVALS / HLEN / HEXISTS /
          HSETNX / HINCRBY / HMGET / HSTRLEN ── */
    case 'H':
        if (TOKEN_EQ(cmd, "HSET")) {
            if (argc < 3) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }

            uint64_t h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            kv_entry_t *e = entry;
            int existed = 0;

            if (!e) {
                char data[16];
                hash_build_empty(data);
                e = dict_add(&g_dict, key, (uint32_t)strlen(key), data, HASH_EMPTY_SIZE, h);
                if (!e) { dynbuf_cat(wb, "-ERR create hash failed\r\n", 24); return DISPATCH_ERROR; }
            } else {
                if (entry_type(e) != KV_TYPE_HASH) {
                    dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                    return DISPATCH_ERROR;
                }
                if (check_expiry(e, key, h)) {
                    existed = 1;
                    char data[16];
                    hash_build_empty(data);
                    e = dict_add(&g_dict, key, (uint32_t)strlen(key), data, HASH_EMPTY_SIZE, h);
                    if (!e) { dynbuf_cat(wb, "-ERR create hash failed\r\n", 24); return DISPATCH_ERROR; }
                }
            }
            (void)existed;

            int total_added = 0;
            for (int i = 1; i + 1 < argc; i += 2) {
                int added = 0;
                if (hash_set(&e, args[i].ptr, args[i].len,
                            args[i+1].ptr, args[i+1].len, &added, 0) < 0) {
                    dynbuf_cat(wb, "-ERR hset failed\r\n", 17);
                    return DISPATCH_ERROR;
                }
                total_added += added;
            }

            int hl = snprintf(temp, sizeof(temp), ":%d\r\n", total_added);
            dynbuf_cat(wb, temp, hl);
            return DISPATCH_OK;
        }

        /* ── HMSET key f v [f v ...] — legacy multi-set, returns +OK ── */
        if (TOKEN_EQ(cmd, "HMSET")) {
            if (argc < 3 || ((argc - 1) % 2) != 0) {
                dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            kv_entry_t *e = entry;
            if (!e) {
                char data[16]; hash_build_empty(data);
                e = dict_add(&g_dict, key, (uint32_t)strlen(key), data, HASH_EMPTY_SIZE, h);
                if (!e) { dynbuf_cat(wb, "-ERR create hash failed\r\n", 24); return DISPATCH_ERROR; }
            } else if (check_expiry(e, key, h)) {
                char data[16]; hash_build_empty(data);
                e = dict_add(&g_dict, key, (uint32_t)strlen(key), data, HASH_EMPTY_SIZE, h);
                if (!e) { dynbuf_cat(wb, "-ERR create hash failed\r\n", 24); return DISPATCH_ERROR; }
            }
            for (int i = 1; i + 1 < argc; i += 2) {
                int added = 0;
                if (hash_set(&e, args[i].ptr, args[i].len,
                             args[i+1].ptr, args[i+1].len, &added, 0) < 0) {
                    dynbuf_cat(wb, "-ERR hmset failed\r\n", 19);
                    return DISPATCH_ERROR;
                }
            }
            dynbuf_cat(wb, REP_OK, REP_OK_LEN);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "HSETNX")) {
            if (argc < 3) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }

            uint64_t h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            kv_entry_t *e = entry;

            if (!e) {
                char data[16];
                hash_build_empty(data);
                e = dict_add(&g_dict, key, (uint32_t)strlen(key), data, HASH_EMPTY_SIZE, h);
                if (!e) { dynbuf_cat(wb, "-ERR create hash failed\r\n", 24); return DISPATCH_ERROR; }
            } else {
                if (entry_type(e) != KV_TYPE_HASH) {
                    dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                    return DISPATCH_ERROR;
                }
                if (check_expiry(e, key, h)) {
                    char data[16];
                    hash_build_empty(data);
                    e = dict_add(&g_dict, key, (uint32_t)strlen(key), data, HASH_EMPTY_SIZE, h);
                    if (!e) { dynbuf_cat(wb, "-ERR create hash failed\r\n", 24); return DISPATCH_ERROR; }
                }
            }

            int added = 0;
            if (hash_set(&e, args[1].ptr, args[1].len,
                        args[2].ptr, args[2].len, &added, 1) < 0) {
                dynbuf_cat(wb, "-ERR hsetnx failed\r\n", 18);
                return DISPATCH_ERROR;
            }
            dynbuf_cat(wb, added ? REP_ONE : REP_ZERO,
                           added ? REP_ONE_LEN : REP_ZERO_LEN);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "HGET")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, REP_NIL, REP_NIL_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_HASH) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, h)) {
                dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                return DISPATCH_OK;
            }

            int vlen = 0;
            uint8_t enc = 0;
            const char *val = hash_get(entry, args[1].ptr, args[1].len, &vlen, &enc);
            write_value_resp(wb, val, (uint32_t)vlen, enc);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "HMGET")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }

            int field_count = argc - 1;
            char hdr[32];
            int hl = snprintf(hdr, sizeof(hdr), "*%d\r\n", field_count);
            dynbuf_cat(wb, hdr, hl);

            if (!entry) {
                for (int i = 0; i < field_count; i++)
                    dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                return DISPATCH_OK;
            }
            if (entry_type(entry) != KV_TYPE_HASH) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, h)) {
                for (int i = 0; i < field_count; i++)
                    dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                return DISPATCH_OK;
            }

            for (int i = 1; i < argc; i++) {
                int vlen = 0;
                uint8_t enc = 0;
                const char *val = hash_get(entry, args[i].ptr, args[i].len, &vlen, &enc);
                write_value_resp(wb, val, (uint32_t)vlen, enc);
            }
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "HSTRLEN")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_HASH) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, h)) {
                dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN);
                return DISPATCH_OK;
            }

            int vlen = 0;
            uint8_t enc = 0;
            const char *val = hash_get(entry, args[1].ptr, args[1].len, &vlen, &enc);
            if (!val) {
                dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN);
            } else if (enc == KV_ENCODING_INT) {
                int64_t iv;
                memcpy(&iv, val, 8);
                char istr[24];
                int il = snprintf(istr, sizeof(istr), "%" PRId64, iv);
                int hl = snprintf(temp, sizeof(temp), ":%d\r\n", il);
                dynbuf_cat(wb, temp, hl);
            } else {
                int hl = snprintf(temp, sizeof(temp), ":%d\r\n", vlen);
                dynbuf_cat(wb, temp, hl);
            }
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "HDEL")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_HASH) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, h)) {
                dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN);
                return DISPATCH_OK;
            }

            int total_removed = 0;
            for (int i = 1; i < argc; i++) {
                int removed = 0;
                if (hash_del(&entry, args[i].ptr, args[i].len, &removed) < 0) {
                    dynbuf_cat(wb, "-ERR hdel failed\r\n", 17);
                    return DISPATCH_ERROR;
                }
                total_removed += removed;
            }

            int hl = snprintf(temp, sizeof(temp), ":%d\r\n", total_removed);
            dynbuf_cat(wb, temp, hl);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "HINCRBY")) {
            if (argc < 3) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }

            /* Parse increment */
            int64_t delta;
            if (!parse_int64(args[2].ptr, args[2].len, &delta)) {
                dynbuf_cat(wb, REP_ERR_INT, REP_ERR_INT_LEN);
                return DISPATCH_ERROR;
            }

            uint64_t h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            kv_entry_t *e = entry;

            if (!e) {
                char data[16];
                hash_build_empty(data);
                e = dict_add(&g_dict, key, (uint32_t)strlen(key), data, HASH_EMPTY_SIZE, h);
                if (!e) { dynbuf_cat(wb, "-ERR create hash failed\r\n", 24); return DISPATCH_ERROR; }
            } else {
                if (entry_type(e) != KV_TYPE_HASH) {
                    dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                    return DISPATCH_ERROR;
                }
                if (check_expiry(e, key, h)) {
                    char data[16];
                    hash_build_empty(data);
                    e = dict_add(&g_dict, key, (uint32_t)strlen(key), data, HASH_EMPTY_SIZE, h);
                    if (!e) { dynbuf_cat(wb, "-ERR create hash failed\r\n", 24); return DISPATCH_ERROR; }
                }
            }

            int64_t new_val;
            int rc = hash_incrby(&e, args[1].ptr, args[1].len, delta, &new_val);
            if (rc == -2) {
                dynbuf_cat(wb, REP_ERR_INT, REP_ERR_INT_LEN);
                return DISPATCH_ERROR;
            }
            if (rc < 0) {
                dynbuf_cat(wb, "-ERR hincrby failed\r\n", 19);
                return DISPATCH_ERROR;
            }

            char buf[32];
            int bl = snprintf(buf, sizeof(buf), ":%" PRId64 "\r\n", new_val);
            dynbuf_cat(wb, buf, bl);
            return DISPATCH_OK;
        }

        /* ── HINCRBYFLOAT key field delta ── */
        if (TOKEN_EQ(cmd, "HINCRBYFLOAT")) {
            if (argc < 3) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }

            /* Parse delta */
            char dbuf[64];
            int dn = args[2].len < (int)sizeof(dbuf) - 1 ? args[2].len : (int)sizeof(dbuf) - 1;
            memcpy(dbuf, args[2].ptr, (size_t)dn); dbuf[dn] = '\0';
            char *endp;
            double delta = strtod(dbuf, &endp);
            if (*endp != '\0' || dn == 0) {
                dynbuf_cat(wb, "-ERR value is not a valid float\r\n", 33);
                return DISPATCH_ERROR;
            }

            uint64_t h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            kv_entry_t *e = entry;
            if (!e) {
                char data[16]; hash_build_empty(data);
                e = dict_add(&g_dict, key, (uint32_t)strlen(key), data, HASH_EMPTY_SIZE, h);
                if (!e) { dynbuf_cat(wb, "-ERR create hash failed\r\n", 24); return DISPATCH_ERROR; }
            } else if (check_expiry(e, key, h)) {
                char data[16]; hash_build_empty(data);
                e = dict_add(&g_dict, key, (uint32_t)strlen(key), data, HASH_EMPTY_SIZE, h);
                if (!e) { dynbuf_cat(wb, "-ERR create hash failed\r\n", 24); return DISPATCH_ERROR; }
            }

            /* Current value */
            int vlen = 0; uint8_t enc = 0;
            const char *val = hash_get(e, args[1].ptr, args[1].len, &vlen, &enc);
            double cur = 0.0;
            if (val) {
                if (enc == KV_ENCODING_INT) {
                    int64_t iv; memcpy(&iv, val, 8); cur = (double)iv;
                } else {
                    char tmp[64];
                    int n = vlen < (int)sizeof(tmp) - 1 ? vlen : (int)sizeof(tmp) - 1;
                    memcpy(tmp, val, (size_t)n); tmp[n] = '\0';
                    cur = strtod(tmp, &endp);
                    if (*endp != '\0') {
                        dynbuf_cat(wb, "-ERR hash value is not a float\r\n", 32);
                        return DISPATCH_ERROR;
                    }
                }
            }

            double nv = cur + delta;
            if (!isfinite(nv)) {
                dynbuf_cat(wb, "-ERR increment would produce NaN or Infinity\r\n", 46);
                return DISPATCH_ERROR;
            }

            /* Format like Redis: %.17g, strip trailing zeros after dot. */
            char sbuf[48];
            int sl = snprintf(sbuf, sizeof(sbuf), "%.17g", nv);
            /* Strip trailing zeros if there is a '.' */
            if (memchr(sbuf, '.', (size_t)sl)) {
                while (sl > 0 && sbuf[sl - 1] == '0') sl--;
                if (sl > 0 && sbuf[sl - 1] == '.') sl--;
            }
            sbuf[sl] = '\0';

            int added;
            if (hash_set(&e, args[1].ptr, args[1].len, sbuf, sl, &added, 0) < 0) {
                dynbuf_cat(wb, "-ERR hincrbyfloat failed\r\n", 26);
                return DISPATCH_ERROR;
            }
            int hl = snprintf(temp, sizeof(temp), "$%d\r\n", sl);
            dynbuf_cat(wb, temp, hl);
            dynbuf_cat(wb, sbuf, sl);
            dynbuf_cat(wb, "\r\n", 2);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "HLEN")) {
            if (!entry) { dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_HASH) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, h)) {
                dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN);
                return DISPATCH_OK;
            }
            int hl = snprintf(temp, sizeof(temp), ":%u\r\n", hash_len(entry));
            dynbuf_cat(wb, temp, hl);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "HEXISTS")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
            if (!entry) { dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_HASH) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, h)) {
                dynbuf_cat(wb, REP_ZERO, REP_ZERO_LEN);
                return DISPATCH_OK;
            }

            int vlen;
            const char *val = hash_get(entry, args[1].ptr, args[1].len, &vlen, NULL);
            dynbuf_cat(wb, val ? REP_ONE : REP_ZERO,
                           val ? REP_ONE_LEN : REP_ZERO_LEN);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "HGETALL")) {
            if (!entry) { dynbuf_cat(wb, "*0\r\n", 4); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_HASH) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, h)) {
                dynbuf_cat(wb, "*0\r\n", 4);
                return DISPATCH_OK;
            }
            write_all_fields(wb, entry, 0, 0);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "HKEYS")) {
            if (!entry) { dynbuf_cat(wb, "*0\r\n", 4); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_HASH) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, h)) {
                dynbuf_cat(wb, "*0\r\n", 4);
                return DISPATCH_OK;
            }
            write_all_fields(wb, entry, 1, 0);
            return DISPATCH_OK;
        }

        if (TOKEN_EQ(cmd, "HVALS")) {
            if (!entry) { dynbuf_cat(wb, "*0\r\n", 4); return DISPATCH_OK; }
            if (entry_type(entry) != KV_TYPE_HASH) {
                dynbuf_cat(wb, REP_WRONGTYPE, REP_WRONGTYPE_LEN);
                return DISPATCH_ERROR;
            }
            uint64_t h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, h)) {
                dynbuf_cat(wb, "*0\r\n", 4);
                return DISPATCH_OK;
            }
            write_all_fields(wb, entry, 0, 1);
            return DISPATCH_OK;
        }

        /* ── HRANDFIELD key [count [WITHVALUES]] ──
             Without count: returns a single random field (bulk string or nil).
             count > 0:  up to N distinct fields (clamped to hash size).
             count < 0:  N fields with replacement (may repeat).
             WITHVALUES: pairs (field, value) interleaved. */
        if (TOKEN_EQ(cmd, "HRANDFIELD")) {
            if (!entry) {
                if (argc < 2) dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                else          dynbuf_cat(wb, "*0\r\n", 4);
                return DISPATCH_OK;
            }
            uint64_t h = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
            if (check_expiry(entry, key, h)) {
                if (argc < 2) dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                else          dynbuf_cat(wb, "*0\r\n", 4);
                return DISPATCH_OK;
            }

            uint32_t total = hash_len(entry);
            if (total == 0) {
                if (argc < 2) dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
                else          dynbuf_cat(wb, "*0\r\n", 4);
                return DISPATCH_OK;
            }

            /* Materialize (field, value, enc) triples. */
            typedef struct { const char *f; uint32_t fl;
                             const char *v; uint32_t vl; uint8_t enc; } fv_t;
            fv_t *arr = (fv_t *)malloc(sizeof(fv_t) * total);
            if (!arr) { dynbuf_cat(wb, "-ERR oom\r\n", 10); return DISPATCH_ERROR; }

            uint32_t n = 0;
            if (entry_encoding(entry) == KV_ENCODING_PACKED) {
                const char *p = entry->data + 6;
                const char *end = entry->data + entry->val_len;
                while (p < end && n < total) {
                    uint32_t fl = pk_read_u32(p);
                    arr[n].f = p + 4; arr[n].fl = fl;
                    p += 4 + fl;
                    if (p >= end) break;
                    uint8_t enc_norm;
                    int vl_i;
                    const char *v = pk_get_value((char *)p, &vl_i, &enc_norm);
                    arr[n].v = v; arr[n].vl = (uint32_t)vl_i; arr[n].enc = enc_norm;
                    p += pk_value_bytes((char *)p);
                    n++;
                }
            } else {
                kv_dict_t *d = (kv_dict_t *)(entry->data + 2);
                kv_iter_t *it = dict_iter_new(d, 1);
                kv_entry_t *de;
                while (it && (de = dict_iter_next(it)) != NULL && n < total) {
                    arr[n].f = entry_key(de); arr[n].fl = de->key_len;
                    arr[n].v = entry_payload(de); arr[n].vl = entry_payload_len(de);
                    arr[n].enc = entry_encoding(de);
                    n++;
                }
                if (it) dict_iter_free(it);
            }

            /* No count → single field as bulk string */
            if (argc < 2) {
                uint32_t r = (uint32_t)rand() % n;
                int hl = snprintf(temp, sizeof(temp), "$%u\r\n", arr[r].fl);
                dynbuf_cat(wb, temp, hl);
                dynbuf_cat(wb, arr[r].f, (int)arr[r].fl);
                dynbuf_cat(wb, "\r\n", 2);
                free(arr);
                return DISPATCH_OK;
            }

            /* Parse count */
            int64_t count64;
            if (!parse_int64(args[1].ptr, args[1].len, &count64)) {
                free(arr);
                dynbuf_cat(wb, REP_ERR_INT, REP_ERR_INT_LEN);
                return DISPATCH_ERROR;
            }

            int withvalues = 0;
            if (argc >= 3) {
                kvs_token_t w = args[2];
                if (w.len == 10 && (memcmp(w.ptr, "WITHVALUES", 10) == 0 ||
                                     memcmp(w.ptr, "withvalues", 10) == 0)) withvalues = 1;
                else {
                    free(arr);
                    dynbuf_cat(wb, "-ERR syntax error\r\n", 19);
                    return DISPATCH_ERROR;
                }
            }

            uint32_t want = (count64 < 0) ? (uint32_t)(-count64) : (uint32_t)count64;
            int with_replacement = (count64 < 0);
            if (!with_replacement && want > n) want = n;

            uint32_t out_items = withvalues ? want * 2 : want;
            int hl = snprintf(temp, sizeof(temp), "*%u\r\n", out_items);
            dynbuf_cat(wb, temp, hl);

            if (want == 0) { free(arr); return DISPATCH_OK; }

            /* For "without replacement" sampling, perform a partial Fisher-Yates
               shuffle of an index array of size n, take the first `want`. */
            uint32_t *idx = NULL;
            if (!with_replacement) {
                idx = (uint32_t *)malloc(sizeof(uint32_t) * n);
                if (!idx) { free(arr);
                            dynbuf_cat(wb, "-ERR oom\r\n", 10);
                            return DISPATCH_ERROR; }
                for (uint32_t i = 0; i < n; i++) idx[i] = i;
                for (uint32_t i = 0; i < want; i++) {
                    uint32_t j = i + ((uint32_t)rand() % (n - i));
                    uint32_t t = idx[i]; idx[i] = idx[j]; idx[j] = t;
                }
            }

            for (uint32_t i = 0; i < want; i++) {
                uint32_t r = with_replacement ? ((uint32_t)rand() % n) : idx[i];
                int hh = snprintf(temp, sizeof(temp), "$%u\r\n", arr[r].fl);
                dynbuf_cat(wb, temp, hh);
                dynbuf_cat(wb, arr[r].f, (int)arr[r].fl);
                dynbuf_cat(wb, "\r\n", 2);
                if (withvalues) {
                    write_value_resp(wb, arr[r].v, arr[r].vl, arr[r].enc);
                }
            }

            free(idx);
            free(arr);
            return DISPATCH_OK;
        }
        break; /* 'H' */
    }

    dynbuf_cat(wb, REP_ERR_UNK, REP_ERR_UNK_LEN);
    return DISPATCH_ERROR;
}
