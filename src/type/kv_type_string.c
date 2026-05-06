#include "kv_type_string.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

/* =====================================================================
 * Static RESP fragments (compile-time sizeof, zero strlen overhead)
 * ===================================================================== */

#define S(id, str) \
    static const char id[] = str; \
    static const int  id##_LEN = (int)(sizeof(str) - 1)

S(REP_OK,        "+OK\r\n");
S(REP_NIL,       "$-1\r\n");
S(REP_EXIST,     ":1\r\n");
S(REP_ERROR,     "-ERR error\r\n");
S(REP_PONG,      "+PONG\r\n");
S(REP_ERR_UNK,   "-ERR unknown command\r\n");
S(REP_ERR_ARGS,  "-ERR wrong number of arguments\r\n");
S(REP_WRONGTYPE, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");

#undef S

/* =====================================================================
 * Engine access — all through the global dict.
 * Key is passed as (ptr, len), no null-termination required.
 * ===================================================================== */

extern kv_dict_t g_dict;
extern long long get_time_now(void);

/* =====================================================================
 * Integer parsing — fast path for inline int encoding
 * ===================================================================== */

static int try_parse_int64(const char *p, int len, int64_t *out) {
    if (len <= 0 || len > 19) return 0;  /* safe bound for int64 */
    const char *end = p + len;
    int negative = 0;
    if (*p == '-') {
        if (len == 1) return 0;
        negative = 1;
        p++;
    }
    if (p >= end) return 0;
    /* reject leading zero unless the value is exactly "0" */
    if (*p == '0' && (end - p) > 1) return 0;
    int64_t v = 0;
    for (; p < end; p++) {
        if (*p < '0' || *p > '9') return 0;
        v = v * 10 + (*p - '0');
    }
    *out = negative ? -v : v;
    return 1;
}

/* =====================================================================
 * Core engine ops (raw: no KVItem allocations)
 * ===================================================================== */

static int engine_set_raw(const char *key, uint32_t key_len,
                          const char *val, uint32_t val_len,
                          long long expire)
{
    uint64_t hash = kv_hash((const uint8_t *)key, key_len);
    if (dict_find(&g_dict, key, key_len, hash))
        return 1;  /* already exists */

    int64_t iv;
    uint8_t enc;
    const char *pload;
    uint32_t plen;
    if (try_parse_int64(val, (int)val_len, &iv)) {
        enc  = KV_ENCODING_INT;
        pload = (const char *)&iv;
        plen  = sizeof(int64_t);
    } else {
        enc  = KV_ENCODING_RAW;
        pload = val;
        plen  = val_len;
    }
    kv_entry_t *e = dict_add_raw(&g_dict, key, key_len,
                                 KV_TYPE_STRING, enc,
                                 pload, plen, hash);
    if (!e) return -1;
    if (expire) e->expire = expire;
    return 0;
}

static const char *engine_get_raw(const char *key, uint32_t key_len,
                                  uint32_t *vlen_out, long long *expire_out,
                                  uint8_t *enc_out)
{
    uint64_t hash = kv_hash((const uint8_t *)key, key_len);
    kv_entry_t *e = dict_find(&g_dict, key, key_len, hash);
    if (!e) return NULL;

    if (e->expire && get_time_now() > e->expire) {
        dict_delete(&g_dict, key, key_len, e->hash);
        return NULL;
    }
    if (vlen_out)   *vlen_out   = entry_payload_len(e);
    if (expire_out) *expire_out = e->expire;
    if (enc_out)    *enc_out    = entry_encoding(e);
    return entry_payload(e);
}

static int engine_mod_raw(const char *key, uint32_t key_len,
                          const char *val, uint32_t val_len,
                          long long expire)
{
    uint64_t hash = kv_hash((const uint8_t *)key, key_len);
    kv_entry_t *e = dict_find(&g_dict, key, key_len, hash);
    if (!e) return 1;

    if (e->expire && get_time_now() > e->expire) {
        dict_delete(&g_dict, key, key_len, hash);
        return 1;
    }
    int64_t iv;
    uint8_t enc;
    const char *pload;
    uint32_t plen;
    if (try_parse_int64(val, (int)val_len, &iv)) {
        enc  = KV_ENCODING_INT;
        pload = (const char *)&iv;
        plen  = sizeof(int64_t);
    } else {
        enc  = KV_ENCODING_RAW;
        pload = val;
        plen  = val_len;
    }
    uint32_t nd = (uint32_t)(2 + plen);
    e = dict_resize_entry(&g_dict, e, nd);
    if (!e) return -1;
    e->data[0] = (char)KV_TYPE_STRING;
    e->data[1] = (char)enc;
    memcpy(e->data + 2, pload, plen);
    if (expire) e->expire = expire;
    return 0;
}

static int engine_del_raw(const char *key, uint32_t key_len)
{
    uint64_t hash = kv_hash((const uint8_t *)key, key_len);
    kv_entry_t *e = dict_find(&g_dict, key, key_len, hash);
    if (!e) return 1;
    if (e->expire && get_time_now() > e->expire) {
        dict_delete(&g_dict, key, key_len, hash);
        return 1;
    }
    return dict_delete(&g_dict, key, key_len, hash);
}

static int engine_exist_raw(const char *key, uint32_t key_len)
{
    uint64_t hash = kv_hash((const uint8_t *)key, key_len);
    kv_entry_t *e = dict_find(&g_dict, key, key_len, hash);
    if (!e) return -1;
    if (e->expire && get_time_now() > e->expire) {
        dict_delete(&g_dict, key, key_len, hash);
        return -1;
    }
    return 0;
}

void string_build_entry_data(const char *value, int val_len, char *out_data)
{
    out_data[0] = (char)KV_TYPE_STRING;
    out_data[1] = (char)KV_ENCODING_RAW;
    if (value && val_len > 0)
        memcpy(out_data + 2, value, (size_t)val_len);
}

int string_is_write_cmd(kvs_token_t cmd) {
    return TOKEN_EQ(cmd, "SET") || TOKEN_EQ(cmd, "DEL") ||
           TOKEN_EQ(cmd, "MOD");
}

/* =====================================================================
 * STRING command dispatch
 *
 * Uses token pointers directly — zero KVItem allocations.
 * AOF / eBPF handled by kvs_dispatch caller.
 * ===================================================================== */

dispatch_result_t string_cmd(kvs_token_t cmd, kv_entry_t *entry,
                             kvs_token_t *args, int argc, DynBuf *wb,
                             const char *msg_raw, int msg_len)
{
    (void)entry;
    (void)msg_raw;
    (void)msg_len;

    char temp[64];
    int  ret;

    switch (cmd.ptr[0]) {

    case 'S': /* SET */
        if (!TOKEN_EQ(cmd, "SET")) break;
        if (argc < 2) {
            dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN);
            return DISPATCH_ERROR;
        }
        {
            long long expire = 0;
            if (argc >= 3) {
                char eb[32];
                int nl = args[2].len < 31 ? args[2].len : 31;
                memcpy(eb, args[2].ptr, (size_t)nl);
                eb[nl] = '\0';
                expire = get_time_now() + (long long)atoi(eb) * 1000;
            }
            ret = engine_set_raw(args[0].ptr, (uint32_t)args[0].len,
                                args[1].ptr, (uint32_t)args[1].len, expire);
            if (ret < 0)       dynbuf_cat(wb, REP_ERROR, REP_ERROR_LEN);
            else if (ret == 0) dynbuf_cat(wb, REP_OK,    REP_OK_LEN);
            else               dynbuf_cat(wb, REP_EXIST, REP_EXIST_LEN);
        }
        return DISPATCH_OK;

    case 'G': /* GET */
        if (!TOKEN_EQ(cmd, "GET")) break;
        {
            uint32_t vlen;
            uint8_t  enc;
            const char *val = engine_get_raw(args[0].ptr, (uint32_t)args[0].len,
                                              &vlen, NULL, &enc);
            if (!val) {
                dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
            } else if (enc == KV_ENCODING_INT) {
                int64_t iv;
                memcpy(&iv, val, sizeof(iv));
                char istr[24];
                int il = snprintf(istr, sizeof(istr), "%lld", (long long)iv);
                int hl = snprintf(temp, sizeof(temp), "$%d\r\n", il);
                dynbuf_cat(wb, temp, hl);
                dynbuf_cat(wb, istr, il);
                dynbuf_cat(wb, "\r\n", 2);
            } else {
                int hl = snprintf(temp, sizeof(temp), "$%u\r\n", vlen);
                dynbuf_cat(wb, temp, hl);
                dynbuf_cat(wb, val, (int)vlen);
                dynbuf_cat(wb, "\r\n", 2);
            }
        }
        return DISPATCH_OK;

    case 'D': /* DEL */
        if (!TOKEN_EQ(cmd, "DEL")) break;
        ret = engine_del_raw(args[0].ptr, (uint32_t)args[0].len);
        if (ret < 0)       dynbuf_cat(wb, REP_ERROR, REP_ERROR_LEN);
        else if (ret == 0) dynbuf_cat(wb, REP_OK,   REP_OK_LEN);
        else               dynbuf_cat(wb, REP_NIL,  REP_NIL_LEN);
        return DISPATCH_OK;

    case 'M': /* MOD */
        if (!TOKEN_EQ(cmd, "MOD")) break;
        if (argc < 2) {
            dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN);
            return DISPATCH_ERROR;
        }
        {
            long long expire = 0;
            if (argc >= 3) {
                char eb[32];
                int nl = args[2].len < 31 ? args[2].len : 31;
                memcpy(eb, args[2].ptr, (size_t)nl);
                eb[nl] = '\0';
                expire = get_time_now() + (long long)atoi(eb) * 1000;
            }
            ret = engine_mod_raw(args[0].ptr, (uint32_t)args[0].len,
                                args[1].ptr, (uint32_t)args[1].len, expire);
            if (ret < 0)       dynbuf_cat(wb, REP_ERROR, REP_ERROR_LEN);
            else if (ret == 0) dynbuf_cat(wb, REP_OK,   REP_OK_LEN);
            else               dynbuf_cat(wb, REP_NIL,  REP_NIL_LEN);
        }
        return DISPATCH_OK;

    case 'E': /* EXIST */
        if (!TOKEN_EQ(cmd, "EXIST")) break;
        ret = engine_exist_raw(args[0].ptr, (uint32_t)args[0].len);
        dynbuf_cat(wb, (ret == 0) ? REP_EXIST : REP_NIL,
                        (ret == 0) ? REP_EXIST_LEN : REP_NIL_LEN);
        return DISPATCH_OK;
    }

    dynbuf_cat(wb, REP_ERR_UNK, REP_ERR_UNK_LEN);
    return DISPATCH_ERROR;
}
