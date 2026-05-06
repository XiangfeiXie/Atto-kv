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
 * Engine access — all through the global dict
 * ===================================================================== */

extern kv_dict_t g_dict;

extern long long get_time_now(void);

static inline char *token_to_key(kvs_token_t t, char *buf, int sz) {
    int n = t.len < sz - 1 ? t.len : sz - 1;
    memcpy(buf, t.ptr, (size_t)n);
    buf[n] = '\0';
    return buf;
}

/* =====================================================================
 * Item helpers (lightweight KVItem for value/exire transport)
 * ===================================================================== */

static KVItem* item_create_len(const char *value, int vlen) {
    if (!value || vlen < 0) return NULL;
    KVItem *it = (KVItem*)malloc(sizeof(KVItem));
    if (!it) return NULL;
    memset(it, 0, sizeof(KVItem));
    it->len = (size_t)vlen;
    it->value = (char*)malloc((size_t)vlen + 1);
    if (!it->value) { free(it); return NULL; }
    memcpy(it->value, value, (size_t)vlen);
    it->value[vlen] = '\0';
    it->expire = 0;
    return it;
}

static void item_free(KVItem *it) {
    if (!it) return;
    free(it->value);
    free(it);
}

void string_build_entry_data(const char *value, int val_len, char *out_data) {
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
 * Core engine ops
 * ===================================================================== */

static int engine_set(const char *key, KVItem *item) {
    uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
    kv_entry_t *e = dict_find(&g_dict, key, (uint32_t)strlen(key), hash);
    if (e) return 1;  /* already exists */

    int vlen = item ? (int)item->len : 0;
    uint32_t dlen = (uint32_t)(2 + vlen);
    char *data = (char *)malloc((size_t)dlen);
    string_build_entry_data(item ? item->value : NULL, vlen, data);

    e = dict_add(&g_dict, key, (uint32_t)strlen(key), data, dlen, hash);
    free(data);
    if (!e) return -1;
    if (item && item->expire) e->expire = item->expire;
    return 0;
}

static KVItem* engine_get(const char *key) {
    uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
    kv_entry_t *e = dict_find(&g_dict, key, (uint32_t)strlen(key), hash);
    if (!e) return NULL;

    if (e->expire && get_time_now() > e->expire) {
        dict_delete(&g_dict, key, (uint32_t)strlen(key), e->hash);
        return NULL;
    }
    KVItem *it = item_create_len(entry_payload(e), (int)entry_payload_len(e));
    if (it) it->expire = e->expire;
    return it;
}

static int engine_del(const char *key) {
    uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
    kv_entry_t *e = dict_find(&g_dict, key, (uint32_t)strlen(key), hash);
    if (!e) return 1;
    if (e->expire && get_time_now() > e->expire) {
        dict_delete(&g_dict, key, (uint32_t)strlen(key), hash);
        return 1;
    }
    return dict_delete(&g_dict, key, (uint32_t)strlen(key), hash);
}

static int engine_mod(const char *key, KVItem *item) {
    uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
    kv_entry_t *e = dict_find(&g_dict, key, (uint32_t)strlen(key), hash);
    if (!e) return 1;
    if (e->expire && get_time_now() > e->expire) {
        dict_delete(&g_dict, key, (uint32_t)strlen(key), hash);
        return 1;
    }
    uint32_t nd = item ? (uint32_t)(2 + item->len) : 2;
    e = dict_resize_entry(&g_dict, e, nd);
    if (!e) return -1;
    string_build_entry_data(item ? item->value : NULL, item ? (int)item->len : 0, e->data);
    if (item && item->expire) e->expire = item->expire;
    return 0;
}

static int engine_exist(const char *key) {
    uint64_t hash = kv_hash((const uint8_t *)key, (uint32_t)strlen(key));
    kv_entry_t *e = dict_find(&g_dict, key, (uint32_t)strlen(key), hash);
    if (!e) return -1;
    if (e->expire && get_time_now() > e->expire) {
        dict_delete(&g_dict, key, (uint32_t)strlen(key), hash);
        return -1;
    }
    return 0;
}

/* =====================================================================
 * STRING command dispatch
 *
 * Returns wb->len on success, -1 on error.
 * Does NOT handle AOF/eBPF — that's the caller's job (kvs_dispatch).
 * ===================================================================== */

dispatch_result_t string_cmd(kvs_token_t cmd, kv_entry_t *entry,
                             kvs_token_t *args, int argc, DynBuf *wb,
                             const char *msg_raw, int msg_len)
{
    (void)entry;   /* engine helpers do their own dict lookup */
    (void)msg_raw; /* AOF handled by kvs_dispatch */
    (void)msg_len;

    char key_buf[256] = {0};
    const char *key = NULL;
    if (argc >= 1)
        key = token_to_key(args[0], key_buf, (int)sizeof(key_buf));

    char temp[64];
    int  ret;

    switch (cmd.ptr[0]) {

    case 'S': /* SET */
        if (!TOKEN_EQ(cmd, "SET")) break;
        if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
        {
            KVItem *it = item_create_len(args[1].ptr, args[1].len);
            if (it && argc >= 3) {
                char eb[32]; token_to_key(args[2], eb, (int)sizeof(eb));
                it->expire = get_time_now() + (long long)atoi(eb) * 1000;
            }
            ret = engine_set(key, it);
            item_free(it);
            if (ret < 0)       dynbuf_cat(wb, REP_ERROR, REP_ERROR_LEN);
            else if (ret == 0) dynbuf_cat(wb, REP_OK,    REP_OK_LEN);
            else               dynbuf_cat(wb, REP_EXIST, REP_EXIST_LEN);
        }
        return DISPATCH_OK;

    case 'G': /* GET */
        if (!TOKEN_EQ(cmd, "GET")) break;
        {
            KVItem *r = engine_get(key);
            if (!r) {
                dynbuf_cat(wb, REP_NIL, REP_NIL_LEN);
            } else {
                int hl = snprintf(temp, sizeof(temp), "$%zu\r\n", r->len);
                dynbuf_cat(wb, temp, hl);
                dynbuf_cat(wb, r->value, (int)r->len);
                dynbuf_cat(wb, "\r\n", 2);
                item_free(r);
            }
        }
        return DISPATCH_OK;

    case 'D': /* DEL */
        if (!TOKEN_EQ(cmd, "DEL")) break;
        ret = engine_del(key);
        if (ret < 0)       dynbuf_cat(wb, REP_ERROR, REP_ERROR_LEN);
        else if (ret == 0) dynbuf_cat(wb, REP_OK,   REP_OK_LEN);
        else               dynbuf_cat(wb, REP_NIL,  REP_NIL_LEN);
        return DISPATCH_OK;

    case 'M': /* MOD */
        if (!TOKEN_EQ(cmd, "MOD")) break;
        if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN); return DISPATCH_ERROR; }
        {
            KVItem *it = item_create_len(args[1].ptr, args[1].len);
            if (it && argc >= 3) {
                char eb[32]; token_to_key(args[2], eb, (int)sizeof(eb));
                it->expire = get_time_now() + (long long)atoi(eb) * 1000;
            }
            ret = engine_mod(key, it);
            item_free(it);
            if (ret < 0)       dynbuf_cat(wb, REP_ERROR, REP_ERROR_LEN);
            else if (ret == 0) dynbuf_cat(wb, REP_OK,   REP_OK_LEN);
            else               dynbuf_cat(wb, REP_NIL,  REP_NIL_LEN);
        }
        return DISPATCH_OK;

    case 'E': /* EXIST */
        if (!TOKEN_EQ(cmd, "EXIST")) break;
        ret = engine_exist(key);
        dynbuf_cat(wb, (ret == 0) ? REP_EXIST : REP_NIL,
                        (ret == 0) ? REP_EXIST_LEN : REP_NIL_LEN);
        return DISPATCH_OK;
    }

    /* unknown STRING sub-command */
    dynbuf_cat(wb, REP_ERR_UNK, REP_ERR_UNK_LEN);
    return DISPATCH_ERROR;
}
