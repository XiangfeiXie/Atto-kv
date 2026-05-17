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

/* g_dict replaced by sharded g_shards (kv_dict.h) */
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

/* SET semantics:
   mode = 0: unconditional (Redis default — create or replace)
   mode = 1: NX — only set if key absent
   mode = 2: XX — only set if key present
   Returns: 0 ok, 1 condition not met (NX/XX fail), -1 hard error. */
static int engine_set_raw(const char *key, uint32_t key_len,
                          const char *val, uint32_t val_len,
                          long long expire, int mode)
{
    uint64_t hash = kv_hash((const uint8_t *)key, key_len);
    kv_entry_t *existing = dict_find(kv_dict_for(hash), key, key_len, hash);
    if (existing && existing->expire && get_time_now() > existing->expire) {
        kvs_cleanup_entry(existing);  /* free nested structures for collection types */
        dict_delete(kv_dict_for(hash), key, key_len, hash);
        existing = NULL;
    }
    if (mode == 1 && existing) return 1;   /* NX failed */
    if (mode == 2 && !existing) return 1;  /* XX failed */

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

    if (existing) {
        uint32_t nd = (uint32_t)(2 + plen);
        kv_entry_t *e = dict_resize_entry(kv_dict_for(hash), existing, nd);
        if (!e) return -1;
        e->data[0] = (char)KV_TYPE_STRING;
        e->data[1] = (char)enc;
        memcpy(e->data + 2, pload, plen);
        e->expire = expire;
        return 0;
    }

    kv_entry_t *e = dict_add_raw(kv_dict_for(hash), key, key_len,
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
    kv_entry_t *e = dict_find(kv_dict_for(hash), key, key_len, hash);
    if (!e) return NULL;

    if (e->expire && get_time_now() > e->expire) {
        dict_delete(kv_dict_for(hash), key, key_len, e->hash);
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
    kv_entry_t *e = dict_find(kv_dict_for(hash), key, key_len, hash);
    if (!e) return 1;

    if (e->expire && get_time_now() > e->expire) {
        dict_delete(kv_dict_for(hash), key, key_len, hash);
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
    e = dict_resize_entry(kv_dict_for(hash), e, nd);
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
    kv_entry_t *e = dict_find(kv_dict_for(hash), key, key_len, hash);
    if (!e) return 1;
    if (e->expire && get_time_now() > e->expire) {
        dict_delete(kv_dict_for(hash), key, key_len, hash);
        return 1;
    }
    return dict_delete(kv_dict_for(hash), key, key_len, hash);
}

static int engine_exist_raw(const char *key, uint32_t key_len)
{
    uint64_t hash = kv_hash((const uint8_t *)key, key_len);
    kv_entry_t *e = dict_find(kv_dict_for(hash), key, key_len, hash);
    if (!e) return -1;
    if (e->expire && get_time_now() > e->expire) {
        dict_delete(kv_dict_for(hash), key, key_len, hash);
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

/* Public wrappers — delegate to the internal engine_* functions. */
int string_engine_set(const char *key, uint32_t key_len,
                      const char *val, uint32_t val_len,
                      long long expire, int mode)
{
    return engine_set_raw(key, key_len, val, val_len, expire, mode);
}

const char *string_engine_get(const char *key, uint32_t key_len,
                              uint32_t *vlen_out, uint8_t *enc_out)
{
    return engine_get_raw(key, key_len, vlen_out, NULL, enc_out);
}

int string_is_write_cmd(kvs_token_t cmd) {
    return TOKEN_EQ(cmd, "SET")    || TOKEN_EQ(cmd, "SETNX")  ||
           TOKEN_EQ(cmd, "SETEX")  || TOKEN_EQ(cmd, "MOD")    ||
           TOKEN_EQ(cmd, "APPEND") || TOKEN_EQ(cmd, "INCR")   ||
           TOKEN_EQ(cmd, "INCRBY") || TOKEN_EQ(cmd, "DECR")   ||
           TOKEN_EQ(cmd, "DECRBY") || TOKEN_EQ(cmd, "GETSET");
}

/* Parse a slice as a positive integer (returns -1 on error). */
static long long slice_to_ll(kvs_token_t t) {
    if (t.len <= 0 || t.len > 20) return -1;
    int i = 0, neg = 0;
    if (t.ptr[0] == '-') { neg = 1; i = 1; if (t.len == 1) return -1; }
    unsigned long long v = 0;
    for (; i < t.len; i++) {
        unsigned c = (unsigned char)t.ptr[i] - (unsigned)'0';
        if (c > 9) return -1;
        if (v > 9223372036854775807ULL / 10) return -1;
        v = v * 10 + c;
    }
    return neg ? -(long long)v : (long long)v;
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

    case 'S': /* SET / SETNX / SETEX / STRLEN */
        if (TOKEN_EQ(cmd, "SET")) {
            if (argc < 2) {
                dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN);
                return DISPATCH_ERROR;
            }
            /* Parse Redis SET options: [EX seconds | PX ms] [NX | XX] */
            long long expire = 0;
            int mode = 0;       /* 0=default, 1=NX, 2=XX */
            for (int i = 2; i < argc; i++) {
                kvs_token_t opt = args[i];
                if ((opt.len == 2 && (opt.ptr[0]=='E'||opt.ptr[0]=='e') &&
                                      (opt.ptr[1]=='X'||opt.ptr[1]=='x'))) {
                    if (i + 1 >= argc) { dynbuf_cat(wb,
                        "-ERR syntax error\r\n", 19); return DISPATCH_ERROR; }
                    long long s = slice_to_ll(args[++i]);
                    if (s <= 0) { dynbuf_cat(wb,
                        "-ERR invalid expire time in 'set' command\r\n", 43);
                        return DISPATCH_ERROR; }
                    expire = get_time_now() + s * 1000;
                } else if (opt.len == 2 && (opt.ptr[0]=='P'||opt.ptr[0]=='p') &&
                                            (opt.ptr[1]=='X'||opt.ptr[1]=='x')) {
                    if (i + 1 >= argc) { dynbuf_cat(wb,
                        "-ERR syntax error\r\n", 19); return DISPATCH_ERROR; }
                    long long ms = slice_to_ll(args[++i]);
                    if (ms <= 0) { dynbuf_cat(wb,
                        "-ERR invalid expire time in 'set' command\r\n", 43);
                        return DISPATCH_ERROR; }
                    expire = get_time_now() + ms;
                } else if (opt.len == 2 && (opt.ptr[0]=='N'||opt.ptr[0]=='n') &&
                                            (opt.ptr[1]=='X'||opt.ptr[1]=='x')) {
                    mode = 1;
                } else if (opt.len == 2 && (opt.ptr[0]=='X'||opt.ptr[0]=='x') &&
                                            (opt.ptr[1]=='X'||opt.ptr[1]=='x')) {
                    mode = 2;
                } else {
                    dynbuf_cat(wb, "-ERR syntax error\r\n", 19);
                    return DISPATCH_ERROR;
                }
            }
            ret = engine_set_raw(args[0].ptr, (uint32_t)args[0].len,
                                 args[1].ptr, (uint32_t)args[1].len,
                                 expire, mode);
            if (ret < 0)       dynbuf_cat(wb, REP_ERROR, REP_ERROR_LEN);
            else if (ret == 0) dynbuf_cat(wb, REP_OK,    REP_OK_LEN);
            else               dynbuf_cat(wb, REP_NIL,   REP_NIL_LEN); /* NX/XX failed */
            return DISPATCH_OK;
        }
        if (TOKEN_EQ(cmd, "SETNX")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN);
                            return DISPATCH_ERROR; }
            ret = engine_set_raw(args[0].ptr, (uint32_t)args[0].len,
                                 args[1].ptr, (uint32_t)args[1].len, 0, 1);
            char rb[8]; int rl = snprintf(rb, sizeof(rb), ":%d\r\n", ret == 0 ? 1 : 0);
            dynbuf_cat(wb, rb, rl);
            return DISPATCH_OK;
        }
        if (TOKEN_EQ(cmd, "SETEX")) {
            if (argc < 3) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN);
                            return DISPATCH_ERROR; }
            long long s = slice_to_ll(args[1]);
            if (s <= 0) { dynbuf_cat(wb,
                "-ERR invalid expire time in 'setex' command\r\n", 45);
                return DISPATCH_ERROR; }
            ret = engine_set_raw(args[0].ptr, (uint32_t)args[0].len,
                                 args[2].ptr, (uint32_t)args[2].len,
                                 get_time_now() + s * 1000, 0);
            dynbuf_cat(wb, ret == 0 ? REP_OK : REP_ERROR,
                       ret == 0 ? REP_OK_LEN : REP_ERROR_LEN);
            return DISPATCH_OK;
        }
        if (TOKEN_EQ(cmd, "STRLEN")) {
            uint32_t vlen; uint8_t enc;
            const char *val = engine_get_raw(args[0].ptr, (uint32_t)args[0].len,
                                              &vlen, NULL, &enc);
            long long n = 0;
            if (val) {
                if (enc == KV_ENCODING_INT) {
                    int64_t iv; memcpy(&iv, val, sizeof(iv));
                    char ib[24]; n = snprintf(ib, sizeof(ib), "%lld", (long long)iv);
                } else {
                    n = (long long)vlen;
                }
            }
            char rb[24]; int rl = snprintf(rb, sizeof(rb), ":%lld\r\n", n);
            dynbuf_cat(wb, rb, rl);
            return DISPATCH_OK;
        }
        break;

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

    case 'M': /* MOD — legacy alias (Redis has no MOD). Behaves like SET. */
        if (!TOKEN_EQ(cmd, "MOD")) break;
        if (argc < 2) {
            dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN);
            return DISPATCH_ERROR;
        }
        {
            long long expire = 0;
            if (argc >= 3) {
                long long s = slice_to_ll(args[2]);
                if (s > 0) expire = get_time_now() + s * 1000;
            }
            ret = engine_set_raw(args[0].ptr, (uint32_t)args[0].len,
                                 args[1].ptr, (uint32_t)args[1].len, expire, 0);
            dynbuf_cat(wb, ret == 0 ? REP_OK : REP_ERROR,
                       ret == 0 ? REP_OK_LEN : REP_ERROR_LEN);
        }
        return DISPATCH_OK;

    case 'A': /* APPEND */
        if (!TOKEN_EQ(cmd, "APPEND")) break;
        if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN);
                        return DISPATCH_ERROR; }
        {
            uint32_t vlen = 0; uint8_t enc = KV_ENCODING_RAW;
            const char *val = engine_get_raw(args[0].ptr, (uint32_t)args[0].len,
                                              &vlen, NULL, &enc);
            char ibuf[24];
            const char *cur_p = val; uint32_t cur_l = vlen;
            if (val && enc == KV_ENCODING_INT) {
                int64_t iv; memcpy(&iv, val, sizeof(iv));
                cur_l = (uint32_t)snprintf(ibuf, sizeof(ibuf), "%lld", (long long)iv);
                cur_p = ibuf;
            }
            uint32_t new_len = cur_l + (uint32_t)args[1].len;
            char *combined = (char *)malloc((size_t)new_len);
            if (!combined) { dynbuf_cat(wb, REP_ERROR, REP_ERROR_LEN);
                             return DISPATCH_ERROR; }
            if (cur_l) memcpy(combined, cur_p, cur_l);
            memcpy(combined + cur_l, args[1].ptr, (size_t)args[1].len);
            ret = engine_set_raw(args[0].ptr, (uint32_t)args[0].len,
                                 combined, new_len, 0, 0);
            free(combined);
            if (ret < 0) { dynbuf_cat(wb, REP_ERROR, REP_ERROR_LEN);
                           return DISPATCH_ERROR; }
            char rb[24]; int rl = snprintf(rb, sizeof(rb), ":%u\r\n", new_len);
            dynbuf_cat(wb, rb, rl);
        }
        return DISPATCH_OK;

    case 'I': /* INCR / INCRBY */
    case 'D': /* DECR / DECRBY */
        if (TOKEN_EQ(cmd, "INCR")   || TOKEN_EQ(cmd, "DECR") ||
            TOKEN_EQ(cmd, "INCRBY") || TOKEN_EQ(cmd, "DECRBY")) {
            long long delta;
            if (TOKEN_EQ(cmd, "INCR"))      delta =  1;
            else if (TOKEN_EQ(cmd, "DECR")) delta = -1;
            else {
                if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN);
                                return DISPATCH_ERROR; }
                delta = slice_to_ll(args[1]);
                if (delta == -1 && !(args[1].len >= 2 && args[1].ptr[0]=='-'
                                     && args[1].ptr[1]=='1' && args[1].len == 2)) {
                    /* Distinguish slice_to_ll error from genuine "-1" */
                    if (args[1].len == 0 ||
                        (args[1].ptr[0] != '-' && args[1].ptr[0] != '+' &&
                         (args[1].ptr[0] < '0' || args[1].ptr[0] > '9'))) {
                        dynbuf_cat(wb,
                            "-ERR value is not an integer or out of range\r\n", 46);
                        return DISPATCH_ERROR;
                    }
                }
                if (TOKEN_EQ(cmd, "DECRBY")) delta = -delta;
            }

            uint32_t vlen = 0; uint8_t enc = KV_ENCODING_RAW;
            const char *val = engine_get_raw(args[0].ptr, (uint32_t)args[0].len,
                                              &vlen, NULL, &enc);
            long long cur = 0;
            if (val) {
                if (enc == KV_ENCODING_INT) {
                    int64_t iv; memcpy(&iv, val, sizeof(iv)); cur = iv;
                } else {
                    int64_t iv;
                    if (!try_parse_int64(val, (int)vlen, &iv)) {
                        dynbuf_cat(wb,
                            "-ERR value is not an integer or out of range\r\n", 46);
                        return DISPATCH_ERROR;
                    }
                    cur = iv;
                }
            }
            /* Overflow check */
            if ((delta > 0 && cur > 9223372036854775807LL - delta) ||
                (delta < 0 && cur < -9223372036854775807LL - 1 - delta)) {
                dynbuf_cat(wb, "-ERR increment or decrement would overflow\r\n", 44);
                return DISPATCH_ERROR;
            }
            cur += delta;
            char nb[24]; int nl = snprintf(nb, sizeof(nb), "%lld", cur);
            ret = engine_set_raw(args[0].ptr, (uint32_t)args[0].len,
                                 nb, (uint32_t)nl, 0, 0);
            if (ret < 0) { dynbuf_cat(wb, REP_ERROR, REP_ERROR_LEN);
                           return DISPATCH_ERROR; }
            char rb[32]; int rl = snprintf(rb, sizeof(rb), ":%lld\r\n", cur);
            dynbuf_cat(wb, rb, rl);
            return DISPATCH_OK;
        }
        if (TOKEN_EQ(cmd, "INCRBYFLOAT")) {
            if (argc < 2) { dynbuf_cat(wb, REP_ERR_ARGS, REP_ERR_ARGS_LEN);
                            return DISPATCH_ERROR; }
            /* Parse increment as double. */
            char ibuf[64];
            int in = args[1].len < 63 ? args[1].len : 63;
            memcpy(ibuf, args[1].ptr, (size_t)in); ibuf[in] = '\0';
            char *iend = NULL;
            double incr = strtod(ibuf, &iend);
            if (iend == ibuf || (*iend != '\0' && *iend != '\r' && *iend != '\n')) {
                dynbuf_cat(wb, "-ERR value is not a valid float\r\n", 34);
                return DISPATCH_ERROR;
            }
            /* Load current value; parse as double (int encoding OK too). */
            uint32_t vlen = 0; uint8_t enc = KV_ENCODING_RAW;
            const char *val = engine_get_raw(args[0].ptr, (uint32_t)args[0].len,
                                              &vlen, NULL, &enc);
            double cur = 0.0;
            if (val) {
                if (enc == KV_ENCODING_INT) {
                    int64_t iv; memcpy(&iv, val, sizeof(iv)); cur = (double)iv;
                } else {
                    char vbuf[64];
                    int vn = vlen < 63 ? (int)vlen : 63;
                    memcpy(vbuf, val, (size_t)vn); vbuf[vn] = '\0';
                    char *vend = NULL;
                    cur = strtod(vbuf, &vend);
                    if (vend == vbuf || (*vend != '\0' && *vend != '\r' && *vend != '\n')) {
                        dynbuf_cat(wb, "-ERR value is not a valid float\r\n", 34);
                        return DISPATCH_ERROR;
                    }
                }
            }
            double res = cur + incr;
            /* Format without trailing zeros, mimicking Redis ("%.17Lg"-ish). */
            char nb[64];
            int nl = snprintf(nb, sizeof(nb), "%.17g", res);
            /* Trim trailing zeros after decimal point, like Redis. */
            if (strchr(nb, '.') && !strchr(nb, 'e') && !strchr(nb, 'E')) {
                while (nl > 0 && nb[nl-1] == '0') nl--;
                if (nl > 0 && nb[nl-1] == '.') nl--;
                nb[nl] = '\0';
            }
            ret = engine_set_raw(args[0].ptr, (uint32_t)args[0].len,
                                 nb, (uint32_t)nl, 0, 0);
            if (ret < 0) { dynbuf_cat(wb, REP_ERROR, REP_ERROR_LEN);
                           return DISPATCH_ERROR; }
            char rb[96];
            int rl = snprintf(rb, sizeof(rb), "$%d\r\n%s\r\n", nl, nb);
            dynbuf_cat(wb, rb, rl);
            return DISPATCH_OK;
        }
        break;
    }

    dynbuf_cat(wb, REP_ERR_UNK, REP_ERR_UNK_LEN);
    return DISPATCH_ERROR;
}
