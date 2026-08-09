#include "kv_type_string.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* =====================================================================
 * Engine access — all through the global dict.
 * Key is passed as (ptr, len), no null-termination required.
 * ===================================================================== */

extern long long get_time_now(void);

/* =====================================================================
 * Core engine ops (raw: no KVItem allocations)
 * ===================================================================== */

/* Phase 6.1 — engine_set with pre-found entry + pre-computed hash.
 *
 * The dispatcher (kvs_dispatch) handles locking, computes the key hash,
 * calls dict_find once, and clears expired entries.  When string_cmd calls
 * this, `existing` is either NULL or a definitely-non-expired STRING entry
 * (other types are rejected upstream by WRONGTYPE).
 *
 * Saves: 1 × kv_hash, 1 × dict_find, 1 × expire check per SET-family cmd.
 *
 * mode = 0 unconditional, 1 NX, 2 XX.  Returns 0 ok, 1 cond-fail, -1 err. */
static int engine_set_pre(const char *key, uint32_t key_len,
                          const char *val, uint32_t val_len,
                          long long expire, int mode,
                          uint64_t hash, kv_entry_t *existing)
{
    if (mode == 1 && existing) return 1;   /* NX failed */
    if (mode == 2 && !existing) return 1;  /* XX failed */

    int64_t iv;
    uint8_t enc;
    const char *pload;
    uint32_t plen;
    if (kvs_parse_int64(val, (int)val_len, &iv)) {
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
        if (existing->val_len == nd) {
            existing->data[0] = (char)KV_TYPE_STRING;
            existing->data[1] = (char)enc;
            memcpy(entry_payload(existing), pload, plen);
            existing->expire = expire;
            return 0;
        }
        kv_entry_t *e = dict_resize_entry(&g_dict, existing, nd);
        if (!e) return -1;
        e->data[0] = (char)KV_TYPE_STRING;
        e->data[1] = (char)enc;
        memcpy(entry_payload(e), pload, plen);
        e->expire = expire;
        return 0;
    }

    kv_entry_t *e = dict_add_raw(&g_dict, key, key_len,
                                 KV_TYPE_STRING, enc,
                                 pload, plen, hash);
    if (!e) return -1;
    if (expire) e->expire = expire;
    return 0;
}

/* Backwards-compat wrapper for MGET/MSET/MSETNX batch paths in kvstore.c
 * where the entry hasn't been pre-fetched (each key has its own loop). */

/* Phase 6.1 — engine_get with pre-found entry.  Direct field access; no
 * dict_find, no expire check (dispatcher already cleared expired keys). */
static inline const char *engine_get_pre(kv_entry_t *e,
                                         uint32_t *vlen_out, long long *expire_out,
                                         uint8_t *enc_out)
{
    if (!e) return NULL;
    if (vlen_out)   *vlen_out   = entry_payload_len(e);
    if (expire_out) *expire_out = e->expire;
    if (enc_out)    *enc_out    = entry_encoding(e);
    return entry_payload(e);
}


/* Pre-computed hash variant — skip redundant kv_hash + dict_find */
int string_engine_set_pre(const char *key, uint32_t key_len,
                          const char *val, uint32_t val_len,
                          long long expire, int mode,
                          uint64_t hash, kv_entry_t *existing)
{
    return engine_set_pre(key, key_len, val, val_len, expire, mode, hash, existing);
}

const char *string_engine_get_pre(kv_entry_t *entry,
                                  uint32_t *vlen_out,
                                  uint8_t *enc_out)
{
    return engine_get_pre(entry, vlen_out, NULL, enc_out);
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
                             uint64_t hash,
                             kvs_token_t *args, int argc, DynBuf *wb,
                             const char *msg_raw, int msg_len)
{
    (void)msg_raw;
    (void)msg_len;
    /* Phase 6.1 — `entry` and `hash` come pre-computed from kvs_dispatch.
     * Every path below uses the engine_*_pre variants, eliminating the
     * second kv_hash + dict_find that engine_*_raw used to perform. */

    char temp[64];
    int  ret;

    switch (cmd.ptr[0]) {

    case 'S': /* SET / STRLEN */
        if (TOKEN_EQ(cmd, "SET")) {
            if (argc < 2) {
                dynbuf_cat(wb, RESP_ERR_ARGS, RESP_ERR_ARGS_LEN);
                return DISPATCH_ERROR;
            }
            /* Parse Redis SET options: [EX seconds | PX ms] [NX | XX] */
            long long expire = 0;
            int mode = 0;       /* 0=default, 1=NX, 2=XX */
            for (int i = 2; i < argc; i++) {
                kvs_token_t opt = args[i];
                if ((opt.len == 2 && (opt.ptr[0]=='E'||opt.ptr[0]=='e') &&
                                      (opt.ptr[1]=='X'||opt.ptr[1]=='x'))) {
                    if (i + 1 >= argc) { dynbuf_lit(wb, "-ERR syntax error\r\n"); return DISPATCH_ERROR; }
                    long long s = slice_to_ll(args[++i]);
                    if (s <= 0) { dynbuf_lit(wb, "-ERR invalid expire time in 'set' command\r\n");
                        return DISPATCH_ERROR; }
                    expire = get_time_now() + s * 1000;
                } else if (opt.len == 2 && (opt.ptr[0]=='P'||opt.ptr[0]=='p') &&
                                            (opt.ptr[1]=='X'||opt.ptr[1]=='x')) {
                    if (i + 1 >= argc) { dynbuf_lit(wb, "-ERR syntax error\r\n"); return DISPATCH_ERROR; }
                    long long ms = slice_to_ll(args[++i]);
                    if (ms <= 0) { dynbuf_lit(wb, "-ERR invalid expire time in 'set' command\r\n");
                        return DISPATCH_ERROR; }
                    expire = get_time_now() + ms;
                } else if (opt.len == 2 && (opt.ptr[0]=='N'||opt.ptr[0]=='n') &&
                                            (opt.ptr[1]=='X'||opt.ptr[1]=='x')) {
                    mode = 1;
                } else if (opt.len == 2 && (opt.ptr[0]=='X'||opt.ptr[0]=='x') &&
                                            (opt.ptr[1]=='X'||opt.ptr[1]=='x')) {
                    mode = 2;
                } else {
                    dynbuf_lit(wb, "-ERR syntax error\r\n");
                    return DISPATCH_ERROR;
                }
            }
            ret = engine_set_pre(args[0].ptr, (uint32_t)args[0].len,
                                 args[1].ptr, (uint32_t)args[1].len,
                                 expire, mode, hash, entry);
            if (ret < 0)       dynbuf_cat(wb, RESP_ERROR, RESP_ERROR_LEN);
            else if (ret == 0) dynbuf_cat(wb, RESP_OK,    RESP_OK_LEN);
            else               dynbuf_cat(wb, RESP_NIL,   RESP_NIL_LEN); /* NX/XX failed */
            return DISPATCH_OK;
        }
        if (TOKEN_EQ(cmd, "STRLEN")) {
            uint32_t vlen; uint8_t enc;
            const char *val = engine_get_pre(entry, &vlen, NULL, &enc);
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
            const char *val = engine_get_pre(entry, &vlen, NULL, &enc);
            if (!val) {
                dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);
            } else if (enc == KV_ENCODING_INT) {
                int64_t iv;
                memcpy(&iv, val, sizeof(iv));
                char istr[24];
                int il = snprintf(istr, sizeof(istr), "%lld", (long long)iv);
                int hl = snprintf(temp, sizeof(temp), "$%d\r\n", il);
                dynbuf_cat(wb, temp, hl);
                dynbuf_cat(wb, istr, il);
                dynbuf_lit(wb, "\r\n");
            } else {
                int hl = snprintf(temp, sizeof(temp), "$%u\r\n", vlen);
                dynbuf_cat(wb, temp, hl);
                dynbuf_cat(wb, val, (int)vlen);
                dynbuf_lit(wb, "\r\n");
            }
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
                if (argc < 2) { dynbuf_cat(wb, RESP_ERR_ARGS, RESP_ERR_ARGS_LEN);
                                return DISPATCH_ERROR; }
                delta = slice_to_ll(args[1]);
                if (delta == -1 && !(args[1].len >= 2 && args[1].ptr[0]=='-'
                                     && args[1].ptr[1]=='1' && args[1].len == 2)) {
                    /* Distinguish slice_to_ll error from genuine "-1" */
                    if (args[1].len == 0 ||
                        (args[1].ptr[0] != '-' && args[1].ptr[0] != '+' &&
                         (args[1].ptr[0] < '0' || args[1].ptr[0] > '9'))) {
                        dynbuf_lit(wb, "-ERR value is not an integer or out of range\r\n");
                        return DISPATCH_ERROR;
                    }
                }
                if (TOKEN_EQ(cmd, "DECRBY")) delta = -delta;
            }

            uint32_t vlen = 0; uint8_t enc = KV_ENCODING_RAW;
            const char *val = engine_get_pre(entry, &vlen, NULL, &enc);
            long long cur = 0;
            if (val) {
                if (enc == KV_ENCODING_INT) {
                    int64_t iv; memcpy(&iv, val, sizeof(iv)); cur = iv;
                } else {
                    int64_t iv;
                    if (!kvs_parse_int64(val, (int)vlen, &iv)) {
                        dynbuf_lit(wb, "-ERR value is not an integer or out of range\r\n");
                        return DISPATCH_ERROR;
                    }
                    cur = iv;
                }
            }
            /* Overflow check */
            if ((delta > 0 && cur > 9223372036854775807LL - delta) ||
                (delta < 0 && cur < -9223372036854775807LL - 1 - delta)) {
                dynbuf_lit(wb, "-ERR increment or decrement would overflow\r\n");
                return DISPATCH_ERROR;
            }
            cur += delta;
            char nb[24]; int nl = snprintf(nb, sizeof(nb), "%lld", cur);
            ret = engine_set_pre(args[0].ptr, (uint32_t)args[0].len,
                                 nb, (uint32_t)nl, 0, 0, hash, entry);
            if (ret < 0) { dynbuf_cat(wb, RESP_ERROR, RESP_ERROR_LEN);
                           return DISPATCH_ERROR; }
            char rb[32]; int rl = snprintf(rb, sizeof(rb), ":%lld\r\n", cur);
            dynbuf_cat(wb, rb, rl);
            return DISPATCH_OK;
        }
        break;
    }

    dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
    return DISPATCH_ERROR;
}
