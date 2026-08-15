/* kvstore_dispatch.c — RESP command dispatch */

#include "kvstore_internal.h"
#include "rdma_repl.h"

#include <stdio.h>
#include <string.h>
#include <ctype.h>

/* ── Fast inline write-buffer helpers ───────────────────────────────────
 * Bypass dynbuf_cat/dynbuf_reserve overhead on the hot path.
 * The write buffer starts at 64KB, so small fixed-length responses
 * (RESP_OK, RESP_NIL, RESP_PONG, etc.) never trigger growth. */

static inline void wb_write(DynBuf *wb, const char *data, int len) {
    if (wb->len + len > wb->cap)
        dynbuf_reserve(wb, wb->len + len + 4096);
    memcpy(wb->buf + wb->len, data, (size_t)len);
    wb->len += len;
}

static inline void wb_ensure(DynBuf *wb, int need) {
    if (wb->len + need > wb->cap)
        dynbuf_reserve(wb, wb->len + need + 4096);
}


static inline kv_entry_t *dispatch_resolve(kvs_token_t key_tok,
                                            uint64_t *hash_out, int is_write) {
    (void)is_write;
    uint64_t hash = kv_hash((const uint8_t *)key_tok.ptr, (uint32_t)key_tok.len);
    *hash_out = hash;
    return dict_find(&g_dict, key_tok.ptr,
                     (uint32_t)key_tok.len, hash);
}

/* ── Inline GET/SET macros (hottest commands, 60-80% workload) ──────── */

/* Fast uint-to-buffer: writes decimal representation, returns length */
static inline int u2a(char *dst, uint32_t v) {
    if (v == 0) { dst[0] = '0'; return 1; }
    char tmp[12], *p = tmp;
    while (v) { *p++ = (char)('0' + v % 10); v /= 10; }
    int n = (int)(p - tmp);
    for (int i = 0; i < n; i++) dst[i] = tmp[n-1-i];
    return n;
}

/* Inline GET — single reserve + manual formatting, zero snprintf */
#define INLINE_GET_COMMAND(entry, hash, key_tok, wb) do { \
    if (!(entry)) { \
        wb_write((wb), RESP_NIL, RESP_NIL_LEN); \
    } else { \
        uint32_t vlen = 0; \
        uint8_t enc = KV_ENCODING_RAW; \
        const char *val = string_engine_get_pre((entry), &vlen, &enc); \
        if (enc == KV_ENCODING_INT) { \
            int64_t iv; memcpy(&iv, val, sizeof(int64_t)); \
            char buf[64]; \
            int n = snprintf(buf, sizeof(buf), "$%lld\r\n%lld\r\n", \
                             (long long)snprintf(NULL, 0, "%lld", (long long)iv), \
                             (long long)iv); \
            wb_write((wb), buf, n); \
        } else { \
            int need = 24 + (int)vlen; \
            wb_ensure((wb), need); \
            char *dst = (wb)->buf + (wb)->len; \
            *dst++ = '$'; \
            int hl = u2a(dst, vlen); dst += hl; \
            *dst++ = '\r'; *dst++ = '\n'; \
            memcpy(dst, val, (size_t)vlen); dst += vlen; \
            *dst++ = '\r'; *dst++ = '\n'; \
            (wb)->len = (int)(dst - (wb)->buf); \
        } \
    } \
} while (0)

/* Inline SET — write OK response directly */
#define INLINE_SET_COMMAND(entry, hash, tokens, argc, wb, msg_raw, msg_len) do { \
    if ((argc) == 3) { \
        kvs_token_t key_tok = (tokens)[1]; \
        kvs_token_t val_tok = (tokens)[2]; \
        string_engine_set_pre(key_tok.ptr, (uint32_t)key_tok.len, \
                              val_tok.ptr, (uint32_t)val_tok.len, \
                              0, 0, (hash), (entry)); \
        wb_write((wb), RESP_OK, RESP_OK_LEN); \
        if (!g_in_reload) { \
            kvs_write_journal_append((msg_raw), (msg_len)); \
            kvs_repl_incr_hook((msg_raw), (msg_len)); \
        } \
    } else { \
        string_cmd((tokens)[0], (entry), (hash), (tokens) + 1, (argc) - 1, \
                   (wb), (msg_raw), (msg_len)); \
    } \
} while (0)


#include "kv_type_string.h"
#include "persistence.h"

dispatch_result_t kvs_dispatch(kvs_token_t *tokens, int argc,
                               const char *msg_raw, int msg_len,
                               DynBuf *wb, struct conn *c)
{
    if (tokens == NULL || argc <= 0 || wb == NULL)
        return DISPATCH_ERROR;

    atomic_fetch_add_explicit(&g_kvs_total_commands, 1, memory_order_relaxed);

    kvs_token_t cmd = tokens[0];

    /* Case-insensitive command: uppercase into stack buffer when needed.
     * Fast scan: if already uppercase (common case), skip copy entirely. */
    char ucmd_buf[32];
    int  ulen = cmd.len;
    if (ulen > (int)sizeof(ucmd_buf)) ulen = (int)sizeof(ucmd_buf);
    int has_lower = 0;
    for (int i = 0; i < ulen; i++) {
        unsigned char ch = (unsigned char)cmd.ptr[i];
        if (ch >= 'a' && ch <= 'z') { has_lower = 1; break; }
    }
    if (has_lower) {
        for (int i = 0; i < ulen; i++) {
            unsigned char ch = (unsigned char)cmd.ptr[i];
            ucmd_buf[i] = (ch >= 'a' && ch <= 'z') ? (char)(ch & 0xDF)
                                                    : (char)ch;
        }
        cmd.ptr   = ucmd_buf;
        cmd.len   = ulen;
        tokens[0] = cmd;
    }

    /* ── Global / connection commands (no key needed) ── */

    if (TOKEN_EQ(cmd, "PING")) {
        if (argc == 1)
            dynbuf_cat(wb, RESP_PONG, RESP_PONG_LEN);
        else if (argc == 2)
            reply_bulk(wb, tokens[1].ptr, tokens[1].len);
        else
            dynbuf_lit(wb, "-ERR wrong number of arguments for 'ping' command\r\n");
        return DISPATCH_OK;
    }

    /* FULLSYNC — Slave 请求全量同步（Master fork RDMA 子进程） */
    if (TOKEN_EQ(cmd, "FULLSYNC")) {
        int ret = rdma_repl_master_trigger_sync();
        if (ret == 0)
            dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        else
            dynbuf_lit(wb, "-ERR fullsync trigger failed\r\n");
        return DISPATCH_OK;
    }

    /* Inline GET/SET — hottest commands (60-80% workload). */
    if (cmd.len == 3 && cmd.ptr[0] == 'G' && cmd.ptr[1] == 'E' && cmd.ptr[2] == 'T') {
        if (argc >= 2) {
            kvs_token_t key_tok = tokens[1];
            uint64_t hash;
            kv_entry_t *entry = dispatch_resolve(key_tok, &hash, 0);
            INLINE_GET_COMMAND(entry, hash, key_tok, wb);
            return DISPATCH_OK;
        }
    }

    if (cmd.len == 3 && cmd.ptr[0] == 'S' && cmd.ptr[1] == 'E' && cmd.ptr[2] == 'T') {
        if (argc >= 2) {
            kvs_token_t key_tok = tokens[1];
            uint64_t hash;
            kv_entry_t *entry = dispatch_resolve(key_tok, &hash, 1);
            INLINE_SET_COMMAND(entry, hash, tokens, argc, wb, msg_raw, msg_len);
            return DISPATCH_OK;
        }
    }

    if (TOKEN_EQ(cmd, "QUIT")) {
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        return DISPATCH_CLOSE;
    }

    if (TOKEN_EQ(cmd, "ECHO")) {
        if (argc >= 2)
            reply_bulk(wb, tokens[1].ptr, tokens[1].len);
        else
            dynbuf_lit(wb, "-ERR wrong number of arguments for 'echo' command\r\n");
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "DBSIZE")) {
        long long count = (long long)dict_count(&g_dict);
        reply_integer(wb, count);
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "SAVE") || TOKEN_EQ(cmd, "BGSAVE")) {
        rdb_save_async_uring();
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "BGREWRITEAOF")) {
        int rc = kvs_aof_rewrite_bg();
        if (rc == 0)
            dynbuf_lit(wb, "+Background append only file rewriting started\r\n");
        else
            dynbuf_lit(wb, "-ERR BGREWRITEAOF already in progress\r\n");
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "CONFIG"))
        return handle_config(tokens, argc, wb);

    if (TOKEN_EQ(cmd, "INFO"))
        return handle_info(tokens, argc, wb);

    if (TOKEN_EQ(cmd, "FLUSHALL") || TOKEN_EQ(cmd, "FLUSHDB")) {
        kv_iter_t *iter = dict_iter_new(&g_dict, 1);
        kv_entry_t *e;
        while ((e = dict_iter_next(iter)) != NULL)
            entry_cleanup_by_type(e);
        dict_iter_free(iter);
        dict_clear(&g_dict);
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        if (!g_in_reload) {
            kvs_write_journal_append(msg_raw, msg_len);
            kvs_repl_incr_hook(msg_raw, msg_len);
        }
        return DISPATCH_OK;
    }

    /* ── Multi-key commands ── */

    if (TOKEN_EQ(cmd, "MGET")) {
        if (argc < 2) {
            dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
            return DISPATCH_ERROR;
        }
        char hdr[32];
        int hl = snprintf(hdr, sizeof(hdr), "*%d\r\n", argc - 1);
        dynbuf_cat(wb, hdr, hl);
        for (int i = 1; i < argc; i++) {
            uint64_t kh = kv_hash((const uint8_t *)tokens[i].ptr,
                                  (uint32_t)tokens[i].len);
            kv_entry_t *ke = dict_find(&g_dict, tokens[i].ptr,
                                       (uint32_t)tokens[i].len, kh);
            uint32_t vlen = 0; uint8_t enc = KV_ENCODING_RAW;
            if (!ke) {
                dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);
                continue;
            }
            const char *val = string_engine_get_pre(ke, &vlen, &enc);
            if (enc == KV_ENCODING_INT) {
                int64_t iv; memcpy(&iv, val, sizeof(int64_t));
                char ibuf[24];
                int il = snprintf(ibuf, sizeof(ibuf), "%lld", (long long)iv);
                int bh = snprintf(hdr, sizeof(hdr), "$%d\r\n", il);
                dynbuf_cat(wb, hdr, bh); dynbuf_cat(wb, ibuf, il);
                dynbuf_lit(wb, "\r\n");
            } else {
                int bh = snprintf(hdr, sizeof(hdr), "$%u\r\n", vlen);
                dynbuf_cat(wb, hdr, bh);
                dynbuf_cat(wb, val, (int)vlen);
                dynbuf_lit(wb, "\r\n");
            }
        }
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "MSET")) {
        if (argc < 3 || ((argc - 1) % 2) != 0) {
            dynbuf_lit(wb, "-ERR wrong number of arguments for 'mset' command\r\n");
            return DISPATCH_ERROR;
        }
        for (int i = 1; i + 1 < argc; i += 2) {
            uint64_t kh = kv_hash((const uint8_t *)tokens[i].ptr,
                                  (uint32_t)tokens[i].len);
            kv_entry_t *ke = dict_find(&g_dict, tokens[i].ptr,
                                       (uint32_t)tokens[i].len, kh);
            string_engine_set_pre(tokens[i].ptr, (uint32_t)tokens[i].len,
                                  tokens[i+1].ptr, (uint32_t)tokens[i+1].len,
                                  0, 0, kh, ke);
        }
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        if (!g_in_reload) {
            kvs_write_journal_append(msg_raw, msg_len);
            kvs_repl_incr_hook(msg_raw, msg_len);
        }
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "DEL") || TOKEN_EQ(cmd, "UNLINK")) {
        if (argc < 2) {
            dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
            return DISPATCH_ERROR;
        }
        long long ndel = 0;
        for (int i = 1; i < argc; i++) {
            kvs_token_t kt = tokens[i];
            uint64_t kh = kv_hash((const uint8_t *)kt.ptr, (uint32_t)kt.len);
            kv_entry_t *ke = dict_find(&g_dict, kt.ptr,
                                       (uint32_t)kt.len, kh);
            if (!ke) continue;
            entry_cleanup_by_type(ke);
            if (dict_delete(&g_dict, kt.ptr, (uint32_t)kt.len, kh) == 0)
                ndel++;
        }
        reply_integer(wb, ndel);
        if (ndel > 0 && !g_in_reload) {
            kvs_write_journal_append(msg_raw, msg_len);
            kvs_repl_incr_hook(msg_raw, msg_len);
        }
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "EXISTS") || TOKEN_EQ(cmd, "EXIST")) {
        if (argc < 2) {
            dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
            return DISPATCH_ERROR;
        }
        long long nfound = 0;
        for (int i = 1; i < argc; i++) {
            kvs_token_t kt = tokens[i];
            uint64_t kh = kv_hash((const uint8_t *)kt.ptr, (uint32_t)kt.len);
            kv_entry_t *ke = dict_find(&g_dict, kt.ptr,
                                       (uint32_t)kt.len, kh);
            if (!ke) continue;
            nfound++;
        }
        reply_integer(wb, nfound);
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "KEYS")) {
        kv_iter_t *iter = dict_iter_new(&g_dict, 1);
        kv_entry_t *ke;
        long long nkeys = 0;
        while ((ke = dict_iter_next(iter)) != NULL) {
            nkeys++;
        }
        dict_iter_free(iter);
        char hdr[32];
        int hl = snprintf(hdr, sizeof(hdr), "*%lld\r\n", nkeys);
        dynbuf_cat(wb, hdr, hl);
        iter = dict_iter_new(&g_dict, 1);
        while ((ke = dict_iter_next(iter)) != NULL) {
            reply_bulk(wb, entry_key(ke), ke->key_len);
        }
        dict_iter_free(iter);
        return DISPATCH_OK;
    }

    /* ── Single-key commands ── */
single_key:
    if (argc < 2) {
        dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
        return DISPATCH_ERROR;
    }

    kvs_token_t key_tok = tokens[1];
    uint64_t hash = kv_hash((const uint8_t *)key_tok.ptr, (uint32_t)key_tok.len);

    int cmd_type = -1;
    int is_write = 0;
    if (TOKEN_EQ(cmd, "TYPE") || TOKEN_EQ(cmd, "TTL") || TOKEN_EQ(cmd, "PTTL")) {
        cmd_type = -2; is_write = 0;
    } else if (TOKEN_EQ(cmd, "EXPIRE") || TOKEN_EQ(cmd, "PERSIST")) {
        cmd_type = -2; is_write = 1;
    } else if (TOKEN_EQ(cmd, "SET") || TOKEN_EQ(cmd, "INCR") ||
               TOKEN_EQ(cmd, "INCRBY") || TOKEN_EQ(cmd, "DECR") ||
               TOKEN_EQ(cmd, "DECRBY")) {
        cmd_type = KV_TYPE_STRING; is_write = 1;
    } else if (TOKEN_EQ(cmd, "GET") || TOKEN_EQ(cmd, "STRLEN")) {
        cmd_type = KV_TYPE_STRING; is_write = 0;
    } else {
        dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
        return DISPATCH_ERROR;
    }

    kv_entry_t *entry = dispatch_resolve(key_tok, &hash, is_write);

    int pre_len = wb->len;

    if (TOKEN_EQ(cmd, "TYPE")) {
        const char *r = "+none\r\n"; int rlen = 7;
        if (entry) {
            r = "+string\r\n"; rlen = 9;
        }
        dynbuf_cat(wb, r, rlen);
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "EXPIRE")) {
        if (argc < 3) {
            dynbuf_lit(wb, "-ERR wrong number of arguments for 'expire' command\r\n");
            return DISPATCH_ERROR;
        }
        long long secs;
        if (token_to_ll(tokens[2], &secs) < 0) {
            dynbuf_lit(wb, "-ERR value is not an integer or out of range\r\n");
            return DISPATCH_ERROR;
        }
        if (!entry) { reply_integer(wb, 0); return DISPATCH_OK; }
        entry->expire = get_time_now() + secs * 1000;
        reply_integer(wb, 1);
        if (!g_in_reload) {
            kvs_write_journal_append(msg_raw, msg_len);
            kvs_repl_incr_hook(msg_raw, msg_len);
        }
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "TTL") || TOKEN_EQ(cmd, "PTTL")) {
        if (!entry) { reply_integer(wb, -2); return DISPATCH_OK; }
        if (!entry->expire) { reply_integer(wb, -1); return DISPATCH_OK; }
        long long rem_ms = entry->expire - get_time_now();
        if (rem_ms < 0) { reply_integer(wb, -2); return DISPATCH_OK; }
        reply_integer(wb, TOKEN_EQ(cmd, "TTL") ? (rem_ms + 500) / 1000 : rem_ms);
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "PERSIST")) {
        if (!entry || !entry->expire) {
            reply_integer(wb, 0); return DISPATCH_OK;
        }
        entry->expire = 0;
        reply_integer(wb, 1);
        if (!g_in_reload) {
            kvs_write_journal_append(msg_raw, msg_len);
            kvs_repl_incr_hook(msg_raw, msg_len);
        }
        return DISPATCH_OK;
    }

    if (cmd_type == KV_TYPE_STRING)
        string_cmd(cmd, entry, hash, tokens + 1, argc - 1, wb, msg_raw, msg_len);

    /* Write-command post-processing */
    if (is_write) {
        const char *resp = wb->buf + pre_len;
        int rlen = wb->len - pre_len;
        int ok = (rlen > 0 && resp[0] != '-' &&
                  !(rlen >= 5 && memcmp(resp, "$-1\r\n", 5) == 0));
        if (ok && !g_in_reload) {
            kvs_write_journal_append(msg_raw, msg_len);
            kvs_repl_incr_hook(msg_raw, msg_len);
        }
    }

    return DISPATCH_OK;
}
