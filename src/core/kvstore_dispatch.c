/* kvstore_dispatch.c — RESP command dispatch */

#include "kvstore_internal.h"

#include <stdio.h>
#include <string.h>
#include <ctype.h>

/* ── Inline GET/SET macros (hottest commands, 60-80% workload) ──────── */

#define INLINE_GET_COMMAND(entry, hash, key_tok, wb) do { \
    if (!(entry)) { \
        dynbuf_cat((wb), RESP_NIL, RESP_NIL_LEN); \
    } else { \
        uint32_t vlen = 0; \
        uint8_t enc = KV_ENCODING_RAW; \
        const char *val = string_engine_get_pre((entry), &vlen, &enc); \
        if (enc == KV_ENCODING_INT) { \
            int64_t iv; \
            memcpy(&iv, val, sizeof(int64_t)); \
            char ibuf[24]; \
            int il = snprintf(ibuf, sizeof(ibuf), "%lld", (long long)iv); \
            char hdr[24]; \
            int bh = snprintf(hdr, sizeof(hdr), "$%d\r\n", il); \
            dynbuf_cat((wb), hdr, bh); \
            dynbuf_cat((wb), ibuf, il); \
            dynbuf_lit((wb), "\r\n"); \
        } else { \
            char hdr[24]; \
            int bh = snprintf(hdr, sizeof(hdr), "$%u\r\n", vlen); \
            dynbuf_cat((wb), hdr, bh); \
            dynbuf_cat((wb), val, (int)vlen); \
            dynbuf_lit((wb), "\r\n"); \
        } \
    } \
} while (0)

#define INLINE_SET_COMMAND(entry, hash, tokens, argc, wb, msg_raw, msg_len) do { \
    if ((argc) == 3) { \
        kvs_token_t key_tok = (tokens)[1]; \
        kvs_token_t val_tok = (tokens)[2]; \
        string_engine_set_pre(key_tok.ptr, (uint32_t)key_tok.len, \
                              val_tok.ptr, (uint32_t)val_tok.len, \
                              0, 0, (hash), (entry)); \
        dynbuf_cat((wb), RESP_OK, RESP_OK_LEN); \
        if (!g_in_reload) { \
            kvs_write_journal_append((msg_raw), (msg_len)); \
        } \
    } else { \
        string_cmd((tokens)[0], (entry), (hash), (tokens) + 1, (argc) - 1, \
                   (wb), (msg_raw), (msg_len)); \
    } \
} while (0)

extern uint32_t kv_store_count(void);

#include "kv_type_string.h"
#include "kv_type_hash.h"
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

    /* Inline GET/SET — hottest commands (60-80% workload). */
    if (cmd.len == 3 && cmd.ptr[0] == 'G' && cmd.ptr[1] == 'E' && cmd.ptr[2] == 'T') {
        if (argc >= 2) {
            kvs_token_t key_tok = tokens[1];
            uint64_t hash = kv_hash((const uint8_t *)key_tok.ptr, (uint32_t)key_tok.len);
            kv_store_rdlock();
            kv_entry_t *entry = dict_find(kv_dict_for(hash), key_tok.ptr,
                                          (uint32_t)key_tok.len, hash);
            if (entry && entry->expire && get_time_now() > entry->expire) {
                enqueue_expired_key(key_tok.ptr, (uint32_t)key_tok.len, hash);
                entry = NULL;
            }
            if (entry && (int)entry_type(entry) != KV_TYPE_STRING) {
                dynbuf_cat(wb, RESP_WRONGTYPE, RESP_WRONGTYPE_LEN);
                kv_store_unlock();
                return DISPATCH_ERROR;
            }
            INLINE_GET_COMMAND(entry, hash, key_tok, wb);
            kv_store_unlock();
            return DISPATCH_OK;
        }
    }

    if (cmd.len == 3 && cmd.ptr[0] == 'S' && cmd.ptr[1] == 'E' && cmd.ptr[2] == 'T') {
        if (argc >= 2) {
            kvs_token_t key_tok = tokens[1];
            uint64_t hash = kv_hash((const uint8_t *)key_tok.ptr, (uint32_t)key_tok.len);
            kv_store_wrlock();
            kv_entry_t *entry = dict_find(kv_dict_for(hash), key_tok.ptr,
                                          (uint32_t)key_tok.len, hash);
            if (entry && entry->expire && get_time_now() > entry->expire) {
                entry_cleanup_by_type(entry);
                dict_delete(kv_dict_for(hash), key_tok.ptr, (uint32_t)key_tok.len, hash);
                atomic_fetch_add_explicit(
                    kvs_expired_keys_ptr(), 1, memory_order_relaxed);
                entry = NULL;
            }
            if (entry && (int)entry_type(entry) != KV_TYPE_STRING) {
                dynbuf_cat(wb, RESP_WRONGTYPE, RESP_WRONGTYPE_LEN);
                kv_store_unlock();
                return DISPATCH_ERROR;
            }
            INLINE_SET_COMMAND(entry, hash, tokens, argc, wb, msg_raw, msg_len);
            flush_expire_queue();
            kv_store_unlock();
            return DISPATCH_OK;
        }
    }

    switch (cmd.ptr[0]) {
    case 'H':
        if (cmd.len == 4 && cmd.ptr[1] == 'S' && cmd.ptr[2] == 'E' && cmd.ptr[3] == 'T')
            goto single_key;
        if (cmd.len == 4 && cmd.ptr[1] == 'G' && cmd.ptr[2] == 'E' && cmd.ptr[3] == 'T')
            goto single_key;
        break;
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

    if (TOKEN_EQ(cmd, "HELLO")) {
        dynbuf_lit(wb, "-NOPROTO sorry, this protocol version is not supported\r\n");
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "CLIENT")) {
        if (argc >= 2 && (TOKEN_EQ(tokens[1], "SETNAME") ||
                          TOKEN_EQ(tokens[1], "NO-EVICT") ||
                          TOKEN_EQ(tokens[1], "NO-TOUCH") ||
                          TOKEN_EQ(tokens[1], "REPLY")))
            dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        else if (argc >= 2 && TOKEN_EQ(tokens[1], "GETNAME"))
            dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);
        else if (argc >= 2 && TOKEN_EQ(tokens[1], "ID"))
            reply_integer(wb, c ? (long long)c->fd : 0);
        else
            dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "COMMAND")) {
        if (argc >= 2 && TOKEN_EQ(tokens[1], "COUNT"))
            reply_integer(wb, 0);
        else
            dynbuf_lit(wb, "*0\r\n");
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "SELECT")) {
        long long db = 0;
        if (argc != 2 || token_to_ll(tokens[1], &db) < 0 || db != 0)
            dynbuf_lit(wb, "-ERR DB index out of range\r\n");
        else
            dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "DBSIZE")) {
        kv_store_rdlock();
        long long count = (long long)kv_store_count();
        kv_store_unlock();
        reply_integer(wb, count);
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "SAVE") || TOKEN_EQ(cmd, "BGSAVE")) {
        rdb_save_async_uring();
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "CONFIG"))
        return handle_config(tokens, argc, wb);

    if (TOKEN_EQ(cmd, "INFO"))
        return handle_info(tokens, argc, wb);

    if (TOKEN_EQ(cmd, "FLUSHALL") || TOKEN_EQ(cmd, "FLUSHDB")) {
        kv_store_wrlock();
        kv_store_iter_t it; kv_store_iter_init(&it);
        kv_entry_t *e;
        while ((e = kv_store_iter_next(&it)) != NULL)
            entry_cleanup_by_type(e);
        kv_store_iter_free(&it);
        kv_store_clear();
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        if (!g_in_reload)
            kvs_write_journal_append(msg_raw, msg_len);
        kv_store_unlock();
        return DISPATCH_OK;
    }

    /* ── Multi-key commands ── */

    if (TOKEN_EQ(cmd, "MGET")) {
        if (argc < 2) {
            dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
            return DISPATCH_ERROR;
        }
        kv_store_rdlock();
        char hdr[32];
        int hl = snprintf(hdr, sizeof(hdr), "*%d\r\n", argc - 1);
        dynbuf_cat(wb, hdr, hl);
        for (int i = 1; i < argc; i++) {
            uint64_t kh = kv_hash((const uint8_t *)tokens[i].ptr,
                                  (uint32_t)tokens[i].len);
            kv_entry_t *ke = dict_find(kv_dict_for(kh), tokens[i].ptr,
                                       (uint32_t)tokens[i].len, kh);
            if (ke && ke->expire && get_time_now() > ke->expire) {
                dict_delete(kv_dict_for(kh), tokens[i].ptr,
                            (uint32_t)tokens[i].len, kh);
                ke = NULL;
            }
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
        kv_store_unlock();
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "MSET")) {
        if (argc < 3 || ((argc - 1) % 2) != 0) {
            dynbuf_lit(wb, "-ERR wrong number of arguments for 'mset' command\r\n");
            return DISPATCH_ERROR;
        }
        kv_store_wrlock();
        for (int i = 1; i + 1 < argc; i += 2) {
            uint64_t kh = kv_hash((const uint8_t *)tokens[i].ptr,
                                  (uint32_t)tokens[i].len);
            kv_entry_t *ke = dict_find(kv_dict_for(kh), tokens[i].ptr,
                                       (uint32_t)tokens[i].len, kh);
            if (ke && entry_type(ke) != KV_TYPE_STRING) {
                entry_cleanup_by_type(ke);
                dict_delete(kv_dict_for(kh), tokens[i].ptr,
                            (uint32_t)tokens[i].len, kh);
            } else if (ke && ke->expire && get_time_now() > ke->expire) {
                entry_cleanup_by_type(ke);
                dict_delete(kv_dict_for(kh), tokens[i].ptr,
                            (uint32_t)tokens[i].len, kh);
                ke = NULL;
            }
            string_engine_set_pre(tokens[i].ptr, (uint32_t)tokens[i].len,
                                  tokens[i+1].ptr, (uint32_t)tokens[i+1].len,
                                  0, 0, kh, ke);
        }
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        if (!g_in_reload)
            kvs_write_journal_append(msg_raw, msg_len);
        kv_store_unlock();
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "MSETNX")) {
        if (argc < 3 || ((argc - 1) % 2) != 0) {
            dynbuf_lit(wb, "-ERR wrong number of arguments for 'msetnx' command\r\n");
            return DISPATCH_ERROR;
        }
        kv_store_wrlock();
        for (int i = 1; i + 1 < argc; i += 2) {
            uint64_t kh = kv_hash((const uint8_t *)tokens[i].ptr,
                                  (uint32_t)tokens[i].len);
            kv_entry_t *ke = dict_find(kv_dict_for(kh), tokens[i].ptr,
                                       (uint32_t)tokens[i].len, kh);
            if (ke && (!ke->expire || get_time_now() <= ke->expire)) {
                reply_integer(wb, 0);
                kv_store_unlock();
                return DISPATCH_OK;
            }
        }
        for (int i = 1; i + 1 < argc; i += 2) {
            uint64_t kh = kv_hash((const uint8_t *)tokens[i].ptr,
                                  (uint32_t)tokens[i].len);
            string_engine_set_pre(tokens[i].ptr, (uint32_t)tokens[i].len,
                                  tokens[i+1].ptr, (uint32_t)tokens[i+1].len,
                                  0, 0, kh, NULL);
        }
        reply_integer(wb, 1);
        if (!g_in_reload)
            kvs_write_journal_append(msg_raw, msg_len);
        kv_store_unlock();
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "DEL") || TOKEN_EQ(cmd, "UNLINK")) {
        if (argc < 2) {
            dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
            return DISPATCH_ERROR;
        }
        kv_store_wrlock();
        long long ndel = 0;
        for (int i = 1; i < argc; i++) {
            kvs_token_t kt = tokens[i];
            uint64_t kh = kv_hash((const uint8_t *)kt.ptr, (uint32_t)kt.len);
            kv_entry_t *ke = dict_find(kv_dict_for(kh), kt.ptr,
                                       (uint32_t)kt.len, kh);
            if (!ke) continue;
            entry_cleanup_by_type(ke);
            if (dict_delete(kv_dict_for(kh), kt.ptr, (uint32_t)kt.len, kh) == 0)
                ndel++;
        }
        reply_integer(wb, ndel);
        if (ndel > 0 && !g_in_reload)
            kvs_write_journal_append(msg_raw, msg_len);
        kv_store_unlock();
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "EXISTS") || TOKEN_EQ(cmd, "EXIST")) {
        if (argc < 2) {
            dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
            return DISPATCH_ERROR;
        }
        kv_store_rdlock();
        long long nfound = 0;
        for (int i = 1; i < argc; i++) {
            kvs_token_t kt = tokens[i];
            uint64_t kh = kv_hash((const uint8_t *)kt.ptr, (uint32_t)kt.len);
            kv_entry_t *ke = dict_find(kv_dict_for(kh), kt.ptr,
                                       (uint32_t)kt.len, kh);
            if (!ke) continue;
            if (ke->expire && get_time_now() > ke->expire) continue;
            nfound++;
        }
        reply_integer(wb, nfound);
        kv_store_unlock();
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "KEYS")) {
        kv_store_rdlock();
        kv_store_iter_t it; kv_store_iter_init(&it);
        kv_entry_t *ke;
        long long nkeys = 0;
        while ((ke = kv_store_iter_next(&it)) != NULL) {
            if (!(ke->expire && get_time_now() > ke->expire))
                nkeys++;
        }
        kv_store_iter_free(&it);
        char hdr[32];
        int hl = snprintf(hdr, sizeof(hdr), "*%lld\r\n", nkeys);
        dynbuf_cat(wb, hdr, hl);
        kv_store_iter_init(&it);
        while ((ke = kv_store_iter_next(&it)) != NULL) {
            if (ke->expire && get_time_now() > ke->expire) continue;
            reply_bulk(wb, entry_key(ke), ke->key_len);
        }
        kv_store_iter_free(&it);
        kv_store_unlock();
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
    } else if (TOKEN_EQ(cmd, "SET") || TOKEN_EQ(cmd, "SETNX") ||
               TOKEN_EQ(cmd, "SETEX") || TOKEN_EQ(cmd, "GETSET") ||
               TOKEN_EQ(cmd, "APPEND") || TOKEN_EQ(cmd, "INCR") ||
               TOKEN_EQ(cmd, "INCRBY") || TOKEN_EQ(cmd, "DECR") ||
               TOKEN_EQ(cmd, "DECRBY") || TOKEN_EQ(cmd, "MOD")) {
        cmd_type = KV_TYPE_STRING; is_write = 1;
    } else if (TOKEN_EQ(cmd, "GET") || TOKEN_EQ(cmd, "STRLEN")) {
        cmd_type = KV_TYPE_STRING; is_write = 0;
    } else if (TOKEN_EQ(cmd, "HSET") || TOKEN_EQ(cmd, "HMSET") ||
               TOKEN_EQ(cmd, "HSETNX") || TOKEN_EQ(cmd, "HDEL") ||
               TOKEN_EQ(cmd, "HINCRBY") || TOKEN_EQ(cmd, "HINCRBYFLOAT")) {
        cmd_type = KV_TYPE_HASH; is_write = 1;
    } else if (TOKEN_EQ(cmd, "HGET") || TOKEN_EQ(cmd, "HMGET") ||
               TOKEN_EQ(cmd, "HGETALL") || TOKEN_EQ(cmd, "HKEYS") ||
               TOKEN_EQ(cmd, "HVALS") || TOKEN_EQ(cmd, "HLEN") ||
               TOKEN_EQ(cmd, "HEXISTS") || TOKEN_EQ(cmd, "HSTRLEN") ||
               TOKEN_EQ(cmd, "HRANDFIELD")) {
        cmd_type = KV_TYPE_HASH; is_write = 0;
    } else {
        dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
        return DISPATCH_ERROR;
    }

    if (is_write) kv_store_wrlock();
    else           kv_store_rdlock();

    kv_entry_t *entry = dict_find(kv_dict_for(hash), key_tok.ptr,
                                  (uint32_t)key_tok.len, hash);

    /* Lazy-expire */
    if (entry && entry->expire && get_time_now() > entry->expire) {
        if (is_write) {
            entry_cleanup_by_type(entry);
            dict_delete(kv_dict_for(hash), key_tok.ptr, (uint32_t)key_tok.len, hash);
            atomic_fetch_add_explicit(kvs_expired_keys_ptr(), 1, memory_order_relaxed);
        } else {
            enqueue_expired_key(key_tok.ptr, (uint32_t)key_tok.len, hash);
        }
        entry = NULL;
    }

    int pre_len = wb->len;

    if (TOKEN_EQ(cmd, "TYPE")) {
        const char *r = "+none\r\n"; int rlen = 7;
        if (entry) {
            if (entry_type(entry) == KV_TYPE_HASH) { r = "+hash\r\n"; rlen = 7; }
            else { r = "+string\r\n"; rlen = 9; }
        }
        dynbuf_cat(wb, r, rlen);
        kv_store_unlock();
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "EXPIRE")) {
        if (argc < 3) {
            dynbuf_lit(wb, "-ERR wrong number of arguments for 'expire' command\r\n");
            kv_store_unlock();
            return DISPATCH_ERROR;
        }
        long long secs;
        if (token_to_ll(tokens[2], &secs) < 0) {
            dynbuf_lit(wb, "-ERR value is not an integer or out of range\r\n");
            kv_store_unlock();
            return DISPATCH_ERROR;
        }
        if (!entry) { reply_integer(wb, 0); kv_store_unlock(); return DISPATCH_OK; }
        entry->expire = get_time_now() + secs * 1000;
        reply_integer(wb, 1);
        if (!g_in_reload) kvs_write_journal_append(msg_raw, msg_len);
        kv_store_unlock();
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "TTL") || TOKEN_EQ(cmd, "PTTL")) {
        if (!entry) { reply_integer(wb, -2); kv_store_unlock(); return DISPATCH_OK; }
        if (!entry->expire) { reply_integer(wb, -1); kv_store_unlock(); return DISPATCH_OK; }
        long long rem_ms = entry->expire - get_time_now();
        if (rem_ms < 0) { reply_integer(wb, -2); kv_store_unlock(); return DISPATCH_OK; }
        reply_integer(wb, TOKEN_EQ(cmd, "TTL") ? (rem_ms + 500) / 1000 : rem_ms);
        kv_store_unlock();
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "PERSIST")) {
        if (!entry || !entry->expire) {
            reply_integer(wb, 0); kv_store_unlock(); return DISPATCH_OK;
        }
        entry->expire = 0;
        reply_integer(wb, 1);
        if (!g_in_reload) kvs_write_journal_append(msg_raw, msg_len);
        kv_store_unlock();
        return DISPATCH_OK;
    }

    if (entry && (int)entry_type(entry) != cmd_type) {
        dynbuf_cat(wb, RESP_WRONGTYPE, RESP_WRONGTYPE_LEN);
        kv_store_unlock();
        return DISPATCH_ERROR;
    }

    switch (cmd_type) {
    case KV_TYPE_STRING:
        string_cmd(cmd, entry, hash, tokens + 1, argc - 1, wb, msg_raw, msg_len);
        break;
    case KV_TYPE_HASH:
        (void)hash_cmd(cmd, entry, hash, tokens + 1, argc - 1, wb, msg_raw, msg_len,
                       c ? c->client : NULL);
        break;
    default:
        dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
        break;
    }

    /* Write-command post-processing */
    if (is_write) {
        const char *resp = wb->buf + pre_len;
        int rlen = wb->len - pre_len;
        int ok = (rlen > 0 && resp[0] != '-' &&
                  !(rlen >= 5 && memcmp(resp, "$-1\r\n", 5) == 0));
        if (ok && !g_in_reload)
            kvs_write_journal_append(msg_raw, msg_len);
        flush_expire_queue();
    }

    kv_store_unlock();
    return DISPATCH_OK;
}
