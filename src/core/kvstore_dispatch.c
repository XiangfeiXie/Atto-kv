/* kvstore_dispatch.c — RESP command dispatch */

#include "kvstore_internal.h"
#include "rdma_repl.h"
#include "persistence.h"

#include <stdio.h>
#include <string.h>

/* SET core: create or overwrite a key.  Returns 0 on success, 1 when NX/XX
 * fails, -1 on allocation error. */
static int string_set(const char *key, uint32_t key_len,
                      const char *val, uint32_t val_len,
                      long long expire, int mode,
                      uint64_t hash, kv_entry_t *existing)
{
    if (mode == 1 && existing) return 1;   /* NX: key exists */
    if (mode == 2 && !existing) return 1;  /* XX: key missing */

    if (existing) {
        if (existing->val_len == val_len) {
            memcpy(entry_payload(existing), val, val_len);
            existing->expire = expire;
            return 0;
        }
        kv_entry_t *e = dict_resize_entry(&g_dict, existing, val_len);
        if (!e) return -1;
        memcpy(entry_payload(e), val, val_len);
        e->expire = expire;
        return 0;
    }

    kv_entry_t *e = dict_add(&g_dict, key, key_len, val, val_len, hash);
    if (!e) return -1;
    if (expire) e->expire = expire;
    return 0;
}

dispatch_result_t kvs_dispatch(kvs_token_t *tokens, int argc,
                               const char *msg_raw, int msg_len,
                               DynBuf *wb)
{
    if (tokens == NULL || argc <= 0 || wb == NULL)
        return DISPATCH_ERROR;

    atomic_fetch_add_explicit(&g_kvs_total_commands, 1, memory_order_relaxed);

    kvs_token_t cmd = tokens[0];

    /* Commands are case-insensitive: fold to uppercase in a stack buffer. */
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
            ucmd_buf[i] = (ch >= 'a' && ch <= 'z') ? (char)(ch & 0xDF) : (char)ch;
        }
        cmd.ptr   = ucmd_buf;
        cmd.len   = ulen;
        tokens[0] = cmd;
    }

    /* ── Hot path: GET / SET — checked first (60-80% of requests) ── */

    if (TOKEN_EQ(cmd, "GET")) {
        if (argc != 2) {
            dynbuf_cat(wb, RESP_ERR_ARGS, RESP_ERR_ARGS_LEN);
            return DISPATCH_ERROR;
        }
        kvs_token_t key_tok = tokens[1];
        uint64_t hash = kv_hash((const uint8_t *)key_tok.ptr, (uint32_t)key_tok.len);
        kv_entry_t *entry = dict_find(&g_dict, key_tok.ptr, (uint32_t)key_tok.len, hash);
        if (!entry) {
            dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);
        } else {
            reply_bulk(wb, entry_payload(entry), (int)entry_payload_len(entry));
        }
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "SET")) {
        if (argc < 3) {
            dynbuf_cat(wb, RESP_ERR_ARGS, RESP_ERR_ARGS_LEN);
            return DISPATCH_ERROR;
        }
        kvs_token_t key_tok = tokens[1];
        uint64_t hash = kv_hash((const uint8_t *)key_tok.ptr, (uint32_t)key_tok.len);
        kv_entry_t *entry = dict_find(&g_dict, key_tok.ptr, (uint32_t)key_tok.len, hash);

        long long expire = 0;
        int mode = 0;               /* 0=default, 1=NX, 2=XX */
        for (int i = 3; i < argc; i++) {
            kvs_token_t opt = tokens[i];
            if (opt.len == 2 && (opt.ptr[0]=='E'||opt.ptr[0]=='e') &&
                                 (opt.ptr[1]=='X'||opt.ptr[1]=='x')) {
                if (i + 1 >= argc) { dynbuf_lit(wb, "-ERR syntax error\r\n"); return DISPATCH_ERROR; }
                long long s;
                if (token_to_ll(tokens[++i], &s) < 0 || s <= 0) {
                    dynbuf_lit(wb, "-ERR invalid expire time in 'set' command\r\n");
                    return DISPATCH_ERROR;
                }
                expire = get_time_now() + s * 1000;
            } else if (opt.len == 2 && (opt.ptr[0]=='P'||opt.ptr[0]=='p') &&
                                        (opt.ptr[1]=='X'||opt.ptr[1]=='x')) {
                if (i + 1 >= argc) { dynbuf_lit(wb, "-ERR syntax error\r\n"); return DISPATCH_ERROR; }
                long long ms;
                if (token_to_ll(tokens[++i], &ms) < 0 || ms <= 0) {
                    dynbuf_lit(wb, "-ERR invalid expire time in 'set' command\r\n");
                    return DISPATCH_ERROR;
                }
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

        int ret = string_set(key_tok.ptr, (uint32_t)key_tok.len,
                             tokens[2].ptr, (uint32_t)tokens[2].len,
                             expire, mode, hash, entry);
        if (ret < 0) {
            dynbuf_cat(wb, RESP_ERROR, RESP_ERROR_LEN);
        } else if (ret == 0) {
            dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
            if (!g_in_reload) {
                kvs_write_journal_append(msg_raw, msg_len);
                kvs_repl_incr_hook(msg_raw, msg_len);
            }
        } else {
            dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);   /* NX/XX failed */
        }
        return DISPATCH_OK;
    }

    /* ── Hot commands: PING / DEL ── */

    if (TOKEN_EQ(cmd, "PING")) {
        if (argc == 1)
            dynbuf_cat(wb, RESP_PONG, RESP_PONG_LEN);
        else if (argc == 2)
            reply_bulk(wb, tokens[1].ptr, tokens[1].len);
        else
            dynbuf_lit(wb, "-ERR wrong number of arguments for 'ping' command\r\n");
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "DEL")) {
        if (argc < 2) {
            dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
            return DISPATCH_ERROR;
        }
        long long ndel = 0;
        for (int i = 1; i < argc; i++) {
            kvs_token_t kt = tokens[i];
            uint64_t kh = kv_hash((const uint8_t *)kt.ptr, (uint32_t)kt.len);
            kv_entry_t *ke = dict_find(&g_dict, kt.ptr, (uint32_t)kt.len, kh);
            if (!ke) continue;
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

    /* ── Connection / administrative commands ── */

    if (TOKEN_EQ(cmd, "FULLSYNC")) {
        int ret = rdma_repl_master_trigger_sync();
        if (ret == 0)
            dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        else
            dynbuf_lit(wb, "-ERR fullsync trigger failed\r\n");
        return DISPATCH_OK;
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
        dict_clear(&g_dict);
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        if (!g_in_reload) {
            kvs_write_journal_append(msg_raw, msg_len);
            kvs_repl_incr_hook(msg_raw, msg_len);
        }
        return DISPATCH_OK;
    }

    /* ── Remaining single-key: EXPIRE / TTL ── */

    if (argc < 2) {
        dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
        return DISPATCH_ERROR;
    }

    kvs_token_t key_tok = tokens[1];
    uint64_t hash = kv_hash((const uint8_t *)key_tok.ptr, (uint32_t)key_tok.len);
    kv_entry_t *entry = dict_find(&g_dict, key_tok.ptr, (uint32_t)key_tok.len, hash);

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

    if (TOKEN_EQ(cmd, "TTL")) {
        if (!entry) { reply_integer(wb, -2); return DISPATCH_OK; }
        if (!entry->expire) { reply_integer(wb, -1); return DISPATCH_OK; }
        long long rem_ms = entry->expire - get_time_now();
        if (rem_ms < 0) { reply_integer(wb, -2); return DISPATCH_OK; }
        reply_integer(wb, (rem_ms + 500) / 1000);
        return DISPATCH_OK;
    }

    dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
    return DISPATCH_ERROR;
}
