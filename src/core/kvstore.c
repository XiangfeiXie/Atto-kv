#include "kvstore.h"
#include "kv_type_string.h"
#include "kv_type_list.h"
#include "kv_type_set.h"
#include "kv_type_hash.h"
#include "kv_type_zset.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <signal.h>
#include <fcntl.h>

#include "rdma_repl.h"
#include "persistence.h"
#include "block.h"

/* ==================== Global dict (single storage engine) ==================== */
kv_dict_t g_dict;
int g_in_reload = 0;    /* set by persistence layer during AOF/RDB reload */

/* ==================== Utility ==================== */

void *kvs_malloc(size_t size) { return malloc(size); }
void kvs_free(void *ptr)       { free(ptr); }

long long get_time_now(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000LL + tv.tv_usec / 1000;
}

KVItem* create_kvitem_len(const char *value, int vlen) {
    if (!value || vlen < 0) return NULL;
    KVItem *item = (KVItem*)malloc(sizeof(KVItem));
    if (!item) return NULL;
    memset(item, 0, sizeof(KVItem));
    item->len   = (size_t)vlen;
    item->value = (char*)malloc((size_t)vlen + 1);
    if (!item->value) { free(item); return NULL; }
    memcpy(item->value, value, (size_t)vlen);
    item->value[vlen] = '\0';
    item->expire = 0;
    return item;
}

void free_kvitem(KVItem *item) {
    if (!item) return;
    free(item->value);
    free(item);
}

__attribute__((noinline))
void kvs_rdma_incr_probe(const char *msg, int length) {
    /*
     * The compiler barrier prevents -O3 from eliminating call sites.
     * Without this, the compiler sees an empty pure function and removes
     * every call as dead code — the uprobe attaches but never fires.
     */
    asm volatile("" : : "r"(msg), "r"(length) : "memory");
}

/* ── Parse a token as long long (strict). Returns 0 on success, -1 on error. ── */
static int token_to_ll(kvs_token_t t, long long *out) {
    if (t.len <= 0 || t.len > 20) return -1;
    int i = 0;
    int neg = 0;
    if (t.ptr[0] == '-') { neg = 1; i = 1; if (t.len == 1) return -1; }
    else if (t.ptr[0] == '+') { i = 1; if (t.len == 1) return -1; }
    unsigned long long v = 0;
    for (; i < t.len; i++) {
        unsigned c = (unsigned char)t.ptr[i] - (unsigned)'0';
        if (c > 9) return -1;
        if (v > (unsigned long long)9223372036854775807ULL / 10) return -1;
        v = v * 10 + c;
        if (!neg && v > (unsigned long long)9223372036854775807ULL) return -1;
        if (neg && v > (unsigned long long)9223372036854775808ULL) return -1;
    }
    *out = neg ? -(long long)v : (long long)v;
    return 0;
}

/* ── Per-type cleanup before deleting an entry from the dict ── */
static void entry_cleanup_by_type(kv_entry_t *e) {
    switch (entry_type(e)) {
    case KV_TYPE_LIST: list_cleanup_entry(e); break;
    case KV_TYPE_SET:  set_cleanup_entry(e);  break;
    case KV_TYPE_HASH: hash_cleanup_entry(e); break;
    case KV_TYPE_ZSET: zset_cleanup_entry(e); break;
    default: break;
    }
}

/* Append an integer reply ":N\r\n" */
static inline void reply_integer(DynBuf *wb, long long v) {
    char buf[32];
    int n = snprintf(buf, sizeof(buf), ":%lld\r\n", v);
    dynbuf_cat(wb, buf, n);
}

/* Append a bulk string reply "$N\r\n<data>\r\n" */
static inline void reply_bulk(DynBuf *wb, const char *s, int len) {
    char hdr[24];
    int hl = snprintf(hdr, sizeof(hdr), "$%d\r\n", len);
    dynbuf_cat(wb, hdr, hl);
    if (len > 0) dynbuf_cat(wb, s, len);
    dynbuf_cat(wb, "\r\n", 2);
}

/* ── Retry a blocked client's saved command ── */
static void block_retry_client(client_t *wc) {
    if (!wc || !wc->block_cmd_len) return;

    int argc2 = 0;
    kvs_token_t tokens2[KVS_MAX_TOKENS];
    int consumed = resp_parse_slices(wc->block_cmd_buf, wc->block_cmd_len,
                                     tokens2, KVS_MAX_TOKENS, &argc2);
    if (consumed <= 0) return;

    /* Re-dispatch into the woken client's write buffer */
    dispatch_result_t rr = kvs_dispatch(tokens2, argc2,
                                        wc->block_cmd_buf, consumed,
                                        &wc->conn->write_buf, wc->conn);
    (void)rr;
}

/* ==================== Command dispatch ==================== */

dispatch_result_t kvs_dispatch(kvs_token_t *tokens, int argc,
                               const char *msg_raw, int msg_len,
                               DynBuf *wb, struct conn *c)
{
    if (tokens == NULL || argc <= 0 || wb == NULL)
        return DISPATCH_ERROR;

    kvs_token_t cmd = tokens[0];

    /* ── Lazy client allocation ── */
    if (c && !c->client) {
        c->client = (client_t *)calloc(1, sizeof(client_t));
        if (c->client)
            c->client->conn = c;
    }

    /* ── Global / connection commands (no key needed) ── */

    if (TOKEN_EQ(cmd, "PING")) {
        if (argc == 1) {
            dynbuf_cat(wb, RESP_PONG, RESP_PONG_LEN);
        } else if (argc == 2) {
            /* Redis: PING <msg> echoes msg as bulk reply */
            reply_bulk(wb, tokens[1].ptr, tokens[1].len);
        } else {
            dynbuf_cat(wb,
                "-ERR wrong number of arguments for 'ping' command\r\n", 51);
        }
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "QUIT")) {
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        return DISPATCH_CLOSE;       /* reactor must close after flush */
    }

    if (TOKEN_EQ(cmd, "ECHO")) {
        if (argc >= 2) {
            reply_bulk(wb, tokens[1].ptr, tokens[1].len);
        } else {
            dynbuf_cat(wb,
                "-ERR wrong number of arguments for 'echo' command\r\n", 51);
        }
        return DISPATCH_OK;
    }

    /* HELLO — clients use this to negotiate RESP3. Decline gracefully so
       clients fall back to RESP2 without erroring out. */
    if (TOKEN_EQ(cmd, "HELLO")) {
        dynbuf_cat(wb,
            "-NOPROTO sorry, this protocol version is not supported\r\n", 56);
        return DISPATCH_OK;
    }

    /* CLIENT subcommands — accept the common ones used by drivers. */
    if (TOKEN_EQ(cmd, "CLIENT")) {
        if (argc >= 2 && (TOKEN_EQ(tokens[1], "SETNAME") ||
                          TOKEN_EQ(tokens[1], "NO-EVICT") ||
                          TOKEN_EQ(tokens[1], "NO-TOUCH") ||
                          TOKEN_EQ(tokens[1], "REPLY"))) {
            dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        } else if (argc >= 2 && TOKEN_EQ(tokens[1], "GETNAME")) {
            dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);
        } else if (argc >= 2 && TOKEN_EQ(tokens[1], "ID")) {
            reply_integer(wb, c ? (long long)c->fd : 0);
        } else {
            dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        }
        return DISPATCH_OK;
    }

    /* COMMAND — stub: redis-cli sends "COMMAND DOCS" at connect.
       Returning an empty array satisfies introspection without details. */
    if (TOKEN_EQ(cmd, "COMMAND")) {
        if (argc >= 2 && TOKEN_EQ(tokens[1], "COUNT")) {
            reply_integer(wb, 0);
        } else {
            dynbuf_cat(wb, "*0\r\n", 4);
        }
        return DISPATCH_OK;
    }

    /* SELECT — single-DB server: accept "SELECT 0", reject others. */
    if (TOKEN_EQ(cmd, "SELECT")) {
        long long db = 0;
        if (argc != 2 || token_to_ll(tokens[1], &db) < 0 || db != 0) {
            dynbuf_cat(wb, "-ERR DB index out of range\r\n", 28);
        } else {
            dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        }
        return DISPATCH_OK;
    }

    /* DBSIZE — count of keys in dict. */
    if (TOKEN_EQ(cmd, "DBSIZE")) {
        reply_integer(wb, (long long)dict_count(&g_dict));
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "SAVE") || TOKEN_EQ(cmd, "BGSAVE")) {
        rdb_save_async_uring();
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "FLUSHALL") || TOKEN_EQ(cmd, "FLUSHDB")) {
        kv_iter_t *it = dict_iter_new(&g_dict, 1);
        if (it) {
            kv_entry_t *e;
            while ((e = dict_iter_next(it)) != NULL)
                entry_cleanup_by_type(e);
            dict_iter_free(it);
        }
        dict_clear(&g_dict);
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        if (!g_in_reload) {
            if (ENABLE_AOF) aof_async_append(msg_raw, msg_len);
            kvs_rdma_incr_probe(msg_raw, msg_len);
        }
        return DISPATCH_OK;
    }

    /* ── Multi-key STRING commands (handled here before single-key dispatch) ── */

    /* MGET key [key ...] → array of bulk strings (nil for non-string / missing) */
    if (TOKEN_EQ(cmd, "MGET")) {
        if (argc < 2) {
            dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
            return DISPATCH_ERROR;
        }
        char hdr[32];
        int hl = snprintf(hdr, sizeof(hdr), "*%d\r\n", argc - 1);
        dynbuf_cat(wb, hdr, hl);
        for (int i = 1; i < argc; i++) {
            uint32_t vlen = 0; uint8_t enc = KV_ENCODING_RAW;
            const char *val = string_engine_get(tokens[i].ptr, (uint32_t)tokens[i].len,
                                                &vlen, &enc);
            if (!val) {
                dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);
                continue;
            }
            if (enc == KV_ENCODING_INT) {
                int64_t iv; memcpy(&iv, val, sizeof(int64_t));
                char ibuf[24];
                int il = snprintf(ibuf, sizeof(ibuf), "%lld", (long long)iv);
                int bh = snprintf(hdr, sizeof(hdr), "$%d\r\n", il);
                dynbuf_cat(wb, hdr, bh);
                dynbuf_cat(wb, ibuf, il);
                dynbuf_cat(wb, "\r\n", 2);
            } else {
                int bh = snprintf(hdr, sizeof(hdr), "$%u\r\n", vlen);
                dynbuf_cat(wb, hdr, bh);
                dynbuf_cat(wb, val, (int)vlen);
                dynbuf_cat(wb, "\r\n", 2);
            }
        }
        return DISPATCH_OK;
    }

    /* MSET key val [key val ...] → always +OK (overwrites any existing type). */
    if (TOKEN_EQ(cmd, "MSET")) {
        if (argc < 3 || ((argc - 1) % 2) != 0) {
            dynbuf_cat(wb,
                "-ERR wrong number of arguments for 'mset' command\r\n", 51);
            return DISPATCH_ERROR;
        }
        for (int i = 1; i + 1 < argc; i += 2) {
            /* If a non-string entry exists at this key, drop it first so the
             * overwrite is type-safe (Redis MSET replaces any existing type). */
            uint64_t kh = kv_hash((const uint8_t *)tokens[i].ptr,
                                  (uint32_t)tokens[i].len);
            kv_entry_t *ke = dict_find(&g_dict, tokens[i].ptr,
                                        (uint32_t)tokens[i].len, kh);
            if (ke && entry_type(ke) != KV_TYPE_STRING) {
                entry_cleanup_by_type(ke);
                dict_delete(&g_dict, tokens[i].ptr,
                            (uint32_t)tokens[i].len, kh);
            }
            string_engine_set(tokens[i].ptr, (uint32_t)tokens[i].len,
                              tokens[i+1].ptr, (uint32_t)tokens[i+1].len,
                              0, 0);
        }
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        if (!g_in_reload) {
            if (ENABLE_AOF) aof_async_append(msg_raw, msg_len);
            kvs_rdma_incr_probe(msg_raw, msg_len);
        }
        return DISPATCH_OK;
    }

    /* MSETNX key val [key val ...] → :1 if all keys absent and set, else :0 */
    if (TOKEN_EQ(cmd, "MSETNX")) {
        if (argc < 3 || ((argc - 1) % 2) != 0) {
            dynbuf_cat(wb,
                "-ERR wrong number of arguments for 'msetnx' command\r\n", 53);
            return DISPATCH_ERROR;
        }
        /* First pass: any existing (non-expired) key aborts the whole op. */
        for (int i = 1; i + 1 < argc; i += 2) {
            uint64_t kh = kv_hash((const uint8_t *)tokens[i].ptr,
                                  (uint32_t)tokens[i].len);
            kv_entry_t *ke = dict_find(&g_dict, tokens[i].ptr,
                                        (uint32_t)tokens[i].len, kh);
            if (ke && (!ke->expire || get_time_now() <= ke->expire)) {
                reply_integer(wb, 0);
                return DISPATCH_OK;
            }
        }
        for (int i = 1; i + 1 < argc; i += 2) {
            string_engine_set(tokens[i].ptr, (uint32_t)tokens[i].len,
                              tokens[i+1].ptr, (uint32_t)tokens[i+1].len,
                              0, 0);
        }
        reply_integer(wb, 1);
        if (!g_in_reload) {
            if (ENABLE_AOF) aof_async_append(msg_raw, msg_len);
            kvs_rdma_incr_probe(msg_raw, msg_len);
        }
        return DISPATCH_OK;
    }

    /* ── Key-based commands ── */
    if (argc < 2) {
        dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
        return DISPATCH_ERROR;
    }

    kvs_token_t key_tok = tokens[1];
    uint64_t hash = kv_hash((const uint8_t *)key_tok.ptr, (uint32_t)key_tok.len);
    kv_entry_t *entry = dict_find(&g_dict, key_tok.ptr, (uint32_t)key_tok.len, hash);

    /* Treat already-expired entry as absent. */
    if (entry && entry->expire && get_time_now() > entry->expire) {
        entry_cleanup_by_type(entry);
        dict_delete(&g_dict, key_tok.ptr, (uint32_t)key_tok.len, hash);
        entry = NULL;
    }

    int pre_len = wb->len;

    /* ── DEL — type-agnostic, integer reply (Redis-compat) ── */
    if (TOKEN_EQ(cmd, "DEL") || TOKEN_EQ(cmd, "UNLINK")) {
        long long ndel = 0;
        for (int i = 1; i < argc; i++) {
            kvs_token_t kt = tokens[i];
            uint64_t kh = kv_hash((const uint8_t *)kt.ptr, (uint32_t)kt.len);
            kv_entry_t *ke = dict_find(&g_dict, kt.ptr, (uint32_t)kt.len, kh);
            if (!ke) continue;
            entry_cleanup_by_type(ke);
            if (dict_delete(&g_dict, kt.ptr, (uint32_t)kt.len, kh) == 0)
                ndel++;
        }
        reply_integer(wb, ndel);
        if (ndel > 0 && !g_in_reload) {
            if (ENABLE_AOF) aof_async_append(msg_raw, msg_len);
            kvs_rdma_incr_probe(msg_raw, msg_len);
        }
        return DISPATCH_OK;
    }

    /* ── EXISTS — type-agnostic, integer reply (Redis-compat).
         EXIST kept as a 1-arg alias for legacy callers. ── */
    if (TOKEN_EQ(cmd, "EXISTS") || TOKEN_EQ(cmd, "EXIST")) {
        long long nfound = 0;
        for (int i = 1; i < argc; i++) {
            kvs_token_t kt = tokens[i];
            uint64_t kh = kv_hash((const uint8_t *)kt.ptr, (uint32_t)kt.len);
            kv_entry_t *ke = dict_find(&g_dict, kt.ptr, (uint32_t)kt.len, kh);
            if (!ke) continue;
            if (ke->expire && get_time_now() > ke->expire) continue;
            nfound++;
        }
        reply_integer(wb, nfound);
        return DISPATCH_OK;
    }

    /* ── TYPE — Redis returns simple string "+string"/"+list"/etc, "+none" if absent ── */
    if (TOKEN_EQ(cmd, "TYPE")) {
        const char *r = "+none\r\n";
        int rlen = 7;
        if (entry) {
            switch (entry_type(entry)) {
            case KV_TYPE_STRING: r = "+string\r\n";  rlen = 9; break;
            case KV_TYPE_LIST:   r = "+list\r\n";    rlen = 7; break;
            case KV_TYPE_SET:    r = "+set\r\n";     rlen = 6; break;
            case KV_TYPE_HASH:   r = "+hash\r\n";    rlen = 7; break;
            case KV_TYPE_ZSET:   r = "+zset\r\n";    rlen = 7; break;
            }
        }
        dynbuf_cat(wb, r, rlen);
        return DISPATCH_OK;
    }

    /* ── EXPIRE — integer reply :1 (set) / :0 (no key). ── */
    if (TOKEN_EQ(cmd, "EXPIRE")) {
        if (argc < 3) {
            dynbuf_cat(wb,
                "-ERR wrong number of arguments for 'expire' command\r\n", 53);
            return DISPATCH_ERROR;
        }
        long long secs;
        if (token_to_ll(tokens[2], &secs) < 0) {
            dynbuf_cat(wb,
                "-ERR value is not an integer or out of range\r\n", 46);
            return DISPATCH_ERROR;
        }
        if (!entry) {
            reply_integer(wb, 0);
            return DISPATCH_OK;
        }
        entry->expire = get_time_now() + secs * 1000;
        reply_integer(wb, 1);
        if (!g_in_reload) {
            if (ENABLE_AOF) aof_async_append(msg_raw, msg_len);
            kvs_rdma_incr_probe(msg_raw, msg_len);
        }
        return DISPATCH_OK;
    }

    /* TTL / PTTL: Redis returns -2 if key missing, -1 if no TTL set,
       else remaining seconds (TTL) or milliseconds (PTTL). */
    if (TOKEN_EQ(cmd, "TTL") || TOKEN_EQ(cmd, "PTTL")) {
        if (!entry) { reply_integer(wb, -2); return DISPATCH_OK; }
        if (!entry->expire) { reply_integer(wb, -1); return DISPATCH_OK; }
        long long rem_ms = entry->expire - get_time_now();
        if (rem_ms < 0) { reply_integer(wb, -2); return DISPATCH_OK; }
        if (TOKEN_EQ(cmd, "TTL")) reply_integer(wb, (rem_ms + 500) / 1000);
        else                       reply_integer(wb, rem_ms);
        return DISPATCH_OK;
    }

    /* PERSIST — clear TTL. */
    if (TOKEN_EQ(cmd, "PERSIST")) {
        if (!entry || !entry->expire) { reply_integer(wb, 0); return DISPATCH_OK; }
        entry->expire = 0;
        reply_integer(wb, 1);
        if (!g_in_reload) {
            if (ENABLE_AOF) aof_async_append(msg_raw, msg_len);
            kvs_rdma_incr_probe(msg_raw, msg_len);
        }
        return DISPATCH_OK;
    }

    /* ── Resolve command → expected type in a single first-letter switch.
         Performance: 1 dispatch by ptr[0], then a small set of length+memcmp
         compares (via TOKEN_EQ). Avoids the O(n) cmd-list walk previously
         done twice (wrongtype check + new-key branch). ── */
    int cmd_type = -1;       /* -1 = unknown */
    int is_write = 0;
    switch (cmd.ptr[0]) {
    case 'S':
        if (TOKEN_EQ(cmd, "SET"))               { cmd_type = KV_TYPE_STRING; is_write = 1; }
        else if (TOKEN_EQ(cmd, "SETNX"))        { cmd_type = KV_TYPE_STRING; is_write = 1; }
        else if (TOKEN_EQ(cmd, "SETEX"))        { cmd_type = KV_TYPE_STRING; is_write = 1; }
        else if (TOKEN_EQ(cmd, "STRLEN"))       { cmd_type = KV_TYPE_STRING; }
        else if (TOKEN_EQ(cmd, "SADD")        || TOKEN_EQ(cmd, "SREM")  ||
                 TOKEN_EQ(cmd, "SPOP")        || TOKEN_EQ(cmd, "SMOVE") ||
                 TOKEN_EQ(cmd, "SINTERSTORE")|| TOKEN_EQ(cmd, "SUNIONSTORE") ||
                 TOKEN_EQ(cmd, "SDIFFSTORE")) {
                                                  cmd_type = KV_TYPE_SET; is_write = 1;
        }
        else if (TOKEN_EQ(cmd, "SMEMBERS")    || TOKEN_EQ(cmd, "SCARD")     ||
                 TOKEN_EQ(cmd, "SISMEMBER")   || TOKEN_EQ(cmd, "SMISMEMBER")||
                 TOKEN_EQ(cmd, "SRANDMEMBER") || TOKEN_EQ(cmd, "SINTER")    ||
                 TOKEN_EQ(cmd, "SUNION")      || TOKEN_EQ(cmd, "SDIFF")) {
                                                  cmd_type = KV_TYPE_SET;
        }
        break;
    case 'G':
        if (TOKEN_EQ(cmd, "GET"))               { cmd_type = KV_TYPE_STRING; }
        else if (TOKEN_EQ(cmd, "GETSET"))       { cmd_type = KV_TYPE_STRING; is_write = 1; }
        break;
    case 'A':
        if (TOKEN_EQ(cmd, "APPEND"))            { cmd_type = KV_TYPE_STRING; is_write = 1; }
        break;
    case 'I':
        if (TOKEN_EQ(cmd, "INCR") ||
            TOKEN_EQ(cmd, "INCRBY") ||
            TOKEN_EQ(cmd, "INCRBYFLOAT"))       { cmd_type = KV_TYPE_STRING; is_write = 1; }
        break;
    case 'D':
        if (TOKEN_EQ(cmd, "DECR") ||
            TOKEN_EQ(cmd, "DECRBY"))            { cmd_type = KV_TYPE_STRING; is_write = 1; }
        break;
    case 'M':
        if (TOKEN_EQ(cmd, "MOD"))               { cmd_type = KV_TYPE_STRING; is_write = 1; }
        break;
    case 'L':
        if (TOKEN_EQ(cmd, "LPUSH")   || TOKEN_EQ(cmd, "LPOP")   ||
            TOKEN_EQ(cmd, "LPUSHX")  || TOKEN_EQ(cmd, "LSET")   ||
            TOKEN_EQ(cmd, "LREM")    || TOKEN_EQ(cmd, "LTRIM")  ||
            TOKEN_EQ(cmd, "LINSERT") || TOKEN_EQ(cmd, "LMOVE")) {
                                                  cmd_type = KV_TYPE_LIST; is_write = 1;
        } else if (TOKEN_EQ(cmd, "LLEN")   || TOKEN_EQ(cmd, "LRANGE") ||
                   TOKEN_EQ(cmd, "LINDEX") || TOKEN_EQ(cmd, "LPOS")) {
                                                  cmd_type = KV_TYPE_LIST;
        }
        break;
    case 'R':
        if (TOKEN_EQ(cmd, "RPUSH")   || TOKEN_EQ(cmd, "RPOP")  ||
            TOKEN_EQ(cmd, "RPUSHX")  || TOKEN_EQ(cmd, "RPOPLPUSH")) {
                                                  cmd_type = KV_TYPE_LIST; is_write = 1;
        }
        break;
    case 'B':
        if (TOKEN_EQ(cmd, "BLPOP") ||
            TOKEN_EQ(cmd, "BRPOP"))             { cmd_type = KV_TYPE_LIST; is_write = 1; }
        break;
    case 'H':
        if (TOKEN_EQ(cmd, "HSET")    || TOKEN_EQ(cmd, "HDEL")  ||
            TOKEN_EQ(cmd, "HSETNX")  || TOKEN_EQ(cmd, "HMSET") ||
            TOKEN_EQ(cmd, "HINCRBY") || TOKEN_EQ(cmd, "HINCRBYFLOAT")) {
                                                  cmd_type = KV_TYPE_HASH; is_write = 1;
        } else if (TOKEN_EQ(cmd, "HGET")    || TOKEN_EQ(cmd, "HGETALL") ||
                   TOKEN_EQ(cmd, "HKEYS")   || TOKEN_EQ(cmd, "HVALS")   ||
                   TOKEN_EQ(cmd, "HLEN")    || TOKEN_EQ(cmd, "HEXISTS") ||
                   TOKEN_EQ(cmd, "HMGET")   || TOKEN_EQ(cmd, "HSTRLEN") ||
                   TOKEN_EQ(cmd, "HRANDFIELD")) {
                                                  cmd_type = KV_TYPE_HASH;
        }
        break;
    case 'Z':
        if (TOKEN_EQ(cmd, "ZADD")            || TOKEN_EQ(cmd, "ZREM")   ||
            TOKEN_EQ(cmd, "ZINCRBY")         || TOKEN_EQ(cmd, "ZPOPMIN")||
            TOKEN_EQ(cmd, "ZPOPMAX")         ||
            TOKEN_EQ(cmd, "ZREMRANGEBYRANK") ||
            TOKEN_EQ(cmd, "ZREMRANGEBYSCORE")) {
                                                  cmd_type = KV_TYPE_ZSET; is_write = 1;
        } else if (TOKEN_EQ(cmd, "ZSCORE")   || TOKEN_EQ(cmd, "ZMSCORE") ||
                   TOKEN_EQ(cmd, "ZRANGE")   || TOKEN_EQ(cmd, "ZREVRANGE") ||
                   TOKEN_EQ(cmd, "ZRANK")    || TOKEN_EQ(cmd, "ZREVRANK")  ||
                   TOKEN_EQ(cmd, "ZCARD")    || TOKEN_EQ(cmd, "ZCOUNT")    ||
                   TOKEN_EQ(cmd, "ZRANGEBYSCORE") ||
                   TOKEN_EQ(cmd, "ZREVRANGEBYSCORE") ||
                   TOKEN_EQ(cmd, "ZRANDMEMBER")) {
                                                  cmd_type = KV_TYPE_ZSET;
        }
        break;
    default: break;
    }

    if (cmd_type < 0) {
        dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
        return DISPATCH_ERROR;
    }

    /* WRONGTYPE if key exists but with a different type */
    if (entry && (int)entry_type(entry) != cmd_type) {
        dynbuf_cat(wb, RESP_WRONGTYPE, RESP_WRONGTYPE_LEN);
        return DISPATCH_ERROR;
    }

    dispatch_result_t r = DISPATCH_OK;
    switch (cmd_type) {
    case KV_TYPE_STRING:
        string_cmd(cmd, entry, tokens + 1, argc - 1, wb, msg_raw, msg_len);
        r = DISPATCH_OK;
        break;
    case KV_TYPE_LIST:
        r = list_cmd(cmd, entry, tokens + 1, argc - 1, wb,
                     msg_raw, msg_len, c ? c->client : NULL);
        break;
    case KV_TYPE_SET:
        r = set_cmd(cmd, entry, tokens + 1, argc - 1, wb,
                    msg_raw, msg_len, c ? c->client : NULL);
        break;
    case KV_TYPE_HASH:
        r = hash_cmd(cmd, entry, tokens + 1, argc - 1, wb,
                     msg_raw, msg_len, c ? c->client : NULL);
        break;
    case KV_TYPE_ZSET:
        r = zset_cmd(cmd, entry, tokens + 1, argc - 1, wb,
                     msg_raw, msg_len, c ? c->client : NULL);
        break;
    }

    if (r == DISPATCH_BLOCKED)
        return DISPATCH_BLOCKED;

    /* ── Write-command post-processing (AOF + eBPF) ── */
    if (is_write) {
        const char *resp = wb->buf + pre_len;
        int rlen = wb->len - pre_len;
        int ok = (rlen > 0 && resp[0] != '-' &&
                  !(rlen >= 5 && memcmp(resp, "$-1\r\n", 5) == 0));
        if (ok && !g_in_reload) {
            if (ENABLE_AOF) aof_async_append(msg_raw, msg_len);
            kvs_rdma_incr_probe(msg_raw, msg_len);
        }
    }

    /* ── After LPUSH/RPUSH, wake any client blocked on this key ── */
    if (r == DISPATCH_OK && (TOKEN_EQ(cmd, "LPUSH") || TOKEN_EQ(cmd, "RPUSH"))) {
        client_t *wc = block_wake_one(key_tok.ptr, (uint32_t)key_tok.len);
        if (wc) {
            block_retry_client(wc);
            block_notify_woken(wc);
        }
    }

    return DISPATCH_OK;
}

/* ==================== Engine lifecycle ==================== */

int init_kvengine(void) {
    dict_init(&g_dict);
    block_init();
    printf("Using unified dict engine\n");
    return 0;
}

void dest_kvengine(void) {
    dict_destroy(&g_dict);
}

void kvs_flush_all(void) {
    kv_iter_t *it = dict_iter_new(&g_dict, 1);
    if (it) {
        kv_entry_t *e;
        while ((e = dict_iter_next(it)) != NULL)
            entry_cleanup_by_type(e);
        dict_iter_free(it);
    }
    dict_clear(&g_dict);
#ifdef USE_JEMALLOC
    mallctl("arenas.purge", NULL, NULL, NULL, 0);
#endif
}

void engine_reload(void) {
    if (ENABLE_AOF && access(persist_path(AOF_REL), F_OK) == 0) {
        printf("Loading AOF (mmap)...\n");
        aof_reload_mmap();
    } else if (ENABLE_RDB && access(persist_path(RDB_REL), F_OK) == 0) {
        printf("Loading RDB (mmap)...\n");
        rdb_load_mmap();
    } else {
        printf("Engine reload: no persistence file\n");
    }
}

/* ==================== Main ==================== */

int main(int argc, char *argv[]) {
    signal(SIGPIPE, SIG_IGN);
    persist_set_basedir(argv[0]);
    const char *conf_path = (argc >= 2) ? argv[1] : "conf/kvstore.conf";
    config_load(conf_path);
    config_print();

    init_kvengine();
    engine_reload();
    if (ENABLE_AOF) aof_async_init();

    if (kvs_config.rdma_sync_enabled) {
        if (kvs_config.rdma_role == ROLE_MASTER) {
            rdma_repl_master_start();
        } else {
            if (rdma_repl_slave_run() != 0)
                fprintf(stderr, "[RDMA-REPL] slave full-sync failed; "
                                "continuing with local state\n");
        }
    } else {
        printf("[RDMA-REPL] rdma replication disabled by config\n");
    }

    if (kvs_config.network_model == NET_REACTOR)
        reactor_start(kvs_config.port, kvs_protocol_batch);
    else if (kvs_config.network_model == NET_PROACTOR)
        proactor_start(kvs_config.port, kvs_protocol_batch);
    else if (kvs_config.network_model == NET_NTYCO)
        ntyco_start(kvs_config.port, kvs_protocol_batch);

    if (ENABLE_AOF) aof_async_shutdown();
    dest_kvengine();
    return 0;
}
