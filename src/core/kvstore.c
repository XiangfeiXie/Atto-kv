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
#include "wal_repl.h"
#include "persistence.h"
#include "block.h"

/* Pack first 4 bytes of a command name into a uint32_t for jump-table dispatch */
#define CMD4(a,b,c,d) ((uint32_t)(uint8_t)(a) | ((uint32_t)(uint8_t)(b) << 8) | \
                        ((uint32_t)(uint8_t)(c) << 16) | ((uint32_t)(uint8_t)(d) << 24))

/* ==================== Global dict (single storage engine) ==================== */
kv_dict_t g_dict;
int g_in_reload = 0;    /* set by persistence layer during AOF/RDB reload */

/* ==================== Utility ==================== */

void *kvs_malloc(size_t size) { return malloc(size); }
void kvs_free(void *ptr)       { free(ptr); }

/* ── Cached time: updated once per event-loop iteration ── */
static long long g_cached_ms;

static long long get_time_now_real(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000LL + tv.tv_usec / 1000;
}

long long get_time_now(void) {
    return g_cached_ms;
}

void update_cached_time(void) {
    g_cached_ms = get_time_now_real();
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

void kvs_rdma_incr_probe(const char *msg, int length) {
    wal_repl_append(msg, length);
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

/* Public wrapper: free any type-specific internal structures before dict_delete.
   Safe to call on NULL or any entry type (no-op for PACKED/RAW encodings). */
void kvs_cleanup_entry(kv_entry_t *e) {
    entry_cleanup_by_type(e);
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

    /* ── Resolve command → expected type via CMD4 jump table.
         Pack first 4 bytes into a uint32_t and switch directly.
         Only 3 conflict pairs (SINTER/SINTERSTORE, SUNION/SUNIONSTORE,
         SDIFF/SDIFFSTORE) need a secondary cmd.len check. ── */
    uint32_t c4 = 0;
    memcpy(&c4, cmd.ptr, (size_t)(cmd.len < 4 ? cmd.len : 4));
    int cmd_type = -1;
    int is_write = 0;
    switch (c4) {
    /* ── STRING ── */
    case CMD4('S','E','T',0):   cmd_type = KV_TYPE_STRING; is_write = 1; break;
    case CMD4('S','E','T','N'): cmd_type = KV_TYPE_STRING; is_write = 1; break; /* SETNX */
    case CMD4('S','E','T','E'): cmd_type = KV_TYPE_STRING; is_write = 1; break; /* SETEX */
    case CMD4('G','E','T',0):   cmd_type = KV_TYPE_STRING; break;              /* GET */
    case CMD4('G','E','T','S'): cmd_type = KV_TYPE_STRING; is_write = 1; break; /* GETSET */
    case CMD4('A','P','P','E'): cmd_type = KV_TYPE_STRING; is_write = 1; break; /* APPEND */
    case CMD4('I','N','C','R'): cmd_type = KV_TYPE_STRING; is_write = 1; break; /* INCR / INCRBY / INCRBYFLOAT */
    case CMD4('D','E','C','R'): cmd_type = KV_TYPE_STRING; is_write = 1; break; /* DECR / DECRBY */
    case CMD4('M','O','D',0):   cmd_type = KV_TYPE_STRING; is_write = 1; break; /* MOD */
    case CMD4('S','T','R','L'): cmd_type = KV_TYPE_STRING; break;              /* STRLEN */
    /* ── SET ── */
    case CMD4('S','A','D','D'): cmd_type = KV_TYPE_SET; is_write = 1; break; /* SADD */
    case CMD4('S','R','E','M'): cmd_type = KV_TYPE_SET; is_write = 1; break; /* SREM */
    case CMD4('S','P','O','P'): cmd_type = KV_TYPE_SET; is_write = 1; break; /* SPOP */
    case CMD4('S','M','O','V'): cmd_type = KV_TYPE_SET; is_write = 1; break; /* SMOVE */
    case CMD4('S','M','E','M'): cmd_type = KV_TYPE_SET; break;              /* SMEMBERS */
    case CMD4('S','C','A','R'): cmd_type = KV_TYPE_SET; break;              /* SCARD */
    case CMD4('S','I','S','M'): cmd_type = KV_TYPE_SET; break;              /* SISMEMBER */
    case CMD4('S','M','I','S'): cmd_type = KV_TYPE_SET; break;              /* SMISMEMBER */
    case CMD4('S','R','A','N'): cmd_type = KV_TYPE_SET; break;              /* SRANDMEMBER */
    case CMD4('S','I','N','T'):                                           /* SINTER / SINTERSTORE */
        cmd_type = KV_TYPE_SET; is_write = (cmd.len > 6); break;
    case CMD4('S','U','N','I'):                                           /* SUNION / SUNIONSTORE */
        cmd_type = KV_TYPE_SET; is_write = (cmd.len > 6); break;
    case CMD4('S','D','I','F'):                                           /* SDIFF / SDIFFSTORE */
        cmd_type = KV_TYPE_SET; is_write = (cmd.len > 5); break;
    /* ── HASH ── */
    case CMD4('H','S','E','T'): cmd_type = KV_TYPE_HASH; is_write = 1; break; /* HSET / HSETNX */
    case CMD4('H','D','E','L'): cmd_type = KV_TYPE_HASH; is_write = 1; break; /* HDEL */
    case CMD4('H','M','S','E'): cmd_type = KV_TYPE_HASH; is_write = 1; break; /* HMSET */
    case CMD4('H','I','N','C'): cmd_type = KV_TYPE_HASH; is_write = 1; break; /* HINCRBY / HINCRBYFLOAT */
    case CMD4('H','G','E','T'): cmd_type = KV_TYPE_HASH; break;              /* HGET / HGETALL */
    case CMD4('H','K','E','Y'): cmd_type = KV_TYPE_HASH; break;              /* HKEYS */
    case CMD4('H','V','A','L'): cmd_type = KV_TYPE_HASH; break;              /* HVALS */
    case CMD4('H','L','E','N'): cmd_type = KV_TYPE_HASH; break;              /* HLEN */
    case CMD4('H','E','X','I'): cmd_type = KV_TYPE_HASH; break;              /* HEXISTS */
    case CMD4('H','M','G','E'): cmd_type = KV_TYPE_HASH; break;              /* HMGET */
    case CMD4('H','S','T','R'): cmd_type = KV_TYPE_HASH; break;              /* HSTRLEN */
    case CMD4('H','R','A','N'): cmd_type = KV_TYPE_HASH; break;              /* HRANDFIELD */
    /* ── ZSET ── */
    case CMD4('Z','A','D','D'): cmd_type = KV_TYPE_ZSET; is_write = 1; break; /* ZADD */
    case CMD4('Z','R','E','M'): cmd_type = KV_TYPE_ZSET; is_write = 1; break; /* ZREM / ZREMRANGEBYRANK / ZREMRANGEBYSCORE */
    case CMD4('Z','I','N','C'): cmd_type = KV_TYPE_ZSET; is_write = 1; break; /* ZINCRBY */
    case CMD4('Z','P','O','P'): cmd_type = KV_TYPE_ZSET; is_write = 1; break; /* ZPOPMIN / ZPOPMAX */
    case CMD4('Z','S','C','O'): cmd_type = KV_TYPE_ZSET; break;              /* ZSCORE */
    case CMD4('Z','M','S','C'): cmd_type = KV_TYPE_ZSET; break;              /* ZMSCORE */
    case CMD4('Z','R','A','N'): cmd_type = KV_TYPE_ZSET; break;              /* ZRANGE / ZRANK / ZRANGEBYSCORE / ZRANDMEMBER */
    case CMD4('Z','R','E','V'): cmd_type = KV_TYPE_ZSET; break;              /* ZREVRANGE / ZREVRANK / ZREVRANGEBYSCORE */
    case CMD4('Z','C','A','R'): cmd_type = KV_TYPE_ZSET; break;              /* ZCARD */
    case CMD4('Z','C','O','U'): cmd_type = KV_TYPE_ZSET; break;              /* ZCOUNT */
    /* ── LIST ── */
    case CMD4('L','P','U','S'): cmd_type = KV_TYPE_LIST; is_write = 1; break; /* LPUSH / LPUSHX */
    case CMD4('L','P','O','P'): cmd_type = KV_TYPE_LIST; is_write = 1; break; /* LPOP */
    case CMD4('L','S','E','T'): cmd_type = KV_TYPE_LIST; is_write = 1; break; /* LSET */
    case CMD4('L','R','E','M'): cmd_type = KV_TYPE_LIST; is_write = 1; break; /* LREM */
    case CMD4('L','T','R','I'): cmd_type = KV_TYPE_LIST; is_write = 1; break; /* LTRIM */
    case CMD4('L','I','N','S'): cmd_type = KV_TYPE_LIST; is_write = 1; break; /* LINSERT */
    case CMD4('L','M','O','V'): cmd_type = KV_TYPE_LIST; is_write = 1; break; /* LMOVE */
    case CMD4('L','L','E','N'): cmd_type = KV_TYPE_LIST; break;              /* LLEN */
    case CMD4('L','R','A','N'): cmd_type = KV_TYPE_LIST; break;              /* LRANGE */
    case CMD4('L','I','N','D'): cmd_type = KV_TYPE_LIST; break;              /* LINDEX */
    case CMD4('L','P','O','S'): cmd_type = KV_TYPE_LIST; break;              /* LPOS */
    case CMD4('R','P','U','S'): cmd_type = KV_TYPE_LIST; is_write = 1; break; /* RPUSH / RPUSHX */
    case CMD4('R','P','O','P'): cmd_type = KV_TYPE_LIST; is_write = 1; break; /* RPOP / RPOPLPUSH */
    case CMD4('B','L','P','O'): cmd_type = KV_TYPE_LIST; is_write = 1; break; /* BLPOP */
    case CMD4('B','R','P','O'): cmd_type = KV_TYPE_LIST; is_write = 1; break; /* BRPOP */
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

    /* ── Write-command post-processing (AOF + WAL repl) ── */
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
    kvs_flush_all();  /* clean up nested structures before freeing entries */
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
    if (ENABLE_AOF) {
        /* Phase 4: WAL takes priority over legacy AOF */
        if (access(persist_path(WAL_REL), F_OK) == 0 ||
            access(persist_path(AOF_REL), F_OK) == 0) {
            printf("Loading AOF/WAL (mmap)...\n");
            aof_reload_mmap();
            return;
        }
    }
    if (ENABLE_RDB && access(persist_path(RDB_REL), F_OK) == 0) {
        printf("Loading RDB (mmap)...\n");
        rdb_load_mmap();
    } else {
        printf("Engine reload: no persistence file\n");
    }
}

/* ==================== Shutdown ==================== */

volatile sig_atomic_t g_kvs_stop = 0;

static void sig_shutdown(int signo) {
    (void)signo;
    g_kvs_stop = 1;
}

/* ==================== Main ==================== */

int main(int argc, char *argv[]) {
    signal(SIGPIPE, SIG_IGN);
    /* Use sigaction without SA_RESTART so epoll_wait/io_uring_wait
     * are interrupted (return EINTR) instead of being auto-restarted. */
    struct sigaction sa;
    sa.sa_handler = sig_shutdown;
    sa.sa_flags   = 0;          /* no SA_RESTART */
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);
    /* Line-buffer stdout early so all threads inherit it.
     * When stdout is redirected to a file it defaults to fully
     * buffered (4096+ bytes), hiding log messages from bench scripts. */
    setlinebuf(stdout);
    setlinebuf(stderr);
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
            /* Slave: fullsync + incr loop.
             * If incr reconnection fails and master requests a fresh
             * fullsync, re-run the fullsync cycle. */
            for (int fullsync_attempt = 0; fullsync_attempt < 5; fullsync_attempt++) {
                if (rdma_repl_slave_run() != 0) {
                    fprintf(stderr, "[RDMA-REPL] slave full-sync failed; "
                                    "continuing with local state\n");
                    break;
                }
                if (!rdma_repl_slave_need_fullresync())
                    break;
                printf("[RDMA-REPL] slave: fullresync requested, "
                       "re-running fullsync (attempt %d)\n",
                       fullsync_attempt + 2);
            }
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

    /* Graceful WAL flush: stop consumer thread and fsync before exit */
    if (kvs_config.rdma_sync_enabled && kvs_config.rdma_role == ROLE_MASTER)
        ebpf_incr_stop();
    else if (ENABLE_AOF)
        ebpf_incr_stop();  /* standalone WAL started by aof_async_init */
    if (ENABLE_AOF) aof_async_shutdown();
    dest_kvengine();
    return 0;
}
