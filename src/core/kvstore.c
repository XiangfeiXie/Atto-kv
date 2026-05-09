#include "kvstore.h"
#include "kv_type_string.h"
#include "kv_type_list.h"

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

/* ── Parse a token as long long ── */
static long long token_to_ll(kvs_token_t t) {
    long long v = 0;
    for (int i = 0; i < t.len; i++)
        v = v * 10 + (t.ptr[i] - '0');
    return v;
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

    /* ── Global commands (no key needed) ── */

    if (TOKEN_EQ(cmd, "PING")) {
        dynbuf_cat(wb, RESP_PONG, RESP_PONG_LEN);
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "QUIT")) {
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "ECHO")) {
        if (argc >= 2) {
            char hdr[32];
            int hl = snprintf(hdr, sizeof(hdr), "$%d\r\n", tokens[1].len);
            dynbuf_cat(wb, hdr, hl);
            dynbuf_cat(wb, tokens[1].ptr, tokens[1].len);
            dynbuf_cat(wb, "\r\n", 2);
        } else {
            dynbuf_cat(wb, "-ERR wrong number of arguments for 'echo' command\r\n", 50);
        }
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "SAVE")) {
        rdb_save_async_uring();
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "FLUSHALL")) {
        kv_iter_t *it = dict_iter_new(&g_dict, 1);
        if (it) {
            kv_entry_t *e;
            while ((e = dict_iter_next(it)) != NULL)
                list_cleanup_entry(e);
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

    /* ── Key-based commands ── */
    if (argc < 2) {
        dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
        return DISPATCH_ERROR;
    }

    kvs_token_t key_tok = tokens[1];
    uint64_t hash = kv_hash((const uint8_t *)key_tok.ptr, (uint32_t)key_tok.len);
    kv_entry_t *entry = dict_find(&g_dict, key_tok.ptr, (uint32_t)key_tok.len, hash);

    int pre_len = wb->len;

    /* ── EXPIRE — type-agnostic, handle before type dispatch ── */
    if (TOKEN_EQ(cmd, "EXPIRE")) {
        if (argc < 3) {
            dynbuf_cat(wb, "-ERR wrong number of arguments for 'expire' command\r\n", 53);
            return DISPATCH_ERROR;
        }
        if (!entry) {
            dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);
            return DISPATCH_OK;
        }
        entry->expire = get_time_now() + token_to_ll(tokens[2]) * 1000;
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        if (!g_in_reload) {
            if (ENABLE_AOF) aof_async_append(msg_raw, msg_len);
            kvs_rdma_incr_probe(msg_raw, msg_len);
        }
        return DISPATCH_OK;
    }

    dispatch_result_t r = DISPATCH_OK;

    if (entry) {
        /* Key exists — dispatch by entry type */
        switch (entry_type(entry)) {
        case KV_TYPE_STRING:
            string_cmd(cmd, entry, tokens + 1, argc - 1,
                       wb, msg_raw, msg_len);
            r = DISPATCH_OK;
            break;
        case KV_TYPE_LIST:
            r = list_cmd(cmd, entry, tokens + 1, argc - 1,
                        wb, msg_raw, msg_len, c ? c->client : NULL);
            break;
        default:
            dynbuf_cat(wb, RESP_WRONGTYPE, RESP_WRONGTYPE_LEN);
            return DISPATCH_ERROR;
        }
    } else {
        /* New key — dispatch by command name */
        if (TOKEN_EQ(cmd, "SET") || TOKEN_EQ(cmd, "GET") ||
            TOKEN_EQ(cmd, "DEL") || TOKEN_EQ(cmd, "MOD") ||
            TOKEN_EQ(cmd, "EXIST") || TOKEN_EQ(cmd, "EXPIRE")) {
            string_cmd(cmd, NULL, tokens + 1, argc - 1,
                       wb, msg_raw, msg_len);
            r = DISPATCH_OK;
        } else if (TOKEN_EQ(cmd, "LPUSH") || TOKEN_EQ(cmd, "RPUSH") ||
                   TOKEN_EQ(cmd, "LPOP")  || TOKEN_EQ(cmd, "RPOP") ||
                   TOKEN_EQ(cmd, "BLPOP") || TOKEN_EQ(cmd, "BRPOP") ||
                   TOKEN_EQ(cmd, "LLEN")  || TOKEN_EQ(cmd, "LRANGE") ||
                   TOKEN_EQ(cmd, "LINDEX")) {
            r = list_cmd(cmd, NULL, tokens + 1, argc - 1,
                        wb, msg_raw, msg_len, c ? c->client : NULL);
        } else {
            dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
            return DISPATCH_ERROR;
        }
    }

    if (r == DISPATCH_BLOCKED)
        return DISPATCH_BLOCKED;

    /* ── Write-command post-processing (AOF + eBPF) ── */
    if (TOKEN_EQ(cmd, "SET")   || TOKEN_EQ(cmd, "DEL")  ||
        TOKEN_EQ(cmd, "MOD")   || TOKEN_EQ(cmd, "LPUSH") ||
        TOKEN_EQ(cmd, "RPUSH") || TOKEN_EQ(cmd, "LPOP")  ||
        TOKEN_EQ(cmd, "RPOP")  || TOKEN_EQ(cmd, "BLPOP") ||
        TOKEN_EQ(cmd, "BRPOP")) {
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
            list_cleanup_entry(e);
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
