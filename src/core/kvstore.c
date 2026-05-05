#include "kvstore.h"
#include "kv_type_string.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <signal.h>
#include <fcntl.h>

#include "rdma_repl.h"
#include "persistence.h"

/* ==================== Global dict (single storage engine) ==================== */
kv_dict_t g_dict;

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
    (void)msg; (void)length;
}

/* ==================== Command dispatch ==================== */

int kvs_dispatch(kvs_token_t *tokens, int argc,
                 const char *msg_raw, int msg_len,
                 DynBuf *wb)
{
    if (tokens == NULL || argc <= 0 || wb == NULL)
        return -1;

    kvs_token_t cmd = tokens[0];

    /* ── Global commands (no key needed) ── */

    if (TOKEN_EQ(cmd, "PING")) {
        dynbuf_cat(wb, RESP_PONG, RESP_PONG_LEN);
        return wb->len;
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
        return wb->len;
    }

    if (TOKEN_EQ(cmd, "SAVE")) {
        rdb_save_async_uring();
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        return wb->len;
    }

    if (TOKEN_EQ(cmd, "FLUSHALL")) {
        dict_clear(&g_dict);
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        if (ENABLE_AOF) aof_async_append(msg_raw, msg_len);
        kvs_rdma_incr_probe(msg_raw, msg_len);
        return wb->len;
    }

    /* ── Key-based commands ── */
    if (argc < 2) {
        dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
        return -1;
    }

    kvs_token_t key_tok = tokens[1];
    uint64_t hash = kv_hash((const uint8_t *)key_tok.ptr, (uint32_t)key_tok.len);
    kv_entry_t *entry = dict_find(&g_dict, key_tok.ptr, (uint32_t)key_tok.len, hash);

    int pre_len = wb->len;

    /* ── EXPIRE — type-agnostic, handle before type dispatch ── */
    if (TOKEN_EQ(cmd, "EXPIRE")) {
        if (argc < 3) {
            dynbuf_cat(wb, "-ERR wrong number of arguments for 'expire' command\r\n", 53);
            return -1;
        }
        if (!entry) {
            dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);
            return -1;
        }
        long long ms = 0;
        for (int i = 0; i < tokens[2].len; i++)
            ms = ms * 10 + (tokens[2].ptr[i] - '0');
        entry->expire = get_time_now() + ms * 1000;
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        if (ENABLE_AOF) aof_async_append(msg_raw, msg_len);
        kvs_rdma_incr_probe(msg_raw, msg_len);
        return wb->len;
    }

    if (entry) {
        /* Key exists — dispatch by entry type */
        switch (entry_type(entry)) {
        case KV_TYPE_STRING:
            string_cmd(cmd, entry, tokens + 1, argc - 1,
                       wb, msg_raw, msg_len);
            break;
        default:
            dynbuf_cat(wb, RESP_WRONGTYPE, RESP_WRONGTYPE_LEN);
            return -1;
        }
    } else {
        /* New key — dispatch by command name */
        if (TOKEN_EQ(cmd, "SET") || TOKEN_EQ(cmd, "GET") ||
            TOKEN_EQ(cmd, "DEL") || TOKEN_EQ(cmd, "MOD") ||
            TOKEN_EQ(cmd, "EXIST") || TOKEN_EQ(cmd, "EXPIRE")) {
            string_cmd(cmd, NULL, tokens + 1, argc - 1,
                       wb, msg_raw, msg_len);
        } else {
            dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
            return -1;
        }
    }

    /* ── Write-command post-processing (AOF + eBPF) ── */
    if (string_is_write_cmd(cmd)) {
        const char *resp = wb->buf + pre_len;
        int rlen = wb->len - pre_len;
        int ok = (rlen > 0 && resp[0] != '-' &&
                  !(rlen >= 5 && memcmp(resp, "$-1\r\n", 5) == 0));
        if (ok) {
            if (ENABLE_AOF) aof_async_append(msg_raw, msg_len);
            kvs_rdma_incr_probe(msg_raw, msg_len);
        }
    }
    return wb->len;
}

/* ==================== Engine lifecycle ==================== */

int init_kvengine(void) {
    dict_init(&g_dict);
    printf("Using unified dict engine\n");
    return 0;
}

void dest_kvengine(void) {
    dict_destroy(&g_dict);
}

void kvs_flush_all(void) {
    dict_clear(&g_dict);
    mallctl("arenas.purge", NULL, NULL, NULL, 0);
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
