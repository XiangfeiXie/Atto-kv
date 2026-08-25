/* kvstore.c — global state, utilities, write journal, lifecycle, main */

#include "kvstore_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <signal.h>
#include <fcntl.h>
#include <ctype.h>

#include "rdma_repl.h"
#include "persist_aof.h"
#include "persistence.h"
#include "repl_incr.h"

/* ── Global state ───────────────────────────────────────────────────── */

int g_in_reload = 0;

_Atomic uint64_t g_kvs_total_commands     = 0;
_Atomic uint64_t g_kvs_total_connections  = 0;
_Atomic int      g_kvs_connected_clients  = 0;
long long        g_kvs_start_time_ms      = 0;
volatile sig_atomic_t g_kvs_stop = 0;

/* ── Cached time ────────────────────────────────────────────────────── */

static _Atomic long long g_cached_ms;

long long get_time_now_real(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000LL + tv.tv_usec / 1000;
}

long long get_time_now(void) {
    return atomic_load_explicit(&g_cached_ms, memory_order_relaxed);
}

void update_cached_time(void) {
    atomic_store_explicit(&g_cached_ms, get_time_now_real(), memory_order_relaxed);
    if (ENABLE_AOF) {
        kvs_aof_periodic();
        kvs_aof_auto_rewrite_check();
    }
    if (ENABLE_EXPIRE) active_expire_cycle();
}

/* ── Utility ────────────────────────────────────────────────────────── */

int token_to_ll(kvs_token_t t, long long *out) {
    if (t.len <= 0 || t.len > 20) return -1;
    int i = 0, neg = 0;
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

/* Pre-allocated integer responses :0\r\n..:255\r\n */
static const char *g_int_resp[256];
static char g_int_buf[256][16];

static void init_shared_responses(void) {
    for (int i = 0; i < 256; i++) {
        snprintf(g_int_buf[i], sizeof(g_int_buf[i]), ":%d\r\n", i);
        g_int_resp[i] = g_int_buf[i];
    }
}

void reply_integer(DynBuf *wb, long long v) {
    if (v >= 0 && v <= 255) {
        dynbuf_cat(wb, g_int_resp[v], (int)strlen(g_int_resp[v]));
        return;
    }
    char buf[32];
    int n = snprintf(buf, sizeof(buf), ":%lld\r\n", v);
    dynbuf_cat(wb, buf, n);
}

void reply_bulk(DynBuf *wb, const char *s, int len) {
    char hdr[24];
    int hl = snprintf(hdr, sizeof(hdr), "$%d\r\n", len);
    dynbuf_cat(wb, hdr, hl);
    if (len > 0) dynbuf_cat(wb, s, len);
    dynbuf_lit(wb, "\r\n");
}


/* ── Engine lifecycle ───────────────────────────────────────────────── */

int init_kvengine(void) {
    dict_init(&g_dict);
    init_shared_responses();
    printf("Using string storage engine (single global dict)\n");
    return 0;
}

void dest_kvengine(void) {
    kvs_flush_all();
    dict_destroy(&g_dict);
}

void kvs_flush_all(void) {
    dict_clear(&g_dict);
#ifdef USE_JEMALLOC
    mallctl("arenas.purge", NULL, NULL, NULL, 0);
#endif
}

void engine_reload(void) {
    if (ENABLE_AOF) {
        if (access(persist_path(AOF_REL), F_OK) == 0) {
            printf("Loading AOF (mmap)...\n");
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

/* ── Shutdown ───────────────────────────────────────────────────────── */

static void sig_shutdown(int signo) {
    (void)signo;
    g_kvs_stop = 1;
}

/* ── Main ───────────────────────────────────────────────────────────── */

int main(int argc, char *argv[]) {
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    sa.sa_handler = sig_shutdown;
    sa.sa_flags   = 0;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);
    setlinebuf(stdout);
    setlinebuf(stderr);
    g_kvs_start_time_ms = get_time_now_real();
    persist_set_basedir(argv[0]);
    const char *conf_path = (argc >= 2) ? argv[1] : "conf/kvstore.conf";
    config_load(conf_path);
    config_print();

    init_kvengine();
    engine_reload();
    if (ENABLE_AOF) {
        kvs_aof_set_fsync_policy(kvs_config.aof_fsync_policy);
        kvs_aof_persist_start();
    }
    /* ── 复制：先全量（可选）后增量（可选）── */
    if (kvs_config.rdma_role == ROLE_MASTER) {
        if (kvs_config.rdma_sync_enabled)
            rdma_repl_master_start();
        if (kvs_config.incr_sync_enabled) {
            repl_backlog_init();
            repl_incr_master_start(kvs_config.incr_repl_port);
        }
    } else if (kvs_config.rdma_role == ROLE_SLAVE) {
        /* 增量握手返回 FULLRESYNC 说明快照 offset 已滚出 master backlog；
         * 全量同步可用时重做一次全量同步再续传，直到成功或达到上限。 */
        int fullsync_attempts = 0;
        for (;;) {
            if (kvs_config.rdma_sync_enabled) {
                rdma_repl_slave_start();   /* 全量同步 → 设 g_repl_snapshot_offset */
                repl_incr_slave_reset_offset();   /* 增量续传从快照 offset 起 */
            }

            if (!kvs_config.incr_sync_enabled)
                break;                     /* 仅全量同步 */

            int r = repl_incr_slave_start(kvs_config.rdma_master_ip,
                                           kvs_config.incr_repl_port);
            if (r == REPL_INCR_FULLRESYNC) {
                if (kvs_config.rdma_sync_enabled &&
                    fullsync_attempts < REPL_FULLSYNC_MAX_RETRY) {
                    fullsync_attempts++;
                    fprintf(stderr, "[INCR-REPL] 快照 offset 过期，重做全量同步 (%d/%d)\n",
                            fullsync_attempts, REPL_FULLSYNC_MAX_RETRY);
                    continue;
                }
                fprintf(stderr, "[INCR-REPL] 快照 offset 过期且无法全量同步，放弃增量同步\n");
            } else if (r == REPL_INCR_ERROR) {
                fprintf(stderr, "[INCR-REPL] 增量同步初始化失败\n");
            }
            break;
        }
    }

    if (kvs_config.network_model == NET_REACTOR)
        reactor_start(kvs_config.port, kvs_protocol_batch);
    else if (kvs_config.network_model == NET_PROACTOR)
        proactor_start(kvs_config.port, kvs_protocol_batch);
    else if (kvs_config.network_model == NET_NTYCO)
        ntyco_start(kvs_config.port, kvs_protocol_batch);

    kvs_aof_persist_stop();
    dest_kvengine();
    return 0;
}
