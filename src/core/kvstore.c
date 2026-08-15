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
#include <pthread.h>
#include <time.h>
#include <ctype.h>

#include "rdma_repl.h"
#include "wal_persist.h"
#include "persistence.h"
#include "repl_incr.h"
#include "engine_xdp.h"

/* ── Global state ───────────────────────────────────────────────────── */

int g_in_reload = 0;

_Atomic uint64_t g_kvs_total_commands     = 0;
_Atomic uint64_t g_kvs_total_connections  = 0;
_Atomic int      g_kvs_connected_clients  = 0;
long long        g_kvs_start_time_ms      = 0;
volatile sig_atomic_t g_kvs_stop = 0;

/* ── TSC calibration ────────────────────────────────────────────────── */

double g_kvs_tsc_ns_per_tick = 0.0;
int    g_kvs_tsc_usable      = 0;

void kvs_tsc_calibrate(void) {
#if defined(__x86_64__) || defined(__i386__)
    double samples[5];
    for (int i = 0; i < 5; i++) {
        struct timespec a, b, sl = {0, 5 * 1000 * 1000};
        clock_gettime(CLOCK_MONOTONIC, &a);
        uint64_t ta = __builtin_ia32_rdtsc();
        nanosleep(&sl, NULL);
        uint64_t tb = __builtin_ia32_rdtsc();
        clock_gettime(CLOCK_MONOTONIC, &b);
        uint64_t ns = (uint64_t)(b.tv_sec  - a.tv_sec) * 1000000000ull
                    + (uint64_t)(b.tv_nsec - a.tv_nsec);
        uint64_t ticks = tb - ta;
        samples[i] = (ticks > 0 && ns > 0) ? (double)ns / (double)ticks : 0.0;
    }
    for (int i = 1; i < 5; i++) {
        double v = samples[i]; int j = i - 1;
        while (j >= 0 && samples[j] > v) { samples[j+1] = samples[j]; j--; }
        samples[j+1] = v;
    }
    double median = samples[2];
    if (median > 0.05 && median < 5.0) {
        g_kvs_tsc_ns_per_tick = median;
        g_kvs_tsc_usable      = 1;
        printf("[tsc] calibrated %.4f ns/tick (~%.2f GHz)\n",
               median, 1.0 / median);
        return;
    }
    fprintf(stderr, "[tsc] calibration failed (median=%.4g ns/tick), "
                    "falling back to clock_gettime\n", median);
#endif
    g_kvs_tsc_usable = 0;
}

/* ── Latency histogram ──────────────────────────────────────────────── */

struct kvs_lat_slot g_kvs_lat_slots[KVS_LAT_SLOTS] = { { {0}, 0, 0, {0} } };

static inline int kvs_lat_bucket(uint64_t ns) {
    if (ns < 1024u) return 0;
    uint64_t scaled = ns >> 10;
    int idx = 64 - __builtin_clzll(scaled);
    if (idx > KVS_LAT_BUCKETS - 1) idx = KVS_LAT_BUCKETS - 1;
    return idx;
}

static inline int kvs_lat_slot_idx(void) {
    static __thread int s_cached_slot = -1;
    if (__builtin_expect(s_cached_slot < 0, 0)) {
        uintptr_t tid = (uintptr_t)pthread_self();
        uint64_t  h   = (uint64_t)tid * 0x9E3779B97F4A7C15ull;
        s_cached_slot = (int)((h >> 56) & (KVS_LAT_SLOTS - 1));
    }
    return s_cached_slot;
}

void kvs_lat_record_ns(uint64_t ns) {
    int b = kvs_lat_bucket(ns);
    struct kvs_lat_slot *s = &g_kvs_lat_slots[kvs_lat_slot_idx()];
    atomic_fetch_add_explicit(&s->hist[b], 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&s->count,   1, memory_order_relaxed);
    atomic_fetch_add_explicit(&s->sum_ns, ns, memory_order_relaxed);
}

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
        kvs_wal_periodic();
        kvs_aof_auto_rewrite_check();
    }
    if (ENABLE_RDB) rdb_periodic();
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

void entry_cleanup_by_type(kv_entry_t *e) {
    (void)e;
    /* String entries have no internal resources to free.
     * Kept as a hook for future type extensions. */
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
    kv_iter_t *iter = dict_iter_new(&g_dict, 1);
    kv_entry_t *e;
    while ((e = dict_iter_next(iter)) != NULL)
        entry_cleanup_by_type(e);
    dict_iter_free(iter);
    dict_clear(&g_dict);
#ifdef USE_JEMALLOC
    mallctl("arenas.purge", NULL, NULL, NULL, 0);
#endif
}

void engine_reload(void) {
    if (ENABLE_AOF) {
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
    kvs_tsc_calibrate();
    persist_set_basedir(argv[0]);
    const char *conf_path = (argc >= 2) ? argv[1] : "conf/kvstore.conf";
    config_load(conf_path);
    config_print();

    init_kvengine();
    engine_reload();
    if (ENABLE_AOF) {
        kvs_wal_set_fsync_policy(kvs_config.wal_fsync_policy);
        kvs_wal_persist_start();
    }
    if (kvs_config.rdma_sync_enabled) {
        if (kvs_config.rdma_role == ROLE_MASTER)
            rdma_repl_master_start();
        else if (kvs_config.rdma_role == ROLE_SLAVE)
            rdma_repl_slave_start();
    } else {
        printf("[RDMA-REPL] rdma replication disabled by config\n");
    }

    /* ── Incremental replication ── */
    if (kvs_config.incr_sync_enabled) {
        if (kvs_config.rdma_role == ROLE_MASTER) {
            repl_backlog_init();
            repl_incr_master_start(kvs_config.incr_repl_port);
        } else if (kvs_config.rdma_role == ROLE_SLAVE) {
            int r = repl_incr_slave_start(kvs_config.rdma_master_ip,
                                           kvs_config.incr_repl_port);
            if (r < 0 && r != -2) {
                fprintf(stderr, "[INCR-REPL] slave init failed\n");
            }
        }
    }

    /* ── AF_XDP + KCP kernel-bypass incremental sync ── */
    if (kvs_config.xdp_sync_enabled) {
        if (kvs_config.rdma_role == ROLE_MASTER)
            xdp_engine_master_init();
        else if (kvs_config.rdma_role == ROLE_SLAVE)
            xdp_engine_slave_init();
    }

    if (kvs_config.network_model == NET_REACTOR)
        reactor_start(kvs_config.port, kvs_protocol_batch);
    else if (kvs_config.network_model == NET_PROACTOR)
        proactor_start(kvs_config.port, kvs_protocol_batch);
    else if (kvs_config.network_model == NET_NTYCO)
        ntyco_start(kvs_config.port, kvs_protocol_batch);

    xdp_engine_shutdown();
    kvs_wal_persist_stop();
    dest_kvengine();
    return 0;
}
