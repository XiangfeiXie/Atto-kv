#include "kvstore.h"
#include "kv_type_string.h"
#include "vector_store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>      /* strcasecmp / strncasecmp */
#include <errno.h>        /* CONFIG SET maxclients value parsing */
#include <stdbool.h>
#include <stdatomic.h>
#include <signal.h>
#include <fcntl.h>
#include <pthread.h>      /* kvs_lat_slot_idx tid hash */

#include "rdma_repl.h"
#include "wal_repl.h"
#include "journal.h"
#include "wal_persist.h"
#include "repl_stream.h"
#include "persistence.h"
#include "block.h"
#include "xdp_manager.h"
#include "sockmap_manager.h"

/* Pack first 4 bytes of a command name into a uint32_t for jump-table dispatch */
#define CMD4(a,b,c,d) ((uint32_t)(uint8_t)(a) | ((uint32_t)(uint8_t)(b) << 8) | \
                        ((uint32_t)(uint8_t)(c) << 16) | ((uint32_t)(uint8_t)(d) << 24))

/* ──────────────────────────────────────────────────────────────────
 * Phase 6.2 — shard lock guard with read/write differentiation.
 *
 * Single-key dispatch classifies the incoming command as read/write
 * BEFORE locking, then takes rdlock for reads and wrlock for writes.
 * The cleanup attribute auto-releases on every return path.
 *
 * Why this matters: with N workers and 32 shards, an all-write workload
 * has expected wait time ≈ N/32 per request once N > 32.  Adding more
 * workers past that point only adds contention, no throughput.  For
 * read-heavy workloads (the common case: GET/HGET/LRANGE) rdlock lets
 * an arbitrary number of readers run concurrently inside one shard,
 * so the per-shard ceiling becomes "writer-bound" not "worker-bound".
 *
 * Lazy-expire upgrade: if a reader finds an expired entry under rdlock,
 * we call SHARD_UPGRADE_TO_WR() to drop rdlock + take wrlock + re-find.
 * Same hash, same shard, just stronger lock.  The "re-find" is needed
 * because between drop-rd and take-wr, another writer may have already
 * deleted the entry — so we re-check rather than blindly delete.
 *
 * Callers that need to drop the lock early (e.g. before block_wake
 * which may recursively dispatch into a different shard) call
 * SHARD_RELEASE() explicitly.
 * ────────────────────────────────────────────────────────────────── */
typedef struct {
    uint64_t hash;
    int      held;
} kvs_shard_guard_t;

static inline void kvs_shard_guard_release(kvs_shard_guard_t *g) {
    if (g->held) {
        kv_shard_unlock_h(g->hash);
        g->held = 0;
    }
}

/* Take the shard lock in either rdlock (write_mode==0) or wrlock mode.
 * Declares _sg in the enclosing scope; cleanup attribute auto-releases.
 * Use exactly once per scope. */
#define SHARD_LOCK(h, write_mode)                                            \
    kvs_shard_guard_t _sg __attribute__((cleanup(kvs_shard_guard_release))) \
        = { .hash = (h), .held = 1 };                                       \
    do {                                                                     \
        if (write_mode) kv_shard_wrlock_h(h);                                \
        else            kv_shard_rdlock_h(h);                                \
    } while (0)

/* Back-compat: existing callers that need wrlock-only paths
 * (e.g. multi-key MGET/MSET loops in keyless handling). */
#define SHARD_WRLOCK(h) SHARD_LOCK((h), 1)

/* Upgrade an existing _sg (currently held as rdlock) to wrlock.
 * Drops the rdlock, then takes the wrlock on the same shard.
 * The guard remains armed so cleanup still fires on scope exit. */
#define SHARD_UPGRADE_TO_WR()                                                \
    do {                                                                     \
        if (_sg.held) { kv_shard_unlock_h(_sg.hash); _sg.held = 0; }         \
        kv_shard_wrlock_h(_sg.hash);                                         \
        _sg.held = 1;                                                        \
    } while (0)

#define SHARD_RELEASE() kvs_shard_guard_release(&_sg)

/* ==================== Global dict (single storage engine) ==================== */
/* g_dict has been replaced by sharded g_shards (see kv_dict.h). */
int g_in_reload = 0;    /* set by persistence layer during AOF/RDB reload */

/* ==================== INFO counters (declared in kvstore.h) ============= */
_Atomic uint64_t g_kvs_total_commands     = 0;
_Atomic uint64_t g_kvs_total_connections  = 0;
_Atomic int      g_kvs_connected_clients  = 0;
long long        g_kvs_start_time_ms      = 0;

/* Phase 10.7 — TSC fast clock (see kvstore.h banner). */
double g_kvs_tsc_ns_per_tick = 0.0;
int    g_kvs_tsc_usable      = 0;

void kvs_tsc_calibrate(void) {
#if defined(__x86_64__) || defined(__i386__)
    /* Median of 5 short calibration windows; rejects outliers caused by
     * a single host scheduler stall during the sleep. */
    double samples[5];
    for (int i = 0; i < 5; i++) {
        struct timespec a, b, sl = {0, 5 * 1000 * 1000};   /* 5 ms */
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
    /* simple insertion sort */
    for (int i = 1; i < 5; i++) {
        double v = samples[i]; int j = i - 1;
        while (j >= 0 && samples[j] > v) { samples[j+1] = samples[j]; j--; }
        samples[j+1] = v;
    }
    double median = samples[2];
    if (median > 0.05 && median < 5.0) {   /* 0.2 GHz – 20 GHz sanity */
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

/* Latency histogram (Phase 10.6).  See kvstore.h for bucket layout +
 * sharding rationale.  Sharded across KVS_LAT_SLOTS cacheline-aligned
 * slots to avoid false-sharing contention from the wire-path workers. */
struct kvs_lat_slot g_kvs_lat_slots[KVS_LAT_SLOTS] = { { {0}, 0, 0, {0} } };

/* Map ns to bucket index 0..15.
 *   <1024 ns  → 0
 *   else      → min(15, position-of-highest-bit(ns/1024))
 * Compiles to a single LZCNT on x86_64 with -mbmi. */
static inline int kvs_lat_bucket(uint64_t ns) {
    if (ns < 1024u) return 0;
    uint64_t scaled = ns >> 10;                       /* ns / 1024 */
    int idx = 64 - __builtin_clzll(scaled);           /* 1..64 */
    if (idx > KVS_LAT_BUCKETS - 1) idx = KVS_LAT_BUCKETS - 1;
    return idx;
}

/* Pick a slot using the calling thread's id.  pthread_self() is a
 * pointer-sized opaque value; mixing the high bits with a multiplicative
 * hash gives reasonable distribution across slots even if libc gives us
 * sequential thread IDs.  Cached in TLS so the hash is paid once per
 * worker thread, not per command. */
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

/* ==================== Vector cache/search layer ====================
 * AI local cache mode keeps STRING as a lightweight exact-key cache and
 * adds a separate in-memory vector store for semantic nearest-neighbour
 * lookup.  The old short-value fastpath cache is intentionally removed
 * from the main runtime path. */

/* ==================== Utility ==================== */

void *kvs_malloc(size_t size) { return malloc(size); }
void kvs_free(void *ptr)       { free(ptr); }

/* ── Cached time ──
 *
 * Updated by worker 0 once per event-loop iteration (see proactor.c
 * worker_loop tick handler), read by every worker on every TTL/expire
 * check.  Phase 8.2: declared _Atomic so the writer/reader contract is
 * explicit (relaxed ordering is sufficient — we don't need any
 * happens-before from this value, just a torn-read-free 64-bit load).
 * On x86_64 this compiles to a plain mov; the only practical effect is
 * giving TSAN visibility into the unsynchronised cross-thread access. */
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
    atomic_store_explicit(&g_cached_ms,
                          get_time_now_real(),
                          memory_order_relaxed);
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

static inline void kvs_write_journal_append_kind(uint16_t kind, const char *msg, int length) {
    uint64_t seq = 0;
    if (!msg || length <= 0) return;
    if (kvs_journal_append(kind, 0, msg, (uint32_t)length, &seq) == 0) {
        kvs_journal_record_t rec;
        if (kvs_journal_copy(seq, &rec) == 0) {
            kvs_wal_persist_append(&rec);
            repl_stream_publish(&rec);
            kvs_journal_record_release(&rec);
        }
    }
    wal_repl_append(msg, length);
}

static inline void kvs_write_journal_append(const char *msg, int length) {
    kvs_write_journal_append_kind(KVS_MUT_KIND_RESP_STRING, msg, length);
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
    (void)e;
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
    dynbuf_lit(wb, "\r\n");
}

/* =====================================================================
 * Phase 7.2 — CONFIG GET / SET / RESETSTAT / REWRITE
 *
 * Online runtime tuning.  Two classes of params:
 *   1. Read-only at runtime — port, persistence, network, threads,
 *      cpu_pin, xdp, rdma_*.  CONFIG GET returns them; CONFIG SET
 *      refuses with an explanatory error (would require socket
 *      rebind / thread pool restart / module reinit — out of scope
 *      for an online command).
 *   2. Online-settable — loglevel, maxclients.  CONFIG SET writes
 *      via __atomic_store_n(_, _, __ATOMIC_RELAXED) so concurrent
 *      worker reads stay TSAN-clean and benefit from the new value
 *      on their next iteration without any synchronisation cost.
 *
 * Lookup is via a small static table keyed on lowercase name.
 * Values are formatted on demand into a per-call buffer; the table
 * itself stays read-only across the process lifetime.
 *
 * Format follows Redis: CONFIG GET param → flat array [name,value,...]
 * (one pair per matched param; we don't currently support glob, just
 * exact name match, which is what every driver and redis-cli uses).
 * ===================================================================== */

/* Format helpers — return number of bytes written into out. */
static int fmt_loglevel(char *out, int cap) {
    int v = __atomic_load_n(&kvs_config.loglevel, __ATOMIC_RELAXED);
    const char *s = (v == LOG_DEBUG) ? "debug" :
                    (v == LOG_INFO)  ? "info"  :
                    (v == LOG_WARN)  ? "warn"  : "error";
    int n = (int)strlen(s);
    if (n > cap) n = cap;
    memcpy(out, s, n);
    return n;
}
static int fmt_maxclients(char *out, int cap) {
    int v = __atomic_load_n(&kvs_config.maxclients, __ATOMIC_RELAXED);
    return snprintf(out, cap, "%d", v);
}
static int fmt_persistence(char *out, int cap) {
    const char *s = (kvs_config.persist_mode == PERSIST_NONE) ? "none" :
                    (kvs_config.persist_mode == PERSIST_RDB)  ? "rdb"  : "aof";
    int n = (int)strlen(s);
    if (n > cap) n = cap;
    memcpy(out, s, n);
    return n;
}
static int fmt_threads(char *out, int cap) {
    return snprintf(out, cap, "%d", kvs_config.threads);
}
static int fmt_port(char *out, int cap) {
    return snprintf(out, cap, "%d", kvs_config.port);
}
static int fmt_xdp(char *out, int cap) {
    const char *s = kvs_config.xdp_enabled ? "yes" : "no";
    int n = (int)strlen(s);
    if (n > cap) n = cap;
    memcpy(out, s, n);
    return n;
}
static int fmt_appendonly(char *out, int cap) {
    const char *s = ENABLE_AOF ? "yes" : "no";
    int n = (int)strlen(s);
    if (n > cap) n = cap;
    memcpy(out, s, n);
    return n;
}
static int fmt_save(char *out, int cap) {
    const char *s = ENABLE_RDB ? "900 1 300 10 60 10000" : "";
    int n = (int)strlen(s);
    if (n > cap) n = cap;
    memcpy(out, s, n);
    return n;
}
static int fmt_cpu_pin(char *out, int cap) {
    if (kvs_config.cpu_pin_mode == 0) {
        const char *s = "off"; int n = 3;
        if (n > cap) n = cap; memcpy(out, s, n); return n;
    }
    if (kvs_config.cpu_pin_mode == 1) {
        const char *s = "auto"; int n = 4;
        if (n > cap) n = cap; memcpy(out, s, n); return n;
    }
    /* manual list */
    int off = 0;
    for (int i = 0; i < kvs_config.cpu_pin_count && off + 8 < cap; i++) {
        off += snprintf(out + off, cap - off, "%s%d",
                        i ? "," : "", kvs_config.cpu_pin_list[i]);
    }
    return off;
}

/* SET handlers — return 0 on success, -1 with error string written via wb. */
static int set_loglevel(const char *val, DynBuf *wb) {
    int v = -1;
    if      (strcasecmp(val, "debug") == 0) v = LOG_DEBUG;
    else if (strcasecmp(val, "info")  == 0) v = LOG_INFO;
    else if (strcasecmp(val, "warn")  == 0) v = LOG_WARN;
    else if (strcasecmp(val, "error") == 0) v = LOG_ERROR;
    else {
        dynbuf_lit(wb, "-ERR loglevel must be one of: debug info warn error\r\n");
        return -1;
    }
    __atomic_store_n(&kvs_config.loglevel, v, __ATOMIC_RELAXED);
    return 0;
}
static int set_maxclients(const char *val, DynBuf *wb) {
    char *end; errno = 0;
    long v = strtol(val, &end, 10);
    if (*end != '\0' || errno != 0 || v < 64 || v > 65536) {
        dynbuf_lit(wb, "-ERR maxclients must be an integer in [64, 65536]\r\n");
        return -1;
    }
    __atomic_store_n(&kvs_config.maxclients, (int)v, __ATOMIC_RELAXED);
    return 0;
}

/* Param table.  fmt() prints current value; set() applies change (NULL
 * means read-only at runtime).  Names match the .conf file syntax. */
typedef int (*cfg_fmt_fn)(char *out, int cap);
typedef int (*cfg_set_fn)(const char *val, DynBuf *wb);
struct cfg_entry {
    const char *name;
    cfg_fmt_fn  fmt;
    cfg_set_fn  set;       /* NULL → read-only */
};
static const struct cfg_entry g_cfg_table[] = {
    /* online-settable */
    { "loglevel",     fmt_loglevel,    set_loglevel    },
    { "maxclients",   fmt_maxclients,  set_maxclients  },
    /* read-only at runtime (boot-time only) */
    { "persistence",  fmt_persistence, NULL },
    { "threads",      fmt_threads,     NULL },
    { "port",         fmt_port,        NULL },
    { "xdp",          fmt_xdp,         NULL },
    { "appendonly",   fmt_appendonly,  NULL },
    { "save",         fmt_save,        NULL },
    { "cpu_pin",      fmt_cpu_pin,     NULL },
};
static const int g_cfg_table_n =
    (int)(sizeof(g_cfg_table) / sizeof(g_cfg_table[0]));

static const struct cfg_entry *cfg_lookup(const kvs_token_t *t) {
    for (int i = 0; i < g_cfg_table_n; i++) {
        const char *n = g_cfg_table[i].name;
        int nl = (int)strlen(n);
        if (nl == t->len && strncasecmp(n, t->ptr, (size_t)nl) == 0)
            return &g_cfg_table[i];
    }
    return NULL;
}

static dispatch_result_t handle_config(kvs_token_t *tokens, int argc, DynBuf *wb) {
    if (argc < 2) {
        dynbuf_cat(wb,
            "-ERR wrong number of arguments for 'config' command\r\n",
            sizeof("-ERR wrong number of arguments for 'config' command\r\n") - 1);
        return DISPATCH_ERROR;
    }

    /* CONFIG subcommand names are also case-insensitive in Redis. */
    kvs_token_t sub = tokens[1];

    if (sub.len == 3 && strncasecmp(sub.ptr, "GET", 3) == 0) {
        if (argc != 3) {
            dynbuf_cat(wb,
                "-ERR wrong number of arguments for 'config|get' command\r\n",
                sizeof("-ERR wrong number of arguments for 'config|get' command\r\n") - 1);
            return DISPATCH_ERROR;
        }
        const struct cfg_entry *e = cfg_lookup(&tokens[2]);
        if (!e) {
            /* Unknown name → empty array, like Redis. */
            dynbuf_lit(wb, "*0\r\n");
            return DISPATCH_OK;
        }
        char vbuf[256];
        int  vlen = e->fmt(vbuf, (int)sizeof(vbuf));
        if (vlen < 0) vlen = 0;
        if (vlen > (int)sizeof(vbuf)) vlen = (int)sizeof(vbuf);
        dynbuf_lit(wb, "*2\r\n");
        reply_bulk(wb, e->name, (int)strlen(e->name));
        reply_bulk(wb, vbuf, vlen);
        return DISPATCH_OK;
    }

    if (sub.len == 3 && strncasecmp(sub.ptr, "SET", 3) == 0) {
        if (argc != 4) {
            dynbuf_cat(wb,
                "-ERR wrong number of arguments for 'config|set' command\r\n",
                sizeof("-ERR wrong number of arguments for 'config|set' command\r\n") - 1);
            return DISPATCH_ERROR;
        }
        const struct cfg_entry *e = cfg_lookup(&tokens[2]);
        if (!e) {
            dynbuf_cat(wb,
                "-ERR Unknown CONFIG parameter\r\n",
                sizeof("-ERR Unknown CONFIG parameter\r\n") - 1);
            return DISPATCH_ERROR;
        }
        if (!e->set) {
            dynbuf_cat(wb,
                "-ERR parameter is read-only at runtime "
                "(boot-time only — restart with updated .conf)\r\n",
                sizeof("-ERR parameter is read-only at runtime "
                       "(boot-time only — restart with updated .conf)\r\n") - 1);
            return DISPATCH_ERROR;
        }
        /* Materialise value into a NUL-terminated buffer for parsing. */
        char vbuf[192];
        int vlen = tokens[3].len;
        if (vlen >= (int)sizeof(vbuf)) vlen = (int)sizeof(vbuf) - 1;
        memcpy(vbuf, tokens[3].ptr, (size_t)vlen);
        vbuf[vlen] = '\0';
        if (e->set(vbuf, wb) < 0)
            return DISPATCH_ERROR;          /* set() already wrote -ERR */
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        return DISPATCH_OK;
    }

    if (sub.len == 9 && strncasecmp(sub.ptr, "RESETSTAT", 9) == 0) {
        /* No internal stats counter yet — accept, like a no-op. */
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        return DISPATCH_OK;
    }

    if (sub.len == 7 && strncasecmp(sub.ptr, "REWRITE", 7) == 0) {
        /* Rewriting the .conf file in-place needs careful merge logic
         * (preserve comments, only update changed keys).  Out of scope
         * for Phase 7.2; report unsupported so callers fall back. */
        dynbuf_cat(wb,
            "-ERR CONFIG REWRITE not implemented "
            "(use external config management)\r\n",
            sizeof("-ERR CONFIG REWRITE not implemented "
                   "(use external config management)\r\n") - 1);
        return DISPATCH_ERROR;
    }

    dynbuf_cat(wb,
        "-ERR Unknown CONFIG subcommand "
        "(supported: GET, SET, RESETSTAT, REWRITE)\r\n",
        sizeof("-ERR Unknown CONFIG subcommand "
               "(supported: GET, SET, RESETSTAT, REWRITE)\r\n") - 1);
    return DISPATCH_ERROR;
}

/* Note: the saved-command replay (formerly block_retry_client) now lives
 * in proactor.c proactor_block_notify_local so that it runs strictly on
 * the conn's owner worker.  See client.h block_cmd_buf comment. */

/* =====================================================================
 * Phase 9 — INFO command
 *
 * Emits a Redis-compatible INFO dump as a single bulk string.  The body
 * is built into a stack buffer (capped at 4 KB which fits all sections
 * we currently populate), then framed as `$<len>\r\n<body>\r\n`.
 *
 * Sections:
 *   server       — version, process id, port, uptime, network model, threads
 *   clients      — connected_clients, maxclients
 *   memory       — used_memory (jemalloc stats.allocated when available)
 *   persistence  — aof_enabled, rdb_enabled, in_reload state
 *   stats        — total_connections_received, total_commands_processed
 *   replication  — role, rdma_sync_enabled, provider, master/listen address
 *   cpu          — used_cpu_sys, used_cpu_user (rusage)
 *   keyspace     — db0:keys=N
 *
 * `INFO` (no arg)        → all sections
 * `INFO <section>`        → just that section (case-insensitive prefix match)
 * `INFO everything|all`   → same as INFO (Redis-compat alias)
 *
 * Cost: a few formatted snprintfs + one mallctl + one getrusage.  Called
 * by clients on connect and occasionally; not on the hot path. */
#include <sys/resource.h>
#include <sys/utsname.h>

static int section_match(kvs_token_t t, const char *name) {
    size_t n = strlen(name);
    if ((size_t)t.len != n) return 0;
    return strncasecmp(t.ptr, name, n) == 0;
}

static dispatch_result_t handle_info(kvs_token_t *tokens, int argc, DynBuf *wb) {
    /* Argument parsing: zero/one arg supported.  Unknown section → empty body. */
    int want_server      = 1;
    int want_clients     = 1;
    int want_memory      = 1;
    int want_persistence = 1;
    int want_stats       = 1;
    int want_replication = 1;
    int want_cpu         = 1;
    int want_keyspace    = 1;

    if (argc >= 2) {
        kvs_token_t s = tokens[1];
        if (!section_match(s, "all") && !section_match(s, "everything") &&
            !section_match(s, "default")) {
            want_server = section_match(s, "server");
            want_clients = section_match(s, "clients");
            want_memory = section_match(s, "memory");
            want_persistence = section_match(s, "persistence");
            want_stats = section_match(s, "stats");
            want_replication = section_match(s, "replication");
            want_cpu = section_match(s, "cpu");
            want_keyspace = section_match(s, "keyspace");
        }
    }

    /* Build body into a stack buffer.  4 KB is comfortably above the sum of
     * fixed strings + ~16 numeric fields × 32 bytes. */
    char body[4096];
    int  off = 0;
    #define APPEND(...) do {                                                 \
        int _n = snprintf(body + off, sizeof(body) - off, __VA_ARGS__);      \
        if (_n < 0 || _n >= (int)(sizeof(body) - off)) goto truncated;       \
        off += _n;                                                            \
    } while (0)

    long long now_ms = get_time_now_real();
    long long uptime_s = (now_ms - g_kvs_start_time_ms) / 1000;

    if (want_server) {
        struct utsname un = {0};
        uname(&un);
        APPEND("# Server\r\n");
        APPEND("redis_version:9.1.0-kvstore\r\n");
        APPEND("kvstore_version:10.0\r\n");
        APPEND("redis_mode:standalone\r\n");
        APPEND("os:%s %s %s\r\n", un.sysname, un.release, un.machine);
        APPEND("arch_bits:64\r\n");
        APPEND("process_id:%d\r\n", (int)getpid());
        APPEND("tcp_port:%d\r\n", kvs_config.port);
        APPEND("uptime_in_seconds:%lld\r\n", uptime_s);
        APPEND("uptime_in_days:%lld\r\n", uptime_s / 86400);
        const char *netname =
            (kvs_config.network_model == NET_PROACTOR) ? "proactor (io_uring)" :
            (kvs_config.network_model == NET_REACTOR)  ? "reactor (epoll)"     :
                                                         "ntyco (coroutine)";
        APPEND("network_model:%s\r\n", netname);
        APPEND("workers:%d\r\n", kvs_config.threads > 0 ? kvs_config.threads : 1);
        APPEND("shards:%u\r\n", KV_NR_SHARDS);
        APPEND("\r\n");
    }

    if (want_clients) {
        APPEND("# Clients\r\n");
        APPEND("connected_clients:%d\r\n",
               atomic_load_explicit(&g_kvs_connected_clients,
                                    memory_order_relaxed));
        APPEND("maxclients:%d\r\n",
               __atomic_load_n(&kvs_config.maxclients, __ATOMIC_RELAXED));
        APPEND("\r\n");
    }

    if (want_memory) {
        size_t allocated = 0;
        const char *allocator = "libc";
#ifdef USE_JEMALLOC
        size_t sz = sizeof(allocated);
        /* je_mallctl epoch refresh required for stats to be current. */
        uint64_t epoch = 1;
        size_t esz = sizeof(epoch);
        mallctl("epoch", &epoch, &esz, &epoch, esz);
        if (mallctl("stats.allocated", &allocated, &sz, NULL, 0) != 0)
            allocated = 0;
        allocator = "jemalloc";
#else
        /* getrusage ru_maxrss is in KB on Linux; convert to bytes. */
        struct rusage ru;
        if (getrusage(RUSAGE_SELF, &ru) == 0)
            allocated = (size_t)ru.ru_maxrss * 1024;
#endif
        APPEND("# Memory\r\n");
        APPEND("used_memory:%zu\r\n", allocated);
        APPEND("used_memory_human:%.2fM\r\n", (double)allocated / (1024.0 * 1024.0));
        APPEND("mem_allocator:%s\r\n", allocator);
        APPEND("\r\n");
    }

    if (want_persistence) {
        APPEND("# Persistence\r\n");
        APPEND("aof_enabled:%d\r\n", ENABLE_AOF ? 1 : 0);
        APPEND("rdb_enabled:%d\r\n", ENABLE_RDB ? 1 : 0);
        APPEND("loading:%d\r\n", g_in_reload ? 1 : 0);
        APPEND("\r\n");
    }

    if (want_stats) {
        APPEND("# Stats\r\n");
        APPEND("total_connections_received:%llu\r\n",
               (unsigned long long)atomic_load_explicit(&g_kvs_total_connections,
                                                         memory_order_relaxed));
        APPEND("total_commands_processed:%llu\r\n",
               (unsigned long long)atomic_load_explicit(&g_kvs_total_commands,
                                                         memory_order_relaxed));
        APPEND("vector_store_enabled:1\r\n");
        APPEND("vector_store_count:%llu\r\n",
               (unsigned long long)vector_store_count());

        /* Phase 10.6 — dispatch latency histogram (16 log2-spaced buckets,
         * 1 µs .. ≥16 ms).  Emit per-bucket counts for raw consumption
         * (Prometheus, plotting, etc.) plus derived p50/p99/p999/mean for
         * humans reading INFO directly.  Counters are sharded across
         * KVS_LAT_SLOTS cachelines to avoid writer contention; INFO sums
         * them on read.  Tearing across slots/buckets is acceptable for
         * percentile estimation at this granularity. */
        uint64_t lat_buckets[KVS_LAT_BUCKETS] = {0};
        uint64_t lat_total = 0;
        uint64_t lat_sum   = 0;
        for (int s = 0; s < KVS_LAT_SLOTS; s++) {
            for (int i = 0; i < KVS_LAT_BUCKETS; i++) {
                lat_buckets[i] += atomic_load_explicit(
                        &g_kvs_lat_slots[s].hist[i], memory_order_relaxed);
            }
            lat_total += atomic_load_explicit(&g_kvs_lat_slots[s].count,
                                               memory_order_relaxed);
            lat_sum   += atomic_load_explicit(&g_kvs_lat_slots[s].sum_ns,
                                               memory_order_relaxed);
        }
        /* Phase 10.8 — wire path samples 1-in-64 dispatches; surface that
         * rate so external tools can scale counters to estimate true volume. */
        APPEND("latency_sample_rate:64\r\n");
        APPEND("latency_total_samples:%llu\r\n", (unsigned long long)lat_total);
        if (lat_total > 0) {
            APPEND("latency_mean_us:%.2f\r\n",
                   (double)lat_sum / (double)lat_total / 1000.0);
        } else {
            APPEND("latency_mean_us:0.00\r\n");
        }
        /* Bucket upper bounds in microseconds. B0 covers <1 µs; subsequent
         * boundaries double.  Bucket counts emitted as latency_us_le_<N>. */
        static const uint64_t bucket_upper_us[KVS_LAT_BUCKETS] = {
            1, 2, 4, 8, 16, 32, 64, 128, 256, 512,
            1024, 2048, 4096, 8192, 16384, 0 /* +Inf */
        };
        for (int i = 0; i < KVS_LAT_BUCKETS; i++) {
            if (bucket_upper_us[i] == 0) {
                APPEND("latency_us_le_inf:%llu\r\n",
                       (unsigned long long)lat_buckets[i]);
            } else {
                APPEND("latency_us_le_%llu:%llu\r\n",
                       (unsigned long long)bucket_upper_us[i],
                       (unsigned long long)lat_buckets[i]);
            }
        }
        /* Percentile estimation: walk the cumulative distribution and pick
         * the bucket whose CDF crosses each target.  Reported as the bucket's
         * upper bound — a conservative (i.e. never-underestimating) estimate. */
        if (lat_total > 0) {
            const double pcts[3] = { 0.50, 0.99, 0.999 };
            const char  *names[3] = { "p50", "p99", "p999" };
            for (int p = 0; p < 3; p++) {
                uint64_t target = (uint64_t)((double)lat_total * pcts[p]);
                if (target == 0) target = 1;
                uint64_t cum = 0;
                int picked = KVS_LAT_BUCKETS - 1;
                for (int i = 0; i < KVS_LAT_BUCKETS; i++) {
                    cum += lat_buckets[i];
                    if (cum >= target) { picked = i; break; }
                }
                if (bucket_upper_us[picked] == 0) {
                    APPEND("latency_%s_us:+inf\r\n", names[p]);
                } else {
                    APPEND("latency_%s_us:%llu\r\n",
                           names[p],
                           (unsigned long long)bucket_upper_us[picked]);
                }
            }
        } else {
            APPEND("latency_p50_us:0\r\n");
            APPEND("latency_p99_us:0\r\n");
            APPEND("latency_p999_us:0\r\n");
        }

        APPEND("\r\n");
    }

    if (want_replication) {
        APPEND("# Replication\r\n");
        APPEND("role:%s\r\n", kvs_config.rdma_role == ROLE_MASTER ? "master" : "slave");
        APPEND("rdma_sync_enabled:%d\r\n", kvs_config.rdma_sync_enabled ? 1 : 0);
        if (kvs_config.rdma_sync_enabled) {
            APPEND("rdma_provider:%s\r\n", kvs_config.rdma_provider);
            if (kvs_config.rdma_role == ROLE_MASTER) {
                APPEND("rdma_listen:%s:%d\r\n",
                       kvs_config.rdma_listen_ip, kvs_config.rdma_listen_port);
            } else {
                APPEND("master_host:%s\r\n", kvs_config.rdma_master_ip);
                APPEND("master_port:%d\r\n", kvs_config.rdma_master_port);
            }
        }
        APPEND("connected_slaves:0\r\n");
        APPEND("\r\n");
    }

    if (want_cpu) {
        struct rusage ru;
        if (getrusage(RUSAGE_SELF, &ru) == 0) {
            double sys_s = ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1e6;
            double usr_s = ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1e6;
            APPEND("# CPU\r\n");
            APPEND("used_cpu_sys:%.6f\r\n", sys_s);
            APPEND("used_cpu_user:%.6f\r\n", usr_s);
            APPEND("\r\n");
        }
    }

    if (want_keyspace) {
        uint32_t nkeys = kv_shards_count();
        APPEND("# Keyspace\r\n");
        if (nkeys > 0)
            APPEND("db0:keys=%u,expires=0,avg_ttl=0\r\n", nkeys);
        APPEND("\r\n");
    }

truncated:
    /* Frame as RESP bulk string. */
    {
        char hdr[32];
        int hl = snprintf(hdr, sizeof(hdr), "$%d\r\n", off);
        dynbuf_cat(wb, hdr, hl);
        dynbuf_cat(wb, body, off);
        dynbuf_lit(wb, "\r\n");
    }
    #undef APPEND
    return DISPATCH_OK;
}

/* ==================== Command dispatch ==================== */

dispatch_result_t kvs_dispatch(kvs_token_t *tokens, int argc,
                               const char *msg_raw, int msg_len,
                               DynBuf *wb, struct conn *c)
{
    if (tokens == NULL || argc <= 0 || wb == NULL)
        return DISPATCH_ERROR;

    /* Phase 9 — INFO total_commands_processed.  Bump before dispatch even
     * for unknown / malformed commands; INFO reports the wire activity rate
     * regardless of correctness, matching Redis semantics. */
    atomic_fetch_add_explicit(&g_kvs_total_commands, 1, memory_order_relaxed);

    kvs_token_t cmd = tokens[0];

    /* Phase 6.2b — case-insensitive command name.
     *
     * Redis protocol mandates command names are case-insensitive;
     * `SET k v`, `set k v`, `Set k v` are all valid and equivalent.
     * Our previous dispatcher matched raw bytes (TOKEN_EQ memcmp +
     * CMD4 first-4-byte switch), so lowercase commands returned
     * "ERR unknown command".
     *
     * Fix: uppercase the command token into a 32-byte stack buffer
     * once on entry, then both the keyless TOKEN_EQ chain and the
     * CMD4 jump-table see the canonical form.  We also overwrite
     * tokens[0] so the per-type dispatchers (string_cmd / hash_cmd
     * / list_cmd / set_cmd / zset_cmd) — which all run their own
     * TOKEN_EQ on cmd.ptr — see the same canonicalised cmd.
     *
     * ASCII A-Z conversion: bit 5 (0x20) distinguishes uppercase
     * from lowercase ASCII letters.  Bytes outside [a-z] are left
     * unchanged, so digits / hyphens (nonexistent in Redis command
     * names anyway) stay byte-identical.  Cost: ≤16 byte mask per
     * dispatch ≈ 5 ns.  Pre-existing bug found in Phase 6.2; safe
     * to fix here because no existing call site relies on case-
     * sensitive command rejection. */
    char ucmd_buf[32];
    int  ulen = cmd.len;
    if (ulen > (int)sizeof(ucmd_buf)) ulen = (int)sizeof(ucmd_buf);
    for (int i = 0; i < ulen; i++) {
        unsigned char ch = (unsigned char)cmd.ptr[i];
        ucmd_buf[i] = (ch >= 'a' && ch <= 'z') ? (char)(ch & 0xDF)
                                                : (char)ch;
    }
    cmd.ptr   = ucmd_buf;
    cmd.len   = ulen;
    tokens[0] = cmd;

    /* Phase 8.2 — client_t is now eagerly allocated in proactor.c on
     * EVENT_ACCEPT, so c->client is non-NULL by the time any command
     * dispatches.  See proactor.c "Phase 8.2 — eager client_t" block.
     * Reload paths and BLPOP wake replays may pass c==NULL; handle
     * that defensively. */

    /* ── Global / connection commands (no key needed) ── */

    if (TOKEN_EQ(cmd, "PING")) {
        if (argc == 1) {
            dynbuf_cat(wb, RESP_PONG, RESP_PONG_LEN);
        } else if (argc == 2) {
            /* Redis: PING <msg> echoes msg as bulk reply */
            reply_bulk(wb, tokens[1].ptr, tokens[1].len);
        } else {
            dynbuf_lit(wb, "-ERR wrong number of arguments for 'ping' command\r\n");
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
            dynbuf_lit(wb, "-ERR wrong number of arguments for 'echo' command\r\n");
        }
        return DISPATCH_OK;
    }

    /* HELLO — clients use this to negotiate RESP3. Decline gracefully so
       clients fall back to RESP2 without erroring out. */
    if (TOKEN_EQ(cmd, "HELLO")) {
        dynbuf_lit(wb, "-NOPROTO sorry, this protocol version is not supported\r\n");
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
            dynbuf_lit(wb, "*0\r\n");
        }
        return DISPATCH_OK;
    }

    /* VECTOR.* — local AI cache + flat vector search command family. */
    if (cmd.len >= 7 && memcmp(cmd.ptr, "VECTOR.", 7) == 0) {
        vector_mutation_buf_t vmut = {0};
        int is_vector_write = vector_cmd_is_write(cmd);
        int have_vmut = 0;
        if (is_vector_write && !g_in_reload &&
            vector_mutation_encode_from_cmd(cmd, tokens, argc, &vmut) == 0)
            have_vmut = 1;
        dispatch_result_t vr = vector_cmd(cmd, tokens, argc, wb);
        if (vr == DISPATCH_OK && have_vmut) {
            kvs_write_journal_append_kind(KVS_MUT_KIND_VECTOR_BIN, vmut.buf, (int)vmut.len);
        }
        vector_mutation_buf_free(&vmut);
        return vr;
    }

    /* SELECT — single-DB server: accept "SELECT 0", reject others. */
    if (TOKEN_EQ(cmd, "SELECT")) {
        long long db = 0;
        if (argc != 2 || token_to_ll(tokens[1], &db) < 0 || db != 0) {
            dynbuf_lit(wb, "-ERR DB index out of range\r\n");
        } else {
            dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        }
        return DISPATCH_OK;
    }

    /* DBSIZE — count of keys in dict. */
    if (TOKEN_EQ(cmd, "DBSIZE")) {
        reply_integer(wb, (long long)kv_shards_count());
        return DISPATCH_OK;
    }

    if (TOKEN_EQ(cmd, "SAVE") || TOKEN_EQ(cmd, "BGSAVE")) {
        rdb_save_async_uring();
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        return DISPATCH_OK;
    }

    /* Phase 7.1.0 — BGREWRITEAOF stub.  See persistence.c Part 2b for
     * the V2 design plan.  Currently aof_rewrite_bg() always returns
     * -1 (NOT IMPLEMENTED); on this dev box AOF never grows large
     * enough for rewrite to matter, so the stub is intentional. */
    if (TOKEN_EQ(cmd, "BGREWRITEAOF")) {
        if (aof_rewrite_bg() == 0) {
            dynbuf_lit(wb, "+Background append only file rewriting started\r\n");
        } else {
            dynbuf_lit(wb, "-ERR BGREWRITEAOF not implemented yet "
                "(see persistence.c Part 2b for design plan)\r\n");
        }
        return DISPATCH_OK;
    }

    /* Phase 7.2 — online runtime config tuning. */
    if (TOKEN_EQ(cmd, "CONFIG")) {
        return handle_config(tokens, argc, wb);
    }

    /* Phase 9 — INFO server/clients/memory/etc. */
    if (TOKEN_EQ(cmd, "INFO")) {
        return handle_info(tokens, argc, wb);
    }

    if (TOKEN_EQ(cmd, "FLUSHALL") || TOKEN_EQ(cmd, "FLUSHDB")) {
        /* Walk every shard under its own rdlock, then wrlock-clear all. */
        kv_shards_iter_t it; kv_shards_iter_init(&it);
        kv_entry_t *e;
        while ((e = kv_shards_iter_next(&it)) != NULL)
            entry_cleanup_by_type(e);
        kv_shards_iter_free(&it);
        kv_shards_clear();
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        if (!g_in_reload) {
            kvs_write_journal_append(msg_raw, msg_len);
            if (kvs_config.xdp_enabled) xdp_cache_flush_all();
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
            uint64_t kh = kv_hash((const uint8_t *)tokens[i].ptr,
                                  (uint32_t)tokens[i].len);
            /* Phase 6.2 — MGET is pure read; take rdlock to allow concurrent
             * readers on the same shard.  string_engine_get only does dict_find
             * (no mutation), so rdlock is safe even though it'd skip the lazy
             * lazy-expire delete — an expired key just reads as nil here. */
            SHARD_LOCK(kh, 0);
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

    /* MSET key val [key val ...] → always +OK (overwrites any existing type). */
    if (TOKEN_EQ(cmd, "MSET")) {
        if (argc < 3 || ((argc - 1) % 2) != 0) {
            dynbuf_lit(wb, "-ERR wrong number of arguments for 'mset' command\r\n");
            return DISPATCH_ERROR;
        }
        for (int i = 1; i + 1 < argc; i += 2) {
            /* If a non-string entry exists at this key, drop it first so the
             * overwrite is type-safe (Redis MSET replaces any existing type). */
            uint64_t kh = kv_hash((const uint8_t *)tokens[i].ptr,
                                  (uint32_t)tokens[i].len);
            SHARD_WRLOCK(kh);
            kv_entry_t *ke = dict_find(kv_dict_for(kh), tokens[i].ptr, (uint32_t)tokens[i].len, kh);
            if (ke && entry_type(ke) != KV_TYPE_STRING) {
                entry_cleanup_by_type(ke);
                dict_delete(kv_dict_for(kh), tokens[i].ptr, (uint32_t)tokens[i].len, kh);
            }
            string_engine_set(tokens[i].ptr, (uint32_t)tokens[i].len,
                              tokens[i+1].ptr, (uint32_t)tokens[i+1].len,
                              0, 0);
        }
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        if (!g_in_reload) {
            kvs_write_journal_append(msg_raw, msg_len);
        }
        return DISPATCH_OK;
    }

    /* MSETNX key val [key val ...] → :1 if all keys absent and set, else :0 */
    if (TOKEN_EQ(cmd, "MSETNX")) {
        if (argc < 3 || ((argc - 1) % 2) != 0) {
            dynbuf_lit(wb, "-ERR wrong number of arguments for 'msetnx' command\r\n");
            return DISPATCH_ERROR;
        }
        /* First pass: any existing (non-expired) key aborts the whole op.
         * Note: with multiple workers, two concurrent MSETNX on overlapping
         * key-sets may both succeed if their pre-checks interleave; this is
         * the same as Redis's documented behaviour without WATCH. */
        for (int i = 1; i + 1 < argc; i += 2) {
            uint64_t kh = kv_hash((const uint8_t *)tokens[i].ptr,
                                  (uint32_t)tokens[i].len);
            /* Phase 6.2 — MSETNX pre-check is read-only (only deciding
             * whether to abort); use rdlock for concurrent inspection. */
            SHARD_LOCK(kh, 0);
            kv_entry_t *ke = dict_find(kv_dict_for(kh), tokens[i].ptr, (uint32_t)tokens[i].len, kh);
            if (ke && (!ke->expire || get_time_now() <= ke->expire)) {
                reply_integer(wb, 0);
                return DISPATCH_OK;
            }
        }
        for (int i = 1; i + 1 < argc; i += 2) {
            uint64_t kh = kv_hash((const uint8_t *)tokens[i].ptr,
                                  (uint32_t)tokens[i].len);
            SHARD_WRLOCK(kh);
            string_engine_set(tokens[i].ptr, (uint32_t)tokens[i].len,
                              tokens[i+1].ptr, (uint32_t)tokens[i+1].len,
                              0, 0);
        }
        reply_integer(wb, 1);
        if (!g_in_reload) {
            kvs_write_journal_append(msg_raw, msg_len);
        }
        return DISPATCH_OK;
    }

    /* ── DEL / UNLINK — multi-key, type-agnostic, integer reply (Redis-compat).
     *    Locking is per-key so concurrent DELs across shards do not block. ── */
    if (TOKEN_EQ(cmd, "DEL") || TOKEN_EQ(cmd, "UNLINK")) {
        if (argc < 2) {
            dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
            return DISPATCH_ERROR;
        }
        long long ndel = 0;
        for (int i = 1; i < argc; i++) {
            kvs_token_t kt = tokens[i];
            uint64_t kh = kv_hash((const uint8_t *)kt.ptr, (uint32_t)kt.len);
            SHARD_WRLOCK(kh);
            kv_entry_t *ke = dict_find(kv_dict_for(kh), kt.ptr, (uint32_t)kt.len, kh);
            if (!ke) continue;
            entry_cleanup_by_type(ke);
            if (dict_delete(kv_dict_for(kh), kt.ptr, (uint32_t)kt.len, kh) == 0)
                ndel++;
        }
        reply_integer(wb, ndel);
        if (ndel > 0 && !g_in_reload) {
            kvs_write_journal_append(msg_raw, msg_len);
        }
        return DISPATCH_OK;
    }

    /* ── EXISTS — multi-key, integer reply.  Phase 6.2: rdlock — purely
     * read-only.  We don't even delete expired keys here (they'd need an
     * upgrade and EXISTS is supposed to be cheap); they're just reported
     * as absent, and lazy-deleted on the next single-key access. ── */
    if (TOKEN_EQ(cmd, "EXISTS") || TOKEN_EQ(cmd, "EXIST")) {
        if (argc < 2) {
            dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
            return DISPATCH_ERROR;
        }
        long long nfound = 0;
        for (int i = 1; i < argc; i++) {
            kvs_token_t kt = tokens[i];
            uint64_t kh = kv_hash((const uint8_t *)kt.ptr, (uint32_t)kt.len);
            SHARD_LOCK(kh, 0);
            kv_entry_t *ke = dict_find(kv_dict_for(kh), kt.ptr, (uint32_t)kt.len, kh);
            if (!ke) continue;
            if (ke->expire && get_time_now() > ke->expire) continue;
            nfound++;
        }
        reply_integer(wb, nfound);
        return DISPATCH_OK;
    }

    /* ── Single-key commands (lock the relevant shard for the duration). ── */
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
    } else {
        dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
        return DISPATCH_ERROR;
    }

    /* Acquire the shard lock — rdlock for reads, wrlock for writes.
     * Auto-released on every return path via the cleanup attribute on _sg.
     * Released early before block_wake (which may recursively dispatch). */
    SHARD_LOCK(hash, is_write);

    kv_entry_t *entry = dict_find(kv_dict_for(hash), key_tok.ptr, (uint32_t)key_tok.len, hash);

    /* Lazy-expire: if we found an expired entry under rdlock, we must upgrade
     * to wrlock before deleting (otherwise the dict mutation would race with
     * other readers).  After upgrade, re-find — another writer may have
     * already deleted the entry while we were waiting for the wrlock. */
    if (entry && entry->expire && get_time_now() > entry->expire) {
        if (!is_write) {
            SHARD_UPGRADE_TO_WR();
            entry = dict_find(kv_dict_for(hash), key_tok.ptr, (uint32_t)key_tok.len, hash);
        }
        if (entry && entry->expire && get_time_now() > entry->expire) {
            entry_cleanup_by_type(entry);
            dict_delete(kv_dict_for(hash), key_tok.ptr, (uint32_t)key_tok.len, hash);
            entry = NULL;
        }
    }

    int pre_len = wb->len;

    /* ── TYPE — Redis returns simple string "+string"/"+list"/etc, "+none" if absent ── */
    if (TOKEN_EQ(cmd, "TYPE")) {
        const char *r = "+none\r\n";
        int rlen = 7;
        if (entry) {
            r = "+string\r\n";
            rlen = 9;
        }
        dynbuf_cat(wb, r, rlen);
        return DISPATCH_OK;
    }

    /* ── EXPIRE — integer reply :1 (set) / :0 (no key). ── */
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
        if (!entry) {
            reply_integer(wb, 0);
            return DISPATCH_OK;
        }
        entry->expire = get_time_now() + secs * 1000;
        reply_integer(wb, 1);
        if (!g_in_reload) {
            kvs_write_journal_append(msg_raw, msg_len);
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
            kvs_write_journal_append(msg_raw, msg_len);
        }
        return DISPATCH_OK;
    }

    /* Phase 6.2 — cmd_type / is_write were already resolved before locking.
     * If we reach here, cmd_type is a valid KV_TYPE_* (not -2 generic, since
     * those would have returned early via TYPE/TTL/PTTL/EXPIRE/PERSIST). */

    /* WRONGTYPE if key exists but with a different type */
    if (entry && (int)entry_type(entry) != cmd_type) {
        dynbuf_cat(wb, RESP_WRONGTYPE, RESP_WRONGTYPE_LEN);
        return DISPATCH_ERROR;
    }

    dispatch_result_t r = DISPATCH_OK;
    switch (cmd_type) {
    case KV_TYPE_STRING:
        string_cmd(cmd, entry, hash, tokens + 1, argc - 1, wb, msg_raw, msg_len);
        r = DISPATCH_OK;
        break;
    default:
        dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
        r = DISPATCH_ERROR;
        break;
    }

    if (r == DISPATCH_BLOCKED)
        return DISPATCH_BLOCKED;

    /* ── Write-command post-processing (AOF + WAL repl + XDP cache) ── */
    if (is_write) {
        const char *resp = wb->buf + pre_len;
        int rlen = wb->len - pre_len;
        int ok = (rlen > 0 && resp[0] != '-' &&
                  !(rlen >= 5 && memcmp(resp, "$-1\r\n", 5) == 0));
        if (ok && !g_in_reload) {
            kvs_write_journal_append(msg_raw, msg_len);
        }
    }

    /* ── After LPUSH/RPUSH, wake any client blocked on this key ──
     *
     *  Drop the shard lock first: the wake notify path may dispatch into
     *  block subsystem APIs and (on the same-worker fast path) into the
     *  woken client's saved command, which can target any shard.
     *
     *  Phase 9 multi-key fix: do NOT call block_retry_client here.  The
     *  retry must run on the woken client's owner worker so that
     *  conn->write_buf is touched by exactly one worker.  block_notify_woken
     *  routes via proactor_block_notify(): same-worker → runs locally,
     *  cross-worker → enqueues to owner's wake_evfd and the owner's
     *  proactor_block_notify_local will retry block_cmd_buf. */
    SHARD_RELEASE();
    return DISPATCH_OK;
}

/* ==================== Engine lifecycle ==================== */

int init_kvengine(void) {
    kv_shards_init();
    vector_store_init();
    block_init();
    printf("Using string + vector engine (%u id shards, flat vector search)\n", KV_NR_SHARDS);
    return 0;
}

void dest_kvengine(void) {
    kvs_flush_all();
    vector_store_destroy();
    kv_shards_destroy();
}

void kvs_flush_all(void) {
    kv_shards_iter_t it; kv_shards_iter_init(&it);
    kv_entry_t *e;
    while ((e = kv_shards_iter_next(&it)) != NULL)
        entry_cleanup_by_type(e);
    kv_shards_iter_free(&it);
    kv_shards_clear();
    vector_store_clear();
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
    /* Phase 9 — record server start time for INFO uptime_in_seconds. */
    g_kvs_start_time_ms = get_time_now_real();
    kvs_tsc_calibrate();
    persist_set_basedir(argv[0]);
    const char *conf_path = (argc >= 2) ? argv[1] : "conf/kvstore.conf";
    config_load(conf_path);
    config_print();

    init_kvengine();
    engine_reload();
    kvs_journal_init();
    if (ENABLE_AOF) {
        kvs_wal_persist_start();
        aof_async_init();
    }
    if (kvs_config.xdp_repl_enabled && kvs_config.rdma_role == ROLE_MASTER)
        repl_stream_start_master();
    if (kvs_config.xdp_enabled)
        xdp_manager_init(kvs_config.xdp_iface, kvs_config.port);
    if (kvs_config.sockmap_enabled)
        sockmap_manager_init(kvs_config.port, NULL);
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
    repl_stream_stop();
    kvs_wal_persist_stop();
    kvs_journal_shutdown();
    if (kvs_config.sockmap_enabled) sockmap_manager_stop();
    if (kvs_config.xdp_enabled) xdp_manager_stop();
    dest_kvengine();
    return 0;
}
