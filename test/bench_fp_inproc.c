/*
 * bench_fp_inproc.c — Phase 10 in-process CPU microbenchmark for the
 *                     lock-free seqlock fast-path cache.
 *
 * What this measures (and why it matters):
 *   The end-to-end redis-benchmark numbers are dominated by TCP / io_uring
 *   per-op overhead (≈ 50–70 μs/op), which completely masks the lookup-path
 *   savings the fast-path cache delivers.  This bench eliminates the
 *   network entirely and times only the two GET implementations:
 *
 *     A.  baseline:   pthread_rwlock_rdlock(shard)
 *                   + dict_find(shard, key, len, hash)
 *                   + entry_payload extraction
 *                   + pthread_rwlock_unlock(shard)
 *
 *     B.  fastpath:   fp_get(cache, key, len, hash, buf, &outlen, cap)
 *                     (preformatted RESP reply already memcpy'd out)
 *
 *   Both run against the same hot key set (HOT_KEYS keys, VALUE_BYTES bytes
 *   each) populated through the public string_engine_set/fp_put APIs, so
 *   we exercise the exact same code paths the dispatcher does.
 *
 *   Reported numbers are the median of REPEATS runs of OPS_PER_RUN ops
 *   each, in nanoseconds-per-op.  Speedup is baseline_ns / fastpath_ns.
 *
 * Building:
 *   This is wired into CMakeLists.txt as the `bench_fp_inproc` target.
 *
 * Running:
 *   ./build/bench_fp_inproc                # default 64 keys × 64B
 *   HOT_KEYS=256 VALUE_BYTES=128 ./build/bench_fp_inproc
 */

#include "fastpath_cache.h"
#include "kv_dict.h"
#include "kv_type_string.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

/* kv_type_string.c references these; provide stubs since we don't run a
 * real event loop here.  Returning 0 is fine — no key has an expiration,
 * and we never actually trigger a type-specific cleanup in this bench. */
long long get_time_now(void) { return 0; }
void kvs_cleanup_entry(kv_entry_t *e) { (void)e; }

/* Default knobs (overridable via env). */
static int  g_hot_keys    = 64;
static int  g_value_bytes = 64;
static long g_ops_per_run = 50 * 1000 * 1000;   /* 50M ops per run */
static int  g_repeats     = 5;

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* qsort helper for double medians. */
static int cmp_dbl(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (x < y) ? -1 : (x > y) ? 1 : 0;
}

static int env_int(const char *name, int dflt) {
    const char *v = getenv(name);
    if (!v || !*v) return dflt;
    return atoi(v);
}
static long env_long(const char *name, long dflt) {
    const char *v = getenv(name);
    if (!v || !*v) return dflt;
    return atol(v);
}

/* --- Key set ------------------------------------------------------------ */

struct hk {
    char     key[24];
    int      klen;
    uint64_t hash;
};

static struct hk *g_keys;

/* When SAME_SHARD=1, generate keys until all of them land in the same
 * shard.  This deliberately maximises pthread_rwlock_t cacheline
 * contention so the lock-free advantage of fp_get is observable —
 * mirroring a real-world hot-key skew where one shard becomes the
 * bottleneck under many readers. */
static void build_keys(void)
{
    int same_shard = env_int("SAME_SHARD", 0);
    g_keys = calloc((size_t)g_hot_keys, sizeof(*g_keys));
    if (!g_keys) { perror("calloc"); exit(1); }

    if (!same_shard) {
        for (int i = 0; i < g_hot_keys; i++) {
            g_keys[i].klen =
                snprintf(g_keys[i].key, sizeof(g_keys[i].key), "hk:%08d", i);
            g_keys[i].hash = kv_hash((const uint8_t *)g_keys[i].key,
                                      (uint32_t)g_keys[i].klen);
        }
        return;
    }

    /* Pick the shard of the first synthesised key, then accept only keys
     * that hash into the same shard.  Linear probe by index suffix. */
    uint32_t target_shard = UINT32_MAX;
    int collected = 0;
    long probe = 0;
    while (collected < g_hot_keys) {
        char buf[24];
        int  blen = snprintf(buf, sizeof(buf), "ss:%010ld", probe++);
        uint64_t h = kv_hash((const uint8_t *)buf, (uint32_t)blen);
        uint32_t shard = (uint32_t)((h >> 32) & KV_SHARD_MASK);
        if (target_shard == UINT32_MAX) target_shard = shard;
        if (shard != target_shard) continue;
        memcpy(g_keys[collected].key, buf, (size_t)blen);
        g_keys[collected].klen = blen;
        g_keys[collected].hash = h;
        collected++;
    }
    fprintf(stderr, "  [SAME_SHARD=1] forced all %d keys onto shard %u "
            "(scanned %ld candidates)\n",
            g_hot_keys, target_shard, probe);
}

/* --- Population --------------------------------------------------------- */

static void populate(struct fp_cache *fp, char *value)
{
    for (int i = 0; i < g_value_bytes; i++) value[i] = (char)('a' + (i % 26));

    for (int i = 0; i < g_hot_keys; i++) {
        struct hk *h = &g_keys[i];
        /* Engine SET takes the shard wrlock internally? No — the public
         * dispatcher does the locking.  Match that pattern here. */
        pthread_rwlock_t *lock = &kv_shard_for(h->hash)->lock;
        pthread_rwlock_wrlock(lock);
        string_engine_set(h->key, (uint32_t)h->klen,
                          value, (uint32_t)g_value_bytes, 0, 0);
        pthread_rwlock_unlock(lock);

        /* Mirror into the fast-path cache. */
        fp_put(fp, h->key, (uint32_t)h->klen, h->hash,
               value, (uint32_t)g_value_bytes);
    }
}

/* --- Workloads ---------------------------------------------------------- */

/* Both paths fully simulate dispatcher work: locate value, copy
 * preformatted RESP bytes into a per-call output buffer (mirrors what
 * the dispatcher's dynbuf_cat would do).  This keeps the comparison
 * apples-to-apples — fp_get already returns a preformatted reply, so
 * the dict path must format one too. */

static volatile uint64_t g_sink;

static inline uint32_t format_resp(uint8_t *out, const char *val, uint32_t vlen)
{
    /* "$<len>\r\n" + val + "\r\n" — same layout fp_cache stores. */
    int hl = snprintf((char *)out, 16, "$%u\r\n", vlen);
    memcpy(out + hl, val, vlen);
    out[hl + vlen]     = '\r';
    out[hl + vlen + 1] = '\n';
    return (uint32_t)hl + vlen + 2u;
}

static double run_dict_path(long ops)
{
    uint8_t buf[FP_REPLY_MAX];
    uint64_t t0 = now_ns();
    uint64_t sink = 0;
    for (long i = 0; i < ops; i++) {
        struct hk *h = &g_keys[i % g_hot_keys];
        pthread_rwlock_t *lock = &kv_shard_for(h->hash)->lock;
        pthread_rwlock_rdlock(lock);
        kv_entry_t *e = dict_find(kv_dict_for(h->hash),
                                   h->key, (uint32_t)h->klen, h->hash);
        uint32_t rlen = 0;
        if (e) {
            const char *p   = entry_payload(e);
            uint32_t    plen = entry_payload_len(e);
            rlen = format_resp(buf, p, plen);
        }
        pthread_rwlock_unlock(lock);
        sink += (uint64_t)buf[0] + rlen;
    }
    g_sink = sink;
    uint64_t t1 = now_ns();
    return (double)(t1 - t0) / (double)ops;
}

static double run_fp_path(struct fp_cache *fp, long ops)
{
    uint8_t buf[FP_REPLY_MAX];
    uint32_t outlen = 0;
    uint64_t t0 = now_ns();
    uint64_t sink = 0;
    for (long i = 0; i < ops; i++) {
        struct hk *h = &g_keys[i % g_hot_keys];
        if (fp_get(fp, h->key, (uint32_t)h->klen, h->hash,
                   buf, &outlen, sizeof(buf)) == 0) {
            sink += (uint64_t)buf[0] + outlen;
        }
    }
    g_sink = sink;
    uint64_t t1 = now_ns();
    return (double)(t1 - t0) / (double)ops;
}

/* --- Multi-threaded scaling test ----------------------------------------
 *
 * The single-thread numbers above measure raw lookup cost.  The real
 * payoff for a lock-free cache is *scalability*: N reader threads on the
 * same shard should scale linearly with fp_get, while pthread_rwlock_t
 * cacheline bouncing kills the dict path well before N saturates the
 * cores.  We pin all readers on one hot key set living in a single shard.
 */

struct mt_ctx {
    struct fp_cache *fp;
    long             ops;
    int              use_fp;
    _Atomic int     *go;
    double           ns_per_op;
};

static void *mt_worker(void *arg)
{
    struct mt_ctx *c = (struct mt_ctx *)arg;
    while (atomic_load_explicit(c->go, memory_order_acquire) == 0) { /* spin */ }
    if (c->use_fp) c->ns_per_op = run_fp_path(c->fp, c->ops);
    else           c->ns_per_op = run_dict_path(c->ops);
    return NULL;
}

static double run_mt(struct fp_cache *fp, int nthreads, long ops_per_t, int use_fp)
{
    pthread_t *th  = calloc((size_t)nthreads, sizeof(*th));
    struct mt_ctx *ctx = calloc((size_t)nthreads, sizeof(*ctx));
    _Atomic int go = 0;
    for (int i = 0; i < nthreads; i++) {
        ctx[i].fp = fp; ctx[i].ops = ops_per_t;
        ctx[i].use_fp = use_fp; ctx[i].go = &go;
        pthread_create(&th[i], NULL, mt_worker, &ctx[i]);
    }
    /* let workers reach the barrier */
    struct timespec wait = { .tv_sec = 0, .tv_nsec = 50 * 1000 * 1000 };
    nanosleep(&wait, NULL);
    uint64_t t0 = now_ns();
    atomic_store_explicit(&go, 1, memory_order_release);
    for (int i = 0; i < nthreads; i++) pthread_join(th[i], NULL);
    uint64_t t1 = now_ns();
    /* aggregate Mops/s = total_ops / wall_seconds */
    double sec = (double)(t1 - t0) / 1e9;
    double mops = (double)(ops_per_t * nthreads) / 1e6 / sec;
    free(th); free(ctx);
    return mops;
}

/* --- Driver ------------------------------------------------------------- */

int main(void)
{
    g_hot_keys    = env_int ("HOT_KEYS",    g_hot_keys);
    g_value_bytes = env_int ("VALUE_BYTES", g_value_bytes);
    g_ops_per_run = env_long("OPS_PER_RUN", g_ops_per_run);
    g_repeats     = env_int ("REPEATS",     g_repeats);

    if (g_value_bytes < (int)FP_VAL_MIN || g_value_bytes > (int)FP_VAL_MAX) {
        fprintf(stderr, "VALUE_BYTES must be in [%u, %u]\n",
                FP_VAL_MIN, FP_VAL_MAX);
        return 2;
    }

    printf("== bench_fp_inproc ==\n");
    printf("  hot_keys     = %d\n", g_hot_keys);
    printf("  value_bytes  = %d\n", g_value_bytes);
    printf("  ops_per_run  = %ld\n", g_ops_per_run);
    printf("  repeats      = %d\n", g_repeats);

    kv_shards_init();
    struct fp_cache *fp = fp_cache_new();
    if (!fp) { fprintf(stderr, "fp_cache_new failed\n"); return 1; }

    build_keys();
    char *valbuf = malloc((size_t)g_value_bytes);
    populate(fp, valbuf);

    /* Warmup — pull both code paths and the cache hot. */
    (void)run_dict_path(g_hot_keys * 1000);
    (void)run_fp_path(fp, g_hot_keys * 1000);

    double *dict_ns = calloc((size_t)g_repeats, sizeof(double));
    double *fp_ns   = calloc((size_t)g_repeats, sizeof(double));

    for (int r = 0; r < g_repeats; r++) {
        dict_ns[r] = run_dict_path(g_ops_per_run);
        fp_ns[r]   = run_fp_path(fp, g_ops_per_run);
    }

    qsort(dict_ns, (size_t)g_repeats, sizeof(double), cmp_dbl);
    qsort(fp_ns,   (size_t)g_repeats, sizeof(double), cmp_dbl);
    double dict_med = dict_ns[g_repeats / 2];
    double fp_med   = fp_ns[g_repeats / 2];

    printf("\n  Single-threaded (fair: both paths produce a full RESP reply)\n");
    printf("    dict_find + rdlock : %8.2f ns/op  (%.2f Mops/s)\n",
           dict_med, 1000.0 / dict_med);
    printf("    fp_get             : %8.2f ns/op  (%.2f Mops/s)\n",
           fp_med, 1000.0 / fp_med);
    printf("    ratio              : %.2fx (>1 = fp_get faster)\n",
           dict_med / fp_med);

    /* --- Multi-threaded scaling -------------------------------------- */
    printf("\n  Concurrent readers (same hot keys → contended rdlock for"
           " dict, lock-free for fp):\n");
    printf("    %-8s %14s %14s %12s\n",
           "threads", "dict Mops/s", "fp Mops/s", "fp/dict");
    int max_t = env_int("MAX_THREADS", 8);
    long mt_ops = env_long("MT_OPS", 5 * 1000 * 1000);
    for (int n = 1; n <= max_t; n *= 2) {
        double dm = run_mt(fp, n, mt_ops, 0);
        double fm = run_mt(fp, n, mt_ops, 1);
        printf("    %-8d %14.2f %14.2f %11.2fx\n",
               n, dm, fm, fm / dm);
    }

    /* sanity: fp_cache fully populated */
    uint64_t hits = 0, misses = 0, inv = 0, col = 0;
    fp_stats(fp, &hits, &misses, &inv, &col);
    printf("\n  fp_stats: hits=%lu misses=%lu inv=%lu collisions=%lu  (sink=%lu)\n",
           (unsigned long)hits, (unsigned long)misses,
           (unsigned long)inv, (unsigned long)col,
           (unsigned long)g_sink);

    free(dict_ns); free(fp_ns); free(valbuf); free(g_keys);
    fp_cache_free(fp);
    return 0;
}
