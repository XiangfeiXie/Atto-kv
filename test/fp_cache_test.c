/*
 * fp_cache_test.c — Phase 10.1 unit + stress test for the lock-free
 * seqlock fast-path cache.
 *
 * Three test groups:
 *   1. Sanity:        single-thread put/get/del/miss
 *   2. Collision:     hash collisions on the same bucket
 *   3. Stress:        1 writer + 3 readers, 30s, verify no torn read
 *
 * Build: see CMakeLists.txt target `fp_cache_test`.
 * Run:   ./build/fp_cache_test [seconds]   (default: 5s stress)
 *
 * Pass criteria:
 *   - Group 1: every assertion holds
 *   - Group 2: collisions resolve correctly via key compare
 *   - Group 3: zero torn-read events across all readers
 *              (every observed value matches one of the two patterns
 *              the writer publishes — never a mix)
 */

#define _GNU_SOURCE
#include "fastpath_cache.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <inttypes.h>

/* tiny xorshift64 for repeatable randomness without libc rand contention */
static inline uint64_t xs64(uint64_t *s) {
    uint64_t x = *s;
    x ^= x << 13; x ^= x >> 7; x ^= x << 17;
    *s = x;
    return x;
}

/* fnv1a-ish — we don't link against engine/kv_dict, so use a tiny
 * inline hash that's good enough for bucket distribution in tests. */
static uint64_t test_hash(const uint8_t *p, uint32_t n) {
    uint64_t h = 0xcbf29ce484222325ULL;
    for (uint32_t i = 0; i < n; i++) {
        h ^= p[i];
        h *= 0x100000001b3ULL;
    }
    return h;
}

#define ASSERT(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "ASSERT FAIL: %s:%d  (%s)\n", \
                __FILE__, __LINE__, #cond); \
        exit(1); \
    } \
} while (0)

/* ------------------------------------------------------------ */
/* Group 1 — single-thread sanity                                */
/* ------------------------------------------------------------ */

static void test_sanity(void)
{
    printf("  [sanity] basic put/get/del/miss ... ");
    fflush(stdout);

    struct fp_cache *c = fp_cache_new();
    ASSERT(c != NULL);

    /* miss on empty */
    uint8_t buf[FP_REPLY_MAX];
    uint32_t outlen = 0;
    int r = fp_get(c, "k1", 2, test_hash((const uint8_t*)"k1", 2),
                   buf, &outlen, sizeof(buf));
    ASSERT(r == -1);

    /* put + get */
    fp_put(c, "k1", 2, test_hash((const uint8_t*)"k1", 2),
           "hello", 5);
    r = fp_get(c, "k1", 2, test_hash((const uint8_t*)"k1", 2),
               buf, &outlen, sizeof(buf));
    ASSERT(r == 0);
    ASSERT(outlen == 11);
    ASSERT(memcmp(buf, "$5\r\nhello\r\n", 11) == 0);

    /* overwrite */
    fp_put(c, "k1", 2, test_hash((const uint8_t*)"k1", 2),
           "world!!!", 8);
    r = fp_get(c, "k1", 2, test_hash((const uint8_t*)"k1", 2),
               buf, &outlen, sizeof(buf));
    ASSERT(r == 0);
    ASSERT(outlen == 14);
    ASSERT(memcmp(buf, "$8\r\nworld!!!\r\n", 14) == 0);

    /* delete -> miss */
    fp_del(c, "k1", 2, test_hash((const uint8_t*)"k1", 2));
    r = fp_get(c, "k1", 2, test_hash((const uint8_t*)"k1", 2),
               buf, &outlen, sizeof(buf));
    ASSERT(r == -1);

    /* boundary: max-size key + max-size value */
    uint8_t big_k[FP_KEY_MAX];
    uint8_t big_v[FP_VAL_MAX];
    for (uint32_t i = 0; i < FP_KEY_MAX; i++) big_k[i] = (uint8_t)('a' + (i % 26));
    for (uint32_t i = 0; i < FP_VAL_MAX; i++) big_v[i] = (uint8_t)(i & 0xff);
    uint64_t bh = test_hash(big_k, FP_KEY_MAX);
    fp_put(c, big_k, FP_KEY_MAX, bh, big_v, FP_VAL_MAX);

    r = fp_get(c, big_k, FP_KEY_MAX, bh, buf, &outlen, sizeof(buf));
    ASSERT(r == 0);
    ASSERT(outlen == 6 + FP_VAL_MAX + 2);  /* "$256\r\n" + value + "\r\n" */
    ASSERT(memcmp(buf, "$256\r\n", 6) == 0);
    ASSERT(memcmp(buf + 6, big_v, FP_VAL_MAX) == 0);
    ASSERT(memcmp(buf + 6 + FP_VAL_MAX, "\r\n", 2) == 0);

    /* oversized key/value → silent no-op */
    uint8_t oversized[FP_VAL_MAX + 16];
    memset(oversized, 'x', sizeof(oversized));
    fp_put(c, "z", 1, test_hash((const uint8_t*)"z", 1),
           oversized, sizeof(oversized));   /* should be dropped */
    r = fp_get(c, "z", 1, test_hash((const uint8_t*)"z", 1),
               buf, &outlen, sizeof(buf));
    ASSERT(r == -1);

    /* stats sanity */
    uint64_t h = 0, m = 0, inv = 0, col = 0;
    fp_stats(c, &h, &m, &inv, &col);
    ASSERT(h >= 3);
    ASSERT(m >= 2);
    ASSERT(inv >= 1);

    fp_cache_free(c);
    printf("OK\n");
}

/* ------------------------------------------------------------ */
/* Group 2 — hash collision (force two keys to same bucket)      */
/* ------------------------------------------------------------ */

static void test_collision(void)
{
    printf("  [collision] hash collision distinguishes by full key ... ");
    fflush(stdout);

    struct fp_cache *c = fp_cache_new();
    ASSERT(c != NULL);

    /* Use a contrived "hash" of 0 for both keys to force same bucket */
    fp_put(c, "alpha", 5, 0xDEADBEEFULL, "AAA", 3);
    fp_put(c, "bravo", 5, 0xDEADBEEFULL, "BBB", 3);

    uint8_t buf[FP_REPLY_MAX]; uint32_t ol = 0;
    /* Second put overwrote first.  Looking up "alpha" — same hash but
     * different key bytes — must miss (full key compare rejects it). */
    int r = fp_get(c, "alpha", 5, 0xDEADBEEFULL, buf, &ol, sizeof(buf));
    ASSERT(r == -1);

    /* "bravo" is the current occupant — must hit */
    r = fp_get(c, "bravo", 5, 0xDEADBEEFULL, buf, &ol, sizeof(buf));
    ASSERT(r == 0 && ol == 9 && memcmp(buf, "$3\r\nBBB\r\n", 9) == 0);

    /* Delete with non-matching key on same bucket → no-op (not us) */
    fp_del(c, "alpha", 5, 0xDEADBEEFULL);
    r = fp_get(c, "bravo", 5, 0xDEADBEEFULL, buf, &ol, sizeof(buf));
    ASSERT(r == 0);   /* still there */

    /* Delete with matching key → invalidate */
    fp_del(c, "bravo", 5, 0xDEADBEEFULL);
    r = fp_get(c, "bravo", 5, 0xDEADBEEFULL, buf, &ol, sizeof(buf));
    ASSERT(r == -1);

    fp_cache_free(c);
    printf("OK\n");
}

/* ------------------------------------------------------------ */
/* Group 3 — 1 writer + 3 readers stress, no torn-read           */
/* ------------------------------------------------------------ */

/* Writer cycles a single key between TWO distinct payloads.  Readers
 * snapshot the value and verify it is BIT-EXACTLY one of the two
 * patterns.  Any mixed-pattern observation indicates a torn read. */

#define STRESS_KEY    "stress-key-001"
#define STRESS_KLEN   14
#define STRESS_VLEN   200

struct stress_ctx {
    struct fp_cache *c;
    uint64_t         khash;
    uint8_t          pat_a[STRESS_VLEN];
    uint8_t          pat_b[STRESS_VLEN];
    _Atomic int      stop;
    _Atomic uint64_t total_reads;
    _Atomic uint64_t total_writes;
    _Atomic uint64_t torn_reads;
    _Atomic uint64_t hit_count;
};

static void *stress_writer(void *arg)
{
    struct stress_ctx *s = (struct stress_ctx *)arg;
    uint64_t cnt = 0;
    while (!atomic_load_explicit(&s->stop, memory_order_relaxed)) {
        const uint8_t *p = (cnt & 1) ? s->pat_b : s->pat_a;
        fp_put(s->c, STRESS_KEY, STRESS_KLEN, s->khash, p, STRESS_VLEN);
        cnt++;
        if ((cnt & 0xff) == 0) {
            /* occasional invalidate to exercise that path */
            fp_del(s->c, STRESS_KEY, STRESS_KLEN, s->khash);
        }
    }
    atomic_fetch_add(&s->total_writes, cnt);
    return NULL;
}

static void *stress_reader(void *arg)
{
    struct stress_ctx *s = (struct stress_ctx *)arg;
    uint8_t buf[FP_REPLY_MAX];
    uint32_t ol = 0;
    uint64_t reads = 0, hits = 0, torn = 0;

    while (!atomic_load_explicit(&s->stop, memory_order_relaxed)) {
        int r = fp_get(s->c, STRESS_KEY, STRESS_KLEN, s->khash,
                       buf, &ol, sizeof(buf));
        reads++;
        if (r == 0) {
            hits++;
            /* Must be entirely pat_a OR entirely pat_b — never a mix. */
            int is_a = (ol == 6 + STRESS_VLEN + 2
                        && memcmp(buf, "$200\r\n", 6) == 0
                        && memcmp(buf + 6, s->pat_a, STRESS_VLEN) == 0
                        && memcmp(buf + 6 + STRESS_VLEN, "\r\n", 2) == 0);
            int is_b = (ol == 6 + STRESS_VLEN + 2
                        && memcmp(buf, "$200\r\n", 6) == 0
                        && memcmp(buf + 6, s->pat_b, STRESS_VLEN) == 0
                        && memcmp(buf + 6 + STRESS_VLEN, "\r\n", 2) == 0);
            if (!is_a && !is_b) torn++;
        }
    }
    atomic_fetch_add(&s->total_reads, reads);
    atomic_fetch_add(&s->hit_count, hits);
    atomic_fetch_add(&s->torn_reads, torn);
    return NULL;
}

static int test_stress(int seconds)
{
    printf("  [stress] 1 writer + 3 readers × %ds, no torn-read ... ",
           seconds);
    fflush(stdout);

    struct stress_ctx s;
    memset(&s, 0, sizeof(s));
    s.c = fp_cache_new();
    ASSERT(s.c != NULL);

    /* Two distinct patterns: 0xAA-fill vs 0x55-fill; if any byte from
     * one pattern leaks into the other, memcmp catches it. */
    memset(s.pat_a, 0xAA, STRESS_VLEN);
    memset(s.pat_b, 0x55, STRESS_VLEN);
    s.khash = test_hash((const uint8_t *)STRESS_KEY, STRESS_KLEN);

    pthread_t writer, readers[3];
    pthread_create(&writer, NULL, stress_writer, &s);
    for (int i = 0; i < 3; i++) {
        pthread_create(&readers[i], NULL, stress_reader, &s);
    }

    sleep(seconds);
    atomic_store(&s.stop, 1);

    pthread_join(writer, NULL);
    for (int i = 0; i < 3; i++) pthread_join(readers[i], NULL);

    uint64_t hits = atomic_load(&s.hit_count);
    uint64_t reads = atomic_load(&s.total_reads);
    uint64_t writes = atomic_load(&s.total_writes);
    uint64_t torn = atomic_load(&s.torn_reads);

    uint64_t fp_hits, fp_misses, fp_inv, fp_col;
    fp_stats(s.c, &fp_hits, &fp_misses, &fp_inv, &fp_col);

    fp_cache_free(s.c);

    if (torn != 0) {
        printf("FAIL\n");
        printf("    reads=%" PRIu64 " hits=%" PRIu64 " writes=%" PRIu64
               " torn=%" PRIu64 "\n", reads, hits, writes, torn);
        return 1;
    }
    printf("OK\n");
    printf("    reads=%" PRIu64 "  hits=%" PRIu64
           " (%.1f%%)  writes=%" PRIu64 "  torn=0\n",
           reads, hits,
           reads ? (100.0 * (double)hits / (double)reads) : 0.0,
           writes);
    printf("    fp_stats: hits=%" PRIu64 " misses=%" PRIu64
           " invalidations=%" PRIu64 " writer_collisions=%" PRIu64 "\n",
           fp_hits, fp_misses, fp_inv, fp_col);
    return 0;
}

/* ------------------------------------------------------------ */

int main(int argc, char **argv)
{
    int stress_secs = 5;
    if (argc > 1) {
        stress_secs = atoi(argv[1]);
        if (stress_secs < 1) stress_secs = 1;
    }

    printf("== fp_cache_test ==\n");
    printf("  sizeof(struct fp_entry) = %zu (expected 512)\n",
           sizeof(struct fp_entry));
    printf("  sizeof(struct fp_cache) = %zu (expected ~512 KB, L2-resident)\n",
           sizeof(struct fp_cache));

    test_sanity();
    test_collision();
    int rc = test_stress(stress_secs);

    if (rc == 0) printf("== ALL PASS ==\n");
    else         printf("== FAIL ==\n");
    return rc;
}
