/* kvstore_expire.c — lazy + active expiration (Redis-style, single-threaded)
 *
 * Strategy:  two-pronged, same as Redis.
 *   1. Lazy — delete on access (in dispatch_resolve, every read/write path).
 *   2. Active — periodic random sampling in the event loop, so keys that are
 *      never accessed still get cleaned up eventually.                    */

#include "kvstore_internal.h"

static _Atomic uint64_t g_expired_keys = 0;

/* Export expired_keys counter for INFO stats. */
_Atomic uint64_t *kvs_expired_keys_ptr(void) {
    return &g_expired_keys;
}

/* ── Active expiration cycle ─────────────────────────────────────────────
 *
 * Called once per event-loop iteration from update_cached_time().
 * Randomly samples keys and deletes expired ones, with adaptive sampling
 * and a hard time budget so it never blocks the event loop.
 *
 * Algorithm (same as Redis activeExpireCycle):
 *   1. Start with 20 keys per sample.
 *   2. For each round, if >25% of sampled keys are expired, double the
 *      sample size — the dict has a high "expire density" and we should
 *      work harder.
 *   3. Stop when expired rate drops below 25% or sample size exceeds 200.
 *   4. Hard time budget: 1 ms wall-clock per call — bail out immediately
 *      when exceeded.                                                 */

#define EXPIRE_BASE_SAMPLE   20
#define EXPIRE_MAX_SAMPLE   200
#define EXPIRE_TIME_BUDGET_MS 1

void active_expire_cycle(void) {
    kv_dict_t *dict = &g_dict;
    long long   start = get_time_now_real();   /* wall clock, ms */
    int         sample = EXPIRE_BASE_SAMPLE;
    int         total_expired = 0;

    do {
        int expired = 0;

        for (int i = 0; i < sample; i++) {
            /* Check time budget every 16 keys */
            if ((i & 0xF) == 0) {
                if (get_time_now_real() - start > EXPIRE_TIME_BUDGET_MS)
                    goto done;
            }

            kv_entry_t *e = dict_get_random_key(dict);
            if (!e) goto done;                 /* dict is empty */

            if (e->expire && get_time_now() > e->expire) {
                /* Key is inline in entry allocation — save pointers
                 * before deletion (entry_cleanup_by_type only frees
                 * hash-internal structures, not the entry itself). */
                const char *kp = entry_key(e);
                uint32_t    kl = e->key_len;
                uint64_t    h  = e->hash;

                entry_cleanup_by_type(e);
                dict_delete(dict, kp, kl, h);
                expired++;
            }
        }

        total_expired += expired;

        /* Adaptive: if >25% expired, work harder next round */
        if (expired > sample / 4)
            sample *= 2;
        else
            break;

    } while (sample <= EXPIRE_MAX_SAMPLE);

done:
    if (total_expired > 0) {
        atomic_fetch_add_explicit(&g_expired_keys,
                                  (uint64_t)total_expired,
                                  memory_order_relaxed);
    }
}
