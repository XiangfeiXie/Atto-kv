

#include "kvstore_internal.h"

static _Atomic uint64_t g_expired_keys = 0;

/* Export expired_keys counter for INFO stats. */
_Atomic uint64_t *kvs_expired_keys_ptr(void) {
    return &g_expired_keys;
}

                                                

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
                if (get_time_now_real() - start > EXPIRE_TIME_BUDGET_MS) {
                    total_expired += expired;  /* count this round's deletions */
                    goto done;
                }
            }

            kv_entry_t *e = dict_get_random_key(dict);
            if (!e) {
                total_expired += expired;      /* count this round's deletions */
                goto done;                     /* dict is empty */
            }

            if (e->expire && get_time_now() > e->expire) {
                /* Key is inline in the entry — save pointers before deletion. */
                const char *kp = entry_key(e);
                uint32_t    kl = e->key_len;
                uint64_t    h  = e->hash;

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
