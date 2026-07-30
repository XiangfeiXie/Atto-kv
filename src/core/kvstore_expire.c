/* kvstore_expire.c — active expire cycle + lazy deletion queue */

#include "kvstore_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#define ACTIVE_EXPIRE_CYCLE_LOOKUPS 20
#define EXPIRE_QUEUE_SIZE 256

#define ACTIVE_EXPIRE_CYCLE_LOOKUPS_MIN 20
#define ACTIVE_EXPIRE_CYCLE_LOOKUPS_MAX 200

static _Atomic uint64_t g_expired_keys = 0;
static pthread_t g_expire_thread;
static volatile int g_expire_thread_stop = 0;

/* Thread-local expire queue: keys discovered during read ops are batch-deleted
 * during write ops to avoid lock upgrades. */
typedef struct {
    char *keys[EXPIRE_QUEUE_SIZE];
    uint32_t key_lens[EXPIRE_QUEUE_SIZE];
    uint64_t hashes[EXPIRE_QUEUE_SIZE];
    int count;
} expire_queue_t;

static __thread expire_queue_t g_expire_queue = {0};

int enqueue_expired_key(const char *key, uint32_t key_len, uint64_t hash) {
    if (g_expire_queue.count >= EXPIRE_QUEUE_SIZE)
        return 0;

    char *key_copy = (char*)malloc(key_len);
    if (!key_copy) return 0;
    memcpy(key_copy, key, key_len);

    g_expire_queue.keys[g_expire_queue.count] = key_copy;
    g_expire_queue.key_lens[g_expire_queue.count] = key_len;
    g_expire_queue.hashes[g_expire_queue.count] = hash;
    g_expire_queue.count++;
    return 1;
}

/* Batch-delete all expired keys in the thread-local queue.
 * Must be called while holding write lock. */
void flush_expire_queue(void) {
    if (g_expire_queue.count == 0) return;

    long long now = get_time_now();

    for (int i = 0; i < g_expire_queue.count; i++) {
        kv_dict_t *dict = kv_dict_for(g_expire_queue.hashes[i]);
        kv_entry_t *entry = dict_find(dict,
                                       g_expire_queue.keys[i],
                                       g_expire_queue.key_lens[i],
                                       g_expire_queue.hashes[i]);
        if (entry && entry->expire && now > entry->expire) {
            entry_cleanup_by_type(entry);
            dict_delete(dict,
                       g_expire_queue.keys[i],
                       g_expire_queue.key_lens[i],
                       g_expire_queue.hashes[i]);
            atomic_fetch_add_explicit(&g_expired_keys, 1, memory_order_relaxed);
        }
        free(g_expire_queue.keys[i]);
    }
    g_expire_queue.count = 0;
}

/* Adaptive sampling: increase sample size when expire rate is high. */
static void active_expire_cycle(void) {
    kv_dict_t *dict = kv_dict_for(0);
    int sample_size = ACTIVE_EXPIRE_CYCLE_LOOKUPS_MIN;
    int consecutive_high_expire = 0;

    do {
        int expired_total = 0;
        int sampled_total = 0;

        if (__sync_val_compare_and_swap(&g_store_lock, 0, 2) != 0)
            return;

        for (int i = 0; i < sample_size; i++) {
            kv_entry_t *entry = dict_get_random_key(dict);
            if (!entry) break;
            sampled_total++;

            if (entry->expire && get_time_now() > entry->expire) {
                const char *key = entry_key(entry);
                uint32_t key_len = entry->key_len;
                uint64_t hash = entry->hash;

                entry_cleanup_by_type(entry);
                dict_delete(dict, key, key_len, hash);
                expired_total++;
                atomic_fetch_add_explicit(&g_expired_keys, 1, memory_order_relaxed);
            }
        }

        __atomic_store_n(&g_store_lock, 0, __ATOMIC_RELEASE);

        double expire_rate = sampled_total > 0 ?
            (double)expired_total / sampled_total : 0;

        if (expire_rate > 0.5) {
            sample_size = sample_size * 2;
            if (sample_size > ACTIVE_EXPIRE_CYCLE_LOOKUPS_MAX)
                sample_size = ACTIVE_EXPIRE_CYCLE_LOOKUPS_MAX;
            consecutive_high_expire++;
        } else if (expire_rate < 0.1) {
            sample_size = sample_size / 2;
            if (sample_size < ACTIVE_EXPIRE_CYCLE_LOOKUPS_MIN)
                sample_size = ACTIVE_EXPIRE_CYCLE_LOOKUPS_MIN;
            consecutive_high_expire = 0;
        }

        if (consecutive_high_expire > 10) break;

        if (sampled_total == 0 || expired_total * 4 <= sampled_total) break;
    } while (1);
}

/* Export expired_keys counter for INFO stats. */
_Atomic uint64_t *kvs_expired_keys_ptr(void) {
    return &g_expired_keys;
}

static void *expire_thread_main(void *arg) {
    (void)arg;

    while (!g_expire_thread_stop) {
        struct timespec ts = { .tv_sec = 1, .tv_nsec = 0 };
        nanosleep(&ts, NULL);
        active_expire_cycle();
    }
    return NULL;
}

void kvs_start_expire_thread(void) {
    g_expire_thread_stop = 0;
    if (pthread_create(&g_expire_thread, NULL, expire_thread_main, NULL) != 0) {
        fprintf(stderr, "[expire] failed to create background thread\n");
    }
}

void kvs_stop_expire_thread(void) {
    g_expire_thread_stop = 1;
    pthread_join(g_expire_thread, NULL);
}

/* ── KVItem ──────────────────────────────────────────────────────────── */

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
