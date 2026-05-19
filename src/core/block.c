#include "block.h"
#include "dynbuf.h"
#include "server.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#define BLOCK_TABLE_SIZE 256

typedef struct block_waiter_s {
    client_t *client;
    struct block_waiter_s *next;
} block_waiter_t;

typedef struct block_entry_s {
    char *key;
    uint32_t key_len;
    block_waiter_t *waiters;
    struct block_entry_s *next;
} block_entry_t;

static block_entry_t *block_table[BLOCK_TABLE_SIZE];
static block_notify_cb g_notify_cb = NULL;

/* Phase 4B.2 — single coarse mutex protecting the entire block_table.
 * BLPOP traffic is rare; a fine-grained scheme would not pay back its
 * complexity.  All public block_* functions take this lock for the
 * duration of their work; notify dispatch is hoisted *out* of the lock
 * before being delivered to avoid recursive acquisition. */
static pthread_mutex_t g_block_mu = PTHREAD_MUTEX_INITIALIZER;

void block_set_notify_cb(block_notify_cb cb) { g_notify_cb = cb; }

/* djb2 hash */
static unsigned int block_hash(const char *key, uint32_t len) {
    unsigned int h = 5381;
    for (uint32_t i = 0; i < len; i++)
        h = ((h << 5) + h) ^ (unsigned char)key[i];
    return h % BLOCK_TABLE_SIZE;
}

void block_init(void) {
    pthread_mutex_lock(&g_block_mu);
    memset(block_table, 0, sizeof(block_table));
    pthread_mutex_unlock(&g_block_mu);
}

void block_add(const char *key, uint32_t key_len, client_t *c) {
    if (!key || !c) return;

    unsigned int idx = block_hash(key, key_len);
    pthread_mutex_lock(&g_block_mu);
    block_entry_t *e = block_table[idx];

    while (e) {
        if (e->key_len == key_len && memcmp(e->key, key, key_len) == 0)
            break;
        e = e->next;
    }
    if (!e) {
        e = (block_entry_t *)calloc(1, sizeof(*e));
        if (!e) goto out;
        e->key = (char *)malloc(key_len + 1);
        if (!e->key) { free(e); goto out; }
        memcpy(e->key, key, key_len);
        e->key[key_len] = '\0';
        e->key_len = key_len;
        e->next = block_table[idx];
        block_table[idx] = e;
    }

    block_waiter_t *w = (block_waiter_t *)malloc(sizeof(*w));
    if (!w) goto out;
    w->client = c;
    w->next = e->waiters;
    e->waiters = w;
out:
    pthread_mutex_unlock(&g_block_mu);
}

client_t *block_wake_one(const char *key, uint32_t key_len) {
    if (!key) return NULL;

    unsigned int idx = block_hash(key, key_len);
    pthread_mutex_lock(&g_block_mu);
    block_entry_t *e = block_table[idx];
    while (e) {
        if (e->key_len == key_len && memcmp(e->key, key, key_len) == 0)
            break;
        e = e->next;
    }
    if (!e || !e->waiters) {
        pthread_mutex_unlock(&g_block_mu);
        return NULL;
    }

    block_waiter_t *w = e->waiters;
    client_t *c = w->client;
    e->waiters = w->next;
    free(w);

    /* Remove from other keys this client is waiting on */
    if (c) {
        for (int i = 0; i < BLOCK_TABLE_SIZE; i++) {
            block_entry_t *be = block_table[i];
            while (be) {
                block_waiter_t **prev = &be->waiters;
                block_waiter_t *bw = be->waiters;
                while (bw) {
                    if (bw->client == c) {
                        *prev = bw->next;
                        free(bw);
                        bw = *prev;
                    } else {
                        prev = &bw->next;
                        bw = bw->next;
                    }
                }
                be = be->next;
            }
        }
        c->block_key_count = 0;
        c->state = CLIENT_IDLE;
    }
    pthread_mutex_unlock(&g_block_mu);
    return c;
}

void block_remove_client(client_t *c) {
    if (!c) return;

    pthread_mutex_lock(&g_block_mu);
    for (int i = 0; i < BLOCK_TABLE_SIZE; i++) {
        block_entry_t *e = block_table[i];
        while (e) {
            block_waiter_t **prev = &e->waiters;
            block_waiter_t *w = e->waiters;
            while (w) {
                if (w->client == c) {
                    *prev = w->next;
                    free(w);
                    w = *prev;
                } else {
                    prev = &w->next;
                    w = w->next;
                }
            }
            e = e->next;
        }
    }
    c->state = CLIENT_IDLE;
    c->block_key_count = 0;
    pthread_mutex_unlock(&g_block_mu);
}

void block_notify_woken(client_t *c) {
    if (c && g_notify_cb) g_notify_cb(c);
}

/* Extern from kvstore.c — use real wall clock here so the deadline
 * arithmetic agrees with whichever worker registered the BLPOP. */
extern long long get_time_now_real(void);
#define get_time_now get_time_now_real

int block_nearest_timeout(void) {
    long long now = get_time_now();
    long long nearest = -1;

    pthread_mutex_lock(&g_block_mu);
    for (int i = 0; i < BLOCK_TABLE_SIZE; i++) {
        for (block_entry_t *e = block_table[i]; e; e = e->next) {
            for (block_waiter_t *w = e->waiters; w; w = w->next) {
                client_t *c = w->client;
                if (c && c->state == CLIENT_BLOCKED) {
                    long long remain = c->block_until_ms - now;
                    if (remain < 0) remain = 0;
                    if (nearest < 0 || remain < nearest)
                        nearest = remain;
                }
            }
        }
    }
    pthread_mutex_unlock(&g_block_mu);
    return (int)nearest;
}

void block_check_timeouts(void) {
    long long now = get_time_now();

    /* Two-phase to avoid notify-under-lock and avoid re-entering
     * block_remove_client (which would deadlock on the same mutex):
     *   1. Under the lock: collect timed-out clients into a local list
     *      and unlink them from the table.
     *   2. Outside the lock: write reply + dispatch notify per client. */
    enum { MAX_BATCH = 64 };
    client_t *expired[MAX_BATCH];
    int n_expired = 0;

    pthread_mutex_lock(&g_block_mu);
    for (int i = 0; i < BLOCK_TABLE_SIZE && n_expired < MAX_BATCH; i++) {
        block_entry_t *e = block_table[i];
        while (e && n_expired < MAX_BATCH) {
            block_waiter_t **prev = &e->waiters;
            block_waiter_t *w = e->waiters;
            while (w && n_expired < MAX_BATCH) {
                client_t *c = w->client;
                if (c && c->state == CLIENT_BLOCKED && c->block_until_ms <= now) {
                    *prev = w->next;
                    free(w);
                    w = *prev;
                    /* Mark as not blocked so a parallel block_wake_one
                     * doesn't re-fire the callback for this client. */
                    c->state = CLIENT_IDLE;
                    c->block_key_count = 0;
                    expired[n_expired++] = c;
                } else {
                    prev = &w->next;
                    w = w->next;
                }
            }
            e = e->next;
        }
    }
    pthread_mutex_unlock(&g_block_mu);

    /* Phase 2 — outside the lock: scrub remaining waiter entries (uses
     * its own lock acquisition) and dispatch notify_cb.
     *
     * MT-safety: do NOT write conn->write_buf here.  block_check_timeouts
     * runs on worker 0, but the conn may be owned by another worker that
     * is concurrently running EVENT_READ/EVENT_WRITE on the same write_buf.
     * Instead, set block_pending_timeout and let proactor_block_notify_local
     * (which always runs on the owner worker) emit the "$-1\r\n" reply
     * when it processes the wake. */
    for (int i = 0; i < n_expired; i++) {
        client_t *c = expired[i];
        block_remove_client(c);
        c->block_pending_timeout = 1;
        if (g_notify_cb) g_notify_cb(c);
    }
}
