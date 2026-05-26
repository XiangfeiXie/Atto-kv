#include "journal.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
typedef struct {
    uint64_t seq;
    uint16_t kind;
    uint16_t flags;
    uint32_t len;
    uint32_t crc32;
    char *payload;
    uint32_t cap;
} journal_backlog_slot_t;

static journal_backlog_slot_t g_backlog[KVS_JOURNAL_BACKLOG_CAP];
static pthread_mutex_t g_journal_mu = PTHREAD_MUTEX_INITIALIZER;
static _Atomic uint64_t g_next_seq = 0;
static int g_journal_ready = 0;

int kvs_journal_init(void)
{
    pthread_mutex_lock(&g_journal_mu);
    if (!g_journal_ready) {
        memset(g_backlog, 0, sizeof(g_backlog));
        atomic_store_explicit(&g_next_seq, 0, memory_order_relaxed);
        g_journal_ready = 1;
    }
    pthread_mutex_unlock(&g_journal_mu);
    return 0;
}

void kvs_journal_shutdown(void)
{
    pthread_mutex_lock(&g_journal_mu);
    for (uint32_t i = 0; i < KVS_JOURNAL_BACKLOG_CAP; i++) {
        free(g_backlog[i].payload);
        memset(&g_backlog[i], 0, sizeof(g_backlog[i]));
    }
    g_journal_ready = 0;
    pthread_mutex_unlock(&g_journal_mu);
}

uint64_t kvs_journal_next_seq(void)
{
    return atomic_load_explicit(&g_next_seq, memory_order_acquire);
}

int kvs_journal_append(uint16_t kind, uint16_t flags,
                       const char *payload, uint32_t len,
                       uint64_t *out_seq, uint32_t *out_crc)
{
    if (!payload || len == 0 || len > KVS_JOURNAL_MAX_PAYLOAD) return -1;
    if (!g_journal_ready && kvs_journal_init() != 0) return -1;

    uint64_t seq = atomic_fetch_add_explicit(&g_next_seq, 1, memory_order_acq_rel);
    uint32_t idx = (uint32_t)seq & KVS_JOURNAL_BACKLOG_MASK;
    uint32_t crc = kvs_crc32c(0, &seq, sizeof(seq));
    crc = kvs_crc32c(crc, &kind, sizeof(kind));
    crc = kvs_crc32c(crc, &flags, sizeof(flags));
    crc = kvs_crc32c(crc, &len, sizeof(len));
    crc = kvs_crc32c(crc, payload, len);

    pthread_mutex_lock(&g_journal_mu);
    journal_backlog_slot_t *s = &g_backlog[idx];
    if (s->cap < len) {
        char *np = (char *)realloc(s->payload, len);
        if (!np) {
            pthread_mutex_unlock(&g_journal_mu);
            return -1;
        }
        s->payload = np;
        s->cap = len;
    }
    memcpy(s->payload, payload, len);
    s->seq = seq;
    s->kind = kind;
    s->flags = flags;
    s->len = len;
    s->crc32 = crc;
    pthread_mutex_unlock(&g_journal_mu);

    if (out_seq) *out_seq = seq;
    if (out_crc) *out_crc = crc;
    return 0;
}

int kvs_journal_copy(uint64_t seq, kvs_journal_record_t *out)
{
    if (!out) return -1;
    memset(out, 0, sizeof(*out));
    pthread_mutex_lock(&g_journal_mu);
    journal_backlog_slot_t *s = &g_backlog[(uint32_t)seq & KVS_JOURNAL_BACKLOG_MASK];
    if (s->seq != seq || !s->payload || s->len == 0) {
        pthread_mutex_unlock(&g_journal_mu);
        return -1;
    }
    out->payload = (char *)malloc(s->len);
    if (!out->payload) {
        pthread_mutex_unlock(&g_journal_mu);
        return -1;
    }
    memcpy(out->payload, s->payload, s->len);
    out->seq = s->seq;
    out->kind = s->kind;
    out->flags = s->flags;
    out->len = s->len;
    out->crc32 = s->crc32;
    pthread_mutex_unlock(&g_journal_mu);
    return 0;
}

void kvs_journal_record_release(kvs_journal_record_t *rec)
{
    if (!rec) return;
    free(rec->payload);
    memset(rec, 0, sizeof(*rec));
}
