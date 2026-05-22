#include "vec_segment.h"
#include "flat_index.h"
#include "vector_math.h"

#include <stdlib.h>
#include <string.h>

struct vec_mutable_segment {
    uint64_t collection_id;
    uint64_t segment_id;
    uint64_t row_count;
    uint64_t capacity;
    uint64_t min_lsn;
    uint64_t max_lsn;
    uint32_t dim;
    pthread_rwlock_t lock;
    char **ids;
    uint32_t *id_lens;
    float *vectors;
    uint8_t *deleted;
};

static int id_equal(vec_mutable_segment_t *s, uint64_t i, const char *id, uint32_t id_len)
{
    return s->id_lens[i] == id_len && memcmp(s->ids[i], id, id_len) == 0;
}

static int find_row(vec_mutable_segment_t *s, const char *id, uint32_t id_len)
{
    for (uint64_t i = 0; i < s->row_count; i++) {
        if (!s->deleted[i] && id_equal(s, i, id, id_len)) return (int)i;
    }
    return -1;
}

static int ensure_cap(vec_mutable_segment_t *s, uint64_t need)
{
    if (need <= s->capacity) return 0;
    uint64_t nc = s->capacity ? s->capacity * 2 : VEC_MUTABLE_INITIAL_CAP;
    while (nc < need) nc *= 2;

    char **ids = (char **)calloc((size_t)nc, sizeof(char *));
    uint32_t *id_lens = (uint32_t *)calloc((size_t)nc, sizeof(uint32_t));
    uint8_t *deleted = (uint8_t *)calloc((size_t)nc, sizeof(uint8_t));
    float *vectors = (float *)malloc((size_t)nc * s->dim * sizeof(float));
    if (!ids || !id_lens || !deleted || !vectors) {
        free(ids);
        free(id_lens);
        free(deleted);
        free(vectors);
        return -1;
    }

    if (s->row_count > 0) {
        memcpy(ids, s->ids, (size_t)s->row_count * sizeof(char *));
        memcpy(id_lens, s->id_lens, (size_t)s->row_count * sizeof(uint32_t));
        memcpy(deleted, s->deleted, (size_t)s->row_count * sizeof(uint8_t));
        memcpy(vectors, s->vectors, (size_t)s->row_count * s->dim * sizeof(float));
    }

    free(s->ids);
    free(s->id_lens);
    free(s->deleted);
    free(s->vectors);
    s->ids = ids;
    s->id_lens = id_lens;
    s->deleted = deleted;
    s->vectors = vectors;
    s->capacity = nc;
    return 0;
}

vec_mutable_segment_t *vec_mutable_create(uint64_t collection_id,
                                          uint64_t segment_id,
                                          uint32_t dim,
                                          uint64_t capacity)
{
    if (dim == 0 || dim > VEC_MAX_DIM) return NULL;
    vec_mutable_segment_t *s = (vec_mutable_segment_t *)calloc(1, sizeof(*s));
    if (!s) return NULL;
    s->collection_id = collection_id;
    s->segment_id = segment_id;
    s->dim = dim;
    pthread_rwlock_init(&s->lock, NULL);
    if (ensure_cap(s, capacity ? capacity : VEC_MUTABLE_INITIAL_CAP) != 0) {
        vec_mutable_destroy(s);
        return NULL;
    }
    return s;
}

void vec_mutable_destroy(vec_mutable_segment_t *s)
{
    if (!s) return;
    for (uint64_t i = 0; i < s->row_count; i++) free(s->ids[i]);
    free(s->ids);
    free(s->id_lens);
    free(s->vectors);
    free(s->deleted);
    pthread_rwlock_destroy(&s->lock);
    free(s);
}

uint64_t vec_mutable_count(vec_mutable_segment_t *s)
{
    if (!s) return 0;
    pthread_rwlock_rdlock(&s->lock);
    uint64_t n = 0;
    for (uint64_t i = 0; i < s->row_count; i++) if (!s->deleted[i]) n++;
    pthread_rwlock_unlock(&s->lock);
    return n;
}

int vec_mutable_put(vec_mutable_segment_t *s,
                    const char *id,
                    uint32_t id_len,
                    const float *vec,
                    uint64_t lsn)
{
    if (!s || !id || id_len == 0 || !vec) return -1;
    pthread_rwlock_wrlock(&s->lock);
    int row = find_row(s, id, id_len);
    if (row >= 0) {
        memcpy(s->vectors + (uint64_t)row * s->dim, vec, (size_t)s->dim * sizeof(float));
        s->max_lsn = lsn;
        if (s->min_lsn == 0 || lsn < s->min_lsn) s->min_lsn = lsn;
        pthread_rwlock_unlock(&s->lock);
        return 0;
    }
    if (ensure_cap(s, s->row_count + 1) != 0) {
        pthread_rwlock_unlock(&s->lock);
        return -1;
    }
    uint64_t i = s->row_count++;
    s->ids[i] = (char *)malloc(id_len);
    if (!s->ids[i]) {
        s->row_count--;
        pthread_rwlock_unlock(&s->lock);
        return -1;
    }
    memcpy(s->ids[i], id, id_len);
    s->id_lens[i] = id_len;
    s->deleted[i] = 0;
    memcpy(s->vectors + i * s->dim, vec, (size_t)s->dim * sizeof(float));
    s->max_lsn = lsn;
    if (s->min_lsn == 0 || lsn < s->min_lsn) s->min_lsn = lsn;
    pthread_rwlock_unlock(&s->lock);
    return 0;
}

int vec_mutable_delete(vec_mutable_segment_t *s,
                       const char *id,
                       uint32_t id_len,
                       uint64_t lsn)
{
    if (!s || !id || id_len == 0) return -1;
    pthread_rwlock_wrlock(&s->lock);
    int row = find_row(s, id, id_len);
    if (row >= 0) {
        s->deleted[row] = 1;
        s->max_lsn = lsn;
        pthread_rwlock_unlock(&s->lock);
        return 1;
    }
    pthread_rwlock_unlock(&s->lock);
    return 0;
}

int vec_mutable_get(vec_mutable_segment_t *s,
                    const char *id,
                    uint32_t id_len,
                    float *out)
{
    if (!s || !id || !out) return -1;
    pthread_rwlock_rdlock(&s->lock);
    int row = find_row(s, id, id_len);
    if (row < 0) {
        pthread_rwlock_unlock(&s->lock);
        return 0;
    }
    memcpy(out, s->vectors + (uint64_t)row * s->dim, (size_t)s->dim * sizeof(float));
    pthread_rwlock_unlock(&s->lock);
    return 1;
}

int vec_mutable_search(vec_mutable_segment_t *s,
                       const float *query,
                       uint32_t topk,
                       vec_metric_t metric,
                       vec_search_result_t *out)
{
    if (!s || !query || !out || topk == 0) return -1;
    pthread_rwlock_rdlock(&s->lock);
    for (uint64_t i = 0; i < s->row_count; i++) {
        if (s->deleted[i]) continue;
        const float *v = s->vectors + i * s->dim;
        float score = vec_score(query, v, s->dim, metric);
        vec_result_consider(out, s->segment_id, (uint32_t)i, s->ids[i], s->id_lens[i], score);
    }
    pthread_rwlock_unlock(&s->lock);
    return 0;
}
