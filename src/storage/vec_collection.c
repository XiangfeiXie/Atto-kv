#include "vec_collection.h"
#include "flat_index.h"

#include <stdlib.h>
#include <string.h>

struct vec_collection {
    uint64_t collection_id;
    char name[VEC_MAX_COLLECTION_NAME];
    uint32_t dim;
    vec_metric_t metric;
    vec_index_type_t index_type;
    pthread_rwlock_t lock;
    vec_mutable_segment_t *mutable;
    vec_sealed_segment_t **sealed;
    uint32_t sealed_count;
    uint32_t sealed_cap;
    uint64_t next_segment_id;
    uint64_t created_lsn;
    uint64_t checkpoint_lsn;
};

vec_collection_t *vec_collection_create(uint64_t collection_id,
                                        const char *name,
                                        uint32_t name_len,
                                        uint32_t dim,
                                        vec_metric_t metric,
                                        vec_index_type_t index_type)
{
    if (!name || name_len == 0 || name_len >= VEC_MAX_COLLECTION_NAME || dim == 0 || dim > VEC_MAX_DIM)
        return NULL;
    vec_collection_t *c = (vec_collection_t *)calloc(1, sizeof(*c));
    if (!c) return NULL;
    c->collection_id = collection_id;
    memcpy(c->name, name, name_len);
    c->name[name_len] = '\0';
    c->dim = dim;
    c->metric = metric;
    c->index_type = index_type;
    c->next_segment_id = 1;
    pthread_rwlock_init(&c->lock, NULL);
    c->mutable = vec_mutable_create(collection_id, c->next_segment_id++, dim, VEC_MUTABLE_INITIAL_CAP);
    if (!c->mutable) {
        vec_collection_destroy(c);
        return NULL;
    }
    return c;
}

void vec_collection_destroy(vec_collection_t *c)
{
    if (!c) return;
    vec_mutable_destroy(c->mutable);
    free(c->sealed);
    pthread_rwlock_destroy(&c->lock);
    free(c);
}

int vec_collection_put(vec_collection_t *c, const char *id, uint32_t id_len,
                       const float *vec, uint64_t lsn)
{
    if (!c) return -1;
    pthread_rwlock_rdlock(&c->lock);
    int rc = vec_mutable_put(c->mutable, id, id_len, vec, lsn);
    pthread_rwlock_unlock(&c->lock);
    return rc;
}

int vec_collection_delete(vec_collection_t *c, const char *id, uint32_t id_len,
                          uint64_t lsn)
{
    if (!c) return -1;
    pthread_rwlock_rdlock(&c->lock);
    int rc = vec_mutable_delete(c->mutable, id, id_len, lsn);
    pthread_rwlock_unlock(&c->lock);
    return rc;
}

int vec_collection_get(vec_collection_t *c, const char *id, uint32_t id_len,
                       float *out)
{
    if (!c) return -1;
    pthread_rwlock_rdlock(&c->lock);
    int rc = vec_mutable_get(c->mutable, id, id_len, out);
    pthread_rwlock_unlock(&c->lock);
    return rc;
}

int vec_collection_search(vec_collection_t *c, const float *query, uint32_t topk,
                          vec_search_result_t *out)
{
    if (!c || !query || !out) return -1;
    pthread_rwlock_rdlock(&c->lock);
    int rc = vec_mutable_search(c->mutable, query, topk, c->metric, out);
    pthread_rwlock_unlock(&c->lock);
    vec_result_sort(out);
    return rc;
}

uint64_t vec_collection_count(vec_collection_t *c)
{
    if (!c) return 0;
    pthread_rwlock_rdlock(&c->lock);
    uint64_t n = vec_mutable_count(c->mutable);
    pthread_rwlock_unlock(&c->lock);
    return n;
}

uint32_t vec_collection_dim(vec_collection_t *c)
{
    return c ? c->dim : 0;
}

vec_metric_t vec_collection_metric(vec_collection_t *c)
{
    return c ? c->metric : VEC_METRIC_L2;
}

const char *vec_collection_name(vec_collection_t *c)
{
    return c ? c->name : "";
}
