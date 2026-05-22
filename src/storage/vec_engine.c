#include "vec_engine.h"
#include "flat_index.h"

#include <stdlib.h>
#include <string.h>

#define VEC_ENGINE_INITIAL_COLLECTIONS 16u

typedef struct {
    pthread_rwlock_t lock;
    vec_collection_t **collections;
    uint32_t count;
    uint32_t cap;
    uint64_t next_collection_id;
    uint64_t next_lsn;
    int initialized;
} vec_engine_t;

static vec_engine_t g_vec_engine;

static int name_eq(vec_collection_t *c, const char *name, uint32_t len)
{
    const char *cn = vec_collection_name(c);
    return strlen(cn) == len && memcmp(cn, name, len) == 0;
}

static int find_collection_idx(const char *name, uint32_t len)
{
    for (uint32_t i = 0; i < g_vec_engine.count; i++) {
        if (name_eq(g_vec_engine.collections[i], name, len)) return (int)i;
    }
    return -1;
}

static int ensure_col_cap(uint32_t need)
{
    if (need <= g_vec_engine.cap) return 0;
    uint32_t nc = g_vec_engine.cap ? g_vec_engine.cap * 2 : VEC_ENGINE_INITIAL_COLLECTIONS;
    while (nc < need) nc *= 2;
    vec_collection_t **p = (vec_collection_t **)realloc(g_vec_engine.collections, (size_t)nc * sizeof(*p));
    if (!p) return -1;
    g_vec_engine.collections = p;
    g_vec_engine.cap = nc;
    return 0;
}

int vec_engine_init(void)
{
    if (g_vec_engine.initialized) return 0;
    memset(&g_vec_engine, 0, sizeof(g_vec_engine));
    pthread_rwlock_init(&g_vec_engine.lock, NULL);
    g_vec_engine.next_collection_id = 1;
    g_vec_engine.next_lsn = 1;
    g_vec_engine.initialized = 1;
    return 0;
}

void vec_engine_shutdown(void)
{
    if (!g_vec_engine.initialized) return;
    vec_engine_clear();
    pthread_rwlock_destroy(&g_vec_engine.lock);
    memset(&g_vec_engine, 0, sizeof(g_vec_engine));
}

void vec_engine_clear(void)
{
    if (!g_vec_engine.initialized) return;
    pthread_rwlock_wrlock(&g_vec_engine.lock);
    for (uint32_t i = 0; i < g_vec_engine.count; i++)
        vec_collection_destroy(g_vec_engine.collections[i]);
    free(g_vec_engine.collections);
    g_vec_engine.collections = NULL;
    g_vec_engine.count = 0;
    g_vec_engine.cap = 0;
    g_vec_engine.next_collection_id = 1;
    g_vec_engine.next_lsn = 1;
    pthread_rwlock_unlock(&g_vec_engine.lock);
}

int vec_engine_create_collection(const char *name,
                                 uint32_t name_len,
                                 uint32_t dim,
                                 vec_metric_t metric,
                                 vec_index_type_t index_type)
{
    if (!g_vec_engine.initialized && vec_engine_init() != 0) return -1;
    if (!name || name_len == 0 || name_len >= VEC_MAX_COLLECTION_NAME || dim == 0 || dim > VEC_MAX_DIM)
        return -1;
    pthread_rwlock_wrlock(&g_vec_engine.lock);
    if (find_collection_idx(name, name_len) >= 0) {
        pthread_rwlock_unlock(&g_vec_engine.lock);
        return 1;
    }
    if (ensure_col_cap(g_vec_engine.count + 1) != 0) {
        pthread_rwlock_unlock(&g_vec_engine.lock);
        return -1;
    }
    vec_collection_t *c = vec_collection_create(g_vec_engine.next_collection_id++, name, name_len, dim, metric, index_type);
    if (!c) {
        pthread_rwlock_unlock(&g_vec_engine.lock);
        return -1;
    }
    g_vec_engine.collections[g_vec_engine.count++] = c;
    pthread_rwlock_unlock(&g_vec_engine.lock);
    return 0;
}

int vec_engine_drop_collection(const char *name, uint32_t name_len)
{
    if (!g_vec_engine.initialized || !name) return -1;
    pthread_rwlock_wrlock(&g_vec_engine.lock);
    int idx = find_collection_idx(name, name_len);
    if (idx < 0) {
        pthread_rwlock_unlock(&g_vec_engine.lock);
        return 0;
    }
    vec_collection_destroy(g_vec_engine.collections[idx]);
    for (uint32_t i = (uint32_t)idx + 1; i < g_vec_engine.count; i++)
        g_vec_engine.collections[i - 1] = g_vec_engine.collections[i];
    g_vec_engine.count--;
    pthread_rwlock_unlock(&g_vec_engine.lock);
    return 1;
}

vec_collection_t *vec_engine_get_collection(const char *name, uint32_t name_len)
{
    if (!g_vec_engine.initialized || !name) return NULL;
    pthread_rwlock_rdlock(&g_vec_engine.lock);
    int idx = find_collection_idx(name, name_len);
    vec_collection_t *c = idx >= 0 ? g_vec_engine.collections[idx] : NULL;
    pthread_rwlock_unlock(&g_vec_engine.lock);
    return c;
}

int vec_engine_put(const char *collection,
                   uint32_t collection_len,
                   const char *id,
                   uint32_t id_len,
                   const float *vec,
                   uint32_t dim,
                   vec_metric_t default_metric)
{
    if (!collection || collection_len == 0 || !id || id_len == 0 || !vec) return -1;
    if (!g_vec_engine.initialized && vec_engine_init() != 0) return -1;
    vec_collection_t *c = vec_engine_get_collection(collection, collection_len);
    if (!c) {
        int rc = vec_engine_create_collection(collection, collection_len, dim, default_metric, VEC_INDEX_FLAT);
        if (rc < 0) return -1;
        c = vec_engine_get_collection(collection, collection_len);
    }
    if (!c || vec_collection_dim(c) != dim) return -2;
    uint64_t lsn = __atomic_fetch_add(&g_vec_engine.next_lsn, 1, __ATOMIC_RELAXED);
    return vec_collection_put(c, id, id_len, vec, lsn);
}

int vec_engine_delete(const char *collection,
                      uint32_t collection_len,
                      const char *id,
                      uint32_t id_len)
{
    vec_collection_t *c = vec_engine_get_collection(collection, collection_len);
    if (!c) return 0;
    uint64_t lsn = __atomic_fetch_add(&g_vec_engine.next_lsn, 1, __ATOMIC_RELAXED);
    return vec_collection_delete(c, id, id_len, lsn);
}

int vec_engine_get(const char *collection,
                   uint32_t collection_len,
                   const char *id,
                   uint32_t id_len,
                   float *out,
                   uint32_t *dim_out)
{
    vec_collection_t *c = vec_engine_get_collection(collection, collection_len);
    if (!c) return 0;
    if (dim_out) *dim_out = vec_collection_dim(c);
    return vec_collection_get(c, id, id_len, out);
}

int vec_engine_search(const char *collection,
                      uint32_t collection_len,
                      const float *query,
                      uint32_t dim,
                      uint32_t topk,
                      vec_search_result_t *out)
{
    if (!out) return -1;
    vec_collection_t *c = vec_engine_get_collection(collection, collection_len);
    if (!c) {
        if (vec_result_init(out, topk, VEC_METRIC_L2) != 0) return -1;
        return 0;
    }
    if (vec_collection_dim(c) != dim) return -2;
    if (vec_result_init(out, topk, vec_collection_metric(c)) != 0) return -1;
    return vec_collection_search(c, query, topk, out);
}

uint64_t vec_engine_count_collection(const char *collection, uint32_t collection_len)
{
    vec_collection_t *c = vec_engine_get_collection(collection, collection_len);
    return c ? vec_collection_count(c) : 0;
}

uint64_t vec_engine_total_count(void)
{
    if (!g_vec_engine.initialized) return 0;
    pthread_rwlock_rdlock(&g_vec_engine.lock);
    uint64_t n = 0;
    for (uint32_t i = 0; i < g_vec_engine.count; i++)
        n += vec_collection_count(g_vec_engine.collections[i]);
    pthread_rwlock_unlock(&g_vec_engine.lock);
    return n;
}

const char *vec_metric_name(vec_metric_t metric)
{
    switch (metric) {
    case VEC_METRIC_IP: return "IP";
    case VEC_METRIC_COSINE: return "COSINE";
    case VEC_METRIC_L2:
    default: return "L2";
    }
}

int vec_metric_parse(const char *s, uint32_t len, vec_metric_t *out)
{
    if (!s || !out) return -1;
    if (len == 2 && memcmp(s, "L2", 2) == 0) { *out = VEC_METRIC_L2; return 0; }
    if (len == 2 && memcmp(s, "IP", 2) == 0) { *out = VEC_METRIC_IP; return 0; }
    if (len == 6 && memcmp(s, "COSINE", 6) == 0) { *out = VEC_METRIC_COSINE; return 0; }
    if (len == 3 && memcmp(s, "COS", 3) == 0) { *out = VEC_METRIC_COSINE; return 0; }
    return -1;
}
