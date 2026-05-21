#include "vector_store.h"
#include "kv_dict.h"

#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#if defined(__AVX2__)
#include <immintrin.h>
#endif

#define VECTOR_SHARDS KV_NR_SHARDS
#define VECTOR_MAX_DIM 4096u
#define VECTOR_MAX_TOPK 1024u
#define VECTOR_MUT_FLAG_REPLACE 0x1u

typedef enum {
    VECTOR_METRIC_L2 = 0,
    VECTOR_METRIC_IP = 1,
    VECTOR_METRIC_COSINE = 2,
} vector_metric_t;

typedef struct vector_entry {
    struct vector_entry *next;
    uint64_t hash;
    uint32_t id_len;
    uint32_t dim;
    float *vec;
    char id[];
} vector_entry_t;

typedef struct vector_shard {
    pthread_rwlock_t lock;
    vector_entry_t *head;
    uint64_t count;
    char _pad[128 - sizeof(pthread_rwlock_t) - sizeof(vector_entry_t *) - sizeof(uint64_t)];
} __attribute__((aligned(128))) vector_shard_t;

static vector_shard_t g_vector_shards[VECTOR_SHARDS];

static inline vector_shard_t *vector_shard_for_hash(uint64_t hash)
{
    return &g_vector_shards[(hash >> 32) & KV_SHARD_MASK];
}

static void reply_integer_local(DynBuf *wb, long long v)
{
    char buf[32];
    int n = snprintf(buf, sizeof(buf), ":%lld\r\n", v);
    dynbuf_cat(wb, buf, n);
}

static void reply_bulk_local(DynBuf *wb, const char *s, int len)
{
    char hdr[32];
    int n = snprintf(hdr, sizeof(hdr), "$%d\r\n", len);
    dynbuf_cat(wb, hdr, n);
    if (len > 0) dynbuf_cat(wb, s, len);
    dynbuf_lit(wb, "\r\n");
}

static int token_to_float(kvs_token_t t, float *out)
{
    if (t.len <= 0 || t.len >= 64) return -1;
    char buf[64];
    memcpy(buf, t.ptr, (size_t)t.len);
    buf[t.len] = '\0';
    char *end = NULL;
    float v = strtof(buf, &end);
    if (!end || *end != '\0' || !isfinite(v)) return -1;
    *out = v;
    return 0;
}

static int token_eq_lit(kvs_token_t t, const char *lit)
{
    size_t n = strlen(lit);
    return t.len == (int)n && memcmp(t.ptr, lit, n) == 0;
}

static int token_to_metric(kvs_token_t t, vector_metric_t *metric)
{
    if (token_eq_lit(t, "L2")) {
        *metric = VECTOR_METRIC_L2;
        return 0;
    }
    if (token_eq_lit(t, "IP") || token_eq_lit(t, "INNER_PRODUCT")) {
        *metric = VECTOR_METRIC_IP;
        return 0;
    }
    if (token_eq_lit(t, "COSINE") || token_eq_lit(t, "COS")) {
        *metric = VECTOR_METRIC_COSINE;
        return 0;
    }
    return -1;
}

static const char *metric_name(vector_metric_t metric)
{
    switch (metric) {
    case VECTOR_METRIC_IP: return "IP";
    case VECTOR_METRIC_COSINE: return "COSINE";
    case VECTOR_METRIC_L2:
    default: return "L2";
    }
}

static int token_to_u32(kvs_token_t t, uint32_t *out)
{
    if (t.len <= 0 || t.len > 10) return -1;
    uint64_t v = 0;
    for (int i = 0; i < t.len; i++) {
        unsigned c = (unsigned char)t.ptr[i] - (unsigned)'0';
        if (c > 9) return -1;
        v = v * 10 + c;
        if (v > UINT32_MAX) return -1;
    }
    *out = (uint32_t)v;
    return 0;
}

static int parse_vector_args(kvs_token_t *tokens, int start, uint32_t dim, float *vec)
{
    for (uint32_t i = 0; i < dim; i++) {
        if (token_to_float(tokens[start + (int)i], &vec[i]) != 0)
            return -1;
    }
    return 0;
}

static float *vector_alloc(uint32_t dim)
{
    void *p = NULL;
    if (posix_memalign(&p, 32, (size_t)dim * sizeof(float)) != 0)
        return NULL;
    return (float *)p;
}

static float vector_dot(const float *a, const float *b, uint32_t dim)
{
#if defined(__AVX2__)
    __m256 acc = _mm256_setzero_ps();
    uint32_t i = 0;
    for (; i + 8 <= dim; i += 8) {
        __m256 va = _mm256_loadu_ps(a + i);
        __m256 vb = _mm256_loadu_ps(b + i);
        acc = _mm256_add_ps(acc, _mm256_mul_ps(va, vb));
    }
    float tmp[8];
    _mm256_storeu_ps(tmp, acc);
    float sum = tmp[0] + tmp[1] + tmp[2] + tmp[3] + tmp[4] + tmp[5] + tmp[6] + tmp[7];
    for (; i < dim; i++) sum += a[i] * b[i];
    return sum;
#else
    float sum = 0.0f;
    for (uint32_t i = 0; i < dim; i++) sum += a[i] * b[i];
    return sum;
#endif
}

static float vector_norm2(const float *a, uint32_t dim)
{
    return vector_dot(a, a, dim);
}

static float vector_score(const float *query, const float *vec,
                          uint32_t dim, vector_metric_t metric)
{
    if (metric == VECTOR_METRIC_IP)
        return vector_dot(query, vec, dim);
    if (metric == VECTOR_METRIC_COSINE) {
        float qn = vector_norm2(query, dim);
        float vn = vector_norm2(vec, dim);
        if (qn <= 0.0f || vn <= 0.0f) return -1.0f;
        return vector_dot(query, vec, dim) / sqrtf(qn * vn);
    }

#if defined(__AVX2__)
    __m256 acc = _mm256_setzero_ps();
    uint32_t i = 0;
    for (; i + 8 <= dim; i += 8) {
        __m256 va = _mm256_loadu_ps(query + i);
        __m256 vb = _mm256_loadu_ps(vec + i);
        __m256 d = _mm256_sub_ps(va, vb);
        acc = _mm256_add_ps(acc, _mm256_mul_ps(d, d));
    }
    float tmp[8];
    _mm256_storeu_ps(tmp, acc);
    float sum = tmp[0] + tmp[1] + tmp[2] + tmp[3] + tmp[4] + tmp[5] + tmp[6] + tmp[7];
    for (; i < dim; i++) {
        float d = query[i] - vec[i];
        sum += d * d;
    }
    return sum;
#else
    float sum = 0.0f;
    for (uint32_t i = 0; i < dim; i++) {
        float d = query[i] - vec[i];
        sum += d * d;
    }
    return sum;
#endif
}

static int score_better(float a, float b, vector_metric_t metric)
{
    return metric == VECTOR_METRIC_L2 ? a < b : a > b;
}

void vector_store_init(void)
{
    for (uint32_t i = 0; i < VECTOR_SHARDS; i++) {
        pthread_rwlock_init(&g_vector_shards[i].lock, NULL);
        g_vector_shards[i].head = NULL;
        g_vector_shards[i].count = 0;
    }
}

static void vector_entry_free(vector_entry_t *e)
{
    if (!e) return;
    free(e->vec);
    free(e);
}

void vector_store_clear(void)
{
    for (uint32_t i = 0; i < VECTOR_SHARDS; i++) {
        vector_shard_t *s = &g_vector_shards[i];
        pthread_rwlock_wrlock(&s->lock);
        vector_entry_t *e = s->head;
        while (e) {
            vector_entry_t *next = e->next;
            vector_entry_free(e);
            e = next;
        }
        s->head = NULL;
        s->count = 0;
        pthread_rwlock_unlock(&s->lock);
    }
}

void vector_store_destroy(void)
{
    vector_store_clear();
    for (uint32_t i = 0; i < VECTOR_SHARDS; i++)
        pthread_rwlock_destroy(&g_vector_shards[i].lock);
}

uint64_t vector_store_count(void)
{
    uint64_t n = 0;
    for (uint32_t i = 0; i < VECTOR_SHARDS; i++) {
        vector_shard_t *s = &g_vector_shards[i];
        pthread_rwlock_rdlock(&s->lock);
        n += s->count;
        pthread_rwlock_unlock(&s->lock);
    }
    return n;
}

static vector_entry_t *vector_find_locked(vector_shard_t *s,
                                          const char *id,
                                          uint32_t id_len,
                                          uint64_t hash)
{
    for (vector_entry_t *e = s->head; e; e = e->next) {
        if (e->hash == hash && e->id_len == id_len && memcmp(e->id, id, id_len) == 0)
            return e;
    }
    return NULL;
}

static int vector_put(kvs_token_t id, uint32_t dim, const float *vec)
{
    uint64_t hash = kv_hash((const uint8_t *)id.ptr, (uint32_t)id.len);
    vector_shard_t *s = vector_shard_for_hash(hash);
    pthread_rwlock_wrlock(&s->lock);
    vector_entry_t *e = vector_find_locked(s, id.ptr, (uint32_t)id.len, hash);
    if (e) {
        if (e->dim != dim) {
            float *nv = vector_alloc(dim);
            if (!nv) { pthread_rwlock_unlock(&s->lock); return -1; }
            free(e->vec);
            e->vec = nv;
            e->dim = dim;
        }
        memcpy(e->vec, vec, (size_t)dim * sizeof(float));
        pthread_rwlock_unlock(&s->lock);
        return 0;
    }

    e = (vector_entry_t *)calloc(1, sizeof(*e) + (size_t)id.len);
    if (!e) { pthread_rwlock_unlock(&s->lock); return -1; }
    e->vec = vector_alloc(dim);
    if (!e->vec) { free(e); pthread_rwlock_unlock(&s->lock); return -1; }
    e->hash = hash;
    e->id_len = (uint32_t)id.len;
    e->dim = dim;
    memcpy(e->id, id.ptr, (size_t)id.len);
    memcpy(e->vec, vec, (size_t)dim * sizeof(float));
    e->next = s->head;
    s->head = e;
    s->count++;
    pthread_rwlock_unlock(&s->lock);
    return 0;
}

static dispatch_result_t vector_dim_cmd(kvs_token_t id, DynBuf *wb)
{
    uint64_t hash = kv_hash((const uint8_t *)id.ptr, (uint32_t)id.len);
    vector_shard_t *s = vector_shard_for_hash(hash);
    pthread_rwlock_rdlock(&s->lock);
    vector_entry_t *e = vector_find_locked(s, id.ptr, (uint32_t)id.len, hash);
    if (!e) {
        pthread_rwlock_unlock(&s->lock);
        dynbuf_lit(wb, "$-1\r\n");
        return DISPATCH_OK;
    }
    reply_integer_local(wb, (long long)e->dim);
    pthread_rwlock_unlock(&s->lock);
    return DISPATCH_OK;
}

static dispatch_result_t vector_similarity_cmd(kvs_token_t id1,
                                               kvs_token_t id2,
                                               vector_metric_t metric,
                                               DynBuf *wb)
{
    uint64_t h1 = kv_hash((const uint8_t *)id1.ptr, (uint32_t)id1.len);
    uint64_t h2 = kv_hash((const uint8_t *)id2.ptr, (uint32_t)id2.len);
    vector_shard_t *s1 = vector_shard_for_hash(h1);
    vector_shard_t *s2 = vector_shard_for_hash(h2);

    if (s1 == s2) {
        pthread_rwlock_rdlock(&s1->lock);
    } else if (s1 < s2) {
        pthread_rwlock_rdlock(&s1->lock);
        pthread_rwlock_rdlock(&s2->lock);
    } else {
        pthread_rwlock_rdlock(&s2->lock);
        pthread_rwlock_rdlock(&s1->lock);
    }

    vector_entry_t *e1 = vector_find_locked(s1, id1.ptr, (uint32_t)id1.len, h1);
    vector_entry_t *e2 = vector_find_locked(s2, id2.ptr, (uint32_t)id2.len, h2);
    if (!e1 || !e2 || e1->dim != e2->dim) {
        if (s1 == s2) pthread_rwlock_unlock(&s1->lock);
        else { pthread_rwlock_unlock(&s1->lock); pthread_rwlock_unlock(&s2->lock); }
        dynbuf_lit(wb, "$-1\r\n");
        return DISPATCH_OK;
    }

    float score = vector_score(e1->vec, e2->vec, e1->dim, metric);
    char sbuf[64];
    int sl = snprintf(sbuf, sizeof(sbuf), "%.9g", score);
    if (s1 == s2) pthread_rwlock_unlock(&s1->lock);
    else { pthread_rwlock_unlock(&s1->lock); pthread_rwlock_unlock(&s2->lock); }
    reply_bulk_local(wb, sbuf, sl);
    return DISPATCH_OK;
}

static int vector_delete(kvs_token_t id)
{
    uint64_t hash = kv_hash((const uint8_t *)id.ptr, (uint32_t)id.len);
    vector_shard_t *s = vector_shard_for_hash(hash);
    pthread_rwlock_wrlock(&s->lock);
    vector_entry_t **pp = &s->head;
    while (*pp) {
        vector_entry_t *e = *pp;
        if (e->hash == hash && e->id_len == (uint32_t)id.len && memcmp(e->id, id.ptr, (size_t)id.len) == 0) {
            *pp = e->next;
            vector_entry_free(e);
            s->count--;
            pthread_rwlock_unlock(&s->lock);
            return 1;
        }
        pp = &e->next;
    }
    pthread_rwlock_unlock(&s->lock);
    return 0;
}

static int vector_put_raw(const char *id_ptr, uint32_t id_len, uint32_t dim, const float *vec)
{
    kvs_token_t id = { .ptr = id_ptr, .len = (int)id_len };
    return vector_put(id, dim, vec);
}

static int vector_delete_raw(const char *id_ptr, uint32_t id_len)
{
    kvs_token_t id = { .ptr = id_ptr, .len = (int)id_len };
    return vector_delete(id);
}

int vector_cmd_is_write(kvs_token_t cmd)
{
    return (cmd.len == 10 && memcmp(cmd.ptr, "VECTOR.PUT", 10) == 0) ||
           (cmd.len == 10 && memcmp(cmd.ptr, "VECTOR.DEL", 10) == 0) ||
           (cmd.len == 12 && memcmp(cmd.ptr, "VECTOR.CLEAR", 12) == 0);
}

void vector_mutation_buf_free(vector_mutation_buf_t *m)
{
    if (!m) return;
    free(m->buf);
    m->buf = NULL;
    m->len = 0;
}

static int vector_mutation_alloc(vector_mutation_buf_t *out, uint8_t op,
                                 const char *id, uint32_t id_len,
                                 uint32_t dim, const float *vec)
{
    if (!out) return -1;
    memset(out, 0, sizeof(*out));
    if (id_len > (uint32_t)INT_MAX || dim > VECTOR_MAX_DIM) return -1;
    uint64_t total = sizeof(kvs_vector_mut_hdr_t) + (uint64_t)id_len +
                     (uint64_t)dim * sizeof(float);
    if (total > UINT32_MAX) return -1;
    char *buf = (char *)malloc((size_t)total);
    if (!buf) return -1;
    kvs_vector_mut_hdr_t hdr;
    hdr.op = op;
    hdr.id_len = id_len;
    hdr.dim = dim;
    hdr.flags = VECTOR_MUT_FLAG_REPLACE;
    memcpy(buf, &hdr, sizeof(hdr));
    char *p = buf + sizeof(hdr);
    if (id_len > 0) {
        if (!id) { free(buf); return -1; }
        memcpy(p, id, id_len);
        p += id_len;
    }
    if (dim > 0) {
        if (!vec) { free(buf); return -1; }
        memcpy(p, vec, (size_t)dim * sizeof(float));
    }
    out->buf = buf;
    out->len = (uint32_t)total;
    return 0;
}

int vector_mutation_encode_from_cmd(kvs_token_t cmd,
                                    kvs_token_t *tokens,
                                    int argc,
                                    vector_mutation_buf_t *out)
{
    if (!tokens || !out) return -1;
    if (cmd.len == 10 && memcmp(cmd.ptr, "VECTOR.PUT", 10) == 0) {
        uint32_t dim = 0;
        if (argc < 4 || token_to_u32(tokens[2], &dim) != 0 ||
            dim == 0 || dim > VECTOR_MAX_DIM || argc != 3 + (int)dim)
            return -1;
        float *vec = vector_alloc(dim);
        if (!vec) return -1;
        if (parse_vector_args(tokens, 3, dim, vec) != 0) {
            free(vec);
            return -1;
        }
        int rc = vector_mutation_alloc(out, KVS_VECTOR_MUT_PUT,
                                       tokens[1].ptr, (uint32_t)tokens[1].len,
                                       dim, vec);
        free(vec);
        return rc;
    }
    if (cmd.len == 10 && memcmp(cmd.ptr, "VECTOR.DEL", 10) == 0) {
        if (argc != 2) return -1;
        return vector_mutation_alloc(out, KVS_VECTOR_MUT_DEL,
                                     tokens[1].ptr, (uint32_t)tokens[1].len,
                                     0, NULL);
    }
    if (cmd.len == 12 && memcmp(cmd.ptr, "VECTOR.CLEAR", 12) == 0) {
        if (argc != 1) return -1;
        return vector_mutation_alloc(out, KVS_VECTOR_MUT_CLEAR, NULL, 0, 0, NULL);
    }
    return -1;
}

int vector_mutation_apply(const char *buf, uint32_t len)
{
    if (!buf || len < sizeof(kvs_vector_mut_hdr_t)) return -1;
    kvs_vector_mut_hdr_t hdr;
    memcpy(&hdr, buf, sizeof(hdr));
    uint64_t need = sizeof(hdr) + (uint64_t)hdr.id_len +
                    (uint64_t)hdr.dim * sizeof(float);
    if (need != len || hdr.dim > VECTOR_MAX_DIM) return -1;
    const char *id = buf + sizeof(hdr);
    const float *vec = (const float *)(const void *)(id + hdr.id_len);
    if (hdr.op == KVS_VECTOR_MUT_PUT) {
        if (hdr.id_len == 0 || hdr.dim == 0) return -1;
        return vector_put_raw(id, hdr.id_len, hdr.dim, vec);
    }
    if (hdr.op == KVS_VECTOR_MUT_DEL) {
        if (hdr.id_len == 0 || hdr.dim != 0) return -1;
        vector_delete_raw(id, hdr.id_len);
        return 0;
    }
    if (hdr.op == KVS_VECTOR_MUT_CLEAR) {
        if (hdr.id_len != 0 || hdr.dim != 0) return -1;
        vector_store_clear();
        return 0;
    }
    return -1;
}

typedef struct {
    char *id;
    uint32_t id_len;
    float dist;
} topk_item_t;

static void topk_item_clear(topk_item_t *item)
{
    free(item->id);
    item->id = NULL;
    item->id_len = 0;
    item->dist = 0.0f;
}

static int topk_item_set(topk_item_t *item, vector_entry_t *e, float dist)
{
    char *id = (char *)malloc((size_t)e->id_len);
    if (!id) return -1;
    memcpy(id, e->id, (size_t)e->id_len);
    free(item->id);
    item->id = id;
    item->id_len = e->id_len;
    item->dist = dist;
    return 0;
}

static int topk_consider(topk_item_t *top, uint32_t *n, uint32_t k,
                         vector_entry_t *e, float score, vector_metric_t metric)
{
    if (k == 0) return 0;
    if (*n < k) {
        if (topk_item_set(&top[*n], e, score) != 0) return -1;
        (*n)++;
        return 0;
    }
    uint32_t worst = 0;
    for (uint32_t i = 1; i < k; i++) {
        if (score_better(top[worst].dist, top[i].dist, metric)) worst = i;
    }
    if (score_better(score, top[worst].dist, metric))
        return topk_item_set(&top[worst], e, score);
    return 0;
}

static void topk_sort(topk_item_t *top, uint32_t n, vector_metric_t metric)
{
    for (uint32_t i = 1; i < n; i++) {
        topk_item_t v = top[i];
        uint32_t j = i;
        while (j > 0 && score_better(v.dist, top[j - 1].dist, metric)) {
            top[j] = top[j - 1];
            j--;
        }
        top[j] = v;
    }
}

static void vector_reply_get(DynBuf *wb, vector_entry_t *e)
{
    char hdr[32];
    int n = snprintf(hdr, sizeof(hdr), "*%u\r\n", e->dim + 1);
    dynbuf_cat(wb, hdr, n);
    reply_bulk_local(wb, e->id, (int)e->id_len);
    for (uint32_t i = 0; i < e->dim; i++) {
        char fbuf[48];
        int fl = snprintf(fbuf, sizeof(fbuf), "%.9g", e->vec[i]);
        reply_bulk_local(wb, fbuf, fl);
    }
}

static dispatch_result_t vector_get_cmd(kvs_token_t id, DynBuf *wb)
{
    uint64_t hash = kv_hash((const uint8_t *)id.ptr, (uint32_t)id.len);
    vector_shard_t *s = vector_shard_for_hash(hash);
    pthread_rwlock_rdlock(&s->lock);
    vector_entry_t *e = vector_find_locked(s, id.ptr, (uint32_t)id.len, hash);
    if (!e) {
        pthread_rwlock_unlock(&s->lock);
        dynbuf_lit(wb, "$-1\r\n");
        return DISPATCH_OK;
    }
    vector_reply_get(wb, e);
    pthread_rwlock_unlock(&s->lock);
    return DISPATCH_OK;
}

static dispatch_result_t vector_search_cmd(kvs_token_t *tokens, int argc, DynBuf *wb)
{
    uint32_t dim = 0, topk = 0;
    vector_metric_t metric = VECTOR_METRIC_L2;
    int dim_idx = 1;
    int vec_idx = 3;

    if (argc >= 5 && token_to_metric(tokens[1], &metric) == 0) {
        dim_idx = 2;
        vec_idx = 4;
    }
    if (argc <= vec_idx || token_to_u32(tokens[dim_idx], &dim) != 0 ||
        token_to_u32(tokens[dim_idx + 1], &topk) != 0 ||
        dim == 0 || dim > VECTOR_MAX_DIM || topk == 0 ||
        argc != vec_idx + (int)dim) {
        dynbuf_lit(wb, "-ERR usage: VECTOR.SEARCH [L2|IP|COSINE] <dim> <topk> <f1> ... <fdim>\r\n");
        return DISPATCH_ERROR;
    }
    if (topk > VECTOR_MAX_TOPK) topk = VECTOR_MAX_TOPK;
    float *query = vector_alloc(dim);
    topk_item_t *top = (topk_item_t *)calloc(topk, sizeof(*top));
    if (!query || !top) {
        free(query); free(top);
        dynbuf_lit(wb, "-ERR out of memory\r\n");
        return DISPATCH_ERROR;
    }
    if (parse_vector_args(tokens, vec_idx, dim, query) != 0) {
        free(query); free(top);
        dynbuf_lit(wb, "-ERR invalid vector value\r\n");
        return DISPATCH_ERROR;
    }

    uint32_t n = 0;
    for (uint32_t si = 0; si < VECTOR_SHARDS; si++) {
        vector_shard_t *s = &g_vector_shards[si];
        pthread_rwlock_rdlock(&s->lock);
        for (vector_entry_t *e = s->head; e; e = e->next) {
            if (e->dim == dim) {
                float score = vector_score(query, e->vec, dim, metric);
                if (topk_consider(top, &n, topk, e, score, metric) != 0) {
                    pthread_rwlock_unlock(&s->lock);
                    free(query);
                    for (uint32_t j = 0; j < n; j++) topk_item_clear(&top[j]);
                    free(top);
                    dynbuf_lit(wb, "-ERR out of memory\r\n");
                    return DISPATCH_ERROR;
                }
            }
        }
        pthread_rwlock_unlock(&s->lock);
    }
    topk_sort(top, n, metric);

    char hdr[32];
    int hl = snprintf(hdr, sizeof(hdr), "*%u\r\n", n);
    dynbuf_cat(wb, hdr, hl);
    for (uint32_t i = 0; i < n; i++) {
        dynbuf_lit(wb, "*2\r\n");
        reply_bulk_local(wb, top[i].id, (int)top[i].id_len);
        char dbuf[64];
        int dl = snprintf(dbuf, sizeof(dbuf), "%.9g", top[i].dist);
        reply_bulk_local(wb, dbuf, dl);
    }
    free(query);
    for (uint32_t i = 0; i < n; i++) topk_item_clear(&top[i]);
    free(top);
    return DISPATCH_OK;
}

dispatch_result_t vector_cmd(kvs_token_t cmd,
                             kvs_token_t *tokens,
                             int argc,
                             DynBuf *wb)
{
    if (cmd.len == 10 && memcmp(cmd.ptr, "VECTOR.PUT", 10) == 0) {
        uint32_t dim = 0;
        if (argc < 4 || token_to_u32(tokens[2], &dim) != 0 ||
            dim == 0 || dim > VECTOR_MAX_DIM || argc != 3 + (int)dim) {
            dynbuf_lit(wb, "-ERR usage: VECTOR.PUT <id> <dim> <f1> ... <fdim>\r\n");
            return DISPATCH_ERROR;
        }
        float *vec = vector_alloc(dim);
        if (!vec) { dynbuf_lit(wb, "-ERR out of memory\r\n"); return DISPATCH_ERROR; }
        if (parse_vector_args(tokens, 3, dim, vec) != 0) {
            free(vec);
            dynbuf_lit(wb, "-ERR invalid vector value\r\n");
            return DISPATCH_ERROR;
        }
        int rc = vector_put(tokens[1], dim, vec);
        free(vec);
        if (rc != 0) { dynbuf_lit(wb, "-ERR out of memory\r\n"); return DISPATCH_ERROR; }
        dynbuf_lit(wb, "+OK\r\n");
        return DISPATCH_OK;
    }
    if (cmd.len == 10 && memcmp(cmd.ptr, "VECTOR.GET", 10) == 0) {
        if (argc != 2) {
            dynbuf_lit(wb, "-ERR usage: VECTOR.GET <id>\r\n");
            return DISPATCH_ERROR;
        }
        return vector_get_cmd(tokens[1], wb);
    }
    if (cmd.len == 10 && memcmp(cmd.ptr, "VECTOR.DEL", 10) == 0) {
        if (argc != 2) {
            dynbuf_lit(wb, "-ERR usage: VECTOR.DEL <id>\r\n");
            return DISPATCH_ERROR;
        }
        reply_integer_local(wb, vector_delete(tokens[1]));
        return DISPATCH_OK;
    }
    if (cmd.len == 10 && memcmp(cmd.ptr, "VECTOR.DIM", 10) == 0) {
        if (argc != 2) {
            dynbuf_lit(wb, "-ERR usage: VECTOR.DIM <id>\r\n");
            return DISPATCH_ERROR;
        }
        return vector_dim_cmd(tokens[1], wb);
    }
    if (cmd.len == 17 && memcmp(cmd.ptr, "VECTOR.SIMILARITY", 17) == 0) {
        vector_metric_t metric = VECTOR_METRIC_COSINE;
        if (argc != 3 && argc != 4) {
            dynbuf_lit(wb, "-ERR usage: VECTOR.SIMILARITY <id1> <id2> [L2|IP|COSINE]\r\n");
            return DISPATCH_ERROR;
        }
        if (argc == 4 && token_to_metric(tokens[3], &metric) != 0) {
            dynbuf_lit(wb, "-ERR invalid vector metric\r\n");
            return DISPATCH_ERROR;
        }
        return vector_similarity_cmd(tokens[1], tokens[2], metric, wb);
    }
    if (cmd.len == 12 && memcmp(cmd.ptr, "VECTOR.CLEAR", 12) == 0) {
        if (argc != 1) {
            dynbuf_lit(wb, "-ERR usage: VECTOR.CLEAR\r\n");
            return DISPATCH_ERROR;
        }
        vector_store_clear();
        dynbuf_lit(wb, "+OK\r\n");
        return DISPATCH_OK;
    }
    if (cmd.len == 13 && memcmp(cmd.ptr, "VECTOR.SEARCH", 13) == 0) {
        return vector_search_cmd(tokens, argc, wb);
    }
    if ((cmd.len == 11 && memcmp(cmd.ptr, "VECTOR.INFO", 11) == 0) ||
        (cmd.len == 12 && memcmp(cmd.ptr, "VECTOR.COUNT", 12) == 0)) {
        if (cmd.len == 12) {
            reply_integer_local(wb, (long long)vector_store_count());
            return DISPATCH_OK;
        }
        dynbuf_lit(wb, "*10\r\n");
        reply_bulk_local(wb, "vectors", 7);
        char nbuf[32];
        int nl = snprintf(nbuf, sizeof(nbuf), "%llu", (unsigned long long)vector_store_count());
        reply_bulk_local(wb, nbuf, nl);
        reply_bulk_local(wb, "shards", 6);
        nl = snprintf(nbuf, sizeof(nbuf), "%u", VECTOR_SHARDS);
        reply_bulk_local(wb, nbuf, nl);
        reply_bulk_local(wb, "max_dim", 7);
        nl = snprintf(nbuf, sizeof(nbuf), "%u", VECTOR_MAX_DIM);
        reply_bulk_local(wb, nbuf, nl);
        reply_bulk_local(wb, "max_topk", 8);
        nl = snprintf(nbuf, sizeof(nbuf), "%u", VECTOR_MAX_TOPK);
        reply_bulk_local(wb, nbuf, nl);
        reply_bulk_local(wb, "default_metric", 14);
        reply_bulk_local(wb, metric_name(VECTOR_METRIC_L2), 2);
        return DISPATCH_OK;
    }
    dynbuf_lit(wb, "-ERR unknown vector command\r\n");
    return DISPATCH_ERROR;
}
