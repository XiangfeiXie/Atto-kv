#ifndef KVS_VEC_INDEX_H
#define KVS_VEC_INDEX_H

#include "vec_types.h"

typedef struct vector_index_ops {
    int  (*build)(void *idx, const float *vectors, uint64_t n, uint32_t dim);
    int  (*search)(void *idx, const float *query, uint32_t topk, vec_search_result_t *out);
    void (*destroy)(void *idx);
} vector_index_ops_t;

typedef struct vector_index {
    vec_index_type_t type;
    vec_metric_t metric;
    uint32_t dim;
    const vector_index_ops_t *ops;
    void *impl;
} vector_index_t;

#endif
