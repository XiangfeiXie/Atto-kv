#ifndef KVS_VEC_TYPES_H
#define KVS_VEC_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include <stdatomic.h>

#define VEC_DEFAULT_COLLECTION "_default"
#define VEC_MAX_COLLECTION_NAME 128u
#define VEC_MAX_DIM 4096u
#define VEC_MAX_TOPK 1024u
#define VEC_MUTABLE_INITIAL_CAP 1024u

typedef enum {
    VEC_METRIC_L2 = 0,
    VEC_METRIC_IP = 1,
    VEC_METRIC_COSINE = 2,
} vec_metric_t;

typedef enum {
    VEC_INDEX_FLAT = 0,
    VEC_INDEX_HNSW = 1,
} vec_index_type_t;

typedef struct {
    uint64_t segment_id;
    uint32_t row_ord;
    float score;
    const char *id;
    uint32_t id_len;
} vec_search_hit_t;

typedef struct {
    vec_search_hit_t *items;
    uint32_t len;
    uint32_t cap;
    vec_metric_t metric;
} vec_search_result_t;

typedef struct vec_mutable_segment vec_mutable_segment_t;
typedef struct vec_sealed_segment vec_sealed_segment_t;
typedef struct vec_collection vec_collection_t;

#endif
