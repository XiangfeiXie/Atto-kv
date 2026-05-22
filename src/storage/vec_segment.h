#ifndef KVS_VEC_SEGMENT_H
#define KVS_VEC_SEGMENT_H

#include "vec_types.h"

vec_mutable_segment_t *vec_mutable_create(uint64_t collection_id,
                                          uint64_t segment_id,
                                          uint32_t dim,
                                          uint64_t capacity);
void vec_mutable_destroy(vec_mutable_segment_t *s);
uint64_t vec_mutable_count(vec_mutable_segment_t *s);
int vec_mutable_put(vec_mutable_segment_t *s,
                    const char *id,
                    uint32_t id_len,
                    const float *vec,
                    uint64_t lsn);
int vec_mutable_delete(vec_mutable_segment_t *s,
                       const char *id,
                       uint32_t id_len,
                       uint64_t lsn);
int vec_mutable_get(vec_mutable_segment_t *s,
                    const char *id,
                    uint32_t id_len,
                    float *out);
int vec_mutable_search(vec_mutable_segment_t *s,
                       const float *query,
                       uint32_t topk,
                       vec_metric_t metric,
                       vec_search_result_t *out);

#endif
