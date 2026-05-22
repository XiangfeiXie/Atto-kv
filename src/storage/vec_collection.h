#ifndef KVS_VEC_COLLECTION_H
#define KVS_VEC_COLLECTION_H

#include "vec_types.h"
#include "vec_segment.h"

vec_collection_t *vec_collection_create(uint64_t collection_id,
                                        const char *name,
                                        uint32_t name_len,
                                        uint32_t dim,
                                        vec_metric_t metric,
                                        vec_index_type_t index_type);
void vec_collection_destroy(vec_collection_t *c);
int vec_collection_put(vec_collection_t *c, const char *id, uint32_t id_len,
                       const float *vec, uint64_t lsn);
int vec_collection_delete(vec_collection_t *c, const char *id, uint32_t id_len,
                          uint64_t lsn);
int vec_collection_get(vec_collection_t *c, const char *id, uint32_t id_len,
                       float *out);
int vec_collection_search(vec_collection_t *c, const float *query, uint32_t topk,
                          vec_search_result_t *out);
uint64_t vec_collection_count(vec_collection_t *c);
uint32_t vec_collection_dim(vec_collection_t *c);
vec_metric_t vec_collection_metric(vec_collection_t *c);
const char *vec_collection_name(vec_collection_t *c);

#endif
