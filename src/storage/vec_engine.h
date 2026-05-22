#ifndef KVS_VEC_ENGINE_H
#define KVS_VEC_ENGINE_H

#include "vec_collection.h"

int vec_engine_init(void);
void vec_engine_shutdown(void);
void vec_engine_clear(void);

int vec_engine_create_collection(const char *name,
                                 uint32_t name_len,
                                 uint32_t dim,
                                 vec_metric_t metric,
                                 vec_index_type_t index_type);
int vec_engine_drop_collection(const char *name, uint32_t name_len);
vec_collection_t *vec_engine_get_collection(const char *name, uint32_t name_len);

int vec_engine_put(const char *collection,
                   uint32_t collection_len,
                   const char *id,
                   uint32_t id_len,
                   const float *vec,
                   uint32_t dim,
                   vec_metric_t default_metric);
int vec_engine_delete(const char *collection,
                      uint32_t collection_len,
                      const char *id,
                      uint32_t id_len);
int vec_engine_get(const char *collection,
                   uint32_t collection_len,
                   const char *id,
                   uint32_t id_len,
                   float *out,
                   uint32_t *dim_out);
int vec_engine_search(const char *collection,
                      uint32_t collection_len,
                      const float *query,
                      uint32_t dim,
                      uint32_t topk,
                      vec_search_result_t *out);
uint64_t vec_engine_count_collection(const char *collection, uint32_t collection_len);
uint64_t vec_engine_total_count(void);

const char *vec_metric_name(vec_metric_t metric);
int vec_metric_parse(const char *s, uint32_t len, vec_metric_t *out);

#endif
