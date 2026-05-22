#ifndef KVS_FLAT_INDEX_H
#define KVS_FLAT_INDEX_H

#include "vec_types.h"

int vec_result_init(vec_search_result_t *r, uint32_t topk, vec_metric_t metric);
void vec_result_free(vec_search_result_t *r);
int vec_result_consider(vec_search_result_t *r,
                        uint64_t segment_id,
                        uint32_t row_ord,
                        const char *id,
                        uint32_t id_len,
                        float score);
void vec_result_sort(vec_search_result_t *r);

#endif
