#ifndef KVS_VECTOR_MATH_H
#define KVS_VECTOR_MATH_H

#include "vec_types.h"

float vec_l2_distance(const float *a, const float *b, uint32_t dim);
float vec_inner_product(const float *a, const float *b, uint32_t dim);
float vec_cosine_similarity(const float *a, const float *b, uint32_t dim);
float vec_score(const float *a, const float *b, uint32_t dim, vec_metric_t metric);
int   vec_score_better(float a, float b, vec_metric_t metric);

#endif
