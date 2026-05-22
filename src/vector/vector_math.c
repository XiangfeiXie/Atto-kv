#include "vector_math.h"

#include <math.h>

#if defined(__AVX2__)
#include <immintrin.h>
#endif

float vec_inner_product(const float *a, const float *b, uint32_t dim)
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

float vec_l2_distance(const float *a, const float *b, uint32_t dim)
{
#if defined(__AVX2__)
    __m256 acc = _mm256_setzero_ps();
    uint32_t i = 0;
    for (; i + 8 <= dim; i += 8) {
        __m256 va = _mm256_loadu_ps(a + i);
        __m256 vb = _mm256_loadu_ps(b + i);
        __m256 d = _mm256_sub_ps(a + i, b + i);
        (void)va; (void)vb;
        acc = _mm256_add_ps(acc, _mm256_mul_ps(d, d));
    }
    float tmp[8];
    _mm256_storeu_ps(tmp, acc);
    float sum = tmp[0] + tmp[1] + tmp[2] + tmp[3] + tmp[4] + tmp[5] + tmp[6] + tmp[7];
    for (; i < dim; i++) {
        float d = a[i] - b[i];
        sum += d * d;
    }
    return sum;
#else
    float sum = 0.0f;
    for (uint32_t i = 0; i < dim; i++) {
        float d = a[i] - b[i];
        sum += d * d;
    }
    return sum;
#endif
}

float vec_cosine_similarity(const float *a, const float *b, uint32_t dim)
{
    float an = vec_inner_product(a, a, dim);
    float bn = vec_inner_product(b, b, dim);
    if (an <= 0.0f || bn <= 0.0f) return -1.0f;
    return vec_inner_product(a, b, dim) / sqrtf(an * bn);
}

float vec_score(const float *a, const float *b, uint32_t dim, vec_metric_t metric)
{
    if (metric == VEC_METRIC_IP) return vec_inner_product(a, b, dim);
    if (metric == VEC_METRIC_COSINE) return vec_cosine_similarity(a, b, dim);
    return vec_l2_distance(a, b, dim);
}

int vec_score_better(float a, float b, vec_metric_t metric)
{
    return metric == VEC_METRIC_L2 ? a < b : a > b;
}
