#include "flat_index.h"
#include "vector_math.h"

#include <stdlib.h>
#include <string.h>

int vec_result_init(vec_search_result_t *r, uint32_t topk, vec_metric_t metric)
{
    if (!r || topk == 0 || topk > VEC_MAX_TOPK) return -1;
    memset(r, 0, sizeof(*r));
    r->items = (vec_search_hit_t *)calloc(topk, sizeof(*r->items));
    if (!r->items) return -1;
    r->cap = topk;
    r->metric = metric;
    return 0;
}

void vec_result_free(vec_search_result_t *r)
{
    if (!r) return;
    free(r->items);
    memset(r, 0, sizeof(*r));
}

int vec_result_consider(vec_search_result_t *r,
                        uint64_t segment_id,
                        uint32_t row_ord,
                        const char *id,
                        uint32_t id_len,
                        float score)
{
    if (!r || !r->items || r->cap == 0) return -1;
    if (r->len < r->cap) {
        vec_search_hit_t *h = &r->items[r->len++];
        h->segment_id = segment_id;
        h->row_ord = row_ord;
        h->id = id;
        h->id_len = id_len;
        h->score = score;
        return 0;
    }
    uint32_t worst = 0;
    for (uint32_t i = 1; i < r->len; i++) {
        if (vec_score_better(r->items[worst].score, r->items[i].score, r->metric))
            worst = i;
    }
    if (!vec_score_better(score, r->items[worst].score, r->metric)) return 0;
    r->items[worst].segment_id = segment_id;
    r->items[worst].row_ord = row_ord;
    r->items[worst].id = id;
    r->items[worst].id_len = id_len;
    r->items[worst].score = score;
    return 0;
}

void vec_result_sort(vec_search_result_t *r)
{
    if (!r || !r->items) return;
    for (uint32_t i = 1; i < r->len; i++) {
        vec_search_hit_t v = r->items[i];
        uint32_t j = i;
        while (j > 0 && vec_score_better(v.score, r->items[j - 1].score, r->metric)) {
            r->items[j] = r->items[j - 1];
            j--;
        }
        r->items[j] = v;
    }
}
