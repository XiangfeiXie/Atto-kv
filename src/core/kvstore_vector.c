/*=============================================================================
 *  kvstore_vector.c — 向量命令：VSET / VGET / VDEL / VSEARCH
 *
 *  向量不是新引擎、也不是新节点类型：它就是一条 payload 带 16B 头的普通 KV
 *  记录。于是 dict_* / entry_* / TTL / RDB / AOF / 复制全部沿用现有路径。
 *
 *  payload 布局（全部按本机字节序；项目跑在 x86_64 小端，RDB 也是直接
 *  memcpy 原生整数/浮点）：
 *    [0..3]   "AIV1"
 *    [4]      version 1
 *    [5]      dtype 0 (float32)
 *    [6]      metric 0 (cosine)
 *    [7]      flags 0
 *    [8..9]   dim
 *    [10..11] reserved
 *    [12..15] meta_len
 *    [16..]   meta
 *    [..]     dim 个 float32，已 L2 归一化
 *
 *  边界（本阶段）：
 *    - 没有 ANN 索引，VSEARCH 每次全表暴力扫描 O(N * dim)；
 *    - 没有元数据过滤 / 多集合 / 批量命令；
 *    - 单线程，VSEARCH 扫描期间阻塞其它命令。
 *
 *  内存：payload 最坏 16 + 64KB + 4096*4 ≈ 80KB，VGET/VSEARCH 的查询向量
 *  16KB。NtyCo 协程默认栈只有 128KB，所以用文件级 static 数组，不放栈。
 *  数据面单线程，不需要锁。
 *============================================================================*/

#include "kvstore_vector.h"
#include "kvstore_internal.h"   /* token_to_ll / reply_integer / reply_bulk */

#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* 单条 VSET payload 的最大长度（头 + 最大 meta + 最大向量） */
#define KV_VEC_MAX_PAYLOAD \
    (KV_VEC_HDR_LEN + KV_VEC_MAX_META + KV_VEC_MAX_DIM * 4u)

static char  g_vec_payload[KV_VEC_MAX_PAYLOAD];
static float g_vec_query[KV_VEC_MAX_DIM];

/*=============================================================================
 *  按字节读写
 *
 *  payload 里向量起点是 16 + meta_len，key 又在节点头之后，都不保证 4 字节
 *  对齐；统一用 memcpy，既避免 (float *) 强转的未定义行为，也不依赖编译器
 *  对非对齐访问的容忍。
 *============================================================================*/

static inline uint16_t rd_u16(const char *p) {
    uint16_t v; memcpy(&v, p, sizeof v); return v;
}
static inline void wr_u16(char *p, uint16_t v) {
    memcpy(p, &v, sizeof v);
}
static inline uint32_t rd_u32(const char *p) {
    uint32_t v; memcpy(&v, p, sizeof v); return v;
}
static inline void wr_u32(char *p, uint32_t v) {
    memcpy(p, &v, sizeof v);
}
static inline float rd_f32(const char *p) {
    float f; memcpy(&f, p, sizeof f); return f;
}
static inline void wr_f32(char *p, float f) {
    memcpy(p, &f, sizeof f);
}

/*=============================================================================
 *  payload 头
 *============================================================================*/

typedef struct {
    uint16_t    dim;
    const char *meta;
    uint32_t    meta_len;
    const char *vec;
} vec_view_t;

/** 解析一条 payload；0 = 合法向量，-1 = 非向量/损坏 */
static int vec_payload_parse(const char *p, uint32_t len, vec_view_t *v) {
    if (len < KV_VEC_HDR_LEN) return -1;
    if (memcmp(p, "AIV1", 4) != 0) return -1;
    if ((uint8_t)p[4] != KV_VEC_VERSION)   return -1;
    if ((uint8_t)p[5] != KV_VEC_DTYPE_F32) return -1;
    if ((uint8_t)p[6] != KV_VEC_METRIC_COS) return -1;

    uint16_t dim = rd_u16(p + 8);
    uint32_t mlen = rd_u32(p + 12);
    if (dim == 0 || dim > KV_VEC_MAX_DIM) return -1;
    if (mlen > KV_VEC_MAX_META) return -1;

    uint64_t need = (uint64_t)KV_VEC_HDR_LEN + mlen + (uint64_t)dim * 4u;
    if (need != (uint64_t)len) return -1;

    v->dim      = dim;
    v->meta     = p + KV_VEC_HDR_LEN;
    v->meta_len = mlen;
    v->vec      = p + KV_VEC_HDR_LEN + mlen;
    return 0;
}

static void vec_hdr_write(char *p, uint16_t dim, uint32_t meta_len) {
    memcpy(p, "AIV1", 4);
    p[4] = (char)KV_VEC_VERSION;
    p[5] = (char)KV_VEC_DTYPE_F32;
    p[6] = (char)KV_VEC_METRIC_COS;
    p[7] = 0;
    wr_u16(p + 8, dim);
    wr_u16(p + 10, 0);
    wr_u32(p + 12, meta_len);
}

/*=============================================================================
 *  归一化 / 点积
 *============================================================================*/

/** 校验 float 全部有限，L2 归一化到 dst；零向量/NaN/Inf 返回 -1 */
static int vec_normalize(const char *blob, uint32_t dim, float *dst) {
    double sum = 0.0;
    for (uint32_t i = 0; i < dim; i++) {
        float f = rd_f32(blob + (size_t)i * 4);
        if (!isfinite(f)) return -1;
        sum += (double)f * (double)f;
    }
    double norm = sqrt(sum);
    if (!isfinite(norm) || norm <= 0.0) return -1;

    for (uint32_t i = 0; i < dim; i++) {
        float f = rd_f32(blob + (size_t)i * 4);
        dst[i] = (float)((double)f / norm);
    }
    return 0;
}

/** 存储向量（可能非对齐）与已归一化查询向量的点积 */
static double vec_dot(const char *stored, const float *q, uint32_t dim) {
    double sum = 0.0;
    for (uint32_t i = 0; i < dim; i++)
        sum += (double)rd_f32(stored + (size_t)i * 4) * (double)q[i];
    return sum;
}

/*=============================================================================
 *  回复 / 错误小工具
 *============================================================================*/

static void reply_array_len(DynBuf *wb, int n) {
    char b[24];
    int  l = snprintf(b, sizeof b, "*%d\r\n", n);
    if (l > 0) dynbuf_cat(wb, b, l);
}

static dispatch_result_t vec_err(DynBuf *wb, const char *msg) {
    dynbuf_lit(wb, "-ERR ");
    dynbuf_cat(wb, msg, (int)strlen(msg));
    dynbuf_lit(wb, "\r\n");
    return DISPATCH_ERROR;
}

/*=============================================================================
 *  VSET <key> <dim> <blob> [META <meta>] [EX <seconds>]
 *============================================================================*/

dispatch_result_t vec_cmd_vset(kvs_token_t *tokens, int argc,
                               DynBuf *wb, int *journaled) {
    if (argc != 4 && argc != 6 && argc != 8)
        return vec_err(wb, "wrong number of arguments for 'vset' command");

    long long dim_ll;
    if (token_to_ll(tokens[2], &dim_ll) < 0 ||
        dim_ll <= 0 || dim_ll > KV_VEC_MAX_DIM)
        return vec_err(wb, "invalid vector dim");
    uint16_t dim = (uint16_t)dim_ll;

    kvs_token_t blob = tokens[3];
    if (blob.len != (int)(dim * 4u))
        return vec_err(wb, "vector length mismatch");

    const char *meta     = NULL;
    uint32_t    meta_len = 0;
    long long   ttl_secs = 0;

    for (int i = 4; i < argc; i += 2) {
        if (TOKEN_EQ(tokens[i], "META")) {
            meta = tokens[i + 1].ptr;
            meta_len = (uint32_t)tokens[i + 1].len;
            if (meta_len > KV_VEC_MAX_META)
                return vec_err(wb, "meta too large");
        } else if (TOKEN_EQ(tokens[i], "EX")) {
            if (token_to_ll(tokens[i + 1], &ttl_secs) < 0 || ttl_secs <= 0)
                return vec_err(wb, "invalid expire time");
        } else {
            return vec_err(wb, "syntax error in 'vset' command");
        }
    }

    /* 先归一化，失败时引擎一个字节都没动 */
    if (vec_normalize(blob.ptr, dim, g_vec_query) < 0)
        return vec_err(wb, "invalid vector value");

    uint32_t payload_len = (uint32_t)KV_VEC_HDR_LEN + meta_len + (uint32_t)dim * 4u;
    vec_hdr_write(g_vec_payload, dim, meta_len);
    if (meta_len) memcpy(g_vec_payload + KV_VEC_HDR_LEN, meta, meta_len);
    memcpy(g_vec_payload + KV_VEC_HDR_LEN + meta_len, g_vec_query,
           (size_t)dim * 4u);

    /* 与 SET 同一套 upsert：已存在时 add_or_find 不写 payload，自己补 */
    int exists = 0;
    kv_node_t *e = dict_add_or_find(tokens[1].ptr, (uint32_t)tokens[1].len,
                                    g_vec_payload, payload_len, &exists);
    if (!e) return vec_err(wb, "out of memory");

    if (exists) {
        if (entry_payload_len(e) != payload_len) {
            e = dict_resize_entry(e, payload_len);
            if (!e) return vec_err(wb, "out of memory");
        }
        memcpy(entry_payload(e), g_vec_payload, payload_len);
    }
    entry_set_expire(e, ttl_secs ? get_time_now() + ttl_secs * 1000 : 0);

    dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
    *journaled = 1;
    return DISPATCH_OK;
}

/*=============================================================================
 *  VGET <key> — [dim, 归一化 vector_blob, meta]
 *============================================================================*/

dispatch_result_t vec_cmd_vget(kvs_token_t *tokens, int argc,
                               DynBuf *wb, int *journaled) {
    (void)journaled;
    if (argc != 2)
        return vec_err(wb, "wrong number of arguments for 'vget' command");

    kv_node_t *e = dict_find(tokens[1].ptr, (uint32_t)tokens[1].len);
    if (!e) {
        dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);
        return DISPATCH_OK;
    }

    int64_t exp = entry_expire(e);
    if (exp && exp <= get_time_now()) {
        dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);
        return DISPATCH_OK;
    }

    vec_view_t v;
    if (vec_payload_parse(entry_payload(e), entry_payload_len(e), &v) < 0) {
        dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);
        return DISPATCH_OK;
    }

    reply_array_len(wb, 3);
    reply_integer(wb, (long long)v.dim);
    reply_bulk(wb, v.vec, (int)v.dim * 4);
    reply_bulk(wb, v.meta, (int)v.meta_len);
    return DISPATCH_OK;
}

/*=============================================================================
 *  VDEL <key> — 只删向量键
 *============================================================================*/

dispatch_result_t vec_cmd_vdel(kvs_token_t *tokens, int argc,
                               DynBuf *wb, int *journaled) {
    if (argc != 2)
        return vec_err(wb, "wrong number of arguments for 'vdel' command");

    kv_node_t *e = dict_find(tokens[1].ptr, (uint32_t)tokens[1].len);
    if (!e) {
        reply_integer(wb, 0);
        return DISPATCH_OK;
    }

    int64_t exp = entry_expire(e);
    if (exp && exp <= get_time_now()) {
        reply_integer(wb, 0);
        return DISPATCH_OK;
    }

    vec_view_t v;
    if (vec_payload_parse(entry_payload(e), entry_payload_len(e), &v) < 0) {
        reply_integer(wb, 0);
        return DISPATCH_OK;
    }

    const char *kp = entry_key(e);
    uint32_t    kl = entry_key_len(e);
    if (dict_delete(kp, kl) == 0) {
        reply_integer(wb, 1);
        *journaled = 1;
    } else {
        reply_integer(wb, 0);
    }
    return DISPATCH_OK;
}

/*=============================================================================
 *  VSEARCH <topk> <dim> <blob> [MINSIM <score>]
 *============================================================================*/

typedef struct {
    double      score;
    const char *key;
    uint32_t    klen;
} vec_hit_t;

/** 输出排序：分数降序（同分不保证顺序） */
static int vec_hit_cmp(const void *pa, const void *pb) {
    double a = ((const vec_hit_t *)pa)->score;
    double b = ((const vec_hit_t *)pb)->score;
    return (a < b) - (a > b);
}

/** 堆顶是分数最小的一条 */
static int vec_hit_worse(const vec_hit_t *a, const vec_hit_t *b) {
    return a->score < b->score;
}

static void vec_heap_sift_up(vec_hit_t *h, int i) {
    while (i > 0) {
        int p = (i - 1) / 2;
        if (!vec_hit_worse(&h[i], &h[p])) break;
        vec_hit_t t = h[i]; h[i] = h[p]; h[p] = t;
        i = p;
    }
}

static void vec_heap_sift_down(vec_hit_t *h, int n, int i) {
    for (;;) {
        int l = 2 * i + 1, r = l + 1, w = i;
        if (l < n && vec_hit_worse(&h[l], &h[w])) w = l;
        if (r < n && vec_hit_worse(&h[r], &h[w])) w = r;
        if (w == i) break;
        vec_hit_t t = h[i]; h[i] = h[w]; h[w] = t;
        i = w;
    }
}

static int token_to_double(kvs_token_t t, double *out) {
    char buf[64];
    if (t.len <= 0 || t.len >= (int)sizeof buf) return -1;
    memcpy(buf, t.ptr, (size_t)t.len);
    buf[t.len] = '\0';

    errno = 0;
    char *end = NULL;
    double v = strtod(buf, &end);
    if (errno == ERANGE || end != buf + t.len || !isfinite(v))
        return -1;
    *out = v;
    return 0;
}

dispatch_result_t vec_cmd_vsearch(kvs_token_t *tokens, int argc,
                                  DynBuf *wb, int *journaled) {
    (void)journaled;
    if (argc != 4 && argc != 6)
        return vec_err(wb, "wrong number of arguments for 'vsearch' command");

    long long topk_ll, dim_ll;
    if (token_to_ll(tokens[1], &topk_ll) < 0 ||
        topk_ll <= 0 || topk_ll > KV_VEC_MAX_TOPK)
        return vec_err(wb, "invalid topk");
    if (token_to_ll(tokens[2], &dim_ll) < 0 ||
        dim_ll <= 0 || dim_ll > KV_VEC_MAX_DIM)
        return vec_err(wb, "invalid vector dim");

    int      topk = (int)topk_ll;
    uint16_t dim  = (uint16_t)dim_ll;
    kvs_token_t blob = tokens[3];
    if (blob.len != (int)(dim * 4u))
        return vec_err(wb, "vector length mismatch");

    double minsim = -1.0;
    if (argc == 6) {
        if (!TOKEN_EQ(tokens[4], "MINSIM"))
            return vec_err(wb, "syntax error in 'vsearch' command");
        if (token_to_double(tokens[5], &minsim) < 0)
            return vec_err(wb, "invalid min score");
    }

    if (vec_normalize(blob.ptr, dim, g_vec_query) < 0)
        return vec_err(wb, "invalid vector value");

    vec_hit_t *heap = (vec_hit_t *)malloc(sizeof(vec_hit_t) * (size_t)topk);
    if (!heap) return vec_err(wb, "out of memory");

    kv_iter_t *it = dict_iter_new();
    if (!it) {
        free(heap);
        return vec_err(wb, "out of memory");
    }

    int       n   = 0;
    int64_t   now = get_time_now();
    vec_view_t v;

    for (kv_node_t *e; (e = dict_iter_next(it)) != NULL; ) {
        int64_t exp = entry_expire(e);
        if (exp && exp <= now) continue;

        if (vec_payload_parse(entry_payload(e), entry_payload_len(e), &v) < 0)
            continue;
        if (v.dim != dim) continue;

        double score = vec_dot(v.vec, g_vec_query, dim);
        if (!isfinite(score)) continue;
        if (score < minsim) continue;

        vec_hit_t cand;
        cand.score = score;
        cand.key   = entry_key(e);
        cand.klen  = entry_key_len(e);

        if (n < topk) {
            heap[n] = cand;
            vec_heap_sift_up(heap, n);
            n++;
        } else if (cand.score > heap[0].score) {
            heap[0] = cand;
            vec_heap_sift_down(heap, n, 0);
        }
    }
    dict_iter_free(it);

    qsort(heap, (size_t)n, sizeof(vec_hit_t), vec_hit_cmp);

    reply_array_len(wb, n);
    for (int i = 0; i < n; i++) {
        char sbuf[32];
        int  sl = snprintf(sbuf, sizeof sbuf, "%.6f", heap[i].score);
        reply_array_len(wb, 2);
        reply_bulk(wb, heap[i].key, (int)heap[i].klen);
        reply_bulk(wb, sbuf, sl);
    }

    free(heap);
    return DISPATCH_OK;
}
