/*=============================================================================
 *  kv_pool.c — kvstore 自研内存池实现
 *
 *  v0 最基本版。结构：
 *
 *    kv_pool_t
 *      ├─ cls[]          每档一个描述符（空闲链 + 当前切分超块 + 计数）
 *      ├─ slabs          全部超块链（destroy 遍历归还底座）
 *      ├─ cls_map[128]   请求尺寸 → 档位下标 的 O(1) 表（16B 桶）
 *      └─ 统计
 *
 *    超块（slab）= 一次底座 malloc(KV_POOL_SUPERBLOCK)，全部分给同一档位，
 *    内部按 obj_size（= 档位尺寸，16 倍数）连续切分（bump 分配）；档内释放
 *    的对象链在 class->freelist（LIFO），分配优先吃空闲链，空了才切新块。
 *
 *    为什么 free 携带尺寸、对象不带头（免费带尺寸）？
 *      - kv entry 的 key_len/val_len 在头部，删除/重建时总能原样算出当初的
 *        malloc 尺寸 → 集成无需改 kv_entry_t 布局，无需块头。
 *      - size 同时也用于「底座兜底」路由：> KV_POOL_CLASS_MAX 走底座。
 *        底座对象不带任何标记，free 靠同一个 size 判定，两侧天然一致。
 *
 *    档位表设计（对齐 16，升序，末位 == KV_POOL_CLASS_MAX）：
 *      前段密（步 16，覆盖 kv entry 常见小值域）、后段疏（步 32/64/128），
 *      碎片上界 ≈ 步长 - 1（典型 67B entry 取 80，浪费 ≤15B）。>2048 全走底座。
 *
 *    线程模型：本池假定单线程（reactor 主循环）。无锁无原子。
 *============================================================================*/

#include "kv_pool.h"

#include <stdlib.h>
#include <string.h>

/* 热路径统计计数开关（默认开）。
 * 开：alloc/free 每次更新 alloc_calls/free_calls/alloc_bytes/live_n/free_n/
 *     base_alloc/base_free —— 供 kv_pool_stats 观测（kv_dict_pool_test 的
 *     账本守恒断言依赖它）。
 * 关（编 kv_pool.c 时 -DKV_POOL_DISABLE_STATS）：上述计数全部不维护——
 *     基准矩阵里用来量化「计数器 RMW 是否占热路径开销」的探针形态；
 *     此形态下 kv_pool_stats 的计数类字段恒 0，仅 slab_count/slab_bytes
 *     （超块分配时才更新，非热路径）仍有效。
 * 默认开，关开关不改变 alloc/free 的语义与路由（仅省掉计数）。 */
#ifndef KV_POOL_DISABLE_STATS
#define KV_POOL_ENABLE_STATS 1
#else
#define KV_POOL_ENABLE_STATS 0
#endif

/*=============================================================================
 *  内部常量 / 档位表
 *============================================================================*/

#define KV_POOL_CLS_BASE   (-1)    /* 底座兜底哨兵：pool_class_of 返回值 */

/* 桶粒度 = KV_POOL_ALIGN。请求尺寸桶 = (size-1)>>4，最大桶 = MAX/16 - 1 */
#define KV_POOL_NMAP       (KV_POOL_CLASS_MAX / KV_POOL_ALIGN)

/**
 * 档位表（必须升序、16 倍数、末位 == KV_POOL_CLASS_MAX）
 * 请求尺寸向上取到「首个 ≥ 它的档位」。
 */
static const uint16_t k_class_sizes[] = {
    /* 密：32..256，步 16 —— kv entry 常见值域 */
    32,  48,  64,  80,  96,  112, 128,
    144, 160, 176, 192, 208, 224, 240, 256,
    /* 中：288..512，步 32 */
    288, 320, 352, 384, 416, 448, 480, 512,
    /* 中大：576..1024，步 64 */
    576, 640, 704, 768, 832, 896, 960, 1024,
    /* 大：1152..2048，步 128 */
    1152, 1280, 1408, 1536, 1664, 1792, 1920, 2048,
};

#define KV_POOL_NCLASS  (sizeof(k_class_sizes) / sizeof(k_class_sizes[0]))

_Static_assert(KV_POOL_NCLASS > 0, "class table empty");

/**
 * 档位表自检（建池时跑一次）：升序、16 倍数、首项 32、末项 == KV_POOL_CLASS_MAX
 * C11 静态断言取不到数组元素，故落运行期（create 非热路径，成本可忽略）。
 * @return 0 表合法，-1 表被改坏
 */
static int pool_table_ok(void) {
    if (k_class_sizes[0] != KV_POOL_ALIGN * 2) return -1;
    if (k_class_sizes[KV_POOL_NCLASS - 1] != KV_POOL_CLASS_MAX) return -1;
    for (uint32_t i = 0; i < KV_POOL_NCLASS; i++) {
        if (k_class_sizes[i] % KV_POOL_ALIGN != 0) return -1;
        if (i > 0 && k_class_sizes[i] <= k_class_sizes[i - 1]) return -1;
    }
    return 0;
}

/*=============================================================================
 *  内部结构
 *============================================================================*/

/** 超块：一次底座 malloc，专供某一档位切块 */
typedef struct kv_pool_slab {
    struct kv_pool_slab *next;      /* 池级链表（destroy 归还） */
    uint32_t             cls;       /* 档位下标（debug/校验） */
    uint32_t             obj_size;  /* 切块大小（= 档位尺寸，16 倍数） */
    uint32_t             cap;       /* 本超块可容纳对象数 */
    uint32_t             used;      /* 已切出对象数（0..cap，bump 游标） */
    char                *objs;      /* 对象区起点（对齐到 16） */
} kv_pool_slab_t;

/** 档位描述符 */
typedef struct {
    kv_pool_slab_t *cur;            /* 当前正在切分的超块（空则需新开） */
    void           *freelist;       /* 空闲对象链（LIFO；对象首 8B 存 next） */
    uint32_t        free_n;         /* 空闲链长度（观测） */
    uint32_t        live_n;         /* 存活对象数（已 alloc 未 free） */
} kv_pool_cls_t;

struct kv_pool {
    kv_pool_cls_t   cls[KV_POOL_NCLASS];          /* 每档固定描述符 */
    kv_pool_slab_t *slabs;                        /* 全部超块链 */
    uint8_t         cls_map[KV_POOL_NMAP];        /* 16B 桶 → 档位下标 */
    size_t          obj_hdr;                      /* 超块头部占用（16 对齐） */

    /* 统计 */
    uint64_t        alloc_calls, free_calls;
    uint64_t        alloc_bytes;
    uint64_t        base_alloc, base_free;
    uint64_t        slab_count, slab_bytes;
};

/*=============================================================================
 *  工具：尺寸 → 档位
 *============================================================================*/

/**
 * 请求尺寸 → 档位下标（升序首档 ≥ 尺寸），或 KV_POOL_CLS_BASE（底座兜底）
 * 单调且同尺寸必同档 → alloc/free 用同一函数路由，保证两侧一致。
 */
static inline int pool_class_of(const kv_pool_t *p, size_t size) {
    if (size > KV_POOL_CLASS_MAX) return KV_POOL_CLS_BASE;
    if (size < 1) size = 1;              /* 0 → 最小桶，落最小档 */
    return p->cls_map[(size - 1) >> 4];
}

/** 建 16B 桶 → 档位表：桶内最大尺寸取首个能装下的档位 */
static void pool_build_map(kv_pool_t *p) {
    for (uint32_t b = 0; b < KV_POOL_NMAP; b++) {
        uint32_t upper = (b + 1) * KV_POOL_ALIGN;   /* 桶内最大可能尺寸 */
        uint32_t i     = 0;
        while (k_class_sizes[i] < upper) i++;       /* 末档==MAX，必终止 */
        p->cls_map[b] = (uint8_t)i;
    }
}

/*=============================================================================
 *  超块
 *============================================================================*/

/** 新开一个超块（底座 malloc 一次），挂到池链，返回之；失败返回 NULL */
static kv_pool_slab_t *pool_slab_new(kv_pool_t *p, uint32_t cls) {
    uint32_t obj_size = k_class_sizes[cls];
    uint32_t cap = (uint32_t)((KV_POOL_SUPERBLOCK - p->obj_hdr) / obj_size);
    if (cap < 1) cap = 1;

    char *mem = (char *)malloc(KV_POOL_SUPERBLOCK);
    if (!mem) return NULL;

    kv_pool_slab_t *s = (kv_pool_slab_t *)mem;
    s->next     = p->slabs;
    s->cls      = cls;
    s->obj_size = obj_size;
    s->cap      = cap;
    s->used     = 0;
    s->objs     = mem + p->obj_hdr;     /* malloc 16B 对齐 + hdr 16 倍数 → 对齐 */

    p->slabs = s;
    p->slab_count++;
    p->slab_bytes += KV_POOL_SUPERBLOCK;
    return s;
}

/*=============================================================================
 *  生命周期
 *============================================================================*/

kv_pool_t *kv_pool_create(void) {
    if (pool_table_ok() < 0) return NULL;   /* 档位表被改坏则拒绝建池 */

    kv_pool_t *p = (kv_pool_t *)calloc(1, sizeof(kv_pool_t));
    if (!p) return NULL;

    p->obj_hdr = (sizeof(kv_pool_slab_t) + KV_POOL_ALIGN - 1)
                 & ~(size_t)(KV_POOL_ALIGN - 1);   /* 16 对齐超块头 */
    pool_build_map(p);
    return p;
}

/**
 * 整批归还超块（"全清归还"）。
 * 契约：池内无存活对象（dict_clear 全清后必然满足）——此时每块超块必空，
 * 直接 free 回底座，并把各档切分/空闲状态重置为空，下次 alloc 重新开块。
 */
void kv_pool_release_all(kv_pool_t *p) {
    if (!p) return;

    kv_pool_slab_t *s = p->slabs;
    while (s) {
        kv_pool_slab_t *next = s->next;
        free(s);
        s = next;
    }
    p->slabs      = NULL;
    p->slab_count = 0;
    p->slab_bytes = 0;

    for (uint32_t i = 0; i < KV_POOL_NCLASS; i++) {
        kv_pool_cls_t *c = &p->cls[i];
        c->cur      = NULL;   /* 空超块已全部归还，cur 悬指旧块 → 清空 */
        c->freelist = NULL;   /* 空闲对象在已归还的超块里 → 清空 */
        c->free_n   = 0;
        c->live_n   = 0;      /* 契约下已为 0，显式清零防漂移 */
    }
}

void kv_pool_destroy(kv_pool_t *p) {
    if (!p) return;
    kv_pool_release_all(p);   /* 归还全部超块 */
    free(p);
}

/*=============================================================================
 *  分配 / 释放
 *============================================================================*/

void *kv_pool_alloc(kv_pool_t *p, size_t size) {
    if (!p) return malloc(size);                    /* 容错：无池直走底座 */

#if KV_POOL_ENABLE_STATS
    p->alloc_calls++;
    p->alloc_bytes += size;
#endif

    int cls = pool_class_of(p, size);
    if (cls == KV_POOL_CLS_BASE) {                  /* 大对象：底座兜底 */
#if KV_POOL_ENABLE_STATS
        p->base_alloc++;
#endif
        return malloc(size);
    }

    kv_pool_cls_t *c = &p->cls[cls];
    void *ptr;

    if (c->freelist) {                              /* 优先吃档内空闲链 */
        ptr = c->freelist;
        c->freelist = *(void **)ptr;                /* 空闲对象首 8B 存 next */
#if KV_POOL_ENABLE_STATS
        c->free_n--;
#endif
    } else {
        kv_pool_slab_t *s = c->cur;
        if (!s || s->used >= s->cap) {              /* 空 / 切满：新开超块 */
            s = pool_slab_new(p, (uint32_t)cls);
            if (!s) return NULL;
            c->cur = s;
        }
        ptr = s->objs + (size_t)s->used * s->obj_size;
        s->used++;
    }

#if KV_POOL_ENABLE_STATS
    c->live_n++;
#endif
    return ptr;
}

void kv_pool_free(kv_pool_t *p, void *ptr, size_t size) {
    if (!ptr) return;                               /* no-op */
    if (!p) { free(ptr); return; }                  /* 容错：无池直走底座 */

#if KV_POOL_ENABLE_STATS
    p->free_calls++;
#endif

    int cls = pool_class_of(p, size);
    if (cls == KV_POOL_CLS_BASE) {                  /* 大对象：底座兜底 */
#if KV_POOL_ENABLE_STATS
        p->base_free++;
#endif
        free(ptr);
        return;
    }

    kv_pool_cls_t *c = &p->cls[cls];
    *(void **)ptr = c->freelist;                    /* 链头压入空闲对象 */
    c->freelist    = ptr;
#if KV_POOL_ENABLE_STATS
    c->free_n++;
    c->live_n--;
#endif
}

/*=============================================================================
 *  统计
 *============================================================================*/

void kv_pool_stats(const kv_pool_t *p, kv_pool_stats_t *out) {
    if (!out) return;
    memset(out, 0, sizeof(*out));
    if (!p) return;

    out->slab_count  = p->slab_count;
    out->slab_bytes  = p->slab_bytes;
    out->alloc_calls = p->alloc_calls;
    out->free_calls  = p->free_calls;
    out->alloc_bytes = p->alloc_bytes;
    out->base_alloc  = p->base_alloc;
    out->base_free   = p->base_free;

    for (uint32_t i = 0; i < KV_POOL_NCLASS; i++) {
        out->live_objects += p->cls[i].live_n;
        out->free_objects += p->cls[i].free_n;
    }
}
