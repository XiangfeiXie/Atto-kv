/*=============================================================================
 *  kv_hash.c — 哈希引擎
 *
 *  功能：桶数组 + 桶内单向链表
 *  设计特点：
 *  1. 单表设计，无渐进 rehash（适合单线程场景）
 *  2. 负载因子 >= 1 时一次性扩容（2 倍）
 *  3. 节点内存布局：头部 + key + payload 连续存储，一次整块分配
 *  4. 支持随机采样（用于过期键淘汰）
 *  5. 节点尺寸变化一律重建（缩小/放大统一，不做原地 realloc）
 *
 *  节点是本引擎私有类型，对上层只是一个 void *（见 kv_hash.h）。
 *  哈希引擎是四个里唯一把「链指针」内联进节点的 —— 索引结构不额外占内存，
 *  代价是节点比别的引擎多 8B。
 *
 *  哈希函数也封在本文件里（key_hash）：只有本引擎的桶定位要用它，外面不需要
 *  算、也拿不到。于是「一次增删改查只算一次哈希」不再靠调用方自觉传参，
 *  而是结构上就没有第二次可算。
 *============================================================================*/

#include "kv_hash.h"
#include "kv_key.h"
#include "kv_pool.h"
#include <stdlib.h>
#include <string.h>

/* 桶数组容量（必须是 2 的幂，取模靠掩码） */
#define HASH_INIT_SIZE  16          /* 初始桶数 */
#define HASH_MAX_SIZE   (1U << 24)  /* 桶数上限 ~16M */

/*=============================================================================
 *  哈希函数（FNV-1a + Murmur3 finalizer）
 *
 *  - 8 字节块处理，充分利用 CPU 位宽
 *  - 最终混合（avalanche）提升分布均匀性
 *  - 结果缓存进节点（hash 字段）：查找与扩容重链都要用，缓存避免重算
 *============================================================================*/

static uint64_t key_hash(const char *key, uint32_t len) {
    uint64_t h = 14695981039346656037ULL;  /* FNV-1a offset basis */
    uint32_t i = 0;

    for (; i + 8 <= len; i += 8) {
        uint64_t word;
        memcpy(&word, key + i, 8);
        h ^= word;
        h *= 1099511628211ULL;             /* FNV prime */
    }
    for (; i < len; i++) {
        h ^= key[i];
        h *= 1099511628211ULL;
    }

    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return h;
}

/*=============================================================================
 *  节点
 *
 *  内存布局（一次整块分配）：
 *  +----------------+------------------+------------------+
 *  | hash_node_t 头 | key 数据 (N 字节) | payload 数据 (M 字节) |
 *  +----------------+------------------+------------------+
 *
 *  key 恒在 data[] 头部、永不移动；payload 紧跟其后。
 *  块尺寸恒 == sizeof(hash_node_t) + key_len + val_len，node_free 据此还原
 *  分配尺寸（kv_pool「free 携带尺寸」契约）。
 *============================================================================*/

typedef struct hash_node_s {
    struct hash_node_s *next;    /* 桶链指针 */
    uint64_t            hash;    /* 缓存的哈希值：查找与 rehash 都要用，必须缓存 */
    int64_t             expire;  /* 过期时间戳（毫秒，0=永不过期）；引擎自己不读 */
    uint32_t            key_len;
    uint32_t            val_len;
    char                data[];  /* 柔性数组：[key][payload] */
} hash_node_t;

/**
 * 桶数组
 * 字段说明：
 *  - buckets：桶数组指针（每格是桶内链表头）
 *  - size：桶数量（必须是 2 的幂）
 *  - size_mask：size - 1（用于快速取模）
 *  - used：当前记录数
 */
typedef struct {
    hash_node_t **buckets;
    uint32_t      size;
    uint32_t      size_mask;
    uint32_t      used;
} hash_store_t;

static hash_store_t g_hash;   /* 哈希引擎的单例表（单线程，无分片） */

/*=============================================================================
 *  节点分配（本引擎自己管内存）
 *============================================================================*/

/**
 * 分配并填好一个节点
 *
 * 尺寸约定：total = sizeof(hash_node_t) + key_len + payload_len。
 * 必须与 node_free 里从头部推导出来的值一致 —— 见 kv_pool.h 的池契约。
 */
static hash_node_t *node_new(const char *key, uint32_t key_len,
                             const char *payload, uint32_t payload_len,
                             uint64_t hash)
{
    uint32_t total = (uint32_t)sizeof(hash_node_t) + key_len + payload_len;
    hash_node_t *e = (hash_node_t *)kv_pool_alloc((size_t)total);
    if (unlikely(!e)) return NULL;

    e->next    = NULL;
    e->hash    = hash;
    e->expire  = 0;
    e->key_len = key_len;
    e->val_len = payload_len;

    /* 复制 key 和 payload 到连续内存 */
    if (key && key_len)
        memcpy(e->data, key, key_len);
    if (payload && payload_len)
        memcpy(e->data + key_len, payload, payload_len);

    return e;
}

/** 释放节点：尺寸从头部推导（kv_pool「free 携带尺寸」契约） */
static void node_free(hash_node_t *e) {
    uint32_t total = (uint32_t)sizeof(hash_node_t) + e->key_len + e->val_len;
    kv_pool_free(e, (size_t)total);
}

/*=============================================================================
 *  字段访问器（上层唯一的读/写途径）
 *============================================================================*/

const char *hash_key(void *n)     { return ((hash_node_t *)n)->data; }
uint32_t    hash_key_len(void *n) { return ((hash_node_t *)n)->key_len; }
char       *hash_val(void *n) {
    hash_node_t *e = (hash_node_t *)n;
    return e->data + e->key_len;
}
uint32_t hash_val_len(void *n) { return ((hash_node_t *)n)->val_len; }
int64_t  hash_expire(void *n)  { return ((hash_node_t *)n)->expire; }
void     hash_set_expire(void *n, int64_t v) { ((hash_node_t *)n)->expire = v; }

/*=============================================================================
 *  表分配 / 生命周期管理
 *============================================================================*/

/**
 * 分配桶数组
 * @param size 桶数量（2 的幂）
 * @return 0 成功，-1 失败
 */
static int table_alloc(uint32_t size) {
    g_hash.size      = size;
    g_hash.size_mask = size - 1;   /* 用于快速取模（hash & mask） */
    g_hash.used      = 0;
    g_hash.buckets   = (hash_node_t **)calloc((size_t)size, sizeof(hash_node_t *));
    return g_hash.buckets ? 0 : -1;
}

void hash_init(void) {
    g_hash.buckets   = NULL;
    g_hash.size      = 0;
    g_hash.size_mask = 0;
    g_hash.used      = 0;
}

/**
 * 全清：删除所有记录并归还桶数组
 *
 * 记录全释放后若注入池则整批归还空超块给底座（kv_pool_release_all）
 * ——被清掉的记录内存真正还回底座，后续 jemalloc purge 才能把页还给 OS
 * （对 FLUSHALL / RDB 重载后的 RSS 回落必需）。全清后无存活记录，恰是
 * release_all 的契约前提；下次插入由池重新开块，容量测量各档独立不串高水位。
 */
void hash_clear(void) {
    if (!g_hash.buckets) return;

    /* 遍历所有桶，释放每条记录 */
    for (uint32_t i = 0; i < g_hash.size; i++) {
        hash_node_t *e = g_hash.buckets[i];
        while (e) {
            hash_node_t *next = e->next;
            node_free(e);
            e = next;
        }
    }
    free(g_hash.buckets);
    hash_init();                        /* 重置为空状态 */
    kv_pool_release_all();
}

/**
 * 预分配桶（批量加载前调用）
 * 消除加载过程中的反复扩容，提高性能
 * 必须作用在空表上；若非空则先清空
 * @param count 预期的记录数
 */
void hash_reserve(uint32_t count) {
    if (g_hash.buckets) hash_clear();
    uint32_t size = kv_next_pow2(count, HASH_INIT_SIZE);
    if (size > HASH_MAX_SIZE) size = HASH_MAX_SIZE;
    table_alloc(size);
}

/*=============================================================================
 *  扩容
 *
 *  设计决策：采用一次性扩容（非渐进式 rehash）
 *
 *  优点：
 *  - 插入路径更简单，无热路径分支
 *  - 无双表维护开销
 *  - 适合单线程场景
 *
 *  缺点：
 *  - 超大表扩容时有短暂停顿
 *  - 但 demo / 批量加载场景通过 dict_reserve 可避免
 *
 *  对比渐进式 rehash（双表 + rehash_idx）：
 *  - 实现更简单，代码更易维护
 *  - 无 rehash 过程中的指针跳转开销
 *============================================================================*/

/**
 * 检查并执行扩容：负载因子 >= 1 时触发扩容到 2 倍
 * @return 0 成功，-1 失败
 */
static int maybe_grow(void) {
    /* 惰性初始化：首个 add 时才建表 */
    if (g_hash.size == 0) return table_alloc(HASH_INIT_SIZE);

    /* 负载因子 >= 1 且未达到上限时扩容 */
    if (g_hash.size < HASH_MAX_SIZE && g_hash.used >= g_hash.size) {
        uint32_t new_size = kv_next_pow2(g_hash.used * 2, HASH_INIT_SIZE);
        if (new_size > HASH_MAX_SIZE) new_size = HASH_MAX_SIZE;
        if (new_size <= g_hash.size) return 0;   /* 已达上限 */

        /* 分配新桶数组 */
        hash_node_t **nb =
            (hash_node_t **)calloc((size_t)new_size, sizeof(hash_node_t *));
        if (!nb) return -1;

        /* 整体重链到新表：复用缓存的 hash，无需重算 */
        for (uint32_t i = 0; i < g_hash.size; i++) {
            hash_node_t *e = g_hash.buckets[i];
            while (e) {
                hash_node_t *next = e->next;
                uint32_t idx = (uint32_t)(e->hash & (new_size - 1));
                e->next = nb[idx];   /* 头插法 */
                nb[idx] = e;
                e = next;
            }
        }
        free(g_hash.buckets);
        g_hash.buckets   = nb;
        g_hash.size      = new_size;
        g_hash.size_mask = new_size - 1;
    }
    return 0;
}

/*=============================================================================
 *  核心操作
 *============================================================================*/

/**
 * 桶内查找
 * 查找策略（按优先级）：
 * 1. 先比对缓存 hash（一个 load + cmp 提前拒绝）
 * 2. 再比 key 长度（整数比较）
 * 3. 最后字节比对（kv_fast_cmp）
 */
static hash_node_t *bucket_find(const char *key, uint32_t key_len,
                                uint64_t hash)
{
    if (unlikely(g_hash.size == 0)) return NULL;   /* 空表：未建表 */

    uint32_t idx = (uint32_t)(hash & g_hash.size_mask);
    hash_node_t *e = g_hash.buckets[idx];

    while (e) {
        if (likely(e->hash == hash) &&
            e->key_len == key_len &&
            kv_fast_cmp(e->data, key, (int)key_len) == 0)
            return e;
        e = e->next;
    }
    return NULL;
}

/**
 * 插入一条新记录（不查重）：扩容 + 头插，哈希由调用方算好传进来
 *
 * 把哈希作为形参而不是在这里算，是为了让下面两个入口都只算一次：add 算完
 * 直接插，add_or_find 算完先查重、未命中再拿同一个值来插。
 */
static hash_node_t *insert_new(const char *key, uint32_t key_len,
                               const char *payload, uint32_t payload_len,
                               uint64_t hash)
{
    /* 扩容 fast-path：常见路径一个可预测的分支直接跳过 */
    if (unlikely(g_hash.used >= g_hash.size)) {
        if (maybe_grow() < 0) return NULL;
    }

    hash_node_t *e = node_new(key, key_len, payload, payload_len, hash);
    if (!e) return NULL;

    uint32_t idx = (uint32_t)(hash & g_hash.size_mask);
    e->next = g_hash.buckets[idx];   /* 头插法 */
    g_hash.buckets[idx] = e;
    g_hash.used++;
    return e;
}

/** 新增记录（不查重）；失败 NULL */
void *hash_add(const char *key, uint32_t key_len,
               const char *payload, uint32_t payload_len)
{
    return insert_new(key, key_len, payload, payload_len,
                      key_hash(key, key_len));
}

/**
 * 新增记录，键已存在则不插入（SET 专用）
 *
 * 查重与插入合在一次调用里，是为了让「一次操作只算一次哈希」成立：哈希在
 * 这里算完先用于查重，未命中再用于插入定位，全操作只有这一次。
 * 键已存在时返回既有节点并置 *exists=1，由上层决定算不算失败（SET 算）。
 * @param exists 输出：1 表示键本来就在（记录没动），0 表示本次真插入了；可为 NULL
 * @return 记录（新插入的，或既有的那条），失败 NULL
 */
void *hash_add_or_find(const char *key, uint32_t key_len,
                       const char *payload, uint32_t payload_len, int *exists)
{
    if (exists) *exists = 0;

    uint64_t hash = key_hash(key, key_len);   /* 本次操作唯一的一次哈希 */

    hash_node_t *old = bucket_find(key, key_len, hash);
    if (old) {
        if (exists) *exists = 1;
        return old;
    }
    return insert_new(key, key_len, payload, payload_len, hash);
}

/** 查找记录，未找到返回 NULL */
void *hash_find(const char *key, uint32_t key_len) {
    return bucket_find(key, key_len, key_hash(key, key_len));
}

int hash_delete(const char *key, uint32_t key_len) {
    if (unlikely(g_hash.size == 0)) return -1;

    uint64_t hash = key_hash(key, key_len);
    uint32_t idx  = (uint32_t)(hash & g_hash.size_mask);
    hash_node_t **pp = &g_hash.buckets[idx];

    /* 遍历链表查找匹配的记录 */
    while (*pp) {
        hash_node_t *e = *pp;
        if (e->hash == hash && e->key_len == key_len &&
            kv_fast_cmp(e->data, key, (int)key_len) == 0)
        {
            *pp = e->next;   /* 从链表中摘除 */
            node_free(e);
            g_hash.used--;
            return 0;
        }
        pp = &e->next;
    }
    return -1;  /* 未找到 */
}

/**
 * 重建节点以改变 payload 长度
 *
 * 为什么尺寸一改就重建，连缩小也不原地（只改 val_len）？
 * 见 kv_pool.h：块尺寸必须恒等于「sizeof(节点) + key_len + val_len」，
 * 原地缩会让 free 时推导出的尺寸小于实际块，池的档位 live 计数被减成负值。
 *
 * 新节点自带 next 为空，接回桶链由这里负责：按 e->hash 定位桶，沿链找到
 * 指向 e 的那一格（或桶头）换成新节点。payload 不拷贝 —— 按契约由调用方
 * 随后整段写入。
 */
void *hash_resize_entry(void *n, uint32_t new_payload_len) {
    hash_node_t *e = (hash_node_t *)n;
    if (new_payload_len == e->val_len)
        return e;  /* 长度不变，直接返回 */

    hash_node_t *ne = node_new(e->data, e->key_len, NULL, new_payload_len, e->hash);
    if (unlikely(!ne)) return NULL;
    ne->expire = e->expire;   /* expire 沿用旧值 */

    /* 在桶链中找到 e 的位置，用 ne 顶替；旧节点随即释放 */
    uint32_t idx = (uint32_t)(e->hash & g_hash.size_mask);
    hash_node_t **pp = &g_hash.buckets[idx];
    while (*pp && *pp != e) pp = &(*pp)->next;
    ne->next = e->next;
    if (*pp) *pp = ne;   /* e 不在链中时只释放旧的（理论上不应发生） */
    node_free(e);

    return ne;
}

uint32_t hash_count(void) {
    return g_hash.used;
}

/*=============================================================================
 *  随机采样
 *============================================================================*/

/**
 * 随机取一条记录（用于过期采样 active_expire_cycle）
 *
 * 算法：随机起点线性探测到第一个非空桶，返回该桶链头。
 * 不再做「链内均匀采样」：负载因子 < 1 时链长几乎恒为 1，那套随机基本空转，
 * 换来的均匀性对过期采样没有意义。采样偏差留在两处 —— 长链只有链头会被取到，
 * 且每个桶的权重正比于它前面那段连续空桶的长度。
 */
void *hash_get_random_key(void) {
    if (g_hash.used == 0) return NULL;   /* 空表 ⇒ 桶数组还没建 */

    uint32_t bucket_idx = (uint32_t)rand() % g_hash.size;
    while (g_hash.buckets[bucket_idx] == NULL)
        bucket_idx = (bucket_idx + 1) % g_hash.size;   /* used > 0 ⇒ 至多绕一圈必命中 */

    return g_hash.buckets[bucket_idx];
}

/*=============================================================================
 *  迭代器
 *  单表设计无渐进 rehash，迭代期间表不会切换
 *
 *  游标停在本条上（cur = 本次要返回的记录）：桶内顺链走，走到链尾就往后
 *  找第一个非空桶。不需要预热，也不需要「下一格」前瞻。
 *
 *  句柄本身就是游标（返回 void *）—— 不再有「公共盒子 + 游标」两层，
 *  一次 malloc、一次 free。
 *============================================================================*/

typedef struct {
    int64_t      bucket;   /* 当前桶下标（-1 表示未开始） */
    hash_node_t *cur;      /* 本次要返回的记录 */
} hash_iter_t;

void *hash_iter_new(void) {
    hash_iter_t *c = (hash_iter_t *)malloc(sizeof(hash_iter_t));
    if (!c) return NULL;

    c->bucket = -1;
    c->cur    = NULL;
    return c;
}

void *hash_iter_next(void *it) {
    hash_iter_t *c = (hash_iter_t *)it;

    /* 当前桶还有下一条就顺链前进 */
    if (c->cur) {
        c->cur = c->cur->next;
        if (c->cur) return c->cur;
    }

    /* 当前桶走完了：往后找第一个非空桶，停到它的链头 */
    for (c->bucket++; c->bucket < (int64_t)g_hash.size; c->bucket++) {
        if (g_hash.buckets[c->bucket]) {
            c->cur = g_hash.buckets[c->bucket];
            return c->cur;
        }
    }
    return NULL;   /* 表已走完；bucket 停在 size，再次调用直接落空 */
}

void hash_iter_free(void *it) {
    free(it);
}
