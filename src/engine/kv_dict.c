/*=============================================================================
 *  kv_dict.c — 哈希字典实现
 *  
 *  功能：提供 K/V 存储的核心数据结构
 *  设计特点：
 *  1. 单表设计，无渐进 rehash（适合单线程场景）
 *  2. 负载因子 >= 1 时一次性扩容（2 倍）
 *  3. 内存布局：header + key + payload 连续存储
 *  4. 支持随机采样（用于过期键淘汰）
 *  5. 支持条目原地扩容（值变大时重建）
 *============================================================================*/

#include "kv_dict.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/*=============================================================================
 *  内部常量与工具
 *============================================================================*/

#define DICT_INIT_SIZE  16          /* 初始桶数（必须是 2 的幂） */
#define DICT_MAX_SIZE   (1U << 24)  /* 桶数上限 ~16M */

/**
 * 计算大于等于 v 的最小 2 的幂
 * 用于确定哈希表大小
 * @param v 输入值
 * @return 2 的幂
 */
static uint32_t next_pow2(uint32_t v) {
    if (v <= DICT_INIT_SIZE) return DICT_INIT_SIZE;
    v--;
    v |= v >> 1;  v |= v >> 2;
    v |= v >> 4;  v |= v >> 8;
    v |= v >> 16;
    return v + 1;
}

/**
 * 分配一个 entry
 * 内存布局：kv_entry_t 头部 + key 数据 + payload 数据
 * 一次 malloc 完成，减少内存碎片
 * 
 * @param key 键指针
 * @param key_len 键长度
 * @param payload 值指针
 * @param payload_len 值长度
 * @param hash 键的哈希值（预计算）
 * @return 分配的 entry 指针，失败返回 NULL
 */
static kv_entry_t *entry_alloc(const char *key, uint32_t key_len,
                               const char *payload, uint32_t payload_len,
                               uint64_t hash)
{
    uint32_t total = (uint32_t)sizeof(kv_entry_t) + key_len + payload_len;
    kv_entry_t *e = (kv_entry_t *)malloc((size_t)total);
    if (unlikely(!e)) return NULL;

    e->next    = NULL;
    e->hash    = hash;
    e->expire  = 0;
    e->key_len = key_len;
    e->val_len = payload_len;

    /* 复制 key 和 payload 到连续内存 */
    memcpy(e->data, key, key_len);
    if (payload && payload_len)
        memcpy(e->data + key_len, payload, payload_len);

    return e;
}

/*=============================================================================
 *  表分配 / 生命周期管理
 *============================================================================*/

/**
 * 分配哈希表桶数组
 * @param d 字典指针
 * @param size 桶数量（2 的幂）
 * @return 0 成功，-1 失败
 */
static int dict_alloc_table(kv_dict_t *d, uint32_t size) {
    d->size      = size;
    d->size_mask = size - 1;   /* 用于快速取模（hash & mask） */
    d->used      = 0;
    d->buckets   = (kv_entry_t **)calloc((size_t)size, sizeof(kv_entry_t *));
    return d->buckets ? 0 : -1;
}

/**
 * 初始化字典结构
 * @param d 字典指针
 */
void dict_init(kv_dict_t *d) {
    d->buckets   = NULL;
    d->size      = 0;
    d->size_mask = 0;
    d->used      = 0;
}

/**
 * 清空字典（删除所有条目）
 * @param d 字典指针
 */
void dict_clear(kv_dict_t *d) {
    if (!d->buckets) return;
    
    /* 遍历所有桶，释放每个 entry */
    for (uint32_t i = 0; i < d->size; i++) {
        kv_entry_t *e = d->buckets[i];
        while (e) {
            kv_entry_t *next = e->next;
            free(e);
            e = next;
        }
    }
    free(d->buckets);
    dict_init(d);  /* 重置为空状态 */
}

/**
 * 销毁字典
 * @param d 字典指针
 */
void dict_destroy(kv_dict_t *d) {
    dict_clear(d);
}

/**
 * 预分配桶（批量加载前调用）
 * 消除加载过程中的反复扩容，提高性能
 * 必须作用在空 dict 上；若非空则先清空
 * @param d 字典指针
 * @param count 预期的条目数
 */
void dict_reserve(kv_dict_t *d, uint32_t count) {
    if (d->buckets) dict_clear(d);
    uint32_t size = next_pow2(count);
    if (size > DICT_MAX_SIZE) size = DICT_MAX_SIZE;
    dict_alloc_table(d, size);
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
 * 检查并执行扩容
 * 负载因子 >= 1 时触发扩容到 2 倍
 * @param d 字典指针
 * @return 0 成功，-1 失败
 */
static int dict_maybe_grow(kv_dict_t *d) {
    /* 惰性初始化：首个 add 时才建表 */
    if (d->size == 0) return dict_alloc_table(d, DICT_INIT_SIZE);

    /* 负载因子 >= 1 且未达到上限时扩容 */
    if (d->size < DICT_MAX_SIZE && d->used >= d->size) {
        uint32_t new_size = next_pow2(d->used * 2);
        if (new_size > DICT_MAX_SIZE) new_size = DICT_MAX_SIZE;
        if (new_size <= d->size) return 0;  /* 已达上限 */

        /* 分配新桶数组 */
        kv_entry_t **nb = (kv_entry_t **)calloc((size_t)new_size, sizeof(kv_entry_t *));
        if (!nb) return -1;

        /* 整体重链到新表：复用缓存的 hash，无需重算 */
        for (uint32_t i = 0; i < d->size; i++) {
            kv_entry_t *e = d->buckets[i];
            while (e) {
                kv_entry_t *next = e->next;
                uint32_t idx = (uint32_t)(e->hash & (new_size - 1));
                /* 头插法 */
                e->next = nb[idx];
                nb[idx] = e;
                e = next;
            }
        }
        free(d->buckets);
        d->buckets   = nb;
        d->size      = new_size;
        d->size_mask = new_size - 1;
    }
    return 0;
}

/*=============================================================================
 *  核心操作
 *============================================================================*/

/**
 * 添加条目
 * @param d 字典指针
 * @param key 键指针
 * @param key_len 键长度
 * @param payload 值指针
 * @param payload_len 值长度
 * @param hash 键的哈希值
 * @return 新增的 entry 指针，失败返回 NULL
 */
kv_entry_t *dict_add(kv_dict_t *d, const char *key, uint32_t key_len,
                     const char *payload, uint32_t payload_len, uint64_t hash)
{
    /* 扩容 fast-path：常见路径一个可预测的分支直接跳过 */
    if (unlikely(d->used >= d->size)) {
        if (dict_maybe_grow(d) < 0) return NULL;
    }

    kv_entry_t *e = entry_alloc(key, key_len, payload, payload_len, hash);
    if (!e) return NULL;

    /* 头插法 */
    uint32_t idx = (uint32_t)(hash & d->size_mask);
    e->next = d->buckets[idx];
    d->buckets[idx] = e;
    d->used++;
    return e;
}

/**
 * 删除条目
 * @param d 字典指针
 * @param key 键指针
 * @param key_len 键长度
 * @param hash 键的哈希值
 * @return 0 成功，-1 未找到
 */
int dict_delete(kv_dict_t *d, const char *key, uint32_t key_len,
                uint64_t hash)
{
    if (unlikely(d->size == 0)) return -1;

    uint32_t idx = (uint32_t)(hash & d->size_mask);
    kv_entry_t **pp = &d->buckets[idx];
    
    /* 遍历链表查找匹配的 entry */
    while (*pp) {
        kv_entry_t *e = *pp;
        if (e->hash == hash && e->key_len == key_len &&
            kv_fast_cmp(entry_key(e), key, (int)key_len) == 0)
        {
            *pp = e->next;   /* 从链表中摘除 */
            free(e);
            d->used--;
            return 0;
        }
        pp = &e->next;
    }
    return -1;  /* 未找到 */
}

/**
 * 调整 entry 的 payload 大小（原地或重建）
 * @param d 字典指针
 * @param e 待调整的 entry
 * @param new_payload_len 新的 payload 长度
 * @return 调整后的 entry 指针（可能与原指针不同），失败返回 NULL
 */
kv_entry_t *dict_resize_entry(kv_dict_t *d, kv_entry_t *e,
                              uint32_t new_payload_len)
{
    if (new_payload_len == e->val_len)
        return e;  /* 长度不变，直接返回 */

    /* 缩小：key 在头部不动，只改尾部的 val_len 即可 */
    if (new_payload_len < e->val_len) {
        e->val_len = new_payload_len;
        return e;
    }

    /* 变大：新建 entry 顶替链中旧 entry，再释放旧的
     * 
     * 为什么不用 realloc 原地扩容？
     * 1. jemalloc 下 realloc 可能移动指针，破坏桶链
     * 2. 对 arena 内 entry 做 realloc 是未定义行为
     */
    uint32_t total = (uint32_t)sizeof(kv_entry_t) + e->key_len + new_payload_len;
    kv_entry_t *ne = (kv_entry_t *)malloc((size_t)total);
    if (unlikely(!ne)) return NULL;

    /* 复制原有数据 */
    ne->next    = NULL;
    ne->hash    = e->hash;
    ne->expire  = e->expire;
    ne->key_len = e->key_len;
    ne->val_len = new_payload_len;
    memcpy(ne->data, entry_key(e), e->key_len);

    /* 在桶链中找到 e 的位置，用 ne 顶替 */
    uint32_t idx = (uint32_t)(e->hash & d->size_mask);
    kv_entry_t **pp = &d->buckets[idx];
    while (*pp && *pp != e) pp = &(*pp)->next;
    ne->next = e->next;
    if (*pp) *pp = ne;   /* e 不在链中时只释放旧的（理论上不应发生） */
    free(e);

    return ne;
}

/*=============================================================================
 *  迭代器
 *  单表设计无渐进 rehash，天然安全
 *============================================================================*/

/**
 * 创建迭代器
 * @param d 字典指针
 * @param safe 是否安全迭代（单线程无 rehash，此参数无实际作用）
 * @return 迭代器指针，失败返回 NULL
 */
kv_iter_t *dict_iter_new(kv_dict_t *d, int safe) {
    (void)safe;   /* 单线程 + 无渐进 rehash：迭代期间表不会切换 */

    kv_iter_t *it = (kv_iter_t *)calloc(1, sizeof(kv_iter_t));
    if (!it) return NULL;

    it->dict   = d;
    it->bucket = -1;
    it->next   = NULL;

    dict_iter_next(it);   /* 定位到第一个 entry */
    return it;
}

/**
 * 获取迭代器的下一个 entry
 * @param it 迭代器指针
 * @return 下一个 entry，无更多返回 NULL
 */
kv_entry_t *dict_iter_next(kv_iter_t *it) {
    kv_entry_t *result = it->next;

    while (1) {
        /* 如果当前桶还有下一个，直接返回 */
        if (it->next) {
            it->next = it->next->next;
            if (it->next) break;
        }

        /* 移到下一个非空桶 */
        it->bucket++;
        kv_dict_t *d = it->dict;
        while (it->bucket < (int64_t)d->size) {
            if (d->buckets[it->bucket]) {
                it->next = d->buckets[it->bucket];
                goto done;
            }
            it->bucket++;
        }
        it->next = NULL;
        break;
    }
done:
    return result;
}

/**
 * 释放迭代器
 * @param it 迭代器指针
 */
void dict_iter_free(kv_iter_t *it) {
    free(it);
}

/*=============================================================================
 *  统计和辅助功能
 *============================================================================*/

/**
 * 获取字典中的条目数
 * @param d 字典指针
 * @return 条目数
 */
uint32_t dict_count(kv_dict_t *d) {
    return d->used;
}

/**
 * 从字典中获取一个随机键
 * 用于过期采样（active_expire_cycle）
 * 
 * 算法：
 * 1. 随机选择一个桶
 * 2. 线性探测找第一个非空桶
 * 3. 在桶的链表中随机选择一个 entry
 * 
 * @param d 字典指针
 * @return 随机 entry，空字典返回 NULL
 */
kv_entry_t *dict_get_random_key(kv_dict_t *d) {
    if (d->used == 0) return NULL;

    uint32_t bucket_idx = (uint32_t)rand() % d->size;

    /* 线性探测找第一个非空桶 */
    uint32_t max_steps = d->size;
    while (max_steps-- > 0) {
        if (d->buckets[bucket_idx] != NULL) {
            kv_entry_t *entry = d->buckets[bucket_idx];
            
            /* 计算链长 */
            int chain_len = 0;
            for (kv_entry_t *tmp = entry; tmp; tmp = tmp->next) chain_len++;

            /* 链内随机取一个（均匀分布） */
            int target = rand() % chain_len;
            for (int i = 0; i < target; i++) entry = entry->next;
            return entry;
        }
        bucket_idx = (bucket_idx + 1) % d->size;
    }
    return NULL;  /* 理论上不会到达（d->used > 0） */
}

/*=============================================================================
 *  全局字典实例
 *  单线程、无分片设计
 *============================================================================*/

kv_dict_t g_dict;