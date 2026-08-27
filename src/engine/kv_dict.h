/*=============================================================================
 *  kv_dict.h — 哈希字典头文件
 *  
 *  设计目标：面向单线程 io_uring 事件循环场景的性能强化版字典
 *  使用场景：redis-benchmark SET/GET 热路径
 *  
 *  设计原则：
 *  1. 热路径极致优化（inline、likely/unlikely、缓存 hash）
 *  2. 内存布局连续（header + key + payload，CPU cache 友好）
 *  3. 阻塞式扩容（负载因子 >= 1 时整体搬迁）
 *  4. 单表设计，无渐进 rehash（适合单线程）
 *  5. 内存分配交给 jemalloc，不引入自定义内存池
 *============================================================================*/

#ifndef __KV_DICT_H__
#define __KV_DICT_H__

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/*=============================================================================
 *  设计理念说明
 *  
 *  面向当前使用场景（单线程 io_uring + redis-benchmark SET/GET）
 *  设计，不做工业级通用（无渐进 rehash / 无内存池），只求热路径最快：
 *  
 *  - kv_fast_cmp：寄存器级 8/4 字节步长比对，替代 libc memcmp
 *  - dict_find：static inline 下沉到调用方，dispatch 热路径零函数调用
 *  - 链内先比对缓存 hash（一个 load + cmp 提前拒绝），再比长度、最后字节比对
 *  - likely/unlikely 分支提示（__builtin_expect）
 *  - 每个 entry 单次 malloc：header + key + payload（连续布局，CPU cache 友好）
 *  - 阻塞式扩容（负载因子 >= 1 时整体搬迁到 2x 大表）
 *  - entry 内缓存 hash（扩容/查找时无需重算）
 *  - 内存分配/释放全部交给 jemalloc（USE_JEMALLOC 默认 ON）
 *  
 *  Entry 布局: [header][key: key_len][payload: val_len]
 *  key 固定在 data[] 头部，扩容时永不移动；仅尾部 payload 会移位。
 *============================================================================*/

/*=============================================================================
 *  分支预测宏
 *============================================================================*/

#ifndef likely
#define likely(x)   __builtin_expect(!!(x), 1)   /* 大概率 true */
#define unlikely(x) __builtin_expect(!!(x), 0)   /* 小概率 true */
#endif

/*=============================================================================
 *  哈希条目结构
 *  
 *  内存布局（连续分配，一次 malloc）：
 *  +------------------+------------------+------------------+
 *  | kv_entry_t 头部  | key 数据 (N 字节) | payload 数据 (M 字节) |
 *  +------------------+------------------+------------------+
 *  
 *  优点：
 *  - 一次 malloc 减少内存碎片
 *  - key 和 payload 连续存储，CPU cache 友好
 *  - 扩容时指针不变，只移动 payload（通过 val_len 调整）
 *============================================================================*/

typedef struct kv_entry_s {
    struct kv_entry_s *next;    /* 链表指针（同桶中下一个）    (8B) */
    uint64_t           hash;    /* 缓存的哈希值（避免重算）    (8B) */
    long long          expire;  /* 过期时间戳（毫秒，0=永不过期）(8B) */
    uint32_t           key_len; /* key 长度                    (4B) */
    uint32_t           val_len; /* payload 长度               (4B) */
    char               data[];  /* 柔性数组：[key][payload]    (0B) */
} kv_entry_t;

/*=============================================================================
 *  条目访问器（内联函数）
 *============================================================================*/

/**
 * 获取 entry 的 key 指针
 * @param e entry 指针
 * @return key 字符串指针（非 \0 终止）
 */
static inline const char *entry_key(kv_entry_t *e) { 
    return e->data; 
}

/**
 * 获取 entry 的 payload 指针
 * @param e entry 指针
 * @return payload 数据指针（非 \0 终止）
 */
static inline char *entry_payload(kv_entry_t *e) { 
    return e->data + e->key_len; 
}

/**
 * 获取 entry 的 payload 长度
 * @param e entry 指针
 * @return payload 长度
 */
static inline uint32_t entry_payload_len(kv_entry_t *e) { 
    return e->val_len; 
}

/*=============================================================================
 *  极速字节比对
 *  
 *  策略：8/4 字节块比对 + 尾部逐字节
 *  使用 memcpy 装入 uint64_t/uint32_t：
 *  - 编译器在 -O2 下退化为单条非对齐 load
 *  - 既快又规避了 `*(uint64_t*)p` 的 strict-aliasing UB
 *  
 * @param p1 第一个字符串指针
 * @param p2 第二个字符串指针
 * @param len 比较长度
 * @return 0 相等，非 0 不等
 *============================================================================*/

static inline int kv_fast_cmp(const char *p1, const char *p2, int len) {
    int i = 0;
    
    /* 8 字节块比对（一次比较 8 字节） */
    while (len - i >= 8) {
        uint64_t a, b;
        memcpy(&a, p1 + i, 8);
        memcpy(&b, p2 + i, 8);
        if (a != b) return 1;
        i += 8;
    }
    
    /* 4 字节块比对 */
    if (len - i >= 4) {
        uint32_t a, b;
        memcpy(&a, p1 + i, 4);
        memcpy(&b, p2 + i, 4);
        if (a != b) return 1;
        i += 4;
    }
    
    /* 尾部逐字节比对 */
    while (i < len) {
        if (p1[i] != p2[i]) return 1;
        i++;
    }
    return 0;
}

/*=============================================================================
 *  哈希函数
 *  使用 FNV-1a + Murmur3 finalizer
 *  
 *  特点：
 *  - 8 字节块处理，充分利用 CPU 位宽
 *  - 最终混合（avalanche）提升分布均匀性
 *  - 缓存 hash 值避免查找/扩容时重算
 *============================================================================*/

static inline uint64_t kv_hash(const uint8_t *key, uint32_t len) {
    uint64_t h = 14695981039346656037ULL;  /* FNV-1a offset basis */
    uint32_t i = 0;
    
    /* 8 字节块处理 */
    for (; i + 8 <= len; i += 8) {
        uint64_t word;
        memcpy(&word, key + i, 8);
        h ^= word;
        h *= 1099511628211ULL;             /* FNV prime */
    }
    
    /* 剩余字节处理 */
    for (; i < len; i++) {
        h ^= key[i];
        h *= 1099511628211ULL;
    }
    
    /* Murmur3 64-bit finalizer（雪崩效应，提升分布均匀性） */
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return h;
}

/*=============================================================================
 *  字典结构
 *  单表设计，阻塞式扩容
 *  
 *  字段说明：
 *  - buckets：桶数组指针
 *  - size：桶数量（必须是 2 的幂）
 *  - size_mask：size - 1（用于快速取模）
 *  - used：当前条目数
 *============================================================================*/

typedef struct {
    kv_entry_t **buckets;   /* 桶数组指针 */
    uint32_t     size;      /* 桶数量（2 的幂） */
    uint32_t     size_mask; /* size - 1（用于 hash & mask 取模） */
    uint32_t     used;      /* 当前条目总数 */
} kv_dict_t;

/*=============================================================================
 *  迭代器结构
 *  用于 RDB 保存 / AOF rewrite / SCAN
 *============================================================================*/

typedef struct {
    kv_dict_t  *dict;       /* 正在迭代的字典 */
    int64_t     bucket;     /* 当前桶下标（-1 表示未开始） */
    kv_entry_t *next;       /* 下一个待返回的 entry */
} kv_iter_t;

/*=============================================================================
 *  查找操作（inline 热路径）
 *  
 *  查找策略（按优先级）：
 *  1. 先比对缓存 hash（一个 load + cmp 提前拒绝）
 *  2. 再比 key 长度（整数比较）
 *  3. 最后字节比对（kv_fast_cmp）
 *  
 *  设计为 static inline，下沉到调用方：
 *  - GET/SET/DEL 热路径零函数调用
 *  - 编译器可内联优化
 *============================================================================*/

static inline kv_entry_t *dict_find(kv_dict_t *d, const char *key,
                                    uint32_t key_len, uint64_t hash) {
    if (unlikely(d->size == 0)) return NULL;   /* 空字典：未建表 */

    uint32_t idx = (uint32_t)(hash & d->size_mask);
    kv_entry_t *e = d->buckets[idx];
    
    /* 遍历链表 */
    while (e) {
        /* 优先级：hash > key_len > 字节比对 */
        if (likely(e->hash == hash) &&
            e->key_len == key_len &&
            kv_fast_cmp(entry_key(e), key, (int)key_len) == 0)
            return e;
        e = e->next;
    }
    return NULL;
}

/*=============================================================================
 *  生命周期函数
 *============================================================================*/

/**
 * 初始化字典结构
 * @param d 字典指针
 */
void dict_init(kv_dict_t *d);

/**
 * 销毁字典（释放所有内存）
 * @param d 字典指针
 */
void dict_destroy(kv_dict_t *d);

/**
 * 清空字典（删除所有条目，保留结构）
 * @param d 字典指针
 */
void dict_clear(kv_dict_t *d);

/**
 * 预分配桶（批量加载前调用）
 * 消除加载过程中的反复扩容，提高性能
 * 必须作用在空字典上
 * @param d 字典指针
 * @param count 预期条目数
 */
void dict_reserve(kv_dict_t *d, uint32_t count);

/*=============================================================================
 *  核心操作函数
 *============================================================================*/

/**
 * 添加条目
 * @param d 字典指针
 * @param key 键指针
 * @param key_len 键长度
 * @param payload 值指针
 * @param payload_len 值长度
 * @param hash 预计算的哈希值
 * @return 新增的 entry 指针，失败返回 NULL
 */
kv_entry_t *dict_add(kv_dict_t *d, const char *key, uint32_t key_len,
                     const char *payload, uint32_t payload_len, uint64_t hash);

/**
 * 删除条目
 * @param d 字典指针
 * @param key 键指针
 * @param key_len 键长度
 * @param hash 预计算的哈希值
 * @return 0 成功，-1 未找到
 */
int dict_delete(kv_dict_t *d, const char *key, uint32_t key_len, uint64_t hash);

/**
 * 调整 payload 大小（原地或重建）
 * @param d 字典指针
 * @param e 待调整的 entry
 * @param new_payload_len 新的 payload 长度
 * @return 调整后的 entry 指针（可能与原指针不同），失败返回 NULL
 */
kv_entry_t *dict_resize_entry(kv_dict_t *d, kv_entry_t *e, uint32_t new_payload_len);

/*=============================================================================
 *  迭代器函数
 *============================================================================*/

/**
 * 创建迭代器
 * @param d 字典指针
 * @param safe 是否安全迭代（单线程无 rehash，此参数无实际作用）
 * @return 迭代器指针，失败返回 NULL
 */
kv_iter_t *dict_iter_new(kv_dict_t *d, int safe);

/**
 * 获取下一个 entry（并前进）
 * @param it 迭代器指针
 * @return 下一个 entry，无更多返回 NULL
 */
kv_entry_t *dict_iter_next(kv_iter_t *it);

/**
 * 释放迭代器
 * @param it 迭代器指针
 */
void dict_iter_free(kv_iter_t *it);

/**
 * 查看下一个 entry 但不前进
 * 用于批量序列化时先查大小再提交
 * @param it 迭代器指针
 * @return 下一个 entry，无更多返回 NULL
 */
static inline kv_entry_t *dict_iter_peek(kv_iter_t *it) {
    return it ? it->next : NULL;
}

/*=============================================================================
 *  统计和辅助
 *============================================================================*/

/**
 * 获取字典中的条目数
 * @param d 字典指针
 * @return 条目数
 */
uint32_t dict_count(kv_dict_t *d);

/**
 * 获取一个随机 key（用于过期采样）
 * @param d 字典指针
 * @return 随机 entry，空字典返回 NULL
 */
kv_entry_t *dict_get_random_key(kv_dict_t *d);

/*=============================================================================
 *  全局字典实例
 *  单线程、无分片设计（类似 Redis server.db[0]）
 *============================================================================*/

extern kv_dict_t g_dict;

#endif /* __KV_DICT_H__ */