/*=============================================================================
 *  kv_hash.h — 哈希引擎（原 kv_dict.c 的哈希字典）
 *
 *  结构：桶数组 + 桶内单向链表（链指针内联在自己的节点里，见 kv_hash.c）
 *  特点：单表、无渐进 rehash、负载因子 >= 1 时一次性翻倍扩容
 *
 *  本文件是「引擎对外边界」的样板：节点是 void *（真实布局 hash_node_t 封在
 *  kv_hash.c 里），迭代器也是 void *（游标形态封在里面）。因此这里**不 include
 *  任何项目头**，只有 <stdint.h> —— 引擎能脱离路由层单独编译、单独测试。
 *
 *  其余三个引擎（数组 / 红黑树 / 跳表）接口与此完全同构：
 *  20 个 hash_ / array_ / rb_ / skip_ 前缀操作（14 个增删改查 + 6 个字段访问器），
 *  由 kv_engine.h 的 dict_ / entry_ 路由按 g_kv_engine 分派。
 *  新增引擎 = 新增一对 .c/.h 实现这 20 个操作 + 自己定义一个节点类型。
 *
 *  add / find / delete 一律只收 key：索引怎么定位是引擎自己的事，哈希引擎
 *  收在 key_hash 里，另外三个按 key 有序/顺序找 —— 路由层因此不再经手哈希。
 *
 *  add 与 add_or_find 的分工见各自注释：前者不查重（批量加载），后者查重
 *  （SET）。哈希引擎的两个入口共用同一次 key_hash，是唯一必须分开写的引擎。
 *============================================================================*/

#ifndef __KV_HASH_H__
#define __KV_HASH_H__

#include <stdint.h>

/* 生命周期 ---------------------------------------------------------------*/

/** 初始化：置为空表（首次插入时才真正建桶数组） */
void hash_init(void);

/** 销毁：等价于 clear（结构本身是文件级静态变量，不走堆） */

/** 全清：释放所有节点与桶数组，并把空超块归还底座（FLUSHALL / RDB 重载后 RSS 回落） */
void hash_clear(void);

/** 预分配桶（批量加载前调用）：必须作用在空表上，非空则先全清 */
void hash_reserve(uint32_t count);

/* 增删改查 ---------------------------------------------------------------*/

/** 新增记录（不查重，直接插）；失败 NULL */
void *hash_add(const char *key, uint32_t key_len,
               const char *payload, uint32_t payload_len);

/**
 * 新增记录，键已存在则不插入（返回既有节点并置 *exists=1）；失败 NULL
 * 哈希在函数内算且只算一次，键的哈希是引擎内部事务，形参里没有它
 * @param exists 输出：1 = 键本来就在（记录没动），0 = 本次真插入了；可为 NULL
 */
void *hash_add_or_find(const char *key, uint32_t key_len,
                       const char *payload, uint32_t payload_len, int *exists);

/** 查找记录，未找到返回 NULL */
void *hash_find(const char *key, uint32_t key_len);

/** 按 key 删除记录；0 成功，-1 未找到 */
int hash_delete(const char *key, uint32_t key_len);

/** 调整 payload 长度（一律重建）；返回新节点，旧指针作废 */
void *hash_resize_entry(void *n, uint32_t new_payload_len);

/** 当前记录数（O(1)） */
uint32_t hash_count(void);

/** 随机取一条记录（过期采样用），空表返回 NULL */
void *hash_get_random_key(void);

/* 字段访问器 -------------------------------------------------------------*/

const char *hash_key(void *n);              /* 内联 key 指针（非 \0 终止） */
uint32_t    hash_key_len(void *n);
char       *hash_val(void *n);              /* payload 指针 */
uint32_t    hash_val_len(void *n);
int64_t     hash_expire(void *n);
void        hash_set_expire(void *n, int64_t v);

/* 迭代器 -----------------------------------------------------------------*/

void *hash_iter_new(void);
void *hash_iter_next(void *it);
void  hash_iter_free(void *it);

#endif /* __KV_HASH_H__ */
