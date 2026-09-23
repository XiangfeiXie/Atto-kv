/*=============================================================================
 *  kv_array.h — 数组引擎
 *
 *  结构：定容槽位数组（array_node_t *slots[cap]，满则翻倍）
 *  特点：删除用「尾部元素补位」，槽位永远无空洞 —— 因此
 *  - 插入永远是 slots[used++] 一步
 *  - 查找/改尺寸/迭代都是一段紧凑的 0..used 顺序扫描，cache 友好
 *  代价：删除会打乱顺序（本来也无序），且查找是 O(n) 线性扫描。
 *
 *  demo 定位：不做哈希索引、不存哈希值，就是要一个「最笨但一定对」的对照
 *  引擎。节点比哈希引擎少一个链指针、少一个 hash 字段（判等直接比 key），
 *  其余布局同构。查重也因此退化成一次顺序扫描，而不是哈希引擎那样先看 hash。
 *
 *  接口形态与其余三个引擎完全一致：节点是 void *（真实布局 array_node_t
 *  封在 kv_array.c 里），因此这里**不 include 任何项目头**，只有 <stdint.h>。
 *============================================================================*/

#ifndef __KV_ARRAY_H__
#define __KV_ARRAY_H__

#include <stdint.h>

/* 生命周期 ---------------------------------------------------------------*/

void array_init(void);
void array_clear(void);
void array_reserve(uint32_t count);

/* 增删改查 ---------------------------------------------------------------*/

/* 三个操作都只收 key：本引擎没有哈希索引，定位靠 key_len + kv_fast_cmp
 * 的顺序扫描，与红黑树 / 跳表的按 key 有序查找同构。 */

/** 新增记录（不查重，直接追加在尾部）；失败 NULL */
void *array_add(const char *key, uint32_t key_len,
                const char *payload, uint32_t payload_len);

/**
 * 新增记录，键已存在则不插入（返回既有节点并置 *exists=1）；失败 NULL
 * @param exists 输出：1 = 键本来就在（记录没动），0 = 本次真插入了；可为 NULL
 */
void *array_add_or_find(const char *key, uint32_t key_len,
                        const char *payload, uint32_t payload_len, int *exists);
void *array_find(const char *key, uint32_t key_len);
int   array_delete(const char *key, uint32_t key_len);
void *array_resize_entry(void *n, uint32_t new_payload_len);
uint32_t array_count(void);
void *array_get_random_key(void);

/* 字段访问器 -------------------------------------------------------------*/

const char *array_key(void *n);
uint32_t    array_key_len(void *n);
char       *array_val(void *n);
uint32_t    array_val_len(void *n);
int64_t     array_expire(void *n);
void        array_set_expire(void *n, int64_t v);

/* 迭代器 -----------------------------------------------------------------*/

void *array_iter_new(void);
void *array_iter_next(void *it);
void  array_iter_free(void *it);

#endif /* __KV_ARRAY_H__ */
