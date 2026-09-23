/*=============================================================================
 *  kv_skiplist.h — 跳表引擎
 *
 *  结构：多层有序链表，层高 1/2 概率逐层上升（上限 SKIP_MAXLEVEL 层）
 *  特点：查找/插入/删除期望 O(log n)，实现比红黑树短，且天然有序可范围扫
 *
 *  节点把「索引」与「记录」合并成一块内存（forward[] 数组之后紧跟 key /
 *  payload），所以每条记录一次分配 —— 不再是「记录一次 + 索引节点一次」。
 *  插删只改 forward 指针，节点地址恒不变（上层 dict_find 后原地
 *  entry_set_expire 依赖这一点）；只有 resize_entry 会换地址。
 *
 *  接口形态与其余三个引擎完全一致：节点是 void *（真实布局 skip_node_t 封在
 *  kv_skiplist.c 里），因此这里**不 include 任何项目头**，只有 <stdint.h>。
 *============================================================================*/

#ifndef __KV_SKIPLIST_H__
#define __KV_SKIPLIST_H__

#include <stdint.h>

/* 生命周期 ---------------------------------------------------------------*/

void skip_init(void);
void skip_clear(void);
void skip_reserve(uint32_t count);

/* 增删改查 ---------------------------------------------------------------*/

/** 新增记录（不查重，直接逐层接入）；失败 NULL */
void *skip_add(const char *key, uint32_t key_len,
               const char *payload, uint32_t payload_len);

/**
 * 新增记录，键已存在则不插入（返回既有节点并置 *exists=1）；失败 NULL
 * @param exists 输出：1 = 键本来就在（记录没动），0 = 本次真插入了；可为 NULL
 */
void *skip_add_or_find(const char *key, uint32_t key_len,
                       const char *payload, uint32_t payload_len, int *exists);
void *skip_find(const char *key, uint32_t key_len);
int   skip_delete(const char *key, uint32_t key_len);
void *skip_resize_entry(void *n, uint32_t new_payload_len);
uint32_t skip_count(void);
void *skip_get_random_key(void);

/* 字段访问器 -------------------------------------------------------------*/

const char *skip_key(void *n);
uint32_t    skip_key_len(void *n);
char       *skip_val(void *n);
uint32_t    skip_val_len(void *n);
int64_t     skip_expire(void *n);
void        skip_set_expire(void *n, int64_t v);

/* 迭代器 -----------------------------------------------------------------*/

void *skip_iter_new(void);
void *skip_iter_next(void *it);
void  skip_iter_free(void *it);

#endif /* __KV_SKIPLIST_H__ */
