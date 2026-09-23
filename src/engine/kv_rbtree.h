/*=============================================================================
 *  kv_rbtree.h — 红黑树引擎
 *
 *  结构：带哨兵（nil）的红黑树，节点按 key 排序（kv_key_cmp 全序）
 *  特点：查找/插入/删除都是 O(log n)，遍历天然有序
 *
 *  节点把「索引」与「记录」合并成一块内存（rb_node_t 里直接跟 key/payload），
 *  所以每条记录一次分配 —— 不再是「记录一次 + 索引节点一次」。
 *  旋转只改节点间的指针，节点地址在旋转中恒不变（上层 dict_find 后原地
 *  entry_set_expire 依赖这一点）；只有 resize_entry 会换地址。
 *
 *  实现按 CLRS 第三版：插入 fixup + 删除 fixup（含哨兵）。
 *
 *  接口形态与其余三个引擎完全一致：节点是 void *（真实布局 rb_node_t 封在
 *  kv_rbtree.c 里），因此这里**不 include 任何项目头**，只有 <stdint.h>。
 *============================================================================*/

#ifndef __KV_RBTREE_H__
#define __KV_RBTREE_H__

#include <stdint.h>

/* 生命周期 ---------------------------------------------------------------*/

void rb_init(void);
void rb_clear(void);
void rb_reserve(uint32_t count);

/* 增删改查 ---------------------------------------------------------------*/

/** 新增记录（不查重，直接挂到叶子下）；失败 NULL */
void *rb_add(const char *key, uint32_t key_len,
             const char *payload, uint32_t payload_len);

/**
 * 新增记录，键已存在则不插入（返回既有节点并置 *exists=1）；失败 NULL
 * @param exists 输出：1 = 键本来就在（记录没动），0 = 本次真插入了；可为 NULL
 */
void *rb_add_or_find(const char *key, uint32_t key_len,
                     const char *payload, uint32_t payload_len, int *exists);
void *rb_find(const char *key, uint32_t key_len);
int   rb_delete(const char *key, uint32_t key_len);
void *rb_resize_entry(void *n, uint32_t new_payload_len);
uint32_t rb_count(void);
void *rb_get_random_key(void);

/* 字段访问器 -------------------------------------------------------------*/

const char *rb_key(void *n);
uint32_t    rb_key_len(void *n);
char       *rb_val(void *n);
uint32_t    rb_val_len(void *n);
int64_t     rb_expire(void *n);
void        rb_set_expire(void *n, int64_t v);

/* 迭代器 -----------------------------------------------------------------*/

void *rb_iter_new(void);
void *rb_iter_next(void *it);
void  rb_iter_free(void *it);

#endif /* __KV_RBTREE_H__ */
