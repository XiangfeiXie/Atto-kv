/*=============================================================================
 *  kv_engine.h — 存储引擎路由层
 *
 *  四个引擎（哈希 / 数组 / 红黑树 / 跳表）各自独立，像四个热插拔插件：
 *  自己的节点类型、自己的索引结构、自己的增删改查、自己的迭代器、自己管
 *  自己节点的内存。它们之间没有任何共享状态，也没有互相的痕迹 —— 切换全靠
 *  这个文件里的路由。上层（dispatch / persist / transport / expire）一律只调
 *  dict_* 与 entry_*，由 g_kv_engine 决定落到哪个引擎。
 *
 *  引擎的边界长什么样（以哈希为例，见 kv_hash.h）：
 *  - 节点句柄是 void *：引擎头文件对「节点」零类型依赖，不 include 任何
 *    项目头（只有 <stdint.h>），真实布局完全封在自己的 .c 里；
 *  - 19 个操作：13 个增删改查 + 6 个字段访问器，全部 hash_ 前缀。
 *  本文件把它们分派成有类型的 kv_node_t *，上层因此只能传、不能解引用。
 *
 *  为什么用 switch 而不是函数指针表？
 *  1. 配置切换是「启动时定一次」，热路径上 switch 的分支预测器认得出来，
 *     比多一次 indirect call 好；函数指针表还多一张要初始化、可能没填全的表。
 *  2. 简单：加引擎 = 加一个 case；漏了引擎编译器还能靠 -Wswitch 提醒。
 *
 *  为什么路由不带 handle（原来都传 &g_dict）？
 *  各引擎的索引是自己的文件级静态变量，句柄由引擎自己持有；上层不需要、
 *  也不该知道「你在用哪张表」。少一个参数，调用点也更短。
 *
 *  图层：kv_key.h（键原语）/ kv_pool.h（记录内存池）← 四个引擎头 ← 本文件
 *
 *  本文件不转口 kv_key.h：路由这一层只用 <stdint.h> 里的类型，一个键原语都不碰，
 *  也不经手哈希 —— add / find / delete 只收 key，怎么定位是引擎自己的事。
 *============================================================================*/

#ifndef __KV_ENGINE_H__
#define __KV_ENGINE_H__

#include <stddef.h>   /* NULL —— 路由的兜底返回值 */
#include <stdint.h>

#include "kv_hash.h"
#include "kv_array.h"
#include "kv_rbtree.h"
#include "kv_skiplist.h"

/*=============================================================================
 *  引擎对上层暴露的两种句柄
 *
 *  都只是**不完整类型**：真实布局由各引擎在自己的 .c 里定义（hash_node_t /
 *  array_node_t / rb_node_t / skip_node_t），这里只有名字。上层拿到的是一个
 *  只能往下传的指针 —— 想读内容只能走 entry_* 访问器。
 *
 *  放在本文件而不是单开一个头：引擎的 .h 用 void * 收发，压根不需要这两个
 *  名字；需要它们的是上层，而上层本来就看 kv_engine.h。
 *
 *  有类型的句柄比裸 void * 好在能挡住「把迭代器当节点传」这类错误。
 *============================================================================*/

typedef struct kv_node kv_node_t;   /* 一条记录（各引擎的节点） */
typedef struct kv_iter kv_iter_t;   /* 一个迭代器（各引擎的游标） */

/*=============================================================================
 *  引擎枚举与选择
 *============================================================================*/

typedef enum {
    KV_ENGINE_HASH = 0,   /* 哈希字典（默认） */
    KV_ENGINE_ARRAY,      /* 定容槽位数组 */
    KV_ENGINE_RBTREE,     /* 红黑树 */
    KV_ENGINE_SKIPLIST,   /* 跳表 */
} kv_engine_t;

extern kv_engine_t g_kv_engine;

/**
 * 按名字选中引擎（conf 里的 engine hash|array|rbtree|skiplist）
 * 必须在任何 dict_* 调用之前执行：kvstore 在 init_kvengine 里做。
 * @return 0 成功，-1 名字未知（调用方保留原引擎）
 */
int kv_engine_select(const char *name);

/** 当前引擎名（取自上面的枚举，供启动日志 / INFO 展示） */
const char *kv_engine_name(void);

/*=============================================================================
 *  路由：上层只认这 19 个函数（13 个 dict_* + 6 个 entry_*）
 *
 *  每个都是薄薄一层 static inline + switch，没有额外语义。
 *  语义契约（四个引擎都必须满足，原因写在各自的实现里）：
 *   1. 节点地址稳定 —— find 返回后，上层会原地写 entry_set_expire，也会先
 *      peek 再消费再序列化，引擎不得在别处搬动节点。
 *   2. add_or_find 自带查重 —— 键已存在则不插入，返回既有那条并置 *exists=1。
 *      于是 SET 只需一次路由调用：不用先 find 再 add，哈希引擎也就不会算
 *      两次哈希（另外三个按 key 定位，省不出什么，老老实实 find 再 add）。
 *   3. reserve = 预分配（消除批量加载时的反复扩容），四个引擎一律「先清后建」，
 *      于是「reserve 之后表必是空的」对任何调用方都成立。rdma.c 全量加载的
 *      去重靠的是加载前那次 clear，不是 reserve 的副作用 —— 这里的清理只是
 *      让上面那条性质不被调用方的顺序左右。
 *   4. delete 按 key 定位，不按节点指针 —— 过期路径是先存 key 副本再删。
 *   5. resize_entry 返回的是新节点，旧指针作废；尺寸变化一律重建，
 *      保证「块尺寸 == 头部推导值」（kv_pool free 契约）。
 *   6. iter_* 与 get_random_key 四个引擎都要有 —— SAVE / 全量同步走迭代器，
 *      主动过期走随机采样。
 *   7. entry_* 是读写节点字段的唯一途径；节点里有哪些字段由各引擎自己定，
 *      expire 是唯一横切字段（引擎不读、只有上层读写），hash 则只有哈希
 *      引擎用得上（它的桶定位依赖哈希），外面没有它的访问器。
 *============================================================================*/

/** 初始化引擎（清空并建表） */
static inline void dict_init(void) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     hash_init();     break;
    case KV_ENGINE_ARRAY:    array_init();    break;
    case KV_ENGINE_RBTREE:   rb_init();       break;
    case KV_ENGINE_SKIPLIST: skip_init();     break;
    }
}

/** 全清（FLUSHALL / 加载前重置；关闭时即销毁） */
static inline void dict_clear(void) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     hash_clear();     break;
    case KV_ENGINE_ARRAY:    array_clear();    break;
    case KV_ENGINE_RBTREE:   rb_clear();       break;
    case KV_ENGINE_SKIPLIST: skip_clear();     break;
    }
}

/** 预分配（批量加载前，消除反复扩容）；先清后建，返回后表必为空 */
static inline void dict_reserve(uint32_t count) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     hash_reserve(count);     break;
    case KV_ENGINE_ARRAY:    array_reserve(count);    break;
    case KV_ENGINE_RBTREE:   rb_reserve(count);       break;
    case KV_ENGINE_SKIPLIST: skip_reserve(count);     break;
    }
}

/** 新增记录（不查重）；返回新记录，失败 NULL */
static inline kv_node_t *dict_add(const char *key, uint32_t key_len,
                                  const char *val, uint32_t val_len) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     return hash_add(key, key_len, val, val_len);
    case KV_ENGINE_ARRAY:    return array_add(key, key_len, val, val_len);
    case KV_ENGINE_RBTREE:   return rb_add(key, key_len, val, val_len);
    case KV_ENGINE_SKIPLIST: return skip_add(key, key_len, val, val_len);
    }
    return NULL;
}

/**
 * 新增记录，键已存在则不插入（SET 专用）
 *
 * 和 dict_add 分成两个入口而不是加个布尔：两者的前提不同 —— dict_add 要
 * 调用方保证键不在（只有刚清过表的批量加载满足），本函数自己负责这件事。
 * 合在一起会让「不查重」这个快路径变成一个默认会被走错的开关。
 * @param exists 输出：1 = 键本来就在（记录没动），0 = 本次真插入了；可为 NULL
 * @return 记录（新插入的，或既有的那条），失败 NULL
 */
static inline kv_node_t *dict_add_or_find(const char *key, uint32_t key_len,
                                          const char *val, uint32_t val_len,
                                          int *exists) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     return hash_add_or_find(key, key_len, val, val_len, exists);
    case KV_ENGINE_ARRAY:    return array_add_or_find(key, key_len, val, val_len, exists);
    case KV_ENGINE_RBTREE:   return rb_add_or_find(key, key_len, val, val_len, exists);
    case KV_ENGINE_SKIPLIST: return skip_add_or_find(key, key_len, val, val_len, exists);
    }
    return NULL;
}

/** 查找记录，未找到返回 NULL */
static inline kv_node_t *dict_find(const char *key, uint32_t key_len) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     return hash_find(key, key_len);
    case KV_ENGINE_ARRAY:    return array_find(key, key_len);
    case KV_ENGINE_RBTREE:   return rb_find(key, key_len);
    case KV_ENGINE_SKIPLIST: return skip_find(key, key_len);
    }
    return NULL;
}

/** 按 key 删除；0 成功，-1 未找到 */
static inline int dict_delete(const char *key, uint32_t key_len) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     return hash_delete(key, key_len);
    case KV_ENGINE_ARRAY:    return array_delete(key, key_len);
    case KV_ENGINE_RBTREE:   return rb_delete(key, key_len);
    case KV_ENGINE_SKIPLIST: return skip_delete(key, key_len);
    }
    return -1;
}

/** 调整记录 payload 长度（一律重建）；返回新记录，旧指针作废 */
static inline kv_node_t *dict_resize_entry(kv_node_t *e,
                                           uint32_t new_val_len) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     return hash_resize_entry(e, new_val_len);
    case KV_ENGINE_ARRAY:    return array_resize_entry(e, new_val_len);
    case KV_ENGINE_RBTREE:   return rb_resize_entry(e, new_val_len);
    case KV_ENGINE_SKIPLIST: return skip_resize_entry(e, new_val_len);
    }
    return NULL;
}

/** 当前记录数 */
static inline uint32_t dict_count(void) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     return hash_count();
    case KV_ENGINE_ARRAY:    return array_count();
    case KV_ENGINE_RBTREE:   return rb_count();
    case KV_ENGINE_SKIPLIST: return skip_count();
    }
    return 0;
}

/** 随机取一条记录（过期采样），空表返回 NULL */
static inline kv_node_t *dict_get_random_key(void) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     return hash_get_random_key();
    case KV_ENGINE_ARRAY:    return array_get_random_key();
    case KV_ENGINE_RBTREE:   return rb_get_random_key();
    case KV_ENGINE_SKIPLIST: return skip_get_random_key();
    }
    return NULL;
}

/** 建迭代器（已定位到第一条） */
static inline kv_iter_t *dict_iter_new(void) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     return hash_iter_new();
    case KV_ENGINE_ARRAY:    return array_iter_new();
    case KV_ENGINE_RBTREE:   return rb_iter_new();
    case KV_ENGINE_SKIPLIST: return skip_iter_new();
    }
    return NULL;
}

/** 取当前记录并前进，无更多返回 NULL */
static inline kv_node_t *dict_iter_next(kv_iter_t *it) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     return hash_iter_next(it);
    case KV_ENGINE_ARRAY:    return array_iter_next(it);
    case KV_ENGINE_RBTREE:   return rb_iter_next(it);
    case KV_ENGINE_SKIPLIST: return skip_iter_next(it);
    }
    return NULL;
}

/** 释放迭代器 */
static inline void dict_iter_free(kv_iter_t *it) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     hash_iter_free(it);     break;
    case KV_ENGINE_ARRAY:    array_iter_free(it);    break;
    case KV_ENGINE_RBTREE:   rb_iter_free(it);       break;
    case KV_ENGINE_SKIPLIST: skip_iter_free(it);     break;
    }
}

/*=============================================================================
 *  节点字段访问器
 *
 *  节点布局各引擎不同，上层只认这 6 个函数。名字与原来 kv_node.h 里的访问器
 *  保持一致（entry_key / entry_payload / entry_payload_len 三个调用点一个字
 *  都不用改），只是从「直接读字段」变成了路由。
 *
 *  没有 entry_hash：hash 是索引关注点而不是记录关注点 —— 只有哈希引擎自己
 *  缓存它（桶定位要用），红黑树 / 跳表 / 数组的节点里连字段都不存在，外面
 *  也没有任何读者。键的哈希自此完全是哈希引擎的内部事务。
 *============================================================================*/

/** 节点内联的 key 指针（非 \0 终止） */
static inline const char *entry_key(kv_node_t *e) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     return hash_key(e);
    case KV_ENGINE_ARRAY:    return array_key(e);
    case KV_ENGINE_RBTREE:   return rb_key(e);
    case KV_ENGINE_SKIPLIST: return skip_key(e);
    }
    return NULL;
}

/** key 长度 */
static inline uint32_t entry_key_len(kv_node_t *e) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     return hash_key_len(e);
    case KV_ENGINE_ARRAY:    return array_key_len(e);
    case KV_ENGINE_RBTREE:   return rb_key_len(e);
    case KV_ENGINE_SKIPLIST: return skip_key_len(e);
    }
    return 0;
}

/** payload 指针（非 \0 终止） */
static inline char *entry_payload(kv_node_t *e) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     return hash_val(e);
    case KV_ENGINE_ARRAY:    return array_val(e);
    case KV_ENGINE_RBTREE:   return rb_val(e);
    case KV_ENGINE_SKIPLIST: return skip_val(e);
    }
    return NULL;
}

/** payload 长度 */
static inline uint32_t entry_payload_len(kv_node_t *e) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     return hash_val_len(e);
    case KV_ENGINE_ARRAY:    return array_val_len(e);
    case KV_ENGINE_RBTREE:   return rb_val_len(e);
    case KV_ENGINE_SKIPLIST: return skip_val_len(e);
    }
    return 0;
}

/** 过期时间戳（毫秒，0 = 永不过期） */
static inline int64_t entry_expire(kv_node_t *e) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     return hash_expire(e);
    case KV_ENGINE_ARRAY:    return array_expire(e);
    case KV_ENGINE_RBTREE:   return rb_expire(e);
    case KV_ENGINE_SKIPLIST: return skip_expire(e);
    }
    return 0;
}

/** 写过期时间戳（引擎自己不读它，纯横切元数据） */
static inline void entry_set_expire(kv_node_t *e, int64_t v) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     hash_set_expire(e, v);     break;
    case KV_ENGINE_ARRAY:    array_set_expire(e, v);    break;
    case KV_ENGINE_RBTREE:   rb_set_expire(e, v);       break;
    case KV_ENGINE_SKIPLIST: skip_set_expire(e, v);     break;
    }
}

#endif /* __KV_ENGINE_H__ */
