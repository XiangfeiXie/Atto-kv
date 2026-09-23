/*=============================================================================
 *  kv_engine.c — 存储引擎路由层
 *
 *  本文件只有「选哪个引擎」这一件事：枚举的定义与选择。全部 dict_* / entry_*
 *  路由是 kv_engine.h 里的 static inline（见该文件头）。
 *
 *  记录分配器原本也在这里，但那是反的 —— 四个引擎要调它，等于引擎依赖路由层，
 *  单个引擎文件没法脱离路由层编译。现已改为各引擎自己管自己节点的内存（在各自
 *  的 .c 里），只共用「记录从哪个池取」这一份状态，见 kv_pool.h。
 *============================================================================*/

#include "kv_engine.h"
#include <strings.h>   /* strcasecmp：引擎名不区分大小写 */

kv_engine_t g_kv_engine = KV_ENGINE_HASH;   /* 默认哈希引擎 */

int kv_engine_select(const char *name) {
    if      (strcasecmp(name, "hash")     == 0) g_kv_engine = KV_ENGINE_HASH;
    else if (strcasecmp(name, "array")    == 0) g_kv_engine = KV_ENGINE_ARRAY;
    else if (strcasecmp(name, "rbtree")   == 0) g_kv_engine = KV_ENGINE_RBTREE;
    else if (strcasecmp(name, "skiplist") == 0) g_kv_engine = KV_ENGINE_SKIPLIST;
    else return -1;
    return 0;
}

const char *kv_engine_name(void) {
    switch (g_kv_engine) {
    case KV_ENGINE_HASH:     return "hash";
    case KV_ENGINE_ARRAY:    return "array";
    case KV_ENGINE_RBTREE:   return "rbtree";
    case KV_ENGINE_SKIPLIST: return "skiplist";
    }
    return "unknown";
}
