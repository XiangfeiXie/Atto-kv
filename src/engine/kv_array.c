/*=============================================================================
 *  kv_array.c — 数组引擎
 *
 *  功能：一条挨一条摆放的定容槽位数组（array_node_t *slots[cap]）
 *  设计特点：
 *  1. 槽位永无空洞 —— 删除用「尾部记录补位」，因此 used 恒等于真实记录数
 *  2. 插入固定是 slots[used++] 一步，查找/改尺寸/迭代都是一段紧凑顺序扫描
 *  3. 容量满（used == cap）时一次性翻倍
 *  4. 记录尺寸变化一律重建（见 kv_pool.h 的池契约）
 *
 *  相比哈希引擎：没有链指针、没有桶取模，但查找是 O(n) 线性扫描。
 *  demo 定位：一个「最笨但一定对」的对照引擎。
 *
 *  节点是本引擎私有类型，对上层只是一个 void *（见 kv_array.h）。
 *============================================================================*/

#include "kv_array.h"
#include "kv_key.h"
#include "kv_pool.h"
#include <stdlib.h>
#include <string.h>

#define ARRAY_INIT_CAP  16          /* 初始槽位数（必须是 2 的幂） */
#define ARRAY_MAX_CAP   (1U << 24)  /* 槽位上限 ~16M */

/*=============================================================================
 *  节点
 *
 *  内存布局（一次整块分配）：
 *  +-----------------+------------------+------------------+
 *  | array_node_t 头 | key 数据 (N 字节) | payload 数据 (M 字节) |
 *  +-----------------+------------------+------------------+
 *
 *  比哈希节点少一个 next、少一个 hash（数组靠槽位下标定位，判等直接比
 *  key 本身），16B + 载荷。块尺寸恒 == sizeof(array_node_t) + key_len +
 *  val_len，node_free 据此还原分配尺寸（kv_pool.h 的池契约）。
 *============================================================================*/

typedef struct array_node_s {
    int64_t  expire;   /* 过期时间戳（毫秒，0=永不过期）；引擎自己不读 */
    uint32_t key_len;
    uint32_t val_len;
    char     data[];   /* 柔性数组：[key][payload] */
} array_node_t;

/**
 * 槽位数组
 * 字段说明：
 *  - slots：槽位数组（每格一条记录，无空洞）
 *  - cap：槽位总数（必须是 2 的幂）
 *  - used：当前记录数，也是槽位的使用水位
 */
typedef struct {
    array_node_t **slots;
    uint32_t       cap;
    uint32_t       used;
} array_store_t;

static array_store_t g_array;   /* 数组引擎的单例表 */

/*=============================================================================
 *  节点分配（本引擎自己管内存）
 *============================================================================*/

static array_node_t *node_new(const char *key, uint32_t key_len,
                              const char *payload, uint32_t payload_len)
{
    uint32_t total = (uint32_t)sizeof(array_node_t) + key_len + payload_len;
    array_node_t *e = (array_node_t *)kv_pool_alloc((size_t)total);
    if (unlikely(!e)) return NULL;

    e->expire  = 0;
    e->key_len = key_len;
    e->val_len = payload_len;

    if (key && key_len)
        memcpy(e->data, key, key_len);
    if (payload && payload_len)
        memcpy(e->data + key_len, payload, payload_len);

    return e;
}

/** 释放节点：尺寸从头部推导（kv_pool.h 的池契约） */
static void node_free(array_node_t *e) {
    uint32_t total = (uint32_t)sizeof(array_node_t) + e->key_len + e->val_len;
    kv_pool_free(e, (size_t)total);
}

/*=============================================================================
 *  字段访问器（上层唯一的读/写途径）
 *============================================================================*/

const char *array_key(void *n)     { return ((array_node_t *)n)->data; }
uint32_t    array_key_len(void *n) { return ((array_node_t *)n)->key_len; }
char       *array_val(void *n) {
    array_node_t *e = (array_node_t *)n;
    return e->data + e->key_len;
}
uint32_t array_val_len(void *n) { return ((array_node_t *)n)->val_len; }
int64_t  array_expire(void *n)  { return ((array_node_t *)n)->expire; }
void     array_set_expire(void *n, int64_t v) { ((array_node_t *)n)->expire = v; }

/*=============================================================================
 *  表分配 / 生命周期管理
 *============================================================================*/

/** 分配槽位数组（cap 为 2 的幂） */
static int slots_alloc(uint32_t cap) {
    g_array.cap   = cap;
    g_array.used  = 0;
    g_array.slots = (array_node_t **)calloc((size_t)cap, sizeof(array_node_t *));
    return g_array.slots ? 0 : -1;
}

void array_init(void) {
    g_array.slots = NULL;
    g_array.cap   = 0;
    g_array.used  = 0;
}

/**
 * 全清：释放所有记录与槽位数组
 * 记录全释放后归还空超块给底座，理由同哈希引擎（见 kv_hash.c:hash_clear）
 */
void array_clear(void) {
    if (!g_array.slots) return;

    for (uint32_t i = 0; i < g_array.used; i++)
        node_free(g_array.slots[i]);

    free(g_array.slots);
    array_init();
    kv_pool_release_all();
}

/** 预分配槽位（批量加载前调用）；非空表先全清 */
void array_reserve(uint32_t count) {
    if (g_array.slots) array_clear();
    uint32_t cap = kv_next_pow2(count, ARRAY_INIT_CAP);
    if (cap > ARRAY_MAX_CAP) cap = ARRAY_MAX_CAP;
    slots_alloc(cap);
}

/*=============================================================================
 *  扩容
 *  与哈希引擎同样是一次性扩容（非渐进），理由相同：单线程、插入路径无分支
 *============================================================================*/

/**
 * 检查并执行扩容：used == cap 时翻倍；首个 add 惰性建表
 * @return 0 成功（含无需扩容），-1 失败（OOM 或已达 ARRAY_MAX_CAP）
 *
 * 「已达上限」算失败而不是无位可扩地返回成功 —— 后者会把「没得扩」和
 * 「扩成功」混成同一个返回值，逼调用方在 add 里再查一遍水位。
 */
static int maybe_grow(void) {
    if (g_array.cap == 0) return slots_alloc(ARRAY_INIT_CAP);
    if (g_array.used < g_array.cap) return 0;      /* 仍有空槽 */
    if (g_array.cap >= ARRAY_MAX_CAP) return -1;   /* 已到上限，拒收 */

    uint32_t new_cap = g_array.cap * 2;
    if (new_cap > ARRAY_MAX_CAP) new_cap = ARRAY_MAX_CAP;

    array_node_t **ns =
        (array_node_t **)realloc(g_array.slots,
                                 (size_t)new_cap * sizeof(array_node_t *));
    if (!ns) return -1;

    /* 新槽位清零，旧记录原地保留（槽位下标不变，无需搬动） */
    memset(ns + g_array.cap, 0,
           (size_t)(new_cap - g_array.cap) * sizeof(array_node_t *));
    g_array.slots = ns;
    g_array.cap   = new_cap;
    return 0;
}

/*=============================================================================
 *  核心操作
 *============================================================================*/

/** 顺序扫描找 key 所在槽位（先用 key 长度拒掉绝大多数槽位）；未找到返回 used */
static uint32_t slots_find(const char *key, uint32_t key_len) {
    for (uint32_t i = 0; i < g_array.used; i++) {
        array_node_t *e = g_array.slots[i];
        if (e->key_len == key_len &&
            kv_fast_cmp(e->data, key, (int)key_len) == 0)
            return i;
    }
    return g_array.used;   /* 不是合法下标，即「没有」 */
}

/** 新增记录（不查重，直接追加在尾部）；失败 NULL */
void *array_add(const char *key, uint32_t key_len,
                const char *payload, uint32_t payload_len)
{
    if (unlikely(g_array.used >= g_array.cap)) {
        if (maybe_grow() < 0) return NULL;   /* OOM 或已满 */
    }

    array_node_t *e = node_new(key, key_len, payload, payload_len);
    if (!e) return NULL;

    g_array.slots[g_array.used++] = e;   /* 直接追加在尾部 */
    return e;
}

/** 查找：复用上面的扫描，未找到返回 NULL */
void *array_find(const char *key, uint32_t key_len) {
    uint32_t i = slots_find(key, key_len);
    return (i < g_array.used) ? g_array.slots[i] : NULL;
}

/**
 * 新增记录，键已存在则不插入（SET 专用）
 *
 * 本引擎没有哈希索引，查重只能顺序扫描 —— 这一趟省不掉，所以写成「先 find
 * 再 add」两步，而不是像哈希引擎那样把两者揉进一次桶定位里。
 * @param exists 输出：1 表示键本来就在（记录没动），0 表示本次真插入了；可为 NULL
 */
void *array_add_or_find(const char *key, uint32_t key_len,
                        const char *payload, uint32_t payload_len, int *exists)
{
    if (exists) *exists = 0;

    void *hit = array_find(key, key_len);
    if (hit) {
        if (exists) *exists = 1;
        return hit;
    }
    return array_add(key, key_len, payload, payload_len);
}

/**
 * 删除：找到槽位后释放，再把尾部记录搬进来补位
 *
 * 补位而不是留空洞，是为了让 used 恒等于记录数——插入、随机采样、迭代
 * 都不必再处理空洞。代价是删除会打乱顺序（本来也无序）。
 */
int array_delete(const char *key, uint32_t key_len) {
    uint32_t i = slots_find(key, key_len);
    if (i == g_array.used) return -1;   /* 未找到 */

    node_free(g_array.slots[i]);
    g_array.slots[i] = g_array.slots[--g_array.used];   /* 尾部补位 */
    g_array.slots[g_array.used] = NULL;
    return 0;
}

/**
 * 重建节点以改变 payload 长度
 * 尺寸一改就重建（缩小也不原地），理由见 kv_pool.h 的池契约。
 * 槽位下标不变、key 也没变 ⇒ 新节点直接写回原槽位，接回工作只有一行。
 */
void *array_resize_entry(void *n, uint32_t new_payload_len) {
    array_node_t *e = (array_node_t *)n;
    if (new_payload_len == e->val_len)
        return e;  /* 长度不变，直接返回 */

    /* 按记录地址找槽位（句柄是节点指针，槽位下标只能这样反查） */
    uint32_t idx = g_array.used;
    for (uint32_t i = 0; i < g_array.used; i++) {
        if (g_array.slots[i] == e) { idx = i; break; }
    }
    if (idx == g_array.used) return NULL;   /* 理论上不应发生 */

    /* payload 不拷贝 —— 按契约由调用方随后整段写入 */
    array_node_t *ne = node_new(e->data, e->key_len, NULL, new_payload_len);
    if (unlikely(!ne)) return NULL;
    ne->expire = e->expire;   /* expire 沿用旧值 */

    g_array.slots[idx] = ne;   /* 槽位下标不变 */
    node_free(e);
    return ne;
}

uint32_t array_count(void) {
    return g_array.used;
}

/** 随机取一条记录（用于过期采样）：槽位无空洞，一次取模即可 */
void *array_get_random_key(void) {
    if (g_array.used == 0) return NULL;
    return g_array.slots[rand() % g_array.used];
}

/*=============================================================================
 *  迭代器
 *  游标只是一个槽位下标；无空洞 ⇒ 顺序扫到 used 即结束
 *  句柄本身就是游标（返回 void *），一次 malloc、一次 free
 *============================================================================*/

typedef struct {
    uint32_t idx;   /* 下一条待返回的槽位下标 */
} array_iter_t;

void *array_iter_new(void) {
    array_iter_t *c = (array_iter_t *)malloc(sizeof(array_iter_t));
    if (!c) return NULL;

    c->idx = 0;
    return c;
}

void *array_iter_next(void *it) {
    array_iter_t *c = (array_iter_t *)it;
    if (c->idx >= g_array.used) return NULL;
    return g_array.slots[c->idx++];
}

void array_iter_free(void *it) {
    free(it);
}
