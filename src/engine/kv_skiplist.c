/*=============================================================================
 *  kv_skiplist.c — 跳表引擎
 *
 *  功能：多层有序链表，层高按 1/2 概率逐层上升
 *  设计特点：
 *  1. 查找/插入/删除期望 O(log n)，且实现比红黑树短得多
 *  2. 索引节点与记录合并成一块内存 —— 每条记录一次分配（原来是 8·lvl 的
 *     索引节点 + 32B 记录、两次 malloc）。插删只改 forward 指针，节点地址
 *     恒不变
 *  3. 记录尺寸变化一律重建（见 kv_pool.h 的池契约）
 *
 *  头节点（header）不存记录，只作为每层链表的起点，固定 SKIP_MAXLEVEL 层。
 *
 *  节点是本引擎私有类型，对上层只是一个 void *（见 kv_skiplist.h）。
 *  节点里没有 hash 字段 —— 跳表按 key 有序查找，哈希值对它是多余的。
 *============================================================================*/

#include "kv_skiplist.h"
#include "kv_key.h"
#include "kv_pool.h"
#include <stdlib.h>
#include <string.h>

#define SKIP_MAXLEVEL  32    /* 最大层数：1/2 概率上升，32 层可撑 2^32 条 */

/*=============================================================================
 *  节点
 *
 *  这是四个引擎里唯一不能把载荷直接放进柔性数组的：C 要求柔性数组必须是
 *  最后一个成员，而 forward[] 又必须留在最后（它的元素个数随 lvl 变）。
 *  于是改成「forward[] 之后接着放 key/payload」——同一块内存里数组之后
 *  的那片普通字节，用 skip_key() 算地址：
 *
 *  +-------------+-------------------+------------------+------------------+
 *  | skip_node_t | forward[0..lvl-1] | key 数据 (N 字节) | payload (M 字节) |
 *  +-------------+-------------------+------------------+------------------+
 *
 *  不需要任何对齐取整：sizeof(skip_node_t) = 24 是 8 的倍数，而块基址
 *  至少 16 对齐（malloc 与 kv_pool 都是），所以 forward 必然 8 对齐。
 *
 *  块尺寸恒 == sizeof(skip_node_t) + lvl·8 + key_len + val_len，node_free
 *  据此还原分配尺寸（kv_pool.h 的池契约）。
 *============================================================================*/

typedef struct skip_node_s {
    int64_t  expire;   /* 过期时间戳（毫秒，0=永不过期）；引擎自己不读 */
    uint32_t lvl;      /* 本节点的层高（key 的位置要靠它算，故必须存下来） */
    uint32_t key_len;
    uint32_t val_len;
    struct skip_node_s *forward[];   /* forward[0..lvl-1]：各层的后继 */
} skip_node_t;                       /* sizeof = 24（val_len 之后 4B 对齐填充） */

/** key 紧跟在 forward[] 数组之后；非 \0 终止 */
static inline char *skip_key_of(skip_node_t *n) {
    return (char *)(n->forward + n->lvl);
}

/** payload 紧跟在 key 之后 */
static inline char *skip_val_of(skip_node_t *n) {
    return skip_key_of(n) + n->key_len;
}

/**
 * 跳表
 * 字段说明：
 *  - header：头节点（固定 SKIP_MAXLEVEL 层，不存记录）
 *  - level：当前有效层数（1..SKIP_MAXLEVEL）
 *  - used：当前记录数
 */
typedef struct {
    skip_node_t *header;
    uint32_t     level;
    uint32_t     used;
} skip_store_t;

static skip_store_t g_skip;   /* 跳表引擎的单例表 */

/*=============================================================================
 *  节点分配（本引擎自己管内存）
 *============================================================================*/

/** 分配并填好一个 lvl 层的节点；key/payload 为 NULL 时只造空壳（头节点用） */
static skip_node_t *node_new(uint32_t lvl, const char *key, uint32_t key_len,
                             const char *payload, uint32_t payload_len)
{
    uint32_t total = (uint32_t)sizeof(skip_node_t)
                   + lvl * (uint32_t)sizeof(skip_node_t *)
                   + key_len + payload_len;
    skip_node_t *n = (skip_node_t *)kv_pool_alloc((size_t)total);
    if (unlikely(!n)) return NULL;

    n->expire  = 0;
    n->lvl     = lvl;
    n->key_len = key_len;
    n->val_len = payload_len;
    memset(n->forward, 0, (size_t)lvl * sizeof(skip_node_t *));   /* 各层后继置空 */

    if (key && key_len)         memcpy(skip_key_of(n), key, key_len);
    if (payload && payload_len) memcpy(skip_val_of(n), payload, payload_len);
    return n;
}

/** 释放节点：尺寸从头部推导（kv_pool.h 的池契约） */
static void node_free(skip_node_t *n) {
    uint32_t total = (uint32_t)sizeof(skip_node_t)
                   + n->lvl * (uint32_t)sizeof(skip_node_t *)
                   + n->key_len + n->val_len;
    kv_pool_free(n, (size_t)total);
}

/**
 * 头节点不走池，永远用底座 malloc/free
 *
 * 它不是记录（key_len = val_len = 0，没有内容），池的档位/契约对它没有意义；
 * 更要紧的是生命周期不对称：header 在 skip_init 时造，而池是 kvstore 启动
 * 稍后才绑定的 —— 若 header 走池，`skip_clear` 里那次释放就会拿一个「绑定
 * 前的 malloc 指针」去调 kv_pool_free。
 */
static skip_node_t *header_new(void) {
    uint32_t total = (uint32_t)sizeof(skip_node_t)
                   + SKIP_MAXLEVEL * (uint32_t)sizeof(skip_node_t *);
    skip_node_t *n = (skip_node_t *)malloc((size_t)total);
    if (!n) return NULL;

    n->expire  = 0;
    n->lvl     = SKIP_MAXLEVEL;
    n->key_len = 0;
    n->val_len = 0;
    memset(n->forward, 0, (size_t)SKIP_MAXLEVEL * sizeof(skip_node_t *));
    return n;
}

/*=============================================================================
 *  字段访问器（上层唯一的读/写途径）
 *============================================================================*/

const char *skip_key(void *n) { return skip_key_of((skip_node_t *)n); }
uint32_t    skip_key_len(void *n) { return ((skip_node_t *)n)->key_len; }
char       *skip_val(void *n) { return skip_val_of((skip_node_t *)n); }
uint32_t    skip_val_len(void *n) { return ((skip_node_t *)n)->val_len; }
int64_t     skip_expire(void *n)  { return ((skip_node_t *)n)->expire; }
void        skip_set_expire(void *n, int64_t v) { ((skip_node_t *)n)->expire = v; }

/** 随机层高：1/2 概率上升一层，返 1..SKIP_MAXLEVEL */
static uint32_t random_level(void) {
    uint32_t lvl = 1;
    while (lvl < SKIP_MAXLEVEL && (rand() & 1)) lvl++;
    return lvl;
}

/*=============================================================================
 *  生命周期管理
 *============================================================================*/

void skip_init(void) {
    g_skip.header = header_new();
    g_skip.level  = 1;
    g_skip.used   = 0;
}

/**
 * 全清：释放所有节点与记录
 * 记录全释放后归还空超块给底座，理由同哈希引擎（见 kv_hash.c:hash_clear）
 */
void skip_clear(void) {
    if (g_skip.header) {
        /* 层 0 就是完整的有序链表，顺着它走一遍即可 */
        skip_node_t *x = g_skip.header->forward[0];
        while (x) {
            skip_node_t *nx = x->forward[0];
            node_free(x);            /* 节点即记录，一次释放 */
            x = nx;
        }
        free(g_skip.header);         /* 与 header_new 配对（不走池） */
    }
    skip_init();
    kv_pool_release_all();
}

/**
 * 预分配：跳表没有表结构可预留（容量靠随机层自然增长），
 * 只需满足「作用在非空表上先全清」的契约（rdma.c 的全量加载靠它去重）
 */
void skip_reserve(uint32_t count) {
    (void)count;
    skip_clear();
}

/*=============================================================================
 *  查找（所有操作的公共骨架）
 *============================================================================*/

/**
 * 从最高层往下找 key 的插入位置
 * @param upd 非 NULL 时回填每层的「前驱节点」，供插入/删除/重连用
 * @return 第一个 >= key 的节点（可能是 NULL），即候选命中节点
 */
static skip_node_t *search_node(const char *key, uint32_t key_len,
                                skip_node_t **upd)
{
    skip_node_t *x = g_skip.header;

    for (int i = (int)g_skip.level - 1; i >= 0; i--) {
        while (x->forward[i] &&
               kv_key_cmp(skip_key_of(x->forward[i]),
                          x->forward[i]->key_len, key, key_len) < 0)
            x = x->forward[i];
        if (upd) upd[i] = x;
    }
    return x->forward[0];
}

void *skip_find(const char *key, uint32_t key_len) {
    if (unlikely(!g_skip.header)) return NULL;

    skip_node_t *n = search_node(key, key_len, NULL);
    if (n && kv_key_eq(skip_key_of(n), n->key_len, key, key_len)) return n;
    return NULL;
}

/*=============================================================================
 *  增删改
 *============================================================================*/

/** 新增记录（不查重，直接逐层接入）；失败 NULL */
void *skip_add(const char *key, uint32_t key_len,
               const char *payload, uint32_t payload_len)
{
    if (unlikely(!g_skip.header)) return NULL;

    skip_node_t *upd[SKIP_MAXLEVEL];
    search_node(key, key_len, upd);   /* 回填每层前驱 */

    /* 层高超过当前层数时，多出来的层以前驱 = 头节点接入 */
    uint32_t lvl = random_level();
    if (lvl > g_skip.level) {
        for (uint32_t i = g_skip.level; i < lvl; i++) upd[i] = g_skip.header;
        g_skip.level = lvl;
    }

    /* 一次分配就是一条记录（索引节点与记录同块），没有半成品可回滚 */
    skip_node_t *n = node_new(lvl, key, key_len, payload, payload_len);
    if (unlikely(!n)) return NULL;

    /* 逐层把自己接在前驱之后 */
    for (uint32_t i = 0; i < lvl; i++) {
        n->forward[i]     = upd[i]->forward[i];
        upd[i]->forward[i] = n;
    }
    g_skip.used++;
    return n;
}

/**
 * 新增记录，键已存在则不插入（SET 专用）
 *
 * 查重就是一趟普通查找，没命中再走 add —— 两步各下一次行，加起来与改造前
 * 的「先 find 再 add」完全一样。本引擎按 key 有序定位，没有哈希可省，也就
 * 没有把两者揉成一次下行的必要（哈希引擎才有）。
 * @param exists 输出：1 表示键本来就在（记录没动），0 表示本次真插入了；可为 NULL
 */
void *skip_add_or_find(const char *key, uint32_t key_len,
                       const char *payload, uint32_t payload_len, int *exists)
{
    if (exists) *exists = 0;

    void *hit = skip_find(key, key_len);
    if (hit) {
        if (exists) *exists = 1;
        return hit;
    }
    return skip_add(key, key_len, payload, payload_len);
}

int skip_delete(const char *key, uint32_t key_len) {
    if (unlikely(!g_skip.header)) return -1;

    skip_node_t *upd[SKIP_MAXLEVEL];
    skip_node_t *x = search_node(key, key_len, upd);
    if (!x || !kv_key_eq(skip_key_of(x), x->key_len, key, key_len))
        return -1;   /* 未找到 */

    /* 逐层摘除：某层前驱的后继不是 x 就说明 x 没到那一层，可以停 */
    for (uint32_t i = 0; i < g_skip.level; i++) {
        if (upd[i]->forward[i] != x) break;
        upd[i]->forward[i] = x->forward[i];
    }
    node_free(x);    /* 节点即记录，一次释放 */
    g_skip.used--;

    /* 顶层空了就降层，避免空层拖慢后续查找 */
    while (g_skip.level > 1 && !g_skip.header->forward[g_skip.level - 1])
        g_skip.level--;
    return 0;
}

/**
 * 重建节点以改变 payload 长度
 *
 * 尺寸一改就重建（缩小也不原地），理由见 kv_pool.h 的池契约。
 * key 没变 ⇒ 层高与每层前驱都不变：拿 search_node 回填的 upd 把新节点按
 * 原来的层数逐层接回去即可，等价于「原样插入一次」。
 */
void *skip_resize_entry(void *n, uint32_t new_payload_len) {
    skip_node_t *old = (skip_node_t *)n;
    if (new_payload_len == old->val_len)
        return old;  /* 长度不变，直接返回 */

    skip_node_t *upd[SKIP_MAXLEVEL];
    skip_node_t *cur = search_node(skip_key_of(old), old->key_len, upd);
    if (unlikely(cur != old)) return NULL;   /* 理论上不应发生 */

    /* payload 不拷贝 —— 按契约由调用方随后整段写入 */
    skip_node_t *ne = node_new(old->lvl, skip_key_of(old), old->key_len,
                               NULL, new_payload_len);
    if (unlikely(!ne)) return NULL;
    ne->expire = old->expire;   /* expire 沿用旧值 */

    /* 逐层顶替：前驱的第 i 层改指新节点，新节点接上旧节点的后继 */
    for (uint32_t i = 0; i < old->lvl; i++) {
        ne->forward[i]     = old->forward[i];
        upd[i]->forward[i] = ne;
    }

    node_free(old);
    return ne;
}

uint32_t skip_count(void) {
    return g_skip.used;
}

/**
 * 随机取一条记录（用于过期采样）
 *
 * 跳表没有桶，只能从最底层走 rand()%used 步 —— O(n)。active expire 本身
 * 带时间预算与抽样上限（见 kvstore_expire.c），且过期默认关闭，demo 够用。
 */
void *skip_get_random_key(void) {
    if (g_skip.used == 0) return NULL;

    uint32_t k = (uint32_t)rand() % g_skip.used;
    skip_node_t *x = g_skip.header->forward[0];
    while (k-- && x) x = x->forward[0];
    return x;
}

/*=============================================================================
 *  迭代器
 *  层 0 就是完整有序链表 ⇒ 顺着 forward[0] 走即可（天然按 key 有序）
 *  句柄本身就是游标（返回 void *），一次 malloc、一次 free
 *============================================================================*/

typedef struct {
    skip_node_t *cur;   /* 下一条待返回的节点 */
} skip_iter_t;

void *skip_iter_new(void) {
    skip_iter_t *c = (skip_iter_t *)malloc(sizeof(skip_iter_t));
    if (!c) return NULL;

    c->cur = g_skip.header ? g_skip.header->forward[0] : NULL;
    return c;
}

void *skip_iter_next(void *it) {
    skip_iter_t *c = (skip_iter_t *)it;
    if (!c->cur) return NULL;
    skip_node_t *n = c->cur;
    c->cur = n->forward[0];
    return n;
}

void skip_iter_free(void *it) {
    free(it);
}
