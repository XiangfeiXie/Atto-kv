/*=============================================================================
 *  kv_rbtree.c — 红黑树引擎
 *
 *  功能：按 key 排序的红黑树（CLRS 第三版实现，带哨兵 nil）
 *  设计特点：
 *  1. 查找/插入/删除都是 O(log n)，且遍历天然按 key 有序
 *  2. 索引节点与记录合并成一块内存 —— 每条记录一次分配（原来是 32B 索引
 *     节点 + 32B 记录、两次 malloc）。旋转只改指针，节点地址恒不变
 *  3. 记录尺寸变化一律重建（见 kv_pool.h 的池契约）
 *
 *  哨兵：所有「空」指针（叶子的左右孩子、根的父指针）都指向同一个 nil
 *  节点，于是旋转/修复路径上不必到处判 NULL。nil 恒为黑色。
 *
 *  节点是本引擎私有类型，对上层只是一个 void *（见 kv_rbtree.h）。
 *  节点里没有 hash 字段 —— 红黑树按 key 有序查找，哈希值对它是多余的。
 *============================================================================*/

#include "kv_rbtree.h"
#include "kv_key.h"
#include "kv_pool.h"
#include <stdlib.h>
#include <string.h>

/*=============================================================================
 *  节点
 *
 *  内存布局（一次整块分配）：
 *  +--------------+------------------+------------------+
 *  | rb_node_t 头 | key 数据 (N 字节) | payload 数据 (M 字节) |
 *  +--------------+------------------+------------------+
 *
 *  块尺寸恒 == sizeof(rb_node_t) + key_len + val_len，node_free 据此还原
 *  分配尺寸（kv_pool.h 的池契约）。哨兵 nil 复用同一个类型但只是个空壳
 *  （key_len = val_len = 0），且固定在底座上分配、不进池 —— 见 nil_new。
 *============================================================================*/

typedef struct rb_node_s {
    struct rb_node_s *l;
    struct rb_node_s *r;
    struct rb_node_s *p;        /* 父指针（迭代取后继 / 删除修复都要用） */
    int32_t           red;      /* 1=红，0=黑 */
    int64_t           expire;   /* 过期时间戳（毫秒，0=永不过期）；引擎自己不读 */
    uint32_t          key_len;
    uint32_t          val_len;
    char              data[];   /* 柔性数组：[key][payload] */
} rb_node_t;                    /* sizeof = 48（含 red 之后的 4B 对齐填充） */

/**
 * 红黑树
 * 字段说明：
 *  - root：根（指向 nil 表示空树）
 *  - nil：哨兵节点
 *  - used：当前记录数
 */
typedef struct {
    rb_node_t *root;
    rb_node_t *nil;
    uint32_t   used;
} rb_store_t;

static rb_store_t g_rb;   /* 红黑树引擎的单例表 */

/*=============================================================================
 *  节点分配（本引擎自己管内存）
 *============================================================================*/

/** 分配并填好一个节点；key/payload 为 NULL 时只造空壳（哨兵用） */
static rb_node_t *node_new(const char *key, uint32_t key_len,
                           const char *payload, uint32_t payload_len)
{
    uint32_t total = (uint32_t)sizeof(rb_node_t) + key_len + payload_len;
    rb_node_t *n = (rb_node_t *)kv_pool_alloc((size_t)total);
    if (unlikely(!n)) return NULL;

    /* 叶子必须指向哨兵而不是 NULL —— 旋转与修复路径只判「是不是 nil」，
     * 留 NULL 会让 rotate_left 里的 `y->l != nil → y->l->p = x` 解空指针 */
    n->l = g_rb.nil;
    n->r = g_rb.nil;
    n->p = g_rb.nil;
    n->red = 1;   /* 新节点一律先染红，由插入修复调整 */
    n->expire  = 0;
    n->key_len = key_len;
    n->val_len = payload_len;

    if (key && key_len)         memcpy(n->data, key, key_len);
    if (payload && payload_len) memcpy(n->data + key_len, payload, payload_len);
    return n;
}

/** 释放节点：尺寸从头部推导（kv_pool.h 的池契约） */
static void node_free(rb_node_t *n) {
    uint32_t total = (uint32_t)sizeof(rb_node_t) + n->key_len + n->val_len;
    kv_pool_free(n, (size_t)total);
}

/**
 * 哨兵不走池，永远用底座 malloc/free
 *
 * 它不是记录（key_len = val_len = 0，没有内容），池的档位/契约对它没有意义；
 * 更要紧的是生命周期不对称：nil 在 rb_init 时造，而池是 kvstore 启动稍后
 * 才绑定的 —— 若 nil 走池，`rb_clear` 里那次释放就会拿一个「绑定前的
 * malloc 指针」去调 kv_pool_free。固定 48B 的一个对象，不值得冒这个险。
 */
static rb_node_t *nil_new(void) {
    rb_node_t *n = (rb_node_t *)malloc(sizeof(rb_node_t));
    if (!n) return NULL;
    n->l = n->r = n->p = n;
    n->red    = 0;   /* 哨兵恒黑 */
    n->expire = 0;
    n->key_len = 0;
    n->val_len = 0;
    return n;
}

/*=============================================================================
 *  字段访问器（上层唯一的读/写途径）
 *============================================================================*/

const char *rb_key(void *n)     { return ((rb_node_t *)n)->data; }
uint32_t    rb_key_len(void *n) { return ((rb_node_t *)n)->key_len; }
char       *rb_val(void *n) {
    rb_node_t *x = (rb_node_t *)n;
    return x->data + x->key_len;
}
uint32_t rb_val_len(void *n) { return ((rb_node_t *)n)->val_len; }
int64_t  rb_expire(void *n)  { return ((rb_node_t *)n)->expire; }
void     rb_set_expire(void *n, int64_t v) { ((rb_node_t *)n)->expire = v; }

/*=============================================================================
 *  生命周期管理
 *============================================================================*/

void rb_init(void) {
    g_rb.nil  = nil_new();     /* 固定形状的空壳，不走池：见 nil_new */
    g_rb.root = g_rb.nil;
    g_rb.used = 0;
}

/** 后序释放整棵子树（树高 O(log n)，递归深度有界） */
static void free_subtree(rb_node_t *x) {
    if (x == g_rb.nil) return;
    free_subtree(x->l);
    free_subtree(x->r);
    node_free(x);
}

/**
 * 全清：释放所有节点与记录，并重建哨兵
 * 记录全释放后归还空超块给底座，理由同哈希引擎（见 kv_hash.c:hash_clear）
 */
void rb_clear(void) {
    if (!g_rb.nil) { rb_init(); return; }   /* 从未初始化过 */

    free_subtree(g_rb.root);
    free(g_rb.nil);            /* 与 nil_new 配对（不走池） */
    rb_init();
    kv_pool_release_all();
}

/**
 * 预分配：红黑树没有数组可预留（容量随节点自然增长），
 * 只需满足「作用在非空表上先全清」的契约（rdma.c 的全量加载靠它去重）
 */
void rb_reserve(uint32_t count) {
    (void)count;
    rb_clear();
}

/*=============================================================================
 *  旋转与查找骨架
 *============================================================================*/

/** 左旋：x 的右孩子 y 上浮，x 下沉为 y 的左孩子 */
static void rotate_left(rb_node_t *x) {
    rb_node_t *y = x->r;
    x->r = y->l;
    if (y->l != g_rb.nil) y->l->p = x;
    y->p = x->p;
    if (x->p == g_rb.nil)        g_rb.root = y;
    else if (x == x->p->l)       x->p->l = y;
    else                         x->p->r = y;
    y->l = x;
    x->p = y;
}

/** 右旋：x 的左孩子 y 上浮，x 下沉为 y 的右孩子 */
static void rotate_right(rb_node_t *x) {
    rb_node_t *y = x->l;
    x->l = y->r;
    if (y->r != g_rb.nil) y->r->p = x;
    y->p = x->p;
    if (x->p == g_rb.nil)        g_rb.root = y;
    else if (x == x->p->r)       x->p->r = y;
    else                         x->p->l = y;
    y->r = x;
    x->p = y;
}

/** 从根往下按 key 找节点，未找到返回哨兵 */
static rb_node_t *find_node(const char *key, uint32_t key_len) {
    rb_node_t *x = g_rb.root;
    while (x != g_rb.nil) {
        int c = kv_key_cmp(key, key_len, x->data, x->key_len);
        if (c == 0) return x;
        x = (c < 0) ? x->l : x->r;
    }
    return g_rb.nil;
}

/** 子树最小节点（最左） */
static rb_node_t *subtree_min(rb_node_t *x) {
    while (x->l != g_rb.nil) x = x->l;
    return x;
}

/** 中序后继：右子树存在则取右子树最左，否则沿父指针上溯 */
static rb_node_t *successor(rb_node_t *x) {
    if (x->r != g_rb.nil) return subtree_min(x->r);
    rb_node_t *y = x->p;
    while (y != g_rb.nil && x == y->r) { x = y; y = y->p; }
    return y;
}

void *rb_find(const char *key, uint32_t key_len) {
    if (unlikely(!g_rb.nil)) return NULL;

    rb_node_t *n = find_node(key, key_len);
    return (n != g_rb.nil) ? n : NULL;
}

/*=============================================================================
 *  插入（CLRS 13.3）
 *============================================================================*/

/** 用 v 顶替 u 的位置（v 可以是哨兵，所以也要回填 p 指针） */
static void transplant(rb_node_t *u, rb_node_t *v) {
    if (u->p == g_rb.nil)      g_rb.root = v;
    else if (u == u->p->l)     u->p->l = v;
    else                       u->p->r = v;
    v->p = u->p;
}

/** 插入修复：红红相连时靠变色与旋转恢复性质 */
static void insert_fixup(rb_node_t *z) {
    while (z->p->red) {
        if (z->p == z->p->p->l) {
            rb_node_t *y = z->p->p->r;          /* 叔节点 */
            if (y->red) {                       /* 叔红：变色上溯 */
                z->p->red = 0;
                y->red    = 0;
                z->p->p->red = 1;
                z = z->p->p;
            } else {
                if (z == z->p->r) {             /* 内侧：先左旋掰直 */
                    z = z->p;
                    rotate_left(z);
                }
                z->p->red    = 0;
                z->p->p->red = 1;
                rotate_right(z->p->p);
            }
        } else {                                /* 镜像 */
            rb_node_t *y = z->p->p->l;
            if (y->red) {
                z->p->red = 0;
                y->red    = 0;
                z->p->p->red = 1;
                z = z->p->p;
            } else {
                if (z == z->p->l) {
                    z = z->p;
                    rotate_right(z);
                }
                z->p->red    = 0;
                z->p->p->red = 1;
                rotate_left(z->p->p);
            }
        }
    }
    g_rb.root->red = 0;   /* 根恒黑 */
}

/** 新增记录（不查重，直接挂到叶子下）；失败 NULL */
void *rb_add(const char *key, uint32_t key_len,
             const char *payload, uint32_t payload_len)
{
    if (unlikely(!g_rb.nil)) return NULL;

    /* 一次分配就是一条记录（索引节点与记录同块），没有半成品可回滚 */
    rb_node_t *z = node_new(key, key_len, payload, payload_len);
    if (unlikely(!z)) return NULL;

    /* 按 key 找到插入位置（先走到叶子，再挂上去） */
    rb_node_t *y = g_rb.nil;
    rb_node_t *x = g_rb.root;
    int c = 0;
    while (x != g_rb.nil) {
        y = x;
        c = kv_key_cmp(z->data, z->key_len, x->data, x->key_len);
        x = (c < 0) ? x->l : x->r;
    }

    z->p = y;
    if (y == g_rb.nil)   g_rb.root = z;   /* 空树 */
    else if (c < 0)      y->l = z;
    else                 y->r = z;

    insert_fixup(z);
    g_rb.used++;
    return z;
}

/**
 * 新增记录，键已存在则不插入（SET 专用）
 *
 * 查重就是一趟普通查找，没命中再走 add —— 两步各下一次行，加起来与改造前
 * 的「先 find 再 add」完全一样。本引擎按 key 有序定位，没有哈希可省，也就
 * 没有把两者揉成一次下行的必要（哈希引擎才有）。
 * @param exists 输出：1 表示键本来就在（记录没动），0 表示本次真插入了；可为 NULL
 */
void *rb_add_or_find(const char *key, uint32_t key_len,
                     const char *payload, uint32_t payload_len, int *exists)
{
    if (exists) *exists = 0;

    void *hit = rb_find(key, key_len);
    if (hit) {
        if (exists) *exists = 1;
        return hit;
    }
    return rb_add(key, key_len, payload, payload_len);
}

/*=============================================================================
 *  删除（CLRS 13.4）
 *============================================================================*/

/** 删除修复：x 带着一重额外的黑，靠变色与旋转把它推上去 */
static void delete_fixup(rb_node_t *x) {
    while (x != g_rb.root && !x->red) {
        if (x == x->p->l) {
            rb_node_t *w = x->p->r;              /* 兄弟 */
            if (w->red) {                        /* 兄弟红 → 转成兄弟黑 */
                w->red   = 0;
                x->p->red = 1;
                rotate_left(x->p);
                w = x->p->r;
            }
            if (!w->l->red && !w->r->red) {      /* 兄弟两孩子都黑：变色上溯 */
                w->red = 1;
                x = x->p;
            } else {
                if (!w->r->red) {                /* 兄弟右孩子黑 → 先右旋 */
                    w->l->red = 0;
                    w->red    = 1;
                    rotate_right(w);
                    w = x->p->r;
                }
                w->red    = x->p->red;
                x->p->red = 0;
                w->r->red = 0;
                rotate_left(x->p);
                x = g_rb.root;                   /* 收工 */
            }
        } else {                                 /* 镜像 */
            rb_node_t *w = x->p->l;
            if (w->red) {
                w->red   = 0;
                x->p->red = 1;
                rotate_right(x->p);
                w = x->p->l;
            }
            if (!w->r->red && !w->l->red) {
                w->red = 1;
                x = x->p;
            } else {
                if (!w->l->red) {
                    w->r->red = 0;
                    w->red    = 1;
                    rotate_left(w);
                    w = x->p->l;
                }
                w->red    = x->p->red;
                x->p->red = 0;
                w->l->red = 0;
                rotate_right(x->p);
                x = g_rb.root;
            }
        }
    }
    x->red = 0;
}

/**
 * 把节点 z 从树上摘掉
 *
 * 两个孩子都在时，物理搬走的是 z 的中序后继 y（把 y 挪到 z 的位置），
 * 这样树序自然保持 —— 返回的仍是 z，由调用方释放它（连同它的记录）
 */
static rb_node_t *detach_node(rb_node_t *z) {
    rb_node_t *y = z;
    rb_node_t *x;
    int        y_red = y->red;

    if (z->l == g_rb.nil) {
        x = z->r;
        transplant(z, z->r);
    } else if (z->r == g_rb.nil) {
        x = z->l;
        transplant(z, z->l);
    } else {
        y = subtree_min(z->r);
        y_red = y->red;
        x = y->r;
        if (y->p == z) {
            x->p = y;            /* x 可能就是哨兵，也要认对父 */
        } else {
            transplant(y, y->r);
            y->r = z->r;
            y->r->p = y;
        }
        transplant(z, y);
        y->l = z->l;
        y->l->p = y;
        y->red = z->red;
    }

    if (!y_red) delete_fixup(x);   /* 搬走的是黑节点才需要修复 */
    return z;
}

int rb_delete(const char *key, uint32_t key_len) {
    if (unlikely(!g_rb.nil)) return -1;

    rb_node_t *n = find_node(key, key_len);
    if (n == g_rb.nil) return -1;   /* 未找到 */

    rb_node_t *z = detach_node(n);
    node_free(z);                   /* 节点即记录，一次释放 */
    g_rb.used--;
    return 0;
}

/**
 * 重建节点以改变 payload 长度
 *
 * 尺寸一改就重建（缩小也不原地），理由见 kv_pool.h 的池契约。
 * key 没变 ⇒ 新节点在树上的位置与旧节点完全相同，把四条连线原样接过去即可：
 * 左右孩子的父指针、父节点的孩子指针（或根）。
 *
 * 陷阱：p == nil 表示旧节点就是根，此时只能改 g_rb.root —— nil 是四个引擎
 * 之外唯一被共享的哨兵，**绝不能**写 nil->l / nil->r，那会把整个空树认成
 * 有子节点。
 */
void *rb_resize_entry(void *n, uint32_t new_payload_len) {
    rb_node_t *old = (rb_node_t *)n;
    if (new_payload_len == old->val_len)
        return old;  /* 长度不变，直接返回 */

    /* payload 不拷贝 —— 按契约由调用方随后整段写入 */
    rb_node_t *ne = node_new(old->data, old->key_len, NULL, new_payload_len);
    if (unlikely(!ne)) return NULL;
    ne->expire = old->expire;   /* expire 沿用旧值 */

    /* 接手旧节点的四条连线 */
    ne->l   = old->l;
    ne->r   = old->r;
    ne->p   = old->p;
    ne->red = old->red;

    if (ne->l != g_rb.nil) ne->l->p = ne;
    if (ne->r != g_rb.nil) ne->r->p = ne;
    if (ne->p == g_rb.nil)      g_rb.root = ne;        /* 旧节点是根 */
    else if (ne->p->l == old)   ne->p->l = ne;
    else                        ne->p->r = ne;

    node_free(old);
    return ne;
}

uint32_t rb_count(void) {
    return g_rb.used;
}

/**
 * 随机取一条记录（用于过期采样）
 *
 * 树没有桶，只能中序走到第 rand()%used 个 —— O(n)。active expire 本身
 * 带时间预算与抽样上限（见 kvstore_expire.c），且过期默认关闭，demo 够用。
 */
void *rb_get_random_key(void) {
    if (g_rb.used == 0) return NULL;

    uint32_t k = (uint32_t)rand() % g_rb.used;
    rb_node_t *x = subtree_min(g_rb.root);
    while (k-- > 0 && x != g_rb.nil) x = successor(x);
    return (x != g_rb.nil) ? x : NULL;
}

/*=============================================================================
 *  迭代器
 *  从中序第一个出发，一路走后继 ⇒ 天然按 key 有序
 *  句柄本身就是游标（返回 void *），一次 malloc、一次 free
 *============================================================================*/

typedef struct {
    rb_node_t *cur;   /* 下一条待返回的节点（哨兵表示结束） */
} rb_iter_t;

void *rb_iter_new(void) {
    rb_iter_t *c = (rb_iter_t *)malloc(sizeof(rb_iter_t));
    if (!c) return NULL;

    c->cur = g_rb.nil ? subtree_min(g_rb.root) : NULL;   /* nil 未建时退化 */
    return c;
}

void *rb_iter_next(void *it) {
    rb_iter_t *c = (rb_iter_t *)it;
    if (!c->cur || c->cur == g_rb.nil) return NULL;
    rb_node_t *n = c->cur;
    c->cur = successor(n);
    return n;
}

void rb_iter_free(void *it) {
    free(it);
}
