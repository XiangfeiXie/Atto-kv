/*=============================================================================
 *  rbuf.c — 节点队列读缓冲实现
 *
 *  数据结构：一串固定大小节点（单链队列）。
 *    - 生产：reserve 暴露一段连续可写空间（队尾节点剩余空间，或池中一个
 *      新节点）→ recv / io_uring_prep_recv 直接灌，commit 把实际字节入队；
 *      没用上的节点自动回池。
 *    - 消费：只认逻辑字节流。contig 拿连续段、consume 按字节吃（整段回池）、
 *      copy 任意逻辑偏移跨段读。节点边界对消费者不可见。
 *
 *  节点生命周期：alloc 时 malloc 一次；整段消费完回 free-list 复用；
 *  destroy 时统一释放。
 *============================================================================*/

#include "rbuf.h"

#include <stdlib.h>
#include <string.h>

struct rbuf_node {
    rbuf_node_t *next;      /* 链指针：队列链 / 空闲链共用（节点同一时刻只在一处） */
    uint32_t     len;       /* 有效字节数 */
    uint32_t     cap;       /* 容量（= node_cap，留便于调试） */
    char         data[];    /* 数据（柔性数组，紧随头部） */
};

/*-------------------------------------------------------------------------
 *  节点池
 *-------------------------------------------------------------------------*/

static rbuf_node_t *node_alloc(rbuf_t *rb) {
    rbuf_node_t *n = malloc(sizeof(*n) + rb->node_cap);
    if (!n) return NULL;
    n->next = NULL;
    n->len  = 0;
    n->cap  = rb->node_cap;
    return n;
}

/* 取一个节点：优先回池复用，池空才新分配 */
static rbuf_node_t *pool_pop(rbuf_t *rb) {
    rbuf_node_t *n = rb->free;
    if (!n)
        return node_alloc(rb);
    rb->free = n->next;
    n->next = NULL;
    n->len  = 0;    /* 复位：可能来自已满节点 */
    return n;
}

static void pool_push(rbuf_t *rb, rbuf_node_t *n) {
    n->next = rb->free;
    n->len  = 0;
    rb->free = n;
}

/*-------------------------------------------------------------------------
 *  生命周期
 *-------------------------------------------------------------------------*/

int rbuf_init(rbuf_t *rb, uint32_t node_cap) {
    memset(rb, 0, sizeof(*rb));
    rb->node_cap = node_cap ? node_cap : RBUF_NODE_SIZE;
    return 0;
}

void rbuf_destroy(rbuf_t *rb) {
    rbuf_node_t *n, *nx;

    /* 队列链 */
    for (n = rb->head; n; n = nx) { nx = n->next; free(n); }
    /* 在途保留节点（尚未入队，不在任何链上） */
    if (rb->resv_node) free(rb->resv_node);
    /* 空闲链 */
    for (n = rb->free; n; n = nx) { nx = n->next; free(n); }
    memset(rb, 0, sizeof(*rb));
}

/*-------------------------------------------------------------------------
 *  生产侧
 *-------------------------------------------------------------------------*/

int rbuf_reserve(rbuf_t *rb, char **base, uint32_t *cap) {
    /* 1) 队尾还有剩余空间：直接给它，不取新节点 */
    if (rb->tail && rb->tail->len < rb->tail->cap) {
        uint32_t f = rb->tail->cap - rb->tail->len;
        *base = rb->tail->data + rb->tail->len;
        *cap  = f;
        rb->resv_node = NULL;            /* NULL = 保留的是队尾剩余空间 */
        rb->resv_cap  = f;
        return 1;
    }

    /* 2) 队尾写满或为空：从池取一个新节点 */
    rbuf_node_t *nd = pool_pop(rb);
    if (!nd) {                           /* 池空且分配失败：OOM */
        rb->resv_node = NULL;            /* 显式复位：不留上一次的保留态 */
        rb->resv_cap  = 0;
        return 0;
    }
    *base = nd->data;
    *cap  = rb->node_cap;
    rb->resv_node = nd;
    rb->resv_cap  = rb->node_cap;
    return 1;
}

void rbuf_commit(rbuf_t *rb, int got) {
    /* 防御：负值（误把 recv 错误当结果提交）按 0 处理，保留段整体回池。
     * resv_cap 同时是"有无在途保留"的判据 —— 无保留时它把 n 钳成 0，
     * 本函数据此退化为 no-op（多余的 commit 不会凭空计数）。 */
    uint32_t n = (got < 0) ? 0u : (uint32_t)got;
    if (n > rb->resv_cap)
        n = rb->resv_cap;

    if (rb->resv_node == NULL) {
        /* 保留的是队尾剩余空间：填进去。n 已 ≤ 保留时的剩余容量，这里再按
         * 当前剩余重算钳制 —— 契约若被破坏也只安全丢弃，不越界写坏账本。 */
        if (n && rb->tail) {
            uint32_t f = rb->tail->cap - rb->tail->len;
            if (f > n) f = n;
            rb->tail->len += f;
            rb->len       += f;
        }
    } else {
        rbuf_node_t *nd = rb->resv_node;
        rb->resv_node = NULL;            /* 先摘除：下面两条路都已把它消化掉 */
        if (n == 0) {
            pool_push(rb, nd);           /* 没用上：回池 */
        } else {
            nd->len  = n;                /* n ≤ node_cap */
            nd->next = NULL;
            if (rb->tail)
                rb->tail->next = nd;
            else
                rb->head = nd;
            rb->tail = nd;
            rb->len += n;
        }
    }
    rb->resv_cap = 0;
}

/*-------------------------------------------------------------------------
 *  消费侧
 *-------------------------------------------------------------------------*/

uint32_t rbuf_len(const rbuf_t *rb) {
    return rb->len;
}

uint32_t rbuf_copy(const rbuf_t *rb, uint32_t off, void *dst, uint32_t len) {
    if (off >= rb->len)
        return 0;
    if (len > rb->len - off)
        len = rb->len - off;

    /* 先定位逻辑 off 落在哪个节点 */
    const rbuf_node_t *nd  = rb->head;
    uint32_t           pos = rb->head_off;   /* 当前在 nd 内的字节下标 */
    uint32_t           skip = off;
    while (nd && skip) {
        uint32_t avail = nd->len - pos;
        if (skip < avail) { pos += skip; skip = 0; break; }
        skip -= avail;
        nd = nd->next;
        pos = 0;
    }
    if (skip)
        return 0;                            /* off 越过数据尾（不该发生） */

    /* 逐段拷贝 */
    char *out = (char *)dst;
    uint32_t copied = 0;
    while (nd && copied < len) {
        uint32_t avail = nd->len - pos;
        uint32_t f = avail < (len - copied) ? avail : (len - copied);
        memcpy(out + copied, nd->data + pos, f);
        copied += f;
        pos += f;
        if (pos == nd->len) { nd = nd->next; pos = 0; }
    }
    return copied;
}

uint32_t rbuf_contig(const rbuf_t *rb, uint32_t off, const char **base) {
    if (off >= rb->len)
        return 0;

    /* 定位逻辑 off 落在哪个节点（跳段逻辑与 rbuf_copy 同构，仅不拷贝） */
    const rbuf_node_t *nd  = rb->head;
    uint32_t           pos = rb->head_off;   /* 当前在 nd 内的字节下标 */
    uint32_t           skip = off;
    while (nd && skip) {
        uint32_t avail = nd->len - pos;
        if (skip < avail) { pos += skip; skip = 0; break; }
        skip -= avail;
        nd = nd->next;
        pos = 0;
    }
    if (skip)
        return 0;                            /* off 越过数据尾（off < len 时不该发生） */

    *base = nd->data + pos;
    return nd->len - pos;                    /* 自 off 起到本节点数据末端连续可读 */
}

void rbuf_consume(rbuf_t *rb, uint32_t n) {
    if (n > rb->len)
        n = rb->len;

    while (n && rb->head) {
        uint32_t avail = rb->head->len - rb->head_off;
        uint32_t f = avail < n ? avail : n;
        rb->head_off += f;
        rb->len      -= f;
        n            -= f;

        if (rb->head_off == rb->head->len) {   /* 整段吃完 → 回池 */
            rbuf_node_t *nx = rb->head->next;
            pool_push(rb, rb->head);
            rb->head = nx;
            rb->head_off = 0;
            if (!rb->head)
                rb->tail = NULL;
        }
    }
}

void rbuf_reset(rbuf_t *rb) {
    /* 在途保留节点既不在队列也不在空闲链上，必须先归还 ——
     * 连接槽位复用路径（conn_release 保留整个 rbuf_t 后重进本函数）若不清理，
     * 该节点会在下次 reserve 覆盖 resv_node 后永久泄漏。 */
    if (rb->resv_node) {
        pool_push(rb, rb->resv_node);
        rb->resv_node = NULL;
    }
    rb->resv_cap = 0;

    rbuf_node_t *nd = rb->head;
    while (nd) {
        rbuf_node_t *nx = nd->next;
        pool_push(rb, nd);
        nd = nx;
    }
    rb->head = NULL;
    rb->tail = NULL;
    rb->head_off = 0;
    rb->len      = 0;
}
