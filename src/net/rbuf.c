/*=============================================================================
 *  rbuf.c — 节点队列读缓冲实现
 *
 *  数据结构：一串固定大小节点（单链队列）。
 *    - 生产：reserve 把"队尾剩余 + 池中新节点"暴露成 iovec[] → recvmsg 直接灌，
 *      commit 把实际字节入队；填不满的段自动回池。
 *    - 消费：只认逻辑字节流。peek 拿当前段、consume 按字节吃（整段回池）、
 *      copy 任意逻辑偏移跨段读。节点边界对消费者不可见。
 *
 *  节点生命周期：alloc 时 malloc 一次；整段消费完回 free-list 复用；
 *  destroy 时统一释放。稳态下（流量平稳）pool_alloc 应不增长。
 *============================================================================*/

#include "rbuf.h"

#include <stdlib.h>
#include <string.h>

struct rbuf_node {
    rbuf_node_t *next;      /* 队列/空闲/保留三段链共用（节点同一时刻只在一段） */
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
    rb->pool_alloc++;
    return n;
}

/* 取一个节点：优先回池复用，池空才新分配 */
static rbuf_node_t *pool_pop(rbuf_t *rb) {
    rbuf_node_t *n = rb->free;
    if (!n)
        return node_alloc(rb);
    rb->free = n->next;
    rb->pool_free--;
    n->next = NULL;
    n->len  = 0;    /* 复位：可能来自已满节点 */
    return n;
}

static void pool_push(rbuf_t *rb, rbuf_node_t *n) {
    n->next = rb->free;
    n->len  = 0;
    rb->free = n;
    rb->pool_free++;
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

    /* 队列链（含保留段中未提交的新节点） */
    for (n = rb->head; n; n = nx) { nx = n->next; free(n); }
    for (n = rb->resv_head; n; n = nx) { nx = n->next; free(n); }
    /* 空闲链 */
    for (n = rb->free; n; n = nx) { nx = n->next; free(n); }
    memset(rb, 0, sizeof(*rb));
}

/*-------------------------------------------------------------------------
 *  生产侧
 *-------------------------------------------------------------------------*/

int rbuf_reserve(rbuf_t *rb, struct iovec *iov, int max_iov) {
    if (max_iov > (int)RBUF_MAX_IOV) max_iov = (int)RBUF_MAX_IOV;
    if (max_iov < 1)                max_iov = 1;

    int      cnt   = 0;
    uint32_t total = 0;

    /* 1) 队尾剩余空间（若队尾节点没写满） */
    uint32_t tail_free = 0;
    if (rb->tail && rb->tail->len < rb->tail->cap) {
        tail_free = rb->tail->cap - rb->tail->len;
        iov[cnt].iov_base = rb->tail->data + rb->tail->len;
        iov[cnt].iov_len  = tail_free;
        total += tail_free;
        cnt++;
    }

    /* 2) 从池取新节点补齐段数 */
    while (cnt < max_iov) {
        rbuf_node_t *nd = pool_pop(rb);
        if (!nd) break;                      /* 分配失败：给多少用多少 */
        if (rb->resv_tail)
            rb->resv_tail->next = nd;
        else
            rb->resv_head = nd;
        rb->resv_tail = nd;
        iov[cnt].iov_base = nd->data;
        iov[cnt].iov_len  = rb->node_cap;
        total += rb->node_cap;
        cnt++;
    }

    rb->resv_tail_free = tail_free;          /* 记下 iov[0] 属队尾多少字节 */
    rb->resv_cap       = total;
    return cnt;
}

void rbuf_commit(rbuf_t *rb, int got) {
    /* 防御：负值（误把 recv 错误当结果提交）按 0 处理，保留段整体回池 */
    uint32_t n = (got < 0) ? 0u : (uint32_t)got;
    if (n > rb->resv_cap)
        n = rb->resv_cap;

    /* 1) 先填队尾剩余（reserve 时暴露的那段） */
    if (n && rb->resv_tail_free) {
        uint32_t f = rb->tail->cap - rb->tail->len;
        if (f > n) f = n;
        rb->tail->len += f;
        rb->len       += f;
        n             -= f;
    }

    /* 2) 逐节点填满保留的新节点并链接入队（尾节点可为部分填充） */
    while (n && rb->resv_head) {
        rbuf_node_t *nd = rb->resv_head;
        rb->resv_head = nd->next;
        uint32_t f = rb->node_cap;
        if (f > n) f = n;
        nd->len  = f;
        nd->next = NULL;
        if (rb->tail)
            rb->tail->next = nd;
        else
            rb->head = nd;
        rb->tail = nd;
        rb->len += f;
        n       -= f;
    }

    /* 3) 没用上的新节点回池 */
    while (rb->resv_head) {
        rbuf_node_t *nd = rb->resv_head;
        rb->resv_head = nd->next;
        pool_push(rb, nd);
    }
    rb->resv_tail  = NULL;
    rb->resv_tail_free = 0;
    rb->resv_cap   = 0;
}

int rbuf_write(rbuf_t *rb, const void *data, uint32_t n) {
    const char *src = (const char *)data;
    uint32_t    rem = n;

    while (rem) {
        /* 队尾写满或空 → 取新节点 */
        if (!rb->tail || rb->tail->len == rb->tail->cap) {
            rbuf_node_t *nd = pool_pop(rb);
            if (!nd) return -1;              /* 分配失败（可能已部分写入） */
            if (rb->tail)
                rb->tail->next = nd;
            else
                rb->head = nd;
            rb->tail = nd;
        }
        uint32_t f = rb->tail->cap - rb->tail->len;
        if (f > rem) f = rem;
        memcpy(rb->tail->data + rb->tail->len, src, f);
        rb->tail->len += f;
        rb->len       += f;
        src += f;
        rem -= f;
    }
    return 0;
}

/*-------------------------------------------------------------------------
 *  消费侧
 *-------------------------------------------------------------------------*/

uint32_t rbuf_len(const rbuf_t *rb) {
    return rb->len;
}

int rbuf_peek(const rbuf_t *rb, const char **ptr, uint32_t *n) {
    if (!rb->head || rb->head_off >= rb->head->len)
        return 0;
    *ptr = rb->head->data + rb->head_off;
    *n   = rb->head->len - rb->head_off;
    return 1;
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

/*-------------------------------------------------------------------------
 *  观测
 *-------------------------------------------------------------------------*/

uint32_t rbuf_pool_free(const rbuf_t *rb) {
    return rb->pool_free;
}

uint32_t rbuf_pool_alloc(const rbuf_t *rb) {
    return rb->pool_alloc;
}
