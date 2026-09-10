/*=============================================================================
 *  rbuf.h — 节点队列读缓冲（网络读缓冲的存储层）
 *
 *  目标：替换网络层现用的"单块连续 DynBuf + r_off + memmove 压缩"读缓冲，
 *  解决缓冲物理布局账本被网络层/解析层各记一半的混乱。
 *
 *  设计：
 *  1. 数据存放在一串固定大小节点里（链表队列）；生产灌尾部、消费吃头部。
 *  2. 节点内部分段消费，整段吃完自动回空闲池（free-list，热路径零 malloc）。
 *  3. 消费侧只认"逻辑字节流"：peek 取当前连续段、consume 按字节吃、
 *     copy 可跨段任意偏移读取——节点边界对消费者不可见。
 *  4. 生产侧暴露 struct iovec[]，recvmsg / io_uring_recvmsg 一次灌多段；
 *     实际收到的字节数由 commit 入队，未用上的段自动回池。
 *
 *  职责边界：
 *   - 本模块只管"读缓冲的存储与队列语义"，不做协议解析。
 *   - 写缓冲继续用 DynBuf（wbuff），本模块不涉及。
 *
 *  使用约定：
 *   - reserve() 之后必须 commit()（哪怕 0 字节）配对，期间不得调用其它接口。
 *   - 节点大小 init 时定死（默认 RBUF_NODE_SIZE=1KB）；测试可用小值压跨段。
 *============================================================================*/

#ifndef KVS_RBUF_H
#define KVS_RBUF_H

#include <stdint.h>
#include <sys/uio.h>

/* 默认节点大小（字节）。测试初始化时可传更小的 node_cap 强制跨段 */
#define RBUF_NODE_SIZE  1024u

/* 单次 reserve 最多暴露的 iovec 段数（≈ 一个 recvmsg 最多灌满的节点数） */
#define RBUF_MAX_IOV    16u

typedef struct rbuf_node rbuf_node_t;   /* 节点内部布局在 .c，本头只见不透明指针 */

typedef struct rbuf {
    uint32_t      node_cap;             /* 每节点容量（init 定死） */

    /* 队列（消费在 head，生产在 tail；单链） */
    rbuf_node_t  *head, *tail;          /* 队头 / 队尾节点 */
    uint32_t      head_off;             /* head 节点已消费前缀（仅头可有部分消费） */
    uint32_t      len;                  /* 逻辑未消费字节数 = Σnode.len − head_off */

    /* 空闲池（用完回池复用，池空才新分配） */
    rbuf_node_t  *free;                 /* 空闲链 */
    uint32_t      pool_alloc;           /* 累计分配节点数（观测：稳态应不涨） */
    uint32_t      pool_free;            /* 空闲节点数 */

    /* 生产保留段（reserve → commit 之间临时记账，未入队） */
    rbuf_node_t  *resv_head, *resv_tail;/* 本次 reserve 从池取出的新节点链 */
    uint32_t      resv_tail_free;       /* 其中落在现有 tail 上的可写字节（iov[0]） */
    uint32_t      resv_cap;             /* 保留段总可写容量 */
} rbuf_t;

/**
 * 初始化读缓冲
 * @param node_cap 节点大小（0 → 默认 RBUF_NODE_SIZE；测试传小值压跨段）
 */
int  rbuf_init(rbuf_t *rb, uint32_t node_cap);

/** 释放所有节点与内部状态 */
void rbuf_destroy(rbuf_t *rb);

/*-------------------------------------------------------------------------
 *  生产侧（网络层）：reserve 取可写段 → recvmsg/io_uring 灌 → commit 入队
 *-------------------------------------------------------------------------*/

/**
 * 取可写段（只读填充目标，未入队）
 * 返回的 iov 起点为：现有队尾剩余空间 + 从池新取的节点。
 * @param iov 输出：可写段数组；长度须 ≥ max_iov（引擎惯例：固定
 *        RBUF_MAX_IOV 大小，max_iov 传 RBUF_MAX_IOV）
 * @param max_iov iov 数组容量（内部再截断到 ≤ RBUF_MAX_IOV）
 * @return 段数（可能为 0 = 池空且分配失败）
 * 约定：随后必须 commit()（可传 0）配对
 */
int  rbuf_reserve(rbuf_t *rb, struct iovec *iov, int max_iov);

/**
 * 提交实际写入的字节数（recv 返回值）
 * 填不满时只把用到的段入队，未用的段自动回池。
 * @param got 实际写入字节数（须 ≤ 上次 reserve 的总容量）
 */
void rbuf_commit(rbuf_t *rb, int got);

/**
 * 直接写入 n 字节（测试/非 socket 场景喂数据用，内部走节点分配）
 * @return 0 成功，-1 分配失败（可能已部分写入）
 */
int  rbuf_write(rbuf_t *rb, const void *data, uint32_t n);

/*-------------------------------------------------------------------------
 *  消费侧（协议层）：只认逻辑字节流，不关心节点物理布局
 *-------------------------------------------------------------------------*/

/** 逻辑未消费字节数 */
uint32_t rbuf_len(const rbuf_t *rb);

/**
 * 查看当前队头的连续段（不消费）
 * @param ptr 输出：段内起始指针（指向节点内存）
 * @param n   输出：段内可读字节数
 * @return 1 有数据，0 空
 */
int  rbuf_peek(const rbuf_t *rb, const char **ptr, uint32_t *n);

/**
 * 逻辑读：把 [off, off+len) 拷到 dst（可跨节点；不消费）
 * @return 实际拷出的字节数（越界时截断/为 0）
 */
uint32_t rbuf_copy(const rbuf_t *rb, uint32_t off, void *dst, uint32_t len);

/**
 * 逻辑只读取段：返回自 off 起、在同一节点内可连续读取的字节数
 * （0 = off 已到流尾）；*base 指向该连续区起点（节点内存）。
 * 供分段解析器做前向顺序扫描：段内直接取，越段（读满返回长度）再调本函数
 * 换段。返回的连续区到本节点数据末端为止，与节点边界一致。
 *
 * 注意：仅只读查询，返回的指针在对应节点被 consume 回收前有效；解析期间
 * 不得穿插 reserve/commit（引擎单线程 recv 路径成立）。
 */
uint32_t rbuf_contig(const rbuf_t *rb, uint32_t off, const char **base);

/** 从逻辑头消费 n 字节；整段吃完自动回池 */
void rbuf_consume(rbuf_t *rb, uint32_t n);

/** 清空缓冲，节点全部回池 */
void rbuf_reset(rbuf_t *rb);

/*-------------------------------------------------------------------------
 *  观测（测试/调参）
 *-------------------------------------------------------------------------*/

/** 空闲节点数 */
uint32_t rbuf_pool_free(const rbuf_t *rb);
/** 累计分配节点数（稳态读写后应趋稳不涨） */
uint32_t rbuf_pool_alloc(const rbuf_t *rb);

#endif /* KVS_RBUF_H */
