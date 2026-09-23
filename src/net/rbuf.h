/*=============================================================================
 *  rbuf.h — 节点队列读缓冲（网络读缓冲的存储层）
 *
 *  目标：替换网络层现用的"单块连续 DynBuf + r_off + memmove 压缩"读缓冲，
 *  解决缓冲物理布局账本被网络层/解析层各记一半的混乱。
 *
 *  设计：
 *  1. 数据存放在一串固定大小节点里（链表队列）；生产灌尾部、消费吃头部。
 *  2. 节点内部分段消费，整段吃完自动回空闲池（free-list，热路径零 malloc）。
 *  3. 消费侧只认"逻辑字节流"：contig 取连续段、consume 按字节吃、
 *     copy 可跨段任意偏移读取——节点边界对消费者不可见。
 *  4. 生产侧一次只暴露**一段**连续可写空间：段 = "队尾节点剩余空间，或池中
 *     一个新节点"，两者只给其一。三个引擎（reactor / ntyco / proactor）统一
 *     走这条路——单缓冲读（recv / io_uring_prep_recv）一次只能灌一段连续内存，
 *     语义严丝合缝。实际收到的字节数由 commit 入队，没用上的节点自动回池。
 *
 *  职责边界：
 *   - 本模块只管"读缓冲的存储与队列语义"，不做协议解析。
 *   - 写缓冲继续用 DynBuf（wbuff），本模块不涉及。
 *
 *  使用约定：
 *   - reserve() 之后必须 commit()（哪怕 0 字节）配对，期间不得调用其它接口。
 *   - 节点大小 init 时定死（默认 RBUF_NODE_SIZE=1KB）。
 *============================================================================*/

#ifndef KVS_RBUF_H
#define KVS_RBUF_H

#include <stdint.h>

/* 默认节点大小（字节） */
#define RBUF_NODE_SIZE  1024u

typedef struct rbuf_node rbuf_node_t;   /* 节点内部布局在 .c，本头只见不透明指针 */

typedef struct rbuf {
    uint32_t      node_cap;             /* 每节点容量（init 定死） */

    /* 队列（消费在 head，生产在 tail；单链） */
    rbuf_node_t  *head, *tail;          /* 队头 / 队尾节点 */
    uint32_t      head_off;             /* head 节点已消费前缀（仅头可有部分消费） */
    uint32_t      len;                  /* 逻辑未消费字节数 = Σnode.len − head_off */

    /* 空闲池（用完回池复用，池空才新分配） */
    rbuf_node_t  *free;                 /* 空闲链 */

    /* 生产保留段（reserve → commit 之间临时记账，未入队）。
     * resv_node 的 NULL/非 NULL 编码两义，靠 resv_cap 区分"有在途保留"与"空闲"：
     *   resv_node==NULL && resv_cap>0   → 保留的是队尾剩余空间
     *   resv_node==NULL && resv_cap==0  → 空闲，无在途保留
     *   resv_node!=NULL                 → 保留的是该新节点
     * resv_cap 不可省：多余的 commit 必须被它钳成 0 而成为 no-op，
     * 否则会按当前队尾剩余空间凭空计数，静默写坏数据流。 */
    rbuf_node_t  *resv_node;            /* 本次 reserve 取出的新节点 */
    uint32_t      resv_cap;             /* 本次 reserve 的可写容量；0 = 无在途保留 */
} rbuf_t;

/**
 * 初始化读缓冲
 * @param node_cap 节点大小（0 → 默认 RBUF_NODE_SIZE）
 */
int  rbuf_init(rbuf_t *rb, uint32_t node_cap);

/** 释放所有节点与内部状态（含在途保留节点） */
void rbuf_destroy(rbuf_t *rb);

/*-------------------------------------------------------------------------
 *  生产侧（网络层）：reserve 取一段可写空间 → recv 灌 → commit 入队
 *-------------------------------------------------------------------------*/

/**
 * 取一段连续可写空间（只读填充目标，未入队）
 * 段 = 现有队尾节点的剩余空间；队尾写满或为空时 = 从池新取的一个节点。
 * @param base 输出：可写起始地址
 * @param cap  输出：可写字节数（成功时恒 > 0）
 * @return 1 成功；0 池空且新节点分配失败（OOM）
 * 约定：随后必须 commit()（可传 0）配对
 */
int  rbuf_reserve(rbuf_t *rb, char **base, uint32_t *cap);

/**
 * 提交实际写入的字节数（recv 返回值）
 * @param got 实际写入字节数（须 ≤ 上次 reserve 的 cap；负值按 0 处理）
 */
void rbuf_commit(rbuf_t *rb, int got);

/*-------------------------------------------------------------------------
 *  消费侧（协议层）：只认逻辑字节流，不关心节点物理布局
 *-------------------------------------------------------------------------*/

/** 逻辑未消费字节数 */
uint32_t rbuf_len(const rbuf_t *rb);

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

/** 清空缓冲，节点全部回池（含在途保留节点） */
void rbuf_reset(rbuf_t *rb);

#endif /* KVS_RBUF_H */
