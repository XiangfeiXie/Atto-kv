#ifndef __RDMA_TRANSPORT_H__
#define __RDMA_TRANSPORT_H__

/**
 * rdma_transport.h — RDMA 传输层
 *
 * 封装 chunk 拆分、滑动窗口流水线、选择性信号、批量 CQ poll。
 * 从 tools/rdma_bw.c 提取核心引擎，适配持久连接 + 多远程 buffer。
 *
 * 设计原则：
 *   - transport 包装已有 rdma_cm_id / QP，不管连接生命周期
 *   - 上层控制 buffer pool、ACK 协议、RDB 序列化
 *   - 调用者仍可直接使用 rdma_post_send/rdma_post_recv 发小消息
 */

#include <stdint.h>
#include <stddef.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

/* ── 可配置常量 ──────────────────────────────────────────────────── */

#define RDMA_XPORT_CHUNK_SIZE    (1024 * 1024)   /* 1 MB per WRITE */
#define RDMA_XPORT_QP_DEPTH      16              /* 滑动窗口深度  */

/* ── 传输句柄 ────────────────────────────────────────────────────── */

struct rdma_xport {
    struct rdma_cm_id *id;   /* 包装的 QP，不拥有所有权 — 上层可直读 */
    int unpolled_wr;         /* 自上次 flush 以来 post 的 WR 总数     */
    int signaled_wr;         /* 其中标记了 SIGNALED 的数量            */
};

typedef struct rdma_xport rdma_xport_t;

/* ── 生命周期 ────────────────────────────────────────────────────── */

/* 包装已有 rdma_cm_id，不取得所有权 */
rdma_xport_t *rdma_xport_create(struct rdma_cm_id *id);
void          rdma_xport_destroy(rdma_xport_t *x);

/* ── 单边写入（master → slave）──────────────────────────────────── */
/*
 * 自动将 len 字节按 CHUNK_SIZE 拆分，以滑动窗口流水线发出。
 *   - 非末尾 chunk: IBV_WR_RDMA_WRITE, 非 SIGNALED
 *   - 末尾 chunk:    IBV_WR_RDMA_WRITE_WITH_IMM + SIGNALED
 *   - 每 QP_DEPTH 个 WR 插入一个 SIGNALED（保证 SQ 不溢出）
 *
 * 参数:
 *   data        本地 MR 缓冲区起始地址
 *   len         发送字节数
 *   lkey        本地 MR lkey
 *   remote_addr 远程目标 buffer 起始地址
 *   rkey        远程 buffer rkey
 *   imm_data    IMM 数据（仅末尾 chunk 携带，htonl 后传入）
 *
 * 返回已 post 的 WR 数（>0），错误返回 -1。
 * 调用者应在每次逻辑批次结束后调用 rdma_xport_flush() 排空 SQ。
 */
int rdma_xport_write(rdma_xport_t *x,
                     const void *data, size_t len, uint32_t lkey,
                     uint64_t remote_addr, uint32_t rkey,
                     uint32_t imm_data);

/* 排空所有未完成的 send CQE，之后本地 buffer 可安全复用 */
int rdma_xport_flush(rdma_xport_t *x);

/* ── CQ poll 原语 ───────────────────────────────────────────────── */

/* 轮询一个 send CQE（阻塞），错误返回 -1 */
int rdma_xport_poll_send(rdma_xport_t *x);

/* 轮询一个 recv CQE（阻塞），填充 wc，错误返回 -1 */
int rdma_xport_poll_recv(rdma_xport_t *x, struct ibv_wc *wc);

#endif /* __RDMA_TRANSPORT_H__ */
