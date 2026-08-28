/*=============================================================================
 *  rdma_transport.h — RDMA 整块流式传输层（照搬 kvs_rdma.c 的机制）
 *
 *  机制：
 *   - 客户端(slave 连接发起方)注册一块接收缓冲，把 {addr,rkey} 发给服务端；
 *   - 服务端(master 监听方)把整份快照序列化进本地暂存块，按 CHUNK 拆分、
 *     以滑动窗口流水线 RDMA_WRITE 直写对端内存；最后一个分块挂
 *     WRITE_WITH_IMM 作为完成通知；
 *   - 客户端等到该 IMM 时整份数据已在自己的缓冲里，就地加载。
 *
 *  单块大缓冲、无 ACK 流控（RC 保序可靠 + SQ 窗口
 *  排空即安全）、连接走 librdmacm 标准流程。快照上限 = RDMA_BUF_SIZE，
 *  demo 定位下足够且换取最简单的实现。
 *
 *  两个实测经验固化在实现里（勿删）：
 *   1. rdma_connect/accept 必须显式 memset+赋值 conn_param（NULL 时字段
 *      内容不定，某些进程状态下会被对端 REJ_INVALID_RETRY_COUNT）
 *   2. 服务端发完必须 poll 干净全部 send_cqe 才能断连销毁，
 *      否则排队中的分块会被丢弃、对端永远等不到尾部 IMM
 *============================================================================*/

#ifndef __RDMA_TRANSPORT_H__
#define __RDMA_TRANSPORT_H__

#include <stdint.h>
#include <stddef.h>

/*=============================================================================
 *  传输参数
 *============================================================================*/

#define RDMA_BUF_SIZE       (256ULL * 1024 * 1024) /* 收发缓冲大小=快照上限 */
#define RDMA_CHUNK_SIZE     (4u << 20)             /* 单次 WRITE 分块 */
#define RDMA_QP_DEPTH       16                     /* 滑动窗口深度 */
#define RDMA_QP_SEND_WR     (RDMA_QP_DEPTH * 2)    /* SQ 深度 */
#define RDMA_QP_RECV_WR     16

typedef struct rdma_xport rdma_xport_t;

/*=============================================================================
 *  连接管理
 *============================================================================*/

/**
 * 服务端：监听 → 接受连接 → 收到对端缓冲凭证后返回
 *
 * @param ip 监听地址（NULL 表示全接口）
 * @param port 监听端口
 * @param ready_fd 监听就绪管道 fd（写 1 字节通知父进程），-1 表示无
 * @return 传输句柄，失败返回 NULL
 */
rdma_xport_t *rdma_xport_server_accept(const char *ip, int port, int ready_fd);

/**
 * 客户端：连接 → 注册接收缓冲（远程写目标）→ 把缓冲凭证发给服务端
 *
 * @param ip 服务端地址
 * @param port 服务端端口
 * @return 传输句柄，失败返回 NULL
 */
rdma_xport_t *rdma_xport_client_connect(const char *ip, int port);

/**
 * 服务端：取发送暂存缓冲基址（容量恒为 RDMA_BUF_SIZE）
 * 数据块组织（含协议头）由上层负责，随后整块交给 rdma_xport_send
 */
void *rdma_xport_send_buf(rdma_xport_t *x, size_t *capacity);

/**
 * 客户端：取接收缓冲基址（等 rdma_xport_wait_done 成功后才允许读取）
 */
void *rdma_xport_recv_buf(rdma_xport_t *x);

/**
 * 释放传输句柄（含连接、MR、缓冲）
 */
void rdma_xport_destroy(rdma_xport_t *x);

/*=============================================================================
 *  数据面
 *============================================================================*/

/**
 * 服务端：把 [buf, len) 整块按窗口流水线直写对端缓冲，尾部带 IMM 通知
 *
 * @return 0 成功（含本地全部完成的排空），-1 失败
 */
int rdma_xport_send(rdma_xport_t *x, const void *buf, size_t len);

/**
 * 客户端：阻塞等待尾部 IMM 完成（此后整个缓冲内容就绪；
 * 实际字节数由上层从数据块协议头读取）
 *
 * @return 0 成功，-1 失败
 */
int rdma_xport_wait_done(rdma_xport_t *x);

#endif /* __RDMA_TRANSPORT_H__ */
