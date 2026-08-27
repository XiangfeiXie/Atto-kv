/*=============================================================================
 *  rdma_transport.h — RDMA 最小流式传输层
 *
 *  职责：把整块内存一次性流式推给对端（chunk 窗口流水线）
 *  机制照搬 kvs_rdma.c：
 *   - 数据按 CHUNK 拆成多个 RDMA WRITE 连续发出
 *   - QP_DEPTH 滑动窗口回收 CQE，控制 SQ 占用
 *   - 最后一块 WRITE_WITH_IMM 通知对端"整块到齐"
 *   - 无 ACK / 无环形缓冲 / 无流控状态机
 *
 *  单次连接单次传输：全量同步一次连接发一整份快照
 *  上层（rdma.c）负责 RDB 序列化进缓冲 / 从缓冲加载
 *============================================================================*/

#ifndef __RDMA_TRANSPORT_H__
#define __RDMA_TRANSPORT_H__

#include <stdint.h>
#include <stddef.h>

/*=============================================================================
 *  传输参数
 *============================================================================*/

#define RDMA_BUF_SIZE         (256ULL * 1024 * 1024)  /* 单缓冲：快照大小上限 */
#define RDMA_QP_SEND_WR       32                       /* SQ 深度 = 2×窗口 */
#define RDMA_QP_RECV_WR       16
#define RDMA_XPORT_CHUNK_SIZE (1024 * 1024)            /* 单次 WRITE chunk */
#define RDMA_XPORT_QP_DEPTH   16                       /* 滑动窗口深度 */

typedef struct rdma_xport rdma_xport_t;

/*=============================================================================
 *  连接管理
 *============================================================================*/

/**
 * 服务端：监听 → 接受连接 → 收对端缓冲信息（addr/rkey）
 *
 * @param ip 监听地址（NULL 表示全接口）
 * @param port 监听端口
 * @param ready_fd 监听就绪管道 fd（写 1 字节通知父进程），-1 表示无
 * @return 传输句柄，失败返回 NULL
 */
rdma_xport_t *rdma_xport_server_accept(const char *ip, int port, int ready_fd);

/**
 * 客户端：连接 → 注册接收缓冲 → 发缓冲信息（addr/rkey）
 *
 * @param ip 服务端地址
 * @param port 服务端端口
 * @return 传输句柄，失败返回 NULL
 */
rdma_xport_t *rdma_xport_client_connect(const char *ip, int port);

/**
 * 释放传输句柄（含连接、MR、缓冲区）
 */
void rdma_xport_destroy(rdma_xport_t *x);

/*=============================================================================
 *  服务端：发送
 *============================================================================*/

/**
 * 取发送缓冲（RDB 序列化目标）
 * 前 4 字节留给总长前缀（勿用），序列化数据从返回指针开始写
 *
 * @param x 传输句柄
 * @param capacity 输出：可用负载字节数
 * @return 缓冲指针，失败返回 NULL
 */
void *rdma_xport_send_buf(rdma_xport_t *x, size_t *capacity);

/**
 * 一次性流式发送整个缓冲内容
 * 自动按 CHUNK 拆分 + QP_DEPTH 窗口流水线，尾部 WRITE_WITH_IMM 携带
 * IMM_DONE，之后本地缓冲可安全复用
 *
 * @param x 传输句柄
 * @param payload_len 负载字节数（不含前 4 字节总长前缀）
 * @return 0 成功，-1 失败
 */
int rdma_xport_send(rdma_xport_t *x, uint32_t payload_len);

/*=============================================================================
 *  客户端：接收
 *============================================================================*/

/**
 * 阻塞接收整块快照
 * 等对端 WRITE_WITH_IMM 完成（数据已全部落进接收缓冲）
 *
 * @param x 传输句柄
 * @param payload 输出：负载指针（不含总长前缀）
 * @param payload_len 输出：负载字节数
 * @return 0 成功，-1 失败
 */
int rdma_xport_recv(rdma_xport_t *x, const void **payload, uint32_t *payload_len);

#endif /* __RDMA_TRANSPORT_H__ */
