/*=============================================================================
 *  rdma_transport.h — RDMA 整块流式传输层
 *
 *  机制：
 *   - 客户端(slave 连接发起方)注册一块接收缓冲，把 {addr,rkey,size} 发给服务端；
 *   - 服务端(master 监听方)把整份快照分块流水线 RDMA_WRITE 直写对端内存；
 *     最后一个分块挂 WRITE_WITH_IMM 作为完成通知；
 *   - 客户端等到该 IMM 时整份数据已落在自己的缓冲里，就地加载。
 *
 *  服务端两阶段（conn_open + conn_send），客户端单次调用；收发缓冲由
 *  调用方自持（服务端持发送缓冲、客户端持接收缓冲），快照上限 = RDMA_BUF_SIZE。
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

/*=============================================================================
 *  服务端传输句柄（两阶段调用）
 *
 *  为什么拆两阶段：监听必须在对端连接之前就绪。快照序列化可能耗时数百毫秒
 *  （大快照），若先序列化再监听，对端 REQ 落在监听就绪之前会被丢弃、
 *  rdma_connect 永远等不到事件，双方挂死。因此先 conn_open（监听+接受+
 *  收缓冲凭证，监听立即就绪），再序列化，最后 conn_send。
 *============================================================================*/

typedef struct rdma_xport_conn rdma_xport_conn_t;

/**
 * 阶段1：建立连接（监听 → 接受 → 收对端缓冲凭证）。
 * 连接就绪即返回；调用方可在其后任意时刻准备数据（如序列化快照），
 * 再调 rdma_xport_conn_send 直写。
 *
 * @param ip 监听地址（NULL 表示全接口）
 * @param port 监听端口
 * @param ready_fd 监听就绪管道 fd（写 1 字节通知父进程），-1 表示无
 * @param out 输出：连接句柄（成功时非 NULL）
 * @return 0 成功，-1 失败
 */
int rdma_xport_conn_open(const char *ip, int port, int ready_fd,
                         rdma_xport_conn_t **out);

/**
 * 阶段2：把 [buf, len) 整块直写客户端缓冲（分块流水线推送 → 排空 → 清理）。
 * 调用后 conn 被销毁，不能再复用。
 *
 * @param conn conn_open 返回的句柄
 * @param buf 发送缓冲（调用方自持，须 ≥ len 字节）
 * @param len 待发送字节数（须 ≤ 对端接收缓冲 size，由内部校验）
 * @return 0 成功（含本地全部完成的排空），-1 失败
 */
int rdma_xport_conn_send(rdma_xport_conn_t *conn, const void *buf, size_t len);

/**
 * 主动关闭连接并释放句柄（conn_open 成功后、conn_send 之前放弃时的错误路径）。
 * 成功发送后由 conn_send 内部自行清理，无需（也不应）再调本函数。
 *
 * @param conn conn_open 返回的句柄
 */
void rdma_xport_conn_close(rdma_xport_conn_t *conn);

/**
 * 客户端：接收整块内存（连接 → 发缓冲凭证 → 等尾部 IMM → 清理，一步完成）
 *
 * 返回时整份数据已在 buf 里（实际字节数由上层从数据块协议头读取）
 *
 * @param ip 服务端地址
 * @param port 服务端端口
 * @param buf 接收缓冲（调用方自持，远程写目标，须 ≥ cap 字节）
 * @param cap 接收缓冲容量
 * @return 0 成功，-1 失败
 */
int rdma_xport_recv(const char *ip, int port, void *buf, size_t cap);

#endif /* __RDMA_TRANSPORT_H__ */
