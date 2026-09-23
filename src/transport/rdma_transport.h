/*=============================================================================
 *  rdma_transport.h — RDMA 整块传输层
 *
 *  机制：
 *   - 客户端(slave 连接发起方)注册一块接收缓冲，把 {addr,rkey,size} 发给服务端；
 *   - 服务端(master 监听方)把整份快照作为**一个 WR** RDMA_WRITE 直写对端内存，
 *     该 WR 挂 WRITE_WITH_IMM 作为完成通知；
 *   - 客户端等到该 IMM 时整份数据已落在自己的缓冲里，就地加载。
 *
 *  为什么只需一个 WR：收发缓冲是调用方自持的固定一块（上限 RDMA_BUF_SIZE），
 *  接收端不可能溢出 ⇒ 无需分块、无需滑动窗口、无需 ACK —— RC 可靠有序 +
 *  缓冲固定，顺序与完整性天然成立，整块一个 WR 即可。
 *
 *  服务端两阶段（conn_open + conn_send），客户端单次调用；收发缓冲由
 *  调用方自持（服务端持发送缓冲、客户端持接收缓冲），快照上限 = RDMA_BUF_SIZE。
 *
 *  两个实测经验固化在实现里（勿删）：
 *   1. rdma_connect/accept 必须显式 memset+赋值 conn_param（NULL 时字段
 *      内容不定，某些进程状态下会被对端 REJ_INVALID_RETRY_COUNT）
 *   2. 服务端发完必须等到那个唯一的 send_cqe 才能断连销毁，
 *      否则在途的 WR 会被丢弃、对端永远等不到尾部 IMM
 *
 *  计时归属：本层只**测量**、不打印（conn_send 内的 post→CQE 区间回传给
 *  调用方），汇总结论行由控制层（rdma.c）统一排版 —— 那条线要拼上连接
 *  建立/序列化两段（只有控制层知道），本层自己打就只能另占一行。
 *  速率一律以发送端为准——接收端的墙钟里必然混着对端的序列化时间，测不出
 *  纯传输（服务端是"先连后序列化"，客户端从 post 完 dummy recv 起就在等）。
 *============================================================================*/

#ifndef __RDMA_TRANSPORT_H__
#define __RDMA_TRANSPORT_H__

#include <stdint.h>
#include <stddef.h>

/*=============================================================================
 *  传输参数
 *============================================================================*/

#define RDMA_BUF_SIZE   (512ULL * 1024 * 1024) /* 收发缓冲大小 = 快照硬上限 */

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
 * 单次发送的数据面计时（conn_send 回传，本层不打印）
 *
 * 三段互不重叠，且都不含对端耗时：reg 是本端注册，xfer 是本端 post 到
 * 本端收到 send CQE（RC 下即对端已 ACK）。
 */
typedef struct {
    size_t bytes;     /* 实际发送字节数（= 入参 len） */
    double reg_ms;    /* 发送缓冲注册为本端 MR */
    double xfer_ms;   /* 纯传输：ibv_post_send → send CQE */
} rdma_xport_stat_t;

/**
 * 阶段1：建立连接（监听 → 接受 → 收对端缓冲凭证）。
 * 连接就绪即返回；调用方可在其后任意时刻准备数据（如序列化快照），
 * 再调 rdma_xport_conn_send 直写。
 *
 * @param ip 监听地址（NULL 表示全接口）
 * @param port 监听端口
 * @param out 输出：连接句柄（成功时非 NULL）
 * @return 0 成功，-1 失败
 */
int rdma_xport_conn_open(const char *ip, int port, rdma_xport_conn_t **out);

/**
 * 阶段2：把 [buf, len) 整块直写客户端缓冲（一个 WR 推送 → 等完成 → 清理）。
 * 调用后 conn 被销毁，不能再复用。
 *
 * 内部实测的 xfer 只夹 post_send→send_cqe（纯传输，不含注册/连接），随 st
 * 回传给调用方拼汇总结论行。RDMA_WRITE 的 send CQE 语义是"对端已 ACK"，
 * 即数据确已写入对端内存——与 sendfile 侧"字节进了本端发送缓冲"相比是
 * 真·端到端。
 *
 * @param conn conn_open 返回的句柄
 * @param buf 发送缓冲（调用方自持，须 ≥ len 字节）
 * @param len 待发送字节数（须 ≤ 对端接收缓冲 size，由内部校验）
 * @param st 输出：本次发送的字节数 / 注册耗时 / 纯传输耗时（可为 NULL）
 * @return 0 成功（含本地完成的等待），-1 失败
 */
int rdma_xport_conn_send(rdma_xport_conn_t *conn, const void *buf, size_t len,
                         rdma_xport_stat_t *st);

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
