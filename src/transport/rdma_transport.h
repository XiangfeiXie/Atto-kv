/*=============================================================================
 *  rdma_transport.h — RDMA 流式批量传输层（段池 + 帧流水线 + ack 信用）
 *
 *  设计：
 *   - 收发两端各持一块连续注册内存，切成 NUM_SEGS 个固定大小"段"
 *     （本质上是流水线双缓冲的推广：K≥2 时生产与传输天然重叠）
 *   - 一段 = 一帧。发送端把数据填进空闲段后 post 成一组 RDMA WRITE，
 *     末尾子 WR 用 WRITE_WITH_IMM：低 31 位 = 本帧字节数，
 *     最高位（IMM_DONE）置位表示最后一帧
 *   - RC 保序保证：一段数据的 WRITE 必然先于它关联的 IMM 在对端可见，
 *     所以客户端见到 imm CQE（wr_id 即槽号）时该帧必然完整落地
 *   - 流控 = ack 信用制：客户端"消费完"一帧才回发一个小 SEND；
 *     服务端已发未确认的帧数超过信用窗口就暂停产出新帧。
 *     窗口硬上限 ⇒ 接收端环永不溢出
 *
 *  连接建立走显式事件通道 + poll 超时泵：每个 CM 阶段带超时与日志，
 *  失败快速返回（调用方负责清理重试），绝不静默永久阻塞。
 *============================================================================*/

#ifndef __RDMA_TRANSPORT_H__
#define __RDMA_TRANSPORT_H__

#include <stdint.h>
#include <stddef.h>

/*=============================================================================
 *  传输参数
 *============================================================================*/

#define RDMA_SEG_SIZE      (16u << 20)        /* 单段 = 单帧负载上限 */
#define RDMA_NUM_SEGS      4                  /* 段池深度（≥2 即双缓冲） */
#define RDMA_ACK_WINDOW    RDMA_NUM_SEGS      /* 服务端信用窗口 = 已发未ack上限 */
#define RDMA_CHUNK_SIZE    (4u << 20)         /* 单个 WRITE 子请求拆分上限 */
#define RDMA_QP_SEND_WR    64                 /* 发送方 SQ 深度 */
#define RDMA_QP_RECV_WR    (RDMA_NUM_SEGS + 8)/* 接收方 RQ 深度（dummy+ack） */
#define RDMA_CM_TIMEOUT_MS 5000               /* 每个 CM 握手阶段超时 */

/** imm 编码：最高位 DONE 标志，低 31 位 = 本帧字节数 */
#define RDMA_IMM_DONE   0x80000000u
#define RDMA_IMM_LEN(v) ((v) & 0x7FFFFFFFu)

typedef struct rdma_xport rdma_xport_t;

/*=============================================================================
 *  连接管理
 *============================================================================*/

/**
 * 服务端：监听 →（超时事件泵）接受连接 → 收对端缓冲信息（addr/rkey）
 *
 * @param ip 监听地址（NULL 表示全接口）
 * @param port 监听端口
 * @param ready_fd 监听就绪管道 fd（写 1 字节通知父进程），-1 表示无
 * @return 传输句柄，失败返回 NULL
 */
rdma_xport_t *rdma_xport_server_accept(const char *ip, int port, int ready_fd);

/**
 * 客户端：（超时事件泵）连接 → 注册接收段池 → 发缓冲信息（addr/rkey）
 *
 * @param ip 服务端地址
 * @param port 服务端端口
 * @return 传输句柄，失败返回 NULL
 */
rdma_xport_t *rdma_xport_client_connect(const char *ip, int port);

/**
 * 释放传输句柄（含连接、MR、段池）
 */
void rdma_xport_destroy(rdma_xport_t *x);

/*=============================================================================
 *  发送端：生产帧
 *  循环契约：buf_get → 填数据 → buf_post(…, last=是否末帧)，直到 last=1
 *============================================================================*/

/**
 * 取一个空闲段的基地址（可能阻塞等待信用/回收已完成段）
 *
 * @param x 传输句柄
 * @param capacity 输出：本段可写字节数（恒为 RDMA_SEG_SIZE）
 * @return 段基址，失败返回 NULL
 */
void *rdma_xport_buf_get(rdma_xport_t *x, size_t *capacity);

/**
 * 把刚填写的段作为一帧发出；last=1 时尾部带 IMM_DONE 标志
 *
 * @param x 传输句柄
 * @param bytes 本帧实际字节数（>0 且 ≤ capacity）
 * @param last 是否最后一帧
 * @return 0 成功，-1 失败
 */
int rdma_xport_buf_post(rdma_xport_t *x, size_t bytes, int last);

/**
 * 等待全部已提交帧的本地发送完成（排空 send_cq）
 * 必须在末帧提交后、关闭连接前调用，否则排队中的 WRITE 会被断连丢弃
 *
 * @return 0 成功，-1 失败
 */
int rdma_xport_flush(rdma_xport_t *x);

/*=============================================================================
 *  接收端：消费帧
 *  循环契约：frame_recv → 用完数据（比如加载进字典）→ frame_ack。
 *  注意：数据指针只在 frame_ack 之前有效！先 ack 再读数据会读到被覆盖的内容
 *============================================================================*/

/**
 * 阻塞收取下一帧（等 IMM 完成 CQE，wr_id 定位落地的段）
 *
 * @param x 传输句柄
 * @param data 输出：帧数据指针（就地读取，无拷贝）
 * @param len 输出：本帧字节数
 * @param last 输出：是否最后一帧（IMM_DONE 置位）
 * @return 0 成功，-1 失败
 */
int rdma_xport_frame_recv(rdma_xport_t *x, const void **data,
                          uint32_t *len, int *last);

/**
 * 声明上一帧已消费完：归还会槽位并向服务端回发 ack（归还信用）
 *
 * @return 0 成功，-1 失败
 */
int rdma_xport_frame_ack(rdma_xport_t *x);

#endif /* __RDMA_TRANSPORT_H__ */
