/*=============================================================================
 *  rdma.h — kvstore 用 RDMA 做全量同步的控制层
 *
 *  功能：用 RDMA 传输引擎（rdma_transport.c）做全量同步
 *  传输原语 + 传输协议（连接/chunk 流式/IMM_DONE）都在 rdma_transport.c
 *  本层只做 kvstore 业务控制（RDB 序列化 / 加载 / 性能统计）
 *
 *  设计原则：
 *  1. 传输格式与 sendfile 路径统一（batch_hdr + RDB stream）
 *  2. 一次连接传一整份快照：单缓冲 + chunk 窗口流式
 *  3. 上层由 repl_fullsync.c 按配置开关直接调用
 *
 *  适用场景：高性能计算、低延迟要求高的环境
 *  优势：极低延迟、CPU 卸载、内核旁路
 *============================================================================*/

#ifndef __RDMA_H__
#define __RDMA_H__

#include <stdint.h>

/*=============================================================================
 *  协议信封
 *============================================================================*/

/** RDMA 魔数：用于握手确认 */
#define RDMA_MAGIC      0x52444233u   /* "RDB3" */

/**
 * 快照头部结构
 * 快照载荷开头携带，magic 校验 + token（快照 offset，供增量模块续传）
 */
struct batch_hdr {
    uint32_t magic;                 /* 魔数（0x52444233） */
    uint64_t token;                 /* 快照 offset（会话元数据） */
} __attribute__((packed));

/*=============================================================================
 *  传输入口
 *  由 repl_fullsync.c 按配置开关直接调用（fullsync_server_run / client_run）
 *============================================================================*/

/**
 * RDMA 服务端入口
 *
 * 流程：
 * 1. rdma_xport_server_accept：监听 + 接受连接 + 交换缓冲信息
 * 2. 整个 RDB 序列化进发送缓冲
 * 3. rdma_xport_send：chunk 窗口流式一次性推送（尾部 IMM_DONE）
 *
 * @param token 快照 offset（会话元数据）
 * @param ready_fd 就绪管道 fd：监听成功后写 1 字节通知父进程
 * @return 0 成功，-1 失败
 */
int rdma_server_run(uint64_t token, int ready_fd);

/**
 * RDMA 客户端入口
 *
 * 流程：
 * 1. rdma_xport_client_connect：连接 + 注册接收缓冲 + 交换缓冲信息
 * 2. rdma_xport_recv：一次性接收整块快照（阻塞等尾部 IMM_DONE）
 * 3. load_batch：整块加载到字典，解出快照 offset
 *
 * @param out_token 输出：从流头解出的 token（快照 offset）
 * @return 0 成功，-1 失败
 *
 * 设计要点：
 * - 单接收缓冲（RDMA_BUF_SIZE = 256MB，快照大小上限）
 * - 客户端一次性加载，无需分批
 */
int rdma_client_run(uint64_t *out_token);

#endif /* __RDMA_H__ */
