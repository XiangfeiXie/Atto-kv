/*=============================================================================
 *  rdma.h — kvstore 用 RDMA 做全量同步的控制层
 *
 *  功能：用 RDMA 传输引擎（rdma_transport.c）做全量同步
 *  传输原语（连接 / 分块窗口 / IMM 通知）都在 rdma_transport.c，
 *  本层只做 kvstore 业务控制（RDB 序列化 / 加载 / 性能统计），不触碰 verbs。
 *
 *  设计原则：
 *  1. 线上格式与 sendfile 路径统一（流头 token + RDB stream）
 *  2. 整块流式：快照序列化进单块发送缓冲，RDMA_WRITE 分块流水线直写对端，
 *     客户端等尾部 IMM 后就地加载（快照上限 = RDMA_BUF_SIZE）
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

/**
 * 快照流头（RDMA 整块传输的数据块头）
 * 仅 token（快照 offset）：数据长度由随后的 RDB 流头 key_count 自描述，
 * 加载后核对数量、不一致即失败（无需 magic 等校验字段）。
 * 与 sendfile 路径结构一致：流头 token + RDB 流。
 */
struct batch_hdr {
    uint64_t token;                 /* 快照 offset，htobe64 */
};

/*=============================================================================
 *  传输入口
 *  由 repl_fullsync.c 按配置开关直接调用（fullsync_server_run / client_run）
 *============================================================================*/

/**
 * RDMA 服务端入口
 *
 * 流程：
 * 1. 整个 RDB 序列化进调用方自持的暂存块（流头 + key_count + entries）
 * 2. rdma_xport_send：监听 + 接受 + 分块流水线推送（内部含完成排空）
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
 * 1. rdma_xport_recv：连接 + 注册接收缓冲 + 发缓冲凭证 + 等尾部 IMM
 * 2. 解出流头 token → 按 RDB 流头 key_count 限条数加载 → 数量不一致即失败
 *
 * @param out_token 输出：从流头解出的 token（快照 offset）
 * @return 0 成功，-1 失败
 */
int rdma_client_run(uint64_t *out_token);

#endif /* __RDMA_H__ */
