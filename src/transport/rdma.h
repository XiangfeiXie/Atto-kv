/*=============================================================================
 *  rdma.h — RDMA 全量同步控制层的公开接口（rdma.c 的实现与 rdma.h 一一对应）
 *
 *  分层定位（自上而下，每层只依赖下一层）：
 *    repl_fullsync.c    全量同步编排 + sendfile/RDMA 按配置分派（fork 传输子进程）
 *    rdma.c / 本头      业务控制：RDB 无盘流 序列化 / 加载 / 性能统计，不触碰 verbs
 *    rdma_transport.c   verbs 数据面：连接 / 整块单 WR / IMM 通知
 *
 *  设计原则：
 *  1. 流头 token 语义与 sendfile 路径统一：token = 快照 offset，slave 据此续传对齐
 *  2. 整块一次传：快照序列化进单块发送缓冲，RDMA_WRITE 一个 WR 直写对端，
 *     客户端等尾部 IMM 后就地加载（快照上限 = RDMA_BUF_SIZE）
 *  3. 快照字节 = RDB 磁盘格式同字节序的无盘流（无 crc 槽；见 persist_rdb.c ④）
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

/* 快照流头 = 裸 uint64（快照 offset，htobe64），8B。其后跟 RDB 无盘流
 * [key_count:4][entries...]，数据长度由 RDB 流头 key_count 自描述：
 * slave 只解析 key_count 条，loaded != key_count 即失败。
 * 无盘流无 crc 槽（与磁盘 dump.rdb 唯一差别，磁盘头常量见 persistence.h
 * 的 RDB_FILE_HDR_LEN / RDB_FILE_CRC_OFF）：完整性靠 RC 可靠有序 + 数量核对。
 * 流头 token 语义与 sendfile 路径一致，slave 据此续传对齐。
 * （区别于 rdma.c 里的 RDB_STREAM_HDR_LEN —— 那是 RDB 流自己的 key_count 头） */
#define RDMA_TOKEN_LEN  sizeof(uint64_t)

/*=============================================================================
 *  传输入口
 *  由 repl_fullsync.c 按配置开关直接调用（fullsync_server_run / client_run）
 *============================================================================*/

/**
 * RDMA 服务端入口
 *
 * 流程（连接先行使对端不会早连被丢，见 rdma_transport.h"两阶段"说明）：
 * 1. rdma_xport_conn_open：监听 + 接受 + 收对端接收缓冲凭证（就绪即返回）
 * 2. 整个 RDB 无盘流序列化进调用方自持的暂存块（流头 token + key_count + entries）
 * 3. rdma_xport_conn_send：整块一个 WR 直写对端并等 send CQE
 *
 * 计时分界：只有一行汇总结论 [rdma] xfer-end —— 数据面三段（bytes/reg/xfer，
 * 由 rdma_transport.c 回传）拼上本层的连接建立 / 序列化两段，准备成本一律
 * 不计入速率。形状与 [sendfile] xfer-end 对齐，两条线并排看。
 *
 * @param token 快照 offset（会话元数据）
 * @return 0 成功，-1 失败
 */
int rdma_server_run(uint64_t token);

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
