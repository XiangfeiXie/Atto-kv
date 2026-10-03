/*=============================================================================
 *  rdma.h — RDMA 全量同步的公开接口（实现全在 rdma.c）
 *
 *  定位：与 sendfile 路径并列的一种全量同步传输方式。repl_fullsync.c 按配置
 *  分派（fork 传输子进程）后直接调用下面两个入口。
 *
 *  线上格式（单块，上限 = RDMA_BUF_SIZE）：
 *   [流头 token 8B][RDB 无盘流: key_count 4B + entries...]
 *  无盘流与磁盘 dump.rdb 条目字节序完全一致，唯一差别是不带 crc 槽：无盘传输
 *  不落盘、无防篡改需求，完整性靠 RC 可靠有序 + key_count 数量核对。数据长度
 *  由 RDB 流头 key_count 自描述（slave 只解析 key_count 条），故无 magic /
 *  total_len 等额外校验字段。流头 token 语义与 sendfile 路径一致（快照
 *  offset，slave 据此续传对齐）。
 *
 *  分层：verbs 数据面（连接 / 整块单 WR / IMM）与业务控制（序列化 / 加载 /
 *  计时）同在 rdma.c，以文件内分段隔开，不再拆独立头文件 —— 一条传输路 =
 *  一对 {.h, .c}，与 sendfile.c 同形状。
 *============================================================================*/

#ifndef __RDMA_H__
#define __RDMA_H__

#include <stdint.h>

/*=============================================================================
 *  传输参数
 *============================================================================*/

/* 收发缓冲大小 = 快照硬上限（两端同值）。
 * 与 sendfile 的 SENDFILE_BUF_SIZE 目前同值，但两条路各自独立定义：
 * 缓冲尺寸没有必须相等的理由，之后要分开调就直接改这里 */
#define RDMA_BUF_SIZE   (512ULL * 1024 * 1024)

/* 快照流头 = 裸 uint64（快照 offset，htobe64），8B。其后跟 RDB 无盘流
 * [key_count:4][entries...]。
 * （区别于 rdma.c 里的 RDB_STREAM_HDR_LEN —— 那是 RDB 流自己的 key_count 头） */
#define RDMA_TOKEN_LEN  sizeof(uint64_t)

/*=============================================================================
 *  传输入口
 *  由 repl_fullsync.c 按配置开关直接调用（fullsync_server_run / client_run）
 *============================================================================*/

/**
 * RDMA 服务端入口（master 全量同步传输子进程，fork 由 repl_fullsync.c 触发）
 *
 * 流程（连接先行使对端不会早连被丢，见 rdma.c 文件头"为什么服务端要两阶段"）：
 * 1. conn_open：监听 + 接受 + 收对端接收缓冲凭证（就绪即返回）
 * 2. 整个 RDB 无盘流序列化进调用方自持的暂存块（流头 token + key_count + entries）
 * 3. conn_send：整块一个 WR 直写对端并等 send CQE
 *
 * 计时分界：只有一行汇总结论 [rdma] xfer-end —— 数据面三段（bytes/reg/xfer）
 * 拼上本层的连接建立 / 序列化两段，准备成本一律不计入速率。形状与
 * [sendfile] xfer-end 对齐，两条线并排看。
 *
 * @param token 快照 offset（会话元数据）
 * @return 0 成功，-1 失败
 */
int rdma_server_run(uint64_t token);

/**
 * RDMA 客户端入口
 *
 * 流程：
 * 1. conn_recv：连接 + 注册接收缓冲 + 发缓冲凭证 + 等尾部 IMM（整块落地）
 * 2. 解出流头 token → 按 RDB 流头 key_count 限条数加载 → 数量不一致即失败
 *
 * @param out_token 输出：从流头解出的 token（快照 offset）
 * @return 0 成功，-1 失败
 */
int rdma_client_run(uint64_t *out_token);

#endif /* __RDMA_H__ */
