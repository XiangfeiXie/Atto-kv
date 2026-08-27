/*=============================================================================
 *  repl_incr.h — 主从增量同步
 *  
 *  功能：管理主从复制中的增量同步（Incremental Sync）流程
 *  
 *  设计概要：
 *  1. Backlog：1MB 环形缓冲区，存原始 RESP 写命令
 *  2. 协议：简化 PSYNC（只有 offset，无 replid）
 *  3. 传输：PSYNC 握手走主服务端口，全量走 transport 层
 *  4. fd 归属：所有复制 fd 交给网络引擎托管
 *  5. 状态机：Master/Slave 两侧各自的协议处理
 *============================================================================*/

#ifndef __REPL_INCR_H__
#define __REPL_INCR_H__

#include <stddef.h>
#include "dynbuf.h"

/*=============================================================================
 *  Master 侧接口
 *============================================================================*/

/**
 * Master 主端口的默认 client handler
 * 
 * 功能：
 * 1. 快速探测 PSYNC 命令
 * 2. 如果是 PSYNC → 切换为 slave handler
 * 3. 否则委托 kvs_drain_resp_commands_off 处理普通命令
 * 
 * 注入方式：kvstore.c 注入给 proactor/reactor 作为默认 handler
 * 
 * @param fd 连接 fd
 * @param rb 读缓冲
 * @param r_off 读偏移
 * @param wb 写缓冲
 * @param want_close 关闭标志
 * @return 处理的命令数，-1 表示协议错误
 */
int repl_incr_master_client_handler(int fd, DynBuf *rb, int *r_off,
                                    DynBuf *wb, int *want_close);

/**
 * Master 端从库连接的 handler
 * 
 * 功能：
 * 1. 从库在 streaming 期间不应再发数据
 * 2. 任何输入视为连接断开
 * 
 * 调用时机：从库 PSYNC 成功后切换为该 handler
 */
int repl_incr_master_slave_handler(int fd, DynBuf *rb, int *r_off,
                                   DynBuf *wb, int *want_close);

/**
 * Master 端连接关闭回调
 *
 * 功能：引擎释放从库连接时清理状态
 *
 * 调用者：repl_on_close（引擎 conn_release → repl_on_close → 本函数）
 *
 * @param fd 被释放的连接 fd
 */
void repl_incr_master_on_close(int fd);

/*=============================================================================
 *  Slave 侧接口
 *============================================================================*/

/**
 * 启动 Slave 增量同步
 * 
 * 功能：
 * 1. 记录 Master 地址（IP + 端口）
 * 2. 首次全量 + 连接由 tick 统一推进（非阻塞）
 * 
 * @param master_ip Master IP 地址
 * @param master_port Master 端口
 * @return 0 成功
 */
int repl_incr_slave_start(const char *master_ip, int master_port);

/**
 * Slave 端 Master 链路 handler
 * 
 * 功能：
 * 1. 握手协议：处理 +CONTINUE / +FULLRESYNC
 * 2. 流式回放：接收并执行 Master 发送的增量命令
 * 
 * 状态机：
 * - 握手阶段：等待 +CONTINUE 或 +FULLRESYNC
 * - 回放阶段：逐条执行 RESP 命令
 * 
 * @param fd 连接 fd
 * @param rb 读缓冲
 * @param r_off 读偏移
 * @param wb 写缓冲
 * @param want_close 关闭标志
 * @return 处理的命令数，-1 表示协议错误
 */
int repl_incr_slave_master_link_handler(int fd, DynBuf *rb, int *r_off,
                                        DynBuf *wb, int *want_close);

/**
 * Slave 端连接关闭回调
 *
 * 功能：Master 链路断开时安排重连
 *
 * 调用者：repl_on_close（引擎 conn_release → repl_on_close → 本函数）
 *
 * @param fd 被释放的连接 fd
 */
void repl_incr_slave_on_close(int fd);

/**
 * Slave 端事件循环 Tick
 *
 * 功能：
 * 1. 断线重连
 * 2. 首次连接
 * 3. 全量同步完成后续传
 *
 * 调用者：repl_tick（引擎每轮事件循环调用 → repl_tick → 本函数）
 * 调用频率：每轮事件循环一次
 */
void repl_incr_slave_tick(void);

/**
 * Slave 端重置偏移量（全量同步完成后调用）
 * 
 * 功能：把续传 offset 对齐到快照 offset
 * 
 * 调用者：全量同步完成后
 */
void repl_incr_slave_reset_offset(void);

/**
 * 获取 Slave 当前复制偏移量
 * 
 * 用途：INFO replication 命令展示
 * 
 * @return 当前偏移量
 */
size_t repl_incr_slave_offset(void);

/*=============================================================================
 *  写命令传播（Master 热路径）
 *============================================================================*/

/**
 * 向 streaming 从库推送写命令
 *
 * 功能：把写命令推送给已完成 +CONTINUE 握手的从库（streaming 状态）
 *
 * 调用位置：每次成功的写命令后（SET/DEL/FLUSHALL/EXPIRE 等）
 * 调用者：kvs_repl_incr_hook（kvstore_internal.h）
 *         ——backlog 追加在 hook 内完成，本函数只负责推送
 *
 * @param resp_data RESP 格式的命令数据
 * @param len 数据长度
 *
 * 注意：热路径函数，应尽量高效
 */
void repl_incr_propagate(const char *resp_data, int len);

#endif /* __REPL_INCR_H__ */