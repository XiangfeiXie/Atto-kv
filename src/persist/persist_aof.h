/*=============================================================================
 *  persist_aof.h — AOF（Append-Only File）持久化接口
 *  
 *  功能：提供增量命令日志的持久化能力
 *  设计特点：
 *  1. 无锁环形缓冲区（单线程写入）
 *  2. io_uring 异步落盘
 *  3. 支持三种 fsync 策略
 *============================================================================*/

#ifndef KVS_PERSIST_AOF_H
#define KVS_PERSIST_AOF_H

#include <stdint.h>

/*=============================================================================
 *  可调参数
 *============================================================================*/

/**
 * 单条命令写入 AOF 的字节上限（512KB）
 * 
 * 与 AOF 槽大小 AOF_SLOT_SIZE（persist_aof.c）一致
 * 保证 flush 后新记录总能装进空槽
 * 
 * 超限命令执行成功但不落 AOF（kvs_aof_persist_append_raw 拒绝）
 * 
 * 设计考虑：
 * - 防止超大命令占满环形缓冲区
 * - 限制单次 I/O 大小，避免阻塞
 * - 512KB 对大多数 Redis 命令已足够
 */
#define KVS_JOURNAL_MAX_PAYLOAD  (512u * 1024u)

/*=============================================================================
 *  公开 API
 *============================================================================*/

/**
 * 启动 AOF 持久化
 * 
 * 初始化：
 * - 环形缓冲区
 * - io_uring 实例
 * - AOF 文件（追加模式）
 * 
 * @return 0 成功，-1 失败
 */
int kvs_aof_persist_start(void);

/**
 * 追加原始 RESP 字节到环形缓冲区
 * 
 * 特点：
 * - 无锁（单线程调用）
 * - 非阻塞（写入环形缓冲区即返回）
 * - 异步落盘（由后台 io_uring 处理）
 * 
 * @param payload RESP 命令数据
 * @param len 数据长度
 * @return 0 成功（入队），-1 失败（缓冲区满或超限）
 * 
 * 注意：调用者需保证 payload 是完整的 RESP 命令
 */
int kvs_aof_persist_append_raw(const char *payload, uint32_t len);

/**
 * 停止 AOF 持久化
 * 
 * 流程：
 * 1. 刷新所有待写入数据
 * 2. 关闭 io_uring
 * 3. 关闭 AOF 文件
 * 
 * 在程序退出时调用，确保数据不丢失
 */
void kvs_aof_persist_stop(void);

/**
 * 设置 fsync 策略
 * 
 * @param policy 策略字符串：
 *   - "always"：每条命令后 fsync（最安全，性能最低）
 *   - "everysec"：每秒 fsync（平衡，默认）
 *   - "no"：由操作系统决定（最快，可能丢失数据）
 *
 * 只在启动时调用一次（CONFIG SET 不可运行时修改）
 */
void kvs_aof_set_fsync_policy(const char *policy);

/**
 * 周期性维护函数
 *
 * 由事件循环（update_cached_time）每轮调用
 * 功能：
 * - 收割已完成的 io_uring 写入
 * - 时间驱动的缓冲区刷新（wall-clock 判断，非 tick 计数）
 * - 检查 fsync 定时器（everysec 模式）
 *
 * 调用频率：每轮事件循环
 */
void kvs_aof_periodic(void);

#endif /* KVS_PERSIST_AOF_H */