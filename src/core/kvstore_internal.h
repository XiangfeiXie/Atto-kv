/*=============================================================================
 *  kvstore_internal.h — KV 存储内部声明
 *  
 *  用途：供 kvstore_*.c 各模块共享的内部声明
 *  包含：kvstore_dispatch.c, kvstore_config.c, kvstore_expire.c
 *  
 *  重要：不要在 network / persist / repl 层包含此头文件！
 *  这些层应该直接包含 kvstore.h（公开接口）
 *  
 *  设计原则：
 *  1. 隔离内部实现细节，对外只暴露必要的接口
 *  2. 内联函数用于热路径优化（日志写入、复制钩子）
 *  3. 集中声明跨文件使用的函数和变量
 *============================================================================*/

#ifndef KVSTORE_INTERNAL_H
#define KVSTORE_INTERNAL_H

#include "kvstore.h"        /* 公开接口：基本类型、字典、配置等 */
#include "persistence.h"    /* AOF 持久化接口（AOF 接口已并入） */

/*=============================================================================
 *  kvstore.c 提供的工具函数
 *============================================================================*/

/* 将 token 转换为 long long 整数
 * @param t 输入 token
 * @param out 输出整数值
 * @return 0 成功，-1 失败（非数字或溢出）
 */
int  token_to_ll(kvs_token_t t, long long *out);

/* 回复整数（RESP 整数格式）
 * @param wb 写缓冲
 * @param v 整数值
 */
void reply_integer(DynBuf *wb, long long v);

/* 回复 Bulk String（RESP 批量字符串格式）
 * @param wb 写缓冲
 * @param s 字符串指针
 * @param len 字符串长度
 */
void reply_bulk(DynBuf *wb, const char *s, int len);

/*=============================================================================
 *  持久化和复制钩子（内联函数，优化热路径）
 *============================================================================*/

/**
 * 写入操作日志（AOF + RDB 变更计数）
 * 
 * 在每次成功的写入命令（SET/DEL/FLUSHALL/EXPIRE）后调用
 * 功能：
 * 1. AOF 模式：追加命令到 AOF 缓冲区（持久化）
 *
 * @param msg 原始 RESP 命令消息
 * @param length 消息长度
 *
 * 内联原因：热路径上每次写入都调用，函数调用开销不可忽略
 */
static inline void kvs_write_journal_append(const char *msg, int length) {
    if (!msg || length <= 0) return;

    if (ENABLE_AOF) kvs_aof_persist_append_raw(msg, (uint32_t)length);  /* AOF：追加日志 */
}

/**
 * 增量复制钩子
 * 
 * 在每次成功的写入命令后调用，将命令传播到从库
 * 功能：
 * 1. 追加到复制积压缓冲区（repl_backlog）
 * 2. 推送给 streaming 从库
 * 
 * 优化策略：
 * - 只在 Master 角色且 incr_sync 开启时才执行
 * - g_in_reload 时跳过（数据加载中，避免重复传播）
 * 
 * @param msg 原始 RESP 命令消息
 * @param length 消息长度
 * 
 * 设计决策：维护 backlog 和传播是写热路径上的开销
 * 在"无复制"或"仅全量"基准下，通过提前返回避免浪费
 */
#include "repl.h"
static inline void kvs_repl_incr_hook(const char *msg, int length) {
    extern int g_in_reload;
    
    if (!msg || length <= 0 || g_in_reload) return;
    
    /* 只有 master 且增量复制开启时才需要维护 backlog 和传播
     * repl_role 非 master、或 incr_sync 关闭时，
     * 维护 backlog 是写热路径上的纯浪费 */
    if (kvs_config.repl_role != REPL_ROLE_MASTER) return;
    if (!kvs_config.incr_sync) return;
    
    repl_backlog_append(msg, length);      /* 追加到复制积压缓冲区 */
    repl_incr_propagate(msg, length);      /* 推送给 streaming 从库 */
}

/*=============================================================================
 *  kvstore_expire.c 提供的接口
 *============================================================================*/

/* 主动过期清理循环（每轮事件循环调用一次）
 * 随机采样并删除已过期的键 */
void active_expire_cycle(void);

/*=============================================================================
 *  kvstore_config.c 提供的接口
 *============================================================================*/

/* CONFIG 伪装包命令处理函数（只应答 redis-benchmark 启动探测的 GET） */
dispatch_result_t handle_config(kvs_token_t *tokens, int argc, DynBuf *wb);

/*=============================================================================
 *  kvstore_info.c 提供的接口
 *============================================================================*/

/* INFO 命令处理函数（最小实现，只回 memory 段 used_memory） */
dispatch_result_t handle_info(kvs_token_t *tokens, int argc, DynBuf *wb);

#endif /* KVSTORE_INTERNAL_H */