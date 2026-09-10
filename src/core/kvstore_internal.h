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
 *  写效果收口
 *  kvs_journal_effect 已在 kvstore_dispatch.c（原两个内联钩子 kvs_write_journal_
 *  append / kvs_repl_incr_hook 并入此处）——AOF 与增量复制的配置门 / g_in_reload
 *  收拢为单一外部函数，由各读缓冲 drain 在 dispatch 回传 journaled=1 后调用。
 *  本头文件不再承载内联日志/复制钩子（见 kvstore.h 的 kvs_journal_effect 声明）。
 *============================================================================*/

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