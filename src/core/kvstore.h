/*=============================================================================
 *  kvstore.h — KV 存储公开接口
 *  
 *  用途：提供 KV 存储的核心类型声明和对外接口
 *  包含：网络引擎入口、时间函数、命令分发、全局统计、复制接口
 *  
 *  设计原则：
 *  1. 所有外部模块（network/persist/repl）应包含此头文件
 *  2. 内部实现细节在 kvstore_internal.h 中声明
 *  3. 复制与引擎之间的接口通过函数指针解耦
 *============================================================================*/

#ifndef __KV_STORE_H__
#define __KV_STORE_H__

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <sys/time.h>
#include <unistd.h>

#include "server.h"
#include "config.h"
#include "protocol.h"
#include "kv_dict.h"

#ifdef USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

/*=============================================================================
 *  网络引擎入口函数
 *  三种模型：Reactor(epoll)、Proactor(io_uring)、NtyCo(协程遗留)
 *============================================================================*/

/**
 * 启动 Reactor 引擎（epoll 水平触发）
 * @param port 监听端口
 * @param handler 命令解析处理函数
 * @return 0 成功，-1 失败
 */
extern int reactor_start(unsigned short port, kvs_drain_fn handler);

/**
 * 启动 Proactor 引擎（io_uring）
 * @param port 监听端口
 * @param handler 命令解析处理函数
 * @return 0 成功，-1 失败
 */
extern int proactor_start(unsigned short port, kvs_drain_fn handler);

/**
 * 启动 NtyCo 引擎（协程，遗留引擎）
 * @param port 监听端口
 * @param handler 消息处理函数（旧接口签名）
 * @return 0 成功，-1 失败
 */
extern int ntyco_start(unsigned short port, msg_handler handler);

/*=============================================================================
 *  时间函数
 *============================================================================*/

/**
 * 获取缓存时间
 * 每轮事件循环更新一次，粒度约 1ms
 * 适用于大多数不需要高精度的场景（过期判断、统计等）
 * @return 当前时间戳（毫秒）
 */
long long get_time_now(void);

/**
 * 获取实时时间
 * 直接调用 gettimeofday，开销较大
 * 仅在需要高精度时使用（如跨 worker 的 BLPOP 超时）
 * @return 当前时间戳（毫秒）
 */
long long get_time_now_real(void);

/**
 * 更新缓存时间
 * 每轮事件循环调用一次
 * 同时触发 AOF 周期性刷新和主动过期清理
 */
void update_cached_time(void);

/*=============================================================================
 *  命令分发
 *============================================================================*/

/**
 * 命令分发器：解析并执行命令
 * @param tokens 解析后的 token 数组
 * @param argc 参数个数
 * @param msg_raw 原始命令消息（用于日志和复制）
 * @param msg_len 消息长度
 * @param wb 写缓冲（输出响应）
 * @return DISPATCH_OK/DISPATCH_ERROR/DISPATCH_CLOSE
 */
dispatch_result_t kvs_dispatch(kvs_token_t *tokens, int argc,
                               const char *msg_raw, int msg_len,
                               DynBuf *wb);

/*=============================================================================
 *  数据管理
 *============================================================================*/

/**
 * 清空所有数据
 * 使用 jemalloc 时额外 purge 释放内存
 */
void kvs_flush_all(void);

/*=============================================================================
 *  全局状态
 *============================================================================*/

/**
 * g_in_reload：数据重载标志
 * 1 表示正在从 AOF/RDB 重载数据
 * 重载期间抑制 AOF 追加和复制传播
 */
extern int g_in_reload;

/*=============================================================================
 *  INFO 命令统计计数器
 *  
 *  设计考虑：
 *  - g_kvs_total_commands：仅在单线程事件循环中写入（dispatch 串行执行）
 *    且仅在 INFO 命令同一线程读取，因此使用普通 uint64_t
 *    若使用 _Atomic，x86-64 上会编译为 `lock add` 指令，有额外串行化开销
 *    在热路径上不可忽略
 *  
 *  - g_kvs_total_connections / g_kvs_connected_clients：
 *    连接事件可能在不同上下文发生，使用原子操作保证安全
 *============================================================================*/

extern uint64_t g_kvs_total_commands;                /* 总命令处理数（单线程） */
extern _Atomic uint64_t g_kvs_total_connections;     /* 历史总连接数（原子） */
extern _Atomic int      g_kvs_connected_clients;     /* 当前活跃连接数（原子） */
extern long long        g_kvs_start_time_ms;         /* 启动时间戳（只读） */

/*=============================================================================
 *  复制 ↔ 引擎 访问接口
 *
 *  设计目的：实现复制模块与网络引擎之间的双向解耦
 *  两个模块互不包含对方头文件，通过共享的 kvstore.h 绑定
 *
 *  两种绑定方式（各取其所需）：
 *  - repl_engine_*（repl → 引擎）：函数指针。哪个引擎激活就注册谁，
 *    引擎内部实现为静态函数，repl 不感知 reactor/proactor 差异
 *  - repl_on_close / repl_tick（引擎 → repl）：直接函数。
 *    角色固定、调用时可查，无需指针；内部按 repl_role 判断是否处理
 *
 *  调用方向：
 *  - 引擎 → repl：repl_on_close（连接关闭通知）、repl_tick（事件循环 tick）
 *  - repl → 引擎：repl_engine_register/append/release（连接管理和数据发送）
 *============================================================================*/

/**
 * 引擎提供的接口（由 engine 实现，repl 调用）
 */

/**
 * 注册连接为 repl 连接
 * 切换为 repl 专用的命令处理器
 * @param fd 连接 fd
 * @param handler repl 专用 handler
 * @return 0 成功，-1 失败
 */
extern int  (*repl_engine_register)(int fd, kvs_drain_fn handler);

/**
 * 向 repl 连接追加数据（如 +FULLRESYNC、复制流）
 * @param fd 连接 fd
 * @param data 数据指针
 * @param len 数据长度
 * @return 0 成功，-1 失败
 */
extern int  (*repl_engine_append)(int fd, const char *data, int len);

/**
 * 释放 repl 连接
 * @param fd 连接 fd
 */
extern void (*repl_engine_release)(int fd);

/**
 * repl 提供的接口（由 repl 实现，engine 调用）
 */

/**
 * 连接关闭回调
 * 引擎释放任意连接时调用，通知 repl 清理状态
 * 内部按 repl_role 分发，普通客户端连接为 no-op
 * @param fd 被释放的连接 fd
 */
extern void repl_on_close(int fd);

/**
 * 事件循环 tick 回调
 * 每轮事件循环调用一次，内部按 repl_role 判断
 * 用于：Slave 断线重连 / 全量同步状态推进
 */
extern void repl_tick(void);

#endif /* __KV_STORE_H__ */