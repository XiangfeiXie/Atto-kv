/*=============================================================================
 *  kvstore.h — KV 存储公开接口
 *
 *  用途：提供 KV 存储的核心类型声明和对外接口
 *  包含：服务器基础定义（原 server.h）、网络引擎入口、时间函数、
 *        命令分发、全局状态、复制接口
 *
 *  设计原则：
 *  1. 所有外部模块（network/persist/repl）应包含此头文件
 *  2. 内部实现细节在 kvstore_internal.h 中声明
 *  3. 复制与引擎之间的接口通过函数指针解耦
 *  4. 禁止本文件 include persistence.h（persistence.h 依赖本文件的
 *     类型定义，若互相包含会成环；持久化接口由各 .c 自行 include）
 *============================================================================*/

#ifndef __KV_STORE_H__
#define __KV_STORE_H__

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <signal.h>       /* sig_atomic_t：g_kvs_stop */
#include <sys/time.h>
#include <unistd.h>

#include "dynbuf.h"       /* DynBuf：msg_handler / 回复接口 */
#include "config.h"
#include "protocol.h"
#include "kv_dict.h"

#ifdef USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

/*=============================================================================
 *  服务器基础定义（原 server.h，已并入）
 *  整个系统最底层的基础类型和全局声明，所有模块共享
 *============================================================================*/

/*-------------------------------------------------------------------------
 *  全局优雅关闭标志
 *  使用 sig_atomic_t 确保在信号处理器中安全读写
 *  设置：SIGTERM/SIGINT 信号处理器
 *  读取：主事件循环，检测到后退出
 *-------------------------------------------------------------------------*/

/**
 * g_kvs_stop：服务器停止标志
 *
 * 类型选择说明：
 * - sig_atomic_t：保证在信号处理器中的读写是原子的
 * - volatile：防止编译器优化掉循环中的读取
 *
 * 写入：信号处理器（sig_shutdown）
 * 读取：主循环（reactor/proactor/ntyco 的事件循环）
 *
 * 优雅关闭流程：
 * 1. 用户发送 SIGTERM/SIGINT
 * 2. 信号处理器设置 g_kvs_stop = 1
 * 3. 主循环检测到后退出
 * 4. 执行清理（关闭连接、刷新 AOF、释放资源）
 */
extern volatile sig_atomic_t g_kvs_stop;

/*-------------------------------------------------------------------------
 *  回调函数类型定义
 *
 *  不同网络引擎使用不同的回调签名：
 *  - msg_handler：遗留 ntyco 协程引擎使用
 *  - kvs_drain_fn：proactor/reactor 引擎使用（定义在 protocol.h）
 *
 *  设计目的：隔离网络层与业务层，使引擎可以独立切换
 *-------------------------------------------------------------------------*/

/**
 * 消息处理函数类型（仅用于遗留 ntyco 引擎）
 *
 * @param msg 输入消息指针（注意：不保证是以 \0 结尾的字符串）
 * @param len 消息长度
 * @param write_buf 写缓冲（输出响应）
 * @return 0 成功，非 0 失败
 *
 * 注意：proactor/reactor 引擎使用 kvs_drain_fn（声明在 protocol.h）
 * 该类型保留仅为了兼容 ntyco 引擎
 */
typedef int (*msg_handler)(char *msg, int len, DynBuf *write_buf);

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