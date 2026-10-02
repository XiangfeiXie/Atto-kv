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
#include <time.h>
#include <unistd.h>

#include "dynbuf.h"       /* DynBuf：写缓冲 / 回复接口 */
#include "config.h"
#include "protocol.h"
#include "kv_engine.h"

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
 *  统一签名 kvs_rbuf_drain_fn（protocol.h 定义）：reactor/proactor/ntyco
 *  读缓冲全 rbuf，start(port, handler) 一律注入 kvs_drain_rbuf_commands。
 *  （遗留 msg_handler / kvs_drain_fn 两种签名已随 ntyco 接入 rbuf 删除）
 *-------------------------------------------------------------------------*/

/*=============================================================================
 *  网络引擎入口函数
 *  三种模型：Reactor(epoll)、Proactor(io_uring)、NtyCo(协程)
 *============================================================================*/

/**
 * 启动 Reactor 引擎（epoll 水平触发）
 * reactor 全连接读缓冲统一为 rbuf 节点队列，handler 即 accept 连接的默认
 * 数据入口（kvs_rbuf_drain_fn）。master 与其余角色一律传 kvs_drain_rbuf_commands
 * （PSYNC 握手作为控制命令在命令分发层被接管，无需按角色选 handler）；
 * slave 出站链路连接由 repl 侧按需 eng_repl_register_rbuf 覆盖。
 * @param port 监听端口
 * @param handler 默认命令解析函数（rbuf 形态）
 * @return 0 成功，-1 失败
 */
extern int reactor_start(unsigned short port, kvs_rbuf_drain_fn handler);

/**
 * 启动 Proactor 引擎（io_uring）
 * @param port 监听端口
 * @param handler 命令解析处理函数
 * @return 0 成功，-1 失败
 */
extern int proactor_start(unsigned short port, kvs_rbuf_drain_fn handler);

/**
 * 启动 NtyCo 引擎（协程引擎）
 * ntyco 接入 rbuf 后与 reactor/proactor 统一：读缓冲 rbuf 节点队列、数据入口
 * 一律 kvs_drain_rbuf_commands。引擎参与复制（2026-09 接入）：本函数赋值
 * repl_engine_register_rbuf / repl_engine_append，并在 repl_role!=off 时起一个
 * 常驻 ticker 协程周期调 repl_tick + 冲刷 repl 链路（详见 src/net/ntyco.c）。
 * @param port 监听端口
 * @param handler 命令解析处理函数（rbuf 形态，统一 kvs_drain_rbuf_commands）
 * @return 0 成功，-1 失败
 */
extern int ntyco_start(unsigned short port, kvs_rbuf_drain_fn handler);

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

/**
 * 单调时钟当前时刻（纳秒）
 * CLOCK_MONOTONIC：vDSO 用户态读取，不陷入内核；单调时钟不受
 * NTP / 校时跳变影响，适合耗时测量
 * 传输层计时（sendfile.c / rdma.c）统一走这里，
 * 替代各文件重复的 clock_gettime + 手工换算
 * 注意：单调时钟（相对时刻），不是墙钟；需要 epoch 时间戳请用 get_time_now()
 */
static inline uint64_t now_monotonic_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/**
 * 传输速率：ms 毫秒内搬了 bytes 字节 → MB/s
 *
 * 十进制 MB（1e6），不是 MiB（1024²）。打印写 "MB/s" 就必须除 1e6，
 * 否则名字和数值对不上（旧字段叫 mbps 但除的是 1024²，是 MiB/s）。
 * 全量同步两条路径（sendfile.c / rdma.c）共用这一处换算，避免两边各写
 * 一遍除式之后单位悄悄跑偏。ms <= 0（区间没测到）返回 0。
 */
static inline double xfer_rate_mbs(size_t bytes, double ms)
{
    return (ms > 0.0) ? ((double)bytes / (ms / 1000.0) / 1e6) : 0.0;
}

/*=============================================================================
 *  命令分发
 *============================================================================*/

/**
 * 命令分发器：解析并执行命令（缓冲无关，不持有任何读缓冲数据指针）
 * @param tokens 解析后的 token 数组
 * @param argc 参数个数（第一个是命令名）
 * @param wb 写缓冲（输出响应）
 * @param journaled 输出（必填）：本条命令是否产生需要落 AOF/复制的写效果
 *        （SET 存成 / DEL 命中删除 / FLUSHALL / EXPIRE 命中设置）。
 *        分发是"是否生效"的权威判定，只置 1、入口统一归零。
 * @return DISPATCH_OK/DISPATCH_ERROR/DISPATCH_CLOSE
 */
dispatch_result_t kvs_dispatch(kvs_token_t *tokens, int argc,
                               DynBuf *wb, int *journaled);

/**
 * 写效果收口：drain 在 kvs_dispatch 返回 journaled=1 后、consume 前把整条
 * 原始命令交给本函数，落 AOF 日志与增量复制（backlog + streaming 从库）。
 * 配置门（ENABLE_AOF / master & incr / g_in_reload）统一在此判定一次，
 * 调用方无需预判命令名——效果以 dispatch 的 journaled 回传为准。
 * 字节必须由调用方持有（drain 的物化区/连续读缓冲），本函数同步拷贝、不滞留。
 * @param cmd 整条 RESP 原始命令
 * @param len 命令长度
 */
void kvs_journal_effect(const char *cmd, int len);

/*=============================================================================
 *  数据管理
 *============================================================================*/

/**
 * 清空所有数据
 * 使用 jemalloc 时额外 purge 释放内存
 */
void kvs_flush_all(void);

/**
 * 优雅停机的收尾（AOF 落盘 + 引擎回收），三种网络模型共用
 *
 * reactor / proactor：主循环读到 g_kvs_stop 返回 → main 第 6 段调本函数。
 * ntyco：调度器不返回（见实现处说明），由 ticker 协程调本函数后自行 exit。
 * 调用方负责决定怎么退出；本函数只做清理，不 return 出进程。
 *
 * 不含 SAVE —— RDB 只由显式 SAVE/BGSAVE 产生，三个引擎一致。
 */
void kvs_shutdown(void);

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
extern uint64_t get_used_mem(void);                  /* allocator 存活分配字节（INFO used_memory） */

/**
 * 打印内存快照：一行 "[MEM] <tag> used=<bytes> rss=<KB>"
 * 启动/关闭等关键时点使用（退出后 /proc 消失，关闭时内存只能由进程自报）
 */
void mem_report(const char *tag);

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
 *  - repl → 引擎：repl_engine_register_rbuf/append（连接管理与数据发送）
 *============================================================================*/

/**
 * 引擎提供的接口（由 engine 实现，repl 调用）
 */

/**
 * 注册 repl 连接 handler
 * 引擎接管 fd，后续数据走 repl 专用 handler（rbuf 形态）
 * 全引擎读缓冲均为 rbuf，DynBuf 版 repl_engine_register 已删（M2），
 * 两引擎的 start() 只赋本指针。
 *
 * @param fd 连接 fd
 * @param handler repl 专用 handler（rbuf 形态）
 * @return 0 成功，-1 失败
 */
extern int  (*repl_engine_register_rbuf)(int fd, kvs_rbuf_drain_fn handler);

/**
 * 向 repl 连接追加数据（如 +FULLRESYNC、复制流）
 * @param fd 连接 fd
 * @param data 数据指针
 * @param len 数据长度
 * @return 0 成功，-1 失败
 */
extern int  (*repl_engine_append)(int fd, const char *data, int len);

/**
 * fork 出的传输子进程里重置引擎自身状态（可空，引擎不实现则保持 NULL）
 *
 * 全量同步 master 侧会 fork 出数据面传输子进程（repl_fullsync.c）。子进程
 * 调用本指针**先把自己从父进程的运行时里摘出去**，再开始收发 RDB。
 *
 * 为什么必须要这一步：ntyco 用的 NtyCo 有一套 COROUTINE_HOOK，用全局强符号
 * 重定义 socket/accept/connect/recv/send/write（见 ntyco.c 文件头）。这套劫持
 * 在 fork 后会跟着子进程走，而 epoll fd 是**跨 fork 共享同一个内核对象**的：
 * 子进程调一次 accept()（NtyCo 的 nty_accept 会无条件 nty_poll_inner），就把
 * 自己的监听 fd EPOLL_CTL_ADD 进了**父进程的 epoll**。slave 连上数据面端口后
 * 该 fd 变可读，父进程 epoll_wait 收到事件 → nty_schedule_search_wait 在父进程
 * 的等待树里找不到任何等这个 fd 的协程 → assert(co != NULL) → **master 主进程
 * 直接崩**（实测 2/3 复现）。子进程摘干净后，被劫持的调用全部回落到 libc
 * 阻塞实现（NtyCo 每个 hook 都有 sched==NULL 的透传分支），父进程不再受扰。
 *
 * 只 fork 子进程需要处理；slave 侧全量是进程内内联接收，不涉及。
 */
extern void (*repl_engine_postfork_child)(void);

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