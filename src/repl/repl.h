/*=============================================================================
 *  repl.h — 复制模块统一对外接口
 *
 *  功能：提供复制模块的所有对外入口
 *
 *  分层架构：
 *    src/transport/  传输实现层（sendfile.c / rdma.c / rdma_transport.c）
 *                    —— 只负责搬字节，由 repl_fullsync 按配置开关直接调用
 *    src/repl/       同步逻辑层（repl_fullsync / repl_backlog / repl_incr）
 *                    —— 传输分派收敛在 repl_fullsync.c，其余同步逻辑传输无关
 *
 *  内容：
 *  1. include repl_fullsync.h — 全量同步编排（含内部传输分派）
 *  2. 复制积压缓冲区（原 repl_backlog.h，已并入本文件）
 *  3. 增量同步状态机（原 repl_incr.h，已并入本文件）
 *
 *  使用方式：
 *  外部模块（kvstore.c / kvstore_dispatch.c / kvstore_internal.h）
 *  只需 include 本文件，即可获得全部复制入口
 *============================================================================*/

#ifndef __REPL_H__
#define __REPL_H__

#include <stddef.h>      /* size_t：backlog 偏移量 */
#include "dynbuf.h"      /* DynBuf：增量接口 */

/*=============================================================================
 *  双平面架构
 *
 *  主从复制横跨两个正交平面，由上层调用者（kvstore.c）分别驱动：
 *
 *  控制面（主服务端口）：PSYNC 握手 + 增量命令流
 *    - repl_incr_master.c / repl_incr_slave.c   PSYNC 状态机
 *    - repl_backlog.c                           积压缓冲（+CONTINUE 补发）
 *    - 经 repl_engine_*（注册/追加/释放）+ repl_on_close + repl_tick
 *      与 proactor / reactor 引擎互通
 *
 *  数据面（独立全量端口）：RDB 快照全量传输（+FULLRESYNC 触发）
 *    - repl_fullsync.c   Master fork 传输子进程 / Slave 接收并加载快照
 *    - 内部按 fullsync_transport 配置分派 sendfile / rdma 实现
 *
 *  两者关系：全量完成后数据面交出快照 offset（g_repl_snapshot_offset），
 *  控制面据此从该 offset 续传增量。两个平面互不阻塞、各自独立。
 *============================================================================*/

/*=============================================================================
 *  控制面状态机总览（PSYNC 驱动）
 *
 *  引擎三个回调是控制面全部跳转的入口，控制面逻辑以它们为骨架：
 *    · 数据到达 → client_handler（master：PSYNC 握手决策 / slave：握手+回放）
 *    · 每轮循环 → repl_tick   （slave 连接 / 重连 / 全量交接推进）
 *    · 连接释放 → repl_on_close（master：清理 streaming 从库 / slave：安排重连）
 *
 *  Slave 侧状态（4 态，详见 repl_incr_slave.c）：
 *    IDLE ──tick: 连接成功/adopt──> HANDSHAKE ──+CONTINUE──> STREAMING
 *     │  ↑                            │                          │
 *     │  └── on_close: 断开（offset 保留，1s 重试）──────────────┘
 *     └── tick: 连接失败（1s 重试）
 *    HANDSHAKE ──+FULLRESYNC──> FULLSYNC ──tick: 数据面全量成功──> IDLE（重连）
 *                                         │       └─ incr off: retry=NEVER 终结
 *                                         └──失败: 保留 FULLSYNC 重试
 *
 *  终结由统一重试时间戳表达：tick 门槛 now < g_next_retry 配合
 *  REPL_RETRY_NEVER（LLONG_MAX）即永久退避（仅全量模式完成一次后）
 *
 *  Master 侧（单 streaming 从库，g_slave.fd >= 0 即 streaming）：
 *    无从库 ──PSYNC 决策点（offset 在 backlog 窗口内且 incr on）──> +CONTINUE
 *              + backlog 补发 → streaming（实时传播）
 *         └── 窗口外 / incr off 无 backlog ──> +FULLRESYNC
 *              → fork 数据面传输子进程 → 关连接
 *
 *  交接契约（跨平面）：
 *    +CONTINUE   → 控制面流式回放（backlog 补发 + 实时命令流推送）
 *    +FULLRESYNC → 数据面 RDB 全量；流头 token（g_repl_snapshot_offset）
 *                  为权威续传 offset，从库复位后重新 PSYNC 续连
 *============================================================================*/

#include "repl_fullsync.h"    /* 全量同步编排（Master/Slave 全量流程） */

/*=============================================================================
 *  复制积压缓冲区（Replication Backlog）
 *
 *  功能：保存原始 RESP 写命令，供从库断线后按偏移量补发
 *  大小：1MB 环形缓冲区
 *
 *  设计要点：
 *  1. 只属于增量同步（incr_sync），与全量同步正交
 *  2. 偏移量是绝对写偏移，单调递增不重置
 *  3. 环形缓冲区，新数据覆盖旧数据
 *  4. 单线程无锁（Master 事件循环）
 *============================================================================*/

/**
 * 积压缓冲区大小
 * 1MB 对于大多数场景足够：
 * - 存储最近几秒到几分钟的写命令
 * - 覆盖从库短暂断线重连的时间窗口
 *
 * 可根据实际 workload 调整
 */
#define REPL_BACKLOG_SIZE      (1024 * 1024)   /* 1 MB */

/**
 * 积压缓冲区结构
 *
 * 内存布局：
 *  +------------------------------------------+
 *  |           buf[REPL_BACKLOG_SIZE]          |
 *  +------------------------------------------+
 *
 *  head: 绝对写偏移（单调递增）
 *  used: 当前有效数据量（<= REPL_BACKLOG_SIZE）
 *
 *  环形位置计算：pos = head % REPL_BACKLOG_SIZE
 *
 *  窗口范围：
 *  - 未满：[0, head]
 *  - 已满：[head - size, head]
 */
typedef struct {
    char   buf[REPL_BACKLOG_SIZE];  /* 环形缓冲区 */
    size_t head;                    /* 绝对写偏移（单调递增，永不重置） */
    size_t used;                    /* 当前存量字节数（<= REPL_BACKLOG_SIZE） */
} repl_backlog_t;

/**
 * 初始化积压缓冲区
 * 清空所有状态，在服务器启动时调用
 */
void repl_backlog_init(void);

/**
 * 追加写命令到积压缓冲区
 *
 * @param resp_data RESP 格式的命令数据（如 "*3\r\n$3\r\nSET\r\n..."）
 * @param len 数据长度
 *
 * 特性：
 * - 环形写入，满则覆盖最旧数据
 * - 单线程调用（Master 事件循环），无锁
 * - len 必须 <= REPL_BACKLOG_SIZE
 *
 * 调用位置：每次成功的写命令后（SET/DEL/FLUSHALL/EXPIRE 等）
 */
void repl_backlog_append(const char *resp_data, int len);

/**
 * 从积压缓冲区获取数据
 *
 * @param start_offset 起始偏移量（来自 Slave 的 PSYNC 请求）
 * @param dst 目标缓冲区（用于发送给 Slave）
 * @param max_len 最大可拷贝字节数
 * @param out_end_offset 输出：拷贝后的新偏移量
 * @return 实际拷贝字节数，-1 表示偏移量无效（需要 FULLRESYNC）
 *
 * 设计要点：
 * - 允许在任意字节处截断（不对齐命令边界）
 * - Slave 端用 RESP 解析器（resp_parse_slices）重组完整命令
 * - 一个大于缓冲区的命令会分多批送达
 *
 * 使用场景：PSYNC 增量同步
 */
int repl_backlog_fetch(size_t start_offset, char *dst, int max_len,
                       size_t *out_end_offset);

/**
 * 获取当前 head 偏移量
 * @return 积压缓冲区当前最大偏移量
 *
 * 用途：
 * - 生成 Master 的复制偏移量
 * - Slave 的 PSYNC 请求中使用
 */
size_t repl_backlog_offset(void);

/**
 * 检查偏移量是否在积压窗口内
 * @param offset 要检查的偏移量
 * @return 1 有效（在窗口内），0 无效（需要 FULLRESYNC）
 *
 * 窗口范围：
 * - 缓冲区未满：[0, head]
 * - 缓冲区已满：[head - REPL_BACKLOG_SIZE, head]
 *
 * 特殊设计：
 * offset == head 是"已追平"的合法位置
 * （全量快照刚做完、期间无新写入）
 * 应返回 +CONTINUE 进入 streaming，而非 +FULLRESYNC
 * 因此使用 <= 包含右边界
 *
 * 使用场景：处理 PSYNC 请求时判断是否需要全量同步
 */
int repl_backlog_valid(size_t offset);

/*=============================================================================
 *  主从增量同步（Incremental Sync）
 *
 *  设计概要：
 *  1. Backlog：1MB 环形缓冲区，存原始 RESP 写命令
 *  2. 协议：简化 PSYNC（只有 offset，无 replid）
 *  3. 传输：PSYNC 握手走主服务端口，全量走 transport 层
 *  4. fd 归属：所有复制 fd 交给网络引擎托管
 *  5. 状态机：Master/Slave 两侧各自的协议处理
 *============================================================================*/

/*-------------------------------------------------------------------------
 *  Master 侧接口
 *-------------------------------------------------------------------------*/

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

/*-------------------------------------------------------------------------
 *  Slave 侧接口
 *-------------------------------------------------------------------------*/

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

/*-------------------------------------------------------------------------
 *  写命令传播（Master 热路径）
 *-------------------------------------------------------------------------*/

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

#endif /* __REPL_H__ */
