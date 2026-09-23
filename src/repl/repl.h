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
#include "protocol.h"    /* kvs_token_t / dispatch_result_t：PSYNC 控制命令走命令分发 */

struct rbuf;             /* rbuf 节点队列读缓冲（net/rbuf.h）：rbuf 形态 handler
                          * 的参数类型。本头只依赖类型名，不拉进网络存储头 */


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
 *    · 数据到达 → client_handler（slave：握手+回放）；master 无专属 handler，
 *      PSYNC 作为控制命令经命令分发层 kvs_dispatch 的 PSYNC 分支接管
 *    · 每轮循环 → repl_tick   （slave 连接 / 重连 / 全量交接推进）
 *    · 连接释放 → repl_on_close（master：清理 streaming 从库 / slave：安排重连）
 *
 *  Slave 侧状态（链路存在性 + 标志位，无独立枚举，详见 repl_incr_slave.c）：
 *    链路存在性由 g_repl_fd 表达，子状态由 g_handshake_done / g_fullsync_done /
 *    g_sync_done 区分：
 *      fd>=0 + !handshake_done     → 待握手（等 +CONTINUE / +FULLRESYNC）
 *      fd>=0 +  handshake_done     → 流式回放
 *      fd<0  +  !sync_done         → 待连接 / 重连
 *      g_sync_done=1               → 终结（仅全量完成 / 全量后 rejudge 失败）
 *
 *    转移：
 *      待握手  --+CONTINUE--> 流式回放（fullsync_done 清零）
 *      待握手  --+FULLRESYNC（fullsync_done=0）--> 摘链 + 内联数据面全量
 *               ├─ 全量成功，incr on --> 无链路（fullsync_done=1，重连 rejudge）
 *               ├─ 全量成功，incr off--> g_sync_done 终结（仅全量模式）
 *               └─ 全量失败--> 无链路（fullsync_done 保持 0，1s 重试）
 *      待握手  --+FULLRESYNC（fullsync_done=1，全量后 rejudge 仍失败）
 *               --> g_sync_done 终结（增量同步失败，断开）
 *      待握手  --意外响应--> 无链路（1s 后重试）
 *      流式回放 --on_close: 断开--> 无链路（offset 保留，1s 重试）
 *
 *  终结由 g_sync_done 标志表达：仅全量模式完成 / 全量后 rejudge 失败，
 *  tick 入口直接返回，不再连接。统一重试时间戳 g_next_retry 只承担退避：
 *  now < g_next_retry 时 tick 不行动（初始 / 续传 = 0，立即可行动）。
 *
 *  Master 侧（单 streaming 从库，g_slave_fd >= 0 即 streaming）：
 *    无从库 ──PSYNC 决策点（offset 在 backlog 窗口内且 incr on）──> +CONTINUE
 *              + backlog 补发 → streaming（实时传播）
 *         └── 窗口外 / incr off 无 backlog ──> +FULLRESYNC
 *              → fork 数据面传输子进程 → 关连接
 *
 *  交接契约（跨平面）：
 *    +CONTINUE   → 控制面流式回放（backlog 补发 + 实时命令流推送）
 *    +FULLRESYNC → 数据面 RDB 全量；流头 token（g_repl_snapshot_offset）
 *                  为权威续传 offset，从库复位后重新 PSYNC 续连；
 *                  续连若再次 +FULLRESYNC（backlog 已回绕）→ 增量同步失败终结
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
 * Master 端 PSYNC 命令处理（命令分发层入口，kvstore_dispatch.c 的 PSYNC 分支调用）
 *
 * 仅当 repl_engine_append!=NULL 且本机 repl_role==MASTER 时被 dispatch 接管。
 * 决策：offset 纯数字且在 backlog 窗口内（incr on）→ 写 +CONTINUE + backlog
 * 回补进 wb，置 g_slave_fd = 当前连接 fd（取 g_kvs_drain_fd），返回
 * DISPATCH_OK（不关连接，可继续管道命令 / P4 不设 streaming guard）；
 * 否则 → 写 +FULLRESYNC 进 wb、repl_fullsync_master_trigger() fork 数据面子进程，
 * 返回 DISPATCH_CLOSE（引擎冲刷后关 = QUIT 同路径）。
 * argc!=2 / offset 非纯数字 → 写错误并返回 DISPATCH_CLOSE。
 * journaled 不置位；回复与普通命令同路径直接 dynbuf_cat(wb)（wb 即引擎连接缓冲）。
 *
 * @param tokens 已解析 token（tokens[0] 已大写）
 * @param argc 参数个数
 * @param wb 引擎写缓冲（drain 返回后引擎冲刷）
 * @return DISPATCH_OK（流式，继续）/ DISPATCH_CLOSE（全量或错误，冲刷后关）
 */
dispatch_result_t repl_master_psync_handle(kvs_token_t *tokens, int argc, DynBuf *wb);

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

/**
 * 查询当前 streaming 从库连接 fd（无则 -1）
 *
 * 用途：引擎在自己那条连接上处理完 PSYNC 后，据此判断"本连接刚变成从库链路"，
 * 从而把出站字节流的冲刷权收归自己（ntyco：改由不让出的 ticker 独占冲刷，
 * 详见 src/net/ntyco.c 的 repl 段注释）。
 *
 * @return streaming 从库 fd；-1 = 无
 */
int repl_incr_master_streaming_fd(void);

/*-------------------------------------------------------------------------
 *  Slave 侧接口
 *-------------------------------------------------------------------------*/

/**
 * 启动 Slave 增量同步
 *
 * 功能：
 * 1. 记录 Master 地址（IP + 端口）
 * 2. 连接由 tick 推进；全量在 PSYNC 握手内联完成（见 repl_incr_slave.c）
 *
 * @param master_ip Master IP 地址
 * @param master_port Master 端口
 * @return 0 成功
 */
int repl_incr_slave_start(const char *master_ip, int master_port);

/**
 * Slave 端 Master 链路 handler（slave_adopt 注册给引擎）
 *
 * 链路读缓冲与普通连接一致（rbuf 节点队列）：
 *  - 握手未完成（fd>=0 + !handshake_done）：解析 PSYNC 响应第一行
 *    （rbuf 逻辑流按行扫前缀，+CONTINUE / +FULLRESYNC / 意外）；
 *  - 握手已完成（handshake_done）：流式回放复用协议层 rbuf drain
 *    （kvs_drain_rbuf_commands），整命令粒度与 AOF 一致，offset 推进 =
 *    drain 消费字节；协议错误经 want_close → on_close 安排重连。
 * 由 repl_engine_register_rbuf 挂载，对 reactor/proactor 通用。
 *
 * @param fd 连接 fd
 * @param rb 读缓冲（节点队列）
 * @param wb 写缓冲
 * @param want_close 关闭标志
 * @return 处理的命令数，-1 表示协议错误
 */
int repl_incr_slave_link_rbuf_handler(int fd, struct rbuf *rb, DynBuf *wb,
                                      int *want_close);

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
 * 调用者：kvs_journal_effect（kvstore_dispatch.c）
 *         ——backlog 追加在钩子内完成，本函数只负责推送
 *
 * @param resp_data RESP 格式的命令数据
 * @param len 数据长度
 *
 * 注意：热路径函数，应尽量高效
 */
void repl_incr_propagate(const char *resp_data, int len);

#endif /* __REPL_H__ */
