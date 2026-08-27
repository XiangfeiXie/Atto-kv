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
 *  包含：
 *  1. repl_fullsync.h - 全量同步编排（含内部传输分派）
 *  2. repl_backlog.h  - 复制积压缓冲区
 *  3. repl_incr.h     - 增量同步状态机
 *
 *  使用方式：
 *  外部模块（kvstore.c / kvstore_dispatch.c / kvstore_internal.h）
 *  只需 include 本文件，即可获得全部复制入口
 *============================================================================*/

#ifndef __REPL_H__
#define __REPL_H__

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

#include "repl_fullsync.h"    /* 全量同步编排（Master/Slave 全量流程） */
#include "repl_backlog.h"     /* 复制积压缓冲区（backlog 环形缓冲） */
#include "repl_incr.h"        /* 增量同步（Master/Slave 状态机） */

#endif /* __REPL_H__ */