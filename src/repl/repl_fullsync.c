/*=============================================================================
 *  repl_fullsync.c — 全量同步编排 + 传输分派
 *
 *  职责：
 *  1. Master 端：fork 传输子进程，发送 RDB 快照
 *  2. Slave 端：由 PSYNC +FULLRESYNC 握手触发，经数据面端口接收并加载快照
 *  3. 子进程回收：全局 SIGCHLD handler 回收（单传输子进程）
 *  4. 传输分派：按 fullsync_transport 配置选择 sendfile / RDMA 实现
 *
 *  设计原则：
 *  1. 一主一从：至多一个传输子进程存活（原 session 管理简化）
 *  2. 子进程回收：全局 SIGCHLD handler 防僵尸，trigger 非阻塞 kill 清理
 *  3. 传输分派：fullsync_server_run / fullsync_client_run 按配置开关直选
 *  4. 快照偏移量：流头 token 传递给增量模块续传
 *============================================================================*/

#include "repl_fullsync.h"
#include "repl.h"
#include "kvstore.h"
#include "sendfile.h"
#include "rdma.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>

/*=============================================================================
 *  全局状态
 *============================================================================*/

/**
 * 快照偏移量（数据面 → 控制面的权威续传 offset）
 *
 * 全量传输的流头 token = 快照对应的 master 绝对 offset。
 * Slave 加载完快照后由 repl_fullsync_slave_start 写入，
 * 增量模块据此续传（repl_incr_slave_reset_offset 对齐）。
 *
 * 交接契约：本 token 是续传 offset 的唯一权威。控制面 +FULLRESYNC 回复不带
 * offset（从库只做前缀匹配，从不解析数字），offset 只经数据面流头这一份拷贝
 * 传递（见 repl_incr_master.c 的 master_start_fullsync）。
 */
size_t g_repl_snapshot_offset = 0;

/**
 * 当前传输子进程 PID（一主一从：至多一个传输子进程存活）
 * 回收方式：全局 SIGCHLD handler 回收；trigger 只做残留进程 kill 清理
 */
static pid_t g_child = -1;

/*=============================================================================
 *  传输分派（原 transport.c）
 *  按配置选择 sendfile / RDMA 实现
 *============================================================================*/

/**
 * 服务端入口分派
 *
 * 按配置选择传输实现：
 *   - "sendfile"：使用 TCP sendfile 传输（默认）
 *   - "rdma"：使用 RDMA 传输
 *
 * 配置非 "sendfile" 时默认走 RDMA，方便通过配置文件切换传输方式
 *
 * 调用时机：repl_fullsync_master_trigger fork 子进程
 */
static int fullsync_server_run(uint64_t token, int ready_fd) {
    if (strcmp(kvs_config.fullsync_transport, "sendfile") == 0)
        return sendfile_server_run(token, ready_fd);
    return rdma_server_run(token, ready_fd);  /* 默认 RDMA */
}

/**
 * 客户端入口分派
 *
 * 与 fullsync_server_run 相同规则选择传输实现
 *
 * 调用时机：repl_fullsync_slave_start
 */
static int fullsync_client_run(uint64_t *out_token) {
    if (strcmp(kvs_config.fullsync_transport, "sendfile") == 0)
        return sendfile_client_run(out_token);
    return rdma_client_run(out_token);  /* 默认 RDMA */
}

/*=============================================================================
 *  Master 端全量同步
 *============================================================================*/

/**
 * 触发 Master 全量同步（PSYNC 握手判定 +FULLRESYNC 时）
 *
 * 流程：
 * 1. 清理上一轮残留子进程（非阻塞 kill，占数据面端口的旧进程直接终止）
 * 2. fork 传输子进程发送快照
 *
 * 不等待子进程就绪（ready_fd=-1）也不阻塞回收：
 * - slave 数据面连接有重试兜底
 * - 子进程退出由全局 SIGCHLD handler 回收
 * - 避免事件循环被 RDMA 慢初始化（可长达数秒）阻塞
 *
 * @return 0 成功，-1 失败
 */
int repl_fullsync_master_trigger(void) {
    /* 清理上一轮残留子进程（非阻塞，不阻塞主循环）：
     * 已退出（SIGCHLD handler 已回收）→ kill 返回 ESRCH 跳过；
     * 仍存活 → SIGKILL 终止，socket 由内核释放，
     * 新子进程 bind 竞态由 slave 数据面重试兜底 */
    pid_t old = g_child;
    if (old > 0) {
        if (kill(old, 0) == 0)   /* 仍存活才需要终止 */
            kill(old, SIGKILL);
        g_child = -1;
    }

    /* fork 传输子进程 */
    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return -1;
    }

    if (pid == 0) {
        _exit(fullsync_server_run(repl_backlog_offset(), -1));  /* 无就绪管道 */
    }

    g_child = pid;
    return 0;
}

/*=============================================================================
 *  Slave 端全量同步
 *============================================================================*/

/**
 * Slave 端全量同步
 *
 * 由 PSYNC 握手的 +FULLRESYNC 判定触发（Redis 模型），无独立触发命令。
 * 流程：
 * 1. 调用 fullsync_client_run 连接数据面端口并接收快照
 * 2. 流头 token（快照 offset）写入 g_repl_snapshot_offset
 * 3. 返回给增量模块续传
 *
 * @return 0 成功，-1 失败
 *
 * 设计说明：
 * 当前实现为 inline 同步方式（阻塞式）：Slave 在本进程内直接接收并加载快照
 */
int repl_fullsync_slave_start(void) {
    /* 接收快照（流头 token = 快照 offset） */
    uint64_t token = 0;
    if (fullsync_client_run(&token) < 0)
        return -1;

    /* 记录快照偏移量，供增量模块续传 */
    g_repl_snapshot_offset = (size_t)token;
    return 0;
}
