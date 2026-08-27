/*=============================================================================
 *  repl_fullsync.c — 全量同步编排 + 传输分派
 *
 *  职责：
 *  1. Master 端：fork 传输子进程，发送 RDB 快照
 *  2. Slave 端：由 PSYNC +FULLRESYNC 握手触发，经数据面端口接收并加载快照
 *  3. 子进程回收：单传输子进程 + 统一 SIGCHLD 回收
 *  4. 传输分派：按 fullsync_transport 配置选择 sendfile / RDMA 实现
 *
 *  设计原则：
 *  1. 一主一从：至多一个传输子进程存活（原 session 管理简化）
 *  2. 子进程回收：通过持久化模块统一 SIGCHLD 机制
 *  3. 传输分派：fullsync_server_run / fullsync_client_run 按配置开关直选
 *  4. 快照偏移量：流头 token 传递给增量模块续传
 *============================================================================*/

#include "repl_fullsync.h"
#include "repl_backlog.h"
#include "persistence.h"
#include "kvstore.h"
#include "sendfile.h"
#include "rdma.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <sys/wait.h>

/*=============================================================================
 *  全局状态
 *============================================================================*/

/**
 * 快照偏移量
 * 流头 token = 快照 offset，供增量模块续传
 */
size_t g_repl_snapshot_offset = 0;

/**
 * 当前传输子进程 PID
 * 一主一从：至多一个传输子进程存活
 *
 * 使用 volatile sig_atomic_t：
 * - 被 SIGCHLD 处理器读写
 * - 保证原子性
 */
static volatile sig_atomic_t g_child = -1;

/*=============================================================================
 *  子进程回收
 *  由持久化模块统一 SIGCHLD 处理器分发调用
 *============================================================================*/

/**
 * 子进程回收回调
 *
 * 功能：
 * 1. 匹配当前传输子进程 PID → 清 g_child
 * 2. 记录子进程退出状态（正常退出或信号终止）
 *
 * 调用上下文：异步信号上下文（SIGCHLD handler）
 * 限制：只能读 pid、写 sig_atomic_t、打日志
 */
static void repl_child_reap(pid_t pid, int status) {
    if (pid == g_child) g_child = -1;
    if (WIFEXITED(status))
        printf("[REPL] child %d exit=%d\n", pid, WEXITSTATUS(status));
    else if (WIFSIGNALED(status))
        fprintf(stderr, "[REPL] child %d signal=%d\n", pid, WTERMSIG(status));
}

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
 * 启动 Master 全量同步服务
 * 安装统一 SIGCHLD handler 并注册本模块回收回调
 */
int repl_fullsync_master_start(void) {
    kvs_child_reap_init();           /* 安装统一 SIGCHLD handler（幂等） */
    kvs_child_reap_register(repl_child_reap);  /* 注册本模块回调 */
    return 0;
}

/**
 * 触发 Master 全量同步（PSYNC 握手判定 +FULLRESYNC 时）
 *
 * 流程：
 * 1. 清理上一轮残留子进程（避免占着数据面端口）
 * 2. 创建管道（父子进程通信）
 * 3. fork 传输子进程
 * 4. 子进程调用 fullsync_server_run 发送快照
 * 5. 父进程等待子进程就绪信号
 *
 * @return 0 成功，-1 失败
 */
int repl_fullsync_master_trigger(void) {
    /* 清理上一轮残留子进程
     * 先快照到局部变量，避免 handler 把 g_child 清成 -1 后误伤 kill(-1, ...)
     * 防死锁：先 SIGTERM 再 SIGKILL */
    pid_t old = (pid_t)g_child;
    if (old > 0) {
        kill(old, SIGTERM);
        usleep(50000);  /* 50ms */
        if (kill(old, 0) == 0) {  /* 进程仍在运行 */
            kill(old, SIGKILL);
            usleep(50000);
        }
    }

    /* 创建管道：子进程就绪后通过管道通知父进程 */
    int pipefd[2];
    if (pipe(pipefd) < 0) { perror("pipe"); return -1; }

    /* fork 传输子进程 */
    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        close(pipefd[0]); close(pipefd[1]);
        return -1;
    }

    if (pid == 0) {
        /* 子进程：关闭管道读端，执行传输 */
        close(pipefd[0]);
        _exit(fullsync_server_run(repl_backlog_offset(), pipefd[1]));
    }

    /* 父进程：关闭管道写端，等待子进程就绪 */
    close(pipefd[1]);
    char ready;
    if (read(pipefd[0], &ready, 1) < 0) perror("read pipe");
    close(pipefd[0]);

    g_child = (sig_atomic_t)pid;
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
