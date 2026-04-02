#ifndef __REPL_H__
#define __REPL_H__

#include "server.h"
#include "kvstore.h"
#include <stdbool.h>

// 角色定义
#define REPL_MASTER  0
#define REPL_SLAVE   1

// 配置
#define MAX_SLAVES          16
#define REPL_BACKLOG_SIZE   (1024 * 1024)  // 1MB 复制环形缓冲区

// ------------------- 全局变量 -------------------
extern int repl_role;                // 当前角色：主机/从机
extern long long repl_offset;        // 主机全局偏移量
extern char repl_backlog[];          // 复制缓冲区
extern int slave_fds[];              // 从机fd列表
extern int slave_count;              // 从机数量

// ------------------- 对外接口（协议层调用） -------------------
// 初始化复制模块
void repl_init(void);

// 主机：添加从机fd
void repl_add_slave(int fd);

// 主机：写入命令到复制缓冲区
void repl_feed_backlog(const char *cmd, int len);

// 主机：传播命令给所有从机
void repl_propagate(const char *cmd, int len);

// 从机：连接主机
int repl_slave_connect(const char *ip, int port);

// 从机：同步线程（后台接收主机命令）
void* repl_slave_sync_thread(void *arg);

// 处理主从命令：PSYNC / REPLCONF / PING
int repl_handle_command(char **tokens, int count, DynBuf *wb);

// 切换为主机
void repl_switch_to_master(void);

// 切换为从机
void repl_switch_to_slave(const char *ip, int port);

#endif