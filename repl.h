#ifndef __REPL_H__
#define __REPL_H__

#include "server.h"
#include "kvstore.h"
#include <stdbool.h>
#include <pthread.h>

// 角色定义
#define REPL_MASTER  0
#define REPL_SLAVE   1

// 复制配置
#define MAX_SLAVES          16
#define REPL_BACKLOG_SIZE   (1024 * 1024)
#define REPL_LISTEN_PORT    6380    // 复制专用端口

// 全局变量
extern int repl_role;
extern int repl_listen_fd;          // 主机复制监听fd
extern pthread_t repl_thread;       // 复制网络线程

// 对外接口
void repl_init(void);
void repl_add_slave(int fd);
void repl_feed_backlog(const char *cmd, int len);
void repl_propagate(const char *cmd, int len);
void repl_switch_to_master(void);
void repl_switch_to_slave(const char *ip, int port);

// 内部线程函数
void* repl_master_thread(void *arg);
void* repl_slave_thread(void *arg);

#endif