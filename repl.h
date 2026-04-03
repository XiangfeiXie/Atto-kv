#ifndef __REPL_H__
#define __REPL_H__

#include "server.h"
#include "kvstore.h"
#include <stdbool.h>
#include <pthread.h>

#define REPL_MASTER  0
#define REPL_SLAVE   1

#define MAX_SLAVES          16
#define REPL_BACKLOG_SIZE   (1024 * 1024)  // 1MB，和Redis一样固定大小
#define REPL_LISTEN_PORT    6380
#define REPL_PING_INTERVAL  1000

// 从节点信息结构体
typedef struct {
    int fd;
    long long offset;
} SlaveInfo;

// 全局变量声明
extern int repl_role;
extern int repl_listen_fd;
extern pthread_t repl_thread;

// 主节点核心信息
extern char master_runid[40];
extern long long master_offset;
extern SlaveInfo slaves[MAX_SLAVES];
extern int slave_count;

// 复制积压缓冲区（固定大小环形队列）
extern char repl_backlog[REPL_BACKLOG_SIZE];
extern long long backlog_base;    // 缓冲区起始偏移量
extern int backlog_len;           // 缓冲区当前有效长度

// 从节点核心信息
extern char repl_master_ip[64];
extern int repl_master_port;
extern int repl_master_fd;
extern char master_runid_slave[40];
extern long long slave_offset;

// 对外接口
void repl_init(void);
void repl_add_slave(int fd);
void repl_feed_backlog(const char *cmd, int len);
void repl_propagate(const char *cmd, int len);
void repl_switch_to_master(void);
void repl_switch_to_slave(const char *ip, int port);
void repl_handle_psync(int slave_fd, char **tokens, DynBuf *wb);
void repl_handle_replconf(char **tokens, DynBuf *wb);

// 线程函数
void* repl_master_thread(void *arg);
void* repl_slave_thread(void *arg);

#endif