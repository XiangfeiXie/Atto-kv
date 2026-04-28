#ifndef __REPL_NET_H__
#define __REPL_NET_H__

#include <stdio.h>
#include <sys/socket.h>

/* REPL_PORT removed: use kvs_config.replport at runtime */

// 复制网络初始化（主机监听复制端口）
int repl_net_init(int port);

// 从机：连接主机复制端口
int repl_net_connect(const char *ip, int port);

// 发送数据（专用复制链路，无粘包）
int repl_net_send(int fd, const char *data, int len);

// 接收数据（专用复制链路）
int repl_net_recv(int fd, char *buf, int len);

// 主机：接受从机连接
int repl_net_accept(int listen_fd);

#endif