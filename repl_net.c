#include "repl_net.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>

// 主机：初始化复制监听端口
int repl_net_init(int port) {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) return -1;

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    if (bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(listen_fd);
        return -1;
    }
    listen(listen_fd, 10);
    printf("✅ 复制网络初始化成功，监听端口：%d\n", port);
    return listen_fd;
}

// 从机：连接主机复制端口
int repl_net_connect(const char *ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip, &addr.sin_addr);

    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    printf("✅ 从机成功连接主机复制链路\n");
    return fd;
}

// 复制链路专用发送（保证发完，无阻塞）
int repl_net_send(int fd, const char *data, int len) {
    int total = 0;
    while (total < len) {
        int n = send(fd, data + total, len - total, 0);
        if (n <= 0) return -1;
        total += n;
    }
    return total;
}

// 复制链路专用接收
int repl_net_recv(int fd, char *buf, int len) {
    return recv(fd, buf, len, 0);
}

// 主机接受从机连接
int repl_net_accept(int listen_fd) {
    struct sockaddr_in addr;
    socklen_t len = sizeof(addr);
    return accept(listen_fd, (struct sockaddr*)&addr, &len);
}