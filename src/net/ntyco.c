#include "nty_coroutine.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "server.h"
#include "kvstore.h"

// 全局协议处理器
msg_handler kvs_handler = NULL;

// 协程客户端结构体（堆分配，高并发安全）
typedef struct {
    int fd;
    DynBuf read_buf;   // 动态读缓冲区（粘包/半包核心）
    DynBuf write_buf;  // 动态写缓冲区（批量回复核心）
    int r_off;         // 读偏移（零拷贝，避免memmove）
    int w_off;         // 写偏移（零拷贝，半包发送）
} NtyClient;

/**
 * @brief 客户端协程处理函数：异步读 -> 协议解析 -> 异步发送
 * @param arg 堆分配的NtyClient结构体
 */
void server_reader(void *arg) {
    NtyClient *client = (NtyClient *)arg;
    int fd = client->fd;

    while (1) {
        // 1. 动态缓冲区自动扩容（适配大流量）
        int total_used = client->r_off + client->read_buf.len;
        int free_space = client->read_buf.cap - total_used;
        
        if (free_space <= 0) {
            int new_cap = client->read_buf.cap * 2;
            char *new_buf = realloc(client->read_buf.buf, new_cap);
            if (!new_buf) {
                perror("realloc failed");
                break;
            }
            client->read_buf.buf = new_buf;
            client->read_buf.cap = new_cap;
            free_space = new_cap - total_used;
        }

        // 2. NtyCo异步recv（不阻塞调度器，高并发核心）
        ssize_t ret = nty_recv(fd, client->read_buf.buf + total_used, free_space, 0);
        if (ret <= 0) {
            // 客户端断开/连接错误，退出循环
            break;
        }
        // 追加数据到缓冲区（粘包数据完整保存）
        client->read_buf.len += ret;

        // 3. 协议解析（复用已验证的批量解析函数，不清空write_buf！）
        kvs_drain_resp_commands_off(&client->read_buf, &client->r_off, &client->write_buf);

        // 4. NtyCo异步send（完整处理半包发送，零拷贝）
        while (client->write_buf.len > 0) {
            ssize_t send_len = nty_send(fd, client->write_buf.buf + client->w_off, 
                                       client->write_buf.len, MSG_NOSIGNAL);
            if (send_len <= 0) {
                // 发送失败，断开连接
                goto cleanup;
            }
            // 更新发送偏移和剩余长度
            client->w_off += send_len;
            client->write_buf.len -= send_len;
        }

        // 5. 发送完成：重置偏移 + 清空缓冲区（仅这里清空！）
        if (client->write_buf.len == 0) {
            client->w_off = 0;
            dynbuf_clear(&client->write_buf);
        }
    }

cleanup:
    // 资源释放（堆内存+缓冲区+文件描述符）
    dynbuf_free(&client->read_buf);
    dynbuf_free(&client->write_buf);
    close(fd);
    free(client);
}

/**
 * @brief 设置文件描述符为非阻塞模式
 * @param fd 文件描述符
 */
static void set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/**
 * @brief 监听协程：异步accept客户端连接
 * @param arg 监听端口
 */
void server(void *arg) {
    unsigned short port = *(unsigned short *)arg;

    // 1. 创建TCP套接字
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket failed");
        return;
    }

    // 2. 端口复用+非阻塞（高并发必备）
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    set_nonblock(listen_fd);

    // 3. 绑定地址
    struct sockaddr_in local = {0};
    local.sin_family = AF_INET;
    local.sin_port = htons(port);
    local.sin_addr.s_addr = INADDR_ANY;
    
    if (bind(listen_fd, (struct sockaddr *)&local, sizeof(local)) < 0) {
        perror("bind failed");
        close(listen_fd);
        return;
    }

    // 4. 监听连接
    if (listen(listen_fd, SOMAXCONN) < 0) {
        perror("listen failed");
        close(listen_fd);
        return;
    }
    printf("✅ NtyCo协程模型启动 | 监听端口：%d | 高并发就绪\n", port);

    // 5. 异步accept循环（NtyCo协程模式）
    while (1) {
        struct sockaddr_in remote = {0};
        socklen_t len = sizeof(remote);
        int cli_fd = nty_accept(listen_fd, (struct sockaddr *)&remote, &len);
        
        if (cli_fd < 0) {
            continue;
        }
        // 客户端socket设置非阻塞
        set_nonblock(cli_fd);

        // 6. 堆分配客户端结构体（避免栈溢出，多连接安全）
        NtyClient *client = (NtyClient *)malloc(sizeof(NtyClient));
        if (!client) {
            close(cli_fd);
            continue;
        }

        // 7. 初始化客户端状态（和Reactor/io_uring完全一致）
        client->fd = cli_fd;
        dynbuf_init(&client->read_buf, KVS_NET_BUF_INIT);
        dynbuf_init(&client->write_buf, KVS_NET_BUF_INIT);
        client->r_off = 0;
        client->w_off = 0;

        // 8. 创建协程处理客户端（一对一协程模型）
        nty_coroutine *read_co;
        if (nty_coroutine_create(&read_co, server_reader, client) != 0) {
            // 协程创建失败，释放资源
            dynbuf_free(&client->read_buf);
            dynbuf_free(&client->write_buf);
            free(client);
            close(cli_fd);
        }
    }
}

/**
 * @brief NtyCo协程模型启动入口
 * @param port 监听端口
 * @param handler 协议处理回调
 * @return 0
 */
int ntyco_start(unsigned short port, msg_handler handler) {
    kvs_handler = handler;

    // 创建监听协程
    nty_coroutine *listen_co = NULL;
    nty_coroutine_create(&listen_co, server, &port);

    // 启动NtyCo协程调度器
    nty_schedule_run();
    return 0;
}