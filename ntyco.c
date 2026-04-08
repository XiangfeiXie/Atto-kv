#include "nty_coroutine.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <fcntl.h>
#include "server.h"
#include "kvstore.h"

// 全局协议处理器
msg_handler kvs_handler = NULL;

// 协程客户端结构体（堆分配，高并发安全）
typedef struct {
    int fd;
    DynBuf read_buf;   // 动态读缓冲区
    DynBuf write_buf;  // 动态写缓冲区
    int r_off;         // 读偏移（避免 memmove）
    int w_off;         // 写偏移（避免 memmove）
} NtyClient;

// 协程读包 + 协议解析 + 处理（修复：使用nty异步IO，堆分配客户端）
void server_reader(void *arg) {
    // 从堆上获取客户端结构体（修复：栈变量崩溃问题）
    NtyClient *client = (NtyClient *)arg;
    int fd = client->fd;

    while (1) {
        // 动态缓冲区扩容
        int tail = client->r_off + client->read_buf.len;
        int remain = client->read_buf.cap - tail;
        if (remain <= 0) {
            int new_cap = client->read_buf.cap * 2;
            char *nb = realloc(client->read_buf.buf, new_cap);
            if (!nb) break;
            client->read_buf.buf = nb;
            client->read_buf.cap = new_cap;
            tail = client->r_off + client->read_buf.len;
            remain = client->read_buf.cap - tail;
        }

        // 修复：使用NtyCo协程异步recv，不阻塞调度器
        int ret = nty_recv(fd, client->read_buf.buf + tail, remain, 0);
        if (ret <= 0) break; // 客户端断开
        client->read_buf.len += ret;

        // 协议解析
        dynbuf_clear(&client->write_buf);
        client->w_off = 0;
        kvs_drain_resp_commands_off(&client->read_buf, &client->r_off, &client->write_buf);

        // 修复：使用NtyCo协程异步send
        while (client->write_buf.len > 0) {
            ssize_t count = nty_send(fd, client->write_buf.buf + client->w_off, client->write_buf.len, MSG_NOSIGNAL);
            if (count <= 0) break;
            client->w_off += (int)count;
            client->write_buf.len -= (int)count;
        }
        if (client->write_buf.len == 0) client->w_off = 0;
    }

    // 释放资源
    dynbuf_free(&client->read_buf);
    dynbuf_free(&client->write_buf);
    close(fd);
    free(client); // 堆分配释放
}

// 监听协程（修复：nty_accept + 端口复用 + 错误处理）
void server(void *arg) {
    unsigned short port = *(unsigned short *)arg;

    // 1. 创建套接字
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket failed");
        return;
    }

    // 2. 端口复用（修复：重启绑定失败问题）
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // 3. 绑定地址
    struct sockaddr_in local;
    local.sin_family = AF_INET;
    local.sin_port = htons(port);
    local.sin_addr.s_addr = INADDR_ANY;
    if (bind(listen_fd, (struct sockaddr *)&local, sizeof(local)) < 0) {
        perror("bind failed");
        close(listen_fd);
        return;
    }

    // 4. 监听
    if (listen(listen_fd, 20) < 0) {
        perror("listen failed");
        close(listen_fd);
        return;
    }
    printf("✅ NtyCo协程模型启动，监听端口：%d\n", port);

    // 5. 协程accept（修复：异步accept）
    while (1) {
        struct sockaddr_in remote;
        socklen_t len = sizeof(remote);
        // 修复：使用NtyCo协程accept
        int cli_fd = nty_accept(listen_fd, (struct sockaddr *)&remote, &len);
        if (cli_fd < 0) continue;

        // 堆分配客户端结构体（彻底解决栈变量问题）
        NtyClient *client = (NtyClient *)malloc(sizeof(NtyClient));
        if (!client) { close(cli_fd); continue; }

        // 初始化客户端
        client->fd = cli_fd;
        dynbuf_init(&client->read_buf, KVS_NET_BUF_INIT);
        dynbuf_init(&client->write_buf, KVS_NET_BUF_INIT);
        client->r_off = 0;
        client->w_off = 0;

        // 创建协程处理客户端
        nty_coroutine *read_co;
        nty_coroutine_create(&read_co, server_reader, client);
    }
}

// 启动入口
int ntyco_start(unsigned short port, msg_handler handler) {
    kvs_handler = handler;

    // 创建监听协程
    nty_coroutine *co = NULL;
    nty_coroutine_create(&co, server, &port);

    // 启动协程调度器
    nty_schedule_run();
    return 0;
}