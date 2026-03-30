#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
// 修改点1：包含正确的头文件
#include "server.h"

#define EVENT_ACCEPT    0
#define EVENT_READ      1
#define EVENT_WRITE     2
#define ENTRIES_LENGTH  1024
#define MAX_CONN 1024  // 最大连接数

// 修改点2：定义全局连接池（使用server.h的struct conn，无错误）
static struct conn *g_clients = NULL;
static msg_handler kvs_handler;

struct conn_info {
	int fd;
	int event;
};

// 初始化服务器（无修改）
int p_init_server(unsigned short port) {	
	int sockfd = socket(AF_INET, SOCK_STREAM, 0);	
	struct sockaddr_in serveraddr;	
	memset(&serveraddr, 0, sizeof(serveraddr));	
	serveraddr.sin_family = AF_INET;	
	serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);	
	serveraddr.sin_port = htons(port);	

	if (bind(sockfd, (struct sockaddr*)&serveraddr, sizeof(serveraddr)) == -1) {		
		perror("bind");		
		return -1;	
	}	
	listen(sockfd, 10);
	return sockfd;
}

// 注册recv事件（适配DynBuf）
void set_event_recv(struct io_uring *ring, int sockfd, void *buf, size_t len) {
	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
	struct conn_info ci = {.fd = sockfd, .event = EVENT_READ};
	io_uring_prep_recv(sqe, sockfd, buf, len, 0);
	memcpy(&sqe->user_data, &ci, sizeof(ci));
}

// 注册send事件（适配DynBuf）
void set_event_send(struct io_uring *ring, int sockfd, void *buf, size_t len) {
	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
	struct conn_info ci = {.fd = sockfd, .event = EVENT_WRITE};
	io_uring_prep_send(sqe, sockfd, buf, len, 0);
	memcpy(&sqe->user_data, &ci, sizeof(ci));
}

// 注册accept事件（无修改）
void set_event_accept(struct io_uring *ring, int sockfd, struct sockaddr *addr, socklen_t *addrlen) {
	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
	struct conn_info ci = {.fd = sockfd, .event = EVENT_ACCEPT};
	io_uring_prep_accept(sqe, sockfd, addr, addrlen, 0);
	memcpy(&sqe->user_data, &ci, sizeof(ci));
}

// 主函数（核心修复，无任何旧代码残留）
int proactor_start(unsigned short port, msg_handler handler) {
    int sockfd = p_init_server(port);
    kvs_handler = handler;

    // 修改点3：初始化全局连接池（堆分配，防段错误）
    g_clients = (struct conn*)calloc(MAX_CONN, sizeof(struct conn));

    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    struct io_uring ring;
    io_uring_queue_init_params(ENTRIES_LENGTH, &ring, &params);

    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);
    set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len);

    while (1) {
        io_uring_submit(&ring);
        struct io_uring_cqe *cqes[128];
        int nready = io_uring_peek_batch_cqe(&ring, cqes, 128);

        for (int i = 0; i < nready; i++) {
            struct io_uring_cqe *cqe = cqes[i];
            struct conn_info result;
            memcpy(&result, &cqe->user_data, sizeof(result));
            int fd = result.fd;

            if (result.event == EVENT_ACCEPT) {
                set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len);
                int connfd = cqe->res;

                // 修改点4：初始化动态缓冲区（4096初始大小）
                g_clients[connfd].read_buf.buf = (char*)malloc(4096);
                g_clients[connfd].read_buf.cap = 4096;
                g_clients[connfd].read_buf.len = 0;

                g_clients[connfd].write_buf.buf = (char*)malloc(4096);
                g_clients[connfd].write_buf.cap = 4096;
                g_clients[connfd].write_buf.len = 0;

                // 开始接收数据
                set_event_recv(&ring, connfd,
                               g_clients[connfd].read_buf.buf,
                               g_clients[connfd].read_buf.cap);
            }
            else if (result.event == EVENT_READ) {
                int ret = cqe->res;
                struct conn *c = &g_clients[fd];
                DynBuf *rb = &c->read_buf;

                if (ret <= 0) { close(fd); continue; }

                // 修改点5：更新缓冲区长度（无cqe->buf错误）
                rb->len += ret;

                // 流式解析RESP协议
                while (1) {
                    int cmd_len = resp_try_parse_full_cmd(rb->buf, rb->len);
                    if (cmd_len <= 0) break;

                    // 修改点6：调用KV协议（动态缓冲区，支持超长文本）
                    DynBuf *wb = &c->write_buf;
                    wb->len = 0;
                    kvs_handler(rb->buf, cmd_len, wb);

                    // 发送回复
                    set_event_send(&ring, fd, wb->buf, wb->len);

                    // 移除已处理的命令
                    memmove(rb->buf, rb->buf + cmd_len, rb->len - cmd_len);
                    rb->len -= cmd_len;
                }

                // 继续接收
                set_event_recv(&ring, fd,
                               rb->buf + rb->len,
                               rb->cap - rb->len);
            }
            else if (result.event == EVENT_WRITE) {
                struct conn *c = &g_clients[fd];
                // 发送完成，继续接收
                set_event_recv(&ring, fd,
                               c->read_buf.buf + c->read_buf.len,
                               c->read_buf.cap - c->read_buf.len);
            }
        }
        io_uring_cq_advance(&ring, nready);
    }
}