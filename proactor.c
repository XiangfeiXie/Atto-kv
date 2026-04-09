#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include "server.h"

#define EVENT_ACCEPT    0
#define EVENT_READ      1
#define EVENT_WRITE     2
#define ENTRIES_LENGTH  1024
#define MAX_CONN        1024

static struct conn *g_clients = NULL;
static msg_handler kvs_handler;

struct conn_info {
    int fd;
    int event;
};

int p_init_server(unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    struct sockaddr_in serveraddr;
    memset(&serveraddr, 0, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
    serveraddr.sin_port = htons(port);

    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    if (bind(sockfd, (struct sockaddr*)&serveraddr, sizeof(serveraddr)) == -1) {
        perror("bind");
        return -1;
    }
    listen(sockfd, MAX_CONN);
    return sockfd;
}

void set_event_recv(struct io_uring *ring, int sockfd, void *buf, size_t len) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    struct conn_info ci = {.fd = sockfd, .event = EVENT_READ};
    io_uring_prep_recv(sqe, sockfd, buf, len, 0);
    memcpy(&sqe->user_data, &ci, sizeof(ci));
}

void set_event_send(struct io_uring *ring, int sockfd, void *buf, size_t len) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    struct conn_info ci = {.fd = sockfd, .event = EVENT_WRITE};
    io_uring_prep_send(sqe, sockfd, buf, len, MSG_NOSIGNAL);
    memcpy(&sqe->user_data, &ci, sizeof(ci));
}

void set_event_accept(struct io_uring *ring, int sockfd, struct sockaddr *addr, socklen_t *addrlen) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    struct conn_info ci = {.fd = sockfd, .event = EVENT_ACCEPT};
    io_uring_prep_accept(sqe, sockfd, addr, addrlen, SOCK_NONBLOCK);
    memcpy(&sqe->user_data, &ci, sizeof(ci));
}

int proactor_start(unsigned short port, msg_handler handler) {
    int sockfd = p_init_server(port);
    kvs_handler = handler;

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
            int res = cqe->res;

            if (result.event == EVENT_ACCEPT) {
                // 仅提交一次ACCEPT，杜绝重复
                set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len);
                int connfd = res;
                if (connfd < 0) continue;

                // 初始化缓冲区（和Reactor完全一致，无任何差异）
                dynbuf_init(&g_clients[connfd].read_buf, KVS_NET_BUF_INIT);
                dynbuf_init(&g_clients[connfd].write_buf, KVS_NET_BUF_INIT);
                g_clients[connfd].r_off = 0;
                g_clients[connfd].w_off = 0;

                // 提交第一次读（仅一次）
                struct conn *c = &g_clients[connfd];
                size_t read_pos = c->r_off + c->read_buf.len;
                set_event_recv(&ring, connfd,
                    c->read_buf.buf + read_pos,
                    c->read_buf.cap - read_pos);
            }
            else if (result.event == EVENT_READ) {
                struct conn *c = &g_clients[fd];
                DynBuf *rb = &c->read_buf;
                DynBuf *wb = &c->write_buf;

                // 连接关闭/错误
                if (res <= 0) {
                    close(fd);
                    dynbuf_free(rb);
                    dynbuf_free(wb);
                    continue;
                }

                // ✅ 核心1：仅追加数据，不覆盖、不重复
                rb->len += res;

                // ✅ 核心2：仅解析一次！（和Reactor完全一致）
                kvs_drain_resp_commands_off(rb, &c->r_off, wb);

                // 发送逻辑
                if (wb->len > 0) {
                    set_event_send(&ring, fd, wb->buf + c->w_off, wb->len);
                } else {
                    // 无数据可发，直接提交读
                    size_t read_pos = c->r_off + rb->len;
                    set_event_recv(&ring, fd,
                        rb->buf + read_pos,
                        rb->cap - read_pos);
                }

                // 自动扩容
                if (c->r_off + rb->len + 1024 > rb->cap) {
                    char *new_buf = realloc(rb->buf, rb->cap * 2);
                    if (!new_buf) {
                        close(fd);
                        dynbuf_free(rb);
                        dynbuf_free(wb);
                        continue;
                    }
                    rb->buf = new_buf;
                    rb->cap *= 2;
                }
            }
            else if (result.event == EVENT_WRITE) {
                struct conn *c = &g_clients[fd];
                DynBuf *wb = &c->write_buf;

                if (res <= 0) {
                    close(fd);
                    dynbuf_free(&c->read_buf);
                    dynbuf_free(wb);
                    continue;
                }

                // 更新发送偏移
                c->w_off += res;
                wb->len -= res;

                if (wb->len > 0) {
                    // 半包：继续发送
                    set_event_send(&ring, fd, wb->buf + c->w_off, wb->len);
                } else {
                    // 发送完成：重置缓冲区
                    c->w_off = 0;
                    dynbuf_clear(wb);

                    // 提交下一次读（仅一次）
                    size_t read_pos = c->r_off + c->read_buf.len;
                    set_event_recv(&ring, fd,
                        c->read_buf.buf + read_pos,
                        c->read_buf.cap - read_pos);
                }
            }
        }
        io_uring_cq_advance(&ring, nready);
    }

    io_uring_queue_exit(&ring);
    free(g_clients);
    return 0;
}