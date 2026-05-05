#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include "server.h"
#include "config.h"
#include "protocol.h"

#define EVENT_ACCEPT    0
#define EVENT_READ      1
#define EVENT_WRITE     2
#define ENTRIES_LENGTH  1024

static struct conn *conn_pool = NULL;
static int          conn_pool_size = 0;
static msg_handler  kvs_handler;

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
    listen(sockfd, SOMAXCONN);
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

/* ── Release a connection slot ── */
static void conn_release(int fd) {
    if (fd < 0 || fd >= conn_pool_size) return;
    struct conn *c = &conn_pool[fd];
    dynbuf_free(&c->read_buf);
    dynbuf_free(&c->write_buf);
    close(fd);
    memset(c, 0, sizeof(*c));
}

int proactor_start(unsigned short port, msg_handler handler) {
    int sockfd = p_init_server(port);
    kvs_handler = handler;

    conn_pool_size = kvs_config.maxclients;
    conn_pool = (struct conn *)calloc((size_t)conn_pool_size, sizeof(struct conn));
    if (!conn_pool) {
        fprintf(stderr, "[proactor] failed to allocate conn pool for %d clients\n",
                conn_pool_size);
        return -1;
    }
    printf("[proactor] conn pool: %d slots (%zu MB)\n",
           conn_pool_size,
           (sizeof(struct conn) * (size_t)conn_pool_size) / (1024 * 1024));

    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    struct io_uring ring;
    io_uring_queue_init_params(ENTRIES_LENGTH, &ring, &params);

    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);
    set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len);

    while (1) {
        struct io_uring_cqe *cqes[128];
        int nready = io_uring_peek_batch_cqe(&ring, cqes, 128);

        if (nready == 0) {
            /* Phase 7: before submit_and_wait, check block_nearest_timeout().
               If a timeout exists, submit an IORING_OP_TIMEOUT linked to an
               IORING_OP_READ on a dummy fd to force wake-up at the deadline.
               For now (Phase 0), no connections block, so infinite wait is safe. */
            int ret = io_uring_submit_and_wait(&ring, 1);
            if (ret < 0) {
                if (ret == -EINTR)  continue;
                break;
            }
            nready = io_uring_peek_batch_cqe(&ring, cqes, 128);
            if (nready <= 0) continue;
        }

        for (int i = 0; i < nready; i++) {
            struct io_uring_cqe *cqe = cqes[i];
            struct conn_info result;
            memcpy(&result, &cqe->user_data, sizeof(result));
            int fd = result.fd;
            int res = cqe->res;

            if (result.event == EVENT_ACCEPT) {
                set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len);
                int connfd = res;
                if (connfd < 0) continue;
                if (connfd >= conn_pool_size) {
                    fprintf(stderr, "[proactor] fd %d exceeds maxclients %d, dropping\n",
                            connfd, conn_pool_size);
                    close(connfd);
                    continue;
                }

                dynbuf_init(&conn_pool[connfd].read_buf, KVS_NET_BUF_INIT);
                dynbuf_init(&conn_pool[connfd].write_buf, KVS_NET_BUF_INIT);
                conn_pool[connfd].fd = connfd;
                conn_pool[connfd].active = 1;
                conn_pool[connfd].r_off = 0;
                conn_pool[connfd].w_off = 0;

                struct conn *c = &conn_pool[connfd];
                size_t read_pos = c->r_off + c->read_buf.len;
                set_event_recv(&ring, connfd,
                    c->read_buf.buf + read_pos,
                    c->read_buf.cap - read_pos);
            }
            else if (result.event == EVENT_READ) {
                if (fd < 0 || fd >= conn_pool_size) continue;
                struct conn *c = &conn_pool[fd];
                if (!c->active) continue;
                DynBuf *rb = &c->read_buf;
                DynBuf *wb = &c->write_buf;

                if (res <= 0) {
                    /* EOF/error with unprocessed data — flush before releasing */
                    if (rb->len > 0) {
                        kvs_drain_resp_commands_off(rb, &c->r_off, wb);
                        if (wb->len > 0)
                            set_event_send(&ring, fd, wb->buf + c->w_off, wb->len);
                    }
                    if (wb->len == 0)
                        conn_release(fd);
                    continue;
                }

                rb->len += res;
                kvs_drain_resp_commands_off(rb, &c->r_off, wb);

                if (wb->len > 0) {
                    set_event_send(&ring, fd, wb->buf + c->w_off, wb->len);
                } else if (c->blocked) {
                    /* Phase 7: blocked conn — don't submit recv; wait for wake-up */
                    continue;
                } else {
                    size_t read_pos = c->r_off + rb->len;
                    set_event_recv(&ring, fd,
                        rb->buf + read_pos,
                        rb->cap - read_pos);
                }

                if (c->r_off + rb->len + 1024 > rb->cap) {
                    char *new_buf = realloc(rb->buf, rb->cap * 2);
                    if (!new_buf) {
                        conn_release(fd);
                        continue;
                    }
                    rb->buf = new_buf;
                    rb->cap *= 2;
                }
            }
            else if (result.event == EVENT_WRITE) {
                if (fd < 0 || fd >= conn_pool_size) continue;
                struct conn *c = &conn_pool[fd];
                if (!c->active) continue;
                DynBuf *wb = &c->write_buf;

                if (res <= 0) {
                    conn_release(fd);
                    continue;
                }

                c->w_off += res;
                wb->len -= res;

                if (wb->len > 0) {
                    set_event_send(&ring, fd, wb->buf + c->w_off, wb->len);
                } else {
                    c->w_off = 0;
                    dynbuf_clear(wb);

                    if (c->blocked) {
                        /* Phase 7: blocked conn — don't submit recv */
                        continue;
                    }
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
    free(conn_pool);
    return 0;
}
