#include <errno.h>
#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <stdlib.h>
#include <fcntl.h>

#include "server.h"
#include "kvstore.h"
#include "block.h"

#define MAX_PORTS  20

#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

/* ── Dynamic connection pool ── */
static struct conn *conn_pool = NULL;
static int          conn_pool_size = 0;

#if ENABLE_KVSTORE
extern int epfd;

/* ── Lazy event mask update: only epoll_ctl when mask actually changed ── */
static void conn_set_events(int fd, struct conn *c) {
    uint32_t ev = EPOLLIN;
    if (c->write_buf.len > 0)
        ev |= EPOLLOUT;
    if (c->client && c->client->state == CLIENT_BLOCKED)
        ev &= ~EPOLLIN;

    if (ev == c->cur_events)
        return;                      /* no change — skip syscall */
    c->cur_events = ev;

    struct epoll_event e;
    e.events = ev;
    e.data.fd = fd;
    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &e);
}

/* ── Notify callback: block subsystem calls this when a client is unblocked ── */
static void reactor_block_notify(client_t *c) {
    if (!c || !c->conn) return;
    struct conn *conn = c->conn;
    /* Drain remaining pipelined commands from the read buffer */
    if (conn->read_buf.len > 0) {
        conn->w_off = 0;
        kvs_drain_resp_commands_off(&conn->read_buf, &conn->r_off,
                                     &conn->write_buf, conn);
    }
    conn_set_events(conn->fd, conn);
}
#endif

int accept_cb(int fd);
int recv_cb(int fd);
int send_cb(int fd);

int epfd = 0;
struct timeval begin;

int set_event(int fd, int event, int flag) {
    if (flag) {
        struct epoll_event ev;
        ev.events = event;
        ev.data.fd = fd;
        epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
    } else {
        struct epoll_event ev;
        ev.events = event;
        ev.data.fd = fd;
        epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
    }
}

int event_register(int fd, int event) {
    if (fd < 0) return -1;
    if (fd >= conn_pool_size) {
        fprintf(stderr, "[reactor] fd %d exceeds maxclients %d\n", fd, conn_pool_size);
        close(fd);
        return -1;
    }

    conn_pool[fd].fd = fd;
    conn_pool[fd].active = 1;
    conn_pool[fd].cur_events = (uint32_t)event;
    conn_pool[fd].r_action.recv_callback = recv_cb;
    conn_pool[fd].send_callback = send_cb;

    dynbuf_free(&conn_pool[fd].read_buf);
    dynbuf_free(&conn_pool[fd].write_buf);
    dynbuf_init(&conn_pool[fd].read_buf, KVS_NET_BUF_INIT);
    dynbuf_init(&conn_pool[fd].write_buf, KVS_NET_BUF_INIT);
    conn_pool[fd].r_off = 0;
    conn_pool[fd].w_off = 0;

    if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1)
        perror("fcntl O_NONBLOCK");

    set_event(fd, event, 1);
    return 0;
}

/* ── Release a connection slot ── */
static void conn_release(int fd) {
    if (fd < 0 || fd >= conn_pool_size) return;
    struct conn *c = &conn_pool[fd];
    if (c->client) {
        block_remove_client(c->client);
        free(c->client);
        c->client = NULL;
    }
    dynbuf_free(&c->read_buf);
    dynbuf_free(&c->write_buf);
    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
    close(fd);
    memset(c, 0, sizeof(*c));
}

int accept_cb(int fd) {
    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);

    int clientfd = accept(fd, (struct sockaddr*)&clientaddr, &len);
    if (clientfd < 0) {
        printf("accept errno: %d --> %s\n", errno, strerror(errno));
        return -1;
    }

    event_register(clientfd, EPOLLIN);
    return 0;
}

int recv_cb(int fd) {
    if (fd < 0 || fd >= conn_pool_size) return -1;
    struct conn* c = &conn_pool[fd];
    int eof = 0;

    for (;;) {
        int tail = c->r_off + c->read_buf.len;
        int remain = c->read_buf.cap - tail;
        if (remain <= 0) {
            char *new_buf = realloc(c->read_buf.buf, c->read_buf.cap * 2);
            if (!new_buf) return -1;
            c->read_buf.buf = new_buf;
            c->read_buf.cap *= 2;
            remain = c->read_buf.cap - tail;
        }

        int n = recv(fd, c->read_buf.buf + tail, remain, 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            conn_release(fd);
            return -1;
        }
        if (n == 0) { eof = 1; break; }
        c->read_buf.len += n;
    }

#if ENABLE_KVSTORE
    c->w_off = 0;
    kvs_drain_resp_commands_off(&c->read_buf, &c->r_off, &c->write_buf, c);

    /* QUIT / protocol error path: drain set c->status = 1 to request
       close-after-flush. If nothing to flush, close now; else let send_cb
       drain the buffer first, then close. */
    if (c->status) {
        if (c->write_buf.len == 0) {
            conn_release(fd);
            return 0;
        }
        /* Disable EPOLLIN, keep EPOLLOUT to flush remaining bytes */
        struct epoll_event ev;
        ev.events = EPOLLOUT;
        ev.data.fd = fd;
        c->cur_events = EPOLLOUT;
        epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
        return 0;
    }

    /* If client became blocked, don't re-enable EPOLLIN */
    if (c->client && c->client->state == CLIENT_BLOCKED) {
        conn_set_events(fd, c);
        return 0;
    }
    conn_set_events(fd, c);
#endif

    /* Defer close until response is sent, or close immediately if idle */
    if (eof && c->write_buf.len == 0) {
        conn_release(fd);
        return -1;
    }
    return 0;
}

int send_cb(int fd) {
    if (fd < 0 || fd >= conn_pool_size) return -1;
    struct conn *c = &conn_pool[fd];
    DynBuf *wb = &c->write_buf;

    while (wb->len > 0) {
        ssize_t n = send(fd, wb->buf + c->w_off, wb->len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            conn_release(fd);
            return -1;
        }
        if (n == 0) break;
        c->w_off += (int)n;
        wb->len -= (int)n;
    }

    if (wb->len == 0) {
        c->w_off = 0;
        dynbuf_clear(wb);
        /* Close-after-flush requested by QUIT / protocol error. */
        if (c->status) {
            conn_release(fd);
            return 0;
        }
    }

#if ENABLE_KVSTORE
    conn_set_events(fd, c);
#endif
    return 0;
}

int r_init_server(unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);

    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in servaddr;
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(port);

    if (-1 == bind(sockfd, (struct sockaddr*)&servaddr, sizeof(struct sockaddr))) {
        printf("bind failed: %s\n", strerror(errno));
        return -1;
    }

    listen(sockfd, SOMAXCONN);
    return sockfd;
}

int reactor_start(unsigned short port, msg_handler handler) {
    (void)handler;

    conn_pool_size = kvs_config.maxclients;
    conn_pool = (struct conn *)calloc((size_t)conn_pool_size, sizeof(struct conn));
    if (!conn_pool) {
        fprintf(stderr, "[reactor] failed to allocate conn pool for %d clients\n",
                conn_pool_size);
        return -1;
    }
    printf("[reactor] conn pool: %d slots (%zu MB)\n",
           conn_pool_size,
           (sizeof(struct conn) * (size_t)conn_pool_size) / (1024 * 1024));

    epfd = epoll_create(1024);

    int i = 0;
    for (i = 0; i < MAX_PORTS; i++) {
        int sockfd = r_init_server(port + i);
        if (sockfd < 0) {
            fprintf(stderr, "[reactor] failed to bind port %d, skipping\n", port + i);
            continue;
        }
        conn_pool[sockfd].fd = sockfd;
        conn_pool[sockfd].active = 1;
        conn_pool[sockfd].r_action.recv_callback = accept_cb;
        set_event(sockfd, EPOLLIN, 1);
    }

    gettimeofday(&begin, NULL);

    block_set_notify_cb(reactor_block_notify);

    struct epoll_event events[1024];

    while (!g_kvs_stop) {
        update_cached_time();
        int timeout_ms = block_nearest_timeout();
        /* Cap timeout so the loop re-checks g_kvs_stop at least every
         * 100 ms — avoids a race where SIGTERM arrives outside epoll_wait
         * and the signal has already been delivered before we re-enter. */
        if (timeout_ms < 0 || timeout_ms > 100)
            timeout_ms = 100;
        int nready = epoll_wait(epfd, events, 1024, timeout_ms);
        if (nready < 0 && errno == EINTR) break;

        block_check_timeouts();

        for (i = 0; i < nready; i++) {
            int connfd = events[i].data.fd;

            if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                if (connfd >= 0 && connfd < conn_pool_size)
                    conn_release(connfd);
                continue;
            }

            if (connfd < 0 || connfd >= conn_pool_size)
                continue;

            struct conn *c = &conn_pool[connfd];
            if (!c->active)
                continue;

            if (events[i].events & EPOLLIN) {
                c->r_action.recv_callback(connfd);
            }

            if (events[i].events & EPOLLOUT) {
                c->send_callback(connfd);
            }
        }
    }
}
