#include <errno.h>
#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>     /* TCP_NODELAY */
#include <string.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <stdlib.h>
#include <fcntl.h>

#include "server.h"
#include "kvstore.h"
#include "protocol.h"
#include "config.h"


/* ── Global epoll fd — must be declared before conn_set_events ── */
int epfd = 0;

/* ── Dynamic connection pool ── */
static struct conn *conn_pool = NULL;
static int          conn_pool_size = 0;

/* ── LT event mask: EPOLLIN always armed (level-triggered — no re-arm
 *   needed).  EPOLLOUT toggled dynamically: added when write_buf has
 *   data, removed when empty to avoid LT write-ready storm.
 *
 *   When c->status is set (close-after-flush), EPOLLIN is suppressed
 *   so the peer cannot inject more commands after QUIT / proto error. ── */
static void conn_set_events(int fd, struct conn *c) {
    uint32_t want = c->status ? 0 : EPOLLIN;
    if (c->write_buf.len > 0)
        want |= EPOLLOUT;

    if (want == c->cur_events)
        return;                      /* no change — skip syscall */
    c->cur_events = want;

    struct epoll_event e;
    e.events = want;
    e.data.fd = fd;
    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &e);
}

int recv_cb(int fd);
int send_cb(int fd);

int event_register(int fd) {
    if (fd < 0) return -1;
    if (fd >= conn_pool_size) {
        fprintf(stderr, "[reactor] fd %d exceeds maxclients %d\n", fd, conn_pool_size);
        close(fd);
        return -1;
    }

    struct conn *c = &conn_pool[fd];
    c->fd = fd;
    c->active = 1;
    c->r_action.recv_callback = recv_cb;
    c->send_callback = send_cb;
    c->status = 0;
    c->client = NULL;

    dynbuf_free(&c->read_buf);
    dynbuf_free(&c->write_buf);
    dynbuf_init(&c->read_buf, KVS_NET_BUF_INIT);
    dynbuf_init(&c->write_buf, KVS_NET_BUF_INIT);
    c->r_off = 0;
    c->w_off = 0;

    /* Non-blocking + TCP_NODELAY for low latency */
    if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1)
        perror("fcntl O_NONBLOCK");

    int nodelay = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    struct epoll_event ev;
    ev.events = EPOLLIN;                  /* LT: EPOLLIN only, no EPOLLET */
    ev.data.fd = fd;
    c->cur_events = EPOLLIN;
    epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
    return 0;
}

/* ── Release a connection slot ── */
static void conn_release(int fd) {
    if (fd < 0 || fd >= conn_pool_size) return;
    struct conn *c = &conn_pool[fd];

    if (c->client) {
        free(c->client);
        c->client = NULL;
    }

    /* Free buffers */
    dynbuf_free(&c->read_buf);
    dynbuf_free(&c->write_buf);

    /* Remove from epoll and close socket */
    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
    close(fd);

    /* Zero out the connection slot */
    memset(c, 0, sizeof(*c));
}

int accept_cb(int fd) {
    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);

    /* Level-triggered: drain all pending accepts in one go */
    for (;;) {
        int clientfd = accept(fd, (struct sockaddr*)&clientaddr, &len);
        if (clientfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            fprintf(stderr, "[reactor] accept error: %s\n", strerror(errno));
            return -1;
        }

        if (event_register(clientfd) != 0) {
            close(clientfd);
            continue;
        }

        /* Allocate client state immediately (Phase 8.2 pattern) */
        struct conn *c = &conn_pool[clientfd];
        if (!c->client) {
            c->client = (client_t *)calloc(1, sizeof(client_t));
            if (c->client) {
                c->client->conn = c;
            }
        }
    }
    return 0;
}

int recv_cb(int fd) {
    if (fd < 0 || fd >= conn_pool_size) return -1;
    struct conn* c = &conn_pool[fd];
    if (!c->active) return -1;

    int eof = 0;

    /* Level-triggered: drain all available data in one go */
    for (;;) {
        int tail = c->r_off + c->read_buf.len;
        int remain = c->read_buf.cap - tail;

        /* Expand buffer if needed */
        if (remain <= 0) {
            int new_cap = c->read_buf.cap * 2;
            if (new_cap > 16 * 1024 * 1024) {  /* Cap at 16MB per connection */
                fprintf(stderr, "[reactor] fd %d read buffer overflow\n", fd);
                conn_release(fd);
                return -1;
            }
            char *new_buf = realloc(c->read_buf.buf, new_cap);
            if (!new_buf) {
                fprintf(stderr, "[reactor] fd %d realloc failed\n", fd);
                conn_release(fd);
                return -1;
            }
            c->read_buf.buf = new_buf;
            c->read_buf.cap = new_cap;
            remain = new_cap - tail;
        }

        int n = recv(fd, c->read_buf.buf + tail, remain, 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;  /* No more data available */
            conn_release(fd);
            return -1;
        }
        if (n == 0) {
            eof = 1;
            break;  /* Connection closed by peer */
        }
        c->read_buf.len += n;
    }


    /* Prefetch read buffer for protocol parsing */
    if (__builtin_expect(c->read_buf.len > 64, 1))
        __builtin_prefetch(c->read_buf.buf + c->r_off, 0, 3);

    /* Always drain immediately.
     * If a partial send left stale w_off, compact unsent data
     * to buf[0] so new responses append cleanly. */
    if (c->write_buf.len > 0 && c->w_off > 0) {
        memmove(c->write_buf.buf,
                c->write_buf.buf + c->w_off,
                (size_t)c->write_buf.len);
        c->w_off = 0;
    }

    /* Process all available commands in the buffer */
    kvs_drain_resp_commands_off(&c->read_buf, &c->r_off, &c->write_buf, c);

    /* Reset r_off when buffer is fully consumed to prevent unbounded
     * growth of the read offset — without this, repeated drain→empty
     * cycles let r_off creep past cap, forcing unnecessary realloc
     * (doubling up to 16 MB) even though the buffer is logically empty. */
    if (c->read_buf.len == 0)
        c->r_off = 0;

    /* QUIT / protocol error: close-after-flush */
    if (__builtin_expect(c->status != 0, 0)) {
        if (c->write_buf.len == 0) {
            conn_release(fd);
            return 0;
        }
        /* Suppress EPOLLIN, keep EPOLLOUT to drain remaining bytes.
         * conn_set_events honours c->status and won't re-enable reads. */
        struct epoll_event ev;
        ev.events = EPOLLOUT;
        ev.data.fd = fd;
        c->cur_events = EPOLLOUT;
        epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
        return 0;
    }

    /* Defer close until response is sent */
    if (eof && c->write_buf.len == 0) {
        conn_release(fd);
        return -1;
    }
    return 0;
}

int send_cb(int fd) {
    if (fd < 0 || fd >= conn_pool_size) return -1;
    struct conn *c = &conn_pool[fd];
    if (!c->active) return -1;

    DynBuf *wb = &c->write_buf;

    /* Drain all available send buffer space */
    while (wb->len > 0) {
        ssize_t n = send(fd, wb->buf + c->w_off, wb->len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;  /* Send buffer full, wait for next EPOLLOUT */
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

        /* Close-after-flush requested by QUIT / protocol error */
        if (c->status) {
            conn_release(fd);
            return 0;
        }

        /* Pending read data was deferred in recv_cb (write_buf was busy).
         * Process it now to unblock the pipeline. */
        if (c->read_buf.len > 0) {
            kvs_drain_resp_commands_off(&c->read_buf, &c->r_off, &c->write_buf, c);
            if (c->read_buf.len == 0)
                c->r_off = 0;

            /* New responses may have been generated — re-arm send if needed */
            if (c->status) {
                if (c->write_buf.len > 0) {
                    conn_set_events(fd, c);
                    return 0;
                }
                conn_release(fd);
                return 0;
            }
        }
    }

    /* Update event mask based on new state */
    conn_set_events(fd, c);
    return 0;
}

int r_init_server(unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        fprintf(stderr, "[reactor] socket creation failed: %s\n", strerror(errno));
        return -1;
    }

    /* Enable SO_REUSEADDR for quick restart */
    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    /* Enable SO_REUSEPORT for potential multi-process support */
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

    /* Set non-blocking */
    if (fcntl(sockfd, F_SETFL, O_NONBLOCK) == -1) {
        fprintf(stderr, "[reactor] fcntl O_NONBLOCK failed: %s\n", strerror(errno));
        close(sockfd);
        return -1;
    }

    struct sockaddr_in servaddr;
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(port);

    if (bind(sockfd, (struct sockaddr*)&servaddr, sizeof(struct sockaddr)) == -1) {
        fprintf(stderr, "[reactor] bind failed on port %d: %s\n", port, strerror(errno));
        close(sockfd);
        return -1;
    }

    if (listen(sockfd, SOMAXCONN) == -1) {
        fprintf(stderr, "[reactor] listen failed: %s\n", strerror(errno));
        close(sockfd);
        return -1;
    }

    return sockfd;
}

int reactor_start(unsigned short port, msg_handler handler) {
    (void)handler;

    /* Allocate connection pool based on maxclients config */
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

    /* Create epoll instance */
    epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0) {
        fprintf(stderr, "[reactor] epoll_create1 failed: %s\n", strerror(errno));
        free(conn_pool);
        return -1;
    }

    /* Initialize listen socket on single port */
    int sockfd = r_init_server(port);
    if (sockfd < 0) {
        fprintf(stderr, "[reactor] failed to bind port %d\n", port);
        close(epfd);
        free(conn_pool);
        return -1;
    }

    /* Register listen socket */
    if (sockfd >= conn_pool_size) {
        fprintf(stderr, "[reactor] listen fd %d exceeds maxclients %d\n",
                sockfd, conn_pool_size);
        close(sockfd);
        close(epfd);
        free(conn_pool);
        return -1;
    }

    conn_pool[sockfd].fd = sockfd;
    conn_pool[sockfd].active = 1;
    conn_pool[sockfd].cur_events = EPOLLIN;
    conn_pool[sockfd].r_action.accept_callback = accept_cb;

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = sockfd;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, sockfd, &ev) < 0) {
        fprintf(stderr, "[reactor] epoll_ctl ADD failed: %s\n", strerror(errno));
        close(sockfd);
        close(epfd);
        free(conn_pool);
        return -1;
    }

    printf("[reactor] listening on port %d (epoll level-triggered mode)\n", port);

    /* Main event loop */
    struct epoll_event events[1024];

    while (!g_kvs_stop) {
        /* Update cached time for commands */
        update_cached_time();

        /* 100ms timeout for responsive shutdown */
        int nready = epoll_wait(epfd, events, 1024, 100);
        if (nready < 0) {
            if (errno == EINTR) continue;  /* Signal received — rely on g_kvs_stop to exit */
            fprintf(stderr, "[reactor] epoll_wait error: %s\n", strerror(errno));
            continue;
        }

        /* Process ready events */
        for (int i = 0; i < nready; i++) {
            int connfd = events[i].data.fd;

            /* Handle errors */
            if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                if (connfd >= 0 && connfd < conn_pool_size && connfd != sockfd)
                    conn_release(connfd);
                continue;
            }

            if (connfd < 0 || connfd >= conn_pool_size)
                continue;

            struct conn *c = &conn_pool[connfd];
            if (!c->active)
                continue;

            /* Handle accept on listen socket */
            if (connfd == sockfd) {
                c->r_action.accept_callback(connfd);
                continue;
            }

            /* Handle read events */
            if (events[i].events & EPOLLIN) {
                c->r_action.recv_callback(connfd);
                /* Connection might be closed in recv_cb */
                if (!conn_pool[connfd].active)
                    continue;
            }

            /* Inline flush: send responses in the same epoll iteration
             * instead of waiting for the next EPOLLOUT.  This eliminates
             * the "batch barrier" — one extra epoll_wait round-trip per
             * pipeline batch — that penalises low-P throughput.
             *
             * The first send_callback drains the current write buffer and
             * may process deferred reads (generating new responses).  A
             * second call flushes those new responses.  We stop at two:
             * further sends would spin on a full TCP send buffer (EAGAIN),
             * which is handled by the EPOLLOUT fallback in conn_set_events. */
            if (c->write_buf.len > 0) {
                c->send_callback(connfd);
                if (conn_pool[connfd].active && c->write_buf.len > 0)
                    c->send_callback(connfd);
            }
        }
    }

    printf("[reactor] shutting down gracefully\n");

    /* Cleanup: close all connections */
    for (int i = 0; i < conn_pool_size; i++) {
        if (conn_pool[i].active && i != sockfd) {
            conn_release(i);
        }
    }

    /* Close listen socket and epoll */
    close(sockfd);
    close(epfd);
    free(conn_pool);

    return 0;
}
