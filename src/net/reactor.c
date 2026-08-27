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

/*=============================================================================
 *  连接状态结构体（epoll 引擎）
 *  使用 fd 即索引的池化设计，热路径零哈希查找
 *============================================================================*/
struct conn {
    int fd;                     /* socket 文件描述符，同时也是池索引 */
    DynBuf read_buf;            /* 读缓冲：协议解析的原始数据 */
    DynBuf write_buf;           /* 写缓冲：待发送的响应数据 */
    int r_off, w_off;           /* 读已消费偏移 / 写已发送偏移 */
    int active;                 /* 连接是否活跃（用于幂等释放） */
    int status;                 /* 标记需要关闭（冲刷后关闭） */
    uint32_t cur_events;        /* epoll 缓存的事件掩码（避免重复 epoll_ctl） */
    kvs_drain_fn on_data;       /* 数据处理函数，支持普通/Repl 动态切换 */
};

/*=============================================================================
 *  全局状态
 *============================================================================*/

int epfd = 0;                       /* epoll 文件描述符 */

/* 默认数据处理函数，由 reactor_start(port, handler) 注入
 * 普通客户端连接沿用该默认，repl 连接按需覆盖 */
static kvs_drain_fn g_default_handler = NULL;

/* 连接池（Slab 风格复用，热路径零分配） */
static struct conn *conn_pool = NULL;
static int          conn_pool_size = 0;

/*=============================================================================
 *  epoll 事件管理
 *============================================================================*/

/**
 * 更新连接在 epoll 中的事件监听掩码
 * 设计要点：缓存 cur_events 避免无变化的 epoll_ctl 系统调用
 * @param fd 连接 fd
 * @param c 连接对象指针
 */
static void conn_set_events(int fd, struct conn *c) {
    /* 根据当前状态计算需要监听的事件 */
    uint32_t want = c->status ? 0 : EPOLLIN;     /* status 标记关闭时不监听读 */
    if (c->write_buf.len > 0)
        want |= EPOLLOUT;                         /* 有待发数据时监听写 */

    /* 无变化则跳过系统调用（性能优化） */
    if (want == c->cur_events)
        return;
    c->cur_events = want;

    /* 更新 epoll 事件 */
    struct epoll_event e;
    e.events = want;
    e.data.fd = fd;
    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &e);
}

/*=============================================================================
 *  连接管理
 *============================================================================*/

/**
 * 注册新连接到 epoll
 * 使用 Slab 风格：首次分配缓冲，重连时直接复用
 * @param fd 已建立连接的 socket fd
 * @return 0 成功，-1 失败
 */
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
    c->status = 0;
    c->on_data = g_default_handler;    /* 默认使用普通客户端处理器 */

    /* 惰性分配缓冲，重连时复用（Slab 复用）
     * accept 路径上只需复位长度/偏移游标，无需 malloc */
    if (!c->read_buf.buf)
        dynbuf_init(&c->read_buf, KVS_NET_BUF_INIT);
    if (!c->write_buf.buf)
        dynbuf_init(&c->write_buf, KVS_NET_BUF_INIT);
    c->read_buf.len  = 0;
    c->write_buf.len = 0;
    c->r_off = 0;
    c->w_off = 0;

    /* 设置非阻塞（epoll 必须） */
    if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1)
        perror("fcntl O_NONBLOCK");

    /* 禁用 Nagle 算法，降低延迟 */
    int nodelay = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    /* 注册到 epoll：水平触发模式，仅 EPOLLIN（不用 EPOLLET）
     * 水平触发配合循环读，实现简单且不容易丢失事件 */
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = fd;
    c->cur_events = EPOLLIN;
    epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
    return 0;
}

/**
 * 释放连接槽位
 * 读写缓冲保留底层内存（Slab 复用），只复位元数据与游标
 * 与 proactor 的 conn_reset_slot 一致，保证 accept 路径零 malloc
 * @param fd 连接 fd
 */
static void conn_release(int fd) {
    if (fd < 0 || fd >= conn_pool_size) return;
    struct conn *c = &conn_pool[fd];
    if (!c->active) return;              /* 幂等：已释放则直接返回 */

    /* 先通知 repl 清理该连接的状态（普通客户端是 no-op，内部按角色判断） */
    repl_on_close(fd);

    /* 从 epoll 摘除并关闭套接字 */
    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
    close(fd);

    /* 清零槽位，但保留两个缓冲的底层内存以便复用 */
    char *rb = c->read_buf.buf, *wb = c->write_buf.buf;
    int   rc = c->read_buf.cap,  wc = c->write_buf.cap;
    memset(c, 0, sizeof(*c));
    c->read_buf.buf  = rb;  c->read_buf.cap  = rc;
    c->write_buf.buf = wb;  c->write_buf.cap = wc;
}

/*=============================================================================
 *  事件回调处理
 *  设计原则：水平触发 + 循环处理，一次性处理所有可用数据
 *============================================================================*/

/**
 * Accept 回调：处理新连接
 * 水平触发：一次性排空所有待处理的 accept
 * @param fd 监听 socket fd
 * @return 0 成功，-1 失败
 */
int accept_cb(int fd) {
    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);

    /* 循环 accept 直到 EAGAIN/EWOULDBLOCK（水平触发特性） */
    for (;;) {
        int clientfd = accept(fd, (struct sockaddr*)&clientaddr, &len);
        if (clientfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;      /* 无更多待处理连接 */
            fprintf(stderr, "[reactor] accept error: %s\n", strerror(errno));
            return -1;
        }

        /* 注册到 epoll 并初始化 */
        if (event_register(clientfd) != 0) {
            close(clientfd);
            continue;
        }
    }
    return 0;
}

/**
 * Recv 回调：读取并处理数据
 * 水平触发：一次性读空所有可用数据
 * @param fd 连接 fd
 * @return 0 成功，-1 失败
 */
int recv_cb(int fd) {
    if (fd < 0 || fd >= conn_pool_size) return -1;
    struct conn *c = &conn_pool[fd];
    if (!c->active) return -1;

    int eof = 0;

    /* 水平触发：循环读直到 EAGAIN/EWOULDBLOCK */
    for (;;) {
        int tail = c->r_off + c->read_buf.len;    /* 当前写入位置 */
        int remain = c->read_buf.cap - tail;      /* 剩余空间 */

        /* 空间不足：扩容（翻倍策略） */
        if (remain <= 0) {
            int new_cap = c->read_buf.cap * 2;
            if (new_cap > 16 * 1024 * 1024) {  /* 单连接上限 16MB */
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

        /* 从 socket 读取数据 */
        int n = recv(fd, c->read_buf.buf + tail, remain, 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;      /* 无更多数据可读 */
            conn_release(fd);
            return -1;
        }
        if (n == 0) {
            eof = 1;
            break;          /* 对端关闭连接 */
        }
        c->read_buf.len += n;
    }

    /* 压缩写缓冲（回收已发送前缀） */
    if (c->write_buf.len > 0 && c->w_off > 0) {
        memmove(c->write_buf.buf,
                c->write_buf.buf + c->w_off,
                (size_t)c->write_buf.len);
        c->w_off = 0;
    }

    /* 调用业务处理器解析缓冲中的所有命令 */
    int want_close = 0;
    c->on_data(c->fd, &c->read_buf, &c->r_off, &c->write_buf, &want_close);
    if (want_close) c->status = 1;
    
    /* repl handler 可能在 on_data 内直接释放本连接（如 +FULLRESYNC 交接）
     * 此时槽位已清零，后续逻辑需跳过 */
    if (!c->active) return 0;

    /* 完全消费的读缓冲：重置偏移 */
    if (c->read_buf.len == 0)
        c->r_off = 0;

    /* 处理关闭请求（QUIT / 协议错误）：冲刷后关闭 */
    if (__builtin_expect(c->status != 0, 0)) {
        if (c->write_buf.len == 0) {
            conn_release(fd);
            return 0;
        }
        /* 有待发数据：切换为只监听写事件，发完再关 */
        struct epoll_event ev;
        ev.events = EPOLLOUT;
        ev.data.fd = fd;
        c->cur_events = EPOLLOUT;
        epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
        return 0;
    }

    /* 对端关闭且数据已处理完：释放连接 */
    if (eof && c->write_buf.len == 0) {
        conn_release(fd);
        return -1;
    }
    return 0;
}

/**
 * Send 回调：发送响应数据
 * 一次性发空所有待发数据，部分发送时由 epoll 再次触发
 * @param fd 连接 fd
 * @return 0 成功，-1 失败
 */
int send_cb(int fd) {
    if (fd < 0 || fd >= conn_pool_size) return -1;
    struct conn *c = &conn_pool[fd];
    if (!c->active) return -1;

    DynBuf *wb = &c->write_buf;

    /* 循环发送直到全部发完或发送缓冲区满 */
    while (wb->len > 0) {
        ssize_t n = send(fd, wb->buf + c->w_off, wb->len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;      /* 发送缓冲区已满，等待下次 EPOLLOUT */
            conn_release(fd);
            return -1;
        }
        if (n == 0) break;
        c->w_off += (int)n;
        wb->len -= (int)n;
    }

    /* 全部发完：清空缓冲 */
    if (wb->len == 0) {
        c->w_off = 0;
        dynbuf_clear(wb);

        /* QUIT / 协议错误请求的冲刷后关闭 */
        if (c->status) {
            conn_release(fd);
            return 0;
        }
    }

    /* 依据最新状态更新事件掩码（动态调整读/写监听） */
    conn_set_events(fd, c);
    return 0;
}

/*=============================================================================
 *  Replication ↔ Engine 访问接口
 *  引擎将这些函数实例赋给 kvstore.h 中声明的全局指针
 *  repl 模块只依赖这三个全局指针，不 include 本文件
 *============================================================================*/

/**
 * 注册一个连接为 repl 连接，切换到专用 handler
 * @param fd 连接 fd
 * @param handler repl 专用数据处理函数
 * @return 0 成功，-1 失败
 */
static int eng_repl_register(int fd, kvs_drain_fn handler) {
    if (fd < 0 || fd >= conn_pool_size) return -1;
    struct conn *c = &conn_pool[fd];
    if (!c->active) {
        /* 新 fd：完整初始化 + 入 epoll */
        if (event_register(fd) != 0) return -1;
        c = &conn_pool[fd];
    }
    c->on_data = handler;    /* 切换处理器 */
    return 0;
}

/**
 * 向 repl 连接追加待发送数据（如 +FULLRESYNC 响应）
 * @param fd 连接 fd
 * @param data 数据指针
 * @param len 数据长度
 * @return 0 成功，-1 失败
 */
static int eng_repl_append(int fd, const char *data, int len) {
    if (fd < 0 || fd >= conn_pool_size) return -1;
    struct conn *c = &conn_pool[fd];
    if (!c->active) return -1;
    DynBuf *wb = &c->write_buf;

    /* 计算追加位置 = 已发送前缀 + 待发长度（w_off 与 len 分离） */
    size_t tail = (size_t)(c->w_off + wb->len);
    if (tail + (size_t)len > (size_t)wb->cap) {
        /* 缓冲不足：扩容（额外 +4096 减少频繁 realloc） */
        size_t need = tail + (size_t)len + 4096;
        char *nb = realloc(wb->buf, need);
        if (!nb) return -1;
        wb->buf = nb;
        wb->cap = (int)need;
    }
    memcpy(wb->buf + tail, data, (size_t)len);
    wb->len += len;

    /* 追加后更新 epoll 事件（添加 EPOLLOUT） */
    conn_set_events(fd, c);
    return 0;
}

/**
 * 释放 repl 连接（通过引擎的统一释放路径）
 */
static void eng_repl_release(int fd) {
    conn_release(fd);
}

/*=============================================================================
 *  Socket 初始化
 *============================================================================*/

/**
 * 创建并绑定监听 socket
 * @param port 监听端口
 * @return socket fd，失败返回 -1
 */
int r_init_server(unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        fprintf(stderr, "[reactor] socket creation failed: %s\n", strerror(errno));
        return -1;
    }

    /* 开启 SO_REUSEADDR，便于快速重启 */
    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    /* 设为非阻塞（epoll 必须） */
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

/*=============================================================================
 *  主事件循环
 *  设计要点：
 *  1. 水平触发 + 循环读/写，一次处理所有可用数据
 *  2. 缓存事件掩码避免无变化的 epoll_ctl 系统调用
 *  3. 每轮调用 repl_tick 用于主从复制状态推进
 *  4. timeout 100ms 兼顾响应性和 CPU 开销
 *============================================================================*/

int reactor_start(unsigned short port, kvs_drain_fn handler) {
    /* 设置默认处理器 */
    g_default_handler = handler;

    /* 注册 repl 引擎接口（供 replication 模块使用） */
    repl_engine_register = eng_repl_register;
    repl_engine_append   = eng_repl_append;
    repl_engine_release  = eng_repl_release;

    /* 按 maxclients 配置分配连接池 */
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

    /* 创建 epoll 实例 */
    epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0) {
        fprintf(stderr, "[reactor] epoll_create1 failed: %s\n", strerror(errno));
        free(conn_pool);
        return -1;
    }

    /* 初始化监听套接字 */
    int sockfd = r_init_server(port);
    if (sockfd < 0) {
        fprintf(stderr, "[reactor] failed to bind port %d\n", port);
        close(epfd);
        free(conn_pool);
        return -1;
    }

    /* 监听 socket 也放入连接池（占一个槽位） */
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

    /* 注册监听 socket 到 epoll */
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

    /*=========================================================================
     *  主事件循环
     *=========================================================================*/
    struct epoll_event events[1024];

    while (!g_kvs_stop) {
        /* 每轮更新缓存时间（减少 gettimeofday 系统调用） */
        update_cached_time();

        /* 复制状态 tick（用于 slave 断线重连/全量交接推进） */
        repl_tick();

        /* 阻塞等待事件，超时 100ms（避免信号处理延迟） */
        int timeout_ms = 100;
        int nready = epoll_wait(epfd, events, 1024, timeout_ms);
        if (nready < 0) {
            if (errno == EINTR) continue;  /* 被信号中断，继续循环 */
            fprintf(stderr, "[reactor] epoll_wait error: %s\n", strerror(errno));
            continue;
        }

        /* 处理所有就绪事件 */
        for (int i = 0; i < nready; i++) {
            int connfd = events[i].data.fd;

            /* 处理错误/挂起事件 */
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

            /* 监听套接字：accept 新连接 */
            if (connfd == sockfd) {
                accept_cb(connfd);
                continue;
            }

            /* 处理读事件（水平触发：recv_cb 内循环读取所有数据） */
            if (events[i].events & EPOLLIN) {
                recv_cb(connfd);
                /* recv_cb 中连接可能已被关闭 */
                if (!conn_pool[connfd].active)
                    continue;
            }

            /* 处理写事件（有待发数据时发送） */
            if (c->write_buf.len > 0)
                send_cb(connfd);
        }
    }

    printf("[reactor] shutting down gracefully\n");

    /* 清理：关闭所有客户端连接 */
    for (int i = 0; i < conn_pool_size; i++) {
        if (conn_pool[i].active && i != sockfd) {
            conn_release(i);
        }
    }

    /* 关闭监听套接字与 epoll，释放连接池 */
    close(sockfd);
    close(epfd);
    free(conn_pool);

    return 0;
}