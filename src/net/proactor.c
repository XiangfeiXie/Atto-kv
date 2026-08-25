#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <liburing.h>

#include "server.h"
#include "kvstore.h"
#include "config.h"
#include "repl_incr.h"
#include "protocol.h"


#define ENTRIES_LENGTH   2048

/* 每个客户端连接上至多有一个 op 在飞（串行化模型），recv/send 的 armed
 * 状态合并为一个字段；复制 fd 用 repl_poll_mask 代替。 */
enum { CONN_IDLE = 0, CONN_RECV, CONN_SEND };

/* 连接状态（io_uring 引擎）。 */
typedef struct {
    int     fd;
    DynBuf  read_buf;
    DynBuf  write_buf;
    int     r_off, w_off;
    int     active;
    int     status;           /* 发送完关闭（QUIT / 协议错误） */
    conn_kind_t kind;         /* 客户端 vs 复制角色 */
    int     armed;            /* CONN_IDLE / CONN_RECV / CONN_SEND */
    uint32_t repl_poll_mask;  /* 在飞的 poll 掩码（0 = 无） */
} conn_t;

/* ═══════════════════════════════════════════════════════════════════════
 *  全局状态（单线程，无 worker 抽象）
 * ═══════════════════════════════════════════════════════════════════════ */

static conn_t           *conn_pool      = NULL;
static int               conn_pool_size = 0;
static struct io_uring   g_ring;
static int               g_listen_fd    = -1;
static int               g_accept_armed = 0;

/* ═══════════════════════════════════════════════════════════════════════
 *  连接池
 * ═══════════════════════════════════════════════════════════════════════ */

static int conn_pool_init(int max_clients) {
    conn_pool_size = max_clients;
    conn_pool = calloc((size_t)max_clients, sizeof(conn_t));
    if (!conn_pool) {
        fprintf(stderr, "[proactor] failed to allocate conn pool\n");
        return -1;
    }
    printf("[proactor] conn pool: %d slots (%zu MB)\n",
           max_clients,
           (sizeof(conn_t) * (size_t)max_clients) / (1024 * 1024));
    return 0;
}

/* 复位连接槽位以复用，保留 read/write 缓冲的底层分配（buf/cap 保留、len
 * 清零）——避免在 accept 热路径上反复创建。 */
static void conn_reset_slot(conn_t *c) {
    char *rb = c->read_buf.buf, *wb = c->write_buf.buf;
    int   rc = c->read_buf.cap,  wc = c->write_buf.cap;

    memset(c, 0, sizeof(*c));
    c->read_buf.buf  = rb;  c->read_buf.cap  = rc;
    c->write_buf.buf = wb;  c->write_buf.cap = wc;
}

static void conn_release(int fd) {
    if (fd < 0 || fd >= conn_pool_size) return;
    conn_t *c = &conn_pool[fd];

    if (c->active)
        g_kvs_connected_clients--;

    conn_reset_slot(c);
    close(fd);
}

static void conn_pool_destroy(void) {
    if (!conn_pool) return;
    for (int i = 0; i < conn_pool_size; i++) {
        if (conn_pool[i].active)
            conn_release(i);
        dynbuf_free(&conn_pool[i].read_buf);
        dynbuf_free(&conn_pool[i].write_buf);
    }
    free(conn_pool);
    conn_pool = NULL;
    conn_pool_size = 0;
}

/* ═══════════════════════════════════════════════════════════════════════
 *  SQE 辅助
 * ═══════════════════════════════════════════════════════════════════════ */

/* 确保读缓冲有至少 4KB 空间：先回收已消费的前缀，不够再翻倍扩容。
 * 返回可用写空间，无法扩容时返回 -1。 */
static ssize_t ensure_read_space(conn_t *c) {
    DynBuf *rb = &c->read_buf;
    size_t tail  = (size_t)(c->r_off + rb->len);
    size_t avail = (size_t)rb->cap - tail;

    if (avail >= 4096)
        return (ssize_t)avail;

    if (c->r_off > 0 && rb->len > 0) {
        memmove(rb->buf, rb->buf + c->r_off, (size_t)rb->len);
        tail    = (size_t)rb->len;
        c->r_off = 0;
        avail   = (size_t)rb->cap - tail;
    }
    if (avail >= 4096)
        return (ssize_t)avail;

    size_t new_cap = rb->cap ? (size_t)rb->cap * 2 : KVS_NET_BUF_INIT;
    if (new_cap > KVS_NET_BUF_MAX) return -1;
    char *new_buf = realloc(rb->buf, new_cap);
    if (!new_buf) return -1;
    rb->buf = new_buf;
    rb->cap = (int)new_cap;
    return (ssize_t)(new_cap - tail);
}

static int arm_recv(conn_t *c) {
    if (!c->active || c->armed != CONN_IDLE || c->status)
        return 0;

    ssize_t avail = ensure_read_space(c);
    if (avail < 0) return -1;
    size_t tail = (size_t)(c->r_off + c->read_buf.len);

    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_ring);
    if (!sqe) return -1;

    io_uring_prep_recv(sqe, c->fd, c->read_buf.buf + tail, (size_t)avail, 0);
    sqe->user_data = (uint64_t)(uint32_t)c->fd;
    c->armed = CONN_RECV;
    return 0;
}

static int arm_send(conn_t *c) {
    if (!c->active || c->armed != CONN_IDLE || c->write_buf.len <= 0)
        return 0;

    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_ring);
    if (!sqe) return -1;

    io_uring_prep_send(sqe, c->fd,
                       c->write_buf.buf + c->w_off,
                       (size_t)c->write_buf.len, MSG_NOSIGNAL);
    sqe->user_data = (uint64_t)(uint32_t)c->fd;
    c->armed = CONN_SEND;
    return 0;
}

static int arm_accept(void) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_ring);
    if (!sqe) return -1;
    io_uring_prep_accept(sqe, g_listen_fd, NULL, NULL, SOCK_NONBLOCK);
    sqe->user_data = (uint64_t)(uint32_t)g_listen_fd;
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════════
 *  复制 —— 就绪 poll 集成
 * ═══════════════════════════════════════════════════════════════════════ */

static int repl_desired_mask(conn_t *c) {
    switch (c->kind) {
    case CONN_KIND_REPL_LISTEN: return POLLIN;                     /* accept */
    case CONN_KIND_REPL_MASTER: return POLLIN;                     /* 命令流 */
    case CONN_KIND_REPL_SLAVE:  return repl_incr_master_poll_mask(c->fd);
    default:                    return 0;
    }
}

/* 给复制 fd（重新）arm 就绪 poll。每个 fd 至多一个 poll 在飞：若已 arm 则
 * 保持不动，让完成处理器重新评估掩码，避免中途改掩码产生第二条过期 CQE。 */
static void repl_poll_update(conn_t *c) {
    if (!c->active || c->repl_poll_mask != 0)
        return;                      /* 空闲，或 poll 已在飞 */

    int desired = repl_desired_mask(c);
    if (desired == 0)
        return;                      /* 没有需要等待的事件 */

    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_ring);
    if (!sqe) return;
    io_uring_prep_poll_add(sqe, c->fd, (unsigned)desired);
    sqe->user_data = (uint64_t)(uint32_t)c->fd;
    c->repl_poll_mask = (uint32_t)desired;
}

static int proactor_repl_register(int fd, conn_kind_t kind) {
    if (fd < 0 || fd >= conn_pool_size) return -1;

    conn_t *c = &conn_pool[fd];
    conn_reset_slot(c);

    c->fd     = fd;
    c->active = 1;
    c->kind   = kind;

    repl_poll_update(c);
    return 0;
}

static void proactor_repl_writable(int fd, int on) {
    (void)on;  /* repl_poll_update 会按 repl 状态重算掩码 */
    if (fd < 0 || fd >= conn_pool_size) return;
    repl_poll_update(&conn_pool[fd]);
}

static void proactor_repl_close(int fd) {
    if (fd < 0 || fd >= conn_pool_size) return;
    conn_t *c = &conn_pool[fd];

    conn_reset_slot(c);
    close(fd);
}

static void handle_repl_poll(conn_t *c, int res) {
    c->repl_poll_mask = 0;  /* poll 已消费，下面重新 arm */

    switch (c->kind) {
    case CONN_KIND_REPL_LISTEN:
        repl_incr_master_accept(c->fd);
        break;
    case CONN_KIND_REPL_SLAVE:
        if (res & POLLIN)  repl_incr_master_read(c->fd);
        if (res & POLLOUT) repl_incr_master_write(c->fd);
        break;
    case CONN_KIND_REPL_MASTER:
        repl_incr_slave_read(c->fd);
        break;
    default:
        break;
    }

    repl_poll_update(c);
}

/* ═══════════════════════════════════════════════════════════════════════
 *  CQE 处理器（总是清空，不延迟处理）
 * ═══════════════════════════════════════════════════════════════════════ */

static void handle_accept(struct io_uring_cqe *cqe) {
    /* 消费 accept SQE —— 批次结束后重新 arm */
    g_accept_armed = 0;

    int connfd = cqe->res;
    if (connfd < 0) return;
    if (connfd >= conn_pool_size) {
        close(connfd);
        return;
    }

    int nd = 1;
    setsockopt(connfd, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd));

    conn_t *c = &conn_pool[connfd];

    /* 首次使用时惰性分配 read/write 缓冲，重连时复用（slab 风格，accept
     * 热路径零 malloc）。 */
    if (!c->read_buf.buf)
        dynbuf_init(&c->read_buf, KVS_NET_BUF_INIT);
    if (!c->write_buf.buf)
        dynbuf_init(&c->write_buf, KVS_NET_BUF_INIT);

    c->fd            = connfd;
    c->active        = 1;
    c->kind          = CONN_KIND_CLIENT;
    c->read_buf.len  = 0;
    c->write_buf.len = 0;

    atomic_fetch_add_explicit(&g_kvs_total_connections, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_kvs_connected_clients, 1, memory_order_relaxed);

    arm_recv(c);
}

/* 读完成：解析完整命令，有回复就 arm 发送，否则 arm 下一次读。 */
static void handle_read(struct io_uring_cqe *cqe, conn_t *c) {
    int res = cqe->res;
    c->armed = CONN_IDLE;

    DynBuf *rb = &c->read_buf;
    DynBuf *wb = &c->write_buf;

    if (res > 0)
        rb->len += res;

    if (res <= 0) {
        /* 对端关闭或出错 —— 排掉剩余字节，异步冲刷待发回复后释放。 */
        if (rb->len > 0)
            kvs_drain_resp_commands_off(rb, &c->r_off, wb, NULL);
        if (wb->len > 0) {
            if (arm_send(c) != 0)
                conn_release(c->fd);
            else
                c->status = 1;   /* 发送完成后再释放 */
        } else {
            conn_release(c->fd);
        }
        return;
    }

    int want_close = 0;
    kvs_drain_resp_commands_off(rb, &c->r_off, wb, &want_close);
    if (want_close) c->status = 1;

    if (rb->len == 0)
        c->r_off = 0;

    if (wb->len > 0) {
        if (arm_send(c) != 0)
            conn_release(c->fd);
        return;   /* 已 arm 异步发送 —— handle_write 继续流程 */
    }

    if (c->status) {
        conn_release(c->fd);
        return;
    }

    if (arm_recv(c) != 0)
        conn_release(c->fd);
}

static void handle_write(conn_t *c, int res) {
    c->armed = CONN_IDLE;
    DynBuf *wb = &c->write_buf;

    if (res <= 0) {
        conn_release(c->fd);
        return;
    }

    c->w_off += res;
    wb->len  -= res;

    if (wb->len > 0) {
        /* 部分发送 —— 压紧并继续发送。 */
        if (c->w_off > 0) {
            memmove(wb->buf, wb->buf + c->w_off, (size_t)wb->len);
            c->w_off = 0;
        }
        if (arm_send(c) != 0)
            conn_release(c->fd);
    } else {
        /* 全部发完 —— 清空缓冲并回到读。 */
        c->w_off = 0;
        dynbuf_clear(wb);

        if (c->status) {
            conn_release(c->fd);
        } else if (arm_recv(c) != 0) {
            conn_release(c->fd);
        }
    }
}

static void handle_cqe(struct io_uring_cqe *cqe) {
    int fd = (int)(uint32_t)cqe->user_data;

    if (fd == g_listen_fd) {
        handle_accept(cqe);
        return;
    }

    if (fd < 0 || fd >= conn_pool_size) return;
    conn_t *c = &conn_pool[fd];
    if (!c->active) return;

    if (c->kind != CONN_KIND_CLIENT) {
        handle_repl_poll(c, cqe->res);
    } else if (c->armed == CONN_SEND) {
        handle_write(c, cqe->res);
    } else if (c->armed == CONN_RECV) {
        handle_read(cqe, c);
    }
    /* else：该 fd 无 op 在飞 —— 忽略。 */
}

static void proactor_loop(void) {
    if (arm_accept() != 0) {
        fprintf(stderr, "[proactor] failed to arm accept\n");
        return;
    }
    g_accept_armed = 1;

    printf("[proactor] io_uring event loop started\n");

    while (!g_kvs_stop) {
        /* 提交所有待发 SQE 并阻塞等待至少一个完成 —— 一次系统调用，无
         * SQPOLL 内核线程，无忙等。 */
        int ret = io_uring_submit_and_wait(&g_ring, 1);
        if (ret < 0) {
            if (ret == -EINTR) continue;
            fprintf(stderr, "[proactor] submit_and_wait: %s\n", strerror(-ret));
            break;
        }

        update_cached_time();

        /* 清空所有就绪 CQE；处理器各自 arm 下一项工作。 */
        unsigned head;
        int nready = 0;
        struct io_uring_cqe *cqe;
        io_uring_for_each_cqe(&g_ring, head, cqe) {
            handle_cqe(cqe);
            nready++;
        }
        io_uring_cq_advance(&g_ring, nready);

        /* 保持一个 accept SQE 待发，让下一次等待有活可干。 */
        if (!g_accept_armed && arm_accept() == 0)
            g_accept_armed = 1;
    }

    fprintf(stderr, "[proactor] exit stop=%d\n", (int)g_kvs_stop);
}

/* ═══════════════════════════════════════════════════════════════════════
 *  初始化
 * ═══════════════════════════════════════════════════════════════════════ */

static int setup_io_uring(void) {
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    int ret = io_uring_queue_init_params(ENTRIES_LENGTH, &g_ring, &params);
    if (ret < 0) {
        fprintf(stderr, "[proactor] io_uring init: %s\n", strerror(-ret));
        return -1;
    }

    printf("[proactor] io_uring ready (entries=%d)\n", ENTRIES_LENGTH);
    return 0;
}

static int init_listen_socket(unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (sockfd < 0) return -1;

    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons(port);

    if (bind(sockfd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        perror("[proactor] bind");
        close(sockfd);
        return -1;
    }
    listen(sockfd, SOMAXCONN);
    return sockfd;
}

static void worker_destroy(void) {
    io_uring_queue_exit(&g_ring);
    if (g_listen_fd >= 0) {
        close(g_listen_fd);
        g_listen_fd = -1;
    }
}

/* ═══════════════════════════════════════════════════════════════════════
 *  入口
 * ═══════════════════════════════════════════════════════════════════════ */

int proactor_start(unsigned short port, msg_handler handler) {
    (void)handler;

    if (conn_pool_init(kvs_config.maxclients) < 0) return -1;

    g_listen_fd = init_listen_socket(port);
    if (g_listen_fd < 0) {
        conn_pool_destroy();
        return -1;
    }

    if (setup_io_uring() < 0) {
        close(g_listen_fd);
        conn_pool_destroy();
        return -1;
    }

    if (kvs_config.incr_sync_enabled) {
        repl_engine_register = proactor_repl_register;
        repl_engine_writable = proactor_repl_writable;
        repl_engine_close    = proactor_repl_close;
        repl_incr_attach();
    }

    printf("[proactor] single-threaded io_uring on port %u\n", port);

    proactor_loop();

    worker_destroy();
    conn_pool_destroy();
    return 0;
}
