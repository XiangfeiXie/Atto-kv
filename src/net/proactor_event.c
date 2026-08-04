#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <liburing.h>
#include "proactor.h"
#include "protocol.h"
#include "kvstore.h"
#include "server.h"

/* Arm multishot recv */
static int set_event_recv_multishot(struct io_uring *ring, int sockfd, uint32_t gen) {
    struct io_uring_sqe *sqe = get_sqe_submit(ring);
    if (!sqe) return -1;
    io_uring_prep_recv_multishot(sqe, sockfd, NULL, 0, 0);
    sqe->buf_group = KVS_BUF_RING_BGID;
    sqe->flags |= IOSQE_BUFFER_SELECT;
    sqe->user_data = conn_info_pack(sockfd, EVENT_READ, gen);
    return 0;
}

int conn_arm_recv(struct io_uring *ring, struct conn *c) {
    if (!c || !c->active || c->recv_armed)
        return 0;
    if (c->status)
        return 0;
    if (set_event_recv_multishot(ring, c->fd, c->gen) != 0)
        return -1;
    c->recv_armed = 1;
    if (proactor_debug_enabled())
        fprintf(stderr, "[proactor] arm recv fd=%d gen=%u\n", c->fd, c->gen);
    return 0;
}

/* Arm send */
static int set_event_send(struct io_uring *ring, int sockfd, uint32_t gen, void *buf, size_t len) {
    struct io_uring_sqe *sqe = get_sqe_submit(ring);
    if (!sqe) return -1;
    io_uring_prep_send(sqe, sockfd, buf, len, MSG_NOSIGNAL);
    sqe->user_data = conn_info_pack(sockfd, EVENT_WRITE, gen);
    return 0;
}

int conn_arm_send(struct io_uring *ring, struct conn *c) {
    if (!c || !c->active || c->send_armed || c->write_buf.len <= 0)
        return 0;
    if (set_event_send(ring, c->fd, c->gen, c->write_buf.buf + c->w_off,
                       c->write_buf.len) != 0)
        return -1;
    c->send_armed = 1;
    return 0;
}

/* OPTIMIZED: Multishot Accept */
static int set_event_accept(struct io_uring *ring, int sockfd) {
    struct io_uring_sqe *sqe = get_sqe_submit(ring);
    if (!sqe) return -1;
    io_uring_prep_multishot_accept(sqe, sockfd, NULL, NULL, SOCK_NONBLOCK);
    sqe->user_data = conn_info_pack(sockfd, EVENT_ACCEPT, 0);
    return 0;
}

int worker_arm_accept(proactor_worker_t *w) {
    if (!w || w->accept_armed)
        return 0;
    if (set_event_accept(&w->ring, w->listen_fd) != 0)
        return -1;
    w->accept_armed = 1;
    if (proactor_debug_enabled())
        fprintf(stderr, "[proactor] accept armed\n");
    return 0;
}

/* Handle ACCEPT event */
void event_handle_accept(proactor_worker_t *w, struct io_uring_cqe *cqe) {
    int res = cqe->res;

    /* Multishot: only clear armed flag when terminated */
    if (!(cqe->flags & IORING_CQE_F_MORE))
        w->accept_armed = 0;

    int connfd = res;
    if (connfd < 0) {
        if (connfd != -EAGAIN && connfd != -ECANCELED)
            fprintf(stderr, "[proactor] accept cqe: %s\n",
                    strerror(-connfd));
        return;
    }

    if (proactor_debug_enabled())
        fprintf(stderr, "[proactor] accept fd=%d\n", connfd);

    if (connfd >= conn_pool_size) {
        fprintf(stderr, "[proactor] fd %d exceeds maxclients %d\n",
                connfd, conn_pool_size);
        close(connfd);
        return;
    }

    /* Set TCP_NODELAY */
    int nd = 1;
    setsockopt(connfd, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd));

    struct conn *nc = &conn_pool[connfd];
    /* Lazy-allocate + pre-fault on first use; subsequent reuse gets warm buffer */
    if (!nc->read_buf.buf) {
        dynbuf_init(&nc->read_buf, KVS_NET_BUF_INIT);
        if (nc->read_buf.buf) {
            for (size_t off = 0; off < (size_t)KVS_NET_BUF_INIT; off += 4096)
                nc->read_buf.buf[off] = 0;
        }
    } else {
        nc->read_buf.len = 0;
    }
    if (!nc->write_buf.buf) {
        dynbuf_init(&nc->write_buf, KVS_NET_BUF_INIT);
        if (nc->write_buf.buf) {
            for (size_t off = 0; off < (size_t)KVS_NET_BUF_INIT; off += 4096)
                nc->write_buf.buf[off] = 0;
        }
    } else {
        nc->write_buf.len = 0;
    }
    nc->fd = connfd;
    nc->active = 1;
    nc->r_off = 0;
    nc->w_off = 0;
    nc->recv_armed = 0;
    nc->send_armed = 0;
    nc->gen = next_conn_gen();

    atomic_fetch_add_explicit(&g_kvs_total_connections, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_kvs_connected_clients, 1, memory_order_relaxed);

    /* client_t pre-allocated in conn_pool_init — just reset */
    if (nc->client) {
        memset(nc->client, 0, sizeof(client_t));
        nc->client->conn = nc;
    }

    if (conn_arm_recv(&w->ring, nc) != 0)
        conn_release(connfd);

    if (proactor_debug_enabled())
        fprintf(stderr, "[proactor] armed recv for fd=%d\n", connfd);
}

/* Handle READ event */
void event_handle_read(proactor_worker_t *w, struct io_uring_cqe *cqe, int fd,
                      struct conn_info result) {
    struct io_uring *ring = &w->ring;
    int res = cqe->res;

    if (fd < 0 || fd >= conn_pool_size) {
        if (cqe->flags & IORING_CQE_F_BUFFER)
            buf_ring_recycle(cqe->flags >> IORING_CQE_BUFFER_SHIFT);
        return;
    }

    struct conn *c = &conn_pool[fd];

    if (!c->active) {
        if (cqe->flags & IORING_CQE_F_BUFFER)
            buf_ring_recycle(cqe->flags >> IORING_CQE_BUFFER_SHIFT);
        return;
    }

    if (result.gen != (c->gen & 0xFFFFFFu)) {
        if (proactor_debug_enabled())
            fprintf(stderr, "[proactor] stale read fd=%d cqe_gen=%u cur_gen=%u\n",
fd, result.gen, c->gen & 0xFFFFFFu);
        if (cqe->flags & IORING_CQE_F_BUFFER)
            buf_ring_recycle(cqe->flags >> IORING_CQE_BUFFER_SHIFT);
        return;
    }

    /* Multishot: only clear when terminated */
    if (!(cqe->flags & IORING_CQE_F_MORE))
        c->recv_armed = 0;

    DynBuf *rb = &c->read_buf;
    DynBuf *wb = &c->write_buf;

    if (res > 0 && (cqe->flags & IORING_CQE_F_BUFFER)) {
        unsigned bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
        char *src = w->buf_base + (size_t)bid * KVS_BUF_SIZE;

        /* Copy data from buf ring into read buffer, then drain.
         * With 64KB initial buffer, realloc is rare — pipeline bursts
         * (~8KB @ P=160) fit comfortably. Page faults are pre-faulted
         * at accept time and on realloc expansion. */
        size_t need = c->r_off + rb->len + (size_t)res;
        if (need > rb->cap) {
            size_t new_cap = rb->cap ? rb->cap : 4096;
            while (new_cap < need && new_cap < (16u << 20)) new_cap *= 2;
            size_t old_cap = rb->cap;
            char *new_buf = realloc(rb->buf, new_cap);
            if (!new_buf) { buf_ring_recycle(bid); conn_release(fd); return; }
            for (size_t p = (old_cap + 4095) & ~(size_t)4095; p < new_cap; p += 4096) new_buf[p] = 0;
            rb->buf = new_buf; rb->cap = (int)new_cap;
        }
        size_t old_len = (size_t)rb->len;
        /* If the previous drain consumed all data, reset r_off
         * BEFORE copying so data lands at offset 0. */
        if (old_len == 0)
            c->r_off = 0;
        memcpy(rb->buf + c->r_off + rb->len, src, (size_t)res);
        rb->len += res;
        buf_ring_recycle(bid);
    } else if (res > 0) {
        rb->len += res;
    }

    if (res <= 0) {
        if (res == -ENOBUFS) {
            if (conn_arm_recv(ring, c) != 0)
                conn_release(fd);
            return;
        }
        if (res < 0 && res != -ECANCELED)
            fprintf(stderr, "[proactor] recv fd=%d: %s\n",
fd, strerror(-res));

        if (rb->len > 0) {
            kvs_drain_resp_commands_off(rb, &c->r_off, wb, c);
        }
        if (wb->len > 0) {
            if (conn_arm_send(ring, c) != 0)
                conn_release(fd);
        } else {
            conn_release(fd);
        }
        return;
    }

    /* Defer drain if send is armed */
    if (c->send_armed || wb->len > 0) {
        conn_arm_recv(ring, c);
        return;
    }

    kvs_drain_resp_commands_off(rb, &c->r_off, wb, c);

    if (proactor_debug_enabled())
        fprintf(stderr, "[proactor] fd=%d read res=%d rb_len=%d wb_len=%d\n",
                fd, res, rb->len, wb->len);

    if (c->status) {
        if (wb->len > 0) {
            if (conn_arm_send(ring, c) != 0)
                conn_release(fd);
        } else {
            conn_release(fd);
        }
    } else if (wb->len > 0) {
        if (conn_arm_send(ring, c) != 0)
            conn_release(fd);
    } else {
        if (conn_arm_recv(ring, c) != 0)
            conn_release(fd);
    }
}

/* Handle WRITE event */
void event_handle_write(proactor_worker_t *w, int fd, struct conn_info result, int res) {
    struct io_uring *ring = &w->ring;

    if (fd < 0 || fd >= conn_pool_size) return;

    struct conn *c = &conn_pool[fd];
    if (!c->active) return;
    if (result.gen != (c->gen & 0xFFFFFFu)) return;

    c->send_armed = 0;
    DynBuf *wb = &c->write_buf;

    if (res <= 0) {
        conn_release(fd);
        return;
    }

    c->w_off += res;
    wb->len -= res;

    if (proactor_debug_enabled())
        fprintf(stderr, "[proactor] fd=%d send res=%d wb_len=%d\n",
                fd, res, wb->len);

    if (wb->len > 0) {
        if (conn_arm_send(ring, c) != 0)
            conn_release(fd);
    } else {
        c->w_off = 0;
        dynbuf_clear(wb);
        if (c->status) {
            conn_release(fd);
        } else if (c->read_buf.len > 0) {
            kvs_drain_resp_commands_off(&c->read_buf, &c->r_off, wb, c);
            if (c->status) {
                if (wb->len > 0) {
                    if (conn_arm_send(ring, c) != 0)
                        conn_release(fd);
                } else {
                    conn_release(fd);
                }
            } else if (wb->len > 0) {
                if (conn_arm_send(ring, c) != 0)
                    conn_release(fd);
            } else {
                if (conn_arm_recv(ring, c) != 0)
                    conn_release(fd);
            }
        } else {
            if (conn_arm_recv(ring, c) != 0)
                conn_release(fd);
        }
    }
}

/* Handle single CQE */
void event_handle_cqe(proactor_worker_t *w, struct io_uring_cqe *cqe) {
    struct conn_info result = conn_info_unpack(cqe->user_data);
    int fd = result.fd;
    int res = cqe->res;

    /* Timeout */
    if (fd == 0 && result.event == 0) return;

    if (result.event == EVENT_ACCEPT) {
        event_handle_accept(w, cqe);
    } else if (result.event == EVENT_READ) {
        event_handle_read(w, cqe, fd, result);
    } else if (result.event == EVENT_WRITE) {
        event_handle_write(w, fd, result, res);
    }
}
