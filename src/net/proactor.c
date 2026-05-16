#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/mman.h>
#include "server.h"
#include "config.h"
#include "protocol.h"
#include "block.h"
#include "kvstore.h"

#define EVENT_ACCEPT    0
#define EVENT_READ      1
#define EVENT_WRITE     2
#define ENTRIES_LENGTH  1024

/* Phase 3.2: provided buffer ring for multishot recv.
 *
 * A pool of recv buffers is registered with the kernel as a single
 * mmaped ring.  When data arrives, the kernel picks an unused buffer,
 * copies into it, and reports the buffer id (bid) on the CQE.  We then
 * memcpy into the per-connection DynBuf and recycle the bid.
 *
 * Net effect: ONE recv SQE per connection for its lifetime (as opposed
 * to one per recv completion), and zero per-recv buffer-pointer
 * arithmetic.  Big syscall reduction for small-pipeline workloads. */
#define KVS_BUF_RING_BGID  1
#define KVS_NR_BUFS        1024     /* must be power of two */
#define KVS_BUF_SIZE       4096

static struct conn *conn_pool = NULL;
static int          conn_pool_size = 0;
static msg_handler  kvs_handler;
static struct io_uring *g_ring = NULL;
static struct io_uring_buf_ring *g_buf_ring = NULL;
static char *g_buf_base = NULL;

struct conn_info {
    int fd;
    int event;
};

/* Hand a buffer back to the kernel ring so it can be reused. */
static inline void buf_ring_recycle(unsigned bid) {
    io_uring_buf_ring_add(g_buf_ring,
                          g_buf_base + bid * KVS_BUF_SIZE,
                          KVS_BUF_SIZE,
                          (unsigned short)bid,
                          io_uring_buf_ring_mask(KVS_NR_BUFS),
                          0);
    io_uring_buf_ring_advance(g_buf_ring, 1);
}

/* Phase 3.2: arm a multishot recv that pulls buffers from the buf ring. */
static void set_event_recv_multishot(struct io_uring *ring, int sockfd) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    struct conn_info ci = {.fd = sockfd, .event = EVENT_READ};
    io_uring_prep_recv_multishot(sqe, sockfd, NULL, 0, 0);
    sqe->buf_group = KVS_BUF_RING_BGID;
    sqe->flags |= IOSQE_BUFFER_SELECT;
    memcpy(&sqe->user_data, &ci, sizeof(ci));
}

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

void set_event_send(struct io_uring *ring, int sockfd, void *buf, size_t len) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    struct conn_info ci = {.fd = sockfd, .event = EVENT_WRITE};
    io_uring_prep_send(sqe, sockfd, buf, len, MSG_NOSIGNAL);
    memcpy(&sqe->user_data, &ci, sizeof(ci));
}

/* Phase 3.1: multishot accept
 *
 * One SQE keeps producing CQEs for every successful accept until the kernel
 * reports an error (IORING_CQE_F_MORE clears).  Eliminates per-accept
 * re-arm and a syscall hop on every new connection.
 *
 * The kernel does not write the peer addr for multishot accept (it is
 * common to all CQEs), so passing NULL/NULL is the documented way.
 */
void set_event_accept(struct io_uring *ring, int sockfd) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    struct conn_info ci = {.fd = sockfd, .event = EVENT_ACCEPT};
    io_uring_prep_multishot_accept(sqe, sockfd, NULL, NULL, SOCK_NONBLOCK);
    memcpy(&sqe->user_data, &ci, sizeof(ci));
}

/* ── Notify callback: block subsystem calls this when a client is unblocked ── */
static void proactor_block_notify(client_t *cl) {
    if (!cl || !cl->conn || !g_ring) return;
    struct conn *conn = cl->conn;
    /* Drain remaining pipelined commands from the read buffer */
    if (conn->read_buf.len > 0) {
        conn->w_off = 0;
        kvs_drain_resp_commands_off(&conn->read_buf, &conn->r_off,
                                     &conn->write_buf, conn);
    }
    if (conn->write_buf.len > 0) {
        set_event_send(g_ring, conn->fd,
                       conn->write_buf.buf + conn->w_off,
                       conn->write_buf.len);
    }
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

    /* Phase 3.3: io_uring setup tuning for single-threaded proactor.
     *
     *   SINGLE_ISSUER  — promise the kernel that only this thread will
     *                    submit SQEs.  Skips internal locking.
     *   DEFER_TASKRUN  — defer kernel-side completion task work until the
     *                    next io_uring_enter; avoids costly IPIs and
     *                    lets work batch with submit_and_wait.  Requires
     *                    SINGLE_ISSUER.
     *   COOP_TASKRUN   — never use IPIs to wake user space; user polls
     *                    on its own schedule.
     *   SUBMIT_ALL     — keep submitting after a single-SQE failure
     *                    (e.g. a stale fd) instead of stopping the batch.
     *
     * Falls back gracefully on older kernels: if init fails with EINVAL
     * we drop the new flags and retry. */
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    params.flags = IORING_SETUP_SINGLE_ISSUER
                 | IORING_SETUP_DEFER_TASKRUN
                 | IORING_SETUP_COOP_TASKRUN
                 | IORING_SETUP_SUBMIT_ALL;
    struct io_uring ring;
    int qret = io_uring_queue_init_params(ENTRIES_LENGTH, &ring, &params);
    if (qret == -EINVAL) {
        memset(&params, 0, sizeof(params));
        qret = io_uring_queue_init_params(ENTRIES_LENGTH, &ring, &params);
    }
    if (qret < 0) {
        fprintf(stderr, "[proactor] io_uring_queue_init_params: %s\n",
                strerror(-qret));
        free(conn_pool);
        return -1;
    }
    printf("[proactor] io_uring flags: 0x%x (single_issuer/defer_taskrun/coop)\n",
           params.flags);
    g_ring = &ring;

    /* Phase 3.2: provided buffer ring for multishot recv */
    size_t buf_pool_bytes = (size_t)KVS_NR_BUFS * KVS_BUF_SIZE;
    g_buf_base = mmap(NULL, buf_pool_bytes, PROT_READ | PROT_WRITE,
                      MAP_ANON | MAP_PRIVATE, -1, 0);
    if (g_buf_base == MAP_FAILED) {
        perror("[proactor] mmap buf pool");
        free(conn_pool);
        return -1;
    }
    int br_err = 0;
    g_buf_ring = io_uring_setup_buf_ring(&ring, KVS_NR_BUFS,
                                          KVS_BUF_RING_BGID, 0, &br_err);
    if (!g_buf_ring) {
        fprintf(stderr, "[proactor] setup_buf_ring failed: %s\n",
                strerror(-br_err));
        munmap(g_buf_base, buf_pool_bytes);
        free(conn_pool);
        return -1;
    }
    int br_mask = io_uring_buf_ring_mask(KVS_NR_BUFS);
    for (int i = 0; i < KVS_NR_BUFS; i++) {
        io_uring_buf_ring_add(g_buf_ring,
                              g_buf_base + (size_t)i * KVS_BUF_SIZE,
                              KVS_BUF_SIZE,
                              (unsigned short)i, br_mask, i);
    }
    io_uring_buf_ring_advance(g_buf_ring, KVS_NR_BUFS);
    printf("[proactor] buf ring: %d x %d B = %zu MB\n",
           KVS_NR_BUFS, KVS_BUF_SIZE, buf_pool_bytes / (1024 * 1024));

    block_set_notify_cb(proactor_block_notify);

    set_event_accept(&ring, sockfd);

    while (!g_kvs_stop) {
        update_cached_time();
        struct io_uring_cqe *cqes[128];
        int nready = io_uring_peek_batch_cqe(&ring, cqes, 128);

        if (nready == 0) {
            block_check_timeouts();
            int to_ms = block_nearest_timeout();
            if (to_ms >= 0) {
                /* Submit a timeout SQE to bound the wait */
                struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
                if (sqe) {
                    struct __kernel_timespec ts;
                    ts.tv_sec = to_ms / 1000;
                    ts.tv_nsec = (to_ms % 1000) * 1000000L;
                    io_uring_prep_timeout(sqe, &ts, 0, 0);
                    sqe->user_data = 0; /* sentinel */
                }
            }
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

            /* Skip timeout CQEs (user_data == 0) */
            if (fd == 0 && result.event == 0) continue;

            if (result.event == EVENT_ACCEPT) {
                /* Multishot accept: while IORING_CQE_F_MORE is set the SQE
                 * is still alive in the kernel.  Only re-arm if the kernel
                 * tells us it is gone (typically on a fatal accept error). */
                if (!(cqe->flags & IORING_CQE_F_MORE))
                    set_event_accept(&ring, sockfd);
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

                /* Phase 3.2: ONE multishot recv per connection lifetime. */
                set_event_recv_multishot(&ring, connfd);
            }
            else if (result.event == EVENT_READ) {
                if (fd < 0 || fd >= conn_pool_size) {
                    /* Stale CQE for a dropped fd: recycle the buffer if any */
                    if (cqe->flags & IORING_CQE_F_BUFFER)
                        buf_ring_recycle(cqe->flags >> IORING_CQE_BUFFER_SHIFT);
                    continue;
                }
                struct conn *c = &conn_pool[fd];
                int multishot_dead = !(cqe->flags & IORING_CQE_F_MORE);

                if (!c->active) {
                    if (cqe->flags & IORING_CQE_F_BUFFER)
                        buf_ring_recycle(cqe->flags >> IORING_CQE_BUFFER_SHIFT);
                    continue;
                }
                DynBuf *rb = &c->read_buf;
                DynBuf *wb = &c->write_buf;

                /* Copy data from the kernel-selected buffer into the
                 * connection's parsing buffer, then recycle the buffer. */
                if (res > 0 && (cqe->flags & IORING_CQE_F_BUFFER)) {
                    unsigned bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
                    char *src = g_buf_base + (size_t)bid * KVS_BUF_SIZE;

                    /* Grow rb if necessary BEFORE memcpy. */
                    size_t need = c->r_off + rb->len + (size_t)res;
                    while (need > rb->cap) {
                        size_t new_cap = rb->cap * 2;
                        char *new_buf = realloc(rb->buf, new_cap);
                        if (!new_buf) {
                            buf_ring_recycle(bid);
                            conn_release(fd);
                            goto event_read_done;
                        }
                        rb->buf = new_buf;
                        rb->cap = new_cap;
                    }
                    memcpy(rb->buf + c->r_off + rb->len, src, (size_t)res);
                    rb->len += res;
                    buf_ring_recycle(bid);
                }

                /* res <= 0 means EOF or transport error; ENOBUFS just
                 * means the kernel had no buffer at the moment, retry. */
                if (res <= 0) {
                    if (res == -ENOBUFS) {
                        /* multishot died; will re-arm below */
                    } else {
                        if (rb->len > 0)
                            kvs_drain_resp_commands_off(rb, &c->r_off, wb, c);
                        if (wb->len > 0)
                            set_event_send(&ring, fd, wb->buf + c->w_off, wb->len);
                        else
                            conn_release(fd);
                        continue;
                    }
                } else {
                    kvs_drain_resp_commands_off(rb, &c->r_off, wb, c);

                    if (c->client && c->client->state == CLIENT_BLOCKED) {
                        if (wb->len > 0)
                            set_event_send(&ring, fd, wb->buf + c->w_off, wb->len);
                    } else if (c->status) {
                        /* QUIT / protocol error: flush then close. */
                        if (wb->len > 0)
                            set_event_send(&ring, fd, wb->buf + c->w_off, wb->len);
                        else
                            conn_release(fd);
                        if (multishot_dead) /* connection going away anyway */
                            multishot_dead = 0;
                    } else if (wb->len > 0) {
                        set_event_send(&ring, fd, wb->buf + c->w_off, wb->len);
                    }
                }

                /* If kernel terminated the multishot (typically -ENOBUFS
                 * or other recoverable condition), re-arm. */
                if (multishot_dead && c->active && !c->status)
                    set_event_recv_multishot(&ring, fd);
event_read_done: ;
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

                    /* Close-after-flush requested by QUIT / protocol error. */
                    if (c->status) {
                        conn_release(fd);
                        continue;
                    }
                    /* Phase 3.2: multishot recv stays armed across the
                     * write completion; nothing to do here. */
                }
            }
        }
        io_uring_cq_advance(&ring, nready);
    }

    if (g_buf_ring) {
        io_uring_free_buf_ring(&ring, g_buf_ring, KVS_NR_BUFS, KVS_BUF_RING_BGID);
        g_buf_ring = NULL;
    }
    if (g_buf_base && g_buf_base != MAP_FAILED) {
        munmap(g_buf_base, buf_pool_bytes);
        g_buf_base = NULL;
    }
    io_uring_queue_exit(&ring);
    free(conn_pool);
    return 0;
}
