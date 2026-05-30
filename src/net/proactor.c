#define _GNU_SOURCE
#include <stdio.h>
#include <sched.h>
#include <liburing.h>
#include <netinet/in.h>
#include <netinet/tcp.h>     /* TCP_NODELAY — disables Nagle on accepted conns */
#include <sys/socket.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/eventfd.h>
#include "server.h"
#include "config.h"
#include "protocol.h"
#include "block.h"
#include "client.h"        /* Phase 8.2 — eager client_t alloc on accept */
#include "kvstore.h"

#define EVENT_ACCEPT    0
#define EVENT_READ      1
#define EVENT_WRITE     2
#define EVENT_WAKE      3
#define ENTRIES_LENGTH  8192     /* Increased from 4096 for high-concurrency workloads */
#define CQE_BATCH_SIZE  512      /* Increased from 256 for better batching */

/* Phase 3.2: provided buffer ring for multishot recv. */
#define KVS_BUF_RING_BGID  1
#define KVS_NR_BUFS        8192     /* Increased from 4096 for high-concurrency (must be power of two) */
#define KVS_BUF_SIZE       4096

/* Proactor using io_uring.
 *
 * Owns:
 *   - One io_uring instance (SINGLE_ISSUER + SQPOLL),
 *   - One provided-buffer ring,
 *   - One listen socket.
 */

typedef struct proactor_worker {
    int                       id;
    int                       listen_fd;
    struct io_uring           ring;
    struct io_uring_buf_ring *buf_ring;
    char                     *buf_base;
    size_t                    buf_pool_bytes;
    int                       accept_armed;
    int                       sqpoll_enabled;
} proactor_worker_t;

static struct conn   *conn_pool = NULL;
static int            conn_pool_size = 0;
static msg_handler    kvs_handler;
static proactor_worker_t *g_worker = NULL;
static atomic_uint         g_next_conn_gen = 1;
static int                 g_proactor_debug = -1;

static int proactor_debug_enabled(void) {
    if (g_proactor_debug < 0)
        g_proactor_debug = getenv("KVS_PROACTOR_DEBUG") ? 1 : 0;
    return g_proactor_debug;
}

struct conn_info {
    int fd;
    int event;
    uint32_t gen;
};

static uint64_t conn_info_pack(int fd, int event, uint32_t gen) {
    return ((uint64_t)(uint32_t)fd)
         | ((uint64_t)(uint8_t)event << 32)
         | ((uint64_t)(gen & 0xFFFFFFu) << 40);
}

static struct conn_info conn_info_unpack(uint64_t data) {
    struct conn_info ci;
    ci.fd = (int)(uint32_t)data;
    ci.event = (int)((data >> 32) & 0xFFu);
    ci.gen = (uint32_t)((data >> 40) & 0xFFFFFFu);
    return ci;
}

static uint32_t next_conn_gen(void) {
    uint32_t gen;
    do {
        gen = atomic_fetch_add_explicit(&g_next_conn_gen, 1,
                                        memory_order_relaxed) & 0xFFFFFFu;
    } while (gen == 0);
    return gen;
}

static struct io_uring_sqe *get_sqe_submit(struct io_uring *ring) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (sqe) return sqe;
    if (io_uring_submit(ring) < 0) return NULL;
    return io_uring_get_sqe(ring);
}

/* Hand a buffer back to the kernel ring so it can be reused. */
static inline void buf_ring_recycle(unsigned bid) {
    proactor_worker_t *w = g_worker;
    io_uring_buf_ring_add(w->buf_ring,
                          w->buf_base + bid * KVS_BUF_SIZE,
                          KVS_BUF_SIZE,
                          (unsigned short)bid,
                          io_uring_buf_ring_mask(KVS_NR_BUFS),
                          0);
    io_uring_buf_ring_advance(w->buf_ring, 1);
}

/* Arm a true multishot recv (IORING_RECV_MULTISHOT) that pulls
 * buffers from the provided buffer ring (buf_group KVS_BUF_RING_BGID).
 *
 * One SQE delivers multiple CQEs — the kernel picks a fresh buffer from
 * the ring for each chunk of received data.  CQEs carry IORING_CQE_F_MORE
 * until the last delivery.  This eliminates the per-read re-arm overhead:
 * a single SQE can service an entire connection's read workload.
 *
 * The buffer and length parameters are left zero — the kernel obtains
 * them from the provided-buffer ring via IOSQE_BUFFER_SELECT. */
static int set_event_recv_multishot(struct io_uring *ring, int sockfd, uint32_t gen) {
    struct io_uring_sqe *sqe = get_sqe_submit(ring);
    if (!sqe) return -1;
    io_uring_prep_recv_multishot(sqe, sockfd, NULL, 0, 0);
    sqe->buf_group = KVS_BUF_RING_BGID;
    sqe->flags |= IOSQE_BUFFER_SELECT;
    sqe->user_data = conn_info_pack(sockfd, EVENT_READ, gen);
    return 0;
}

static int conn_arm_recv(struct io_uring *ring, struct conn *c) {
    if (!c || !c->active || c->recv_armed)
        return 0;
    if (c->client && c->client->state == CLIENT_BLOCKED)
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

static int p_init_listen(unsigned short port, int reuseport) {
    int sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    struct sockaddr_in serveraddr;
    memset(&serveraddr, 0, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
    serveraddr.sin_port = htons(port);

    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    if (reuseport)
        setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

    if (bind(sockfd, (struct sockaddr*)&serveraddr, sizeof(serveraddr)) == -1) {
        perror("bind");
        close(sockfd);
        return -1;
    }
    listen(sockfd, SOMAXCONN);
    return sockfd;
}

static int set_event_send(struct io_uring *ring, int sockfd, uint32_t gen, void *buf, size_t len) {
    struct io_uring_sqe *sqe = get_sqe_submit(ring);
    if (!sqe) return -1;
    io_uring_prep_send(sqe, sockfd, buf, len, MSG_NOSIGNAL);
    sqe->user_data = conn_info_pack(sockfd, EVENT_WRITE, gen);
    return 0;
}

static int conn_arm_send(struct io_uring *ring, struct conn *c) {
    if (!c || !c->active || c->send_armed || c->write_buf.len <= 0)
        return 0;
    if (set_event_send(ring, c->fd, c->gen, c->write_buf.buf + c->w_off,
                       c->write_buf.len) != 0)
        return -1;
    c->send_armed = 1;
    return 0;
}

static int set_event_accept(struct io_uring *ring, int sockfd) {
    struct io_uring_sqe *sqe = get_sqe_submit(ring);
    if (!sqe) return -1;
    io_uring_prep_multishot_accept(sqe, sockfd, NULL, NULL, SOCK_NONBLOCK);
    sqe->user_data = conn_info_pack(sockfd, EVENT_ACCEPT, 0);
    return 0;
}

static int worker_arm_accept(proactor_worker_t *w) {
    if (!w || w->accept_armed)
        return 0;
    if (set_event_accept(&w->ring, w->listen_fd) != 0)
        return -1;
    w->accept_armed = 1;
    if (proactor_debug_enabled())
        fprintf(stderr, "[proactor#%d] accept armed\n", w->id);
    return 0;
}

static void conn_release(int fd);

/* ── Local notify: drain pipelined input, post send SQE.
 *
 *    This is the place where a BLPOP/BRPOP wake reply (either retried
 *    saved command or pending timeout) gets emitted. ── */
static void proactor_block_notify_local(client_t *cl) {
    if (!cl || !cl->conn || !g_worker) return;
    struct conn *conn = cl->conn;

    /* (a) Timed-out BLPOP: emit the nil reply locally. */
    if (cl->block_pending_timeout) {
        dynbuf_cat(&conn->write_buf, "$-1\r\n", 5);
        cl->block_pending_timeout = 0;
    }

    /* (b) Awakened BLPOP via LPUSH/RPUSH: re-dispatch the saved BLPOP
     *     command so the standard list_cmd path emits the reply. */
    if (cl->block_cmd_len > 0) {
        kvs_token_t tokens2[KVS_MAX_TOKENS];
        int argc2 = 0;
        int consumed = resp_parse_slices(cl->block_cmd_buf,
                                          cl->block_cmd_len,
                                          tokens2, KVS_MAX_TOKENS, &argc2);
        if (consumed > 0) {
            kvs_dispatch(tokens2, argc2,
                         cl->block_cmd_buf, consumed,
                         &conn->write_buf, conn);
        }
        cl->block_cmd_len = 0;
    }

    /* (c) Drain any pipelined RX bytes that arrived while blocked. */
    if (conn->read_buf.len > 0) {
        conn->w_off = 0;
        kvs_drain_resp_commands_off(&conn->read_buf, &conn->r_off,
                                     &conn->write_buf, conn);
    }
    if (conn->write_buf.len > 0) {
        if (conn_arm_send(&g_worker->ring, conn) != 0)
            conn_release(conn->fd);
    } else {
        if (conn_arm_recv(&g_worker->ring, conn) != 0)
            conn_release(conn->fd);
    }
}

/* ── Notify callback: block subsystem calls this when a client is unblocked. ── */
static void proactor_block_notify(client_t *cl) {
    if (!cl || !cl->conn) return;
    proactor_block_notify_local(cl);
}

/* Public API stub */
void proactor_kick_worker(int id) {
    (void)id;
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
    /* Phase 9 — INFO connected_clients gauge.  Decrement only if the slot
     * was actually active (defensive in case conn_release is called twice
     * on the same slot via overlapping CQEs). */
    if (c->active) {
        atomic_fetch_sub_explicit(&g_kvs_connected_clients, 1,
                                   memory_order_relaxed);
    }
    if (proactor_debug_enabled())
        fprintf(stderr, "[proactor] release fd=%d gen=%u recv_armed=%d send_armed=%d rb_len=%d wb_len=%d\n",
                fd, c->gen, c->recv_armed, c->send_armed, c->read_buf.len, c->write_buf.len);
    dynbuf_free(&c->read_buf);
    dynbuf_free(&c->write_buf);
    memset(c, 0, sizeof(*c));
    close(fd);
}

/* Listen socket initialization */
static int worker_listen(proactor_worker_t *w, unsigned short port, int reuseport) {
    w->listen_fd = p_init_listen(port, reuseport);
    if (w->listen_fd < 0) return -1;
    return 0;
}

static int worker_setup_ring(proactor_worker_t *w)
{
    /* SQPOLL and DEFER_TASKRUN are mutually exclusive:
     * - SQPOLL: kernel thread polls SQ, best for high throughput
     * - DEFER_TASKRUN: userspace polling, best for low latency
     * Try SQPOLL first, then fall back to DEFER_TASKRUN path. */
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    /* Try SQPOLL + SINGLE_ISSUER (high throughput path) */
    params.flags = IORING_SETUP_SQPOLL | IORING_SETUP_SINGLE_ISSUER;
    params.sq_thread_idle = 1000;  /* SQPOLL thread sleeps after 1s idle */
    int qret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);

    if (qret == -EINVAL) {
        /* SQPOLL failed, try DEFER_TASKRUN path (low latency) */
        memset(&params, 0, sizeof(params));
        params.flags = IORING_SETUP_SINGLE_ISSUER
                     | IORING_SETUP_DEFER_TASKRUN
                     | IORING_SETUP_COOP_TASKRUN;
        qret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);
    }
    if (qret == -EINVAL) {
        /* Retry without COOP_TASKRUN (kernel < 6.1) */
        memset(&params, 0, sizeof(params));
        params.flags = IORING_SETUP_SINGLE_ISSUER
                     | IORING_SETUP_DEFER_TASKRUN;
        qret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);
    }
    if (qret == -EINVAL) {
        /* Retry with only SINGLE_ISSUER (kernel < 5.19) */
        memset(&params, 0, sizeof(params));
        params.flags = IORING_SETUP_SINGLE_ISSUER;
        qret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);
    }
    if (qret == -EINVAL) {
        /* Bare minimum (kernel < 5.13) */
        memset(&params, 0, sizeof(params));
        params.flags = 0;
        qret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);
    }
    if (qret < 0) {
        fprintf(stderr, "[proactor#%d] io_uring init: %s\n",
                w->id, strerror(-qret));
        return -1;
    }

    w->sqpoll_enabled = (params.flags & IORING_SETUP_SQPOLL) ? 1 : 0;

    /* Phase 3.2 buffer ring */
    w->buf_pool_bytes = (size_t)KVS_NR_BUFS * KVS_BUF_SIZE;
    w->buf_base = mmap(NULL, w->buf_pool_bytes, PROT_READ | PROT_WRITE,
                       MAP_ANON | MAP_PRIVATE, -1, 0);
    if (w->buf_base == MAP_FAILED) {
        perror("[proactor] mmap buf pool");
        io_uring_queue_exit(&w->ring);
        return -1;
    }
    int br_err = 0;
    w->buf_ring = io_uring_setup_buf_ring(&w->ring, KVS_NR_BUFS,
                                          KVS_BUF_RING_BGID, 0, &br_err);
    if (!w->buf_ring) {
        fprintf(stderr, "[proactor#%d] setup_buf_ring: %s\n",
                w->id, strerror(-br_err));
        munmap(w->buf_base, w->buf_pool_bytes);
        io_uring_queue_exit(&w->ring);
        return -1;
    }
    int br_mask = io_uring_buf_ring_mask(KVS_NR_BUFS);
    for (int i = 0; i < KVS_NR_BUFS; i++) {
        io_uring_buf_ring_add(w->buf_ring,
                              w->buf_base + (size_t)i * KVS_BUF_SIZE,
                              KVS_BUF_SIZE,
                              (unsigned short)i, br_mask, i);
    }
    io_uring_buf_ring_advance(w->buf_ring, KVS_NR_BUFS);

    /* Single-threaded: no wake eventfd needed */

    if (w->id == 0) {
        printf("[proactor] io_uring flags: 0x%x%s\n",
               params.flags,
               w->sqpoll_enabled ? " (SQPOLL enabled - reduced syscalls)" : "");
        printf("[proactor] buf ring: %d x %d B = %zu MB per worker\n",
               KVS_NR_BUFS, KVS_BUF_SIZE, w->buf_pool_bytes / (1024 * 1024));
    }
    return 0;
}

static void worker_teardown(proactor_worker_t *w) {
    /* Single-threaded: no wake nodes to drain */

    if (w->buf_ring) {
        io_uring_free_buf_ring(&w->ring, w->buf_ring, KVS_NR_BUFS, KVS_BUF_RING_BGID);
        w->buf_ring = NULL;
    }
    if (w->buf_base && w->buf_base != MAP_FAILED) {
        munmap(w->buf_base, w->buf_pool_bytes);
        w->buf_base = NULL;
    }
    io_uring_queue_exit(&w->ring);
    if (w->listen_fd >= 0) close(w->listen_fd);
}

/* Single-threaded: no CPU pinning needed */
static int worker_pin_self(int id) {
    (void)id;
    return -1;  /* pinning disabled */
}

static void *worker_loop(void *arg) {
    proactor_worker_t *w = (proactor_worker_t *)arg;
    g_worker = w;

    /* Bind to a CPU core BEFORE setting up the io_uring ring so the ring
     * is associated with this core's NUMA / cache domain from creation. */
    int pinned = worker_pin_self(w->id);
    if (pinned >= 0)
        printf("[proactor] worker %d pinned to core %d\n", w->id, pinned);

    /* Init the ring inside the issuing thread — IORING_SETUP_SINGLE_ISSUER
     * binds the ring to whichever task first submits an SQE. */
    if (worker_setup_ring(w) < 0) {
        g_kvs_stop = 1;
        return NULL;
    }
    struct io_uring *ring = &w->ring;
    int sockfd = w->listen_fd;

    if (worker_arm_accept(w) != 0) {
        fprintf(stderr, "[proactor#%d] failed to arm accept\n", w->id);
        return NULL;
    }

    while (!g_kvs_stop) {
        if (w->id == 0) update_cached_time();   /* one writer is enough */

        struct io_uring_cqe *cqes[CQE_BATCH_SIZE];
        int nready = io_uring_peek_batch_cqe(ring, cqes, CQE_BATCH_SIZE);

        if (nready == 0) {
            /* Block subsystem is now MT-safe; only worker 0 sweeps timeouts
             * to keep work centralised, but any worker may have callbacks
             * routed to it via the wake queue. */
            if (w->id == 0) block_check_timeouts();
            int to_ms = (w->id == 0) ? block_nearest_timeout() : -1;
            if (to_ms >= 0) {
                struct io_uring_sqe *sqe = get_sqe_submit(ring);
                if (sqe) {
                    struct __kernel_timespec ts;
                    ts.tv_sec  = to_ms / 1000;
                    ts.tv_nsec = (to_ms % 1000) * 1000000L;
                    io_uring_prep_timeout(sqe, &ts, 0, 0);
                    sqe->user_data = conn_info_pack(0, 0, 0);
                }
            }
            int ret = io_uring_submit_and_wait(ring, 1);
            if (ret < 0) {
                if (ret == -EINTR) continue;
                fprintf(stderr, "[proactor#%d] submit_and_wait: %s\n",
                        w->id, strerror(-ret));
                break;
            }
            nready = io_uring_peek_batch_cqe(ring, cqes, CQE_BATCH_SIZE);
            if (nready <= 0) continue;
        }

        for (int i = 0; i < nready; i++) {
            struct io_uring_cqe *cqe = cqes[i];
            struct conn_info result = conn_info_unpack(cqe->user_data);
            int fd = result.fd;
            int res = cqe->res;

            if (fd == 0 && result.event == 0) continue;     /* timeout */

            if (result.event == EVENT_ACCEPT) {
                if (!(cqe->flags & IORING_CQE_F_MORE))
                    w->accept_armed = 0;
                int connfd = res;
                if (connfd < 0) {
                    if (connfd != -EAGAIN && connfd != -ECANCELED)
                        fprintf(stderr, "[proactor#%d] accept cqe: %s\n",
                                w->id, strerror(-connfd));
                    continue;
                }
                if (proactor_debug_enabled())
                    fprintf(stderr, "[proactor#%d] accept fd=%d\n", w->id, connfd);
                if (connfd >= conn_pool_size) {
                    fprintf(stderr, "[proactor#%d] fd %d exceeds maxclients %d\n",
                            w->id, connfd, conn_pool_size);
                    close(connfd);
                    continue;
                }
                /* TCP_NODELAY: disable Nagle on the accepted socket.  Without
                 * this, the kernel buffers small replies (e.g. "+OK\r\n",
                 * "$5\r\nworld\r\n") waiting for either ~40 ms or piggybacking
                 * on the next ACK.  Combined with the client's delayed-ACK
                 * timer, single-RTT loopback latency degrades from ~50 µs to
                 * 3-40 ms, collapsing single-connection QPS by ~100x.
                 * One setsockopt per accept is cheap (~150 ns) and only paid
                 * once per connection. */
                int nd = 1;
                setsockopt(res, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd));

                struct conn *nc = &conn_pool[connfd];
                dynbuf_init(&nc->read_buf,  KVS_NET_BUF_INIT);
                dynbuf_init(&nc->write_buf, KVS_NET_BUF_INIT);
                nc->fd = connfd;
                nc->active = 1;
                nc->r_off = 0;
                nc->w_off = 0;
                nc->recv_armed = 0;
                nc->send_armed = 0;
                nc->gen = next_conn_gen();
                nc->owner_worker_id = w->id;   /* Phase 4B.2 */
                /* Phase 9 — INFO total/live counters. */
                atomic_fetch_add_explicit(&g_kvs_total_connections, 1,
                                          memory_order_relaxed);
                atomic_fetch_add_explicit(&g_kvs_connected_clients, 1,
                                          memory_order_relaxed);
                /* Phase 8.2 — eager client_t allocation.  Used to be lazy
                 * inside kvs_dispatch, which TSAN flagged as racing with
                 * the previous owner's conn_release on the same slot.
                 * Allocating here keeps every conn_pool[fd] write inside
                 * the kernel-mediated accept→close ownership window. */
                nc->client = (client_t *)calloc(1, sizeof(client_t));
                if (nc->client) nc->client->conn = nc;
                if (conn_arm_recv(ring, nc) != 0)
                    conn_release(connfd);
                else if (proactor_debug_enabled()) {
                    int submitted = io_uring_submit(ring);
                    fprintf(stderr, "[proactor#%d] submit after accept fd=%d ret=%d\n",
                            w->id, connfd, submitted);
                } else {
                    io_uring_submit(ring);
                }
            }
            else if (result.event == EVENT_READ) {
                if (fd < 0 || fd >= conn_pool_size) {
                    if (cqe->flags & IORING_CQE_F_BUFFER)
                        buf_ring_recycle(cqe->flags >> IORING_CQE_BUFFER_SHIFT);
                    continue;
                }
                struct conn *c = &conn_pool[fd];

                if (!c->active) {
                    if (cqe->flags & IORING_CQE_F_BUFFER)
                        buf_ring_recycle(cqe->flags >> IORING_CQE_BUFFER_SHIFT);
                    continue;
                }
                if (result.gen != (c->gen & 0xFFFFFFu)) {
                    if (proactor_debug_enabled())
                        fprintf(stderr, "[proactor#%d] stale read fd=%d cqe_gen=%u cur_gen=%u res=%d flags=0x%x\n",
                                w->id, fd, result.gen, c->gen & 0xFFFFFFu,
                                res, cqe->flags);
                    if (cqe->flags & IORING_CQE_F_BUFFER)
                        buf_ring_recycle(cqe->flags >> IORING_CQE_BUFFER_SHIFT);
                    continue;
                }
                /* Multishot recv: the SQE stays armed across multiple CQEs.
                 * Only clear recv_armed when the kernel signals termination
                 * (IORING_CQE_F_MORE absent).  On the last delivery — or on
                 * error/close — conn_arm_recv() will submit a fresh SQE. */
                if (!(cqe->flags & IORING_CQE_F_MORE))
                    c->recv_armed = 0;
                DynBuf *rb = &c->read_buf;
                DynBuf *wb = &c->write_buf;

                if (res > 0 && (cqe->flags & IORING_CQE_F_BUFFER)) {
                    unsigned bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
                    char *src = w->buf_base + (size_t)bid * KVS_BUF_SIZE;

                    size_t need = c->r_off + rb->len + (size_t)res;
                    if (need > rb->cap) {
                        /* Optimized growth: for large jumps (pipeline bursts),
                         * allocate 1.5x to reduce realloc frequency while limiting waste.
                         * For normal growth, use 2x doubling. Cap at 16MB to prevent DoS. */
                        size_t new_cap = rb->cap;
                        const size_t MAX_BUF_CAP = 16 * 1024 * 1024;  /* 16MB limit */

                        if (need > MAX_BUF_CAP) {
                            buf_ring_recycle(bid);
                            conn_release(fd);
                            goto event_read_done;
                        }

                        if (need > rb->cap * 2) {
                            /* Large jump: use 1.5x growth */
                            new_cap = need + (need >> 1);
                        } else {
                            /* Normal growth: 2x doubling */
                            new_cap = rb->cap * 2;
                            while (new_cap < need) new_cap *= 2;
                        }

                        if (new_cap > MAX_BUF_CAP) new_cap = MAX_BUF_CAP;

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
                } else if (res > 0) {
                    rb->len += res;
                }

                if (res <= 0) {
                    if (res == -ENOBUFS) {
                        if (conn_arm_recv(ring, c) != 0)
                            conn_release(fd);
                        continue;
                    }
                    if (res < 0 && res != -ECANCELED)
                        fprintf(stderr, "[proactor#%d] recv fd=%d cqe: %s\n",
                                w->id, fd, strerror(-res));
                    /* Connection closed or error */
                    if (rb->len > 0) {
                        int drained = kvs_drain_resp_commands_off(rb, &c->r_off, wb, c);
                        if (proactor_debug_enabled())
                            fprintf(stderr, "[proactor#%d] fd=%d eof-drain res=%d rb_len=%d r_off=%d wb_len=%d\n",
                                    w->id, fd, drained, rb->len, c->r_off, wb->len);
                    }
                    if (wb->len > 0) {
                        if (conn_arm_send(ring, c) != 0)
                            conn_release(fd);
                    } else {
                        conn_release(fd);
                    }
                    continue;
                } else {
                    /* Defer drain while a send SQE is in flight: drain may
                     * append to write_buf and trigger realloc, which would
                     * corrupt the in-flight send buffer pointer.  Re-arm
                     * recv so more data can arrive while we wait. */
                    if (c->send_armed || wb->len > 0) {
                        conn_arm_recv(ring, c);
                        goto event_read_done;
                    }

                    int drained = kvs_drain_resp_commands_off(rb, &c->r_off, wb, c);
                    if (proactor_debug_enabled())
                        fprintf(stderr, "[proactor#%d] fd=%d read res=%d drained=%d rb_len=%d r_off=%d wb_len=%d send_armed=%d recv_armed=%d\n",
                                w->id, fd, res, drained, rb->len, c->r_off, wb->len,
                                c->send_armed, c->recv_armed);

                    if (c->client && c->client->state == CLIENT_BLOCKED) {
                        if (wb->len > 0 && conn_arm_send(ring, c) != 0)
                            conn_release(fd);
                    } else if (c->status) {
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
event_read_done: ;
            }
            /* Single-threaded: no EVENT_WAKE handling needed */
            else if (result.event == EVENT_WRITE) {
                if (fd < 0 || fd >= conn_pool_size) continue;
                struct conn *c = &conn_pool[fd];
                if (!c->active) continue;
                if (result.gen != (c->gen & 0xFFFFFFu)) continue;
                c->send_armed = 0;
                DynBuf *wb = &c->write_buf;

                if (res <= 0) { conn_release(fd); continue; }

                c->w_off += res;
                wb->len  -= res;
                if (proactor_debug_enabled())
                    fprintf(stderr, "[proactor#%d] fd=%d send res=%d wb_len=%d w_off=%d rb_len=%d r_off=%d\n",
                            w->id, fd, res, wb->len, c->w_off, c->read_buf.len, c->r_off);

                if (wb->len > 0) {
                    /* More data to send */
                    if (conn_arm_send(ring, c) != 0)
                        conn_release(fd);
                } else {
                    /* Write complete */
                    c->w_off = 0;
                    dynbuf_clear(wb);
                    if (c->status) {
                        conn_release(fd);
                    } else if (c->read_buf.len > 0) {
                        int drained = kvs_drain_resp_commands_off(&c->read_buf, &c->r_off, wb, c);
                        if (proactor_debug_enabled())
                            fprintf(stderr, "[proactor#%d] fd=%d post-send-drain drained=%d rb_len=%d r_off=%d wb_len=%d\n",
                                    w->id, fd, drained, c->read_buf.len, c->r_off, wb->len);
                        if (c->status) {
                            if (wb->len > 0) {
                                if (conn_arm_send(ring, c) != 0)
                                    conn_release(fd);
                            } else {
                                conn_release(fd);
                            }
                        } else if (c->client && c->client->state == CLIENT_BLOCKED) {
                            if (wb->len > 0 && conn_arm_send(ring, c) != 0)
                                conn_release(fd);
                        } else if (wb->len > 0) {
                            if (conn_arm_send(ring, c) != 0)
                                conn_release(fd);
                        } else {
                            if (conn_arm_recv(ring, c) != 0)
                                conn_release(fd);
                            else
                                io_uring_submit(ring);
                        }
                    } else {
                        if (conn_arm_recv(ring, c) != 0)
                            conn_release(fd);
                        else
                            io_uring_submit(ring);
                    }
                }
            }
        }
        if (worker_arm_accept(w) != 0)
            fprintf(stderr, "[proactor#%d] failed to re-arm accept\n", w->id);
        io_uring_cq_advance(ring, nready);
        io_uring_submit(ring);
    }
    fprintf(stderr, "[proactor#%d] worker exit stop=%d\n", w->id, (int)g_kvs_stop);
    return NULL;
}

int proactor_start(unsigned short port, msg_handler handler) {
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

    /* Single-threaded mode: only one worker */
    g_worker = (proactor_worker_t *)calloc(1, sizeof(proactor_worker_t));
    if (!g_worker) { free(conn_pool); return -1; }

    g_worker->id = 0;
    g_worker->listen_fd = -1;
    if (worker_listen(g_worker, port, 0) < 0) {
        free(g_worker); free(conn_pool);
        return -1;
    }

    printf("[proactor] single-threaded mode, single global dict\n");

    /* Single-threaded: simplified notify callback */
    block_set_notify_cb(proactor_block_notify);

    /* Run single worker on main thread */
    worker_loop(g_worker);

    /* Tear down */
    worker_teardown(g_worker);
    free(g_worker); g_worker = NULL;
    free(conn_pool); conn_pool = NULL;
    return 0;
}
