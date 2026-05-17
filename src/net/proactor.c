#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
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
#include "kvstore.h"

#define EVENT_ACCEPT    0
#define EVENT_READ      1
#define EVENT_WRITE     2
#define EVENT_WAKE      3   /* Phase 4B.2: cross-worker wake eventfd readable */
#define ENTRIES_LENGTH  1024

/* Phase 3.2: provided buffer ring for multishot recv. */
#define KVS_BUF_RING_BGID  1
#define KVS_NR_BUFS        1024     /* must be power of two */
#define KVS_BUF_SIZE       4096

/* Phase 4: per-worker proactor.
 *
 * Each worker owns:
 *   - its own io_uring instance (so SINGLE_ISSUER stays valid),
 *   - its own provided-buffer ring,
 *   - its own SO_REUSEPORT listen socket bound to the same port.
 *
 * The connection pool (struct conn) is *shared* across workers because
 * file descriptors are unique within the process and each conn is only
 * ever touched by the worker that accepted it.  No locking needed.
 *
 * Sharded dict locks (kv_shard_*) provide the actual cross-worker
 * mutual exclusion at the dispatch layer.
 */
/* Phase 4B.2 — pending cross-worker wake event.  When worker A unblocks
 * a client whose connection is owned by worker B, A pushes the client
 * onto B's pending_wake list (under wake_mu); B drains its own list at
 * the top of its event loop and runs proactor_block_notify() locally
 * (so the SQE goes onto B's own ring, preserving SINGLE_ISSUER). */
typedef struct wake_node {
    struct client    *client;
    struct wake_node *next;
} wake_node_t;

typedef struct proactor_worker {
    int                       id;
    int                       listen_fd;
    pthread_t                 tid;
    struct io_uring           ring;
    struct io_uring_buf_ring *buf_ring;
    char                     *buf_base;
    size_t                    buf_pool_bytes;

    /* cross-worker wake queue (protected by wake_mu) */
    pthread_mutex_t           wake_mu;
    wake_node_t              *wake_head;
    int                       wake_evfd;   /* eventfd; another worker writes to it
                                              to nudge this worker out of
                                              io_uring_submit_and_wait */
} proactor_worker_t;

static struct conn   *conn_pool = NULL;
static int            conn_pool_size = 0;
static msg_handler    kvs_handler;
static proactor_worker_t *g_workers = NULL;
static int                g_n_workers = 0;

/* Thread-local pointer to the currently-executing worker.  Used by
 * buf_ring_recycle() and the block-notify callback so they don't need
 * an explicit worker argument plumbed through every call site. */
static __thread proactor_worker_t *tls_worker = NULL;

struct conn_info {
    int fd;
    int event;
};

/* Hand a buffer back to the kernel ring so it can be reused. */
static inline void buf_ring_recycle(unsigned bid) {
    proactor_worker_t *w = tls_worker;
    io_uring_buf_ring_add(w->buf_ring,
                          w->buf_base + bid * KVS_BUF_SIZE,
                          KVS_BUF_SIZE,
                          (unsigned short)bid,
                          io_uring_buf_ring_mask(KVS_NR_BUFS),
                          0);
    io_uring_buf_ring_advance(w->buf_ring, 1);
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

void set_event_send(struct io_uring *ring, int sockfd, void *buf, size_t len) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    struct conn_info ci = {.fd = sockfd, .event = EVENT_WRITE};
    io_uring_prep_send(sqe, sockfd, buf, len, MSG_NOSIGNAL);
    memcpy(&sqe->user_data, &ci, sizeof(ci));
}

/* Phase 3.1: multishot accept. */
void set_event_accept(struct io_uring *ring, int sockfd) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    struct conn_info ci = {.fd = sockfd, .event = EVENT_ACCEPT};
    io_uring_prep_multishot_accept(sqe, sockfd, NULL, NULL, SOCK_NONBLOCK);
    memcpy(&sqe->user_data, &ci, sizeof(ci));
}

/* ── Local notify: drain pipelined input, post send SQE on **this** worker's
 *    ring.  Only safe when invoked on the conn's owner worker.  Caller in
 *    cross-worker scenarios must route through proactor_block_notify(). ── */
static void proactor_block_notify_local(client_t *cl) {
    if (!cl || !cl->conn || !tls_worker) return;
    struct conn *conn = cl->conn;
    if (conn->read_buf.len > 0) {
        conn->w_off = 0;
        kvs_drain_resp_commands_off(&conn->read_buf, &conn->r_off,
                                     &conn->write_buf, conn);
    }
    if (conn->write_buf.len > 0) {
        set_event_send(&tls_worker->ring, conn->fd,
                       conn->write_buf.buf + conn->w_off,
                       conn->write_buf.len);
    }
}

/* ── Notify callback: block subsystem calls this when a client is unblocked.
 *    If the conn's owner worker == current worker, dispatch directly;
 *    otherwise enqueue on the owner worker's wake_head and kick its
 *    eventfd so it leaves io_uring_submit_and_wait() and processes us. ── */
static void proactor_block_notify(client_t *cl) {
    if (!cl || !cl->conn) return;
    struct conn *conn = cl->conn;

    /* Same-worker fast path. */
    if (tls_worker && conn->owner_worker_id == tls_worker->id) {
        proactor_block_notify_local(cl);
        return;
    }

    /* Cross-worker: enqueue + eventfd kick. */
    int oid = conn->owner_worker_id;
    if (oid < 0 || oid >= g_n_workers) return;
    proactor_worker_t *owner = &g_workers[oid];

    wake_node_t *n = (wake_node_t *)malloc(sizeof(*n));
    if (!n) return;
    n->client = cl;
    pthread_mutex_lock(&owner->wake_mu);
    n->next = owner->wake_head;
    owner->wake_head = n;
    pthread_mutex_unlock(&owner->wake_mu);

    if (owner->wake_evfd >= 0) {
        uint64_t one = 1;
        ssize_t r = write(owner->wake_evfd, &one, sizeof(one));
        (void)r;
    }
}

/* Drain the local wake queue, called at the top of each loop iteration. */
static void proactor_drain_wakes(proactor_worker_t *w) {
    pthread_mutex_lock(&w->wake_mu);
    wake_node_t *head = w->wake_head;
    w->wake_head = NULL;
    pthread_mutex_unlock(&w->wake_mu);
    while (head) {
        wake_node_t *next = head->next;
        proactor_block_notify_local(head->client);
        free(head);
        head = next;
    }
}

/* Public API: ask worker `id` to wake up from io_uring_submit_and_wait.
 * Used by the BLPOP register path so worker 0 re-evaluates its
 * block_nearest_timeout() right after a new blocked client lands. */
void proactor_kick_worker(int id) {
    if (id < 0 || id >= g_n_workers || !g_workers) return;
    int fd = g_workers[id].wake_evfd;
    if (fd < 0) return;
    uint64_t one = 1;
    ssize_t r = write(fd, &one, sizeof(one));
    (void)r;
}

/* Per-worker eventfd read buffer; one slot is enough since each successful
 * read drains the counter to zero before we re-arm. */
static __thread uint64_t tls_wake_evfd_buf;

/* Arm a read on the worker's wake eventfd so that cross-worker kicks
 * unblock io_uring_submit_and_wait().  The CQE is handled as
 * EVENT_WAKE in the main loop, which re-arms it and drains the queue. */
static void set_event_wake_read(struct io_uring *ring, int evfd) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) return;
    struct conn_info ci = {.fd = evfd, .event = EVENT_WAKE};
    io_uring_prep_read(sqe, evfd, &tls_wake_evfd_buf,
                       sizeof(tls_wake_evfd_buf), 0);
    memcpy(&sqe->user_data, &ci, sizeof(ci));
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

/* Listen socket is opened in the main thread (so we surface bind errors
 * before forking off worker threads); the ring itself MUST be initialised
 * inside the issuing thread for IORING_SETUP_SINGLE_ISSUER to be valid. */
static int worker_listen(proactor_worker_t *w, unsigned short port, int reuseport) {
    w->listen_fd = p_init_listen(port, reuseport);
    if (w->listen_fd < 0) return -1;
    /* Cross-worker wake eventfd.  Do NOT set EFD_NONBLOCK — we want the
     * io_uring IORING_OP_READ to park in async context until another
     * worker actually writes to the fd; non-blocking would short-cut
     * with -EAGAIN every iteration and burn CPU. */
    w->wake_evfd = eventfd(0, EFD_CLOEXEC);
    if (w->wake_evfd < 0) {
        perror("[proactor] eventfd");
        close(w->listen_fd); w->listen_fd = -1;
        return -1;
    }
    pthread_mutex_init(&w->wake_mu, NULL);
    w->wake_head = NULL;
    return 0;
}

static int worker_setup_ring(proactor_worker_t *w)
{
    /* Phase 3.3 setup flags.  With multiple workers each ring still has
     * a single issuer thread, so SINGLE_ISSUER + DEFER_TASKRUN remain
     * legal per-ring.  Falls back gracefully on older kernels. */
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    params.flags = IORING_SETUP_SINGLE_ISSUER
                 | IORING_SETUP_DEFER_TASKRUN
                 | IORING_SETUP_COOP_TASKRUN
                 | IORING_SETUP_SUBMIT_ALL;
    int qret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);
    if (qret == -EINVAL) {
        memset(&params, 0, sizeof(params));
        qret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);
    }
    if (qret < 0) {
        fprintf(stderr, "[proactor#%d] io_uring init: %s\n",
                w->id, strerror(-qret));
        return -1;
    }

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

    if (w->id == 0) {
        printf("[proactor] io_uring flags: 0x%x (single_issuer/defer_taskrun/coop)\n",
               params.flags);
        printf("[proactor] buf ring: %d x %d B = %zu MB per worker\n",
               KVS_NR_BUFS, KVS_BUF_SIZE, w->buf_pool_bytes / (1024 * 1024));
    }
    return 0;
}

static void worker_teardown(proactor_worker_t *w) {
    /* drain any straggler wake nodes */
    pthread_mutex_lock(&w->wake_mu);
    wake_node_t *n = w->wake_head; w->wake_head = NULL;
    pthread_mutex_unlock(&w->wake_mu);
    while (n) { wake_node_t *next = n->next; free(n); n = next; }
    pthread_mutex_destroy(&w->wake_mu);
    if (w->wake_evfd >= 0) { close(w->wake_evfd); w->wake_evfd = -1; }

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

static void *worker_loop(void *arg) {
    proactor_worker_t *w = (proactor_worker_t *)arg;
    tls_worker = w;

    /* Init the ring inside the issuing thread — IORING_SETUP_SINGLE_ISSUER
     * binds the ring to whichever task first submits an SQE. */
    if (worker_setup_ring(w) < 0) {
        g_kvs_stop = 1;
        return NULL;
    }
    struct io_uring *ring = &w->ring;
    int sockfd = w->listen_fd;

    set_event_accept(ring, sockfd);
    set_event_wake_read(ring, w->wake_evfd);     /* Phase 4B.2 */

    while (!g_kvs_stop) {
        if (w->id == 0) update_cached_time();   /* one writer is enough */

        struct io_uring_cqe *cqes[128];
        int nready = io_uring_peek_batch_cqe(ring, cqes, 128);

        if (nready == 0) {
            /* Block subsystem is now MT-safe; only worker 0 sweeps timeouts
             * to keep work centralised, but any worker may have callbacks
             * routed to it via the wake queue. */
            if (w->id == 0) block_check_timeouts();
            int to_ms = (w->id == 0) ? block_nearest_timeout() : -1;
            if (to_ms >= 0) {
                struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
                if (sqe) {
                    struct __kernel_timespec ts;
                    ts.tv_sec  = to_ms / 1000;
                    ts.tv_nsec = (to_ms % 1000) * 1000000L;
                    io_uring_prep_timeout(sqe, &ts, 0, 0);
                    sqe->user_data = 0;
                }
            }
            int ret = io_uring_submit_and_wait(ring, 1);
            if (ret < 0) {
                if (ret == -EINTR) continue;
                break;
            }
            nready = io_uring_peek_batch_cqe(ring, cqes, 128);
            if (nready <= 0) continue;
        }

        for (int i = 0; i < nready; i++) {
            struct io_uring_cqe *cqe = cqes[i];
            struct conn_info result;
            memcpy(&result, &cqe->user_data, sizeof(result));
            int fd = result.fd;
            int res = cqe->res;

            if (fd == 0 && result.event == 0) continue;     /* timeout */

            if (result.event == EVENT_ACCEPT) {
                if (!(cqe->flags & IORING_CQE_F_MORE))
                    set_event_accept(ring, sockfd);
                int connfd = res;
                if (connfd < 0) continue;
                if (connfd >= conn_pool_size) {
                    fprintf(stderr, "[proactor#%d] fd %d exceeds maxclients %d\n",
                            w->id, connfd, conn_pool_size);
                    close(connfd);
                    continue;
                }
                dynbuf_init(&conn_pool[connfd].read_buf,  KVS_NET_BUF_INIT);
                dynbuf_init(&conn_pool[connfd].write_buf, KVS_NET_BUF_INIT);
                conn_pool[connfd].fd = connfd;
                conn_pool[connfd].active = 1;
                conn_pool[connfd].r_off = 0;
                conn_pool[connfd].w_off = 0;
                conn_pool[connfd].owner_worker_id = w->id;   /* Phase 4B.2 */
                set_event_recv_multishot(ring, connfd);
            }
            else if (result.event == EVENT_READ) {
                if (fd < 0 || fd >= conn_pool_size) {
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

                if (res > 0 && (cqe->flags & IORING_CQE_F_BUFFER)) {
                    unsigned bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
                    char *src = w->buf_base + (size_t)bid * KVS_BUF_SIZE;

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

                if (res <= 0) {
                    if (res == -ENOBUFS) {
                        /* multishot died; will re-arm below */
                    } else {
                        if (rb->len > 0)
                            kvs_drain_resp_commands_off(rb, &c->r_off, wb, c);
                        if (wb->len > 0)
                            set_event_send(ring, fd, wb->buf + c->w_off, wb->len);
                        else
                            conn_release(fd);
                        continue;
                    }
                } else {
                    kvs_drain_resp_commands_off(rb, &c->r_off, wb, c);

                    if (c->client && c->client->state == CLIENT_BLOCKED) {
                        if (wb->len > 0)
                            set_event_send(ring, fd, wb->buf + c->w_off, wb->len);
                    } else if (c->status) {
                        if (wb->len > 0)
                            set_event_send(ring, fd, wb->buf + c->w_off, wb->len);
                        else
                            conn_release(fd);
                        if (multishot_dead) multishot_dead = 0;
                    } else if (wb->len > 0) {
                        set_event_send(ring, fd, wb->buf + c->w_off, wb->len);
                    }
                }

                if (multishot_dead && c->active && !c->status)
                    set_event_recv_multishot(ring, fd);
event_read_done: ;
            }
            else if (result.event == EVENT_WAKE) {
                /* eventfd became readable: another worker enqueued a wake
                 * (or someone kicked worker 0 to recompute timeouts).
                 * Re-arm the read first, then drain the queue.  This
                 * keeps the per-iteration hot path lock-free. */
                set_event_wake_read(ring, w->wake_evfd);
                proactor_drain_wakes(w);
            }
            else if (result.event == EVENT_WRITE) {
                if (fd < 0 || fd >= conn_pool_size) continue;
                struct conn *c = &conn_pool[fd];
                if (!c->active) continue;
                DynBuf *wb = &c->write_buf;

                if (res <= 0) { conn_release(fd); continue; }

                c->w_off += res;
                wb->len  -= res;

                if (wb->len > 0) {
                    set_event_send(ring, fd, wb->buf + c->w_off, wb->len);
                } else {
                    c->w_off = 0;
                    dynbuf_clear(wb);
                    if (c->status) { conn_release(fd); continue; }
                }
            }
        }
        io_uring_cq_advance(ring, nready);
    }
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

    g_n_workers = kvs_config.threads > 0 ? kvs_config.threads : 1;
    g_workers = (proactor_worker_t *)calloc((size_t)g_n_workers,
                                            sizeof(proactor_worker_t));
    if (!g_workers) { free(conn_pool); return -1; }

    int reuseport = (g_n_workers > 1);
    for (int i = 0; i < g_n_workers; i++) {
        g_workers[i].id = i;
        g_workers[i].listen_fd = -1;
        if (worker_listen(&g_workers[i], port, reuseport) < 0) {
            for (int j = 0; j < i; j++)
                if (g_workers[j].listen_fd >= 0) close(g_workers[j].listen_fd);
            free(g_workers); free(conn_pool);
            return -1;
        }
    }
    if (g_n_workers > 1)
        printf("[proactor] %d workers, SO_REUSEPORT, sharded dict (32 shards)\n",
               g_n_workers);

    /* Phase 4B.2: block subsystem is now MT-safe; arm the notify_cb
     * unconditionally.  Cross-worker wakeups route through the per-
     * worker wake queue + eventfd inside proactor_block_notify(). */
    block_set_notify_cb(proactor_block_notify);

    /* Worker 0 runs on main thread; spawn pthreads for the rest. */
    for (int i = 1; i < g_n_workers; i++) {
        if (pthread_create(&g_workers[i].tid, NULL, worker_loop, &g_workers[i])) {
            perror("[proactor] pthread_create");
            g_kvs_stop = 1;
            break;
        }
    }
    worker_loop(&g_workers[0]);

    /* Tear down */
    for (int i = 1; i < g_n_workers; i++) {
        pthread_join(g_workers[i].tid, NULL);
    }
    for (int i = 0; i < g_n_workers; i++) worker_teardown(&g_workers[i]);
    free(g_workers); g_workers = NULL;
    free(conn_pool); conn_pool = NULL;
    return 0;
}
