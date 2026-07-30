#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <liburing.h>

#include "proactor.h"
#include "config.h"
#include "kvstore.h"

/* ── Global state ───────────────────────────────────────────────────── */

struct conn *conn_pool = NULL;
int conn_pool_size = 0;
proactor_worker_t *g_worker = NULL;
uint32_t g_next_conn_gen = 1;

/* External function declarations */
extern void event_handle_cqe(proactor_worker_t *w, struct io_uring_cqe *cqe);

/* ═══════════════════════════════════════════════════════════════════════
 *  Part 1. Connection pool
 * ═══════════════════════════════════════════════════════════════════════ */

int conn_pool_init(int max_clients) {
    conn_pool_size = max_clients;
    conn_pool = calloc(max_clients, sizeof(struct conn));
    if (!conn_pool) {
        fprintf(stderr, "[conn] failed to allocate connection pool\n");
        return -1;
    }
    /* Pre-allocate client_t for every slot so accept never calls calloc */
    for (int i = 0; i < max_clients; i++) {
        conn_pool[i].client = calloc(1, sizeof(client_t));
        if (conn_pool[i].client)
            conn_pool[i].client->conn = &conn_pool[i];
    }
    printf("[conn] pool: %d slots, struct %zu MB (lazy buffer, pre-alloc client)\n",
           max_clients,
           (sizeof(struct conn) * (size_t)max_clients) / (1024 * 1024));
    return 0;
}

void conn_release(int fd) {
    if (fd < 0 || fd >= conn_pool_size) return;

    struct conn *c = &conn_pool[fd];

    if (c->client)
        memset(c->client, 0, sizeof(client_t));  /* pre-allocated, reuse */

    if (c->active) {
        atomic_fetch_sub_explicit(&g_kvs_connected_clients, 1,
                                   memory_order_relaxed);
    }

    if (proactor_debug_enabled())
        fprintf(stderr, "[proactor] release fd=%d gen=%u\n", fd, c->gen);

    /* Save pre-allocated buffers before memset zeroes them */
    char *rb = c->read_buf.buf;
    char *wb = c->write_buf.buf;
    int   rcap = c->read_buf.cap;
    int   wcap = c->write_buf.cap;

    memset(c, 0, sizeof(*c));
    c->read_buf.buf = rb;
    c->read_buf.cap = rcap;
    c->write_buf.buf = wb;
    c->write_buf.cap = wcap;
    close(fd);
}

void conn_pool_destroy(void) {
    if (!conn_pool) return;

    for (int i = 0; i < conn_pool_size; i++) {
        if (conn_pool[i].active)
            conn_release(i);
        dynbuf_free(&conn_pool[i].read_buf);
        dynbuf_free(&conn_pool[i].write_buf);
        free(conn_pool[i].client);
    }
    free(conn_pool);
    conn_pool = NULL;
    conn_pool_size = 0;

    printf("[conn] pool destroyed\n");
}

/* ═══════════════════════════════════════════════════════════════════════
 *  Part 2. Listen socket + io_uring + buffer ring
 * ═══════════════════════════════════════════════════════════════════════ */

static int init_listen_socket(unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (sockfd < 0) return -1;

    struct sockaddr_in serveraddr;
    memset(&serveraddr, 0, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
    serveraddr.sin_port = htons(port);

    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    if (bind(sockfd, (struct sockaddr*)&serveraddr, sizeof(serveraddr)) == -1) {
        perror("bind");
        close(sockfd);
        return -1;
    }
    listen(sockfd, SOMAXCONN);
    return sockfd;
}

static int setup_io_uring(proactor_worker_t *w) {
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    /* SQPOLL: kernel thread polls SQ continuously, eliminating
     * io_uring_submit syscalls. Benchmarks run with sudo. */
    params.flags = IORING_SETUP_SQPOLL | IORING_SETUP_SINGLE_ISSUER;
    params.sq_thread_idle = 200;  /* keep polling 200ms after last submit */
    int qret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);

    if (qret == -EINVAL) {
        memset(&params, 0, sizeof(params));
        params.flags = IORING_SETUP_SINGLE_ISSUER
                     | IORING_SETUP_DEFER_TASKRUN
                     | IORING_SETUP_COOP_TASKRUN;
        qret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);
    }
    if (qret == -EINVAL) {
        memset(&params, 0, sizeof(params));
        params.flags = 0;
        qret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);
    }
    if (qret < 0) {
        fprintf(stderr, "[proactor] io_uring init: %s\n", strerror(-qret));
        return -1;
    }

    printf("[proactor] io_uring ready (flags=0x%x%s)\n", params.flags,
           (params.flags & IORING_SETUP_SQPOLL) ? " SQPOLL" : "");
    return 0;
}

static int setup_buffer_ring(proactor_worker_t *w) {
    w->buf_pool_bytes = (size_t)KVS_NR_BUFS * KVS_BUF_SIZE;
    w->buf_base = mmap(NULL, w->buf_pool_bytes, PROT_READ | PROT_WRITE,
                       MAP_ANON | MAP_PRIVATE, -1, 0);
    if (w->buf_base == MAP_FAILED) {
        perror("[proactor] mmap buf pool");
        return -1;
    }

    int br_err = 0;
    w->buf_ring = io_uring_setup_buf_ring(&w->ring, KVS_NR_BUFS,
                                          KVS_BUF_RING_BGID, 0, &br_err);
    if (!w->buf_ring) {
        fprintf(stderr, "[proactor] setup_buf_ring: %s\n", strerror(-br_err));
        munmap(w->buf_base, w->buf_pool_bytes);
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

    printf("[proactor] buf ring: %d x %d B = %zu MB\n",
           KVS_NR_BUFS, KVS_BUF_SIZE, w->buf_pool_bytes / (1024 * 1024));
    return 0;
}

int worker_init(proactor_worker_t *w, unsigned short port) {
    memset(w, 0, sizeof(*w));
    w->listen_fd = -1;

    w->listen_fd = init_listen_socket(port);
    if (w->listen_fd < 0) {
        fprintf(stderr, "[proactor] failed to init listen socket\n");
        return -1;
    }

    if (setup_io_uring(w) < 0) {
        close(w->listen_fd);
        return -1;
    }

    if (setup_buffer_ring(w) < 0) {
        io_uring_queue_exit(&w->ring);
        close(w->listen_fd);
        return -1;
    }

    return 0;
}

void worker_destroy(proactor_worker_t *w) {
    if (w->buf_ring) {
        io_uring_free_buf_ring(&w->ring, w->buf_ring, KVS_NR_BUFS, KVS_BUF_RING_BGID);
        w->buf_ring = NULL;
    }
    if (w->buf_base && w->buf_base != MAP_FAILED) {
        munmap(w->buf_base, w->buf_pool_bytes);
        w->buf_base = NULL;
    }
    io_uring_queue_exit(&w->ring);
    if (w->listen_fd >= 0) {
        close(w->listen_fd);
        w->listen_fd = -1;
    }
}

/* ═══════════════════════════════════════════════════════════════════════
 *  Part 3. Event loop — Redis-style delayed submission for batching
 * ═══════════════════════════════════════════════════════════════════════ */

/* Forward declarations (defined in proactor_event.c) */
extern int worker_arm_accept(proactor_worker_t *w);

static void proactor_loop(proactor_worker_t *w) {
    struct io_uring *ring = &w->ring;

    if (worker_arm_accept(w) != 0) {
        fprintf(stderr, "[proactor] failed to arm accept\n");
        return;
    }

    printf("[proactor] event loop started (Redis-style delayed submit)\n");

    while (!g_kvs_stop) {
        update_cached_time();

        /* Submit pending SQEs from last iteration (only if any) */
        if (w->pending_sqes > 0) {
            int submitted = io_uring_submit(ring);
            if (submitted < 0 && submitted != -EBUSY)
                fprintf(stderr, "[proactor] submit: %s\n", strerror(-submitted));
            w->pending_sqes = 0;
        }

        /* Batch peek CQEs */
        struct io_uring_cqe *cqes[CQE_BATCH_SIZE];
        int nready = io_uring_peek_batch_cqe(ring, cqes, CQE_BATCH_SIZE);

        if (nready == 0) {
            /* Dynamic timeout: 100μs idle poll (vs Redis 100μs epoll).
             * Adds a timeout SQE so submit_and_wait returns quickly. */
            struct io_uring_sqe *sqe = get_sqe_submit(ring);
            if (sqe) {
                struct __kernel_timespec ts = { .tv_sec = 0, .tv_nsec = 100000L };
                io_uring_prep_timeout(sqe, &ts, 0, 0);
                sqe->user_data = conn_info_pack(0, 0, 0);
                w->pending_sqes = 1;
            }

            int ret = io_uring_submit_and_wait(ring, 1);
            if (ret < 0) {
                if (ret == -EINTR) continue;
                fprintf(stderr, "[proactor] submit_and_wait: %s\n", strerror(-ret));
                break;
            }
            w->pending_sqes = 0;
            nready = io_uring_peek_batch_cqe(ring, cqes, CQE_BATCH_SIZE);
            if (nready <= 0) continue;
        }

        /* Process CQEs (prepare SQEs for next iteration) */
        for (int i = 0; i < nready; i++) {
            w->pending_sqes++;  /* each handler may produce SQEs */
            event_handle_cqe(w, cqes[i]);
        }

        if (worker_arm_accept(w) != 0)
            fprintf(stderr, "[proactor] failed to re-arm accept\n");

        io_uring_cq_advance(ring, nready);
    }

    io_uring_submit(ring);
    fprintf(stderr, "[proactor] exit stop=%d\n", (int)g_kvs_stop);
}

/* ═══════════════════════════════════════════════════════════════════════
 *  Part 4. Entry point
 * ═══════════════════════════════════════════════════════════════════════ */

int proactor_start(unsigned short port, msg_handler handler) {
    (void)handler;

    if (conn_pool_init(kvs_config.maxclients) < 0) {
        fprintf(stderr, "[proactor] failed to init conn pool\n");
        return -1;
    }

    g_worker = calloc(1, sizeof(proactor_worker_t));
    if (!g_worker) {
        conn_pool_destroy();
        return -1;
    }

    if (worker_init(g_worker, port) < 0) {
        free(g_worker);
        conn_pool_destroy();
        return -1;
    }

    printf("[proactor] single-threaded mode, single global dict\n");

    proactor_loop(g_worker);

    worker_destroy(g_worker);
    free(g_worker);
    g_worker = NULL;

    conn_pool_destroy();
    return 0;
}
