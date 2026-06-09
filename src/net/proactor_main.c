#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <liburing.h>
#include "proactor_types.h"
#include "proactor_inline.h"
#include "server.h"
#include "config.h"
#include "kvstore.h"
#include "block.h"
#include "protocol.h"
#include "shared.h"

/* Global variables */
struct conn *conn_pool = NULL;
int conn_pool_size = 0;
proactor_worker_t *g_worker = NULL;
atomic_uint g_next_conn_gen = 1;

/* External function declarations */
extern int worker_init(proactor_worker_t *w, unsigned short port, int worker_id);
extern void worker_destroy(proactor_worker_t *w);
extern void event_handle_cqe(proactor_worker_t *w, struct io_uring_cqe *cqe);
extern int conn_pool_init(int max_clients);
extern void conn_pool_destroy(void);
extern int worker_arm_accept(proactor_worker_t *w);
extern int conn_arm_recv(struct io_uring *ring, struct conn *c);
extern int conn_arm_send(struct io_uring *ring, struct conn *c);
extern void conn_release(int fd);

/* Block notify callback */
static void proactor_block_notify_local(client_t *cl) {
    if (!cl || !cl->conn || !g_worker) return;
    struct conn *conn = cl->conn;

    /* Timed-out BLPOP: emit nil reply */
    if (cl->block_pending_timeout) {
        dynbuf_cat(&conn->write_buf, "$-1\r\n", 5);
        cl->block_pending_timeout = 0;
    }

    /* Awakened BLPOP: re-dispatch saved command */
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

    /* Drain pipelined RX bytes */
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

static void proactor_block_notify(client_t *cl) {
    if (!cl || !cl->conn) return;
    proactor_block_notify_local(cl);
}

void proactor_kick_worker(int id) {
    (void)id;
}

/* Main event loop - kept compact for performance */
static void *worker_loop(void *arg) {
    proactor_worker_t *w = (proactor_worker_t *)arg;
    g_worker = w;

    struct io_uring *ring = &w->ring;
    struct io_uring_cqe *cqe;

    if (worker_arm_accept(w) != 0) {
        fprintf(stderr, "[proactor#%d] failed to arm accept\n", w->id);
        return NULL;
    }

    printf("[proactor#%d] event loop started\n", w->id);

    while (!g_kvs_stop) {
        if (w->id == 0) update_cached_time();

        /* Batch peek CQEs */
        struct io_uring_cqe *cqes[CQE_BATCH_SIZE];
        int nready = io_uring_peek_batch_cqe(ring, cqes, CQE_BATCH_SIZE);

        /* Safety check: ensure SQ has space */
        if (nready > 0) {
            int sq_space = io_uring_sq_space_left(ring);
            if (sq_space < nready * 2) {
                io_uring_submit(ring);
            }
        }

        if (nready == 0) {
            /* Check block timeouts */
            if (w->id == 0) block_check_timeouts();
            int to_ms = (w->id == 0) ? block_nearest_timeout() : -1;
            if (to_ms >= 0) {
                struct io_uring_sqe *sqe = get_sqe_submit(ring);
                if (sqe) {
                    struct __kernel_timespec ts;
                    ts.tv_sec = to_ms / 1000;
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

        /* Process CQEs */
        for (int i = 0; i < nready; i++) {
            event_handle_cqe(w, cqes[i]);
        }

        /* Re-arm accept if needed */
        if (worker_arm_accept(w) != 0)
            fprintf(stderr, "[proactor#%d] failed to re-arm accept\n", w->id);

        /* Advance and submit */
        io_uring_cq_advance(ring, nready);
        io_uring_submit(ring);
    }

    fprintf(stderr, "[proactor#%d] worker exit stop=%d\n", w->id, (int)g_kvs_stop);
    return NULL;
}

/* Start proactor */
int proactor_start(unsigned short port, msg_handler handler) {
    (void)handler;

    /* Initialize shared response objects */
    init_shared_responses();

    if (conn_pool_init(kvs_config.maxclients) < 0) {
        fprintf(stderr, "[proactor] failed to init conn pool\n");
        return -1;
    }

    printf("[proactor] conn pool: %d slots (%zu MB)\n",
           conn_pool_size,
           (sizeof(struct conn) * (size_t)conn_pool_size) / (1024 * 1024));

    g_worker = calloc(1, sizeof(proactor_worker_t));
    if (!g_worker) {
        conn_pool_destroy();
        return -1;
    }

    if (worker_init(g_worker, port, 0) < 0) {
        free(g_worker);
        conn_pool_destroy();
        return -1;
    }

    printf("[proactor] single-threaded mode, single global dict\n");

    block_set_notify_cb(proactor_block_notify);

    /* Run worker on main thread */
    worker_loop(g_worker);

    /* Teardown */
    worker_destroy(g_worker);
    free(g_worker);
    g_worker = NULL;

    conn_pool_destroy();

    return 0;
}
