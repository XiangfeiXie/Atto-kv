#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <liburing.h>
#include "proactor_types.h"
#include "proactor_inline.h"
#include "server.h"
#include "config.h"
#include "kvstore.h"

/* Global variables */
struct conn *conn_pool = NULL;
int conn_pool_size = 0;
proactor_worker_t *g_worker = NULL;
uint32_t g_next_conn_gen = 1;

/* External function declarations */
extern int worker_init(proactor_worker_t *w, unsigned short port);
extern void worker_destroy(proactor_worker_t *w);
extern void event_handle_cqe(proactor_worker_t *w, struct io_uring_cqe *cqe);
extern int conn_pool_init(int max_clients);
extern void conn_pool_destroy(void);
extern int worker_arm_accept(proactor_worker_t *w);
extern int conn_arm_recv(struct io_uring *ring, struct conn *c);
extern int conn_arm_send(struct io_uring *ring, struct conn *c);
extern void conn_release(int fd);

/* Main event loop - Redis-style delayed submission for batching */
static void proactor_loop(proactor_worker_t *w) {
    struct io_uring *ring = &w->ring;

    if (worker_arm_accept(w) != 0) {
        fprintf(stderr, "[proactor] failed to arm accept\n");
        return;
    }

    printf("[proactor] event loop started (Redis-style delayed submit)\n");

    while (!g_kvs_stop) {
        update_cached_time();

        /* [Redis-style] Submit all pending SQEs from last iteration first */
        int submitted = io_uring_submit(ring);
        if (submitted < 0 && submitted != -EBUSY) {
            fprintf(stderr, "[proactor] submit failed: %s\n",
                    strerror(-submitted));
        }

        /* Batch peek CQEs */
        struct io_uring_cqe *cqes[CQE_BATCH_SIZE];
        int nready = io_uring_peek_batch_cqe(ring, cqes, CQE_BATCH_SIZE);

        if (nready == 0) {
            int to_ms = 1;  /* 1ms idle poll for low latency */

            struct io_uring_sqe *sqe = get_sqe_submit(ring);
            if (sqe) {
                struct __kernel_timespec ts;
                ts.tv_sec = to_ms / 1000;
                ts.tv_nsec = (to_ms % 1000) * 1000000L;
                io_uring_prep_timeout(sqe, &ts, 0, 0);
                sqe->user_data = conn_info_pack(0, 0, 0);
            }

            int ret = io_uring_submit_and_wait(ring, 1);
            if (ret < 0) {
                if (ret == -EINTR) continue;
                fprintf(stderr, "[proactor] submit_and_wait: %s\n",
                        strerror(-ret));
                break;
            }
            nready = io_uring_peek_batch_cqe(ring, cqes, CQE_BATCH_SIZE);
            if (nready <= 0) continue;
        }

        /* Process CQEs (only prepare SQEs, don't submit yet) */
        for (int i = 0; i < nready; i++) {
            event_handle_cqe(w, cqes[i]);
        }

        /* Re-arm accept if needed */
        if (worker_arm_accept(w) != 0)
            fprintf(stderr, "[proactor] failed to re-arm accept\n");

        /* Advance CQ (submission happens at next loop iteration) */
        io_uring_cq_advance(ring, nready);
    }

    /* Final submit before exit */
    io_uring_submit(ring);

    fprintf(stderr, "[proactor] exit stop=%d\n", (int)g_kvs_stop);
}

/* Start proactor */
int proactor_start(unsigned short port, msg_handler handler) {
    (void)handler;

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

    if (worker_init(g_worker, port) < 0) {
        free(g_worker);
        conn_pool_destroy();
        return -1;
    }

    printf("[proactor] single-threaded mode, single global dict\n");

    /* Run event loop on main thread */
    proactor_loop(g_worker);

    /* Teardown */
    worker_destroy(g_worker);
    free(g_worker);
    g_worker = NULL;

    conn_pool_destroy();

    return 0;
}
