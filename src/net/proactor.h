#ifndef PROACTOR_H
#define PROACTOR_H

#include <stdint.h>
#include <stdlib.h>
#include <liburing.h>
#include "server.h"

/* Event types */
#define EVENT_ACCEPT    0
#define EVENT_READ      1
#define EVENT_WRITE     2

/* io_uring configuration */
#define ENTRIES_LENGTH  2048
#define CQE_BATCH_SIZE  128

/* Buffer ring configuration */
#define KVS_BUF_RING_BGID  0
#define KVS_NR_BUFS        8192
#define KVS_BUF_SIZE       4096

/* Proactor state (single-threaded, like Redis) */
typedef struct proactor_worker {
    int                       listen_fd;
    struct io_uring           ring;
    struct io_uring_buf_ring *buf_ring;
    char                     *buf_base;
    size_t                    buf_pool_bytes;
    int                       accept_armed;
    int                       pending_sqes;      /* batched SQE count */
} proactor_worker_t;

/* Connection info packing */
struct conn_info {
    int fd;
    int event;
    uint32_t gen;
};

/* Global state (declared here, defined in proactor.c) */
extern struct conn *conn_pool;
extern int conn_pool_size;
extern proactor_worker_t *g_worker;
extern uint32_t g_next_conn_gen;

/* ── Inline helpers ──────────────────────────────────────────────────── */

/* Pack connection info into user_data */
static inline uint64_t conn_info_pack(int fd, int event, uint32_t gen) {
    return ((uint64_t)(uint32_t)fd)
         | ((uint64_t)(uint8_t)event << 32)
         | ((uint64_t)(gen & 0xFFFFFFu) << 40);
}

/* Unpack connection info from user_data */
static inline struct conn_info conn_info_unpack(uint64_t data) {
    struct conn_info ci;
    ci.fd = (int)(uint32_t)data;
    ci.event = (int)((data >> 32) & 0xFFu);
    ci.gen = (uint32_t)((data >> 40) & 0xFFFFFFu);
    return ci;
}

/* Get next connection generation number */
static inline uint32_t next_conn_gen(void) {
    uint32_t gen;
    do {
        gen = ++g_next_conn_gen & 0xFFFFFFu;
    } while (gen == 0);
    return gen;
}

/* Get SQE with auto-submit if queue full */
static inline struct io_uring_sqe *get_sqe_submit(struct io_uring *ring) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (sqe) return sqe;
    if (io_uring_submit(ring) < 0) return NULL;
    return io_uring_get_sqe(ring);
}

/* Hand a buffer back to the kernel ring so it can be reused */
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

/* Check if debug mode is enabled */
static inline int proactor_debug_enabled(void) {
    static int g_proactor_debug = -1;
    if (g_proactor_debug < 0)
        g_proactor_debug = getenv("KVS_PROACTOR_DEBUG") ? 1 : 0;
    return g_proactor_debug;
}

/* ── Public API ──────────────────────────────────────────────────────── */

int  proactor_start(unsigned short port, msg_handler handler);
void conn_release(int fd);

#endif /* PROACTOR_H */
