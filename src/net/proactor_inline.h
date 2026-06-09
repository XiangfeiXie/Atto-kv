#ifndef PROACTOR_INLINE_H
#define PROACTOR_INLINE_H

#include "proactor_types.h"
#include <liburing.h>
#include <stdlib.h>

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
        gen = atomic_fetch_add_explicit(&g_next_conn_gen, 1,
                                        memory_order_relaxed) & 0xFFFFFFu;
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

#endif /* PROACTOR_INLINE_H */
