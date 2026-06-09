#ifndef PROACTOR_TYPES_H
#define PROACTOR_TYPES_H

#include <stdint.h>
#include <stdatomic.h>
#include <liburing.h>
#include "server.h"

/* Event types */
#define EVENT_ACCEPT    0
#define EVENT_READ      1
#define EVENT_WRITE     2
#define EVENT_WAKE      3

/* io_uring configuration */
#define ENTRIES_LENGTH  8192
#define CQE_BATCH_SIZE  512

/* Buffer ring configuration */
#define KVS_BUF_RING_BGID  1
#define KVS_NR_BUFS        8192
#define KVS_BUF_SIZE       4096

/* Proactor worker structure */
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

/* Connection info packing */
struct conn_info {
    int fd;
    int event;
    uint32_t gen;
};

/* Global state (declared here, defined in proactor_main.c) */
extern struct conn *conn_pool;
extern int conn_pool_size;
extern proactor_worker_t *g_worker;
extern atomic_uint g_next_conn_gen;

#endif /* PROACTOR_TYPES_H */
