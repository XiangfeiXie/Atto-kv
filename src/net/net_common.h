#ifndef NET_COMMON_H
#define NET_COMMON_H

#include <stdint.h>
#include "dynbuf.h"

/* ── Per-client application state ── */
struct conn;
typedef struct client {
    struct conn *conn;
} client_t;

/* ── Per-connection state (shared by reactor and proactor) ── */
typedef struct conn {
    int fd;
    DynBuf read_buf;
    DynBuf write_buf;
    int r_off, w_off;
    int active;
    int status;             /* close-after-flush */

    /* ── Proactor ── */
    int recv_armed;
    int send_armed;
    uint32_t gen;           /* stale-CQE detection */

    /* ── Reactor ── */
    uint32_t cur_events;    /* epoll cached mask */
    int (*send_callback)(int fd);
    union {
        int (*recv_callback)(int fd);
        int (*accept_callback)(int fd);
    } r_action;

    client_t *client;
} conn_t;

/* ── Network buffer defaults ── */
#define KVS_NET_BUF_INIT  65536     /* 64 KB */
#ifndef KVS_NET_BUF_MAX
#define KVS_NET_BUF_MAX   (16 * 1024 * 1024)  /* 16 MB */
#endif

#endif /* NET_COMMON_H */
