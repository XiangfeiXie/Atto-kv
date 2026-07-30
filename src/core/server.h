#ifndef __SERVER_H__
#define __SERVER_H__

#include <stdint.h>
#include <signal.h>
#include "dynbuf.h"

/* ── Per-client application state ── */
struct conn;
typedef struct client {
    struct conn *conn;
} client_t;

/* ── Global graceful-shutdown flag ── */
extern volatile sig_atomic_t g_kvs_stop;

/* ── Callback types ── */
typedef int (*RCALLBACK)(int fd);
typedef int (*msg_handler)(char *msg, int len, DynBuf *write_buf);

/* ── Per-connection state ── */
struct conn {
    int fd;
    DynBuf read_buf;
    DynBuf write_buf;
    int r_off, w_off;
    int active;
    int status;
    uint32_t cur_events;    /* epoll cached mask (reactor) */
    int recv_armed;
    int send_armed;
    uint32_t gen;           /* generation (proactor) */

    client_t *client;

    RCALLBACK send_callback;         /* reactor */
    union {
        RCALLBACK recv_callback;
        RCALLBACK accept_callback;
    } r_action;                      /* reactor */
};

/* Network-layer read buffer initial size */
#ifndef KVS_NET_BUF_INIT
#define KVS_NET_BUF_INIT 65536
#endif

#endif
