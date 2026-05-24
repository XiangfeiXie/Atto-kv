#ifndef __SERVER_H__
#define __SERVER_H__

#include <stdint.h>
#include <signal.h>
#include "dynbuf.h"
#include "client.h"

#define ENABLE_HTTP      0
#define ENABLE_WEBSOCKET 0
#define ENABLE_KVSTORE   1

/* Global graceful-shutdown flag; set by SIGTERM/SIGINT handler.
 * Network event loops check this flag to exit cleanly. */
extern volatile sig_atomic_t g_kvs_stop;

/* Callback types */
typedef int (*RCALLBACK)(int fd);
typedef int (*msg_handler)(char *msg, int len, DynBuf *write_buf);

/* Per-connection state (pure network I/O) */
struct conn {
    int fd;
    DynBuf read_buf;
    DynBuf write_buf;
    int r_off;              /* read buffer consumed offset (avoids memmove) */
    int w_off;              /* write buffer sent offset (avoids memmove) */
    int active;             /* slot is in use (dynamic pool) */
    uint32_t cur_events;    /* cached epoll mask (lazy MOD optimization) */
    int recv_armed;
    int send_armed;
    uint32_t gen;

    struct client *client;  /* application-layer session (NULL until first command) */

    RCALLBACK send_callback;
    union {
        RCALLBACK recv_callback;
        RCALLBACK accept_callback;
    } r_action;

    int status;

    /* Phase 4B.2 — index into the proactor worker that accepted this fd.
     * Used to deliver cross-worker wake events (BLPOP unblock / timeout)
     * back to the owning worker without violating SINGLE_ISSUER on
     * someone else's io_uring instance. */
    int owner_worker_id;
};

#if ENABLE_KVSTORE
/* Network-layer read buffer initial size for pipeline batching */
#ifndef KVS_NET_BUF_INIT
#define KVS_NET_BUF_INIT 65536
#endif
#endif

#endif
