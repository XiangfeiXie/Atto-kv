#ifndef __SERVER_H__
#define __SERVER_H__

#include "dynbuf.h"

#define ENABLE_HTTP      0
#define ENABLE_WEBSOCKET 0
#define ENABLE_KVSTORE   1

/* Callback types */
typedef int (*RCALLBACK)(int fd);
typedef int (*msg_handler)(char *msg, int len, DynBuf *write_buf);

/* Per-connection state */
struct conn {
    int fd;
    DynBuf read_buf;
    DynBuf write_buf;
    int r_off;         /* read buffer consumed offset (avoids memmove) */
    int w_off;         /* write buffer sent offset (avoids memmove) */

    RCALLBACK send_callback;
    union {
        RCALLBACK recv_callback;
        RCALLBACK accept_callback;
    } r_action;

    int status;
};

#if ENABLE_KVSTORE
/* Network-layer read buffer initial size for pipeline batching */
#ifndef KVS_NET_BUF_INIT
#define KVS_NET_BUF_INIT 65536
#endif
#endif

#endif
