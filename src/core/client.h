#ifndef __CLIENT_H__
#define __CLIENT_H__

#include <stdint.h>

struct conn;

/* ── Per-client application state ── */
typedef struct client {
    struct conn *conn;
    /* Future: transaction queue, pubsub channels, lua state */
} client_t;

#endif
