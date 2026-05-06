#ifndef __CLIENT_H__
#define __CLIENT_H__

#include <stdint.h>

struct conn;

/* ── Client state ── */
typedef enum {
    CLIENT_IDLE    = 0,
    CLIENT_BLOCKED = 1,
} client_state_t;

/* ── Block reason (extensible for future types) ── */
typedef enum {
    BLOCK_NONE = 0,
    BLOCK_LIST = 1,
    /* Future: BLOCK_ZSET, BLOCK_HASH */
} block_reason_t;

/* ── Per-client application state ── */
typedef struct client {
    struct conn *conn;

    client_state_t state;
    block_reason_t block_reason;
    long long      block_until_ms;
    int            block_key_count;
    char           block_keys[4][64];

    /* Saved raw RESP command for wake-up replay */
    char  block_cmd_buf[4096];
    int   block_cmd_len;

    /* Future: transaction queue, pubsub channels, lua state */
} client_t;

#endif
