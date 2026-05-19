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

    /* Saved raw RESP command for wake-up replay.
     *
     * MT-safety contract (Phase 9 multi-key fix):
     *   - block_cmd_buf is written by the worker that handled BLPOP at
     *     register time; never re-written until the owner worker has
     *     consumed and cleared it.
     *   - On wake (LPUSH/RPUSH side), the LPUSH worker MUST NOT dispatch
     *     this buffer directly: doing so writes conn->write_buf from a
     *     non-owner worker, racing with the owner's send-completion path.
     *     Instead, the LPUSH worker only calls block_notify_woken(),
     *     which routes the wake through the owner worker's eventfd
     *     queue.  The owner worker drains the queue in EVENT_WAKE,
     *     re-dispatches block_cmd_buf inside its own ring, and posts
     *     send SQE on its own ring.
     *   - block_pending_timeout: same hazard for the timeout-reply
     *     path.  block_check_timeouts (worker 0) sets this flag instead
     *     of writing "$-1\r\n" directly; owner worker emits the reply
     *     when it processes the wake. */
    char  block_cmd_buf[4096];
    int   block_cmd_len;
    int   block_pending_timeout;   /* 1 = owner must emit "$-1\r\n" on wake */

    /* Future: transaction queue, pubsub channels, lua state */
} client_t;

#endif
