/* stream.h — TCP stream reconstruction (hole detection, no OOO buffering)
 *
 * Replaces the old xdp_reasm module.  Single-threaded, lock-free.
 *
 * Responsibilities:
 *   - Connection tracking keyed by (saddr, sport), the client side.
 *   - Strict in-order byte-stream reconstruction driven by TCP seq numbers.
 *   - Hole detection: a gap (seq > rcv_nxt) that is not filled within
 *     HOLE_TIMEOUT_MS marks the connection corrupt (observer-side loss is
 *     unrecoverable — the client already delivered to master and will not
 *     retransmit).  No OOO linked list, no sorting, no merging.
 *   - Emits ordered byte segments via the on_data callback; the consumer
 *     (replay layer) forwards them verbatim to a slave connection.
 */

#ifndef XDP_STREAM_H
#define XDP_STREAM_H

#include <stdint.h>
#include <stddef.h>
#include "xdp_common.h"    /* struct xdp_repl_event */

typedef struct stream_conn stream_conn_t;

struct stream_conn {
    uint32_t saddr;         /* client IPv4 (network byte order) */
    uint16_t sport;         /* client TCP port (network byte order) */
    uint32_t rcv_nxt;       /* next expected seq (host byte order) */
    uint32_t isn;           /* initial seq (host byte order) */
    int      isn_known;
    int      corrupt;       /* unrecoverable gap or replay overflow */

    int      hole;          /* 1 = a gap is pending */
    uint64_t hole_deadline_ms;

    uint64_t last_active_ms;

    void    *user;          /* replay layer attachment (struct replay_conn) */

    stream_conn_t *next;    /* hash chain */
};

/* Called for each ordered byte segment.  `data` is valid only during the
 * call (it points into the ringbuf event).  The consumer must copy it.
 *
 * The consumer may set sc->corrupt = 1 to signal it can no longer accept
 * data for this connection (e.g. send-buffer overflow); the stream layer
 * will stop emitting and reap the connection on the next tick. */
typedef void (*stream_data_fn)(stream_conn_t *sc,
                               const uint8_t *data, uint32_t len);

/* Called right before the stream layer frees a connection (FIN/RST, GC,
 * or corruption).  The consumer must flush/close its per-connection state
 * and free sc->user here. */
typedef void (*stream_conn_fn)(stream_conn_t *sc);

typedef struct {
    stream_data_fn on_data;
    stream_conn_fn on_close;
} stream_ops_t;

struct stream_stats {
    uint64_t events_total;
    uint64_t events_data;
    uint64_t events_syn;
    uint64_t events_fin_rst;
    uint64_t bytes_ordered;      /* ordered bytes emitted to consumer */
    uint64_t dup_discarded;      /* duplicate bytes dropped */
    uint64_t holes;              /* gap events detected */
    uint64_t corrupt;            /* connections destroyed as corrupt */
    uint64_t fragment_events;
    uint32_t conns_active;
};

int  stream_init(const stream_ops_t *ops);
void stream_on_event(const struct xdp_repl_event *e);

/* Periodic: hole-timeout → corrupt, idle GC, reap corrupt conns. */
void stream_tick(void);

void stream_shutdown(void);
void stream_get_stats(struct stream_stats *st);

#endif /* XDP_STREAM_H */
