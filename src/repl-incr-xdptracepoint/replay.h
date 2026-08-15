/* replay.h — verbatim replay of reconstructed source streams to the slave
 *
 * Each source connection (stream_conn_t) is mapped 1:1 onto a slave TCP
 * connection.  Ordered bytes emitted by the stream layer are appended to
 * that connection's send buffer and flushed non-blockingly from the epoll
 * loop.  Slave replies (read commands the slave still executes) are drained
 * and discarded so the slave's output buffer never backs up.
 *
 * No RESP parsing, no write-command filtering — the byte stream is replayed
 * exactly as observed.  A send buffer that grows past REPLAY_BUF_MAX marks
 * the source connection corrupt (observer-side divergence is unrecoverable
 * without a later resync layer).
 */

#ifndef XDP_REPLAY_H
#define XDP_REPLAY_H

#include <stdint.h>
#include <stddef.h>

#include "stream.h"

typedef struct replay_conn replay_conn_t;

/* ── stream_ops callbacks ──────────────────────────────────────────────── */

/* Appends an ordered byte segment to the source connection's replay buffer
 * and (on first use) opens its slave connection. */
void replay_on_data(stream_conn_t *sc, const uint8_t *data, uint32_t len);

/* Called when the source connection is torn down (FIN/RST/GC/corrupt).
 * Detaches the replay connection and lets it drain + close on its own. */
void replay_on_close(stream_conn_t *sc);

/* ── Lifecycle ─────────────────────────────────────────────────────────── */

int  replay_init(int epfd);
void replay_shutdown(void);

/* ── epoll integration (single-threaded loop) ──────────────────────────── */

/* Handle one replay connection's slave-fd events. */
void replay_handle_event(replay_conn_t *rc, uint32_t events);

/* Recompute + apply epoll interest for every connection.  Call once after
 * each epoll_wait batch so new buffers/connects arm EPOLLOUT correctly. */
void replay_sync_interest(void);

/* Periodic: retry pending connects, force-drop conns that could not drain. */
void replay_tick(void);

/* ── Stats ─────────────────────────────────────────────────────────────── */

void replay_get_stats(uint64_t *active, uint64_t *sent, uint64_t *dropped);

#endif /* XDP_REPLAY_H */
