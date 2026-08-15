/* config.h — Compile-time configuration for kvstore-incr-sync
 *
 * All tunables are #define macros. Edit and recompile to change.
 * Each macro has a sensible default via #ifndef guard.
 *
 * Design note: this component is a pure-bypass "verbatim replay" daemon.
 * It does NOT parse RESP — it reconstructs the client→master byte stream
 * (per connection) and replays it verbatim to a 1:1 slave connection.
 * Reliability gaps (observer-side loss) are detected via hole timeouts and
 * left to a later resync layer (out of scope for now).
 */

#ifndef KVS_CONFIG_H
#define KVS_CONFIG_H

/* ── Compiler hints ────────────────────────────────────────────────────── */

#ifndef likely
#define likely(x)   __builtin_expect(!!(x), 1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect(!!(x), 0)
#endif

/* ── Slave connection ──────────────────────────────────────────────────── */

#ifndef SLAVE_IP
#define SLAVE_IP            "127.0.0.1"
#endif
#ifndef SLAVE_PORT
#define SLAVE_PORT           2001      /* slave kvstore's main `port` (NOT its incr_repl_port) */
#endif

/* ── XDP capture ───────────────────────────────────────────────────────── */

#ifndef KVS_FILTER_PORT
#define KVS_FILTER_PORT      2000      /* TCP port XDP filters on */
#endif
#ifndef XDP_IFACE
#define XDP_IFACE            "lo"     /* network interface for XDP attach */
#endif

/* ── Connection tracking ───────────────────────────────────────────────── */

#define CONN_HASH_BITS       8         /* → 256 buckets */
#define CONN_HASH_SIZE       (1u << CONN_HASH_BITS)
#define CONN_MAX             4096      /* max concurrent source connections */
#define CONN_GC_TIMEOUT_MS   30000     /* idle connection GC (ms) */

/* ── Stream reconstruction (hole detection) ──────────────────────────────
 *
 * A gap (seq > rcv_nxt) is recorded with a deadline.  If the hole is not
 * filled within this window (i.e. the client won't retransmit because the
 * segment actually reached master), the connection is declared corrupt.  */

#define HOLE_TIMEOUT_MS      2000

/* ── Replay (per-connection slave send buffer) ─────────────────────────── */

#define REPLAY_BUF_INIT      4096
#define REPLAY_BUF_MAX       (16 * 1024 * 1024)   /* hard cap → corrupt conn */
#define REPLAY_RECONNECT_MS  1000                 /* slave connect retry backoff */
#define REPLAY_DRAIN_MS      5000                 /* max linger to flush after FIN */

/* ── Event loop ────────────────────────────────────────────────────────── */

#define REPLAY_EPOLL_MAXEVENTS 64                 /* epoll_wait batch size */

/* ── Periodic maintenance ──────────────────────────────────────────────── */

#define STATS_INTERVAL_S     10
#define TIMER_TICK_MS        100

#endif /* KVS_CONFIG_H */
