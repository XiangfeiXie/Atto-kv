/* config.h — Compile-time configuration for kvstore-incr-sync
 *
 * All tunables are #define macros. Edit and recompile to change.
 * Each macro has a sensible default via #ifndef guard.
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
#define SLAVE_PORT           2001
#endif

/* ── XDP capture ───────────────────────────────────────────────────────── */

#ifndef KVS_FILTER_PORT
#define KVS_FILTER_PORT      2000      /* TCP port XDP filters on */
#endif
#ifndef XDP_IFACE
#define XDP_IFACE            "lo"   /* network interface for XDP attach */
#endif

/* ── Connection tracking ───────────────────────────────────────────────── */

#define CONN_HASH_BITS       8         /* → 256 buckets */
#define CONN_HASH_SIZE       (1u << CONN_HASH_BITS)
#define CONN_MAX             4096      /* max concurrent connections */
#define CONN_GC_TIMEOUT      30        /* seconds idle before GC */
#define OOO_MAX_SEGMENTS     64        /* max OOO segments per connection */

/* ── OOO data buffer pool (pre-allocated, avoids hot-path malloc) ──────── */

#define OOO_DBUF_POOL_SIZE   256       /* total pre-allocated data buffers */

/* ── Per-connection circular buffer ────────────────────────────────────── */

#define CMD_BUF_CAP_MIN      4096      /* initial circular buffer size */
#define CMD_BUF_CAP_MAX      (4u * 1024 * 1024)  /* 4 MB hard cap */
#define RESP_CMD_MAX_LEN     (4 * 1024 * 1024)   /* max single RESP command */

/* ── Slave write buffer (single-thread direct send) ────────────────────── */

#define SLAVE_WB_HIGH_WATER  (4 * 1024 * 1024)   /* 4 MB — drop writes above this */
#define SLAVE_WB_LOW_WATER   (1 * 1024 * 1024)   /* 1 MB — resume below this */
#define SLAVE_RECONNECT_S    3         /* seconds between reconnect attempts */
#define SLAVE_POLL_FAST_MS   5         /* poll timeout when send backlog exists */
#define SLAVE_POLL_IDLE_MS   100       /* poll timeout when idle */

/* ── Periodic maintenance ──────────────────────────────────────────────── */

#define GC_INTERVAL_S        5         /* periodic GC interval */

#endif /* KVS_CONFIG_H */
