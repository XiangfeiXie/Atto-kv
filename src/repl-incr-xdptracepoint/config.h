/* config.h — Compile-time configuration for kvstore-incr-sync
 *
 * All tunables are #define macros. Edit and recompile to change.
 * Each macro has a sensible default via #ifndef guard.
 */

#ifndef KVS_CONFIG_H
#define KVS_CONFIG_H

/* ── Slave connection ────────────────────────────────────────────────── */

#ifndef SLAVE_IP
#define SLAVE_IP            "127.0.0.1"
#endif
#ifndef SLAVE_PORT
#define SLAVE_PORT           6379
#endif

/* ── XDP capture ─────────────────────────────────────────────────────── */

#ifndef KVS_FILTER_PORT
#define KVS_FILTER_PORT      6379      /* TCP port XDP filters on */
#endif
#ifndef XDP_IFACE
#define XDP_IFACE            "lo"      /* network interface for XDP attach */
#endif

/* ── Connection tracking ─────────────────────────────────────────────── */

#define CONN_HASH_BITS       8         /* → 256 buckets */
#define CONN_HASH_SIZE       (1u << CONN_HASH_BITS)
#define CONN_MAX             4096      /* max concurrent connections */
#define CONN_GC_TIMEOUT      30        /* seconds idle before GC */
#define OOO_MAX_SEGMENTS     64        /* max OOO segments per connection */

/* ── Per-connection circular buffer ──────────────────────────────────── */

#define CMD_BUF_CAP_MIN      4096      /* initial circular buffer size */
#define CMD_BUF_CAP_MAX      (4u * 1024 * 1024)  /* 4 MB hard cap */
#define RESP_CMD_MAX_LEN     (4 * 1024 * 1024)   /* max single RESP command */

/* ── Write-command queue (to writer thread) ──────────────────────────── */

#define CMD_QUEUE_SIZE       16384     /* max pending commands in queue */

/* ── Writer thread ───────────────────────────────────────────────────── */

#define WRITER_BATCH_SIZE    64        /* commands per pipeline drain */
#define WRITER_RECONNECT_S   3         /* seconds between reconnect attempts */
#define WRITER_POP_TIMEOUT_MS 1000    /* queue pop timeout (checks reconnect) */

/* ── Ringbuf poll ────────────────────────────────────────────────────── */

#define RINGBUF_POLL_MS      100       /* ring_buffer__poll timeout */
#define GC_INTERVAL_S        5         /* periodic GC interval */

#endif /* KVS_CONFIG_H */
