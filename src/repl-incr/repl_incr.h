#ifndef __REPL_INCR_H__
#define __REPL_INCR_H__

#include <stddef.h>
#include "dynbuf.h"
#include "net_common.h"    /* conn_kind_t */

/* =========================================================================
 * Master-Slave Incremental Replication
 *
 *  Backlog: 1 MB circular buffer, stores raw RESP write commands.
 *  Protocol: simplified PSYNC (offset only, no replid).
 *  Transport: dedicated TCP port (default 2001), independent of full sync.
 *
 *  fd ownership: replication fds are handed to the network engine and
 *  dispatched by it (via conn->kind + the callbacks below).  repl_incr
 *  keeps only protocol/state-machine logic; it no longer self-polls.
 * ========================================================================= */

#define REPL_BACKLOG_SIZE  (1024 * 1024)   /* 1 MB */

/* ── Replication backlog ────────────────────────────────────────────── */

typedef struct {
    char   buf[REPL_BACKLOG_SIZE];
    size_t head;            /* absolute write offset (monotonic, never resets) */
    size_t used;            /* bytes currently stored (<= REPL_BACKLOG_SIZE) */
} repl_backlog_t;

void   repl_backlog_init(void);
void   repl_backlog_append(const char *resp_data, int len);

/* Copy as many complete RESP commands as fit within max_len bytes from
 * start_offset.  Returns bytes written to dst (>=0) and sets *out_end_offset
 * to the byte offset immediately after the last copied command.
 * Returns -1 if start_offset is no longer in the backlog. */
int    repl_backlog_fetch(size_t start_offset, char *dst, int max_len,
                           size_t *out_end_offset);

size_t repl_backlog_offset(void);         /* current head (absolute) */
int    repl_backlog_valid(size_t offset); /* offset within backlog range? */

/* ── Slave connection (Master side) ──────────────────────────────────── */

typedef enum {
    REPL_STATE_NONE,        /* slot unused */
    REPL_STATE_CONNECTED,   /* just accepted, waiting for PSYNC */
    REPL_STATE_STREAMING    /* +CONTINUE sent, pushing command stream */
} repl_slave_state_t;

#define REPL_MAX_SLAVES  8

/* Dedicated replication state only — the fd and its read/write buffers
 * live in conn_t, owned by the network engine. */
typedef struct {
    int                fd;         /* association key into the engine's conn table */
    int                active;
    repl_slave_state_t state;
    DynBuf             write_buf;  /* pending bytes to push to this slave */
    size_t             repl_offset;/* PSYNC offset this slave requested */
} repl_slave_t;

/* ── Global repl_offset for fullsync bridge ────────────────────────────
 *
 * The fullsync protocol carries repl_offset in batch_hdr.repl_offset.
 * Slave stores it here after loading an RDB snapshot so the incremental
 * module knows where to resume.                                  */
extern size_t g_repl_snapshot_offset;   /* 0 = no fullsync loaded yet */

/* ── Engine integration hooks ─────────────────────────────────────────
 *
 * Set by the active network engine before repl_incr_attach() is called.
 * NULL means "no engine attached" — repl falls back to self-managed I/O
 * (the legacy poll path used by proactor until it adopts the same model). */
extern int  (*repl_engine_register)(int fd, conn_kind_t kind);
extern void (*repl_engine_writable)(int fd, int on);   /* arm/disarm EPOLLOUT */
extern void (*repl_engine_close)(int fd);              /* release the conn slot */

/* ── Setup (called before the engine loop starts) ────────────────────── */

int  repl_incr_master_start(int port);                    /* create listen socket */
int  repl_incr_slave_start(const char *master_ip, int master_port);

/* Register repl fds into the engine's event loop.  Called once by the
 * engine after its epoll/io_uring is ready. */
void repl_incr_attach(void);

/* ── Master-side event callbacks (fd already registered) ─────────────── */

int  repl_incr_master_accept(int listen_fd);   /* accept all pending slaves */
int  repl_incr_master_read(int fd);            /* PSYNC handshake / detect drop */
int  repl_incr_master_write(int fd);           /* flush write_buf */
void repl_incr_master_close(int fd);           /* teardown a slave link */

/* Poll mask the engine should wait on for a slave fd (proactor readiness
 * query): POLLIN while awaiting PSYNC, POLLOUT while streaming data is
 * pending, 0 when idle-and-streaming.  The engine re-arms its readiness
 * notification whenever this mask changes. */
int  repl_incr_master_poll_mask(int fd);

/* ── Slave-side event callbacks ──────────────────────────────────────── */

int  repl_incr_slave_read(int fd);             /* read command stream, replay */
void repl_incr_slave_close(int fd);            /* teardown the master link */

/* ── Propagate (hot path, master only) ───────────────────────────────── */

void repl_incr_propagate(const char *resp_data, int len);

/* ── fd getters (used by repl_incr_attach) ───────────────────────────── */

int  repl_incr_master_listen_fd(void);
int  repl_incr_slave_link_fd(void);

#endif /* __REPL_INCR_H__ */
