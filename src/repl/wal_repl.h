/* wal_repl.h — WAL-based incremental replication (replaces ebpf_incr_repl.h) */
#ifndef WAL_REPL_H
#define WAL_REPL_H

#include <stdint.h>
#include <pthread.h>
#include <stdatomic.h>

/* ── WAL on-disk record header (20 bytes, packed) ── */
#define WAL_MAGIC  0x574C5243U   /* "WLRC" */
typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t crc32;    /* crc32 of seq+length+payload */
    uint64_t seq;      /* monotonic sequence number   */
    uint32_t length;   /* payload length in bytes     */
} wal_record_hdr_t;

#define WAL_HDR_SIZE  ((int)sizeof(wal_record_hdr_t))   /* 20 bytes */

/* ── Limits ── */
#define INCR_MAX_CMD       262144   /* max single command bytes */
#define INCR_MAX_SLAVES    8

/* ── In-memory backlog for fast catchup ── */
#define INCR_BACKLOG_CAP   16384
#define INCR_BACKLOG_MASK  (INCR_BACKLOG_CAP - 1)

typedef struct {
    uint64_t  seq;
    uint32_t  length;
    char     *msg;
    int       msg_cap;
} backlog_slot_t;

/* ── Fullsync states ── */
#define FULLSYNC_IDLE        0
#define FULLSYNC_INPROGRESS  1
#define FULLSYNC_CATCHUP     2

/* ── Replication ID ── */
#define INCR_REPL_ID_LEN  40

/* ── Handshake tokens (wire protocol unchanged) ── */
#define INCR_SYNC_CMD    "INCR_SYNC"
#define INCR_CONTINUE    "+CONTINUE"
#define INCR_FULLRESYNC  "-FULLRESYNC"

/* ── Fanout callback (Phase 2 hook, backward compat) ── */
typedef int (*incr_fanout_fn)(void *ctx, uint64_t seq,
                              const char *msg, uint32_t len);

/* ── Public API (same names as ebpf_incr_repl.h for zero-diff on callers) ── */
int          ebpf_incr_master_start(void);
int          ebpf_incr_add_slave(int tcp_fd, uint64_t start_seq);
int          ebpf_incr_master_healthy(void);
void         ebpf_incr_stop(void);
uint64_t     ebpf_incr_read_global_seq(void);
uint64_t     ebpf_incr_fullsync_begin(void);
void         ebpf_incr_fullsync_send_done(void);
void         ebpf_incr_fullsync_finish(void);
int          ebpf_incr_fullsync_status(void);
uint64_t     ebpf_incr_fullsync_base_seq(void);
void         ebpf_incr_fullsync_set_base_seq(uint64_t seq);
void         ebpf_incr_set_fanout(incr_fanout_fn fn);
const char  *ebpf_incr_get_replid(void);

/* ── Returns 1 if the WAL module is running (for aof_async_init) ── */
int  ebpf_incr_is_started(void);

/* ── New entry point called by kvs_rdma_incr_probe ── */
void wal_repl_append(const char *msg, int length);

#endif /* WAL_REPL_H */
