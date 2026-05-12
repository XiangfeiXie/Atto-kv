#ifndef EBPF_INCR_REPL_H
#define EBPF_INCR_REPL_H

#include <stdint.h>
#include <pthread.h>
#include <stdatomic.h>

/* Maximum RESP command size captured by BPF.
 * Must match the definition in ebpf_incr.bpf.c.                    */
#define INCR_MAX_CMD   262144   /* 256 KB */

/* Wire frame layout (over TCP):
 *   [ uint64_t cmd_len ][ cmd_len bytes of RESP command ]
 * A cmd_len of 0 is the graceful-disconnect sentinel.              */
#define INCR_SEND_BUF  ((int)(sizeof(uint64_t) + INCR_MAX_CMD))
#define INCR_RECV_BUF  INCR_SEND_BUF

/* Maximum number of simultaneously connected slaves */
#define INCR_MAX_SLAVES 8

/* Backlog capacity — sized to cover fullsync duration.
 * At 1000 QPS with 10-second fullsync, ~10000 entries needed.
 * 16384 gives ~16 seconds of headroom at 1000 QPS.                */
#define INCR_BACKLOG_CAP 16384

/* ================================================================
 * Ring buffer event (must match BPF side struct incr_event)
 *
 * Fixed-size struct: 16-byte header + INCR_MAX_CMD payload inline.
 * BPF uses this for constant-size ringbuf reservation (kernel 6.8
 * verifier requires compile-time-constant size).
 * ================================================================ */

typedef struct {
    uint64_t seq;
    uint32_t length;
    uint32_t _pad;         /* explicit padding to 16 bytes */
    char     msg[INCR_MAX_CMD];
} incr_event;

/* ================================================================
 * Pre-allocated backlog slot (replaces malloc-based model)
 * ================================================================ */
typedef struct {
    uint64_t seq;
    uint32_t length;
    char    *msg;          /* pre-allocated at startup, INCR_MAX_CMD */
    int      msg_cap;
    int      valid;
} backlog_slot_t;

/* ================================================================
 * Per-slave lock-free SPSC queue
 *
 * Pattern: AOF ring buffer from persistence.c (head/tail atomics)
 * Depth 4096 provides ~40x headroom for TCP stalls at 100K QPS.
 * Slots use lazy buffer allocation (4 KB initial, grow on demand).
 * ================================================================ */
#define SLAVE_SPSC_SIZE     4096
#define SLAVE_SPSC_MASK     (SLAVE_SPSC_SIZE - 1)
#define SPSC_SLOT_INIT_CAP  4096

typedef struct {
    uint64_t  seq;
    uint32_t  length;
    char     *msg;          /* lazily allocated, grows as needed */
    int       msg_cap;
} spsc_slot_t;

typedef struct {
    spsc_slot_t      slots[SLAVE_SPSC_SIZE];
    _Atomic uint32_t head;           /* producer writes (monotonic) */
    _Atomic uint32_t tail;           /* consumer writes (monotonic) */
    int              evt_fd;         /* eventfd for consumer wakeup  */
    int              fd;             /* TCP socket to slave           */
    int              active;         /* slot in use                   */
    pthread_t        send_tid;       /* per-slave send thread         */
    volatile int     running;        /* send thread exit signal       */
} slave_spsc_t;

/* ================================================================
 * Phase 2: kernel-level multicast fanout (future)
 *
 * When BPF gains multicast capability, set g_fanout_fn to bypass
 * the userspace SPSC path and push directly from kernel context.
 * ================================================================ */
typedef int (*incr_fanout_fn)(void *ctx, uint64_t seq,
                              const char *msg, uint32_t len);

/* --- Fullsync state (set by RDMA master thread, read by incr thread) --- */
#define FULLSYNC_IDLE       0   /* normal streaming                    */
#define FULLSYNC_INPROGRESS 1   /* fullsync RDMA in flight, backlog only */
#define FULLSYNC_CATCHUP    2   /* fullsync done, replaying backlog    */

/* --- Master-side API --- */

/* Load BPF program, attach uprobe, start consumer thread AND a TCP
 * incr listener on kvs_config.incr_port.
 * Returns 0 on success, -1 on failure.                          */
int  ebpf_incr_master_start(void);

/* Register a connected TCP slave for incremental delivery.
 * start_seq is inclusive: replays backlog records with
 * record.seq >= start_seq before entering live streaming.
 * Ownership of tcp_fd transfers to this module on success.
 * Returns 0 on success, -1 on failure.                           */
int  ebpf_incr_add_slave(int tcp_fd, uint64_t start_seq);

/* Returns 1 when incremental stream is healthy, 0 otherwise.     */
int  ebpf_incr_master_healthy(void);

/* Gracefully disconnect all slaves, stop TCP listener and unload
 * BPF program.                                                    */
void ebpf_incr_stop(void);

/* --- Fullsync coordination (called from RDMA master thread) --- */

/* Read current eBPF global_seq value. Used to determine the
 * fullsync cut point before fork().                              */
uint64_t ebpf_incr_read_global_seq(void);

/* Begin fullsync: record base_seq, set status=FULLSYNC_INPROGRESS.
 * Consumer thread stops direct-sending and only appends to backlog.
 * Returns the base_seq that the fullsync snapshot covers.         */
uint64_t ebpf_incr_fullsync_begin(void);

/* Signal that the fullsync RDMA send has completed.
 * Transitions status from INPROGRESS to CATCHUP.
 * Consumer thread continues to only append to backlog.
 * When the slave later connects via TCP incr with INCR_SYNC,
 * backlog replay from base_seq serves as the catchup.            */
void ebpf_incr_fullsync_send_done(void);

/* Signal that the catching-up slave has received all backlog
 * entries >= base_seq. Transitions status from CATCHUP to IDLE.
 * Consumer resumes direct-sending to all slaves.                  */
void ebpf_incr_fullsync_finish(void);

/* Query current fullsync status (FULLSYNC_IDLE / INPROGRESS / CATCHUP) */
int  ebpf_incr_fullsync_status(void);

/* Get the base_seq of the current/last fullsync */
uint64_t ebpf_incr_fullsync_base_seq(void);

/* Override base_seq after fork() — the post-fork read is the authoritative
 * cut point because the COW snapshot is taken at fork(). */
void ebpf_incr_fullsync_set_base_seq(uint64_t seq);

/* Register a kernel-level fanout function (Phase 2 readiness).
 * When set, on_incr_event calls this instead of SPSC push. */
void ebpf_incr_set_fanout(incr_fanout_fn fn);

/* TCP incr handshake: slave sends this after fullsync to request
 * catchup from a specific base_seq.                               */
#define INCR_SYNC_CMD  "INCR_SYNC"

#endif /* EBPF_INCR_REPL_H */
