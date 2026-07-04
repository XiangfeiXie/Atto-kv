/* repl_tcp.h — TCP-based replication transport layer with incremental recovery
 *
 * Architecture:
 *   eBPF filter → ringbuf → backlog → TCP socket → network
 *   TCP (6483): Control plane + incremental sync
 *   RDMA (2101): Data plane (fullsync child process only)
 *
 * Features:
 *   - Smart filtering: only write commands captured (60-80% reduction)
 *   - TCP reliability: built-in ordering, retransmission, flow control
 *   - Incremental recovery: backlog buffer for partial resync on reconnect
 *   - RDMA fullsync: fork child process for zero-copy data transfer
 *   - Simple deployment: no special kernel features required
 *   - Integrated into kvstore main process (no separate daemon)
 *
 * Replaces: KCP + AF_XDP with standard TCP sockets
 */
#ifndef REPL_TCP_H
#define REPL_TCP_H

#include <stdint.h>

/* RDMA fullsync port (child process) */
#define RDMA_FULLSYNC_PORT  2101

/* Replication role */
typedef enum {
    REPL_ROLE_NONE = 0,
    REPL_ROLE_MASTER,
    REPL_ROLE_SLAVE
} repl_role_t;

/* Replication configuration */
struct repl_tcp_config {
    repl_role_t      role;
    char             master_ip[64];    /* slave: master IP address */
    uint16_t         repl_port;        /* master: listen port; slave: master's port to connect */
    uint16_t         kv_port;          /* local kvstore TCP port */
    int              enable_nodelay;   /* TCP_NODELAY flag (default: 1) */
    int              sndbuf_size;      /* TCP send buffer size (0 = default) */
    int              rcvbuf_size;      /* TCP receive buffer size (0 = default) */
    int              enable_backlog;   /* Enable backlog for incremental recovery (default: 1) */
};

/* Initialize replication subsystem (called from kvstore main) */
int repl_tcp_init(const struct repl_tcp_config *cfg);

/* Shutdown replication subsystem */
void repl_tcp_shutdown(void);

/* Get current replication offset (for slave to save) */
uint64_t repl_tcp_get_offset(void);

/* Get replication statistics */
struct repl_tcp_stats {
    uint64_t bytes_sent;
    uint64_t bytes_recv;
    uint64_t frames_sent;
    uint64_t frames_recv;
    uint64_t reconnect_count;
    uint64_t master_offset;      /* Master current write offset */
    uint64_t slave_offset;       /* Slave acknowledged offset */
    uint64_t large_commands;     /* Long commands (>4KB) via notify path */
    int      connected;
    int      partial_resync;     /* 1 if last reconnect used partial resync */
};

void repl_tcp_get_stats(struct repl_tcp_stats *stats);

#endif /* REPL_TCP_H */
