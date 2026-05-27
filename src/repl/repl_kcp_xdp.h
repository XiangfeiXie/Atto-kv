/* repl_kcp_xdp.h — KCP + AF_XDP replication transport layer
 *
 * Architecture:
 *   eBPF filter → ringbuf → KCP reliability → AF_XDP zero-copy → network
 *
 * Features:
 *   - Smart filtering: only write commands captured (60-80% reduction)
 *   - KCP reliability: ARQ, congestion control, fast retransmit
 *   - AF_XDP zero-copy: bypass kernel network stack
 *   - Integrated into kvstore main process (no separate daemon)
 */
#ifndef REPL_KCP_XDP_H
#define REPL_KCP_XDP_H

#include <stdint.h>

/* Replication role */
typedef enum {
    REPL_ROLE_NONE = 0,
    REPL_ROLE_MASTER,
    REPL_ROLE_SLAVE
} repl_role_t;

/* Transport mode */
typedef enum {
    TRANSPORT_UDP = 0,      /* UDP socket (current default) */
    TRANSPORT_AF_XDP        /* AF_XDP zero-copy (requires libxdp) */
} transport_mode_t;

/* Replication configuration */
struct repl_kcp_config {
    repl_role_t      role;
    transport_mode_t transport;   /* UDP or AF_XDP */
    char             master_ip[64];    /* slave: master IP address */
    uint16_t         repl_port;        /* master: listen port; slave: master's port to connect */
    uint16_t         local_port;       /* slave: local bind port (0 = use repl_port) */
    uint16_t         kv_port;          /* local kvstore TCP port */
    char             ifname[16];       /* network interface for AF_XDP */
    int              queue_id;         /* XDP queue ID (default: 0) */
    int              use_af_xdp;       /* 1 = try AF_XDP, 0 = UDP only */
};

/* Initialize replication subsystem (called from kvstore main) */
int repl_kcp_init(const struct repl_kcp_config *cfg);

/* Shutdown replication subsystem */
void repl_kcp_shutdown(void);

/* Periodic tick for KCP (call every 10-20ms) */
void repl_kcp_update(void);

/* Get replication statistics */
struct repl_kcp_stats {
    uint64_t bytes_sent;
    uint64_t bytes_recv;
    uint64_t pkts_sent;
    uint64_t pkts_recv;
    uint64_t pkts_lost;
    uint64_t rtt_ms;
};

void repl_kcp_get_stats(struct repl_kcp_stats *stats);

#endif
