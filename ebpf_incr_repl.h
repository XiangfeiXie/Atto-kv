#ifndef EBPF_INCR_REPL_H
#define EBPF_INCR_REPL_H

#include <stdint.h>

/* Maximum RESP command size captured by BPF.
 * Must match the definition in ebpf_incr.bpf.c.                    */
#define INCR_MAX_CMD   8192

/* Wire frame layout (over TCP):
 *   [ uint64_t cmd_len ][ cmd_len bytes of RESP command ]
 * A cmd_len of 0 is the graceful-disconnect sentinel.              */
#define INCR_SEND_BUF  ((int)(sizeof(uint64_t) + INCR_MAX_CMD))
#define INCR_RECV_BUF  INCR_SEND_BUF

/* Maximum number of simultaneously connected slaves */
#define INCR_MAX_SLAVES 8

/* Shared event struct (mirrors BPF-side struct incr_event) */
struct incr_event {
    uint32_t length;
    char     msg[INCR_MAX_CMD];
};

/* --- Master-side API --- */

/* Load BPF program, attach uprobe to kvs_rdma_incr_probe, start
 * consumer thread AND a TCP incr listener on kvs_config.incr_port.
 * Returns 0 on success, -1 on failure.                          */
int  ebpf_incr_master_start(void);

/* Register a connected TCP slave for incremental delivery.
 * start_seq is inclusive: module replays backlog records with
 * record.seq >= start_seq before entering live streaming.
 * Ownership of tcp_fd transfers to this module on success;
 * caller keeps ownership on failure (and should close it).
 * Returns 0 on success, -1 on failure.                           */
int  ebpf_incr_add_slave(int tcp_fd, uint64_t start_seq);

/* Returns 1 when incremental stream is healthy, 0 otherwise.
 * When unhealthy, callers should fall back to full-sync only.     */
int  ebpf_incr_master_healthy(void);

/* Gracefully disconnect all slaves, stop TCP listener and unload
 * BPF program.                                                    */
void ebpf_incr_stop(void);

#endif /* EBPF_INCR_REPL_H */
