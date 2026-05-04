#ifndef __RDMA_REPL_H__
#define __RDMA_REPL_H__

#include <stdint.h>

/* ===================================================================
 * RDMA-based full synchronization over libfabric (Soft-RoCE capable)
 *
 * Architecture:
 *   - Full sync uses fork() for COW-consistent engine snapshot.
 *   - Incremental sync is handled by ebpf_incr_repl (TCP, not RDMA).
 *   - Fullsync and incremental use independent connections.
 *   - Controlled by kvs_config.rdma_sync_enabled.
 *
 * Fullsync wire protocol (single FI_EP_MSG connection):
 *   1. master -> slave : uint64_t base_seq      (fullsync cut point)
 *   2. master -> slave : uint64_t payload_len
 *   3. master -> slave : payload_len bytes of RESP SET commands
 *   4. master shuts down the endpoint.
 *
 * After fullsync, the slave connects to the master's TCP incr port
 * and sends "INCR_SYNC <base_seq>\r\n" to begin incremental catchup.
 * =================================================================== */

/* Master: spin up a background listener thread. On each slave
 * connection, fork()s a child for COW-consistent serialization,
 * sends the dataset via RDMA, and coordinates with the incremental
 * thread for backlog catchup. Returns 0 on success. */
int rdma_repl_master_start(void);

/* Slave: blocking call. Connects to master, receives the fullsync
 * dataset (with base_seq), reconstructs state, then connects to the
 * master's TCP incr port for incremental updates. Returns 0 on
 * success, -1 on failure. */
int rdma_repl_slave_run(void);

/* --- Fullsync wire format helpers --- */

/* Header sent before the RESP payload on the RDMA fullsync connection */
struct fullsync_header {
    uint64_t base_seq;
    uint64_t payload_len;
};

#endif /* __RDMA_REPL_H__ */
