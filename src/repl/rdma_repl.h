#ifndef __RDMA_REPL_H__
#define __RDMA_REPL_H__

#include <stdint.h>

/* ===================================================================
 * RDMA-based full synchronization over libfabric (Soft-RoCE capable)
 *
 * Architecture (Plan B — streaming pipeline):
 *   - Master: fork() for COW snapshot, child serializes entries into
 *     MAP_SHARED ring buffer, parent worker thread sends via RDMA in
 *     a 4-slot pipeline. Pipelining hides serialization latency.
 *   - Slave: pre-posts 4 recv buffers, receives chunks incrementally,
 *     loads entries via rdb_load_stream_append(). No disk I/O.
 *   - Incremental sync is handled by ebpf_incr_repl (TCP, not RDMA).
 *   - Fullsync and incremental use independent connections.
 *   - Controlled by kvs_config.rdma_sync_enabled.
 *
 * Streaming wire protocol (each chunk is one fi_send):
 *   [chunk_len:4][chunk_type:1][data:chunk_len]
 *   chunk_type = 0 HEADER  (RDB header + base_seq)
 *   chunk_type = 1 DATA    (serialized entries)
 *   chunk_type = 2 EOF     (end of stream)
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

#endif /* __RDMA_REPL_H__ */
