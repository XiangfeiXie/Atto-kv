#ifndef __RDMA_REPL_H__
#define __RDMA_REPL_H__

/* ===================================================================
 * RDMA-based full synchronization over libfabric (Soft-RoCE capable)
 *
 * Scope (development stage):
 *   - Full sync only. Incremental sync is NOT implemented here and
 *     is intentionally left to a future module.
 *   - Completely independent from the legacy TCP replication in repl.c;
 *     the two subsystems are isolated via config flags.
 *
 * Protocol on the wire (single FI_EP_MSG connection):
 *   1. master  -> slave : uint64_t payload_len  (little-endian host order)
 *   2. master  -> slave : payload_len bytes of RESP-encoded SET commands
 *                         (one command per KV pair, in the standard
 *                          Redis RESP array-of-bulk-string format)
 *   3. master shuts down the endpoint.
 *
 * The slave parses the RESP stream with the existing
 * resp_try_parse_full_cmd() + kvs_protocol() path, so the in-memory
 * representation is reconstructed purely via the normal command path.
 * =================================================================== */

/* Master: spin up a background listener thread that performs a full
 * sync for every incoming slave connection. Returns 0 on success. */
int rdma_repl_master_start(void);

/* Slave: blocking call. Connects to master, receives the serialized
 * dataset, reconstructs state, then returns. Returns 0 on success,
 * -1 on failure (caller may proceed without sync on failure). */
int rdma_repl_slave_run(void);

#endif /* __RDMA_REPL_H__ */
