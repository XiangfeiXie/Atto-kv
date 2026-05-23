#ifndef __RDMA_REPL_H__
#define __RDMA_REPL_H__

#include <stdint.h>

/* ===================================================================
 * RDMA-based full synchronization over libfabric (Soft-RoCE capable)
 *
 * Architecture:
 *   - Full sync remains RDMA/libfabric.
 *   - Incremental sync is handled by XDP/AF_XDP via repl_stream.
 *   - On reconnect we intentionally re-run full sync first, then
 *     re-enter incremental mode with a new session/epoch.
 * =================================================================== */

int rdma_repl_master_start(void);
int rdma_repl_slave_run(void);
int rdma_repl_slave_need_fullresync(void);

#endif /* __RDMA_REPL_H__ */
