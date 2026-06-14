#ifndef __RDMA_REPL_H__
#define __RDMA_REPL_H__

#include <stdint.h>
#include "journal.h"

/* ===================================================================
 * RDMA batch fullsync over libfabric (FI_EP_MSG)
 *
 * Architecture:
 *   - Full sync: FI_EP_MSG batch protocol (header + data via fi_send/fi_recv)
 *   - Incremental sync: XDP/AF_XDP via repl_stream
 * =================================================================== */

struct fid_domain;
struct fid_ep;
struct fid_cq;

int rdma_repl_master_start(void);
int rdma_repl_slave_run(void);
int rdma_repl_slave_need_fullresync(void);
int rdma_repl_capture_write(const kvs_journal_record_t *rec);

/* Fullsync entry points */
int master_fullsync_planB(struct fid_domain *domain, struct fid_ep *ep,
                          struct fid_cq *cq);
int slave_receive_fullsync_batched(struct fid_domain *domain,
                                   struct fid_ep *ep, struct fid_cq *cq);

#endif /* __RDMA_REPL_H__ */
