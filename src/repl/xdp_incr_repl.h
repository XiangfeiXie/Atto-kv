#ifndef KVS_XDP_INCR_REPL_H
#define KVS_XDP_INCR_REPL_H

#include "journal.h"
#include "repl_frame.h"
#include "dynbuf.h"

#include <stdint.h>

#define XDP_INCR_STATE_STOPPED       0
#define XDP_INCR_STATE_WAIT_FULLSYNC 1
#define XDP_INCR_STATE_RUNNING       2
#define XDP_INCR_STATE_NEED_FULLSYNC 3
#define XDP_INCR_STATE_ERROR         4

int      xdp_incr_master_start(void);
int      xdp_incr_slave_start(uint64_t session_id, uint64_t epoch,
                              uint64_t base_seq);
void     xdp_incr_stop(void);
int      xdp_incr_publish(const kvs_journal_record_t *rec);
int      xdp_incr_apply_frame(const kvs_repl_frame_hdr_t *hdr,
                              const char *payload, uint32_t len,
                              DynBuf *write_buf);
void     xdp_incr_request_fullsync(void);
int      xdp_incr_need_fullsync(void);
uint64_t xdp_incr_last_ack(void);

uint64_t xdp_incr_fullsync_begin(void);
void     xdp_incr_fullsync_finish(void);
uint64_t xdp_incr_fullsync_base_seq(void);
uint64_t xdp_incr_current_session_id(void);
uint64_t xdp_incr_current_epoch(void);
uint64_t xdp_incr_read_global_seq(void);

void     xdp_incr_handle_ack(uint64_t ack_seq);
int      xdp_incr_retrans_tick(void);

#endif /* KVS_XDP_INCR_REPL_H */
