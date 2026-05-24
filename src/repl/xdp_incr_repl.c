#define _GNU_SOURCE
#include "xdp_incr_repl.h"
#include "repl_frame.h"
#include "xdp_repl_manager.h"
#include "config.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

/* ===================================================================
 * XDP Incremental Replication - Simplified
 *
 * NOTE: This module is now deprecated in favor of tcp_xdp_repl.c
 * Kept for compatibility with RDMA fullsync coordination.
 * =================================================================== */

static _Atomic int g_started = 0;
static _Atomic int g_state = XDP_INCR_STATE_STOPPED;
static _Atomic uint64_t g_global_seq = 0;
static _Atomic uint64_t g_session_id = 0;
static _Atomic uint64_t g_epoch = 0;
static _Atomic uint64_t g_base_seq = 0;
static _Atomic int g_need_fullsync = 0;

static uint64_t next_session_id(void)
{
    uint64_t v = atomic_fetch_add_explicit(&g_session_id, 1, memory_order_acq_rel) + 1;
    if (v == 0) v = atomic_fetch_add_explicit(&g_session_id, 1, memory_order_acq_rel) + 1;
    return v;
}

int xdp_incr_master_start(void)
{
    atomic_store_explicit(&g_started, 1, memory_order_release);
    atomic_store_explicit(&g_state, XDP_INCR_STATE_WAIT_FULLSYNC, memory_order_release);
    return 0;
}

int xdp_incr_slave_start(uint64_t session_id, uint64_t epoch, uint64_t base_seq)
{
    atomic_store_explicit(&g_session_id, session_id, memory_order_release);
    atomic_store_explicit(&g_epoch, epoch, memory_order_release);
    atomic_store_explicit(&g_base_seq, base_seq, memory_order_release);
    atomic_store_explicit(&g_need_fullsync, 0, memory_order_release);
    atomic_store_explicit(&g_state, XDP_INCR_STATE_RUNNING, memory_order_release);
    atomic_store_explicit(&g_started, 1, memory_order_release);
    return 0;
}

void xdp_incr_stop(void)
{
    xdp_repl_manager_stop();
    atomic_store_explicit(&g_started, 0, memory_order_release);
    atomic_store_explicit(&g_state, XDP_INCR_STATE_STOPPED, memory_order_release);
}

void xdp_incr_request_fullsync(void)
{
    atomic_store_explicit(&g_need_fullsync, 1, memory_order_release);
    atomic_store_explicit(&g_state, XDP_INCR_STATE_NEED_FULLSYNC, memory_order_release);
}

int xdp_incr_need_fullsync(void)
{
    return atomic_load_explicit(&g_need_fullsync, memory_order_acquire);
}

uint64_t xdp_incr_fullsync_begin(void)
{
    uint64_t epoch = atomic_fetch_add_explicit(&g_epoch, 1, memory_order_acq_rel) + 1;
    uint64_t session = next_session_id();
    atomic_store_explicit(&g_session_id, session, memory_order_release);
    atomic_store_explicit(&g_state, XDP_INCR_STATE_WAIT_FULLSYNC, memory_order_release);
    atomic_store_explicit(&g_base_seq, atomic_load_explicit(&g_global_seq, memory_order_acquire), memory_order_release);
    atomic_store_explicit(&g_need_fullsync, 0, memory_order_release);
    return epoch;
}

void xdp_incr_fullsync_finish(void)
{
    atomic_store_explicit(&g_state, XDP_INCR_STATE_RUNNING, memory_order_release);
}

uint64_t xdp_incr_fullsync_base_seq(void)
{
    return atomic_load_explicit(&g_base_seq, memory_order_acquire);
}

uint64_t xdp_incr_current_session_id(void)
{
    return atomic_load_explicit(&g_session_id, memory_order_acquire);
}

uint64_t xdp_incr_current_epoch(void)
{
    return atomic_load_explicit(&g_epoch, memory_order_acquire);
}

uint64_t xdp_incr_read_global_seq(void)
{
    return atomic_load_explicit(&g_global_seq, memory_order_acquire);
}

/* Deprecated functions - kept for compatibility */
int xdp_incr_publish(const kvs_journal_record_t *rec)
{
    (void)rec;
    return 0;
}

int xdp_incr_apply_frame(const kvs_repl_frame_hdr_t *hdr,
                         const char *payload, uint32_t len,
                         DynBuf *write_buf)
{
    (void)hdr;
    (void)payload;
    (void)len;
    (void)write_buf;
    return 0;
}

uint64_t xdp_incr_last_ack(void)
{
    return 0;
}

void xdp_incr_handle_ack(uint64_t ack_seq)
{
    (void)ack_seq;
}

int xdp_incr_retrans_tick(void)
{
    return 0;
}
