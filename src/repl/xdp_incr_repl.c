#define _GNU_SOURCE
#include "xdp_incr_repl.h"
#include "repl_frame.h"
#include "xdp_repl_manager.h"
#include "config.h"
#include "protocol.h"
#include "kvstore.h"
#include "dynbuf.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <zlib.h>

static _Atomic int g_started = 0;
static _Atomic int g_state = XDP_INCR_STATE_STOPPED;
static _Atomic uint64_t g_last_ack = 0;
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

static uint32_t calc_checksum(const void *buf, uint32_t len)
{
    return (uint32_t)crc32(0L, (const Bytef *)buf, len);
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
    atomic_store_explicit(&g_last_ack, base_seq, memory_order_release);
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

int xdp_incr_publish(const kvs_journal_record_t *rec)
{
    if (!rec) return -1;
    if (!atomic_load_explicit(&g_started, memory_order_acquire)) return 0;
    if (!kvs_config.xdp_repl_enabled) return 0;
    if (atomic_load_explicit(&g_state, memory_order_acquire) != XDP_INCR_STATE_RUNNING)
        return 0;

    uint32_t mtu = (uint32_t)kvs_config.xdp_repl_mtu;
    uint32_t hdr_room = (uint32_t)sizeof(kvs_repl_frame_hdr_t) + 42u;
    uint32_t max_payload = (mtu > hdr_room + 64u) ? (mtu - hdr_room) : 512u;
    if (rec->len > max_payload) {
        xdp_incr_request_fullsync();
        return -1;
    }

    uint64_t seq = atomic_fetch_add_explicit(&g_global_seq, 1, memory_order_acq_rel) + 1;
    kvs_repl_frame_hdr_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic = KVS_REPL_FRAME_MAGIC;
    hdr.version = KVS_REPL_FRAME_VERSION;
    hdr.type = KVS_REPL_FRAME_DATA;
    hdr.repl_epoch = atomic_load_explicit(&g_epoch, memory_order_acquire);
    hdr.seq = seq;
    hdr.payload_len = rec->len;
    hdr.crc32 = calc_checksum(rec->payload, rec->len);
    hdr.frag_idx = 0;
    hdr.frag_cnt = 1;
    hdr.frag_off = 0;
    hdr.total_len = rec->len;
    hdr.kind = rec->kind;
    hdr.flags = rec->flags;

    if (xdp_repl_send_fragment(&hdr, rec->payload, rec->len) != 0) {
        xdp_incr_request_fullsync();
        return -1;
    }

    atomic_store_explicit(&g_last_ack, seq, memory_order_release);
    return 0;
}

int xdp_incr_apply_frame(const kvs_repl_frame_hdr_t *hdr,
                         const char *payload, uint32_t len,
                         DynBuf *write_buf)
{
    if (!hdr || (!payload && len)) return -1;
    if (!atomic_load_explicit(&g_started, memory_order_acquire)) return 0;
    if (atomic_load_explicit(&g_state, memory_order_acquire) != XDP_INCR_STATE_RUNNING)
        return 0;
    if (hdr->magic != KVS_REPL_FRAME_MAGIC || hdr->version != KVS_REPL_FRAME_VERSION)
        return -1;
    if (hdr->type != KVS_REPL_FRAME_DATA) return 0;
    if (hdr->payload_len != len || hdr->total_len != len) return -1;
    if (hdr->crc32 != calc_checksum(payload, len)) {
        xdp_incr_request_fullsync();
        return -1;
    }
    if (hdr->repl_epoch != atomic_load_explicit(&g_epoch, memory_order_acquire) &&
        atomic_load_explicit(&g_epoch, memory_order_acquire) != 0) {
        xdp_incr_request_fullsync();
        return -1;
    }

    uint64_t expected = atomic_load_explicit(&g_last_ack, memory_order_acquire) + 1;
    if (hdr->seq != expected && expected != 1) {
        xdp_incr_request_fullsync();
        return -1;
    }

    if (write_buf) {
        kvs_token_t tokens[KVS_MAX_TOKENS];
        int argc = 0;
        int cmd_len = resp_parse_slices(payload, (int)len, tokens, KVS_MAX_TOKENS, &argc);
        if (cmd_len <= 0) return -1;
        dispatch_result_t rc = kvs_dispatch(tokens, argc, payload, cmd_len, write_buf, NULL);
        if (rc != DISPATCH_OK && rc != DISPATCH_CLOSE) return -1;
    }

    atomic_store_explicit(&g_last_ack, hdr->seq, memory_order_release);
    return 0;
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

uint64_t xdp_incr_last_ack(void)
{
    return atomic_load_explicit(&g_last_ack, memory_order_acquire);
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
