#include "repl_stream.h"
#include "config.h"
#include "xdp_repl_manager.h"

#include <stdatomic.h>
#include <stdio.h>

#define KVS_REPL_STREAM_MAX_REASSEMBLY (1024u * 1024u)

static _Atomic uint64_t g_last_ack = 0;
static _Atomic int g_master_started = 0;
static _Atomic int g_slave_started = 0;
static _Atomic int g_need_fullsync = 0;

int repl_stream_start_master(void)
{
    atomic_store_explicit(&g_master_started, 1, memory_order_release);
    if (kvs_config.xdp_repl_enabled) {
        printf("[REPL-STREAM] master AF_XDP data plane requested: iface=%s queue=%d port=%d mtu=%d batch=%d zero_copy=%s\n",
               kvs_config.xdp_repl_ifname,
               kvs_config.xdp_repl_queue,
               kvs_config.xdp_repl_udp_port,
               kvs_config.xdp_repl_mtu,
               kvs_config.xdp_repl_batch,
               kvs_config.xdp_repl_zero_copy ? "require" : "auto");
        xdp_repl_manager_init(kvs_config.xdp_repl_ifname,
                              kvs_config.xdp_repl_queue,
                              kvs_config.xdp_repl_udp_port,
                              kvs_config.xdp_repl_zero_copy);
    }
    return 0;
}

int repl_stream_start_slave(uint64_t start_seq)
{
    atomic_store_explicit(&g_last_ack, start_seq, memory_order_release);
    atomic_store_explicit(&g_slave_started, 1, memory_order_release);
    atomic_store_explicit(&g_need_fullsync, 0, memory_order_release);
    return 0;
}

int repl_stream_publish(const kvs_journal_record_t *rec)
{
    if (!rec) return -1;
    if (!atomic_load_explicit(&g_master_started, memory_order_acquire))
        return 0;
    if (!kvs_config.xdp_repl_enabled) return 0;

    uint32_t mtu = (uint32_t)kvs_config.xdp_repl_mtu;
    uint32_t hdr_room = (uint32_t)sizeof(kvs_repl_frame_hdr_t) + 42u;
    uint32_t frag_payload = (mtu > hdr_room + 64u) ? (mtu - hdr_room) : 512u;
    uint16_t frag_cnt = (uint16_t)((rec->len + frag_payload - 1u) / frag_payload);
    if (frag_cnt == 0) frag_cnt = 1;
    for (uint16_t i = 0; i < frag_cnt; i++) {
        uint32_t off = (uint32_t)i * frag_payload;
        uint32_t left = rec->len - off;
        uint32_t chunk = left > frag_payload ? frag_payload : left;
        kvs_repl_frame_hdr_t hdr;
        hdr.magic = KVS_REPL_FRAME_MAGIC;
        hdr.version = KVS_REPL_FRAME_VERSION;
        hdr.type = KVS_REPL_FRAME_DATA;
        hdr.repl_epoch = 0;
        hdr.seq = rec->seq;
        hdr.payload_len = chunk;
        hdr.crc32 = rec->crc32;
        hdr.frag_idx = i;
        hdr.frag_cnt = frag_cnt;
        hdr.frag_off = off;
        hdr.total_len = rec->len;
        hdr.kind = rec->kind;
        hdr.flags = rec->flags;
        xdp_repl_send_fragment(&hdr, rec->payload + off, chunk);
    }
    return 0;
}

void repl_stream_ack(uint64_t seq)
{
    atomic_store_explicit(&g_last_ack, seq, memory_order_release);
}

void repl_stream_request_fullsync(void)
{
    atomic_store_explicit(&g_need_fullsync, 1, memory_order_release);
}

void repl_stream_stop(void)
{
    xdp_repl_manager_stop();
    atomic_store_explicit(&g_master_started, 0, memory_order_release);
    atomic_store_explicit(&g_slave_started, 0, memory_order_release);
}

uint64_t repl_stream_last_ack(void)
{
    return atomic_load_explicit(&g_last_ack, memory_order_acquire);
}
