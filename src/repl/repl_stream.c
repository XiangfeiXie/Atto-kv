#include "repl_stream.h"
#include "config.h"
#include "tcp_xdp_repl.h"
#include "xdp_incr_repl.h"
#include "rdma_repl.h"

#include <stdatomic.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>

/* ===================================================================
 * Replication Stream - Unified interface for incremental replication
 *
 * Now uses TCP + XDP for reliable, high-performance replication
 * =================================================================== */

static _Atomic int g_master_started = 0;
static _Atomic int g_slave_started = 0;

int repl_stream_start_master(void)
{
    if (kvs_config.xdp_repl_enabled) {
        printf("[REPL-STREAM] Starting TCP + XDP incremental replication (master)\n");
        printf("[REPL-STREAM] TCP Port: %d, Interface: %s\n",
               kvs_config.xdp_repl_tcp_port,
               kvs_config.xdp_repl_ifname);

        if (tcp_xdp_repl_master_start() != 0) {
            fprintf(stderr, "[REPL-STREAM] TCP + XDP master start failed\n");
            xdp_incr_request_fullsync();
            return -1;
        }

        xdp_incr_master_start();
        printf("[REPL-STREAM] Master started successfully\n");
    }

    /* Set flag after backlog is initialized */
    atomic_store_explicit(&g_master_started, 1, memory_order_release);

    return 0;
}

int repl_stream_start_slave(uint64_t start_seq)
{
    atomic_store_explicit(&g_slave_started, 1, memory_order_release);
    xdp_incr_slave_start(0, 0, start_seq);

    if (kvs_config.xdp_repl_enabled) {
        printf("[REPL-STREAM] Starting TCP + XDP incremental replication (slave)\n");
        printf("[REPL-STREAM] Master: %s:%d\n",
               kvs_config.rdma_master_ip,
               kvs_config.xdp_repl_tcp_port);

        if (tcp_xdp_repl_slave_start(kvs_config.rdma_master_ip,
                                     kvs_config.xdp_repl_tcp_port) != 0) {
            fprintf(stderr, "[REPL-STREAM] TCP + XDP slave start failed\n");
            xdp_incr_request_fullsync();
            return -1;
        }

        printf("[REPL-STREAM] Slave started successfully\n");
    }

    return 0;
}

int repl_stream_publish(const kvs_journal_record_t *rec)
{
    if (!rec) return -1;
    if (!atomic_load_explicit(&g_master_started, memory_order_acquire))
        return 0;
    if (!kvs_config.xdp_repl_enabled) return 0;

    return tcp_xdp_repl_master_publish(rec);
}

void repl_stream_ack(uint64_t seq)
{
    (void)seq;
}

void repl_stream_request_fullsync(void)
{
    xdp_incr_request_fullsync();
}

void repl_stream_stop(void)
{
    if (atomic_load_explicit(&g_master_started, memory_order_acquire)) {
        tcp_xdp_repl_master_stop();
    }

    if (atomic_load_explicit(&g_slave_started, memory_order_acquire)) {
        tcp_xdp_repl_slave_stop();
    }

    xdp_incr_stop();
    atomic_store_explicit(&g_master_started, 0, memory_order_release);
    atomic_store_explicit(&g_slave_started, 0, memory_order_release);

    printf("[REPL-STREAM] Stopped\n");
}

uint64_t repl_stream_last_ack(void)
{
    return tcp_xdp_repl_get_slave_offset();
}
