#include "repl_stream.h"
#include "config.h"
#include "repl_tcp.h"
#include "repl_incr.h"
#include "rdma_repl.h"

#include <stdatomic.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>

/* ===================================================================
 * Replication Stream - Unified interface for incremental replication
 *
 * Now uses TCP + eBPF for reliable, high-performance replication
 * =================================================================== */

static _Atomic int g_master_started = 0;
static _Atomic int g_slave_started = 0;

int repl_stream_start_master(void)
{
    if (kvs_config.incr_repl_enabled) {
        printf("[REPL-STREAM] Starting TCP + eBPF incremental replication (master)\n");
        printf("[REPL-STREAM] TCP Port: %d\n", kvs_config.incr_repl_port);

        /* Initialize incremental recovery */
        if (repl_incr_init() != 0) {
            fprintf(stderr, "[REPL-STREAM] Incremental recovery init failed\n");
            return -1;
        }

        struct repl_tcp_config cfg = {
            .role = REPL_ROLE_MASTER,
            .repl_port = kvs_config.incr_repl_port,
            .kv_port = kvs_config.port,
            .enable_nodelay = 1,
            .sndbuf_size = 256 * 1024,
            .rcvbuf_size = 256 * 1024,
            .enable_backlog = 1,
        };

        if (repl_tcp_init(&cfg) != 0) {
            fprintf(stderr, "[REPL-STREAM] TCP replication master start failed\n");
            repl_incr_shutdown();
            return -1;
        }

        printf("[REPL-STREAM] Master started successfully with incremental recovery\n");
    }

    /* Set flag after initialization */
    atomic_store_explicit(&g_master_started, 1, memory_order_release);

    return 0;
}

int repl_stream_start_slave(uint64_t start_seq)
{
    (void)start_seq;
    atomic_store_explicit(&g_slave_started, 1, memory_order_release);

    if (kvs_config.incr_repl_enabled) {
        printf("[REPL-STREAM] Starting TCP + eBPF incremental replication (slave)\n");
        printf("[REPL-STREAM] Master: %s:%d\n",
               kvs_config.rdma_master_ip,
               kvs_config.incr_repl_master_port);

        /* Initialize incremental recovery */
        if (repl_incr_init() != 0) {
            fprintf(stderr, "[REPL-STREAM] Incremental recovery init failed\n");
            return -1;
        }

        struct repl_tcp_config cfg = {
            .role = REPL_ROLE_SLAVE,
            .repl_port = kvs_config.incr_repl_master_port,
            .kv_port = kvs_config.port,
            .enable_nodelay = 1,
            .sndbuf_size = 256 * 1024,
            .rcvbuf_size = 256 * 1024,
            .enable_backlog = 1,
        };
        snprintf(cfg.master_ip, sizeof(cfg.master_ip), "%s", kvs_config.rdma_master_ip);

        if (repl_tcp_init(&cfg) != 0) {
            fprintf(stderr, "[REPL-STREAM] TCP replication slave start failed\n");
            repl_incr_shutdown();
            return -1;
        }

        printf("[REPL-STREAM] Slave started successfully with incremental recovery\n");
    }

    return 0;
}

int repl_stream_publish(const kvs_journal_record_t *rec)
{
    (void)rec;
    if (!atomic_load_explicit(&g_master_started, memory_order_acquire))
        return 0;
    if (!kvs_config.incr_repl_enabled) return 0;

    /* TCP replication is handled by eBPF filter automatically */
    return 0;
}

void repl_stream_ack(uint64_t seq)
{
    (void)seq;
}

void repl_stream_request_fullsync(void)
{
    /* Full sync handled by RDMA layer */
}

void repl_stream_stop(void)
{
    if (atomic_load_explicit(&g_master_started, memory_order_acquire) ||
        atomic_load_explicit(&g_slave_started, memory_order_acquire)) {
        repl_tcp_shutdown();
        repl_incr_shutdown();
    }

    atomic_store_explicit(&g_master_started, 0, memory_order_release);
    atomic_store_explicit(&g_slave_started, 0, memory_order_release);

    printf("[REPL-STREAM] Stopped\n");
}

uint64_t repl_stream_last_ack(void)
{
    return repl_incr_get_master_offset();
}
