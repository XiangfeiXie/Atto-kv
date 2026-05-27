#include "repl_stream.h"
#include "config.h"
#include "repl_kcp_xdp.h"
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
        printf("[REPL-STREAM] Starting KCP + XDP incremental replication (master)\n");
        printf("[REPL-STREAM] UDP Port: %d, Interface: %s\n",
               kvs_config.xdp_repl_port,
               kvs_config.xdp_repl_ifname);

        struct repl_kcp_config cfg = {
            .role = REPL_ROLE_MASTER,
            .repl_port = kvs_config.xdp_repl_port,
            .kv_port = kvs_config.port,
            .use_af_xdp = kvs_config.xdp_repl_zero_copy,
            .queue_id = kvs_config.xdp_repl_queue,
        };
        snprintf(cfg.ifname, sizeof(cfg.ifname), "%s", kvs_config.xdp_repl_ifname);

        if (repl_kcp_init(&cfg) != 0) {
            fprintf(stderr, "[REPL-STREAM] KCP + XDP master start failed\n");
            return -1;
        }

        printf("[REPL-STREAM] Master started successfully\n");
    }

    /* Set flag after backlog is initialized */
    atomic_store_explicit(&g_master_started, 1, memory_order_release);

    return 0;
}

int repl_stream_start_slave(uint64_t start_seq)
{
    (void)start_seq;
    atomic_store_explicit(&g_slave_started, 1, memory_order_release);

    if (kvs_config.xdp_repl_enabled) {
        printf("[REPL-STREAM] Starting KCP + XDP incremental replication (slave)\n");
        printf("[REPL-STREAM] Local port: %d, Master: %s:%d\n",
               kvs_config.xdp_repl_port,
               kvs_config.rdma_master_ip,
               kvs_config.xdp_repl_master_port);

        struct repl_kcp_config cfg = {
            .role = REPL_ROLE_SLAVE,
            .repl_port = kvs_config.xdp_repl_master_port,  /* Master's port to connect */
            .local_port = kvs_config.xdp_repl_port,        /* Slave's local bind port */
            .kv_port = kvs_config.port,
            .use_af_xdp = kvs_config.xdp_repl_zero_copy,
            .queue_id = kvs_config.xdp_repl_queue,
        };
        snprintf(cfg.master_ip, sizeof(cfg.master_ip), "%s", kvs_config.rdma_master_ip);
        snprintf(cfg.ifname, sizeof(cfg.ifname), "%s", kvs_config.xdp_repl_ifname);

        if (repl_kcp_init(&cfg) != 0) {
            fprintf(stderr, "[REPL-STREAM] KCP + XDP slave start failed\n");
            return -1;
        }

        printf("[REPL-STREAM] Slave started successfully\n");
    }

    return 0;
}

int repl_stream_publish(const kvs_journal_record_t *rec)
{
    (void)rec;
    if (!atomic_load_explicit(&g_master_started, memory_order_acquire))
        return 0;
    if (!kvs_config.xdp_repl_enabled) return 0;

    /* KCP replication is handled by eBPF filter automatically */
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
        repl_kcp_shutdown();
    }

    atomic_store_explicit(&g_master_started, 0, memory_order_release);
    atomic_store_explicit(&g_slave_started, 0, memory_order_release);

    printf("[REPL-STREAM] Stopped\n");
}

uint64_t repl_stream_last_ack(void)
{
    return 0;  /* KCP handles ACKs internally */
}
