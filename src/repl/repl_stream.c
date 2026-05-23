#include "repl_stream.h"
#include "config.h"
#include "xdp_repl_manager.h"
#include "xdp_incr_repl.h"
#include "rdma_repl.h"

#include <stdatomic.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>

static _Atomic uint64_t g_last_ack = 0;
static _Atomic int g_master_started = 0;
static _Atomic int g_slave_started = 0;
static _Atomic int g_need_fullsync = 0;
static pthread_t g_slave_poll_tid;
static _Atomic int g_slave_poll_running = 0;
static pthread_t g_master_retrans_tid;
static _Atomic int g_master_retrans_running = 0;

static void *slave_xdp_poll_loop(void *arg)
{
    (void)arg;
    while (atomic_load_explicit(&g_slave_poll_running, memory_order_acquire)) {
        int n = xdp_repl_poll();
        if (n <= 0) usleep(1000);
    }
    return NULL;
}

static void *master_retrans_loop(void *arg)
{
    (void)arg;
    while (atomic_load_explicit(&g_master_retrans_running, memory_order_acquire)) {
        xdp_repl_poll();
        xdp_incr_retrans_tick();
        usleep(10000);  /* 10ms tick */
    }
    return NULL;
}

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
        if (xdp_repl_manager_init(kvs_config.xdp_repl_ifname,
                                  kvs_config.xdp_repl_queue,
                                  kvs_config.xdp_repl_udp_port,
                                  kvs_config.xdp_repl_zero_copy) == 0) {
            xdp_incr_master_start();
            atomic_store_explicit(&g_master_retrans_running, 1, memory_order_release);
            int rc = pthread_create(&g_master_retrans_tid, NULL, master_retrans_loop, NULL);
            if (rc != 0) {
                atomic_store_explicit(&g_master_retrans_running, 0, memory_order_release);
                fprintf(stderr, "[REPL-STREAM] master retrans thread failed: %d\n", rc);
                xdp_incr_request_fullsync();
                return -1;
            }
        } else {
            xdp_incr_request_fullsync();
            return -1;
        }
    }
    return 0;
}

int repl_stream_start_slave(uint64_t start_seq)
{
    atomic_store_explicit(&g_last_ack, start_seq, memory_order_release);
    atomic_store_explicit(&g_slave_started, 1, memory_order_release);
    atomic_store_explicit(&g_need_fullsync, 0, memory_order_release);
    xdp_incr_slave_start(0, 0, start_seq);
    if (kvs_config.xdp_repl_enabled) {
        printf("[REPL-STREAM] slave AF_XDP data plane requested: iface=%s queue=%d port=%d mtu=%d batch=%d zero_copy=%s\n",
               kvs_config.xdp_repl_ifname,
               kvs_config.xdp_repl_queue,
               kvs_config.xdp_repl_udp_port,
               kvs_config.xdp_repl_mtu,
               kvs_config.xdp_repl_batch,
               kvs_config.xdp_repl_zero_copy ? "require" : "auto");
        if (xdp_repl_manager_init(kvs_config.xdp_repl_ifname,
                                  kvs_config.xdp_repl_queue,
                                  kvs_config.xdp_repl_udp_port,
                                  kvs_config.xdp_repl_zero_copy) == 0) {
            atomic_store_explicit(&g_slave_poll_running, 1, memory_order_release);
            int rc = pthread_create(&g_slave_poll_tid, NULL, slave_xdp_poll_loop, NULL);
            if (rc != 0) {
                atomic_store_explicit(&g_slave_poll_running, 0, memory_order_release);
                fprintf(stderr, "[REPL-STREAM] slave XDP poll thread failed: %d\n", rc);
                xdp_incr_request_fullsync();
                return -1;
            }
        } else {
            xdp_incr_request_fullsync();
            return -1;
        }
    }
    return 0;
}

int repl_stream_publish(const kvs_journal_record_t *rec)
{
    if (!rec) return -1;
    if (!atomic_load_explicit(&g_master_started, memory_order_acquire))
        return 0;
    if (!kvs_config.xdp_repl_enabled) return 0;

    /* Capture to rewrite buffer during fullsync */
    rdma_repl_capture_write(rec);

    return xdp_incr_publish(rec);
}

void repl_stream_ack(uint64_t seq)
{
    atomic_store_explicit(&g_last_ack, seq, memory_order_release);
}

void repl_stream_request_fullsync(void)
{
    atomic_store_explicit(&g_need_fullsync, 1, memory_order_release);
    xdp_incr_request_fullsync();
}

void repl_stream_stop(void)
{
    if (atomic_exchange_explicit(&g_slave_poll_running, 0, memory_order_acq_rel)) {
        pthread_join(g_slave_poll_tid, NULL);
    }
    if (atomic_exchange_explicit(&g_master_retrans_running, 0, memory_order_acq_rel)) {
        pthread_join(g_master_retrans_tid, NULL);
    }
    xdp_incr_stop();
    atomic_store_explicit(&g_master_started, 0, memory_order_release);
    atomic_store_explicit(&g_slave_started, 0, memory_order_release);
}

uint64_t repl_stream_last_ack(void)
{
    return atomic_load_explicit(&g_last_ack, memory_order_acquire);
}
