/* kvstore_info.c — INFO command handler */

#include "kvstore_internal.h"

#include <stdio.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/utsname.h>

#include "persistence.h"

static int section_match(kvs_token_t t, const char *name) {
    size_t n = strlen(name);
    if ((size_t)t.len != n) return 0;
    return strncasecmp(t.ptr, name, n) == 0;
}

dispatch_result_t handle_info(kvs_token_t *tokens, int argc, DynBuf *wb) {
    int want_server      = 1;
    int want_clients     = 1;
    int want_memory      = 1;
    int want_persistence = 1;
    int want_stats       = 1;
    int want_replication = 1;
    int want_cpu         = 1;
    int want_keyspace    = 1;

    if (argc >= 2) {
        kvs_token_t s = tokens[1];
        if (!section_match(s, "all") && !section_match(s, "everything") &&
            !section_match(s, "default")) {
            want_server = section_match(s, "server");
            want_clients = section_match(s, "clients");
            want_memory = section_match(s, "memory");
            want_persistence = section_match(s, "persistence");
            want_stats = section_match(s, "stats");
            want_replication = section_match(s, "replication");
            want_cpu = section_match(s, "cpu");
            want_keyspace = section_match(s, "keyspace");
        }
    }

    char body[4096];
    int  off = 0;
    #define APPEND(...) do {                                                 \
        int _n = snprintf(body + off, sizeof(body) - off, __VA_ARGS__);      \
        if (_n < 0 || _n >= (int)(sizeof(body) - off)) goto truncated;       \
        off += _n;                                                            \
    } while (0)

    long long now_ms = get_time_now_real();
    long long uptime_s = (now_ms - g_kvs_start_time_ms) / 1000;

    if (want_server) {
        struct utsname un = {0};
        uname(&un);
        APPEND("# Server\r\n");
        APPEND("redis_version:9.1.0-kvstore\r\n");
        APPEND("kvstore_version:10.0\r\n");
        APPEND("redis_mode:standalone\r\n");
        APPEND("os:%s %s %s\r\n", un.sysname, un.release, un.machine);
        APPEND("arch_bits:64\r\n");
        APPEND("process_id:%d\r\n", (int)getpid());
        APPEND("tcp_port:%d\r\n", kvs_config.port);
        APPEND("uptime_in_seconds:%lld\r\n", uptime_s);
        APPEND("uptime_in_days:%lld\r\n", uptime_s / 86400);
        const char *netname =
            (kvs_config.network_model == NET_PROACTOR) ? "proactor (io_uring)" :
            (kvs_config.network_model == NET_REACTOR)  ? "reactor (epoll)"     :
                                                         "ntyco (coroutine)";
        APPEND("network_model:%s\r\n", netname);
        APPEND("workers:%d\r\n", 1);
        APPEND("\r\n");
    }

    if (want_clients) {
        APPEND("# Clients\r\n");
        APPEND("connected_clients:%d\r\n",
               atomic_load_explicit(&g_kvs_connected_clients,
                                    memory_order_relaxed));
        APPEND("maxclients:%d\r\n",
               __atomic_load_n(&kvs_config.maxclients, __ATOMIC_RELAXED));
        APPEND("\r\n");
    }

    if (want_memory) {
        size_t allocated = 0;
        const char *allocator = "libc";
#ifdef USE_JEMALLOC
        size_t sz = sizeof(allocated);
        uint64_t epoch = 1;
        size_t esz = sizeof(epoch);
        mallctl("epoch", &epoch, &esz, &epoch, esz);
        if (mallctl("stats.allocated", &allocated, &sz, NULL, 0) != 0)
            allocated = 0;
        allocator = "jemalloc";
#else
        struct rusage ru;
        if (getrusage(RUSAGE_SELF, &ru) == 0)
            allocated = (size_t)ru.ru_maxrss * 1024;
#endif
        APPEND("# Memory\r\n");
        APPEND("used_memory:%zu\r\n", allocated);
        APPEND("used_memory_human:%.2fM\r\n",
               (double)allocated / (1024.0 * 1024.0));
        APPEND("mem_allocator:%s\r\n", allocator);
        APPEND("\r\n");
    }

    if (want_persistence) {
        APPEND("# Persistence\r\n");
        APPEND("aof_enabled:%d\r\n", ENABLE_AOF ? 1 : 0);
        APPEND("rdb_enabled:%d\r\n", ENABLE_RDB ? 1 : 0);
        APPEND("loading:%d\r\n", g_in_reload ? 1 : 0);
        APPEND("rdb_bgsave_in_progress:%d\r\n", rdb_bgsave_in_progress());
        APPEND("rdb_last_save_time:%llu\r\n",
               (unsigned long long)rdb_save_time());
        APPEND("rdb_changes_since_last_save:%llu\r\n",
               (unsigned long long)g_rdb_changes);
        APPEND("rdb_bgsave_total:%llu\r\n",
               (unsigned long long)rdb_bgsave_total());
        APPEND("rdb_bgsave_skipped:%llu\r\n",
               (unsigned long long)rdb_bgsave_skipped());
        APPEND("rdb_bgsave_cooldown:%d\r\n", rdb_bgsave_cooldown_sec());
        APPEND("rdb_bgsave_last_duration:%d\r\n", rdb_bgsave_last_duration());
        APPEND("\r\n");
    }

    if (want_stats) {
        _Atomic uint64_t *pexp = kvs_expired_keys_ptr();
        APPEND("# Stats\r\n");
        APPEND("total_connections_received:%llu\r\n",
               (unsigned long long)atomic_load_explicit(
                   &g_kvs_total_connections, memory_order_relaxed));
        APPEND("total_commands_processed:%llu\r\n",
               (unsigned long long)atomic_load_explicit(
                   &g_kvs_total_commands, memory_order_relaxed));
        APPEND("expired_keys:%llu\r\n",
               (unsigned long long)atomic_load_explicit(
                   pexp, memory_order_relaxed));

        /* Latency histogram (16 log2-spaced buckets, 1µs..≥16ms).
         * Distributed across KVS_LAT_SLOTS cachelines; INFO sums them. */
        uint64_t lat_buckets[KVS_LAT_BUCKETS] = {0};
        uint64_t lat_total = 0, lat_sum = 0;
        for (int s = 0; s < KVS_LAT_SLOTS; s++) {
            for (int i = 0; i < KVS_LAT_BUCKETS; i++)
                lat_buckets[i] += atomic_load_explicit(
                    &g_kvs_lat_slots[s].hist[i], memory_order_relaxed);
            lat_total += atomic_load_explicit(
                &g_kvs_lat_slots[s].count, memory_order_relaxed);
            lat_sum   += atomic_load_explicit(
                &g_kvs_lat_slots[s].sum_ns, memory_order_relaxed);
        }
        APPEND("latency_sample_rate:64\r\n");
        APPEND("latency_total_samples:%llu\r\n",
               (unsigned long long)lat_total);
        if (lat_total > 0)
            APPEND("latency_mean_us:%.2f\r\n",
                   (double)lat_sum / (double)lat_total / 1000.0);
        else
            APPEND("latency_mean_us:0.00\r\n");

        static const uint64_t bucket_us[KVS_LAT_BUCKETS] = {
            1, 2, 4, 8, 16, 32, 64, 128, 256, 512,
            1024, 2048, 4096, 8192, 16384, 0
        };
        for (int i = 0; i < KVS_LAT_BUCKETS; i++) {
            if (bucket_us[i] == 0)
                APPEND("latency_us_le_inf:%llu\r\n",
                       (unsigned long long)lat_buckets[i]);
            else
                APPEND("latency_us_le_%llu:%llu\r\n",
                       (unsigned long long)bucket_us[i],
                       (unsigned long long)lat_buckets[i]);
        }
        if (lat_total > 0) {
            const double pcts[3] = { 0.50, 0.99, 0.999 };
            const char  *names[3] = { "p50", "p99", "p999" };
            for (int p = 0; p < 3; p++) {
                uint64_t target = (uint64_t)((double)lat_total * pcts[p]);
                if (target == 0) target = 1;
                uint64_t cum = 0;
                int picked = KVS_LAT_BUCKETS - 1;
                for (int i = 0; i < KVS_LAT_BUCKETS; i++) {
                    cum += lat_buckets[i];
                    if (cum >= target) { picked = i; break; }
                }
                if (bucket_us[picked] == 0)
                    APPEND("latency_%s_us:+inf\r\n", names[p]);
                else
                    APPEND("latency_%s_us:%llu\r\n",
                           names[p], (unsigned long long)bucket_us[picked]);
            }
        } else {
            APPEND("latency_p50_us:0\r\n");
            APPEND("latency_p99_us:0\r\n");
            APPEND("latency_p999_us:0\r\n");
        }
        APPEND("\r\n");
    }

    if (want_replication) {
        APPEND("# Replication\r\n");
        APPEND("role:%s\r\n",
               kvs_config.rdma_role == ROLE_MASTER ? "master" : "slave");
        APPEND("rdma_sync_enabled:%d\r\n",
               kvs_config.rdma_sync_enabled ? 1 : 0);
        if (kvs_config.rdma_sync_enabled) {
            APPEND("rdma_transport:verbs/RoCE\r\n");
            if (kvs_config.rdma_role == ROLE_MASTER)
                APPEND("rdma_listen:%s:%d\r\n",
                       kvs_config.rdma_listen_ip, kvs_config.rdma_listen_port);
            else {
                APPEND("master_host:%s\r\n", kvs_config.rdma_master_ip);
                APPEND("master_port:%d\r\n", kvs_config.rdma_master_port);
            }
        }
        APPEND("connected_slaves:0\r\n");
        APPEND("\r\n");
    }

    if (want_cpu) {
        struct rusage ru;
        if (getrusage(RUSAGE_SELF, &ru) == 0) {
            double sys_s = ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1e6;
            double usr_s = ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1e6;
            APPEND("# CPU\r\n");
            APPEND("used_cpu_sys:%.6f\r\n", sys_s);
            APPEND("used_cpu_user:%.6f\r\n", usr_s);
            APPEND("\r\n");
        }
    }

    if (want_keyspace) {
        uint32_t nkeys = dict_count(&g_dict);
        APPEND("# Keyspace\r\n");
        if (nkeys > 0)
            APPEND("db0:keys=%u,expires=0,avg_ttl=0\r\n", nkeys);
        APPEND("\r\n");
    }

truncated:
    {
        char hdr[32];
        int hl = snprintf(hdr, sizeof(hdr), "$%d\r\n", off);
        dynbuf_cat(wb, hdr, hl);
        dynbuf_cat(wb, body, off);
        dynbuf_lit(wb, "\r\n");
    }
    #undef APPEND
    return DISPATCH_OK;
}
