#ifndef __KVS_CONFIG_H__
#define __KVS_CONFIG_H__

/* =========================================================
 * Runtime configuration — loaded from .conf at startup.
 * ========================================================= */

/* --- Log levels --- */
#define LOG_DEBUG  0
#define LOG_INFO   1
#define LOG_WARN   2
#define LOG_ERROR  3

/* --- Network models --- */
#define NET_REACTOR   0
#define NET_PROACTOR  1
#define NET_NTYCO     2

/* --- Persistence modes --- */
#define PERSIST_NONE  0
#define PERSIST_RDB   1
#define PERSIST_AOF   2

/* --- Replication roles --- */
#define ROLE_MASTER  0
#define ROLE_SLAVE   1

/* ---- Config struct ---- */
typedef struct {
    /* Service */
    int  port;
    int  loglevel;
    int  persist_mode;
    int  network_model;
    int  maxclients;

    /* Replication master switch */
    int  rdma_sync_enabled;
    int  rdma_role;
    char rdma_provider[32];

    /* Master only: RDMA fullsync listen address */
    char rdma_listen_ip[64];
    int  rdma_listen_port;

    /* Slave only: RDMA fullsync connect target */
    char rdma_master_ip[64];
    int  rdma_master_port;

    /* Both: TCP incremental sync port */
    int  incr_port;

    /* XDP hot-path acceleration */
    int  xdp_enabled;
    char xdp_iface[32];

    /* XDP/AF_XDP WAL replication bypass (Phase 11) */
    int  xdp_repl_enabled;
    char xdp_ifname[32];
    int  xdp_driver_mode;  /* 0 = generic (SKB), 1 = driver mode */
    char xdp_repl_ifname[32];
    int  xdp_repl_queue;
    int  xdp_repl_udp_port;
    int  xdp_repl_zero_copy; /* 0 = auto/copy fallback, 1 = require zero-copy */
    int  xdp_repl_batch;
    int  xdp_repl_mtu;

    /* sockmap stream-layer observability (Phase 2) */
    int  sockmap_enabled;

    int  latency_histogram_enabled;

    /* Phase 4 — multi-worker proactor (0 = auto: nproc-1, 1 = single,
     * N = manual).  Auto/manual flag set after config_load. */
    int  threads;
    int  threads_auto;

    /* Phase 5.1 — CPU pinning.
     *   cpu_pin_mode: 0 = off, 1 = auto, 2 = manual list
     *   cpu_pin_list: explicit cores when mode == 2 (e.g. {1,2,3,4})
     *   cpu_pin_count: number of valid entries in cpu_pin_list */
    int  cpu_pin_mode;
    int  cpu_pin_list[64];
    int  cpu_pin_count;
} KvsConfig;

extern KvsConfig kvs_config;

int  config_load(const char *path);
void config_print(void);

/* ---- Runtime feature flags ---- */
#define ENABLE_RDB \
    (kvs_config.persist_mode == PERSIST_RDB  || kvs_config.persist_mode == PERSIST_AOF)
#define ENABLE_AOF \
    (kvs_config.persist_mode == PERSIST_AOF)

/* ---- Logging helper ---- */
#define KVS_LOG(level, fmt, ...) \
    do { \
        if ((level) >= kvs_config.loglevel) { \
            const char *_l = (level) == LOG_DEBUG ? "DEBUG" : \
                             (level) == LOG_INFO  ? "INFO"  : \
                             (level) == LOG_WARN  ? "WARN"  : "ERROR"; \
            printf("[%s] " fmt "\n", _l, ##__VA_ARGS__); \
        } \
    } while (0)

#endif
