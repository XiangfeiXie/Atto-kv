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
    char wal_fsync_policy[16];  /* "always", "everysec", or "no" */

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

    int  fullsync_timeout;      /* Fullsync timeout (ms) */
    int  fullsync_max_retries;  /* Max retry count */

    /* Latency histogram for performance monitoring */
    int  latency_histogram_enabled;
} KvsConfig;

extern KvsConfig kvs_config;

int  config_load(const char *path);
void config_print(void);

/* ---- Runtime feature flags ---- */
#define ENABLE_RDB \
    (kvs_config.persist_mode == PERSIST_RDB  || kvs_config.persist_mode == PERSIST_AOF)
#define ENABLE_AOF \
    (kvs_config.persist_mode == PERSIST_AOF)

#endif
