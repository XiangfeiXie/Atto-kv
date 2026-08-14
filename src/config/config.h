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

    /* Replication: RDMA fullsync (verbs/RoCE) */
    int  rdma_sync_enabled;
    int  rdma_role;              /* ROLE_MASTER=0 / ROLE_SLAVE=1 */

    /* 传输方式: "sendfile" (TCP sendfile 零拷贝) | "rdma" (RDMA verbs) */
    char fullsync_transport[16];

    /* Master's TCP service port — slave sends FULLSYNC here.
     * Separated from rdma_* so handshake and data use different ports. */
    int  master_port;

    /* Master RDMA CM listener: rdma_listen_ip:rdma_listen_port */
    char rdma_listen_ip[64];
    int  rdma_listen_port;

    /* Slave RDMA data path: rdma_master_ip:rdma_master_port
     * (TCP sendfile uses rdma_master_port + 1).
     * FULLSYNC handshake uses rdma_master_ip + master_port (separate). */
    char rdma_master_ip[64];
    int  rdma_master_port;

    int  fullsync_timeout;      /* Fullsync timeout (ms) */
    int  fullsync_max_retries;  /* Max retry count */

    /* Latency histogram for performance monitoring */
    int  latency_histogram_enabled;

    /* Expiration (lazy + queue + background thread).  Turn off with
     *   expire off
     * in conf to eliminate all expire-related overhead — useful for
     * clean pipeline benchmarks where no keys carry a TTL. */
    int  expire_enabled;

    /* --- Incremental replication --- */
    int  incr_sync_enabled;      /* incr_sync on|off */
    int  incr_repl_port;         /* dedicated port for incremental sync, default 2001 */
} KvsConfig;

extern KvsConfig kvs_config;

int  config_load(const char *path);
void config_print(void);

/* ---- Runtime feature flags ---- */
#define ENABLE_RDB \
    (kvs_config.persist_mode == PERSIST_RDB  || kvs_config.persist_mode == PERSIST_AOF)
#define ENABLE_AOF \
    (kvs_config.persist_mode == PERSIST_AOF)
#define ENABLE_EXPIRE \
    (kvs_config.expire_enabled)

#endif
