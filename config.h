#ifndef __KVS_CONFIG_H__
#define __KVS_CONFIG_H__

/* =========================================================
 * Runtime configuration module
 * Loaded once at startup from a .conf file; no recompile needed.
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
#define PERSIST_BOTH  3

/* --- Replication roles --- */
#define ROLE_MASTER  0
#define ROLE_SLAVE   1

/* --- Storage engines --- */
#define ENGINE_ARRAY     0
#define ENGINE_RBTREE    1
#define ENGINE_HASH      2
#define ENGINE_SKIPTABLE 3

/* ---- Config struct ---- */
typedef struct {
    int  port;            /* listening port, default 6379               */
    int  loglevel;        /* LOG_DEBUG / INFO / WARN / ERROR            */
    int  persist_mode;    /* PERSIST_NONE / RDB / AOF / BOTH            */
    int  network_model;   /* NET_REACTOR / NET_PROACTOR / NET_NTYCO     */
    int  engine;          /* ENGINE_ARRAY/RBTREE/HASH/SKIPTABLE          */

    /* ---- RDMA + eBPF 主从同步总开关 ---- */
    int  rdma_sync_enabled;   /* 0: disable; 1: enable (RDMA 全量 + eBPF 增量) */

    /* ---- RDMA-based full-sync (libfabric / Soft-RoCE) ---- */
    int  rdma_repl_enabled;   /* 子开关: 0: disable; 1: enable (仅当 rdma_sync 启用时生效) */
    int  rdma_role;           /* ROLE_MASTER / ROLE_SLAVE                */
    char rdma_bind_ip[64];    /* master: local bind IP (e.g. 0.0.0.0)    */
    int  rdma_bind_port;      /* master: libfabric listen port           */
    char rdma_master_ip[64];  /* slave:  master IP to connect            */
    int  rdma_master_port;    /* slave:  master port to connect          */
    char rdma_provider[32];   /* libfabric provider: verbs | tcp | ...   */

    /* ---- Incremental sync TCP (代替增量同步的 RDMA) ---- */
    int  incr_port;            /* ebpf incr TCP listener port, default 6382 */
} KvsConfig;

extern KvsConfig kvs_config;

int  config_load(const char *path);  /* load from file; returns 0 OK, -1 missing */
void config_default(void);           /* fill all fields with safe defaults        */
void config_print(void);             /* print active config to stdout             */

/* ---- Runtime feature flags (replace old compile-time macros) ---- */
#define ENABLE_RDB \
    (kvs_config.persist_mode == PERSIST_RDB  || kvs_config.persist_mode == PERSIST_BOTH)
#define ENABLE_AOF \
    (kvs_config.persist_mode == PERSIST_AOF  || kvs_config.persist_mode == PERSIST_BOTH)

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
