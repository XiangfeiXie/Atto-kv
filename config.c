#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>   /* strcasecmp */
#include <ctype.h>

KvsConfig kvs_config;

void config_default(void) {
    kvs_config.port          = 6379;
    kvs_config.loglevel      = LOG_INFO;
    kvs_config.persist_mode  = PERSIST_AOF;
    kvs_config.network_model = NET_REACTOR;
    kvs_config.engine        = ENGINE_RBTREE;

    /* RDMA (libfabric) full-sync — disabled by default */
    kvs_config.rdma_sync_enabled = 0;
    kvs_config.rdma_role         = ROLE_MASTER;
    strncpy(kvs_config.rdma_bind_ip,   "0.0.0.0",   sizeof(kvs_config.rdma_bind_ip)   - 1);
    kvs_config.rdma_bind_port    = 2100;
    strncpy(kvs_config.rdma_master_ip, "127.0.0.1", sizeof(kvs_config.rdma_master_ip) - 1);
    kvs_config.rdma_master_port  = 2100;
    strncpy(kvs_config.rdma_provider,  "tcp",       sizeof(kvs_config.rdma_provider)  - 1);
    kvs_config.incr_port = 6382;
}

/* Strip trailing whitespace / newlines in-place */
static void rtrim(char *s) {
    int len = (int)strlen(s);
    while (len > 0 && (s[len-1] == '\n' || s[len-1] == '\r' ||
                       s[len-1] == ' '  || s[len-1] == '\t'))
        s[--len] = '\0';
}

/* Trim trailing whitespace from val; also strip inline # comment */
static void trim_val(char *val) {
    char *c = strchr(val, '#');
    if (c) *c = '\0';
    rtrim(val);
}

int config_load(const char *path) {
    config_default();

    FILE *fp = fopen(path, "r");
    if (!fp) {
        printf("[CONFIG] '%s' not found, using defaults\n", path);
        return -1;
    } else {
        // 【新增】确认文件成功打开
        printf("[CONFIG] Success: Loading config from '%s'\n", path); 
    }



    char line[256];
    int  lineno = 0;
    while (fgets(line, sizeof(line), fp)) {
        lineno++;
        /* Skip leading whitespace */
        char *p = line;
        while (*p == ' ' || *p == '\t') p++;
        /* Skip blank lines and comment lines */
        if (*p == '#' || *p == '\0' || *p == '\n' || *p == '\r') continue;

        rtrim(p);
        if (*p == '\0') continue;

        char key[64] = {0}, val[192] = {0};
        if (sscanf(p, "%63s %191[^\n]", key, val) < 2) continue;
        trim_val(val);
        if (val[0] == '\0') continue;

        if (strcasecmp(key, "port") == 0) {
            kvs_config.port = atoi(val);

        } else if (strcasecmp(key, "loglevel") == 0) {
            if      (strcasecmp(val, "debug") == 0) kvs_config.loglevel = LOG_DEBUG;
            else if (strcasecmp(val, "info")  == 0) kvs_config.loglevel = LOG_INFO;
            else if (strcasecmp(val, "warn")  == 0) kvs_config.loglevel = LOG_WARN;
            else if (strcasecmp(val, "error") == 0) kvs_config.loglevel = LOG_ERROR;
            else printf("[CONFIG] line %d: unknown loglevel '%s'\n", lineno, val);

        } else if (strcasecmp(key, "persistence") == 0) {
            if      (strcasecmp(val, "none") == 0) kvs_config.persist_mode = PERSIST_NONE;
            else if (strcasecmp(val, "rdb")  == 0) kvs_config.persist_mode = PERSIST_RDB;
            else if (strcasecmp(val, "aof")  == 0) kvs_config.persist_mode = PERSIST_AOF;
            else printf("[CONFIG] line %d: unknown persistence '%s'\n", lineno, val);

        } else if (strcasecmp(key, "engine") == 0) {
            if      (strcasecmp(val, "array")     == 0) kvs_config.engine = ENGINE_ARRAY;
            else if (strcasecmp(val, "rbtree")    == 0) kvs_config.engine = ENGINE_RBTREE;
            else if (strcasecmp(val, "hash")      == 0) kvs_config.engine = ENGINE_HASH;
            else if (strcasecmp(val, "skiptable") == 0) kvs_config.engine = ENGINE_SKIPTABLE;
            else printf("[CONFIG] line %d: unknown engine '%s'\n", lineno, val);

        } else if (strcasecmp(key, "rdma_sync") == 0) {
            if      (strcasecmp(val, "on")  == 0 || strcasecmp(val, "yes") == 0 ||
                     strcasecmp(val, "1")   == 0 || strcasecmp(val, "true") == 0)
                kvs_config.rdma_sync_enabled = 1;
            else if (strcasecmp(val, "off") == 0 || strcasecmp(val, "no")  == 0 ||
                     strcasecmp(val, "0")   == 0 || strcasecmp(val, "false") == 0)
                kvs_config.rdma_sync_enabled = 0;
            else printf("[CONFIG] line %d: unknown rdma_sync '%s'\n", lineno, val);

        } else if (strcasecmp(key, "rdma_role") == 0) {
            if      (strcasecmp(val, "master") == 0) kvs_config.rdma_role = ROLE_MASTER;
            else if (strcasecmp(val, "slave")  == 0) kvs_config.rdma_role = ROLE_SLAVE;
            else printf("[CONFIG] line %d: unknown rdma_role '%s'\n", lineno, val);

        } else if (strcasecmp(key, "rdma_bind_ip") == 0) {
            strncpy(kvs_config.rdma_bind_ip, val, sizeof(kvs_config.rdma_bind_ip) - 1);
            kvs_config.rdma_bind_ip[sizeof(kvs_config.rdma_bind_ip) - 1] = '\0';

        } else if (strcasecmp(key, "rdma_bind_port") == 0) {
            kvs_config.rdma_bind_port = atoi(val);

        } else if (strcasecmp(key, "rdma_master_ip") == 0) {
            strncpy(kvs_config.rdma_master_ip, val, sizeof(kvs_config.rdma_master_ip) - 1);
            kvs_config.rdma_master_ip[sizeof(kvs_config.rdma_master_ip) - 1] = '\0';

        } else if (strcasecmp(key, "rdma_master_port") == 0) {
            kvs_config.rdma_master_port = atoi(val);

        } else if (strcasecmp(key, "rdma_provider") == 0) {
            strncpy(kvs_config.rdma_provider, val, sizeof(kvs_config.rdma_provider) - 1);
            kvs_config.rdma_provider[sizeof(kvs_config.rdma_provider) - 1] = '\0';

        } else if (strcasecmp(key, "incr_port") == 0) {
            kvs_config.incr_port = atoi(val);

        } else if (strcasecmp(key, "network") == 0) {
            if      (strcasecmp(val, "reactor")  == 0) kvs_config.network_model = NET_REACTOR;
            else if (strcasecmp(val, "proactor") == 0) kvs_config.network_model = NET_PROACTOR;
            else if (strcasecmp(val, "ntyco")    == 0) kvs_config.network_model = NET_NTYCO;
            else printf("[CONFIG] line %d: unknown network '%s'\n", lineno, val);

        } else {
            printf("[CONFIG] line %d: unknown key '%s'\n", lineno, key);
        }
    }

    fclose(fp);
    return 0;
}

void config_print(void) {
    static const char *persist_names[] = {"none", "rdb", "aof"};
    static const char *net_names[]     = {"reactor", "proactor", "ntyco"};
    static const char *log_names[]     = {"debug", "info", "warn", "error"};
    static const char *engine_names[]  = {"array", "rbtree", "hash", "skiptable"};

    printf("[CONFIG] port=%d  loglevel=%s  persistence=%s  network=%s  engine=%s\n",
           kvs_config.port,
           log_names[kvs_config.loglevel],
           persist_names[kvs_config.persist_mode],
           net_names[kvs_config.network_model],
           engine_names[kvs_config.engine]);

    printf("[CONFIG] rdma_sync=%s  rdma_role=%s  rdma_provider=%s\n",
           kvs_config.rdma_sync_enabled ? "on" : "off",
           kvs_config.rdma_role == ROLE_MASTER ? "master" : "slave",
           kvs_config.rdma_provider);
    if (kvs_config.rdma_sync_enabled) {
        if (kvs_config.rdma_role == ROLE_MASTER)
            printf("[CONFIG] rdma_bind=%s:%d\n",
                   kvs_config.rdma_bind_ip, kvs_config.rdma_bind_port);
        else
            printf("[CONFIG] rdma_master=%s:%d\n",
                   kvs_config.rdma_master_ip, kvs_config.rdma_master_port);
        printf("[CONFIG] incr_port=%d\n", kvs_config.incr_port);
    }
}
