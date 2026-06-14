#include "config.h"
#include "kv_dict.h"     /* KV_NR_SHARDS */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <errno.h>
#include <unistd.h>      /* sysconf */
#include <sys/stat.h>

KvsConfig kvs_config;

/* ================================================================
 * Defaults — safe values, overridden by .conf
 * ================================================================ */

static void config_default(void) {
    kvs_config.port          = 6379;
    kvs_config.loglevel      = LOG_INFO;
    kvs_config.persist_mode  = PERSIST_AOF;
    kvs_config.network_model = NET_REACTOR;
    kvs_config.maxclients    = 10000;
    strncpy(kvs_config.wal_fsync_policy, "everysec", sizeof(kvs_config.wal_fsync_policy) - 1);

    kvs_config.rdma_sync_enabled = 0;
    kvs_config.rdma_role         = ROLE_MASTER;
    kvs_config.rdma_listen_port  = 2100;
    kvs_config.rdma_master_port  = 2100;

    strncpy(kvs_config.rdma_provider,  "tcp",
            sizeof(kvs_config.rdma_provider) - 1);
    strncpy(kvs_config.rdma_listen_ip, "0.0.0.0",
            sizeof(kvs_config.rdma_listen_ip) - 1);
    strncpy(kvs_config.rdma_master_ip, "127.0.0.1",
            sizeof(kvs_config.rdma_master_ip) - 1);

    /* TCP + eBPF incremental replication (Phase 9.1) */
    kvs_config.incr_repl_enabled = 0;
    kvs_config.incr_repl_port = 6483;  /* Default TCP port for replication */
    kvs_config.incr_repl_master_port = 6483;  /* Slave: master's port to connect */

    kvs_config.fullsync_timeout = 3600000;  /* 1 hour */
    kvs_config.fullsync_max_retries = 3;

    kvs_config.latency_histogram_enabled = 0;
}

/* Resolve sentinel values and emit sanity warnings.  Called once after
 * the .conf file has been parsed. */
static void config_postprocess(void) {
    /* No thread/CPU configuration needed for single-threaded mode */
}

/* ================================================================
 * Helpers
 * ================================================================ */

static void rtrim(char *s) {
    int len = (int)strlen(s);
    while (len > 0 && (s[len-1] == '\n' || s[len-1] == '\r' ||
                       s[len-1] == ' '  || s[len-1] == '\t'))
        s[--len] = '\0';
}

static void trim_val(char *val) {
    char *c = strchr(val, '#');
    if (c) *c = '\0';
    rtrim(val);
}

static int parse_int(const char *val, int lineno, const char *key,
                     long lo, long hi, int *out) {
    char *end;
    errno = 0;
    long v = strtol(val, &end, 10);
    if (*end != '\0' || errno != 0 || v < lo || v > hi) {
        printf("[CONFIG] line %d: invalid value '%s' for '%s' "
               "(expected integer %ld-%ld)\n", lineno, val, key, lo, hi);
        return -1;
    }
    *out = (int)v;
    return 0;
}

static int parse_bool(const char *val, int lineno, const char *key) {
    if (strcasecmp(val, "on")   == 0 || strcasecmp(val, "yes") == 0 ||
        strcasecmp(val, "1")    == 0 || strcasecmp(val, "true") == 0)
        return 1;
    if (strcasecmp(val, "off")  == 0 || strcasecmp(val, "no")  == 0 ||
        strcasecmp(val, "0")    == 0 || strcasecmp(val, "false") == 0)
        return 0;
    printf("[CONFIG] line %d: bad boolean '%s' for %s\n", lineno, val, key);
    return -1;
}

/* ================================================================
 * Config file parser
 * ================================================================ */

int config_load(const char *path) {
    config_default();

    FILE *fp = fopen(path, "r");
    if (!fp) {
        printf("[CONFIG] '%s' not found, using defaults\n", path);
        config_postprocess();
        return -1;
    }
    printf("[CONFIG] Loading %s\n", path);

    char line[256];
    int  lineno = 0;

    while (fgets(line, sizeof(line), fp)) {
        lineno++;
        /* Detect lines longer than the buffer: fgets filled the buffer and
         * the last stored char is not '\n'. Drain the rest and warn. */
        if (strlen(line) == sizeof(line) - 1 && line[sizeof(line) - 2] != '\n') {
            printf("[CONFIG] line %d: line too long (>%d chars), skipped\n",
                   lineno, (int)(sizeof(line) - 2));
            int c;
            while ((c = fgetc(fp)) != EOF && c != '\n') {}
            continue;
        }

        char *p = line;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '#' || *p == '\0' || *p == '\n' || *p == '\r')
            continue;

        rtrim(p);
        if (*p == '\0') continue;

        char key[64] = {0}, val[192] = {0};
        if (sscanf(p, "%63s %191[^\n]", key, val) < 2) continue;
        trim_val(val);
        if (val[0] == '\0') continue;

        /* ---- Service ---- */
        if (strcasecmp(key, "port") == 0) {
            parse_int(val, lineno, "port", 1, 65535, &kvs_config.port);

        } else if (strcasecmp(key, "loglevel") == 0) {
            if      (strcasecmp(val, "debug") == 0) kvs_config.loglevel = LOG_DEBUG;
            else if (strcasecmp(val, "info")  == 0) kvs_config.loglevel = LOG_INFO;
            else if (strcasecmp(val, "warn")  == 0) kvs_config.loglevel = LOG_WARN;
            else if (strcasecmp(val, "error") == 0) kvs_config.loglevel = LOG_ERROR;
            else printf("[CONFIG] line %d: unknown loglevel '%s'\n", lineno, val);

        } else if (strcasecmp(key, "maxclients") == 0) {
            parse_int(val, lineno, "maxclients", 64, 65536, &kvs_config.maxclients);

        } else if (strcasecmp(key, "network") == 0) {
            if      (strcasecmp(val, "reactor")  == 0) kvs_config.network_model = NET_REACTOR;
            else if (strcasecmp(val, "proactor") == 0) kvs_config.network_model = NET_PROACTOR;
            else if (strcasecmp(val, "ntyco")    == 0) kvs_config.network_model = NET_NTYCO;
            else printf("[CONFIG] line %d: unknown network '%s'\n", lineno, val);

        } else if (strcasecmp(key, "persistence") == 0) {
            if      (strcasecmp(val, "none") == 0) kvs_config.persist_mode = PERSIST_NONE;
            else if (strcasecmp(val, "rdb")  == 0) kvs_config.persist_mode = PERSIST_RDB;
            else if (strcasecmp(val, "aof")  == 0) kvs_config.persist_mode = PERSIST_AOF;
            else printf("[CONFIG] line %d: unknown persistence '%s'\n", lineno, val);

        } else if (strcasecmp(key, "wal_fsync_policy") == 0) {
            strncpy(kvs_config.wal_fsync_policy, val,
                    sizeof(kvs_config.wal_fsync_policy) - 1);

        /* ---- Replication ---- */
        } else if (strcasecmp(key, "rdma_sync") == 0) {
            int v = parse_bool(val, lineno, "rdma_sync");
            if (v >= 0) kvs_config.rdma_sync_enabled = v;

        } else if (strcasecmp(key, "rdma_role") == 0) {
            if      (strcasecmp(val, "master") == 0) kvs_config.rdma_role = ROLE_MASTER;
            else if (strcasecmp(val, "slave")  == 0) kvs_config.rdma_role = ROLE_SLAVE;
            else printf("[CONFIG] line %d: unknown rdma_role '%s'\n", lineno, val);

        } else if (strcasecmp(key, "rdma_provider") == 0) {
            strncpy(kvs_config.rdma_provider, val,
                    sizeof(kvs_config.rdma_provider) - 1);

        } else if (strcasecmp(key, "rdma_listen_ip") == 0) {
            strncpy(kvs_config.rdma_listen_ip, val,
                    sizeof(kvs_config.rdma_listen_ip) - 1);

        } else if (strcasecmp(key, "rdma_listen_port") == 0) {
            parse_int(val, lineno, "rdma_listen_port", 1, 65535, &kvs_config.rdma_listen_port);

        } else if (strcasecmp(key, "rdma_master_ip") == 0) {
            strncpy(kvs_config.rdma_master_ip, val,
                    sizeof(kvs_config.rdma_master_ip) - 1);

        } else if (strcasecmp(key, "rdma_master_port") == 0) {
            parse_int(val, lineno, "rdma_master_port", 1, 65535, &kvs_config.rdma_master_port);

        /* ---- TCP + eBPF incremental replication (Phase 9.1) ---- */
        } else if (strcasecmp(key, "incr_repl") == 0) {
            int v = parse_bool(val, lineno, "incr_repl");
            if (v >= 0) kvs_config.incr_repl_enabled = v;

        } else if (strcasecmp(key, "incr_repl_port") == 0) {
            parse_int(val, lineno, "incr_repl_port", 1, 65535, &kvs_config.incr_repl_port);

        } else if (strcasecmp(key, "incr_repl_master_port") == 0) {
            parse_int(val, lineno, "incr_repl_master_port", 1, 65535, &kvs_config.incr_repl_master_port);

        } else if (strcasecmp(key, "fullsync_timeout") == 0) {
            parse_int(val, lineno, "fullsync_timeout", 1000, 7200000, &kvs_config.fullsync_timeout);

        } else if (strcasecmp(key, "fullsync_max_retries") == 0) {
            parse_int(val, lineno, "fullsync_max_retries", 0, 10, &kvs_config.fullsync_max_retries);

        } else if (strcasecmp(key, "latency_histogram") == 0) {
            int v = parse_bool(val, lineno, "latency_histogram");
            if (v >= 0) kvs_config.latency_histogram_enabled = v;

        } else {
            printf("[CONFIG] line %d: unknown key '%s'\n", lineno, key);
        }
    }

    fclose(fp);
    config_postprocess();
    return 0;
}

/* ================================================================
 * Pretty-print active configuration
 * ================================================================ */

void config_print(void) {
    static const char *persist[] = {"none", "rdb", "aof"};
    static const char *netmod[]  = {"reactor", "proactor", "ntyco"};
    static const char *lognames[]= {"debug", "info", "warn", "error"};

    const char *role = (kvs_config.rdma_role == ROLE_MASTER) ? "MASTER" : "SLAVE";

    printf("\n"
           "========================================\n"
           "  KVStore 10.0 — Configuration\n"
           "========================================\n"
           "  Service   : 0.0.0.0:%-5d  loglevel=%s  maxclients=%d\n"
           "  Engine    : dict        persistence=%s\n"
           "  Network   : %s\n",
           kvs_config.port,
           lognames[kvs_config.loglevel],
           kvs_config.maxclients,
           persist[kvs_config.persist_mode],
           netmod[kvs_config.network_model]);

    if (!kvs_config.rdma_sync_enabled) {
        printf("\n"
               "  Replication [DISABLED]\n");
    } else {
        printf("\n"
               "  Replication [%s]\n"
               "    RDMA fullsync : %s://%s:%d\n"
               "    Incremental  : TCP + eBPF tcp://0.0.0.0:%d\n",
               role,
               kvs_config.rdma_provider,
               (kvs_config.rdma_role == ROLE_MASTER)
                   ? kvs_config.rdma_listen_ip
                   : kvs_config.rdma_master_ip,
               (kvs_config.rdma_role == ROLE_MASTER)
                   ? kvs_config.rdma_listen_port
                   : kvs_config.rdma_master_port,
               kvs_config.incr_repl_port);
    }

    if (kvs_config.latency_histogram_enabled)
        printf("  Latency hist  : sampled dispatch histogram enabled\n");
    printf("  Workers       : 1 (single-thread proactor)\n");
    printf("  CPU pin       : off (single-threaded mode)\n");
    printf("========================================\n\n");
}
