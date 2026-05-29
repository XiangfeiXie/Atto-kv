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

    kvs_config.sockmap_enabled = 0;

    kvs_config.latency_histogram_enabled = 0;

    /* 0 = auto: max(1, nproc - 1).  Resolved in config_postprocess(). */
    kvs_config.threads = 0;
    kvs_config.threads_auto = 0;

    /* Phase 5.1 — cpu_pin defaults to auto (1).  Explicit `cpu_pin off`
     * disables affinity.  Manual list parsed at load time. */
    kvs_config.cpu_pin_mode  = 1;
    kvs_config.cpu_pin_count = 0;
}

/* Best-effort cgroup v2 cpu.max read.  Returns -1 if unbounded /
 * unavailable; otherwise the integer cpu count implied by quota/period. */
static int read_cgroup_cpu_quota(void) {
    FILE *fp = fopen("/sys/fs/cgroup/cpu.max", "r");
    if (!fp) return -1;
    char tok[32]; long period = 0;
    if (fscanf(fp, "%31s %ld", tok, &period) != 2 || period <= 0) {
        fclose(fp); return -1;
    }
    fclose(fp);
    if (strcmp(tok, "max") == 0) return -1;          /* no limit */
    long quota = strtol(tok, NULL, 10);
    if (quota <= 0) return -1;
    int cpus = (int)((quota + period - 1) / period);  /* ceil */
    return cpus > 0 ? cpus : 1;
}

/* Resolve sentinel values and emit sanity warnings.  Called once after
 * the .conf file has been parsed.
 *
 * threads value semantics (Phase 7.2-deploy):
 *    threads = 0   → auto: nproc - 1 (leave 1 core for IRQ / system)
 *    threads > 0   → explicit worker count
 *    threads < 0   → "auto-leave |N|": max(1, nproc - |N|)
 *                    e.g. threads = -2 on a 6c box → 4 workers
 *                    e.g. threads = -4 on a 6c box → 2 workers (laptop-friendly)
 * Cgroup quota: if present and tighter than nproc, the auto and
 * auto-leave paths cap at quota.  Explicit (positive) values are
 * honoured as written but a WARN fires if they exceed the quota. */
static void config_postprocess(void) {
    long nproc = sysconf(_SC_NPROCESSORS_ONLN);
    if (nproc < 1) nproc = 1;

    /* Cgroup-aware capacity: if /sys/fs/cgroup/cpu.max says we have
     * 4 cores but the host has 32, the effective ceiling is 4.
     * Container deployments need this to size workers correctly. */
    int cgroup_cpus = read_cgroup_cpu_quota();
    long effective = (cgroup_cpus > 0 && cgroup_cpus < nproc)
                       ? (long)cgroup_cpus : nproc;

    if (kvs_config.threads == 0) {
        /* Auto: leave 1 core for OS / IRQ / softirq. */
        kvs_config.threads_auto = 1;
        kvs_config.threads = (int)((effective > 1) ? (effective - 1) : 1);
    } else if (kvs_config.threads < 0) {
        /* Auto-leave: "reserve |N| cores for everything else".
         * Used for shared servers and dev laptops where you want the
         * KV process to be a polite tenant rather than greedy. */
        int reserve = -kvs_config.threads;
        kvs_config.threads_auto = 1;
        long target = effective - reserve;
        if (target < 1) target = 1;
        kvs_config.threads = (int)target;
    }
    /* else: explicit positive value, honoured as-is */

    if (kvs_config.threads > nproc) {
        printf("[CONFIG] WARN: threads=%d > online cpus=%ld; "
               "workers will oversubscribe cores\n",
               kvs_config.threads, nproc);
    }
    if (kvs_config.threads * 8 > KV_NR_SHARDS) {
        printf("[CONFIG] WARN: threads=%d may contend on %d shards "
               "(rule-of-thumb: shards >= 8 * threads)\n",
               kvs_config.threads, KV_NR_SHARDS);
    }
    if (cgroup_cpus > 0 && kvs_config.threads > cgroup_cpus) {
        printf("[CONFIG] WARN: threads=%d exceeds cgroup cpu.max=%d\n",
               kvs_config.threads, cgroup_cpus);
    }
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

        } else if (strcasecmp(key, "latency_histogram") == 0) {
            int v = parse_bool(val, lineno, "latency_histogram");
            if (v >= 0) kvs_config.latency_histogram_enabled = v;

        /* ---- sockmap (Phase 2) ---- */
        } else if (strcasecmp(key, "sockmap") == 0) {
            int v = parse_bool(val, lineno, "sockmap");
            if (v >= 0) kvs_config.sockmap_enabled = v;

        /* ---- Phase 4: multi-worker proactor ---- */
        } else if (strcasecmp(key, "threads") == 0) {
            /* 0    = auto (nproc - 1)
             * > 0  = explicit worker count
             * < 0  = "auto-leave |N|": shared-server / laptop-friendly
             * Resolved in config_postprocess(). */
            parse_int(val, lineno, "threads", -32, 64, &kvs_config.threads);

        /* ---- Phase 5.1: CPU pinning ----
         *   cpu_pin off            -> mode=0
         *   cpu_pin auto           -> mode=1 (default)
         *   cpu_pin 1,2,3,4        -> mode=2, list parsed
         */
        } else if (strcasecmp(key, "cpu_pin") == 0) {
            if (strcasecmp(val, "off") == 0) {
                kvs_config.cpu_pin_mode = 0;
            } else if (strcasecmp(val, "auto") == 0) {
                kvs_config.cpu_pin_mode = 1;
            } else {
                kvs_config.cpu_pin_mode  = 2;
                kvs_config.cpu_pin_count = 0;
                char *dup = strdup(val); if (!dup) continue;
                char *save = NULL;
                for (char *tok = strtok_r(dup, ",", &save); tok;
                     tok = strtok_r(NULL, ",", &save)) {
                    while (*tok == ' ' || *tok == '\t') tok++;
                    if (!*tok) continue;
                    int v = atoi(tok);
                    if (v < 0 || v > 1023) continue;
                    if (kvs_config.cpu_pin_count <
                        (int)(sizeof(kvs_config.cpu_pin_list) /
                              sizeof(kvs_config.cpu_pin_list[0])))
                        kvs_config.cpu_pin_list[kvs_config.cpu_pin_count++] = v;
                }
                free(dup);
                if (kvs_config.cpu_pin_count == 0) {
                    printf("[CONFIG] line %d: cpu_pin list empty, falling back to auto\n",
                           lineno);
                    kvs_config.cpu_pin_mode = 1;
                }
            }

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

    if (kvs_config.sockmap_enabled)
        printf("  sockmap       : enabled (stream-layer observability)\n");
    if (kvs_config.latency_histogram_enabled)
        printf("  Latency hist  : sampled dispatch histogram enabled\n");
    if (kvs_config.threads > 1) {
        long nproc = sysconf(_SC_NPROCESSORS_ONLN);
        printf("  Workers       : %d %s (nproc=%ld, %d shards, SO_REUSEPORT)\n",
               kvs_config.threads,
               kvs_config.threads_auto ? "[auto]" : "[manual]",
               nproc, KV_NR_SHARDS);
    } else {
        printf("  Workers       : 1 (single-thread proactor)\n");
    }
    if (kvs_config.threads >= 1) {
        if (kvs_config.cpu_pin_mode == 0) {
            printf("  CPU pin       : off\n");
        } else if (kvs_config.cpu_pin_mode == 1) {
            printf("  CPU pin       : auto (worker i → core (i+1) mod nproc)\n");
        } else {
            printf("  CPU pin       : manual [");
            for (int i = 0; i < kvs_config.cpu_pin_count; i++)
                printf("%s%d", i ? "," : "", kvs_config.cpu_pin_list[i]);
            printf("]\n");
        }
    }
    printf("========================================\n\n");
}
