/* kvstore_config.c — CONFIG GET / SET */

#include "kvstore_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <errno.h>

/* ── Format helpers ─────────────────────────────────────────────────── */

static int fmt_loglevel(char *out, int cap) {
    int v = __atomic_load_n(&kvs_config.loglevel, __ATOMIC_RELAXED);
    const char *s = (v == LOG_DEBUG) ? "debug" :
                    (v == LOG_INFO)  ? "info"  :
                    (v == LOG_WARN)  ? "warn"  : "error";
    int n = (int)strlen(s);
    if (n > cap) n = cap;
    memcpy(out, s, n);
    return n;
}
static int fmt_maxclients(char *out, int cap) {
    int v = __atomic_load_n(&kvs_config.maxclients, __ATOMIC_RELAXED);
    return snprintf(out, cap, "%d", v);
}
static int fmt_persistence(char *out, int cap) {
    const char *s = (kvs_config.persist_mode == PERSIST_NONE) ? "none" :
                    (kvs_config.persist_mode == PERSIST_RDB)  ? "rdb"  : "aof";
    int n = (int)strlen(s);
    if (n > cap) n = cap;
    memcpy(out, s, n);
    return n;
}
static int fmt_port(char *out, int cap) {
    return snprintf(out, cap, "%d", kvs_config.port);
}
static int fmt_appendonly(char *out, int cap) {
    const char *s = ENABLE_AOF ? "yes" : "no";
    int n = (int)strlen(s);
    if (n > cap) n = cap;
    memcpy(out, s, n);
    return n;
}

/* ── SET handlers ───────────────────────────────────────────────────── */

static int set_loglevel(const char *val, DynBuf *wb) {
    int v = -1;
    if      (strcasecmp(val, "debug") == 0) v = LOG_DEBUG;
    else if (strcasecmp(val, "info")  == 0) v = LOG_INFO;
    else if (strcasecmp(val, "warn")  == 0) v = LOG_WARN;
    else if (strcasecmp(val, "error") == 0) v = LOG_ERROR;
    else {
        dynbuf_lit(wb, "-ERR loglevel must be one of: debug info warn error\r\n");
        return -1;
    }
    __atomic_store_n(&kvs_config.loglevel, v, __ATOMIC_RELAXED);
    return 0;
}
static int set_maxclients(const char *val, DynBuf *wb) {
    char *end; errno = 0;
    long v = strtol(val, &end, 10);
    if (*end != '\0' || errno != 0 || v < 64 || v > 65536) {
        dynbuf_lit(wb, "-ERR maxclients must be an integer in [64, 65536]\r\n");
        return -1;
    }
    __atomic_store_n(&kvs_config.maxclients, (int)v, __ATOMIC_RELAXED);
    return 0;
}

/* ── Param table ────────────────────────────────────────────────────── */

typedef int (*cfg_fmt_fn)(char *out, int cap);
typedef int (*cfg_set_fn)(const char *val, DynBuf *wb);
struct cfg_entry {
    const char *name;
    cfg_fmt_fn  fmt;
    cfg_set_fn  set;       /* NULL → read-only */
};
static const struct cfg_entry g_cfg_table[] = {
    { "loglevel",     fmt_loglevel,    set_loglevel    },
    { "maxclients",   fmt_maxclients,  set_maxclients  },
    { "persistence",  fmt_persistence, NULL },
    { "port",         fmt_port,        NULL },
    { "appendonly",   fmt_appendonly,  NULL },
};
static const int g_cfg_table_n =
    (int)(sizeof(g_cfg_table) / sizeof(g_cfg_table[0]));

static const struct cfg_entry *cfg_lookup(const kvs_token_t *t) {
    for (int i = 0; i < g_cfg_table_n; i++) {
        const char *n = g_cfg_table[i].name;
        int nl = (int)strlen(n);
        if (nl == t->len && strncasecmp(n, t->ptr, (size_t)nl) == 0)
            return &g_cfg_table[i];
    }
    return NULL;
}

dispatch_result_t handle_config(kvs_token_t *tokens, int argc, DynBuf *wb) {
    if (argc < 2) {
        dynbuf_cat(wb,
            "-ERR wrong number of arguments for 'config' command\r\n",
            sizeof("-ERR wrong number of arguments for 'config' command\r\n") - 1);
        return DISPATCH_ERROR;
    }

    kvs_token_t sub = tokens[1];

    if (sub.len == 3 && strncasecmp(sub.ptr, "GET", 3) == 0) {
        if (argc != 3) {
            dynbuf_cat(wb,
                "-ERR wrong number of arguments for 'config|get' command\r\n",
                sizeof("-ERR wrong number of arguments for 'config|get' command\r\n") - 1);
            return DISPATCH_ERROR;
        }
        const struct cfg_entry *e = cfg_lookup(&tokens[2]);
        if (!e) {
            dynbuf_lit(wb, "*0\r\n");
            return DISPATCH_OK;
        }
        char vbuf[256];
        int  vlen = e->fmt(vbuf, (int)sizeof(vbuf));
        if (vlen < 0) vlen = 0;
        if (vlen > (int)sizeof(vbuf)) vlen = (int)sizeof(vbuf);
        dynbuf_lit(wb, "*2\r\n");
        reply_bulk(wb, e->name, (int)strlen(e->name));
        reply_bulk(wb, vbuf, vlen);
        return DISPATCH_OK;
    }

    if (sub.len == 3 && strncasecmp(sub.ptr, "SET", 3) == 0) {
        if (argc != 4) {
            dynbuf_cat(wb,
                "-ERR wrong number of arguments for 'config|set' command\r\n",
                sizeof("-ERR wrong number of arguments for 'config|set' command\r\n") - 1);
            return DISPATCH_ERROR;
        }
        const struct cfg_entry *e = cfg_lookup(&tokens[2]);
        if (!e) {
            dynbuf_cat(wb,
                "-ERR Unknown CONFIG parameter\r\n",
                sizeof("-ERR Unknown CONFIG parameter\r\n") - 1);
            return DISPATCH_ERROR;
        }
        if (!e->set) {
            dynbuf_cat(wb,
                "-ERR parameter is read-only at runtime "
                "(boot-time only — restart with updated .conf)\r\n",
                sizeof("-ERR parameter is read-only at runtime "
                       "(boot-time only — restart with updated .conf)\r\n") - 1);
            return DISPATCH_ERROR;
        }
        char vbuf[192];
        int vlen = tokens[3].len;
        if (vlen >= (int)sizeof(vbuf)) vlen = (int)sizeof(vbuf) - 1;
        memcpy(vbuf, tokens[3].ptr, (size_t)vlen);
        vbuf[vlen] = '\0';
        if (e->set(vbuf, wb) < 0)
            return DISPATCH_ERROR;
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        return DISPATCH_OK;
    }

    dynbuf_cat(wb,
        "-ERR Unknown CONFIG subcommand (supported: GET, SET)\r\n",
        sizeof("-ERR Unknown CONFIG subcommand (supported: GET, SET)\r\n") - 1);
    return DISPATCH_ERROR;
}
