/* sockmap_kvs.h — shared between sockmap BPF kernel program and userspace */
#ifndef SOCKMAP_KVS_H
#define SOCKMAP_KVS_H

#ifndef __bpf__
# include <linux/types.h>
#endif

/* Capacity of the sockhash map.  Must comfortably exceed `maxclients`. */
#define SOCKMAP_MAX_ENTRIES 65536

/* ── Stats indices (BPF_MAP_TYPE_ARRAY of __u64) ──────────────────────── */
#define SOCKMAP_STAT_ADDED        0  /* sockets inserted by sockops            */
#define SOCKMAP_STAT_ADD_FAIL     1  /* bpf_sock_hash_update non-zero          */
#define SOCKMAP_STAT_VERDICT      2  /* total stream_verdict invocations       */
#define SOCKMAP_STAT_VERDICT_PASS 3  /* skbs passed up to userspace            */
#define SOCKMAP_STAT_PARSED       4  /* skbs where RESP array header parsed OK */
#define SOCKMAP_STAT_PARSE_SKIP   5  /* skbs that were not RESP arrays         */
#define SOCKMAP_STAT_MAX          6

/* ── Per-command counters (BPF_MAP_TYPE_HASH) ─────────────────────────── */

#define SOCKMAP_CMD_NAME_MAX 16
#define SOCKMAP_CMD_MAX_ENTRIES 64    /* upper bound on distinct cmd names */

struct sockmap_cmd_key {
    __u8  len;                              /* 1..SOCKMAP_CMD_NAME_MAX */
    __u8  name[SOCKMAP_CMD_NAME_MAX];       /* uppercased ASCII        */
};

struct sockmap_cmd_stats {
    __u64 count;        /* number of invocations seen      */
    __u64 bytes;        /* total skb bytes attributed here */
};

/* ── Per-connection profile (BPF_MAP_TYPE_LRU_HASH keyed by cookie) ───── */

struct sockmap_conn_profile {
    __u64 first_seen_ns;
    __u64 last_seen_ns;
    __u64 cmd_count;     /* approx — incremented once per parsed-RESP skb */
    __u64 bytes_in;
};

#endif /* SOCKMAP_KVS_H */
