#ifndef SOCKMAP_MANAGER_H
#define SOCKMAP_MANAGER_H

#include <stdint.h>

int  sockmap_manager_init(int port);
void sockmap_manager_stop(void);

/* Aggregate stats counters (any pointer may be NULL). */
void sockmap_manager_stats(uint64_t *added,
                            uint64_t *add_fail,
                            uint64_t *verdict,
                            uint64_t *verdict_pass,
                            uint64_t *parsed,
                            uint64_t *parse_skip);

/* Live sockmap entry count (linear scan). */
uint64_t sockmap_manager_live_socks(void);

/* Per-command stats reporting.  Caller passes a callback invoked once
 * per command bucket.  Returns the number of buckets reported, or -1
 * on error. */
struct sockmap_cmd_report {
    char     name[17];   /* NUL-terminated, uppercased */
    uint64_t count;
    uint64_t bytes;
};

typedef int (*sockmap_cmd_cb)(const struct sockmap_cmd_report *rep, void *uctx);
int sockmap_manager_iter_cmds(sockmap_cmd_cb cb, void *uctx);

/* Per-connection profile reporting (LRU snapshot).  Returns number of
 * profiles reported. */
struct sockmap_conn_report {
    uint64_t cookie;
    uint64_t first_seen_ns;
    uint64_t last_seen_ns;
    uint64_t cmd_count;
    uint64_t bytes_in;
};

typedef int (*sockmap_conn_cb)(const struct sockmap_conn_report *rep, void *uctx);
int sockmap_manager_iter_conns(sockmap_conn_cb cb, void *uctx);

#endif /* SOCKMAP_MANAGER_H */
