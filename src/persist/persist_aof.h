#ifndef KVS_PERSIST_AOF_H
#define KVS_PERSIST_AOF_H

#include <stdint.h>

/* ── Tunables ── */
#define KVS_JOURNAL_MAX_PAYLOAD  (1024u * 1024u)

/* ── Public API ── */

int  kvs_aof_persist_start(void);

/* Append raw RESP bytes to the ring buffer — lock-free, single-threaded. */
int  kvs_aof_persist_append_raw(const char *payload, uint32_t len);

void kvs_aof_persist_stop(void);

/* Set fsync policy: "always", "everysec", or "no" */
void kvs_aof_set_fsync_policy(const char *policy);

/* Periodic maintenance — call from event loop (update_cached_time). */
void kvs_aof_periodic(void);

/* ── AOF rewrite (BGREWRITEAOF) ── */
int  kvs_aof_rewrite_bg(void);
void kvs_aof_auto_rewrite_check(void);

#endif
