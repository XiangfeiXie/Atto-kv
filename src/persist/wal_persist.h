#ifndef KVS_WAL_PERSIST_H
#define KVS_WAL_PERSIST_H

#include <stdint.h>
#include <string.h>

/* ── Tunables ── */
#define KVS_JOURNAL_MAX_PAYLOAD  (1024u * 1024u)

/* ── Public API ── */

int  kvs_wal_persist_start(void);

/* Append raw RESP bytes to ring buffer — lock-free, single-threaded.
 * Mirror of kvs_persist.c:kvs_aof_append: threshold check + single memcpy.
 * Called from the proactor event-loop dispatch path. */
int  kvs_wal_persist_append_raw(const char *payload, uint32_t len);

void kvs_wal_persist_stop(void);

/* Set fsync policy: "always", "everysec", or "no" */
void kvs_wal_set_fsync_policy(const char *policy);

/* Periodic maintenance — call from event loop (update_cached_time).
 * Reaps completions, flushes stale partial data, triggers async fsync. */
void kvs_wal_periodic(void);

/* Reap in-flight io_uring write completions (frees ring slots). */
void kvs_wal_reap_completions(void);

#endif
