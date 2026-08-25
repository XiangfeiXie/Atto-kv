/* kvstore_internal.h — declarations shared across kvstore_*.c
 *
 * Include order: this header pulls in everything needed by command implementations
 * (kvstore_dispatch, kvstore_config, kvstore_info, kvstore_expire).
 *
 * Do NOT include this from network / persist / repl layers — those should include
 * kvstore.h directly. */

#ifndef KVSTORE_INTERNAL_H
#define KVSTORE_INTERNAL_H

#include "kvstore.h"
#include "persist_aof.h"

/* ── RDB changes counter (defined in persist_rdb.c) ── */
extern uint64_t g_rdb_changes;

/* ── from kvstore.c ── */

/* utility */
int  token_to_ll(kvs_token_t t, long long *out);
void reply_integer(DynBuf *wb, long long v);
void reply_bulk(DynBuf *wb, const char *s, int len);

/* write journal — inline for hot-path (threshold check + single memcpy) */
static inline void kvs_write_journal_append(const char *msg, int length) {
    if (!msg || length <= 0) return;
    if (ENABLE_RDB) g_rdb_changes++;
    if (ENABLE_AOF) kvs_aof_persist_append_raw(msg, (uint32_t)length);
}

/* Incremental replication hook — called on every successful write command.
 * Appends to repl_backlog and propagates to streaming slaves.
 * Defined here so it can be called from the same sites as
 * kvs_write_journal_append(). */
#include "repl_incr.h"
static inline void kvs_repl_incr_hook(const char *msg, int length) {
    extern int g_in_reload;
    if (!msg || length <= 0 || g_in_reload) return;
    /* The backlog/propagation only exist to serve incremental sync.  When
     * incr_sync is off there is no replication listener and no slave can
     * connect, so maintaining the backlog is pure waste on the write hot
     * path.  Gate it out so the "no sync" baseline is genuinely sync-free. */
    if (!kvs_config.incr_sync_enabled) return;
    repl_backlog_append(msg, length);
    repl_incr_propagate(msg, length);
}

/* ── from kvstore_expire.c ── */

_Atomic uint64_t *kvs_expired_keys_ptr(void);
void active_expire_cycle(void);   /* periodic sampling in event loop */

/* ── from kvstore_config.c ── */
dispatch_result_t handle_config(kvs_token_t *tokens, int argc, DynBuf *wb);

/* ── from kvstore_info.c ── */
dispatch_result_t handle_info(kvs_token_t *tokens, int argc, DynBuf *wb);

#endif /* KVSTORE_INTERNAL_H */
