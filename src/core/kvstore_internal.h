/* kvstore_internal.h — declarations shared across kvstore_*.c */

#ifndef KVSTORE_INTERNAL_H
#define KVSTORE_INTERNAL_H

#include "kvstore.h"
#include "kv_type_string.h"
#include "kv_type_hash.h"
#include "config.h"
#include "wal_persist.h"

#include <stdbool.h>
#include <stdatomic.h>

/* ── RDB changes counter (defined in persist_rdb.c) ── */
extern uint64_t g_rdb_changes;

/* ── from kvstore.c ── */

/* cached time */
void update_cached_time(void);

/* utility */
int  token_to_ll(kvs_token_t t, long long *out);
void reply_integer(DynBuf *wb, long long v);
void reply_bulk(DynBuf *wb, const char *s, int len);
void entry_cleanup_by_type(kv_entry_t *e);

/* write journal — inline for hot-path (threshold check + single memcpy) */
static inline void kvs_write_journal_append(const char *msg, int length) {
    if (!msg || length <= 0) return;
    if (ENABLE_RDB) g_rdb_changes++;
    if (ENABLE_AOF) kvs_wal_persist_append_raw(msg, (uint32_t)length);
}

/* ── from kvstore_expire.c ── */

void kvs_start_expire_thread(void);
void kvs_stop_expire_thread(void);
int  enqueue_expired_key(const char *key, uint32_t key_len, uint64_t hash);
void flush_expire_queue(void);
_Atomic uint64_t *kvs_expired_keys_ptr(void);

/* ── from kvstore_config.c ── */
dispatch_result_t handle_config(kvs_token_t *tokens, int argc, DynBuf *wb);

/* ── from kvstore_info.c ── */
dispatch_result_t handle_info(kvs_token_t *tokens, int argc, DynBuf *wb);

#endif /* KVSTORE_INTERNAL_H */
