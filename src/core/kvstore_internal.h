/* kvstore_internal.h — declarations shared across kvstore_*.c */

#ifndef KVSTORE_INTERNAL_H
#define KVSTORE_INTERNAL_H

#include "kvstore.h"
#include "kvstore_macros.h"
#include "kv_type_string.h"
#include "kv_type_hash.h"
#include "shared.h"
#include "config.h"

#include <stdbool.h>
#include <stdatomic.h>

/* ── from kvstore.c ── */

/* cached time */
void update_cached_time(void);

/* utility */
int  token_to_ll(kvs_token_t t, long long *out);
int  token_eq_ci(kvs_token_t t, const char *lit);
void reply_integer(DynBuf *wb, long long v);
void reply_bulk(DynBuf *wb, const char *s, int len);
void entry_cleanup_by_type(kv_entry_t *e);

/* write journal */
void kvs_write_journal_append(const char *msg, int length);

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
