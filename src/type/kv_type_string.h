#ifndef __KV_TYPE_STRING_H__
#define __KV_TYPE_STRING_H__

#include "kv_dict.h"
#include "server.h"
#include "kvstore.h"

/* STRING type — handles SET, GET, DEL, MOD, EXIST, PING, ECHO, FLUSHALL, SAVE.
 *
 * data[] layout:
 *   [type:1 = KV_TYPE_STRING][encoding:1 = KV_ENCODING_RAW][value: vlen-2]
 *
 * This is the default type.  All keys created via SET are STRING type.
 */

/* Command dispatch for STRING-type keys (or new keys that become STRING).
 *
 * Phase 6.1: `hash` is the pre-computed key hash from kvs_dispatch.  `entry`
 * is the pre-found dict entry (or NULL).  Passing both lets us skip the
 * redundant `kv_hash + dict_find` that engine_*_raw used to perform on the
 * hot SET/GET path — one fewer hashtable walk per command. */
dispatch_result_t string_cmd(kvs_token_t cmd, kv_entry_t *entry,
                             uint64_t hash,
                             kvs_token_t *args, int argc, DynBuf *wb,
                             const char *msg_raw, int msg_len);

/* Write-command check for AOF / eBPF probe */
int string_is_write_cmd(kvs_token_t cmd);

/* Build a STRING entry's data[] for dict_add.
   out_data must be at least val_len + 2 bytes. */
void string_build_entry_data(const char *value, int val_len, char *out_data);

/* Public helpers used by multi-key string commands (MSET/MGET/MSETNX) in kvs_dispatch.
 * string_engine_set: mode 0 unconditional, 1 NX (absent only), 2 XX (present only).
 *   Returns 0 ok, 1 precondition failed, -1 error.
 * string_engine_get: returns payload pointer + length; NULL if missing / expired.
 *   Caller must honour enc_out (KV_ENCODING_INT means payload is int64). */
int         string_engine_set(const char *key, uint32_t key_len,
                              const char *val, uint32_t val_len,
                              long long expire, int mode);
const char *string_engine_get(const char *key, uint32_t key_len,
                              uint32_t *vlen_out, uint8_t *enc_out);

#endif
