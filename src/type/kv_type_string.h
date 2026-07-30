#ifndef __KV_TYPE_STRING_H__
#define __KV_TYPE_STRING_H__

#include "kv_dict.h"
#include "protocol.h"

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

/* Pre-computed-hash variants for the hot path.
 * Caller already did kv_hash+dict_find; pass the result here to
 * avoid redundant lookup -- saves ~3% CPU on benchmark workloads. */
int string_engine_set_pre(const char *key, uint32_t key_len,
                          const char *val, uint32_t val_len,
                          long long expire, int mode,
                          uint64_t hash, kv_entry_t *existing);
const char *string_engine_get_pre(kv_entry_t *entry,
                                  uint32_t *vlen_out,
                                  uint8_t *enc_out);

#endif
