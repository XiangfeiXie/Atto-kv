#ifndef __KV_TYPE_HASH_H__
#define __KV_TYPE_HASH_H__

#include "kv_dict.h"
#include "server.h"
#include "kvstore.h"
#include "protocol.h"
#include "client.h"

/* HASH type — map of field-value string pairs.

 * Two encodings:
 *   KV_ENCODING_PACKED — inline packed array for small hashes
 *     data[] layout: [type:1][encoding:1][count:4][field1_len:4][field1][val1_len:4][val1]...
 *   KV_ENCODING_DICT  — nested dict for large hashes
 *     data[] layout: [type:1][encoding:1][kv_dict_t inline]
 *     Nested dict entry: key = field name, payload = field value
 *
 * PACKED threshold: 128 fields or 4096 data bytes.
 * Shrink threshold: 64 fields (hysteresis to prevent boundary jitter).
 */

/* Phase 6.1b: pre-computed hash for args[0] passed in from dispatcher
 * to skip redundant kv_hash recomputes inside the handler. */
dispatch_result_t hash_cmd(kvs_token_t cmd, kv_entry_t *entry,
                           uint64_t hash,
                           kvs_token_t *args, int argc, DynBuf *wb,
                           const char *msg_raw, int msg_len,
                           client_t *client);

void hash_cleanup_entry(kv_entry_t *e);

int hash_flatten_to_packed(kv_entry_t *e, char **out_data, uint32_t *out_len);

void hash_rebuild_encoding(kv_entry_t *e);

#endif
