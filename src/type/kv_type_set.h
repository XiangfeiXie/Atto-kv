#ifndef __KV_TYPE_SET_H__
#define __KV_TYPE_SET_H__

#include "kv_dict.h"
#include "server.h"
#include "kvstore.h"
#include "protocol.h"
#include "client.h"

/* SET type — unordered set of unique strings.

 * Two encodings:
 *   KV_ENCODING_PACKED — inline packed array for small sets
 *     data[] layout: [type:1][encoding:1][count:4][elem1_len:4][elem1...]...
 *   KV_ENCODING_DICT  — nested dict for large sets
 *     data[] layout: [type:1][encoding:1][kv_dict_t inline]
 *
 * PACKED threshold: 128 elements or 4096 data bytes.
 * Shrink threshold: 64 elements (hysteresis to prevent boundary jitter).
 */

/* Command dispatch for SET-type keys. */
dispatch_result_t set_cmd(kvs_token_t cmd, kv_entry_t *entry,
                          kvs_token_t *args, int argc, DynBuf *wb,
                          const char *msg_raw, int msg_len,
                          client_t *client);

/* Cleanup nested dict entries (call before dict_delete/dict_clear). */
void set_cleanup_entry(kv_entry_t *e);

/* Flatten a DICT-encoded set to packed format for RDB persistence.
 * Allocates *out_data (caller frees). Returns 0 on success. */
int set_flatten_to_packed(kv_entry_t *e, char **out_data, uint32_t *out_len);

/* Rebuild encoding after RDB load: PACKED->DICT if count > 128. */
void set_rebuild_encoding(kv_entry_t *e);

#endif
