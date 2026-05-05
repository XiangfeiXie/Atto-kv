#ifndef __KV_TYPE_LIST_H__
#define __KV_TYPE_LIST_H__

#include "kv_dict.h"
#include "server.h"
#include "kvstore.h"

/* LIST type — double-ended sequence of strings.
 *
 * Two encodings:
 *   KV_ENCODING_PACKED    — inline packed array for small lists
 *     data[] layout: [type:1][encoding:1][count:4][elem1_len:4][elem1...]...
 *   KV_ENCODING_QUICKLIST — linked list of packed nodes for large lists
 *     data[] layout: [type:1][encoding:1][total_count:4][node_max:4][head:8][tail:8][node_count:4]
 *
 * Packed threshold: 128 elements or 4096 data bytes.
 * Quicklist node capacity: 64 elements or 4096 bytes per node.
 */

/* Command dispatch for LIST-type keys (or new keys created via LPUSH/RPUSH). */
int list_cmd(kvs_token_t cmd, kv_entry_t *entry,
             kvs_token_t *args, int argc, DynBuf *wb,
             const char *msg_raw, int msg_len);

/* Free quicklist nodes owned by a LIST entry (call before dict_delete/dict_clear). */
void list_cleanup_entry(kv_entry_t *e);

/* Flatten a quicklist entry to packed format for RDB persistence.
   Allocates *out_data (caller frees). Returns 0 on success. */
int list_flatten_to_packed(kv_entry_t *e, char **out_data, uint32_t *out_len);

#endif
