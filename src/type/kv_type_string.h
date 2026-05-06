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

/* Command dispatch for STRING-type keys (or new keys that become STRING). */
dispatch_result_t string_cmd(kvs_token_t cmd, kv_entry_t *entry,
                             kvs_token_t *args, int argc, DynBuf *wb,
                             const char *msg_raw, int msg_len);

/* Write-command check for AOF / eBPF probe */
int string_is_write_cmd(kvs_token_t cmd);

/* Build a STRING entry's data[] for dict_add.
   out_data must be at least val_len + 2 bytes. */
void string_build_entry_data(const char *value, int val_len, char *out_data);

#endif
