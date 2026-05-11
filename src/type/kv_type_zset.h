#ifndef __KV_TYPE_ZSET_H__
#define __KV_TYPE_ZSET_H__

#include "kv_dict.h"
#include "server.h"
#include "kvstore.h"
#include "protocol.h"
#include "client.h"

/* ZSET type — sorted set mapping members to scores.

 * Two encodings:
 *   KV_ENCODING_PACKED — inline packed array for small zsets
 *     data[] layout: [type:1][encoding:1][count:4][member1_len:4][member1][score1:8]...
 *     (sorted by score ascending)
 *   KV_ENCODING_SKIPLIST — skiplist + dict for large zsets
 *     data[] layout: [type:1][encoding:1][kv_dict_t][head:8][tail:8][level:4][count:4]
 *
 * PACKED threshold: 128 members or 4096 data bytes.
 * Shrink threshold: 64 members.
 */

/* Offsets within data[] — defined in terms of sizeof(kv_dict_t) for portability.
 * Note: data[2] may be misaligned for 8B types; all accessors use memcpy (safe on x86_64). */
enum {
    ZSET_OFF_DICT    = 2,
    ZSET_OFF_HEAD    = 2 + (int)sizeof(kv_dict_t),
    ZSET_OFF_TAIL    = 2 + (int)sizeof(kv_dict_t) + 8,
    ZSET_OFF_LEVEL   = 2 + (int)sizeof(kv_dict_t) + 16,
    ZSET_OFF_COUNT   = 2 + (int)sizeof(kv_dict_t) + 20,
    ZSET_HEADER_SIZE = 2 + (int)sizeof(kv_dict_t) + 24
};

/* Skiplist node (heap allocated). Member string is owned by this node. */
typedef struct zskiplist_node_s {
    double   score;
    char    *member;
    uint32_t member_len;
    struct zskiplist_node_s *backward;
    struct {
        struct zskiplist_node_s *forward;
        uint32_t span;
    } level[];  /* flexible array, level_count elements */
} zskiplist_node_t;

/* Inline zset header accessors (embedded in e->data[]) */
static inline kv_dict_t *zset_dict(kv_entry_t *e) {
    return (kv_dict_t *)(e->data + ZSET_OFF_DICT);
}
static inline zskiplist_node_t *zset_head(kv_entry_t *e) {
    uint64_t v; memcpy(&v, e->data + ZSET_OFF_HEAD, 8); return (zskiplist_node_t *)(uintptr_t)v;
}
static inline void zset_set_head(kv_entry_t *e, zskiplist_node_t *n) {
    uint64_t v = (uint64_t)(uintptr_t)n; memcpy(e->data + ZSET_OFF_HEAD, &v, 8);
}
static inline zskiplist_node_t *zset_tail(kv_entry_t *e) {
    uint64_t v; memcpy(&v, e->data + ZSET_OFF_TAIL, 8); return (zskiplist_node_t *)(uintptr_t)v;
}
static inline void zset_set_tail(kv_entry_t *e, zskiplist_node_t *n) {
    uint64_t v = (uint64_t)(uintptr_t)n; memcpy(e->data + ZSET_OFF_TAIL, &v, 8);
}
static inline int zset_level(kv_entry_t *e) {
    int v; memcpy(&v, e->data + ZSET_OFF_LEVEL, 4); return v;
}
static inline void zset_set_level(kv_entry_t *e, int lv) {
    memcpy(e->data + ZSET_OFF_LEVEL, &lv, 4);
}
static inline uint32_t zset_count(kv_entry_t *e) {
    uint32_t v; memcpy(&v, e->data + ZSET_OFF_COUNT, 4); return v;
}
static inline void zset_set_count(kv_entry_t *e, uint32_t c) {
    memcpy(e->data + ZSET_OFF_COUNT, &c, 4);
}

dispatch_result_t zset_cmd(kvs_token_t cmd, kv_entry_t *entry,
                           kvs_token_t *args, int argc, DynBuf *wb,
                           const char *msg_raw, int msg_len,
                           client_t *client);

void zset_cleanup_entry(kv_entry_t *e);

int zset_flatten_to_packed(kv_entry_t *e, char **out_data, uint32_t *out_len);

void zset_rebuild_encoding(kv_entry_t *e);

#endif
