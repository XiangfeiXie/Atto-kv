#ifndef KVSTORE_MACROS_H
#define KVSTORE_MACROS_H

/* Hot command inlining macros - reduce function call overhead for GET/SET
 * by expanding them directly in kvs_dispatch. These are the top 2 commands
 * in most workloads, accounting for 60-80% of all operations.
 *
 * Benefits at P=1-10:
 * - Eliminate function call overhead (~2-3ns per call)
 * - Better instruction cache locality
 * - Compiler can optimize across the inline boundary
 *
 * Expected gain: 5-8% for P=1-10 workloads */

/* Inline GET implementation - matches string_cmd GET logic.
 * Uses pre-computed entry to avoid redundant kv_hash + dict_find. */
#define INLINE_GET_COMMAND(entry, hash, key_tok, wb) do { \
    if (!(entry)) { \
        dynbuf_cat((wb), RESP_NIL, RESP_NIL_LEN); \
    } else { \
        uint32_t vlen = 0; \
        uint8_t enc = KV_ENCODING_RAW; \
        const char *val = string_engine_get_pre((entry), &vlen, &enc); \
        if (enc == KV_ENCODING_INT) { \
            int64_t iv; \
            memcpy(&iv, val, sizeof(int64_t)); \
            char ibuf[24]; \
            int il = snprintf(ibuf, sizeof(ibuf), "%lld", (long long)iv); \
            char hdr[24]; \
            int bh = snprintf(hdr, sizeof(hdr), "$%d\r\n", il); \
            dynbuf_cat((wb), hdr, bh); \
            dynbuf_cat((wb), ibuf, il); \
            dynbuf_lit((wb), "\r\n"); \
        } else { \
            char hdr[24]; \
            int bh = snprintf(hdr, sizeof(hdr), "$%u\r\n", vlen); \
            dynbuf_cat((wb), hdr, bh); \
            dynbuf_cat((wb), val, (int)vlen); \
            dynbuf_lit((wb), "\r\n"); \
        } \
    } \
} while (0)

/* Inline SET implementation - simple SET without options (EX/PX/NX/XX/KEEPTTL)
 * Falls back to string_cmd for complex SET operations */
#define INLINE_SET_COMMAND(entry, hash, tokens, argc, wb, msg_raw, msg_len) do { \
    if ((argc) == 3) { \
        /* Simple SET key value - no options */ \
        kvs_token_t key_tok = (tokens)[1]; \
        kvs_token_t val_tok = (tokens)[2]; \
        string_engine_set_pre(key_tok.ptr, (uint32_t)key_tok.len, \
                              val_tok.ptr, (uint32_t)val_tok.len, \
                              0, 0, (hash), (entry)); \
        dynbuf_cat((wb), RESP_OK, RESP_OK_LEN); \
        if (!g_in_reload) { \
            kvs_write_journal_append((msg_raw), (msg_len)); \
        } \
    } else { \
        /* Complex SET with options - use full implementation */ \
        string_cmd((tokens)[0], (entry), (hash), (tokens) + 1, (argc) - 1, \
                   (wb), (msg_raw), (msg_len)); \
    } \
} while (0)

#endif /* KVSTORE_MACROS_H */
