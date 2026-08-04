#ifndef __KVS_PROTOCOL_H__
#define __KVS_PROTOCOL_H__

#include <stdint.h>
#include "dynbuf.h"

#define KVS_MAX_TOKENS         128             /* max args per command */
#define KVS_MAX_BULK_LEN       (512 * 1024 * 1024)
#define KVS_MAX_INLINE_LEN     65536
#define KVS_RB_COMPACT_RATIO   2
#define BUF_INIT_CAP           4096

/* Zero-copy token slice */
typedef struct {
    const char *ptr;
    int         len;
} kvs_token_t;

#define TOKEN_EQ(t, literal) \
    ((t).len == (int)(sizeof(literal)-1) && memcmp((t).ptr, literal, sizeof(literal)-1) == 0)

typedef enum {
    DISPATCH_OK    = 0,
    DISPATCH_CLOSE = 2,
    DISPATCH_ERROR = -1,
} dispatch_result_t;

#define KVS_DRAIN_PROTOERR  (-1)

struct conn;

int resp_parse_slices(const char *buf, int buflen,
                      kvs_token_t *tokens, int max_tokens, int *out_argc);

int kvs_drain_resp_commands_off(DynBuf *rb, int *r_off, DynBuf *wb, struct conn *c);
int kvs_protocol_batch(char *msg, int len, DynBuf *write_buf);

extern const char RESP_OK[];
extern const char RESP_PONG[];
extern const char RESP_NIL[];
extern const char RESP_ONE[];
extern const char RESP_ZERO[];
extern const char RESP_ERROR[];
extern const char RESP_ERR_INV[];
extern const char RESP_ERR_UNK[];
extern const char RESP_ERR_ARGS[];
extern const char RESP_ERR_INT[];
extern const char RESP_WRONGTYPE[];
extern const int  RESP_OK_LEN, RESP_PONG_LEN, RESP_NIL_LEN;
extern const int  RESP_ONE_LEN, RESP_ZERO_LEN, RESP_ERROR_LEN;
extern const int  RESP_ERR_INV_LEN, RESP_ERR_UNK_LEN, RESP_ERR_ARGS_LEN;
extern const int  RESP_ERR_INT_LEN, RESP_WRONGTYPE_LEN;

/* ── Fast int64 parser (overflow-safe, rejects leading zeros) ── */

static inline int kvs_parse_int64(const char *p, int len, int64_t *out) {
    if (len <= 0 || len > 20) return 0;
    const char *end = p + len;
    int negative = 0;
    if (*p == '-') { if (len == 1) return 0; negative = 1; p++; }
    if (p >= end) return 0;
    if (*p == '0' && (end - p) > 1) return 0;
    int64_t v = 0;
    for (; p < end; p++) {
        if (*p < '0' || *p > '9') return 0;
        int64_t nv = v * 10 + (*p - '0');
        if (nv < v) return 0;  /* overflow */
        v = nv;
    }
    *out = negative ? -v : v;
    return 1;
}

#endif
