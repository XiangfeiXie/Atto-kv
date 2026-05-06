#ifndef __KVS_PROTOCOL_H__
#define __KVS_PROTOCOL_H__

#include "dynbuf.h"

#define KVS_MAX_TOKENS 256
#define BUF_INIT_CAP   4096

/* Zero-copy token slice — points into the read buffer, no malloc */
typedef struct {
    const char *ptr;
    int         len;
} kvs_token_t;

/* Length-first, compile-time string comparison for command dispatch */
#define TOKEN_EQ(t, literal) \
    ((t).len == (int)(sizeof(literal)-1) && memcmp((t).ptr, literal, sizeof(literal)-1) == 0)

/* Dispatch return codes */
typedef enum {
    DISPATCH_OK      = 0,   /* response written to wb */
    DISPATCH_BLOCKED = 1,   /* client parked, don't send, don't close */
    DISPATCH_ERROR   = -1,  /* protocol error, close connection */
} dispatch_result_t;

struct conn;

/* Single-pass RESP parser: validates format + extracts (ptr,len) pairs.
   Returns bytes consumed on success, 0 if incomplete, -1 on protocol error. */
int resp_parse_slices(const char *buf, int buflen,
                      kvs_token_t *tokens, int max_tokens,
                      int *out_argc);

/* Drain RESP commands from read buffer, appending responses to write buffer.
   c may be NULL for legacy callers that don't support blocking.
   Returns command count, or -1 if a blocked client was encountered. */
int kvs_drain_resp_commands_off(DynBuf *rb, int *r_off, DynBuf *wb, struct conn *c);

/* Legacy entry points */
int kvs_protocol(char *msg, int length, DynBuf *write_buf);
int kvs_protocol_batch(char *msg, int len, DynBuf *write_buf);

/* Commonly-used RESP fragments */
extern const char RESP_OK[];
extern const char RESP_PONG[];
extern const char RESP_NIL[];
extern const char RESP_ERR_INV[];
extern const char RESP_ERR_UNK[];
extern const char RESP_WRONGTYPE[];
extern const int  RESP_OK_LEN;
extern const int  RESP_PONG_LEN;
extern const int  RESP_NIL_LEN;
extern const int  RESP_ERR_INV_LEN;
extern const int  RESP_ERR_UNK_LEN;
extern const int  RESP_WRONGTYPE_LEN;

#endif
