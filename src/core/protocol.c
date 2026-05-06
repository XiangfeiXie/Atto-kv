#include "protocol.h"
#include <string.h>
#include <stdio.h>

/* ==================== RESP static fragments ==================== */

#define S(id, str) \
    const char id[] = str; \
    const int  id##_LEN = (int)(sizeof(str) - 1)

S(RESP_OK,    "+OK\r\n");
S(RESP_PONG,  "+PONG\r\n");
S(RESP_NIL,   "$-1\r\n");
S(RESP_ERR_INV, "-ERR invalid RESP protocol\r\n");
S(RESP_ERR_UNK, "-ERR unknown command\r\n");
S(RESP_WRONGTYPE, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");

#undef S

/* ==================== Zero-copy RESP parser ==================== */

int resp_parse_slices(const char *buf, int buflen,
                      kvs_token_t *tokens, int max_tokens,
                      int *out_argc)
{
    int idx = 0;
    if (buflen < 1) return 0;
    if (buf[idx] != '*') return -1;
    if (buflen < 5) return 0;

    idx++;
    int argc = 0;
    while (idx < buflen && buf[idx] != '\r') {
        if (buf[idx] < '0' || buf[idx] > '9') return -1;
        argc = argc * 10 + (buf[idx] - '0');
        idx++;
    }
    if (idx + 2 > buflen) return 0;
    idx += 2;
    if (argc <= 0 || argc > max_tokens) return -1;

    for (int i = 0; i < argc; i++) {
        if (idx + 1 >= buflen) return 0;
        if (buf[idx] != '$') return -1;
        idx++;

        int slen = 0;
        while (idx < buflen && buf[idx] != '\r') {
            if (buf[idx] < '0' || buf[idx] > '9') return -1;
            slen = slen * 10 + (buf[idx] - '0');
            idx++;
        }
        if (idx + 2 > buflen) return 0;
        idx += 2;
        if (slen < 0) return -1;
        if (idx + slen + 2 > buflen) return 0;

        tokens[i].ptr = buf + idx;
        tokens[i].len = slen;
        idx += slen + 2;
    }
    *out_argc = argc;
    return idx;
}

/* ==================== Batch drain loop ==================== */

int kvs_drain_resp_commands_off(DynBuf *rb, int *r_off, DynBuf *wb, struct conn *c);

/* Forward declaration — kvs_dispatch lives in kvstore.c */
extern dispatch_result_t kvs_dispatch(kvs_token_t *tokens, int argc,
                        const char *msg_raw, int msg_len, DynBuf *wb, struct conn *c);

int kvs_drain_resp_commands_off(DynBuf *rb, int *r_off, DynBuf *wb, struct conn *c) {
    if (!rb || !rb->buf || !r_off || !wb)
        return -1;

    int ncmd = 0;
    int off = *r_off;
    int remaining = rb->len;

    if (off < 0 || off >= rb->cap) {
        off = 0; remaining = 0;
        rb->len = 0; *r_off = 0;
        return 0;
    }

    kvs_token_t tokens[KVS_MAX_TOKENS];

    while (remaining > 0) {
        while (remaining > 0 &&
               (rb->buf[off] == '\r' || rb->buf[off] == '\n' || rb->buf[off] == ' ')) {
            off++; remaining--;
        }
        if (remaining <= 0) break;
        if (off < 0 || off >= rb->cap) break;

        int argc = 0;
        int cmd_len = resp_parse_slices(rb->buf + off, remaining,
                                        tokens, KVS_MAX_TOKENS, &argc);
        if (cmd_len > 0) {
            dispatch_result_t r = kvs_dispatch(tokens, argc, rb->buf + off, cmd_len, wb, c);
            off += cmd_len;
            remaining -= cmd_len;
            ncmd++;
            if (r == DISPATCH_BLOCKED) {
                /* Client parked — stop processing further commands on this conn */
                *r_off = off;
                rb->len = remaining;
                return -1;
            }
        } else if (cmd_len == 0) {
            break;
        } else {
            int skip = 1;
            while (skip < remaining && (off + skip) < rb->cap &&
                   rb->buf[off + skip] != '*') skip++;
            if (skip >= remaining || (off + skip) >= rb->cap) skip = remaining;
            off += skip; remaining -= skip;
        }
    }

    *r_off = off;
    rb->len = remaining;
    return ncmd;
}

/* ==================== Legacy protocol entry ==================== */

int kvs_protocol(char *msg, int length, DynBuf *write_buf) {
    if (msg == NULL || length <= 0 || write_buf == NULL)
        return -1;

    kvs_token_t tokens[KVS_MAX_TOKENS];
    int argc = 0;
    int cmd_len = resp_parse_slices(msg, length, tokens, KVS_MAX_TOKENS, &argc);
    if (cmd_len <= 0) {
        dynbuf_cat(write_buf, RESP_ERR_INV, RESP_ERR_INV_LEN);
        return -1;
    }
    dispatch_result_t r = kvs_dispatch(tokens, argc, msg, length, write_buf, NULL);
    return (r == DISPATCH_OK) ? write_buf->len : -1;
}

int kvs_protocol_batch(char *msg, int len, DynBuf *write_buf) {
    static int read_offset = 0;
    DynBuf read_buf;
    dynbuf_init(&read_buf, BUF_INIT_CAP);
    dynbuf_append(&read_buf, msg, len);
    dynbuf_clear(write_buf);
    kvs_drain_resp_commands_off(&read_buf, &read_offset, write_buf, NULL);
    dynbuf_free(&read_buf);
    return write_buf->len;
}
