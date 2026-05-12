#include "protocol.h"
#include "server.h"
#include "client.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

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

/* ==================== Zero-copy RESP parser ====================
 *
 * Returns:
 *   > 0  : bytes consumed (one complete command extracted)
 *   = 0  : incomplete frame, need more bytes
 *   < 0  : protocol error (caller MUST close the connection — Redis does)
 *
 * Hardened against integer overflow on argc/bulk length, supports the
 * RESP-2 inline form (telnet-style) and the standard array form ("*N\r\n").
 * Uses memchr to locate CRLF boundaries — far cheaper than per-byte loops
 * for long bulk values where the parser only walks the length header.
 */
int resp_parse_slices(const char *buf, int buflen,
                      kvs_token_t *tokens, int max_tokens,
                      int *out_argc)
{
    if (buflen < 1) return 0;

    /* ── Inline command (telnet/debug) ── */
    if (buf[0] != '*') {
        /* Bound scanning at KVS_MAX_INLINE_LEN to avoid DoS on long garbage */
        int scan = buflen < KVS_MAX_INLINE_LEN ? buflen : KVS_MAX_INLINE_LEN;
        const char *cr = memchr(buf, '\r', (size_t)scan);
        if (!cr) {
            if (buflen >= KVS_MAX_INLINE_LEN) return -1; /* line too long */
            return 0; /* need more bytes */
        }
        int eol = (int)(cr - buf);
        if (eol + 1 >= buflen) return 0;
        if (buf[eol + 1] != '\n') return -1;

        int argc = 0;
        int pos = 0;
        while (pos < eol && argc < max_tokens) {
            while (pos < eol && (buf[pos] == ' ' || buf[pos] == '\t')) pos++;
            if (pos >= eol) break;
            int tok_end = pos;
            while (tok_end < eol && buf[tok_end] != ' ' && buf[tok_end] != '\t')
                tok_end++;
            tokens[argc].ptr = buf + pos;
            tokens[argc].len = tok_end - pos;
            argc++;
            pos = tok_end;
        }
        if (argc == 0) return -1;   /* blank inline line is invalid */
        *out_argc = argc;
        return eol + 2;
    }

    /* ── Array form "*N\r\n" + N bulk strings ── */
    int idx = 1;

    /* Locate first CRLF for argc */
    int hdr_scan = buflen - idx;
    if (hdr_scan > 32) hdr_scan = 32; /* argc digits fit easily */
    const char *cr = memchr(buf + idx, '\r', (size_t)hdr_scan);
    if (!cr) {
        if (buflen - idx >= 32) return -1; /* argc field too long */
        return 0;
    }
    int hdr_end = (int)(cr - buf);
    if (hdr_end + 1 >= buflen) return 0;
    if (buf[hdr_end + 1] != '\n') return -1;
    if (hdr_end == idx) return -1; /* empty argc */

    /* Parse argc with overflow guard */
    long argc = 0;
    for (int i = idx; i < hdr_end; i++) {
        unsigned c = (unsigned char)buf[i] - (unsigned)'0';
        if (c > 9) return -1;
        argc = argc * 10 + (long)c;
        if (argc > max_tokens) return -1;
    }
    if (argc <= 0) return -1;
    idx = hdr_end + 2;

    for (long i = 0; i < argc; i++) {
        if (idx >= buflen) return 0;
        if (buf[idx] != '$') return -1;
        idx++;

        /* Bulk length header */
        int blen_scan = buflen - idx;
        if (blen_scan > 24) blen_scan = 24;
        const char *bcr = memchr(buf + idx, '\r', (size_t)blen_scan);
        if (!bcr) {
            if (buflen - idx >= 24) return -1; /* length field too long */
            return 0;
        }
        int blen_end = (int)(bcr - buf);
        if (blen_end + 1 >= buflen) return 0;
        if (buf[blen_end + 1] != '\n') return -1;
        if (blen_end == idx) return -1;

        long slen = 0;
        int neg = 0;
        int j = idx;
        if (buf[j] == '-') { neg = 1; j++; if (j == blen_end) return -1; }
        for (; j < blen_end; j++) {
            unsigned c = (unsigned char)buf[j] - (unsigned)'0';
            if (c > 9) return -1;
            slen = slen * 10 + (long)c;
            if (slen > KVS_MAX_BULK_LEN) return -1;
        }
        if (neg) return -1;   /* server-side input never has null bulk */
        idx = blen_end + 2;

        if ((long)idx + slen + 2 > (long)buflen) return 0;

        tokens[i].ptr = buf + idx;
        tokens[i].len = (int)slen;
        idx += (int)slen;
        if (buf[idx] != '\r' || buf[idx + 1] != '\n') return -1;
        idx += 2;
    }
    *out_argc = (int)argc;
    return idx;
}

/* ==================== Batch drain loop ==================== */

int kvs_drain_resp_commands_off(DynBuf *rb, int *r_off, DynBuf *wb, struct conn *c);

/* Forward declaration — kvs_dispatch lives in kvstore.c */
extern dispatch_result_t kvs_dispatch(kvs_token_t *tokens, int argc,
                        const char *msg_raw, int msg_len, DynBuf *wb, struct conn *c);

int kvs_drain_resp_commands_off(DynBuf *rb, int *r_off, DynBuf *wb, struct conn *c) {
    if (!rb || !rb->buf || !r_off || !wb)
        return KVS_DRAIN_PROTOERR;

    int ncmd = 0;
    int off = *r_off;
    int remaining = rb->len;

    if (off < 0 || off > rb->cap || remaining < 0) {
        rb->len = 0; *r_off = 0;
        return 0;
    }

    kvs_token_t tokens[KVS_MAX_TOKENS];

    while (remaining > 0) {
        /* Tolerate inter-command CRLF only (Redis strict-parses; keep mild tolerance) */
        while (remaining > 0 && (rb->buf[off] == '\r' || rb->buf[off] == '\n')) {
            off++; remaining--;
        }
        if (remaining <= 0) break;

        int argc = 0;
        int cmd_len = resp_parse_slices(rb->buf + off, remaining,
                                        tokens, KVS_MAX_TOKENS, &argc);
        if (cmd_len > 0) {
            dispatch_result_t r = kvs_dispatch(tokens, argc,
                                               rb->buf + off, cmd_len, wb, c);
            off += cmd_len;
            remaining -= cmd_len;
            ncmd++;
            if (r == DISPATCH_BLOCKED) {
                *r_off = off;
                rb->len = remaining;
                return KVS_DRAIN_BLOCKED;
            }
            if (r == DISPATCH_CLOSE) {
                /* Signal caller to close after flush; mark via conn->status */
                if (c) c->status = 1;
                *r_off = off;
                rb->len = remaining;
                return ncmd;
            }
        } else if (cmd_len == 0) {
            break;  /* incomplete — wait for more data */
        } else {
            /* Protocol error — Redis-compat: write error and close conn */
            dynbuf_cat(wb, RESP_ERR_INV, RESP_ERR_INV_LEN);
            if (c) c->status = 1;   /* close after flush */
            *r_off = off;
            rb->len = remaining;
            return KVS_DRAIN_PROTOERR;
        }
    }

    /* Compaction: if we've consumed enough head space, slide remainder down.
       Prevents unbounded buffer growth on long-lived pipelined connections. */
    if (off > 0 && (remaining == 0 || off >= rb->cap / KVS_RB_COMPACT_RATIO)) {
        if (remaining > 0)
            memmove(rb->buf, rb->buf + off, (size_t)remaining);
        off = 0;
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
    return (r == DISPATCH_OK || r == DISPATCH_CLOSE) ? write_buf->len : -1;
}

/* kvs_protocol_batch: deprecated. Reactor/proactor call
   kvs_drain_resp_commands_off() directly with per-conn offsets. */
int kvs_protocol_batch(char *msg, int len, DynBuf *write_buf) {
    (void)msg; (void)len; (void)write_buf;
    return 0;
}
