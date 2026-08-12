#include "protocol.h"
#include "config.h"
#include "kvstore.h"
#include <stdio.h>
#include <string.h>

/* ── RESP fragments ─────────────────────────────────────────────────── */

#define S(id, str) \
    const char id[] = str; \
    const int  id##_LEN = (int)(sizeof(str) - 1)

S(RESP_OK,        "+OK\r\n");
S(RESP_PONG,      "+PONG\r\n");
S(RESP_NIL,       "$-1\r\n");
S(RESP_ONE,       ":1\r\n");
S(RESP_ZERO,      ":0\r\n");
S(RESP_ERROR,     "-ERR error\r\n");
S(RESP_ERR_INV,   "-ERR invalid RESP protocol\r\n");
S(RESP_ERR_UNK,   "-ERR unknown command\r\n");
S(RESP_ERR_ARGS,  "-ERR wrong number of arguments\r\n");
S(RESP_ERR_INT,   "-ERR value is not an integer or out of range\r\n");
S(RESP_WRONGTYPE, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");

#undef S

/* ── RESP parser — array form "*N\r\n$L\r\n<data>\r\n..." only ──────
 * Returns bytes consumed (>0), 0 (incomplete), or -1 (protocol error). */

int resp_parse_slices(const char *buf, int buflen,
                      kvs_token_t *tokens, int max_tokens, int *out_argc)
{
    if (buflen < 4 || buf[0] != '*') return -1;

    const char *p = buf + 1, *end = buf + buflen;

    /* ── Parse argc inline (max 3 digits for pipeline, 4 for safety) ── */
    int argc = 0;
    while (p < end && *p >= '0' && *p <= '9') {
        argc = argc * 10 + (*p - '0');
        if (argc > max_tokens) return -1;
        p++;
    }
    if (p + 1 >= end || *p != '\r' || p[1] != '\n') return (p >= end) ? 0 : -1;
    if (argc <= 0) return -1;
    p += 2;  /* skip \r\n */

    for (int i = 0; i < argc; i++) {
        if (p >= end) return 0;
        if (*p != '$') return -1;
        p++;

        /* ── Parse bulk length inline ── */
        int slen = 0;
        while (p < end && *p >= '0' && *p <= '9') {
            slen = slen * 10 + (*p - '0');
            if (slen > KVS_MAX_BULK_LEN) return -1;
            p++;
        }
        if (p + 1 >= end || *p != '\r' || p[1] != '\n') return (p >= end) ? 0 : -1;
        p += 2;

        if (p + slen + 2 > end) return 0;

        tokens[i].ptr = p;
        tokens[i].len = slen;
        p += slen;
        if (p + 1 >= end || *p != '\r' || p[1] != '\n') return -1;
        p += 2;
    }
    *out_argc = argc;
    return (int)(p - buf);
}

/* ── Drain loop — extract commands from read buffer into write buffer ─ */

int kvs_drain_resp_commands_off(DynBuf *rb, int *r_off, DynBuf *wb, struct conn *c)
{
    int off = *r_off, remaining = rb->len, ncmd = 0;

    while (remaining > 0) {
        kvs_token_t tokens[KVS_MAX_TOKENS];
        int argc = 0;
        int cmd_len = resp_parse_slices(rb->buf + off, remaining,
                                        tokens, KVS_MAX_TOKENS, &argc);

        /* Inline command fallback (redis-benchmark sends "PING\r\n",
         * redis-cli sends inline commands, etc.) */
        if (cmd_len == -1) {
            const char *p = rb->buf + off;
            const char *end = p + remaining;
            const char *ln = memchr(p, '\n', (size_t)(end - p));
            if (!ln || p == ln) { cmd_len = 0; break; }  /* incomplete line */

            /* Tokenise by spaces.  Replace \r\n with \0\0 for strtok_r. */
            char line[256];
            int linelen = (int)(ln - p);
            if (linelen > 255) linelen = 255;
            if (linelen > 0 && p[linelen - 1] == '\r') linelen--;
            memcpy(line, p, linelen);
            line[linelen] = '\0';

            /* Extract space-delimited tokens */
            argc = 0;
            char *saveptr = NULL;
            char *tok = strtok_r(line, " ", &saveptr);
            while (tok && argc < KVS_MAX_TOKENS) {
                tokens[argc].ptr = tok;
                tokens[argc].len = (int)strlen(tok);
                argc++;
                tok = strtok_r(NULL, " ", &saveptr);
            }
            if (argc == 0) {
                /* Empty line (e.g. leading \r\n from redis-cli --pipe
                 * synchronization marker).  Consume silently and
                 * advance off/remaining directly — continue would
                 * restart the loop without updating either. */
                int consumed = (int)(ln + 1 - p);
                off       += consumed;
                remaining -= consumed;
                continue;
            }

            cmd_len = (int)(ln + 1 - p); /* bytes consumed including \n */
        }

        if (cmd_len <= 0) {
            if (cmd_len == 0) break;
            dynbuf_cat(wb, RESP_ERR_INV, RESP_ERR_INV_LEN);
            if (c) c->status = 1;
            ncmd = -1; break;
        }

        dispatch_result_t r = kvs_dispatch(tokens, argc,
                                            rb->buf + off, cmd_len, wb, c);
        off += cmd_len;
        remaining -= cmd_len;
        ncmd++;

        if (r == DISPATCH_CLOSE) { if (c) c->status = 1; break; }
    }

    /* Compact if r_off exceeds half capacity (rare with 64KB+ buffers).
     * Skip compaction when a recv is in flight (proactor arm-before-drain):
     * the in-flight SQE targets the old physical address and compaction
     * would move the data out from under it, causing buffer corruption. */
    if (off > rb->cap / KVS_RB_COMPACT_RATIO && remaining > 0 && remaining < off
        && !(c && c->recv_armed)) {
        memmove(rb->buf, rb->buf + off, (size_t)remaining);
        off = 0;
    }

    *r_off = off;
    rb->len = remaining;
    return ncmd;
}

int kvs_protocol_batch(char *msg, int len, DynBuf *write_buf) {
    (void)msg; (void)len; (void)write_buf; return 0;
}
