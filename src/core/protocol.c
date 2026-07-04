#include "protocol.h"
#include "server.h"
#include "client.h"
#include "config.h"
#include "kvstore.h"           /* kvs_lat_record_ns, KVS_LAT_BUCKETS */
#include "../repl/repl_notify.h"  /* Long command replication notification */
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#if defined(__AVX2__)
#include <immintrin.h>
#elif defined(__SSE2__)
#include <emmintrin.h>
#endif

/* Locate "\r\n" in a bounded region.  RESP headers are short (argc <= 32B,
 * bulk length <= 24B), but inline commands may scan up to 64KB.  The SIMD
 * path mainly helps inline/debug commands and large pipelined buffers while
 * keeping array-form headers on a tiny scalar fast path.
 * Force inline for hot path optimization. */
static inline __attribute__((always_inline))
const char *resp_find_crlf(const char *p, int n)
{
    if (n <= 0) return NULL;

    if (n < 33) {
        const char *cr = memchr(p, '\r', (size_t)n);
        while (cr) {
            int off = (int)(cr - p);
            if (off + 1 >= n) return NULL;
            if (cr[1] == '\n') return cr;
            cr = memchr(cr + 1, '\r', (size_t)(n - off - 1));
        }
        return NULL;
    }

#if defined(__AVX2__)
    const __m256i vcr = _mm256_set1_epi8('\r');
    const __m256i vlf = _mm256_set1_epi8('\n');
    int i = 0;
    for (; i + 33 <= n; i += 32) {
        __m256i a = _mm256_loadu_si256((const __m256i *)(const void *)(p + i));
        __m256i b = _mm256_loadu_si256((const __m256i *)(const void *)(p + i + 1));
        __m256i m = _mm256_and_si256(_mm256_cmpeq_epi8(a, vcr),
                                     _mm256_cmpeq_epi8(b, vlf));
        uint32_t mask = (uint32_t)_mm256_movemask_epi8(m);
        if (mask) return p + i + __builtin_ctz(mask);
    }
    return resp_find_crlf(p + i, n - i);
#elif defined(__SSE2__)
    const __m128i vcr = _mm_set1_epi8('\r');
    const __m128i vlf = _mm_set1_epi8('\n');
    int i = 0;
    for (; i + 17 <= n; i += 16) {
        __m128i a = _mm_loadu_si128((const __m128i *)(const void *)(p + i));
        __m128i b = _mm_loadu_si128((const __m128i *)(const void *)(p + i + 1));
        __m128i m = _mm_and_si128(_mm_cmpeq_epi8(a, vcr),
                                  _mm_cmpeq_epi8(b, vlf));
        uint32_t mask = (uint32_t)_mm_movemask_epi8(m);
        if (mask) return p + i + __builtin_ctz(mask);
    }
    return resp_find_crlf(p + i, n - i);
#else
    const char *cr = memchr(p, '\r', (size_t)n);
    while (cr) {
        int off = (int)(cr - p);
        if (off + 1 >= n) return NULL;
        if (cr[1] == '\n') return cr;
        cr = memchr(cr + 1, '\r', (size_t)(n - off - 1));
    }
    return NULL;
#endif
}

/* Optimized integer parsing with unrolled loop and reduced branches.
 * Handles common cases (1-4 digits) with minimal overhead.
 * Force inline for hot path optimization. */
static inline __attribute__((always_inline))
int resp_parse_uint_bounded(const char *p, int n,
                                          long max_value, long *out)
{
    if (__builtin_expect(n <= 0, 0)) return -1;

    long v = 0;

    /* Fast path for common small numbers (1-4 digits) */
    if (__builtin_expect(n <= 4, 1)) {
        switch (n) {
        case 1: {
            unsigned c = (unsigned char)p[0] - '0';
            if (__builtin_expect(c > 9, 0)) return -1;
            v = c;
            break;
        }
        case 2: {
            unsigned c0 = (unsigned char)p[0] - '0';
            unsigned c1 = (unsigned char)p[1] - '0';
            if (__builtin_expect((c0 | c1) > 9, 0)) return -1;
            v = c0 * 10 + c1;
            break;
        }
        case 3: {
            unsigned c0 = (unsigned char)p[0] - '0';
            unsigned c1 = (unsigned char)p[1] - '0';
            unsigned c2 = (unsigned char)p[2] - '0';
            if (__builtin_expect((c0 | c1 | c2) > 9, 0)) return -1;
            v = c0 * 100 + c1 * 10 + c2;
            break;
        }
        case 4: {
            unsigned c0 = (unsigned char)p[0] - '0';
            unsigned c1 = (unsigned char)p[1] - '0';
            unsigned c2 = (unsigned char)p[2] - '0';
            unsigned c3 = (unsigned char)p[3] - '0';
            if (__builtin_expect((c0 | c1 | c2 | c3) > 9, 0)) return -1;
            v = c0 * 1000 + c1 * 100 + c2 * 10 + c3;
            break;
        }
        }
        if (__builtin_expect(v > max_value, 0)) return -1;
        *out = v;
        return 0;
    }

    /* Slow path for larger numbers - unrolled by 4 */
    int i = 0;
    for (; i + 4 <= n; i += 4) {
        unsigned c0 = (unsigned char)p[i]   - '0';
        unsigned c1 = (unsigned char)p[i+1] - '0';
        unsigned c2 = (unsigned char)p[i+2] - '0';
        unsigned c3 = (unsigned char)p[i+3] - '0';
        if (__builtin_expect((c0 | c1 | c2 | c3) > 9, 0)) return -1;
        v = v * 10000 + c0 * 1000 + c1 * 100 + c2 * 10 + c3;
        if (__builtin_expect(v > max_value, 0)) return -1;
    }

    /* Handle remaining digits */
    for (; i < n; i++) {
        unsigned c = (unsigned char)p[i] - '0';
        if (__builtin_expect(c > 9, 0)) return -1;
        v = v * 10 + c;
        if (__builtin_expect(v > max_value, 0)) return -1;
    }

    *out = v;
    return 0;
}

/* ==================== RESP static fragments ==================== */

#include "shared.h"

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
    if (__builtin_expect(buflen < 1, 0)) return 0;

    /* ── Inline command (telnet/debug) ── */
    if (__builtin_expect(buf[0] != '*', 0)) {
        /* Bound scanning at KVS_MAX_INLINE_LEN to avoid DoS on long garbage */
        int scan = buflen < KVS_MAX_INLINE_LEN ? buflen : KVS_MAX_INLINE_LEN;
        const char *cr = resp_find_crlf(buf, scan);
        if (!cr) {
            if (__builtin_expect(buflen >= KVS_MAX_INLINE_LEN, 0)) return -1;
            return 0;
        }
        int eol = (int)(cr - buf);
        if (__builtin_expect(eol + 1 >= buflen, 0)) return 0;
        if (__builtin_expect(buf[eol + 1] != '\n', 0)) return -1;

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
        if (__builtin_expect(argc == 0, 0)) return -1;
        *out_argc = argc;
        return eol + 2;
    }

    /* ── Array form "*N\r\n" + N bulk strings ── */
    int idx = 1;

    /* Locate first CRLF for argc */
    int hdr_scan = buflen - idx;
    if (hdr_scan > 32) hdr_scan = 32;
    const char *cr = resp_find_crlf(buf + idx, hdr_scan);
    if (__builtin_expect(!cr, 0)) {
        if (__builtin_expect(buflen - idx >= 32, 0)) return -1;
        return 0;
    }
    int hdr_end = (int)(cr - buf);
    if (__builtin_expect(hdr_end + 1 >= buflen, 0)) return 0;
    if (__builtin_expect(buf[hdr_end + 1] != '\n', 0)) return -1;
    if (__builtin_expect(hdr_end == idx, 0)) return -1;

    /* Parse argc with overflow guard */
    long argc = 0;
    if (__builtin_expect(resp_parse_uint_bounded(buf + idx, hdr_end - idx,
                                (long)max_tokens, &argc) != 0, 0)) {
        return -1;
    }
    if (__builtin_expect(argc <= 0, 0)) return -1;
    idx = hdr_end + 2;

    /* Prefetch next bulk string header for better cache utilization */
    if (__builtin_expect(idx + 32 < buflen, 1))
        __builtin_prefetch(buf + idx, 0, 3);

    for (long i = 0; i < argc; i++) {
        if (__builtin_expect(idx >= buflen, 0)) return 0;
        if (__builtin_expect(buf[idx] != '$', 0)) return -1;
        idx++;

        /* Bulk length header */
        int blen_scan = buflen - idx;
        if (blen_scan > 24) blen_scan = 24;
        const char *bcr = resp_find_crlf(buf + idx, blen_scan);
        if (__builtin_expect(!bcr, 0)) {
            if (__builtin_expect(buflen - idx >= 24, 0)) return -1;
            return 0;
        }
        int blen_end = (int)(bcr - buf);
        if (__builtin_expect(blen_end + 1 >= buflen, 0)) return 0;
        if (__builtin_expect(buf[blen_end + 1] != '\n', 0)) return -1;
        if (__builtin_expect(blen_end == idx, 0)) return -1;

        long slen = 0;
        int j = idx;
        if (__builtin_expect(buf[j] == '-', 0)) return -1; /* server-side never has null bulk */
        if (__builtin_expect(resp_parse_uint_bounded(buf + j, blen_end - j,
                                    KVS_MAX_BULK_LEN, &slen) != 0, 0)) {
            return -1;
        }
        idx = blen_end + 2;

        if (__builtin_expect((long)idx + slen + 2 > (long)buflen, 0)) return 0;

        /* Prefetch bulk data for next iteration to improve cache utilization */
        if (__builtin_expect(i + 1 < argc && idx + slen + 32 < buflen, 1))
            __builtin_prefetch(buf + idx + slen + 2, 0, 3);

        tokens[i].ptr = buf + idx;
        tokens[i].len = (int)slen;
        idx += (int)slen;
        if (__builtin_expect(buf[idx] != '\r' || buf[idx + 1] != '\n', 0)) return -1;
        idx += 2;
    }
    *out_argc = (int)argc;
    return idx;
}

/* ==================== Batch drain loop ==================== */

/* Fast check if command is a write command (for replication notification)
 * Only checks first character for performance */
static inline int is_write_command_fast(kvs_token_t cmd) {
    if (cmd.len == 0) return 0;

    char c0 = cmd.ptr[0];
    if (c0 >= 'a' && c0 <= 'z') c0 -= 32;  /* toupper */

    /* Write commands: SET, DEL, HSET, LPUSH, RPUSH, SADD, ZADD, INCR, DECR, APPEND, MSET */
    switch (c0) {
        case 'S':  /* SET, SETEX, SETNX, SADD, SREM */
        case 'D':  /* DEL, DECR, DECRBY */
        case 'H':  /* HSET, HMSET, HDEL */
        case 'L':  /* LPUSH, LPOP */
        case 'R':  /* RPUSH, RPOP */
        case 'Z':  /* ZADD, ZREM */
        case 'I':  /* INCR, INCRBY */
        case 'A':  /* APPEND */
        case 'M':  /* MSET, MSETNX */
        case 'F':  /* FLUSHALL, FLUSHDB */
            return 1;
        default:
            return 0;
    }
}

int kvs_drain_resp_commands_off(DynBuf *rb, int *r_off, DynBuf *wb, struct conn *c);

/* Batch drain optimization: process multiple connections in one call
 * to reduce function call overhead in P=5~10 scenarios. */
int kvs_drain_batch(struct conn **conns, int conn_count);

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
        /* Prefetch next command location to improve cache hit rate in pipeline scenarios.
         * Only prefetch when buffer has enough data to avoid unnecessary cache pollution. */
        if (__builtin_expect(remaining > 128, 1)) {
            __builtin_prefetch(rb->buf + off + 64, 0, 1);
        }

        /* Tolerate inter-command CRLF only (Redis strict-parses; keep mild tolerance) */
        while (remaining > 0 && (rb->buf[off] == '\r' || rb->buf[off] == '\n')) {
            off++; remaining--;
        }
        if (remaining <= 0) break;

        int argc = 0;
        int cmd_len = resp_parse_slices(rb->buf + off, remaining,
                                        tokens, KVS_MAX_TOKENS, &argc);
        if (cmd_len > 0) {
            /* Check if this is a long write command that needs replication notification
             * >4KB commands are not captured by eBPF, so main process notifies replication */
            int need_repl_notify = (cmd_len > 4096) && (argc > 0) && is_write_command_fast(tokens[0]);
            const char *cmd_start = rb->buf + off;

            /* Phase 10.8 — wire-path latency sample using rdtsc-backed
             * fast clock (Phase 10.7).  Sampled 1-in-(LAT_SAMPLE_MASK+1)=64 to keep
             * hot-path overhead at ~0.2% even at 500k+ QPS: an A/B with
             * unconditional sampling showed an 11–13% regression at t=2
             * caused by per-dispatch rdtsc×2 + 3 atomic adds on the same
             * cacheline.  At 100k+ QPS 64x undersampling still leaves
             * >1500 samples/s — plenty for stable p999.  Replay paths
             * (AOF/WAL reload) call kvs_dispatch directly and bypass
             * this entirely. */
            static __thread uint32_t s_lat_tick = 0;
            const  uint32_t          LAT_SAMPLE_MASK = 63;
            int sample = kvs_config.latency_histogram_enabled
                         && ((s_lat_tick++ & LAT_SAMPLE_MASK) == 0);
            uint64_t _t0 = sample ? kvs_now_ns_fast() : 0;
            dispatch_result_t r = kvs_dispatch(tokens, argc,
                                               rb->buf + off, cmd_len, wb, c);
            if (sample) {
                uint64_t _t1 = kvs_now_ns_fast();
                kvs_lat_record_ns(_t1 - _t0);
            }

            /* Long write command: notify replication module after successful execution
             * Queue push is lock-free (~10ns), master replication thread will handle send */
            if (need_repl_notify && r == DISPATCH_OK) {
                repl_notify_push(cmd_start, (size_t)cmd_len);
            }

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

/* Batch drain optimization: process multiple connections to reduce overhead.
 * In P=5~10 scenarios, this reduces function call overhead by ~3-5%. */
int kvs_drain_batch(struct conn **conns, int conn_count) {
    if (!conns || conn_count <= 0) return 0;

    int total_cmds = 0;

    for (int i = 0; i < conn_count; i++) {
        struct conn *c = conns[i];
        if (!c || !c->active || c->read_buf.len == 0) continue;
        if (c->send_armed || c->write_buf.len > 0) continue;  // Skip if send in flight
        if (c->client && c->client->state == CLIENT_BLOCKED) continue;

        int drained = kvs_drain_resp_commands_off(
            &c->read_buf, &c->r_off, &c->write_buf, c
        );
        if (drained > 0) total_cmds += drained;
    }

    return total_cmds;
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
