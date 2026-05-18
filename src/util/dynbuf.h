#ifndef __DYN_BUF_H__
#define __DYN_BUF_H__

/* Lightweight dynamic buffer — single contiguous allocation with len/cap. */

typedef struct {
    char *buf;
    int   len;
    int   cap;
} DynBuf;

void dynbuf_init(DynBuf *db, int init_cap);
int  dynbuf_reserve(DynBuf *db, int total_cap);
int  dynbuf_append(DynBuf *db, const char *data, int len);
int  dynbuf_cat(DynBuf *db, const char *data, int len);
void dynbuf_clear(DynBuf *db);
void dynbuf_free(DynBuf *db);

/* Compile-time-safe variant: emit a string literal and derive the byte
 * count from sizeof.  Eliminates the entire class of hand-counted-length
 * bugs (truncated RESP frames missing `\r\n`, UTF-8 multibyte miscounts,
 * stale lengths after edits).  Must be used ONLY with a true string
 * literal — passing a `char *` would give pointer width, not strlen.
 *
 *   dynbuf_lit(wb, "-ERR foo\r\n");    // OK
 *   dynbuf_lit(wb, some_char_ptr);     // WRONG — use dynbuf_cat
 *
 * Rationale: the legacy dynbuf_cat(wb, "lit", N) idiom required manual
 * byte counts.  A codebase audit (Phase 7.2) found 41 mis-sized literals
 * out of ~100 call sites — 33 of them truncated the `\r\n` terminator
 * and would hang any Redis client waiting for the missing newline.
 * This macro makes that bug class impossible.
 */
#define dynbuf_lit(wb, s) dynbuf_cat((wb), "" s, (int)(sizeof(s) - 1))

#endif
