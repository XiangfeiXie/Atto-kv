/* repl_incr.c — backlog and shared state */

#include "repl_incr.h"
#include <string.h>

/* ── Global state ───────────────────────────────────────────────────── */

static repl_backlog_t g_backlog;
size_t g_repl_snapshot_offset = 0;

/* ── Engine integration hooks ───────────────────────────────────────── */

int  (*repl_engine_register)(int fd, conn_kind_t kind) = NULL;
void (*repl_engine_writable)(int fd, int on) = NULL;
void (*repl_engine_close)(int fd) = NULL;

/* Register replication fds into the attached network engine. */
void repl_incr_attach(void) {
    if (!repl_engine_register) return;

    int lfd = repl_incr_master_listen_fd();
    if (lfd >= 0)
        repl_engine_register(lfd, CONN_KIND_REPL_LISTEN);

    int sfd = repl_incr_slave_link_fd();
    if (sfd >= 0)
        repl_engine_register(sfd, CONN_KIND_REPL_MASTER);
}

/* ── Backlog ─────────────────────────────────────────────────────────── */

void repl_backlog_init(void) {
    memset(&g_backlog, 0, sizeof(g_backlog));
}

void repl_backlog_append(const char *resp_data, int len) {
    if (len <= 0 || len > REPL_BACKLOG_SIZE) return;

    /* Circular write: split if wrapping around */
    size_t pos = g_backlog.head % REPL_BACKLOG_SIZE;
    size_t remain = REPL_BACKLOG_SIZE - pos;
    size_t wlen   = (size_t)len;

    if (wlen <= remain) {
        memcpy(g_backlog.buf + pos, resp_data, wlen);
    } else {
        memcpy(g_backlog.buf + pos, resp_data, remain);
        memcpy(g_backlog.buf, resp_data + remain, wlen - remain);
    }
    g_backlog.head += wlen;

    /* Cap used at REPL_BACKLOG_SIZE (oldest data overwritten) */
    if (g_backlog.used + wlen > REPL_BACKLOG_SIZE)
        g_backlog.used = REPL_BACKLOG_SIZE;
    else
        g_backlog.used += wlen;

}

size_t repl_backlog_offset(void) {
    return g_backlog.head;
}

int repl_backlog_valid(size_t offset) {
    size_t earliest;
    if (g_backlog.used < REPL_BACKLOG_SIZE)
        earliest = 0;
    else
        earliest = g_backlog.head - REPL_BACKLOG_SIZE;
    return (offset >= earliest && offset < g_backlog.head) ? 1 : 0;
}

/* ── Backlog fetch (scan RESP boundaries) ──────────────────────────────
 *
 * Walk from start_offset in the circular buffer, recognising complete
 * RESP commands.  Each command begins with '*' — parse the argc, then
 * skip argc bulk-string pairs.  Copy raw bytes to dst until max_len
 * would be exceeded.  Set *out_end_offset to the byte after the last
 * copied command.
 *
 * Returns: number of bytes written to dst, or -1 if start_offset is
 *          outside the valid backlog window.                           */

static char backlog_peek(size_t off) {
    return g_backlog.buf[off % REPL_BACKLOG_SIZE];
}

/* Copy 'len' bytes from absolute offset off into dst (handles wrap) */
static void backlog_copy(char *dst, size_t off, size_t len) {
    size_t pos = off % REPL_BACKLOG_SIZE;
    size_t remain = REPL_BACKLOG_SIZE - pos;
    if (len <= remain) {
        memcpy(dst, g_backlog.buf + pos, len);
    } else {
        memcpy(dst, g_backlog.buf + pos, remain);
        memcpy(dst + remain, g_backlog.buf, len - remain);
    }
}

static int resp_skip_one_bulk(size_t *poff, size_t end_off) {
    size_t off = *poff;
    if (off >= end_off) return -1;
    if (backlog_peek(off) != '$') return -1;
    off++;

    int bulk_len = 0;
    while (off < end_off) {
        char c = backlog_peek(off);
        off++;
        if (c == '\r') {
            if (off >= end_off || backlog_peek(off) != '\n') return -1;
            off++;  /* consume \n */
            break;
        }
        if (c < '0' || c > '9') return -1;
        bulk_len = bulk_len * 10 + (c - '0');
        if (bulk_len > 512 * 1024 * 1024) return -1; /* sanity */
    }

    off = off + (size_t)bulk_len;
    if (off + 2 > end_off) return -1;
    if (backlog_peek(off) != '\r' || backlog_peek(off + 1) != '\n') return -1;
    off += 2;

    *poff = off;
    return 0;
}

/* Walk one RESP command: parse *N, then skip N bulks.
 * *poff advances past the complete command. Returns 0 on success, -1 on error. */
static int resp_skip_command(size_t *poff, size_t end_off) {
    size_t off = *poff;
    if (off >= end_off) return -1;
    if (backlog_peek(off) != '*') return -1;
    off++;

    /* parse argc */
    int argc = 0;
    while (off < end_off) {
        char c = backlog_peek(off);
        off++;
        if (c == '\r') {
            if (off >= end_off || backlog_peek(off) != '\n') return -1;
            off++;
            break;
        }
        if (c < '0' || c > '9') return -1;
        argc = argc * 10 + (c - '0');
    }
    if (argc <= 0) return -1;

    /* skip each bulk argument */
    for (int i = 0; i < argc; i++) {
        if (resp_skip_one_bulk(&off, end_off) < 0) return -1;
    }

    *poff = off;
    return 0;
}

int repl_backlog_fetch(size_t start_offset, char *dst, int max_len,
                        size_t *out_end_offset) {
    if (!repl_backlog_valid(start_offset))
        return -1;

    size_t end_off  = g_backlog.head;
    size_t cmd_start = start_offset;
    size_t cmd_end   = start_offset;
    int    written   = 0;

    while (cmd_end < end_off) {
        /* try to skip one command */
        size_t peek = cmd_end;
        if (resp_skip_command(&peek, end_off) < 0)
            break;  /* incomplete command at tail */

        size_t cmd_len = peek - cmd_end;
        if (written + (int)cmd_len > max_len)
            break;  /* won't fit in output buffer */

        /* copy this command */
        backlog_copy(dst + written, cmd_end, cmd_len);
        written   += (int)cmd_len;
        cmd_end    = peek;
    }

    *out_end_offset = cmd_end;
    return written;
}

