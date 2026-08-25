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
    /* offset == head 是"已追平"（全量快照刚做完、期间无新写入）的合法位置，
     * 应回 +CONTINUE 进入 streaming 而非 +FULLRESYNC，故用 <= 包含右边界。 */
    return (offset >= earliest && offset <= g_backlog.head) ? 1 : 0;
}

/* ── Backlog fetch ─────────────────────────────────────────────────────
 *
 * 从 start_offset 起拷贝最多 max_len 字节。允许在任意字节处截断，命令
 * 边界由 slave 端的 resp_parse_slices 重组（一个大于缓冲区的命令会分
 * 多批送达）。返回写入字节数，*out_end_offset 为拷贝后的新偏移；
 * start_offset 不在窗口内返回 -1。                                    */

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

int repl_backlog_fetch(size_t start_offset, char *dst, int max_len,
                        size_t *out_end_offset) {
    if (!repl_backlog_valid(start_offset))
        return -1;

    size_t avail = g_backlog.head - start_offset;
    size_t n = (avail < (size_t)max_len) ? avail : (size_t)max_len;
    backlog_copy(dst, start_offset, n);
    *out_end_offset = start_offset + n;
    return (int)n;
}

