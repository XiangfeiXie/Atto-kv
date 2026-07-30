/* =========================================================
 * persistence.c — path utils, shared buffer, dict scan,
 *                io_uring write, BGSAVE global state.
 *
 * RDB save/load → persist_rdb.c
 * WAL reload    → persist_aof.c
 * WAL write     → wal_persist.c
 * ========================================================= */

#define _GNU_SOURCE
#include "persistence.h"
#include "persist_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <libgen.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <liburing.h>


/* ── 项目根目录：由 persist_set_basedir 在启动时设置 ── */
static char  g_basedir[1024] = ".";
static int   g_basedir_set = 0;

/* 将相对路径拼接到项目根目录上，返回静态 buffer */
const char *persist_path(const char *rel) {
    static char buf[1024];
    int n = snprintf(buf, sizeof(buf), "%s/%s", g_basedir, rel);
    if (n < 0 || (size_t)n >= sizeof(buf)) return rel;
    return buf;
}

void persist_set_basedir(const char *argv0) {
    if (g_basedir_set) return;

    /* 1) 解析二进制真实路径 */
    char binpath[1024] = {0};
    if (argv0 && argv0[0] == '/') {
        strncpy(binpath, argv0, sizeof(binpath) - 1);
    } else if (argv0) {
        ssize_t n = readlink("/proc/self/exe", binpath, sizeof(binpath) - 1);
        if (n <= 0 && realpath(argv0, binpath) == NULL) {
            if (getcwd(binpath, sizeof(binpath) - 1)) {
                size_t blen = strlen(binpath);
                snprintf(binpath + blen, sizeof(binpath) - blen - 1, "/%s", argv0);
            }
        }
    } else {
        ssize_t n = readlink("/proc/self/exe", binpath, sizeof(binpath) - 1);
        if (n > 0) binpath[n] = '\0';
    }

    /* 2) 取二进制所在目录 */
    char *d = dirname(binpath);

    char dircopy[1024];
    strncpy(dircopy, binpath, sizeof(dircopy) - 1);
    dircopy[sizeof(dircopy) - 1] = '\0';
    char *dd = dirname(dircopy);
    char *dn = strrchr(dd, '/');
    if (dn) dn++; else dn = dd;

    if (strcmp(dn, "build") == 0) {
        char *last = strrchr(dd, '/');
        if (last) *last = '\0';
        strncpy(g_basedir, dd, sizeof(g_basedir) - 1);
    } else {
        strncpy(g_basedir, dd, sizeof(g_basedir) - 1);
    }

    g_basedir[sizeof(g_basedir) - 1] = '\0';
    g_basedir_set = 1;
}


/* =============================================================================
 * Part 1. Shared buffer & dict scan
 * ============================================================================= */

void rdb_buf_reserve(rdb_buf_t *b, size_t need) {
    if (b->len + need <= b->cap) return;

    size_t ncap = b->cap ? b->cap : 64 * 1024;

    if (b->len + need > ncap * 2) {
        ncap = b->len + need + (need >> 2);
    } else {
        while (ncap < b->len + need)
            ncap *= 2;
    }

    b->buf = (char *)realloc(b->buf, ncap);
    b->cap = ncap;
}

void rdb_buf_put(rdb_buf_t *b, const void *p, size_t n) {
    rdb_buf_reserve(b, n);
    memcpy(b->buf + b->len, p, n);
    b->len += n;
}

void store_scan(void (*cb)(kv_entry_t *e, void *priv), void *priv) {
    kv_store_iter_t it;
    kv_store_iter_init(&it);
    kv_entry_t *e;
    while ((e = kv_store_iter_next(&it)) != NULL)
        cb(e, priv);
    kv_store_iter_free(&it);
}


/* =============================================================================
 * Part 3. io_uring chunked write + fdatasync (shared by RDB and AOF)
 * ============================================================================= */

void rdb_write_uring(int fd, const char *data, size_t total) {
    const size_t CHUNK = 1 * 1024 * 1024;
    const int    DEPTH = 128;

    struct io_uring ring;
    if (io_uring_queue_init(DEPTH, &ring, 0) < 0) {
        size_t off = 0;
        while (off < total) {
            ssize_t w = write(fd, data + off, total - off);
            if (w <= 0) {
                fprintf(stderr, "[RDB] fallback write failed at offset %zu: %s\n",
                        off, strerror(errno));
                return;
            }
            off += (size_t)w;
        }
        return;
    }

    size_t off = 0;
    int    inflight = 0;
    int    write_error = 0;

    while (off < total || inflight > 0) {
        while (off < total && inflight < DEPTH && !write_error) {
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
            if (!sqe) break;
            size_t n = total - off;
            if (n > CHUNK) n = CHUNK;
            io_uring_prep_write(sqe, fd, data + off, n, off);
            sqe->user_data = n;
            off += n;
            inflight++;
        }

        if (inflight > 0) {
            io_uring_submit(&ring);

            while (inflight > 0) {
                struct io_uring_cqe *cqe = NULL;
                int ret = io_uring_peek_cqe(&ring, &cqe);

                if (ret == -EAGAIN) {
                    ret = io_uring_wait_cqe(&ring, &cqe);
                    if (ret < 0) {
                        fprintf(stderr, "[RDB] io_uring_wait_cqe failed: %s\n", strerror(-ret));
                        write_error = 1;
                        break;
                    }
                }

                if (cqe) {
                    if (cqe->res < 0) {
                        fprintf(stderr, "[RDB] uring write error: %s\n", strerror(-cqe->res));
                        write_error = 1;
                        io_uring_cqe_seen(&ring, cqe);
                        inflight--;
                        break;
                    }

                    size_t expected = (size_t)cqe->user_data;
                    if ((size_t)cqe->res != expected) {
                        fprintf(stderr, "[RDB] short write: %d/%zu bytes\n", cqe->res, expected);
                        write_error = 1;
                        io_uring_cqe_seen(&ring, cqe);
                        inflight--;
                        break;
                    }

                    io_uring_cqe_seen(&ring, cqe);
                    inflight--;
                }
            }

            if (write_error) {
                io_uring_queue_exit(&ring);
                return;
            }
        }
    }

    {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        if (sqe) {
            io_uring_prep_fsync(sqe, fd, IORING_FSYNC_DATASYNC);
            sqe->flags |= IOSQE_IO_DRAIN;
            io_uring_submit(&ring);
            struct io_uring_cqe *cqe = NULL;
            if (io_uring_wait_cqe(&ring, &cqe) == 0) {
                if (cqe->res < 0) {
                    fprintf(stderr, "[RDB] fsync failed: %s\n", strerror(-cqe->res));
                }
                io_uring_cqe_seen(&ring, cqe);
            }
        } else {
            fdatasync(fd);
        }
    }
    io_uring_queue_exit(&ring);
}


/* =============================================================================
 * Part 4. BGSAVE status check & shared SIGCHLD handler
 * ============================================================================= */

int rdb_bgsave_in_progress(void) {
    return (g_rdb_child_pid > 0) ? 1 : 0;
}

/* SIGCHLD 处理：回收已完成的 BGSAVE 子进程 */
void rdb_sigchld(int signo) {
    (void)signo;
    pid_t pid;
    int   status;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (pid == g_rdb_child_pid)
            g_rdb_child_pid = 0;
    }
}


/* =============================================================================
 * Part 5. Binary-safe read helper (shared by RDB load and AOF replay)
 * ============================================================================= */

const char *mread(const char *p, const char *end, void *out, size_t n) {
    if (p + n > end) return NULL;
    memcpy(out, p, n);
    return p + n;
}
