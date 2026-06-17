/* =========================================================
 * persist_aof.c — AOF background rewrite (fork + io_uring),
 *                 AOF reload (mmap WAL + legacy AOF).
 *
 * Depends on persistence.c for path utils, store_scan,
 * rdb_write_uring, and global BGSAVE/AOF state.
 * ========================================================= */

#include "persistence.h"
#include "persist_internal.h"
#include "kvstore.h"
#include "wal_persist.h"
#include "kv_type_hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <inttypes.h>
#include <sys/mman.h>
#include <sys/stat.h>

/* ── AOF rewrite state ── */
volatile pid_t g_aof_rewrite_child_pid = 0;
int g_aof_rewrite_in_progress = 0;

aof_rewrite_buf_t g_aof_rewrite_buf = {0};
pthread_mutex_t g_aof_rewrite_mu = PTHREAD_MUTEX_INITIALIZER;

/* =============================================================================
 * Part 1. AOF rewrite (fork-based, same pattern as RDB BGSAVE)
 * ============================================================================= */

/* Append to rewrite buffer (called during rewrite) */
void aof_rewrite_buf_append(const char *data, size_t len) {
    pthread_mutex_lock(&g_aof_rewrite_mu);
    if (!g_aof_rewrite_in_progress) {
        pthread_mutex_unlock(&g_aof_rewrite_mu);
        return;
    }

    if (g_aof_rewrite_buf.len + len > g_aof_rewrite_buf.cap) {
        size_t new_cap = g_aof_rewrite_buf.cap ? g_aof_rewrite_buf.cap * 2 : 64 * 1024;
        while (new_cap < g_aof_rewrite_buf.len + len) new_cap *= 2;
        char *new_data = realloc(g_aof_rewrite_buf.data, new_cap);
        if (!new_data) {
            pthread_mutex_unlock(&g_aof_rewrite_mu);
            return;
        }
        g_aof_rewrite_buf.data = new_data;
        g_aof_rewrite_buf.cap = new_cap;
    }

    memcpy(g_aof_rewrite_buf.data + g_aof_rewrite_buf.len, data, len);
    g_aof_rewrite_buf.len += len;
    pthread_mutex_unlock(&g_aof_rewrite_mu);
}

/* Generate canonical RESP command for an entry */
void aof_rewrite_entry(kv_entry_t *e, void *priv) {
    if (!e) return;
    rdb_buf_t *b = (rdb_buf_t *)priv;

    const char *key = entry_key(e);
    int klen = (int)e->key_len;
    long long expire = e->expire;

    if (entry_type(e) == KV_TYPE_STRING) {
        const char *val = e->data;
        int vlen = (int)e->val_len;

        char header[256];
        int hlen = snprintf(header, sizeof(header),
                           "*3\r\n$3\r\nSET\r\n$%d\r\n", klen);
        rdb_buf_put(b, header, hlen);
        rdb_buf_put(b, key, klen);
        rdb_buf_put(b, "\r\n", 2);

        hlen = snprintf(header, sizeof(header), "$%d\r\n", vlen);
        rdb_buf_put(b, header, hlen);
        rdb_buf_put(b, val, vlen);
        rdb_buf_put(b, "\r\n", 2);

        if (expire > 0) {
            hlen = snprintf(header, sizeof(header),
                           "*3\r\n$8\r\nEXPIREAT\r\n$%d\r\n", klen);
            rdb_buf_put(b, header, hlen);
            rdb_buf_put(b, key, klen);
            rdb_buf_put(b, "\r\n", 2);

            hlen = snprintf(header, sizeof(header), "$%lld\r\n%lld\r\n",
                           (long long)snprintf(NULL, 0, "%lld", expire), expire);
            rdb_buf_put(b, header, hlen);
        }
    }
}

int aof_rewrite_bg(void) {
    static int installed = 0;
    if (!installed) {
        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = rdb_sigchld;
        sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
        sigaction(SIGCHLD, &sa, NULL);
        installed = 1;
    }

    if (g_aof_rewrite_child_pid > 0) {
        printf("[AOF] BGREWRITEAOF already in progress (pid=%d), skipping\n",
               (int)g_aof_rewrite_child_pid);
        return 0;
    }

    if (g_rdb_child_pid > 0) {
        printf("[AOF] BGSAVE in progress, cannot start BGREWRITEAOF\n");
        return -1;
    }

    pthread_mutex_lock(&g_aof_rewrite_mu);
    g_aof_rewrite_in_progress = 1;
    g_aof_rewrite_buf.len = 0;
    if (!g_aof_rewrite_buf.data) {
        g_aof_rewrite_buf.cap = 64 * 1024;
        g_aof_rewrite_buf.data = malloc(g_aof_rewrite_buf.cap);
    }
    pthread_mutex_unlock(&g_aof_rewrite_mu);

    kv_store_lock_for_snapshot();

    pid_t pid = fork();
    if (pid < 0) {
        fprintf(stderr, "[AOF] fork failed (%s)\n", strerror(errno));
        kv_store_unlock_snapshot();
        pthread_mutex_lock(&g_aof_rewrite_mu);
        g_aof_rewrite_in_progress = 0;
        pthread_mutex_unlock(&g_aof_rewrite_mu);
        return -1;
    }

    if (pid == 0) {
        g_rdb_in_fork_child = 1;

        rdb_buf_t b = {0};
        store_scan(aof_rewrite_entry, &b);

        const char *tmp_path = persist_path("build/appendonly.aof.tmp");
        int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            fprintf(stderr, "[AOF] child: open tmp failed: %s\n", strerror(errno));
            free(b.buf);
            _exit(1);
        }

        rdb_write_uring(fd, b.buf, b.len);
        close(fd);
        free(b.buf);

        printf("[AOF] child: wrote %zu bytes to temp AOF\n", b.len);
        fflush(stdout);
        fflush(stderr);
        _exit(0);
    }

    kv_store_unlock_snapshot();
    g_aof_rewrite_child_pid = pid;
    printf("[AOF] BGREWRITEAOF started, child pid=%d\n", (int)pid);

    return 0;
}

int aof_rewrite_finalize(void) {
    pthread_mutex_lock(&g_aof_rewrite_mu);

    if (!g_aof_rewrite_in_progress) {
        pthread_mutex_unlock(&g_aof_rewrite_mu);
        return -1;
    }

    const char *tmp_path = persist_path("build/appendonly.aof.tmp");
    int fd = open(tmp_path, O_WRONLY | O_APPEND);
    if (fd < 0) {
        fprintf(stderr, "[AOF] finalize: open tmp failed: %s\n", strerror(errno));
        g_aof_rewrite_in_progress = 0;
        free(g_aof_rewrite_buf.data);
        g_aof_rewrite_buf.data = NULL;
        g_aof_rewrite_buf.len = 0;
        g_aof_rewrite_buf.cap = 0;
        pthread_mutex_unlock(&g_aof_rewrite_mu);
        return -1;
    }

    if (g_aof_rewrite_buf.len > 0) {
        size_t written = 0;
        while (written < g_aof_rewrite_buf.len) {
            ssize_t n = write(fd, g_aof_rewrite_buf.data + written,
                            g_aof_rewrite_buf.len - written);
            if (n <= 0) break;
            written += n;
        }
        printf("[AOF] finalize: appended %zu bytes from rewrite buffer\n", written);
    }

    fdatasync(fd);
    close(fd);

    const char *aof_path = persist_path(WAL_REL);
    if (rename(tmp_path, aof_path) < 0) {
        fprintf(stderr, "[AOF] finalize: rename failed: %s\n", strerror(errno));
        g_aof_rewrite_in_progress = 0;
        free(g_aof_rewrite_buf.data);
        g_aof_rewrite_buf.data = NULL;
        g_aof_rewrite_buf.len = 0;
        g_aof_rewrite_buf.cap = 0;
        pthread_mutex_unlock(&g_aof_rewrite_mu);
        return -1;
    }

    g_aof_rewrite_in_progress = 0;
    free(g_aof_rewrite_buf.data);
    g_aof_rewrite_buf.data = NULL;
    g_aof_rewrite_buf.len = 0;
    g_aof_rewrite_buf.cap = 0;

    pthread_mutex_unlock(&g_aof_rewrite_mu);
    printf("[AOF] BGREWRITEAOF completed successfully\n");
    return 0;
}


/* =============================================================================
 * Part 2. AOF / WAL reload (mmap)
 * ============================================================================= */

void aof_reload_mmap(void) {
    int fd = open(persist_path(WAL_REL), O_RDONLY);
    int wal_format = (fd >= 0);
    if (!wal_format)
        fd = open(persist_path(AOF_REL), O_RDONLY);
    if (fd < 0) return;

    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size <= 0) { close(fd); return; }
    size_t fsz = (size_t)st.st_size;

    char *base = (char *)mmap(NULL, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
    int use_mmap = (base != MAP_FAILED);

    if (!use_mmap) {
        printf("[AOF] mmap failed: %s, falling back to read()\n", strerror(errno));

        base = malloc(fsz);
        if (!base) { close(fd); return; }

        lseek(fd, 0, SEEK_SET);
        size_t total_read = 0;
        while (total_read < fsz) {
            ssize_t n = read(fd, base + total_read, fsz - total_read);
            if (n <= 0) break;
            total_read += n;
        }
        close(fd);

        if (total_read != fsz) {
            printf("[AOF] read failed: only read %zu/%zu bytes\n", total_read, fsz);
            free(base);
            return;
        }
    } else {
        close(fd);
        madvise(base, fsz, MADV_SEQUENTIAL);
    }

    printf("[AOF] %s load start, size=%zu\n",
           wal_format ? "WAL" : "legacy AOF", fsz);

    g_in_reload = 1;
    DynBuf wb;
    dynbuf_init(&wb, BUF_INIT_CAP);
    kvs_token_t tokens[KVS_MAX_TOKENS];
    int loaded = 0;

    if (wal_format) {
        const char *p   = base;
        const char *end = base + fsz;
        while (p + sizeof(kvs_wal2_record_hdr_t) <= end) {
            kvs_wal2_record_hdr_t hdr;
            memcpy(&hdr, p, sizeof(hdr));
            if (hdr.magic != KVS_WAL2_MAGIC) break;
            if (p + sizeof(kvs_wal2_record_hdr_t) + hdr.len > end) break;
            const char *payload = p + sizeof(kvs_wal2_record_hdr_t);
            uint32_t computed = kvs_crc32c(0, &hdr.seq, sizeof(hdr.seq));
            computed = kvs_crc32c(computed, &hdr.kind, sizeof(hdr.kind));
            computed = kvs_crc32c(computed, &hdr.flags, sizeof(hdr.flags));
            computed = kvs_crc32c(computed, &hdr.len, sizeof(hdr.len));
            computed = kvs_crc32c(computed, payload, (size_t)hdr.len);
            if (hdr.crc32 != computed) {
                fprintf(stderr, "[AOF] WAL CRC32 mismatch seq=%" PRIu64
                        ", stopping replay\n", hdr.seq);
                break;
            }
            int argc = 0;
            int cmd_len = resp_parse_slices(payload, (int)hdr.len,
                                            tokens, KVS_MAX_TOKENS, &argc);
            if (cmd_len > 0) {
                dynbuf_clear(&wb);
                kvs_dispatch(tokens, argc, payload, cmd_len, &wb, NULL);
                loaded++;
            }
            p += sizeof(kvs_wal2_record_hdr_t) + hdr.len;
        }
    } else {
        const char *p = base;
        long remaining = (long)fsz;
        while (remaining > 0) {
            int argc = 0;
            int cmd_len = resp_parse_slices(p, (int)remaining,
                                            tokens, KVS_MAX_TOKENS, &argc);
            if (cmd_len <= 0) break;
            dynbuf_clear(&wb);
            kvs_dispatch(tokens, argc, p, cmd_len, &wb, NULL);
            p += cmd_len;
            remaining -= cmd_len;
            loaded++;
        }
    }

    dynbuf_free(&wb);

    if (use_mmap) {
        munmap(base, fsz);
    } else {
        free(base);
    }

    g_in_reload = 0;
    printf("[AOF] load finished, %d commands replayed\n", loaded);
}
