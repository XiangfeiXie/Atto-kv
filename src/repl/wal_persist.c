#include "wal_persist.h"
#include "persistence.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

static pthread_mutex_t g_wal_mu = PTHREAD_MUTEX_INITIALIZER;
static int g_wal_fd = -1;

static int wal_open_locked(void)
{
    if (g_wal_fd >= 0) return 0;
    const char *path = persist_path(WAL_REL);
    g_wal_fd = open(path, O_CREAT | O_APPEND | O_WRONLY | O_CLOEXEC, 0644);
    if (g_wal_fd < 0) {
        fprintf(stderr, "[WAL-PERSIST] open %s failed: %s\n", path, strerror(errno));
        return -1;
    }
    return 0;
}

int kvs_wal_persist_start(void)
{
    pthread_mutex_lock(&g_wal_mu);
    int rc = wal_open_locked();
    pthread_mutex_unlock(&g_wal_mu);
    return rc;
}

int kvs_wal_persist_append(const kvs_journal_record_t *rec)
{
    if (!rec || !rec->payload || rec->len == 0) return -1;
    pthread_mutex_lock(&g_wal_mu);
    if (wal_open_locked() != 0) {
        pthread_mutex_unlock(&g_wal_mu);
        return -1;
    }

    kvs_wal2_record_hdr_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic = KVS_WAL2_MAGIC;
    hdr.version = KVS_REPL_FRAME_VERSION;
    hdr.kind = rec->kind;
    hdr.flags = rec->flags;
    hdr.seq = rec->seq;
    hdr.len = rec->len;
    hdr.crc32 = rec->crc32;

    struct iovec iov[2];
    iov[0].iov_base = &hdr;
    iov[0].iov_len = sizeof(hdr);
    iov[1].iov_base = rec->payload;
    iov[1].iov_len = rec->len;
    ssize_t n = writev(g_wal_fd, iov, 2);
    int ok = (n == (ssize_t)(sizeof(hdr) + rec->len)) ? 0 : -1;
    if (ok != 0)
        fprintf(stderr, "[WAL-PERSIST] writev failed: %s\n", strerror(errno));
    pthread_mutex_unlock(&g_wal_mu);
    return ok;
}

void kvs_wal_persist_stop(void)
{
    pthread_mutex_lock(&g_wal_mu);
    if (g_wal_fd >= 0) {
        fdatasync(g_wal_fd);
        close(g_wal_fd);
        g_wal_fd = -1;
    }
    pthread_mutex_unlock(&g_wal_mu);
}
