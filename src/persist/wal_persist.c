#include "wal_persist.h"
#include "persistence.h"

#include <errno.h>
#include <fcntl.h>
#include <liburing.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

static pthread_mutex_t g_wal_mu = PTHREAD_MUTEX_INITIALIZER;
static int g_wal_fd = -1;
static struct io_uring g_wal_ring;
static int g_wal_ring_ready = 0;

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

static int wal_ring_init_locked(void)
{
    if (g_wal_ring_ready) return 0;
    int rc = io_uring_queue_init(64, &g_wal_ring, 0);
    if (rc < 0) {
        fprintf(stderr, "[WAL-PERSIST] io_uring_queue_init failed: %s\n", strerror(-rc));
        return -1;
    }
    g_wal_ring_ready = 1;
    return 0;
}

static int wal_wait_cqe_locked(void)
{
    struct io_uring_cqe *cqe = NULL;
    int rc = io_uring_wait_cqe(&g_wal_ring, &cqe);
    if (rc < 0) {
        fprintf(stderr, "[WAL-PERSIST] wait_cqe failed: %s\n", strerror(-rc));
        return -1;
    }
    int res = cqe->res;
    io_uring_cqe_seen(&g_wal_ring, cqe);
    if (res < 0) {
        fprintf(stderr, "[WAL-PERSIST] async IO failed: %s\n", strerror(-res));
        return -1;
    }
    return res;
}

int kvs_wal_persist_start(void)
{
    pthread_mutex_lock(&g_wal_mu);
    int rc = wal_open_locked();
    if (rc == 0) rc = wal_ring_init_locked();
    pthread_mutex_unlock(&g_wal_mu);
    return rc;
}

int kvs_wal_persist_append(const kvs_journal_record_t *rec)
{
    if (!rec || !rec->payload || rec->len == 0) return -1;
    pthread_mutex_lock(&g_wal_mu);
    if (wal_open_locked() != 0 || wal_ring_init_locked() != 0) {
        pthread_mutex_unlock(&g_wal_mu);
        return -1;
    }

    kvs_wal2_record_hdr_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic = KVS_WAL2_MAGIC;
    hdr.version = KVS_WAL2_VERSION;
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

    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_wal_ring);
    if (!sqe) {
        pthread_mutex_unlock(&g_wal_mu);
        return -1;
    }
    io_uring_prep_writev(sqe, g_wal_fd, iov, 2, -1);
    int rc = io_uring_submit(&g_wal_ring);
    if (rc < 0) {
        fprintf(stderr, "[WAL-PERSIST] io_uring_submit failed: %s\n", strerror(-rc));
        pthread_mutex_unlock(&g_wal_mu);
        return -1;
    }
    int written = wal_wait_cqe_locked();
    int ok = (written == (int)(sizeof(hdr) + rec->len)) ? 0 : -1;
    if (ok != 0 && written >= 0) {
        fprintf(stderr, "[WAL-PERSIST] short async write: %d/%zu\n",
                written, sizeof(hdr) + (size_t)rec->len);
    }
    pthread_mutex_unlock(&g_wal_mu);
    return ok;
}

void kvs_wal_persist_stop(void)
{
    pthread_mutex_lock(&g_wal_mu);
    if (g_wal_fd >= 0) {
        if (g_wal_ring_ready) {
            struct io_uring_sqe *sqe = io_uring_get_sqe(&g_wal_ring);
            if (sqe) {
                io_uring_prep_fsync(sqe, g_wal_fd, IORING_FSYNC_DATASYNC);
                if (io_uring_submit(&g_wal_ring) >= 0)
                    (void)wal_wait_cqe_locked();
            }
        } else {
            fdatasync(g_wal_fd);
        }
        close(g_wal_fd);
        g_wal_fd = -1;
    }
    if (g_wal_ring_ready) {
        io_uring_queue_exit(&g_wal_ring);
        g_wal_ring_ready = 0;
    }
    pthread_mutex_unlock(&g_wal_mu);
}
