#define _GNU_SOURCE
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <liburing.h>
#include "proactor_types.h"
#include "proactor_inline.h"

/* Initialize listen socket */
static int init_listen_socket(unsigned short port, int reuseport) {
    int sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (sockfd < 0) return -1;

    struct sockaddr_in serveraddr;
    memset(&serveraddr, 0, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
    serveraddr.sin_port = htons(port);

    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    if (reuseport)
        setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

    if (bind(sockfd, (struct sockaddr*)&serveraddr, sizeof(serveraddr)) == -1) {
        perror("bind");
        close(sockfd);
        return -1;
    }
    listen(sockfd, SOMAXCONN);
    return sockfd;
}

/* Setup io_uring with fallback strategy */
static int setup_io_uring(proactor_worker_t *w) {
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    /* SQPOLL configuration */
    params.flags = IORING_SETUP_SQPOLL | IORING_SETUP_SINGLE_ISSUER;
    params.sq_thread_idle = 200;
    int qret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);

    if (qret == -EINVAL) {
        memset(&params, 0, sizeof(params));
        params.flags = IORING_SETUP_SINGLE_ISSUER
                     | IORING_SETUP_DEFER_TASKRUN
                     | IORING_SETUP_COOP_TASKRUN;
        qret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);
    }
    if (qret == -EINVAL) {
        memset(&params, 0, sizeof(params));
        params.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN;
        qret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);
    }
    if (qret == -EINVAL) {
        memset(&params, 0, sizeof(params));
        params.flags = IORING_SETUP_SINGLE_ISSUER;
        qret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);
    }
    if (qret == -EINVAL) {
        memset(&params, 0, sizeof(params));
        params.flags = 0;
        qret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);
    }
    if (qret < 0) {
        fprintf(stderr, "[proactor#%d] io_uring init: %s\n",
                w->id, strerror(-qret));
        return -1;
    }

    w->sqpoll_enabled = (params.flags & IORING_SETUP_SQPOLL) ? 1 : 0;

    printf("[proactor] io_uring flags: 0x%x%s\n",
           params.flags,
           w->sqpoll_enabled ? " (SQPOLL enabled - reduced syscalls)" : "");

    return 0;
}

/* Setup buffer ring */
static int setup_buffer_ring(proactor_worker_t *w) {
    w->buf_pool_bytes = (size_t)KVS_NR_BUFS * KVS_BUF_SIZE;
    w->buf_base = mmap(NULL, w->buf_pool_bytes, PROT_READ | PROT_WRITE,
                       MAP_ANON | MAP_PRIVATE, -1, 0);
    if (w->buf_base == MAP_FAILED) {
        perror("[proactor] mmap buf pool");
        return -1;
    }

    int br_err = 0;
    w->buf_ring = io_uring_setup_buf_ring(&w->ring, KVS_NR_BUFS,
                                          KVS_BUF_RING_BGID, 0, &br_err);
    if (!w->buf_ring) {
        fprintf(stderr, "[proactor#%d] setup_buf_ring: %s\n",
                w->id, strerror(-br_err));
        munmap(w->buf_base, w->buf_pool_bytes);
        return -1;
    }

    int br_mask = io_uring_buf_ring_mask(KVS_NR_BUFS);
    for (int i = 0; i < KVS_NR_BUFS; i++) {
        io_uring_buf_ring_add(w->buf_ring,
                              w->buf_base + (size_t)i * KVS_BUF_SIZE,
                              KVS_BUF_SIZE,
                              (unsigned short)i, br_mask, i);
    }
    io_uring_buf_ring_advance(w->buf_ring, KVS_NR_BUFS);

    printf("[proactor] buf ring: %d x %d B = %zu MB per worker\n",
           KVS_NR_BUFS, KVS_BUF_SIZE, w->buf_pool_bytes / (1024 * 1024));

    return 0;
}

/* Initialize worker */
int worker_init(proactor_worker_t *w, unsigned short port, int worker_id) {
    memset(w, 0, sizeof(*w));
    w->id = worker_id;
    w->listen_fd = -1;

    w->listen_fd = init_listen_socket(port, 0);
    if (w->listen_fd < 0) {
        fprintf(stderr, "[proactor#%d] failed to init listen socket\n", w->id);
        return -1;
    }

    if (setup_io_uring(w) < 0) {
        close(w->listen_fd);
        return -1;
    }

    if (setup_buffer_ring(w) < 0) {
        io_uring_queue_exit(&w->ring);
        close(w->listen_fd);
        return -1;
    }

    return 0;
}

/* Destroy worker */
void worker_destroy(proactor_worker_t *w) {
    if (w->buf_ring) {
        io_uring_free_buf_ring(&w->ring, w->buf_ring, KVS_NR_BUFS, KVS_BUF_RING_BGID);
        w->buf_ring = NULL;
    }
    if (w->buf_base && w->buf_base != MAP_FAILED) {
        munmap(w->buf_base, w->buf_pool_bytes);
        w->buf_base = NULL;
    }
    io_uring_queue_exit(&w->ring);
    if (w->listen_fd >= 0) {
        close(w->listen_fd);
        w->listen_fd = -1;
    }
}
