/**
 * tcp_repl_master.c — TCP sendfile 零拷贝全量同步 Master
 *
 * fork 子进程: listen → accept → SAVE → sendfile() → close
 */

#define _GNU_SOURCE
#include "tcp_repl.h"
#include "kvstore.h"
#include "persistence.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/sendfile.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>

int tcp_master_fullsync_child(uint64_t base_seq, int ready_fd) {
    (void)base_seq;
    setlinebuf(stdout); setlinebuf(stderr);
    int port = kvs_config.rdma_listen_port + 1;

    /* 1. listen — 先启动让 slave 能连上 */
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) { perror("socket"); return -1; }
    int one = 1; setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in addr = { .sin_family = AF_INET,
                                .sin_addr.s_addr = INADDR_ANY,
                                .sin_port = htons((uint16_t)port) };
    if (bind(lfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(lfd); return -1;
    }
    if (listen(lfd, 1) < 0) { perror("listen"); close(lfd); return -1; }
    printf("[tcp-master] listening :%d\n", port);
    if (ready_fd >= 0) { char r=1; (void)write(ready_fd, &r, 1); close(ready_fd); }

    /* 2. accept slave */
    int cfd = accept(lfd, NULL, NULL);
    if (cfd < 0) { perror("accept"); close(lfd); return -1; }
    printf("[tcp-master] slave connected\n");

    /* 3. SAVE → open RDB */
    printf("[tcp-master] SAVE ...\n");
    rdb_save_sync();
    const char *path = persist_path("build/dump.rdb");
    printf("[tcp-master] opening %s ...\n", path);
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); close(cfd); close(lfd); return -1; }
    struct stat st; fstat(fd, &st);
    uint64_t file_size = (uint64_t)st.st_size;
    printf("[tcp-master] RDB: %lu bytes\n", (unsigned long)file_size);

    /* 4. sendfile: size(8B) + data */
    uint64_t sz_be = htobe64(file_size);
    if (send(cfd, &sz_be, 8, 0) != 8) { perror("send size"); goto out; }

    struct timespec t1, t2; clock_gettime(CLOCK_MONOTONIC, &t1);
    off_t off = 0;
    ssize_t sent = sendfile(cfd, fd, &off, (size_t)file_size);
    clock_gettime(CLOCK_MONOTONIC, &t2);

    if (sent < 0) { perror("sendfile"); sent = 0; }
    double ms = (t2.tv_sec-t1.tv_sec)*1000.0 + (t2.tv_nsec-t1.tv_nsec)/1e6;
    double mbps = (ms>0) ? ((double)sent / (ms/1000.0) / (1024.0*1024.0)) : 0;
    printf("[tcp-master] xfer-end: bytes=%zd ms=%.1f mbps=%.2f\n", sent, ms, mbps);

out:
    close(fd); close(cfd); close(lfd);
    return 0;
}
