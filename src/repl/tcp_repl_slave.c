/**
 * tcp_repl_slave.c — TCP sendfile 全量同步 Slave
 *
 * fork 子进程: connect → recv size+data → mmap → rdb_load_from_buf
 */

#define _GNU_SOURCE
#include "tcp_repl.h"
#include "kvstore.h"
#include "persistence.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/time.h>

/* recv 全量字节到文件 fd；返回实际收到的字节数，或 -1（真实错误）。
 * EOF（对端提前关闭）返回 < total，由调用方判定并报错。 */
static ssize_t recv_all(int sfd, int dfd, size_t total) {
    char buf[65536];
    size_t left = total;
    size_t got = 0;
    while (left > 0) {
        size_t chunk = left < sizeof(buf) ? left : sizeof(buf);
        ssize_t n = recv(sfd, buf, chunk, MSG_WAITALL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) break;  /* EOF：连接提前关闭，数据不完整 */
        if (write(dfd, buf, (size_t)n) != n) return -1;
        left -= (size_t)n;
        got  += (size_t)n;
    }
    return (ssize_t)got;
}

int tcp_slave_fullsync(void) {
    setlinebuf(stdout); setlinebuf(stderr);
    const char *ip = kvs_config.rdma_master_ip;
    int port = kvs_config.rdma_master_port + 1;  /* master 子进程的数据端口 */
    char pstr[16]; snprintf(pstr, sizeof(pstr), "%d", port);

    printf("[tcp-slave] %s:%s ...\n", ip, pstr);

    /* connect（重试等 master 子进程就绪） */
    struct addrinfo hints = {0}, *ai;
    hints.ai_family = AF_INET; hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(ip, pstr, &hints, &ai) != 0) { perror("getaddrinfo"); return -1; }
    int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) { perror("socket"); freeaddrinfo(ai); return -1; }

    int ok = 0;
    for (int retry = 0; retry < 30; retry++) {
        if (connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) { ok = 1; break; }
        usleep(200000);
    }
    freeaddrinfo(ai);
    if (!ok) { perror("connect"); close(fd); return -1; }

    /* Signal master that we are ready (so master can start SAVE + sendfile timer) */
    { char ready = 1; if (send(fd, &ready, 1, 0) != 1) { perror("send ready"); close(fd); return -1; } }

    /* recv size */
    uint64_t fsz = 0;
    if (recv(fd, &fsz, 8, MSG_WAITALL) != 8) { perror("recv size"); close(fd); return -1; }
    fsz = be64toh(fsz);
    printf("[tcp-slave] %lu bytes\n", (unsigned long)fsz);

    /* recv data → tmp */
    char tmp[256]; snprintf(tmp, sizeof(tmp), "/tmp/kvstore_fs_%d.rdb", getpid());
    int tfd = open(tmp, O_CREAT|O_WRONLY|O_TRUNC, 0644);
    if (tfd < 0) { perror("open tmp"); close(fd); return -1; }

    struct timespec t1, t2; clock_gettime(CLOCK_MONOTONIC, &t1);
    ssize_t got = recv_all(fd, tfd, (size_t)fsz);
    clock_gettime(CLOCK_MONOTONIC, &t2);
    close(fd); close(tfd);
    if (got < 0) { perror("recv data"); return -1; }
    if ((size_t)got != fsz) {
        fprintf(stderr, "[tcp-slave] connection closed early: got %zd/%lu bytes\n",
                got, (unsigned long)fsz);
        return -1;
    }

    double ms = (t2.tv_sec-t1.tv_sec)*1000.0 + (t2.tv_nsec-t1.tv_nsec)/1e6;
    double mbps = (ms>0) ? ((double)fsz / (ms/1000.0) / (1024.0*1024.0)) : 0;
    printf("[tcp-slave] xfer-end: bytes=%lu ms=%.1f mbps=%.2f\n",
           (unsigned long)fsz, ms, mbps);

    /* mmap + load */
    int rfd = open(tmp, O_RDONLY);
    if (rfd < 0) { perror("open tmp load"); return -1; }
    struct stat st; fstat(rfd, &st);
    char *mbuf = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, rfd, 0);
    close(rfd); unlink(tmp);
    if (mbuf == MAP_FAILED) { perror("mmap"); return -1; }

    int loaded = rdb_load_from_buf(mbuf, (size_t)st.st_size);
    munmap(mbuf, (size_t)st.st_size);
    printf("[tcp-slave] loaded %d keys\n", loaded);
    return 0;
}
