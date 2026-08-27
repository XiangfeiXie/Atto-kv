/*=============================================================================
 *  sendfile.c — sendfile（TCP 零拷贝）传输方式
 *  
 *  职责：通过 Linux sendfile() 系统调用实现全量同步的零拷贝传输
 *  
 *  设计原则：
 *  1. 只负责传输字节，不含同步语义
 *  2. 服务端和客户端都在本文件实现
 *  3. 由 repl_fullsync.c 按配置开关直接调用
 *  
 *  协议格式（流头 + 数据）：
 *  - token（8 字节）：快照 offset，对端据此续传
 *  - size（8 字节）：RDB 文件大小
 *  - data：RDB 文件内容（通过 sendfile 零拷贝发送）
 *  
 *  适用场景：本地网络或低延迟要求不高的环境
 *============================================================================*/

#define _GNU_SOURCE
#include "sendfile.h"
#include "kvstore.h"
#include "persistence.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>

/*=============================================================================
 *  服务端实现
 *============================================================================*/

/**
 * sendfile 服务端入口
 * 
 * 流程：
 * 1. 在 fullsync_port 上监听 TCP 连接
 * 2. 接受客户端连接
 * 3. 等待客户端就绪信号
 * 4. 执行 rdb_save_sync（生成 RDB 快照）
 * 5. 发送 token（快照 offset）
 * 6. 发送文件大小
 * 7. 通过 sendfile 零拷贝发送 RDB 数据
 * 
 * @param token 快照 offset（会话元数据）
 * @param ready_fd 就绪管道 fd：监听成功后写 1 字节通知父进程
 * @return 0 成功，-1 失败
 */
int sendfile_server_run(uint64_t token, int ready_fd) {
    setlinebuf(stdout); setlinebuf(stderr);
    int port = kvs_config.fullsync_port;   /* 全量数据端口 */

    /*-------------------------------------------------------------------------
     *  1. 创建监听 socket
     *-------------------------------------------------------------------------*/
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) { perror("socket"); return -1; }
    int one = 1; 
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    
    struct sockaddr_in addr = { 
        .sin_family = AF_INET,
        .sin_addr.s_addr = INADDR_ANY,
        .sin_port = htons((uint16_t)port) 
    };
    if (bind(lfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(lfd); return -1;
    }
    if (listen(lfd, 1) < 0) { perror("listen"); close(lfd); return -1; }
    
    printf("[sendfile] listening :%d\n", port);
    
    /* 通知父进程：监听已就绪 */
    if (ready_fd >= 0) { 
        char r = 1; 
        ssize_t _wr = write(ready_fd, &r, 1); 
        (void)_wr; 
        close(ready_fd); 
    }

    /*-------------------------------------------------------------------------
     *  2. 接受客户端连接
     *-------------------------------------------------------------------------*/
    int cfd = accept(lfd, NULL, NULL);
    if (cfd < 0) { perror("accept"); close(lfd); return -1; }
    printf("[sendfile] client connected\n");

    /*-------------------------------------------------------------------------
     *  3. 等待客户端就绪信号
     *     客户端连接后发送 1 字节确认，服务端收到后才开始 SAVE
     *     目的是让客户端先准备好接收，避免数据发送时客户端未就绪
     *-------------------------------------------------------------------------*/
    printf("[sendfile] waiting for client ready signal...\n");
    { 
        char ready_sig; 
        if (recv(cfd, &ready_sig, 1, 0) != 1) { 
            perror("recv ready"); 
            goto out; 
        } 
    }

    /*-------------------------------------------------------------------------
     *  4. 执行 RDB 保存（同步方式）
     *     计时开始（与 RDMA 路径对齐，便于性能对比）
     *-------------------------------------------------------------------------*/
    struct timespec t1, t2; 
    clock_gettime(CLOCK_MONOTONIC, &t1);

    printf("[sendfile] SAVE ...\n");
    rdb_save_sync();

    /*-------------------------------------------------------------------------
     *  5. 打开 RDB 文件
     *-------------------------------------------------------------------------*/
    const char *path = persist_path("build/dump.rdb");
    printf("[sendfile] opening %s ...\n", path);
    int fd = open(path, O_RDONLY);
    if (fd < 0) { 
        perror(path); 
        close(cfd); close(lfd); 
        return -1; 
    }
    struct stat st; 
    fstat(fd, &st);
    uint64_t file_size = (uint64_t)st.st_size;
    printf("[sendfile] RDB: %lu bytes\n", (unsigned long)file_size);

    /*-------------------------------------------------------------------------
     *  6. 发送数据：token(8B) + size(8B) + data
     *     token 是快照 offset，对端据此续传
     *     
     *     sendfile 可能只部分返回，必须循环发完
     *     否则大 RDB（或跨机）下对端 recv 到一半遇到 EOF
     *-------------------------------------------------------------------------*/
    /* 6a. 发送 token（大端序） */
    uint64_t tok_be = htobe64(token);
    if (send(cfd, &tok_be, 8, 0) != 8) { 
        perror("send token"); 
        goto out; 
    }

    /* 6b. 发送文件大小（大端序） */
    uint64_t sz_be = htobe64(file_size);
    if (send(cfd, &sz_be, 8, 0) != 8) { 
        perror("send size"); 
        goto out; 
    }

    /* 6c. sendfile 零拷贝发送数据（循环发送） */
    off_t off = 0;
    size_t remaining = (size_t)file_size;
    size_t sent = 0;
    while (remaining > 0) {
        ssize_t n = sendfile(cfd, fd, &off, remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("sendfile");
            break;
        }
        if (n == 0) break;  /* 对端关闭或无法继续 */
        sent      += (size_t)n;
        remaining -= (size_t)n;
    }

    clock_gettime(CLOCK_MONOTONIC, &t2);

    /* 统计传输性能 */
    double ms = (t2.tv_sec - t1.tv_sec) * 1000.0 + 
                (t2.tv_nsec - t1.tv_nsec) / 1e6;
    double mbps = (ms > 0) ? ((double)sent / (ms / 1000.0) / (1024.0 * 1024.0)) : 0;
    printf("[sendfile] xfer-end: bytes=%zu ms=%.1f mbps=%.2f\n", sent, ms, mbps);

out:
    close(fd); 
    close(cfd); 
    close(lfd);
    return 0;
}

/*=============================================================================
 *  客户端实现
 *============================================================================*/

/**
 * 接收所有数据到文件
 * 使用 MSG_WAITALL 阻塞接收，直到收到 total 字节或连接关闭
 * 
 * @param sfd socket fd
 * @param dfd 目标文件 fd
 * @param total 期望接收的总字节数
 * @return 实际收到的字节数，-1 表示错误
 */
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

/**
 * sendfile 客户端入口
 * 
 * 流程：
 * 1. 连接服务端的 fullsync_port（带重试）
 * 2. 发送就绪信号
 * 3. 接收 token（快照 offset）
 * 4. 接收文件大小
 * 5. 接收 RDB 数据到临时文件
 * 6. mmap 加载 RDB 数据
 * 
 * @param out_token 输出：从流头解出的 token（快照 offset）
 * @return 0 成功，-1 失败
 */
int sendfile_client_run(uint64_t *out_token) {
    setlinebuf(stdout); setlinebuf(stderr);
    const char *ip = kvs_config.fullsync_master_ip;
    int port = kvs_config.fullsync_port;
    char pstr[16]; 
    snprintf(pstr, sizeof(pstr), "%d", port);

    printf("[sendfile] %s:%s ...\n", ip, pstr);

    /*-------------------------------------------------------------------------
     *  1. 连接服务端（带重试，等待服务端监听就绪）
     *-------------------------------------------------------------------------*/
    struct addrinfo hints = {0}, *ai;
    hints.ai_family = AF_INET; 
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(ip, pstr, &hints, &ai) != 0) { 
        perror("getaddrinfo"); 
        return -1; 
    }
    int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) { 
        perror("socket"); 
        freeaddrinfo(ai); 
        return -1; 
    }

    /* 重试最多 30 次（约 6 秒），等待服务端监听就绪 */
    int ok = 0;
    for (int retry = 0; retry < 30; retry++) {
        if (connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) { 
            ok = 1; 
            break; 
        }
        usleep(200000);  /* 200ms */
    }
    freeaddrinfo(ai);
    if (!ok) { 
        perror("connect"); 
        close(fd); 
        return -1; 
    }

    /*-------------------------------------------------------------------------
     *  2. 发送就绪信号
     *     通知服务端可以开始 SAVE + sendfile
     *-------------------------------------------------------------------------*/
    { 
        char ready = 1; 
        if (send(fd, &ready, 1, 0) != 1) { 
            perror("send ready"); 
            close(fd); 
            return -1; 
        } 
    }

    /*-------------------------------------------------------------------------
     *  3. 接收 token（快照 offset）
     *     交还上层，增量模块据此续传
     *-------------------------------------------------------------------------*/
    uint64_t tok = 0;
    if (recv(fd, &tok, 8, MSG_WAITALL) != 8) { 
        perror("recv token"); 
        close(fd); 
        return -1; 
    }
    if (out_token) *out_token = be64toh(tok);
    printf("[sendfile] token=%lu\n", (unsigned long)be64toh(tok));

    /*-------------------------------------------------------------------------
     *  4. 接收文件大小
     *-------------------------------------------------------------------------*/
    uint64_t fsz = 0;
    if (recv(fd, &fsz, 8, MSG_WAITALL) != 8) { 
        perror("recv size"); 
        close(fd); 
        return -1; 
    }
    fsz = be64toh(fsz);
    printf("[sendfile] %lu bytes\n", (unsigned long)fsz);

    /*-------------------------------------------------------------------------
     *  5. 接收数据到临时文件
     *-------------------------------------------------------------------------*/
    char tmp[256]; 
    snprintf(tmp, sizeof(tmp), "/tmp/kvstore_fs_%d.rdb", getpid());
    int tfd = open(tmp, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (tfd < 0) { 
        perror("open tmp"); 
        close(fd); 
        return -1; 
    }

    struct timespec t1, t2; 
    clock_gettime(CLOCK_MONOTONIC, &t1);
    ssize_t got = recv_all(fd, tfd, (size_t)fsz);
    clock_gettime(CLOCK_MONOTONIC, &t2);
    close(fd); 
    close(tfd);

    /* 验证接收完整性 */
    if (got < 0) { 
        perror("recv data"); 
        return -1; 
    }
    if ((size_t)got != fsz) {
        fprintf(stderr, "[sendfile] connection closed early: got %zd/%lu bytes\n",
                got, (unsigned long)fsz);
        return -1;
    }

    /* 传输性能统计 */
    double ms = (t2.tv_sec - t1.tv_sec) * 1000.0 + 
                (t2.tv_nsec - t1.tv_nsec) / 1e6;
    double mbps = (ms > 0) ? ((double)fsz / (ms / 1000.0) / (1024.0 * 1024.0)) : 0;
    printf("[sendfile] xfer-end: bytes=%lu ms=%.1f mbps=%.2f\n",
           (unsigned long)fsz, ms, mbps);

    /*-------------------------------------------------------------------------
     *  6. mmap + 加载 RDB 数据
     *-------------------------------------------------------------------------*/
    int rfd = open(tmp, O_RDONLY);
    if (rfd < 0) { 
        perror("open tmp load"); 
        return -1; 
    }
    struct stat st; 
    fstat(rfd, &st);
    char *mbuf = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, rfd, 0);
    close(rfd); 
    unlink(tmp);  /* 立即删除临时文件（mmap 后数据仍在内存） */
    if (mbuf == MAP_FAILED) { 
        perror("mmap"); 
        return -1; 
    }

    int loaded = rdb_load_from_buf(mbuf, (size_t)st.st_size);
    munmap(mbuf, (size_t)st.st_size);
    printf("[sendfile] loaded %d keys\n", loaded);
    return 0;
}