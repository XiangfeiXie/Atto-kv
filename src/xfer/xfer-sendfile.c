/*=============================================================================
 *  xfer-sendfile.c — sendfile 传输速率测试组件（自包含，不依赖项目任何头文件）
 *
 *  只做一件事：把命令行给的一个文件用 sendfile(2) 整块推给对端，报发送端的
 *  纯传输速率。没有握手、没有协议头、没有落盘 —— 发完就断，客户端收到 EOF
 *  为止。目的单一：量"sendfile 这条路能跑多快"，量出来的东西再往 kvstore 上搬。
 *
 *  计时口径（与 xfer-rdma.c、与 iperf3 对齐）：从 sendfile 循环开始，到**对端
 *  把数据全部读走**为止。见下面 run_server 里"尾部同步"那段。
 *
 *  用法：
 *    xfer-sendfile server <file> <port>
 *    xfer-sendfile client <ip> <port> [--discard]
 *============================================================================*/

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <netdb.h>

/*=============================================================================
 *  小工具
 *============================================================================*/

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* 十进制 MB/s（与 kvstore 侧同口径，便于两边数字并排看） */
static double rate_mbs(size_t bytes, double ms)
{
    return (ms > 0.0) ? ((double)bytes / (ms / 1000.0) / 1e6) : 0.0;
}

/* 连接对端（带重试：服务端可能还没起来） */
static int connect_to(const char *ip, int port)
{
    char pstr[16];
    struct addrinfo hints = {0}, *ai = NULL;
    snprintf(pstr, sizeof(pstr), "%d", port);
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(ip, pstr, &hints, &ai) != 0) { perror("getaddrinfo"); return -1; }

    int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) { perror("socket"); freeaddrinfo(ai); return -1; }

    int ok = 0;
    for (int i = 0; i < 50; i++) {
        if (connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) { ok = 1; break; }
        usleep(200000);  /* 200ms × 50 = 最多等 10s */
    }
    freeaddrinfo(ai);
    if (!ok) { perror("connect"); close(fd); return -1; }
    return fd;
}

/*=============================================================================
 *  服务端：open 文件 → accept → sendfile 循环
 *============================================================================*/

static int run_server(const char *path, int port)
{
    /* 先把文件打开：打不开就别占着端口了 */
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); return -1; }
    struct stat st;
    if (fstat(fd, &st) != 0) { perror("fstat"); close(fd); return -1; }
    size_t file_size = (size_t)st.st_size;

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) { perror("socket"); close(fd); return -1; }
    int one = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr = {
        .sin_family      = AF_INET,
        .sin_addr.s_addr = INADDR_ANY,
        .sin_port        = htons((uint16_t)port),
    };
    if (bind(lfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(lfd); close(fd); return -1;
    }
    if (listen(lfd, 1) < 0) { perror("listen"); close(lfd); close(fd); return -1; }

    printf("[sendfile] listening :%d  (%s, %zu bytes)\n", port, path, file_size);

    int cfd = accept(lfd, NULL, NULL);
    if (cfd < 0) { perror("accept"); close(lfd); close(fd); return -1; }
    printf("[sendfile] client connected\n");

    /*-------------------------------------------------------------------------
     *  纯传输：sendfile 循环 + 尾部同步。计时从循环开始到"对端已全部收走"，
     *  与 xfer-rdma 的 send CQE、与 iperf3 的接收端口径三者同级
     *-------------------------------------------------------------------------*/
    uint64_t t1 = now_ns();
    off_t off = 0;
    size_t remaining = file_size;
    size_t sent = 0;
    while (remaining > 0) {
        ssize_t n = sendfile(cfd, fd, &off, remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("sendfile");
            break;
        }
        if (n == 0) break;  /* 对端关闭 */
        sent      += (size_t)n;
        remaining -= (size_t)n;
    }
    uint64_t t3 = now_ns();

    /*-------------------------------------------------------------------------
     *  尾部同步（统一口径的关键一步）
     *
     *  阻塞 socket 上 sendfile() 返回只表示这批字节进了**本端**发送缓冲，
     *  末段（≈ 收发缓冲之和，本机实测 ~6MB）可能仍在途。不等它，算出来的
     *  是"本端交出去有多快"，而不是"对端拿到有多快"，与 RDMA 的 send CQE
     *  （对端已 ACK）、与 iperf3 的接收端口径都对不上。
     *
     *  shutdown(SHUT_WR) 让 FIN 排在这批数据之后；客户端收到 EOF 就 close，
     *  FIN 回来时本端 recv 返回 0 = 对端确实把数据全部读走了。协议不用改。
     *-------------------------------------------------------------------------*/
    shutdown(cfd, SHUT_WR);
    for (;;) {
        char ack;
        ssize_t n = recv(cfd, &ack, 1, 0);
        if (n == 0) break;                        /* 对端 FIN：数据已收完 */
        if (n < 0 && errno == EINTR) continue;
        if (n < 0) { perror("recv tail"); break; }
        /* n > 0：客户端不会发数据，忽略 */
    }
    uint64_t t2 = now_ns();

    double ms = (double)(t2 - t1) / 1e6;
    printf("[sendfile] xfer-end: bytes=%zu xfer=%.1fms rate=%.2fMB/s | tail=%.1fms\n",
           sent, ms, rate_mbs(sent, ms), (double)(t2 - t3) / 1e6);

    close(cfd);
    close(lfd);
    close(fd);
    return 0;
}

/*=============================================================================
 *  客户端：收完为止（可选只收不落盘）
 *============================================================================*/

static int run_client(const char *ip, int port, int discard)
{
    int fd = connect_to(ip, port);
    if (fd < 0) return -1;

    /* 默认落到 /tmp（对齐 kvstore 从端的做法）；--discard 连文件都不建 */
    int dfd = -1;
    char tmp[128];
    if (!discard) {
        snprintf(tmp, sizeof(tmp), "/tmp/xfer_%d.rdb", getpid());
        dfd = open(tmp, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (dfd < 0) { perror("open tmp"); close(fd); return -1; }
    }

    /*-------------------------------------------------------------------------
     *  墙钟 = iperf3 接收端的口径（"对端把数据读走了多少、用了多久"），
     *  与服务端那个数（循环 + 尾部同步）测的是同一件事，正好互相校验。
     *  收到 EOF（recv 返回 0）即服务端发完。
     *  不加 --discard 时这段里混着本端 write() 落盘的耗时 —— 那种情况下
     *  整条线本来就被落盘压着，两个数会一起掉，不是口径问题
     *-------------------------------------------------------------------------*/
    char buf[65536];
    uint64_t t1 = now_ns();
    size_t got = 0;
    for (;;) {
        ssize_t n = recv(fd, buf, sizeof(buf), 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("recv");
            break;
        }
        if (n == 0) break;  /* 对端关闭 = 传输结束 */
        if (dfd >= 0 && write(dfd, buf, (size_t)n) != n) { perror("write"); break; }
        got += (size_t)n;
    }
    uint64_t t2 = now_ns();

    close(fd);
    if (dfd >= 0) { close(dfd); unlink(tmp); }

    double wait_ms = (double)(t2 - t1) / 1e6;
    printf("[sendfile] recv-end: bytes=%zu wait=%.1fms rate=%.2fMB/s%s\n",
           got, wait_ms, rate_mbs(got, wait_ms), discard ? "  (discard)" : "");
    return 0;
}

/*=============================================================================
 *  main
 *============================================================================*/

int main(int argc, char **argv)
{
    setlinebuf(stdout);
    setlinebuf(stderr);

    if (argc < 4) {
        fprintf(stderr,
                "用法:\n"
                "  %s server <file> <port>\n"
                "  %s client <ip> <port> [--discard]\n",
                argv[0], argv[0]);
        return 1;
    }

    int port = atoi(argv[3]);
    if (port <= 0 || port > 65535) { fprintf(stderr, "端口不合法\n"); return 1; }

    if (!strcmp(argv[1], "server") && argc == 4)
        return run_server(argv[2], port) == 0 ? 0 : 1;

    if (!strcmp(argv[1], "client")) {
        int discard = 0;
        for (int i = 4; i < argc; i++) {
            if (!strcmp(argv[i], "--discard")) discard = 1;
            else { fprintf(stderr, "未知参数: %s\n", argv[i]); return 1; }
        }
        return run_client(argv[2], port, discard) == 0 ? 0 : 1;
    }

    fprintf(stderr, "角色只能是 server 或 client\n");
    return 1;
}
