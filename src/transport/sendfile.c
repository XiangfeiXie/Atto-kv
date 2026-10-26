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
 *  带宽形状：主端 sendfile 从 page cache 推、从端收到大块内存里就地加载 ——
 *  零拷贝只在主端，从端是"一次 copy_to_user + 就地解析"（RDMA 侧则是网卡直写
 *  对端内存，两端都不经过 CPU 拷贝）。快照上限见本头文件的 SENDFILE_BUF_SIZE。
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
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <netdb.h>
#include <endian.h>   /* htobe64 / be64toh */
#include <fcntl.h>
#include <poll.h>

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
 * @return 0 成功，-1 失败
 */
int sendfile_server_run(uint64_t token) {
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
    {
        char ready_sig;
        if (recv(cfd, &ready_sig, 1, 0) != 1) { 
            perror("recv ready"); 
            goto out; 
        } 
    }

    /*-------------------------------------------------------------------------
     *  4. 执行 RDB 保存（同步方式）
     *     落盘耗时单列进 xfer-end 的 prep: 段（不计入速率），与 RDMA 侧的
     *     conn/ser/reg 并列——两条线都把"传输前必须付的准备成本"放在同一处
     *-------------------------------------------------------------------------*/
    uint64_t t_save = now_monotonic_ns();

    printf("[sendfile] SAVE ...\n");
    rdb_save_sync();
    uint64_t save_ns = now_monotonic_ns() - t_save;

    /*-------------------------------------------------------------------------
     *  5. 打开 RDB 文件
     *-------------------------------------------------------------------------*/
    const char *path = persist_path("build/dump.rdb");
    int fd = open(path, O_RDONLY);
    if (fd < 0) { 
        perror(path); 
        close(cfd); close(lfd); 
        return -1; 
    }
    struct stat st;
    fstat(fd, &st);
    uint64_t file_size = (uint64_t)st.st_size;

    /* 快照硬上限（与 RDMA 同值）：超了直接放弃。从端的接收缓冲就是这么大，
     * 传过去也装不下。检查放在发流头之前 —— 从端在 recv(token) 上拿到 EOF
     * 立刻失败，不会先花几百 ms 摸完缓冲才知道装不下 */
    if (file_size > SENDFILE_BUF_SIZE) {
        fprintf(stderr,
                "[sendfile] snapshot %llu B exceeds transport buffer (%llu B), aborting\n",
                (unsigned long long)file_size,
                (unsigned long long)SENDFILE_BUF_SIZE);
        close(fd); close(cfd); close(lfd);
        return -1;
    }

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

    /* 6c. sendfile 零拷贝发送数据（循环发送）
     *     xfer 计时只夹这个循环 = 纯传输（不含落盘，落盘单列 save=）。
     *     已知偏差：阻塞 socket 上 sendfile 返回只表示这批字节进了本端内核
     *     发送缓冲，末包可能仍在途；RDMA 侧的 send CQE 是对端 ACK，语义更严。
     *     差值上界 ≈ 收发缓冲 + BDP，大快照下可忽略 */
    uint64_t t1 = now_monotonic_ns();
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

    uint64_t t2 = now_monotonic_ns();

    /* 汇总结论行：xfer 是纯传输（只夹 sendfile 循环），rate 由它算。
     * 与 [rdma] xfer-end 排成同一形状 —— 前三个字段两线对齐、速率同列，
     * 准备耗时一律收在 " | prep:" 之后，方便两条线并排看。 */
    double xfer_ms = (t2 > t1) ? (double)(t2 - t1) / 1e6 : 0.0;
    char tb[24];
    printf("[sendfile] xfer-end: bytes=%zu xfer=%.1fms rate=%.2fMB/s | prep: save=%.1fms | at=%s\n",
           sent, xfer_ms, xfer_rate_mbs(sent, xfer_ms), (double)save_ns / 1e6,
           xfer_wall_str(get_time_now_real(), tb, sizeof(tb)));

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
 * 接收所有数据到大块内存缓冲
 *
 * 不能指望 MSG_WAITALL：从端走 ntyco 引擎时，这个 fd 是 NtyCo 的 socket() 钩子
 * 设出来的非阻塞 fd（没有任何地方在用前恢复），而非阻塞 fd 上 MSG_WAITALL 不生效
 * —— 收到多少返回多少，一旦数据出现毫秒级空档就返回 EAGAIN。那是"暂时无数据"
 * 而不是错误，poll 等下一批再收即可。
 *
 * @param sfd socket fd
 * @param dst 目标缓冲（调用方保证 ≥ total 字节）
 * @param total 期望接收的总字节数
 * @return 实际收到的字节数，-1 表示错误
 */
static ssize_t recv_all(int sfd, char *dst, size_t total) {
    size_t got = 0;

    /* 分块上限 1MiB：只让每次 syscall 的等待有界，与速率无关 */
    while (got < total) {
        size_t left  = total - got;
        size_t chunk = left < (1u << 20) ? left : (1u << 20);
        ssize_t n = recv(sfd, dst + got, chunk, MSG_WAITALL);
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                struct pollfd pfd = { .fd = sfd, .events = POLLIN };
                if (poll(&pfd, 1, 100) < 0 && errno != EINTR) return -1;
                continue;       /* 最多等 100ms 一轮，超时再收一次 */
            }
            return -1;
        }
        if (n == 0) break;  /* EOF：连接提前关闭，数据不完整 */
        got += (size_t)n;
    }
    return (ssize_t)got;
}

/**
 * sendfile 客户端入口
 *
 * 流程：
 * 1. 连接服务端的 fullsync_port（带重试）
 * 2. 发送就绪信号
 * 3. 准备接收缓冲：malloc 整块 + 触页
 * 4. 接收 token（快照 offset）
 * 5. 接收文件大小
 * 6. 接收 RDB 数据到大块内存缓冲（不经 /tmp）
 * 7. 就地加载 RDB 数据
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
     *  3. 准备接收缓冲：整份快照就地收进内存，不再经 /tmp 中转
     *
     *  位置：紧跟 ready，先于 recv(token)。服务端收到 ready 即开始 SAVE
     *  （秒级），本端这段触页耗时正好藏在它后面，两端计时字段都不受影响。
     *  这正是 RDMA 侧 rdma_reg_write 的位置与作用（预触 + 钉页，不入任何上报字段）。
     *
     *  触页是这块缓冲的关键动作，不是可省的优化：158MB 是 3.9 万个 4KB 页，
     *  不预触的话每一次缺页 + 清零都落在 recv 的 copy_to_user 途中，从端一慢
     *  就往回压住主端的发送窗口。本机实测同一份快照、同为 4KB 页：
     *  不预触 36.89 MB/s，预触 1480.90 MB/s（40×）。
     *
     *  大页（mmap + MADV_HUGEPAGE）实测没有可分辨的额外贡献（4KB 页 + 预触
     *  1480.90 vs 大页 + 预触 1613.29，在后者自身的散布区间内），所以不用 ——
     *  也就不需要 mmap（madvise 要求页对齐，malloc 返回值带 glibc 头部），
     *  缓冲区回归普通 malloc。
     *-------------------------------------------------------------------------*/
    char *buf = malloc(SENDFILE_BUF_SIZE);
    if (!buf) { perror("malloc"); close(fd); return -1; }
    for (size_t off = 0; off < SENDFILE_BUF_SIZE; off += 4096)
        buf[off] = 0;

    /*-------------------------------------------------------------------------
     *  4. 接收 token（快照 offset）
     *     交还上层，增量模块据此续传
     *     与数据区同走 recv_all：服务端先 SAVE（数秒）再发流头，而这里
     *     在 ntyco 下是非阻塞 fd，MSG_WAITALL 不等数据，会直接返回 EAGAIN
     *-------------------------------------------------------------------------*/
    uint64_t tok = 0;
    if (recv_all(fd, (char *)&tok, sizeof(tok)) != (ssize_t)sizeof(tok)) {
        perror("recv token");
        close(fd);
        return -1;
    }
    if (out_token) *out_token = be64toh(tok);
    printf("[sendfile] token=%lu\n", (unsigned long)be64toh(tok));

    /*-------------------------------------------------------------------------
     *  5. 接收文件大小（同上，必须处理 EAGAIN）
     *-------------------------------------------------------------------------*/
    uint64_t fsz = 0;
    if (recv_all(fd, (char *)&fsz, sizeof(fsz)) != (ssize_t)sizeof(fsz)) {
        perror("recv size");
        close(fd);
        return -1;
    }
    fsz = be64toh(fsz);
    printf("[sendfile] %lu bytes\n", (unsigned long)fsz);

    /* 快照硬上限（与 RDMA 同值）：装不下直接失败，不往下收 */
    if (fsz > SENDFILE_BUF_SIZE) {
        fprintf(stderr,
                "[sendfile] snapshot %lu B exceeds transport buffer (%llu B)\n",
                (unsigned long)fsz, (unsigned long long)SENDFILE_BUF_SIZE);
        free(buf);
        close(fd);
        return -1;
    }

    /*-------------------------------------------------------------------------
     *  6. 接收数据到大块内存缓冲（不经 /tmp）
     *-------------------------------------------------------------------------*/
    uint64_t t1 = now_monotonic_ns();
    ssize_t got = recv_all(fd, buf, (size_t)fsz);
    uint64_t t2 = now_monotonic_ns();
    close(fd);

    /* 验证接收完整性 */
    if (got < 0) {
        perror("recv data");
        free(buf);
        return -1;
    }
    if ((size_t)got != fsz) {
        fprintf(stderr, "[sendfile] connection closed early: got %zd/%lu bytes\n",
                got, (unsigned long)fsz);
        free(buf);
        return -1;
    }

    /* 接收端只报字节数 + 墙钟，不出速率：这段等待里还夹着本端 copy_to_user
     * 的耗时。速率一律以发送端为准 */
    double wait_ms = (t2 > t1) ? (double)(t2 - t1) / 1e6 : 0.0;
    char tb[24];
    printf("[sendfile] recv-end: bytes=%lu wait=%.1f | at=%s\n",
           (unsigned long)fsz, wait_ms,
           xfer_wall_str(get_time_now_real(), tb, sizeof(tb)));

    /*-------------------------------------------------------------------------
     *  7. 就地加载 RDB 数据
     *     线上格式与落盘版完全一致（磁盘格式，带 crc 槽），故仍走
     *     rdb_load_from_buf —— 省掉的是 /tmp 落盘 + 从文件 mmap 两级，
     *     不是解析路径
     *-------------------------------------------------------------------------*/
    int loaded = rdb_load_from_buf(buf, (size_t)fsz);
    free(buf);
    if (loaded < 0) {
        fprintf(stderr, "[sendfile] load failed\n");
        return -1;
    }
    printf("[sendfile] loaded %d keys\n", loaded);
    return 0;
}