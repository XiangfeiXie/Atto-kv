// SPDX-License-Identifier: GPL-2.0
/*=============================================================================
 *  repl-recv.c — 加载 BPF、挂 uprobe/uretprobe，并把镜像字节转发到从机
 *  
 *  功能：
 *  1. 加载 eBPF 程序（repl-recv.bpf.o）
 *  2. 在目标进程的 libc recv 上挂 uprobe/uretprobe
 *  3. 从 BPF ring buffer 读取镜像的 recv 数据
 *  4. 通过 TCP 转发到从机
 *  
 *  设计要点：
 *  - 独立进程，与 kvstore 零耦合
 *  - master PID 由命令行参数传入
 *  - 从机地址通过宏定义配置
 *  - 独立线程排空从机应答，防止 TCP 发送缓冲阻塞
 *============================================================================*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <pthread.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#include <bpf/libbpf.h>
#include <bpf/bpf.h>

#include "repl-recv.h"

/*=============================================================================
 *  全局状态
 *============================================================================*/

static volatile sig_atomic_t g_run = 1;    /* 运行标志（信号触发停止） */
static int g_slave_fd = -1;               /* 到从机的 TCP 连接 */
static struct bpf_link *g_links[8];       /* uprobe 链接（进程存活期间不释放） */

/*=============================================================================
 *  信号处理
 *============================================================================*/

static void on_sig(int s) { 
    (void)s; 
    g_run = 0; 
}

/*=============================================================================
 *  BPF Ring Buffer 回调
 *============================================================================*/

/**
 * BPF ring buffer 事件回调
 * 
 * 功能：把镜像到的字节原样发送到从机
 * 
 * @param ctx 上下文（未使用）
 * @param data 数据指针（来自 BPF ring buffer）
 * @param len 数据长度
 * @return 0 成功
 * 
 * 数据来源：eBPF 程序在 recv 入口/出口捕获的 recv 数据
 * 长度由 ring buffer 帧头给出
 */
static int on_event(void *ctx, void *data, size_t len) {
    (void)ctx;
    const uint8_t *p = (const uint8_t *)data;
    size_t off = 0;
    
    /* 循环发送，直到全部发完 */
    while (off < len) {
        ssize_t w = send(g_slave_fd, p + off, len - off, MSG_NOSIGNAL);
        if (w < 0) {
            perror("send to slave");
            return 0;
        }
        off += (size_t)w;
    }
    return 0;
}

/*=============================================================================
 *  从机应答排空线程
 *  
 *  设计原因：
 *  从机对每条命令都回 RESP 应答（+OK 等）
 *  本程序只负责把镜像字节送过去，应答必须读掉
 *  否则从机侧 TCP 收发缓冲满后 send 阻塞，链路最终卡死
 *============================================================================*/

/**
 * 独立线程：持续 recv 丢弃从机应答
 * 
 * @param arg 未使用
 * @return NULL
 * 
 * 行为：
 * - 循环读取从机应答并丢弃
 * - slave 断开时 recv 返回 0，线程随之退出
 */
static void *drain_slave_replies(void *arg) {
    (void)arg;
    char buf[4096];
    while (recv(g_slave_fd, buf, sizeof(buf), 0) > 0)
        ;
    return NULL;
}

/*=============================================================================
 *  连接从机
 *============================================================================*/

/**
 * 连接到从机
 * 
 * @return socket fd，失败返回 -1
 * 
 * 配置来源：
 * - PEER_IP：从机 IP
 * - PEER_PORT：从机端口
 * 
 * 设置 TCP_NODELAY 减少延迟
 */
static int connect_slave(void) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }

    struct sockaddr_in a = {0};
    a.sin_family = AF_INET;
    a.sin_port   = htons(PEER_PORT);
    if (inet_pton(AF_INET, PEER_IP, &a.sin_addr) != 1) {
        perror("inet_pton"); close(fd); return -1;
    }

    if (connect(fd, (struct sockaddr *)&a, sizeof(a)) < 0) {
        perror("connect"); close(fd); return -1;
    }

    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    return fd;
}

/*=============================================================================
 *  主函数
 *============================================================================*/

int main(int argc, char *argv[]) {
    /*-------------------------------------------------------------------------
     *  用法: repl-recv <master-pid> [repl-recv.bpf.o]
     *  
     *  pid：目标 master 进程 PID（每次换进程传，挂载时按 pid 过滤）
     *  .bpf.o：BPF 对象文件，默认当前目录下同名文件
     * 
     *  优势：无需为每个 master 进程重新编译 BPF
     *-------------------------------------------------------------------------*/
    if (argc < 2) {
        fprintf(stderr, "usage: %s <master-pid> [repl-recv.bpf.o]\n", argv[0]);
        return 1;
    }
    
    int target_pid = atoi(argv[1]);
    if (target_pid <= 0) {
        fprintf(stderr, "invalid target pid: %s\n", argv[1]);
        return 1;
    }
    
    const char *bpf_path = (argc > 2) ? argv[2] : "repl-recv.bpf.o";

    /*-------------------------------------------------------------------------
     *  1. 打开 BPF 对象文件
     *-------------------------------------------------------------------------*/
    struct bpf_object *obj = bpf_object__open_file(bpf_path, NULL);
    if (!obj) {
        fprintf(stderr, "open %s: %s\n", bpf_path, strerror(errno));
        return 1;
    }

    /*-------------------------------------------------------------------------
     *  2. 加载 BPF 程序到内核
     *-------------------------------------------------------------------------*/
    int err = bpf_object__load(obj);
    if (err) {
        char ebuf[128];
        libbpf_strerror(err, ebuf, sizeof(ebuf));
        fprintf(stderr, "load bpf: %s\n", ebuf);
        return 1;
    }

    /*-------------------------------------------------------------------------
     *  3. 挂 uprobe/uretprobe 到 libc 的 recv 符号
     *     只对 target_pid 进程生效
     *-------------------------------------------------------------------------*/
    struct bpf_program *prog;
    int nlink = 0;

    bpf_object__for_each_program(prog, obj) {
        const char *name = bpf_program__name(prog);
        bool retprobe = strncmp(name, "uretprobe", 9) == 0;

        struct bpf_uprobe_opts opts = {
            .sz        = sizeof(opts),
            .retprobe  = retprobe,
            .func_name = "recv",
        };
        
        /* 挂载 uprobe/uretprobe */
        struct bpf_link *l =
            bpf_program__attach_uprobe_opts(prog, target_pid, LIBC_PATH, 0, &opts);
        if (!l) {
            fprintf(stderr, "attach %s: %s\n", name, strerror(errno));
            return 1;
        }
        g_links[nlink++] = l;   /* 存活到进程退出（uprobe 随之自动释放） */
    }

    /*-------------------------------------------------------------------------
     *  4. 获取 BPF ring buffer map
     *-------------------------------------------------------------------------*/
    int rb_fd = bpf_object__find_map_fd_by_name(obj, "events");
    if (rb_fd < 0) { 
        fprintf(stderr, "find map 'events'\n"); 
        return 1; 
    }

    /*-------------------------------------------------------------------------
     *  5. 创建 ring buffer 消费者
     *-------------------------------------------------------------------------*/
    struct ring_buffer *rb = ring_buffer__new(rb_fd, on_event, NULL, NULL);
    if (!rb) { 
        fprintf(stderr, "ring_buffer__new: %s\n", strerror(errno)); 
        return 1; 
    }

    /*-------------------------------------------------------------------------
     *  6. 连接到从机
     *-------------------------------------------------------------------------*/
    g_slave_fd = connect_slave();
    if (g_slave_fd < 0)
        return 1;

    /*-------------------------------------------------------------------------
     *  7. 启动应答排空线程
     *     防止从机 TCP 发送缓冲阻塞
     *-------------------------------------------------------------------------*/
    pthread_t th;
    if (pthread_create(&th, NULL, drain_slave_replies, NULL) != 0)
        fprintf(stderr, "warning: slave-reply drain thread failed\n");
    else
        pthread_detach(th);   /* 不等待线程退出 */

    /*-------------------------------------------------------------------------
     *  8. 注册信号处理
     *-------------------------------------------------------------------------*/
    signal(SIGINT, on_sig);
    signal(SIGTERM, on_sig);

    /*-------------------------------------------------------------------------
     *  9. 主循环：轮询 BPF ring buffer
     *-------------------------------------------------------------------------*/
    printf("[repl-recv] pid=%d, mirroring libc recv (%s) -> %s:%d\n",
           target_pid, LIBC_PATH, PEER_IP, PEER_PORT);

    while (g_run) {
        err = ring_buffer__poll(rb, 100);  /* 超时 100ms */
        if (err < 0) {
            if (err == -EINTR)   /* 被信号中断，继续 */
                continue;
            fprintf(stderr, "poll: %d\n", err);
            break;
        }
    }

    /*-------------------------------------------------------------------------
     *  10. 清理资源
     *-------------------------------------------------------------------------*/
    ring_buffer__free(rb);
    close(g_slave_fd);
    bpf_object__close(obj);
    return 0;
}