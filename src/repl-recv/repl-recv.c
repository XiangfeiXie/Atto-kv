// SPDX-License-Identifier: GPL-2.0
/* repl-recv.c — 加载 BPF、挂 uprobe/uretprobe，并把镜像字节转发到从机 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#include <bpf/libbpf.h>
#include <bpf/bpf.h>

#include "repl-recv.h"

static volatile sig_atomic_t g_run = 1;
static int g_slave_fd = -1;
static struct bpf_link *g_links[8];   /* 持有 uprobe 链接，进程存活期间不释放 */

static void on_sig(int s) { (void)s; g_run = 0; }

/* ring buffer 回调：把镜像到的字节原样发到从机（长度由 ring buffer 帧头给出） */
static int on_event(void *ctx, void *data, size_t len) {
    (void)ctx;
    const uint8_t *p = (const uint8_t *)data;
    size_t off = 0;
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

int main(void) {
    struct bpf_object *obj = bpf_object__open_file("repl-recv.bpf.o", NULL);
    if (!obj) {
        fprintf(stderr, "open repl-recv.bpf.o: %s\n", strerror(errno));
        return 1;
    }

    int err = bpf_object__load(obj);
    if (err) {
        char ebuf[128];
        libbpf_strerror(err, ebuf, sizeof(ebuf));
        fprintf(stderr, "load bpf: %s\n", ebuf);
        return 1;
    }

    /* 把 uprobe/uretprobe 挂到 libc 的 recv 符号上 */
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
        struct bpf_link *l =
            bpf_program__attach_uprobe_opts(prog, -1, LIBC_PATH, 0, &opts);
        if (!l) {
            fprintf(stderr, "attach %s: %s\n", name, strerror(errno));
            return 1;
        }
        g_links[nlink++] = l;   /* 存活到进程退出（uprobe 随之自动释放） */
    }

    int rb_fd = bpf_object__find_map_fd_by_name(obj, "events");
    if (rb_fd < 0) { fprintf(stderr, "find map 'events'\n"); return 1; }

    struct ring_buffer *rb = ring_buffer__new(rb_fd, on_event, NULL, NULL);
    if (!rb) { fprintf(stderr, "ring_buffer__new: %s\n", strerror(errno)); return 1; }

    g_slave_fd = connect_slave();
    if (g_slave_fd < 0)
        return 1;

    signal(SIGINT, on_sig);
    signal(SIGTERM, on_sig);

    printf("[repl-recv] pid=%d, mirroring libc recv -> %s:%d\n",
           TARGET_PID, PEER_IP, PEER_PORT);

    while (g_run) {
        err = ring_buffer__poll(rb, 100);
        if (err < 0) {
            if (err == -EINTR)
                continue;
            fprintf(stderr, "poll: %d\n", err);
            break;
        }
    }

    ring_buffer__free(rb);
    close(g_slave_fd);
    bpf_object__close(obj);
    return 0;
}
