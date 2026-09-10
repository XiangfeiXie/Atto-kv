// SPDX-License-Identifier: GPL-2.0
/*=============================================================================
 *  repl-tcp_recvmsg.c — kprobe 镜像组件的用户态 loader（最小 demo）
 *
 *  用户态只做几件事：
 *    a) 加载 repl-tcp_recvmsg.bpf.o，把 kprobe/kretprobe 挂到
 *       tcp_recvmsg_locked，并把目标 master PID 写入 cfg map（tgid 过滤）；
 *    b) 开一个普通 TCP socket 连从机，把 ring 里镜像到的字节写过去；
 *    c) 读掉从机的 +OK 应答（见 drain_responses 注释，关键）；
 *    d) 每秒打印生产/消费速率统计（produced map vs 本进程消费字节数）。
 *
 *  用法: repl-tcp_recvmsg <master-pid> [repl-tcp_recvmsg.bpf.o]
 *
 *  顺序保证：镜像点是内核把 TCP 重组好的字节拷进应用缓冲那一刻，字节本身已
 *  按序无重复，消费侧只需按记录序拼接转发。BPF 给每条记录打全局单调 seq，
 *  本进程按序核对——seq 不连续说明 ring 跨 CPU 乱序（master 线程在 CPU 间迁
 *  移）或丢采样，直接报错退出（fail-closed，不静默错序）。演示用
 *  `taskset -c N` 固定 master 到单 CPU 即可保证不乱序。
 *
 *  背压设计（ring → 待发缓冲 → socket）：
 *    - socket 非阻塞，ring 每条记录只追加进 g_pend 待发缓冲，绝不在事件回调
 *      里阻塞 → ring 永远即时消费，BPF 采样永不丢弃。
 *    - 每轮循环先尽力 flush g_pend（写不出就下次再写），再 drain 从机应答。
 *      若只发不读，从机 +OK 会把本端 rcvbuf 撑满 → 窗口 0 → 从机写不出去 →
 *      从机停摆（实测死锁，XDP 版踩过）。从机跟不上时 g_pend 增长，demo 规模
 *      有界。
 *============================================================================*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <poll.h>
#include <time.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>

#include "repl-tcp_recvmsg.h"

/* Ring 记录头（与 BPF 侧逐字节一致：16B 头 + 变长负载） */
struct tcp_rec {
    unsigned long long seq;      /* 全局单调记录序号 */
    unsigned int len;            /* 负载字节数 */
    unsigned int __pad;
};

static volatile int g_run = 1;
static int g_sock = -1;                 /* 到从机的连接 */
static unsigned long g_mirrored = 0;    /* 已写入从机的镜像字节数 */

/* 速率监测：本进程从 ring 消费的字节数/记录数（口径 = 记录 size = 16B 头 +
 * 负载，与 BPF produced map 逐字节一致 → 每秒差分两者即生产/消费速率）。 */
static unsigned long g_cons_bytes = 0;
static unsigned long g_cons_recs = 0;

/* 顺序核对：下一个应到记录序号（BPF 从 0 起、每成功进 ring 一条 +1） */
static unsigned long long g_expect_seq = 0;

static struct bpf_link *g_link_ent = NULL;
static struct bpf_link *g_link_ret = NULL;

static void on_sigint(int sig)
{
    (void)sig;
    g_run = 0;
}

/*=============================================================================
 *  待发缓冲（从机暂时跟不上时的吸收层）
 *============================================================================*/

static unsigned char *g_pend;           /* 待发字节 */
static size_t g_pend_head = 0;          /* 已读位置 */
static size_t g_pend_len = 0;           /* 有效字节数 */
static size_t g_pend_cap = 0;
static size_t g_pend_peak = 0;          /* 峰值，观察背压 */

static void pend_append(const void *data, size_t len)
{
    if (g_pend_len + len > g_pend_cap) {
        size_t nc = g_pend_cap ? g_pend_cap : (1 << 16);
        while (nc < g_pend_len + len)
            nc *= 2;
        g_pend = realloc(g_pend, nc);
        g_pend_cap = nc;
    }
    memcpy(g_pend + g_pend_len, data, len);
    g_pend_len += len;
    if (g_pend_len - g_pend_head > g_pend_peak)
        g_pend_peak = g_pend_len - g_pend_head;
}

static FILE *g_tee;    /* 诊断：REPL_TCP_RECV_TEE 指向的文件，写入的字节同步落盘 */

/* 尽力把待发缓冲写进 socket（写不出 EAGAIN 就留到下次） */
static void pend_flush(void)
{
    while (g_pend_head < g_pend_len) {
        ssize_t n = write(g_sock, g_pend + g_pend_head,
                          g_pend_len - g_pend_head);
        if (n > 0) {
            g_pend_head += (size_t)n;
            g_mirrored += (size_t)n;
            if (g_tee)
                fwrite(g_pend + g_pend_head - (size_t)n, 1, (size_t)n, g_tee);
            continue;
        }
        if (n < 0 && errno == EINTR)
            continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            return;                     /* 内核发送缓冲满，下次再发 */
        fprintf(stderr, "[sock] write: %s（从机可能已断）\n", strerror(errno));
        g_run = 0;
        return;
    }
    g_pend_head = g_pend_len = 0;       /* 腾空复用 */
}

/*=============================================================================
 *  从机应答处理（必须读掉丢弃）
 *
 *  从机对每条镜像命令回 +OK。不读掉的话，应答把本端 rcvbuf 撑满 → 窗口 0 →
 *  从机写不出去 → 从机事件循环阻塞 → 从机停摆，进而让 g_pend 越积越多 →
 *  ring 溢出丢采样（实测死锁，见背压设计）。
 *============================================================================*/

static void drain_responses(void)
{
    char buf[8192];
    for (;;) {
        ssize_t n = read(g_sock, buf, sizeof(buf));
        if (n > 0)
            continue;                   /* 丢弃 */
        if (n < 0 && errno == EINTR)
            continue;
        if (n < 0 && errno == EAGAIN)
            break;                      /* 读干净了 */
        if (n == 0) {
            fprintf(stderr, "[sock] 从机关闭连接\n");
            g_run = 0;
            return;
        }
        fprintf(stderr, "[sock] read: %s\n", strerror(errno));
        g_run = 0;
        return;
    }
}

/*=============================================================================
 *  Ring 消费回调：记录 → 顺序核对 → 待发缓冲（绝不阻塞）
 *
 *  记录本身已经是内核重组后的字节流切片，消费侧不做任何重组，只按 seq 核对
 *  顺序后按到达序拼接进待发缓冲即得与 master 完全一致的字节流。
 *============================================================================*/

static int on_event(void *ctx, void *data, size_t size)
{
    (void)ctx;
    const struct tcp_rec *rec = data;
    if (size < sizeof(*rec))
        return 0;
    g_cons_bytes += size;                /* 消费字节（= BPF produced 同口径） */
    g_cons_recs++;                       /* 消费记录条数 */
    if (rec->len > size - sizeof(*rec))
        return 0;                        /* 坏记录：防御性跳过 */
    if (rec->seq != g_expect_seq) {
        fprintf(stderr,
                "[mirror] seq 乱序：期望 %llu 实际 %llu ——"
                " master 线程在 CPU 间迁移？用 taskset 固定 master 后重跑\n",
                g_expect_seq, rec->seq);
        g_run = 0;
        return 0;
    }
    g_expect_seq++;
    pend_append((const unsigned char *)data + sizeof(*rec), rec->len);
    return 0;
}

/*=============================================================================
 *  连从机（普通内核 TCP socket）
 *============================================================================*/

static int connect_slave(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    /* 镜像是实时流，关 Nagle 减少延迟（不影响正确性） */
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    struct sockaddr_in sa = {
        .sin_family = AF_INET,
        .sin_port   = htons(SLAVE_PORT),
    };
    if (inet_pton(AF_INET, SLAVE_IP, &sa.sin_addr) != 1) {
        perror("inet_pton");
        close(fd);
        return -1;
    }
    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        fprintf(stderr, "[sock] connect %s:%d: %s\n",
                SLAVE_IP, SLAVE_PORT, strerror(errno));
        close(fd);
        return -1;
    }

    /* 非阻塞：写不出 / 读干净都以 EAGAIN 判定 */
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    fprintf(stderr, "[sock] 已连从机 %s:%d\n", SLAVE_IP, SLAVE_PORT);
    return fd;
}

/* 读 per-CPU u64 计数并跨 CPU 求和（produced / drops 同构）。
 * per-CPU map 的 LOOKUP_ELEM 会写 value_size*可用CPU数 字节到用户 buffer
 * （只给 8 字节会越界踩栈，历史 bug），必须按 libbpf_num_possible_cpus()
 * 分配数组再逐 CPU 求和。map_fd 无效时返回 0。 */
static unsigned long read_percpu_sum(int map_fd)
{
    __u32 zero = 0;
    int nr_cpus = libbpf_num_possible_cpus();
    if (nr_cpus <= 0)
        return 0;
    __u64 *vals = calloc((size_t)nr_cpus, sizeof(__u64));
    if (!vals)
        return 0;
    unsigned long sum = 0;
    if (bpf_map_lookup_elem(map_fd, &zero, vals) == 0)
        for (int i = 0; i < nr_cpus; i++)
            sum += (unsigned long)vals[i];
    free(vals);
    return sum;
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        fprintf(stderr,
                "用法: %s <master-pid> [repl-tcp_recvmsg.bpf.o]\n"
                "  master-pid 目标 kvstore master 进程 PID（每次换进程传）\n"
                "  提示：用 taskset 固定 master 到单 CPU 保证 ring 顺序\n",
                argv[0]);
        return 1;
    }
    long target_pid = atol(argv[1]);
    if (target_pid <= 0) {
        fprintf(stderr, "无效 master pid: %s\n", argv[1]);
        return 1;
    }
    const char *bpf_path = (argc > 2) ? argv[2] : "repl-tcp_recvmsg.bpf.o";

    signal(SIGINT, on_sigint);
    signal(SIGPIPE, SIG_IGN);           /* 从机断连时 write 触发 EPIPE 而非自杀 */

    const char *tee_path = getenv("REPL_TCP_RECV_TEE");
    if (tee_path)
        g_tee = fopen(tee_path, "wb");

    /* 1. 打开 + 加载 BPF 对象 */
    struct bpf_object *obj = bpf_object__open_file(bpf_path, NULL);
    if (!obj) {
        fprintf(stderr, "[bpf] open %s: %s\n", bpf_path, strerror(errno));
        return 1;
    }
    if (bpf_object__load(obj)) {
        fprintf(stderr, "[bpf] load 失败\n");
        bpf_object__close(obj);
        return 1;
    }

    /* 2. 挂 kprobe/kretprobe 到 tcp_recvmsg_locked（TCP 读路径统一收口，
     *    同时覆盖 reactor 的 recvmsg 与 proactor 的 io_uring_recvmsg） */
    struct bpf_program *p_ent =
        bpf_object__find_program_by_name(obj, "kprobe_tcp_recvmsg_locked");
    struct bpf_program *p_ret =
        bpf_object__find_program_by_name(obj, "kretprobe_tcp_recvmsg_locked");
    if (!p_ent || !p_ret) {
        fprintf(stderr, "[bpf] 找不到 kprobe/kretprobe 程序\n");
        bpf_object__close(obj);
        return 1;
    }
    g_link_ent = bpf_program__attach_kprobe(p_ent, false, "tcp_recvmsg_locked");
    if (!g_link_ent) {
        fprintf(stderr, "[bpf] attach kprobe tcp_recvmsg_locked: %s\n",
                strerror(errno));
        bpf_object__close(obj);
        return 1;
    }
    g_link_ret = bpf_program__attach_kprobe(p_ret, true, "tcp_recvmsg_locked");
    if (!g_link_ret) {
        fprintf(stderr, "[bpf] attach kretprobe tcp_recvmsg_locked: %s\n",
                strerror(errno));
        bpf_link__destroy(g_link_ent);
        bpf_object__close(obj);
        return 1;
    }
    fprintf(stderr, "[bpf] 已挂 tcp_recvmsg_locked (tgid=%ld)\n", target_pid);

    /* 3. 写入目标 master PID → cfg map（BPF 侧按 tgid 过滤） */
    int cfg_fd = bpf_object__find_map_fd_by_name(obj, "cfg");
    if (cfg_fd < 0) {
        fprintf(stderr, "[bpf] 找不到 cfg map\n");
        return 1;
    }
    __u32 key0 = 0;
    __u64 pid = (__u64)target_pid;
    if (bpf_map_update_elem(cfg_fd, &key0, &pid, BPF_ANY) != 0) {
        fprintf(stderr, "[bpf] cfg map 写入失败: %s\n", strerror(errno));
        return 1;
    }

    /* 4. 连从机 */
    g_sock = connect_slave();
    if (g_sock < 0) {
        bpf_link__destroy(g_link_ent);
        bpf_link__destroy(g_link_ret);
        bpf_object__close(obj);
        return 1;
    }

    /* 5. ring 消费者 */
    int rb_fd = bpf_object__find_map_fd_by_name(obj, "events");
    if (rb_fd < 0) {
        fprintf(stderr, "[bpf] 找不到 events map\n");
        return 1;
    }
    struct ring_buffer *rb = ring_buffer__new(rb_fd, on_event, NULL, NULL);
    if (!rb) {
        fprintf(stderr, "[bpf] ring_buffer__new 失败\n");
        return 1;
    }

    /* 速率监测用的两个 per-CPU 计数 map（fd<0 时 read_percpu_sum 兜底为 0） */
    int prod_fd  = bpf_object__find_map_fd_by_name(obj, "produced");
    int drops_fd = bpf_object__find_map_fd_by_name(obj, "drops");

    fprintf(stderr, "[mirror] 开始镜像（Ctrl-C 退出）\n");

    /* 每秒统计：生产速率（BPF produced）vs 消费速率（本进程）。
     * 口径统一为 ring 记录字节：produced/consumed 累计差 = ring 未消费存量。
     * 消费 work-conserving，稳态两者秒差应≈0；backlog>0 表示 ring 积压，
     * drops>0 才真正丢采样。 */
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    long long stat_ms = ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
    unsigned long prod_base  = prod_fd  >= 0 ? read_percpu_sum(prod_fd)  : 0;
    unsigned long cons_base  = g_cons_bytes;
    unsigned long recs_base  = g_cons_recs;

    while (g_run) {
        ring_buffer__poll(rb, 20);          /* 消费 ring → 进待发缓冲 */
        pend_flush();                       /* 尽力发往从机 */
        drain_responses();                  /* 读掉从机应答，绝不让它堵 */

        clock_gettime(CLOCK_MONOTONIC, &ts);
        long long now_ms = ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
        if (now_ms - stat_ms >= 1000) {
            double dt = (now_ms - stat_ms) / 1000.0;
            unsigned long prod  = prod_fd  >= 0 ? read_percpu_sum(prod_fd)  : 0;
            unsigned long drops = drops_fd >= 0 ? read_percpu_sum(drops_fd) : 0;
            long long d_prod = (long long)(prod - prod_base);
            long long d_cons = (long long)(g_cons_bytes - cons_base);
            fprintf(stderr,
                    "[stat] prod=%8.2f MB/s  cons=%8.2f MB/s  "
                    "backlog=%+7.1f KB  drops=%lu  recs=%lu/s\n",
                    d_prod / dt / (1024.0 * 1024.0),
                    d_cons / dt / (1024.0 * 1024.0),
                    (double)(d_prod - d_cons) / 1024.0,
                    drops,
                    (unsigned long)((g_cons_recs - recs_base) / dt));
            stat_ms = now_ms;
            prod_base = prod;
            cons_base = g_cons_bytes;
            recs_base = g_cons_recs;
        }
    }

    /* 6. 清理 + 统计 */
    ring_buffer__free(rb);
    close(g_sock);
    bpf_link__destroy(g_link_ret);
    bpf_link__destroy(g_link_ent);

    unsigned long drops = drops_fd >= 0 ? read_percpu_sum(drops_fd) : 0;
    unsigned long prod  = prod_fd  >= 0 ? read_percpu_sum(prod_fd)  : 0;

    if (g_tee)
        fclose(g_tee);
    bpf_object__close(obj);

    fprintf(stderr, "[mirror] 退出：镜像 %lu 字节，待发峰值 %lu 字节，"
            "BPF ring 丢采样 %lu 条\n"
            "[mirror] 统计：生产总计 %lu 字节，消费总计 %lu 字节"
            "（差 = ring 残留存量）\n",
            g_mirrored, g_pend_peak, drops, prod, g_cons_bytes);
    return 0;
}
