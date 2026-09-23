// SPDX-License-Identifier: GPL-2.0
/*=============================================================================
 *  repl-tcp_recvmsg.c — kprobe 镜像组件的用户态 loader
 *
 *  用法: repl-tcp_recvmsg [bpf.o]        （无参数即可启动）
 *
 *  用户态只做几件事：
 *    a) 定位目标：按固定端口 MASTER_PORT（repl-tcp_recvmsg.h）找到监听它的
 *       socket inode，再找持有该 inode 的进程。**不解析 conf、不传 PID** ——
 *       端口写死一个宏就够了，原先那套从 conf 抠 port 的解析是多余的。
 *    b) 定位 bpf.o：默认取可执行文件同目录（/proc/self/exe 推导），argv 可覆盖。
 *    c) 加载 BPF，挂 kprobe/kretprobe 到 tcp_recvmsg_locked，把目标 PID 写进
 *       cfg map（tgid 过滤）。
 *    d) 开普通 TCP socket 连从机，把 ring 里镜像到的字节转发过去，并读掉
 *       从机的 +OK 应答（见 drain_responses，关键）。
 *    e) 按 seq 核对顺序：对不上就报错退出（见下）。
 *
 *  运行期不周期打印，只在退出时打一次汇总（附属组件不需要周期打印）。
 *
 *  顺序保证：**master 必须 taskset 固定到单核**。
 *    ring 是 per-CPU 的，libbpf 按 subring 下标顺序排空。master 单线程又被钉
 *    在一个核上 → 所有记录只进那一个 subring → 到达序 == 排空序，顺序是结构性
 *    成立的。一旦跨核迁移，"先到的记录进了 CPU3 的 subring、后到的进了 CPU0"
 *    就会出现，而排空是 0 排在 3 前面 —— 顺序就翻了，从库跟着错位。
 *    这里不做重排：乱序在定义上是罕见事件，写出来的重排路径几乎不会被走到，
 *    等于往"一个字节都不许错"的数据面上放一段没被测过的代码。只按 BPF 打的
 *    全局单调 seq 核对（每条记录都走，是热路径），对不上立刻报错退出并提示
 *    固定单核。镜像宁可停，不可错。
 *
 *  背压设计（ring → 待发缓冲 → socket）：
 *    - socket 非阻塞，ring 每条记录只追加进 g_pend 待发缓冲，绝不在事件回调
 *      里阻塞 → ring 永远即时消费，BPF 采样永不丢弃。
 *    - 每轮循环先尽力 flush g_pend（写不出就下次再写），再 drain 从机应答。
 *      若只发不读，从机 +OK 会把本端 rcvbuf 撑满 → 窗口 0 → 从机写不出去 →
 *      从机停摆（实测死锁，XDP 版踩过）。
 *============================================================================*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <dirent.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>

#include "repl-tcp_recvmsg.h"

/* Ring 记录头（与 BPF 侧逐字节一致：16B 头 + 变长负载） */
struct tcp_rec {
    unsigned long long seq;      /* 全局单调记录序号 */
    unsigned int len;            /* 负载字节数；0 = 毒丸，见 on_event */
    unsigned int __pad;
};

static const char BPF_OBJ_NAME[] = "repl-tcp_recvmsg.bpf.o";

static volatile int g_run = 1;
static int g_sock = -1;                 /* 到从机的连接 */
static unsigned long g_mirrored = 0;    /* 已写入从机的镜像字节数 */

/* 统计：本进程从 ring 消费的记录条数。退出时汇总一次。 */
static unsigned long g_cons_recs = 0;

/* 顺序核对：下一个应到记录序号（BPF 从 0 起、每成功进 ring 一条 +1） */
static unsigned long long g_expect_seq = 0;

/* BPF 侧丢采样计数 map 的 fd（<0 = 没找到，读值兜底为 0），seq 报错时区分成因 */
static int g_drops_fd = -1;

static struct bpf_link *g_link_ent = NULL;
static struct bpf_link *g_link_ret = NULL;

static void on_sigint(int sig)
{
    (void)sig;
    g_run = 0;
}

/* 读 per-CPU u64 计数并跨 CPU 求和（drops 用）。
 * per-CPU map 的 LOOKUP_ELEM 会写 value_size*可用CPU数 字节到用户 buffer
 * （只给 8 字节会越界踩栈，历史 bug），必须按 libbpf_num_possible_cpus()
 * 分配数组再逐 CPU 求和。map_fd 无效时返回 0。 */
static unsigned long read_percpu_sum(int map_fd)
{
    __u32 zero = 0;
    int nr_cpus = libbpf_num_possible_cpus();
    if (nr_cpus <= 0 || map_fd < 0)
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

/* 尽力把待发缓冲写进 socket（写不出 EAGAIN 就留到下次）。
 * 返回 0 = 还能再试（写完了，或只是暂时写不动），-1 = 从机已断，再试没意义。 */
static int pend_flush(void)
{
    while (g_pend_head < g_pend_len) {
        ssize_t n = write(g_sock, g_pend + g_pend_head,
                          g_pend_len - g_pend_head);
        if (n > 0) {
            g_pend_head += (size_t)n;
            g_mirrored += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR)
            continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            return 0;                   /* 内核发送缓冲满，下次再发 */
        fprintf(stderr, "[sock] write: %s（从机可能已断）\n", strerror(errno));
        g_run = 0;
        return -1;
    }
    g_pend_head = g_pend_len = 0;       /* 腾空复用 */
    return 0;
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
    /* size 不判界：BPF 侧 reserve 的就是 sizeof(rec) + len（见 .bpf.c 的
     * emit_rec），故 size 恒等于 16 + rec->len，没有越界可防 */
    (void)size;
    const struct tcp_rec *rec = data;

    g_cons_recs++;

    if (rec->len == 0) {
        /* 毒丸：BPF 侧只有 bpf_probe_read_user 失败才会发出 len=0。正常路径
         * ret > 0 保证 len 恒 > 0。少一段字节 = 从库永久错位，必须停。 */
        fprintf(stderr, "[mirror] 收到 len=0 的非法记录（用户内存读取失败）"
                        "—— 镜像流已不完整，fail-closed 退出\n");
        g_run = 0;
        return 0;
    }
    if (rec->len > size - sizeof(*rec)) {
        fprintf(stderr, "[mirror] 记录长度 %u 超过实际 %zu\n",
                rec->len, size - sizeof(*rec));
        g_run = 0;
        return 0;
    }

    /* 顺序核对：BPF 每成功进 ring 一条记录就把全局 seq +1，所以这里必须严格
     * 连续。对不上只有两种成因，且都意味着镜像流已经不可信：
     *   ① 丢采样 > 0 —— ring 满或记录越界，那一条永久没进 ring；
     *   ② 丢采样 = 0 —— master 线程跨了 CPU，记录落进不同 subring，
     *      libbpf 按 subring 下标排空把到达序打乱了。
     * 不重排、不跳过、不续传，直接停：宁可停，不可错。 */
    if (rec->seq != g_expect_seq) {
        fprintf(stderr,
                "[mirror] seq 不连续：期望 %llu 实际 %llu（BPF ring 丢采样 %lu "
                "条）\n"
                "[mirror] 丢采样为 0 则说明 master 跨了 CPU —— 用 "
                "taskset -c N 固定 master 单核后重跑\n"
                "[mirror] 镜像流已不完整，fail-closed 退出\n",
                g_expect_seq, rec->seq, read_percpu_sum(g_drops_fd));
        g_run = 0;
        return 0;
    }
    g_expect_seq++;

    pend_append((const unsigned char *)data + sizeof(*rec), rec->len);
    return 0;
}

/*=============================================================================
 *  自动定位
 *============================================================================*/

/* 在 /proc/net/tcp{,6} 里找监听 <port> 的 socket inode（找不到返回 0） */
static unsigned long listen_inode(unsigned short port)
{
    static const char *files[] = { "/proc/net/tcp", "/proc/net/tcp6" };

    for (unsigned i = 0; i < sizeof(files) / sizeof(files[0]); i++) {
        FILE *f = fopen(files[i], "r");
        char line[512];
        if (!f)
            continue;

        while (fgets(line, sizeof(line), f)) {
            /* sl local_address rem_address st tx:rx tr:tm retrnsmt uid timeout inode
             * 只取 local_address / st / inode，其余字段用 %* 吞掉（rem_address 之类
             * 此前也读进了一个缓冲区，但从来没被用过）。 */
            char local[128];
            unsigned int st;
            unsigned long ino;

            if (sscanf(line, " %*d: %127s %*127s %x %*s %*s %*s %*u %*u %lu",
                       local, &st, &ino) != 3)
                continue;
            if (st != 0x0A)             /* TCP_LISTEN */
                continue;
            char *colon = strrchr(local, ':');
            if (colon && (unsigned short)strtoul(colon + 1, NULL, 16) == port) {
                fclose(f);
                return ino;
            }
        }
        fclose(f);
    }
    return 0;
}

/* 扫 /proc 下每个进程的 fd 目录，找持有该 socket inode 的进程（-1 = 没找到）。
 * 读别人的 fd 目录要 root —— 本组件加载 BPF 本来就要 root。 */
static long pid_of_inode(unsigned long inode)
{
    char want[64];
    snprintf(want, sizeof(want), "socket:[%lu]", inode);

    DIR *pd = opendir("/proc");
    if (!pd)
        return -1;

    long found = -1;
    struct dirent *pe;
    while (found < 0 && (pe = readdir(pd))) {
        if (pe->d_name[0] < '0' || pe->d_name[0] > '9')
            continue;

        char fddir[300];
        snprintf(fddir, sizeof(fddir), "/proc/%s/fd", pe->d_name);
        DIR *fd = opendir(fddir);
        if (!fd)
            continue;                   /* 读不到（别人的进程）就跳过 */

        struct dirent *fe;
        while ((fe = readdir(fd))) {
            char link[320], tgt[512];
            /* fddir 最长 299、fd 名最长 255，合起来可能撑爆 link。截断的路径
             * readlink 必然失败，直接跳过，不拿半截路径去撞 */
            int w = snprintf(link, sizeof(link), "%s/%s", fddir, fe->d_name);
            if (w < 0 || w >= (int)sizeof(link))
                continue;
            ssize_t n = readlink(link, tgt, sizeof(tgt) - 1);
            if (n <= 0)
                continue;
            tgt[n] = '\0';
            if (strcmp(tgt, want) == 0) {
                found = atol(pe->d_name);
                break;
            }
        }
        closedir(fd);
    }
    closedir(pd);
    return found;
}

/* 定位 bpf.o：可执行文件同目录 → 当前目录。
 * 用 /proc/self/exe 推导而不是写死路径 —— CMake 把可执行文件和 .o 产在同一个
 * build/ 下，这个关系挪目录、换机器、换用户都成立；写死绝对路径一换就废。 */
static const char *find_bpf_obj(const char *argv_path, char *buf, size_t cap)
{
    char exe[512];

    if (argv_path)
        return argv_path;

    ssize_t n = readlink("/proc/self/exe", exe, sizeof(exe) - 1);
    if (n > 0) {
        exe[n] = '\0';
        char *slash = strrchr(exe, '/');
        if (slash) {
            *slash = '\0';
            /* exe 最长 511，加 "/" 和 22 字节的 bpf.o 名可能超 cap。截断的
             * 路径 access 必失败，跳过这个候选，落到下面的 ./ 分支 */
            int w = snprintf(buf, cap, "%s/%s", exe, BPF_OBJ_NAME);
            if (w > 0 && (size_t)w < cap && access(buf, R_OK) == 0)
                return buf;
        }
    }
    snprintf(buf, cap, "./%s", BPF_OBJ_NAME);
    if (access(buf, R_OK) == 0)
        return buf;
    return NULL;
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

static void usage(const char *argv0)
{
    fprintf(stderr,
            "用法: %s [repl-tcp_recvmsg.bpf.o]\n"
            "  master 进程按固定端口 %d 反查（见 repl-tcp_recvmsg.h 的 "
            "MASTER_PORT），无需传 conf 或 PID；\n"
            "  bpf.o 省略则取可执行文件同目录下的 %s。\n"
            "\n"
            "注意：master 必须固定到单核（taskset -c N ./kvstore <conf>），\n"
            "      否则 ring 跨 CPU 乱序，本组件会 fail-closed 退出。\n",
            argv0, MASTER_PORT, BPF_OBJ_NAME);
}

int main(int argc, char **argv)
{
    if (argc > 2) {
        usage(argv[0]);
        return 1;
    }

    /* ---- 1. 定位目标 master PID：固定端口 → 监听它的 socket inode → 属主进程 ---- */
    unsigned long ino = listen_inode(MASTER_PORT);
    if (!ino) {
        fprintf(stderr, "[main] 没有进程在监听端口 %d（master 起了吗？）\n",
                MASTER_PORT);
        return 1;
    }
    long target_pid = pid_of_inode(ino);
    if (target_pid <= 0) {
        fprintf(stderr, "[main] 端口 %d 的属主进程找不到（需要 root 读 "
                        "/proc/*/fd）\n", MASTER_PORT);
        return 1;
    }
    fprintf(stderr, "[main] port %d → pid %ld\n", MASTER_PORT, target_pid);

    /* ---- 2. 定位 bpf.o ---- */
    char obj_buf[512];
    const char *bpf_path = find_bpf_obj(argc > 1 ? argv[1] : NULL,
                                        obj_buf, sizeof(obj_buf));
    if (!bpf_path) {
        fprintf(stderr, "[main] 找不到 %s（放在可执行文件同目录，或用命令行"
                        "参数指定）\n", BPF_OBJ_NAME);
        return 1;
    }

    signal(SIGINT, on_sigint);
    signal(SIGPIPE, SIG_IGN);           /* 从机断连时 write 触发 EPIPE 而非自杀 */

    /* ---- 3. 打开 + 加载 BPF 对象 ---- */
    struct bpf_object *obj = bpf_object__open_file(bpf_path, NULL);
    if (!obj) {
        fprintf(stderr, "[bpf] open %s: %s\n", bpf_path, strerror(errno));
        return 1;
    }
    if (bpf_object__load(obj)) {
        fprintf(stderr, "[bpf] load 失败（需要 root / CAP_BPF，且 memlock 要够）\n");
        bpf_object__close(obj);
        return 1;
    }
    fprintf(stderr, "[bpf] 已加载 %s (tgid=%ld)\n", bpf_path, target_pid);

    /* ---- 4. 先写 cfg 再挂探针：反过来的话探针已开、cfg 还是 0，那一小段
     *         窗口里的读会被 tgid 过滤挡掉，白丢字节 ---- */
    int cfg_fd = bpf_object__find_map_fd_by_name(obj, "cfg");
    if (cfg_fd < 0) {
        fprintf(stderr, "[bpf] 找不到 cfg map\n");
        bpf_object__close(obj);
        return 1;
    }
    __u32 key0 = 0;
    __u64 pid = (__u64)target_pid;
    if (bpf_map_update_elem(cfg_fd, &key0, &pid, BPF_ANY) != 0) {
        fprintf(stderr, "[bpf] cfg map 写入失败: %s\n", strerror(errno));
        bpf_object__close(obj);
        return 1;
    }

    /* ---- 5. 挂 kprobe/kretprobe 到 tcp_recvmsg_locked（TCP 读路径统一收口，
     *         同时覆盖 reactor 的 recv 与 proactor 的 io_uring_prep_recv） ---- */
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

    /* ---- 6. 连从机 ---- */
    g_sock = connect_slave();
    if (g_sock < 0) {
        bpf_link__destroy(g_link_ent);
        bpf_link__destroy(g_link_ret);
        bpf_object__close(obj);
        return 1;
    }

    /* ---- 7. ring 消费者 ---- */
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

    /* 丢采样计数 map（fd<0 时 read_percpu_sum 兜底为 0） */
    g_drops_fd = bpf_object__find_map_fd_by_name(obj, "drops");

    fprintf(stderr, "[mirror] 开始镜像（Ctrl-C 退出）\n");

    while (g_run) {
        ring_buffer__poll(rb, 20);      /* 消费 ring → 顺序核对 → 待发缓冲 */
        pend_flush();                   /* 尽力发往从机 */
        drain_responses();              /* 读掉从机应答，绝不让它堵 */
    }

    /* ---- 8. 收尾：Ctrl-C 不该吞掉已经采到的镜像，把待发缓冲剩下的尽力冲完。
     *         socket 是非阻塞的，写不动就等一会儿再试，最多 2s ---- */
    for (int i = 0; i < 200 && g_pend_len > g_pend_head; i++) {
        if (pend_flush() < 0)
            break;                      /* 从机断了，再等也没用 */
        if (g_pend_len > g_pend_head)
            usleep(10 * 1000);
    }

    /* ---- 9. 清理 + 一次性汇总 ---- */
    ring_buffer__free(rb);
    close(g_sock);
    bpf_link__destroy(g_link_ret);
    bpf_link__destroy(g_link_ent);

    unsigned long drops = read_percpu_sum(g_drops_fd);

    bpf_object__close(obj);

    /* "还剩多少没送出去"直接读待发缓冲的未读尾部，不再维护两套字节账去相减 */
    fprintf(stderr,
            "[mirror] 退出：镜像 %lu 字节（%lu 条记录），待发峰值 %lu 字节，"
            "退出时还剩 %zu 字节没送出去，BPF ring 丢采样 %lu 条\n",
            g_mirrored, g_cons_recs, g_pend_peak,
            g_pend_len - g_pend_head, drops);
    return g_run ? 0 : 1;
}
