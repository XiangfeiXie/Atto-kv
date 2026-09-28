// SPDX-License-Identifier: GPL-2.0
/*=============================================================================
 *  repl-xdp-sock.c — XDP 采样 + 内核 TCP socket 发送（最小 demo）
 *
 *  用户态只做三件事：
 *    a) 挂 XDP 到 veth0（只采样客户端 → 主服务的 TCP 负载）
 *    b) 开一个普通 TCP socket 连从机，把 ring 里采到的负载写过去
 *    c) 读掉从机的 +OK 应答（见 drain_responses 注释，关键）
 *
 *  可靠传输（seq / ACK / 重传 / 窗口）全部交给内核 TCP 栈。
 *  与 raw socket 版（repl-xdp.c）相比，没有手动 TCP 头 / 校验和 /
 *  seq 分配 / RTO 重传缓存 / 发送窗口 / 发送队列。
 *
 *  顺序保证：BPF ring 是 per-CPU 的，veth 多 RX 队列把采样分散到多 CPU，
 *  ring_buffer__poll 跨 ring 读回顺序不保证（实测 ~13 万条后乱序一次）。
 *  每条采样带首个负载字节的 TCP seq 与客户端源端口，用户态按 seq 重组：
 *    - 顺序段直接进待发缓冲；乱序段暂存哈希表（pend）等前序补上再发；
 *    - 重复段（seq 已发出）丢弃。
 *  连接切换：每个客户端 TCP 连接有独立 ISN。BPF 在连接 SYN 时发 REC_SYN
 *  锚点（seq = ISN+1，即首个数据段 seq），用户态见新连接就重置重组状态并
 *  按新 seq 起点续排；SYN 前到达的数据段先暂存（pre），锚点到了再补发。
 *
 *  背压设计（ring → 待发缓冲 → socket）：
 *    - socket 非阻塞，ring 每条记录只追加进 g_pend 待发缓冲，绝不在
 *      事件回调里阻塞 → ring 永远即时消费，BPF 采样永不丢弃。
 *    - 每轮循环先尽力 flush g_pend（写不出就下次再写），再 drain 从机
 *      应答。若只发不读，从机 +OK 会把本端 rcvbuf 撑满 → 窗口 0 →
 *      从机 redis 写不出去 → 从机停摆（实测死锁触发 ring 溢出丢采样）。
 *    - 从机跟不上时 g_pend 增长（从机处理多快就写多快，demo 规模有界）。
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
#include <net/if.h>
#include <poll.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <linux/if_link.h>

/* 与 BPF 侧保持一致（MASTER_PORT 由 BPF 采样生效；用户态经 -D 覆盖）。
 * 默认值对齐本机主从拓扑（master .137 / slave .136，压测客户端也在 master
 * 本机、走回环）：MASTER_PORT=2000、SLAVE_PORT=2000、IFACE=lo、
 * SLAVE_IP=192.168.30.136。
 * 换端口/网卡用 -D 覆盖编译（注意：仅数字宏可经 REPL_XDP_DEFINES 安全传递，
 * 字符串宏 SLAVE_IP/IFACE 的覆盖须改本默认值——CMake 对含引号 -D 的 make
 * 展开会粘参数）。
 * 挂网卡跑（如 README 的 netns+veth demo）要把这三个字符串宏改成
 * veth0 / 10.0.1.2 / 2001；跨机则 IFACE 改 ens33、SLAVE_IP 保持 .136。 */
#ifndef MASTER_PORT
#define MASTER_PORT 2000          /* kvstore master 端口 */
#endif
#ifndef SLAVE_IP
#define SLAVE_IP    "192.168.30.136"   /* 从机 IP */
#endif
#ifndef SLAVE_PORT
#define SLAVE_PORT  2000          /* kvstore slave 端口 */
#endif
#ifndef IFACE
#define IFACE       "lo"          /* master 侧客户端流量所经网卡 */
#endif

struct sock_rec {
    unsigned int type;             /* REC_DATA / REC_SYN（与 BPF 一致） */
    unsigned int seq;              /* DATA: 首个负载字节 seq；SYN: ISN+1 */
    unsigned int len;              /* DATA: 负载字节数；SYN: 0 */
    unsigned int sport;            /* 客户端源端口（连接标识） */
};

#define REC_DATA 0
#define REC_SYN  1

static volatile int g_run = 1;
static int g_sock = -1;
static unsigned long g_mirrored = 0;    /* 已写入从机的镜像字节数 */

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

/* 尽力把待发缓冲写进 socket（写不出 EAGAIN 就留到下次） */
static void pend_flush(void)
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
            return;                         /* 内核发送缓冲满，下次再发 */
        fprintf(stderr, "[sock] write: %s（从机可能已断）\n", strerror(errno));
        g_run = 0;
        return;
    }
    g_pend_head = g_pend_len = 0;           /* 腾空复用 */
}

/*=============================================================================
 *  从机应答处理（必须读掉丢弃）
 *
 *  从机对每条镜像命令回 +OK。不读掉的话，应答把本端 rcvbuf 撑满 →
 *  窗口 0 → 从机写不出去 → 从机 redis 事件循环阻塞 → 从机停摆，
 *  进而让 g_pend 越积越多 → ring 溢出丢采样（实测死锁，见背压设计）。
 *============================================================================*/

static void drain_responses(void)
{
    char buf[8192];
    for (;;) {
        ssize_t n = read(g_sock, buf, sizeof(buf));
        if (n > 0)
            continue;                       /* 丢弃 */
        if (n < 0 && errno == EINTR)
            continue;
        if (n < 0 && errno == EAGAIN)
            break;                          /* 读干净了 */
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
 *  按 TCP seq 重组（ring 跨 CPU 乱序修复）
 *
 *  BPF ring 是 per-CPU 的，veth 多 RX 队列把包分散到多 CPU，消费者
 *  ring_buffer__poll 跨 ring 读回顺序不保证 → 字节流乱序。每条采样带
 *  首个负载字节的 TCP seq 与客户端源端口，这里按 seq 严格拼接：
 *  顺序段直接进待发缓冲，乱序段暂存哈希表，等前序段补上后按序补发。
 *  重复段（seq 已发出）丢弃。
 *
 *  连接切换：每个客户端 TCP 连接有独立 ISN/seq 空间。BPF 在每个连接
 *  SYN 时发 REC_SYN（seq=ISN+1）作锚点；用户态见新连接 SYN 就丢弃
 *  上一连接残留乱序记录并重置 seq 起点。SYN 之前到达的数据段暂存
 *  （pre 列表），等 SYN 锚定起点后按序补发。
 *
 *  前提：干净链路无丢段（ring 永不溢出）。若有丢段会卡住，用
 *  待发记录数上限兜底退出。
 *============================================================================*/

struct pend_rec {
    unsigned int seq;
    unsigned int len;
    struct pend_rec *next;
    unsigned char data[];              /* 变长负载 */
};

#define PEND_BUCKETS 256
static struct pend_rec *g_pend_tab[PEND_BUCKETS];
static unsigned int g_pending;         /* 哈希表里暂存的乱序记录数 */
static unsigned int g_next_seq;        /* 下一个应发出的负载字节 seq */
static unsigned int g_cur_sport;       /* 当前镜像的客户端连接 */
static int g_syn_seen;                 /* 当前连接是否已见 SYN 锚点 */

/* ---- SYN 前到达的数据段暂存（等锚点确定起点） ---- */
struct pre_rec {
    unsigned int sport;
    unsigned int seq;
    unsigned int len;
    struct pre_rec *next;
    unsigned char data[];
};
static struct pre_rec *g_pre;          /* 单链表，pre_flush 按 sport 取 */
static unsigned int g_pre_cnt;

static inline unsigned int pend_hash(unsigned int seq)
{
    return (seq >> 12) & (PEND_BUCKETS - 1);
}

static struct pend_rec *pend_find(unsigned int seq)
{
    struct pend_rec *r = g_pend_tab[pend_hash(seq)];
    for (; r; r = r->next)
        if (r->seq == seq)
            return r;
    return NULL;
}

static void pend_insert(unsigned int seq, unsigned int len,
                        const unsigned char *data)
{
    if (pend_find(seq))
        return;                          /* 同 seq 重复采样 */
    struct pend_rec *r = malloc(sizeof(*r) + len);
    r->seq = seq;
    r->len = len;
    memcpy(r->data, data, len);
    unsigned int h = pend_hash(seq);
    r->next = g_pend_tab[h];
    g_pend_tab[h] = r;
    g_pending++;
}

static void pend_remove(struct pend_rec *r)
{
    struct pend_rec **p = &g_pend_tab[pend_hash(r->seq)];
    while (*p != r)
        p = &(*p)->next;
    *p = r->next;
    free(r);
    g_pending--;
}

static void reassemble_data(unsigned int seq, unsigned int len,
                            const unsigned char *data);

static void pend_clear(void)
{
    unsigned int h;
    for (h = 0; h < PEND_BUCKETS; h++) {
        struct pend_rec *r = g_pend_tab[h];
        while (r) {
            struct pend_rec *n = r->next;
            free(r);
            r = n;
        }
        g_pend_tab[h] = NULL;
    }
    g_pending = 0;
}

static void pre_hold(unsigned int sport, unsigned int seq, unsigned int len,
                     const unsigned char *data)
{
    struct pre_rec *r = malloc(sizeof(*r) + len);
    r->sport = sport;
    r->seq = seq;
    r->len = len;
    memcpy(r->data, data, len);
    r->next = g_pre;
    g_pre = r;
    g_pre_cnt++;
    if (g_pre_cnt > 4096) {
        fprintf(stderr, "[reasm] SYN 前暂存超限 %u（SYN 丢失？），退出\n",
                g_pre_cnt);
        g_run = 0;
    }
}

static void pre_flush(unsigned int sport)
{
    /* 把该连接在 SYN 前到达的数据段交给重组（内部 memcpy，先处理再释放） */
    struct pre_rec *r = g_pre, *prev = NULL;
    while (r) {
        struct pre_rec *n = r->next;
        if (r->sport == sport) {
            if (prev) prev->next = n; else g_pre = n;
            g_pre_cnt--;
            reassemble_data(r->seq, r->len, r->data);
            free(r);
        } else {
            prev = r;
        }
        r = n;
    }
}

static void pre_clear_others(unsigned int sport)
{
    /* 丢弃非本连接的暂存段（换连接后旧连接遗留） */
    struct pre_rec *r = g_pre, *prev = NULL;
    while (r) {
        struct pre_rec *n = r->next;
        if (r->sport != sport) {
            if (prev) prev->next = n; else g_pre = n;
            free(r);
            g_pre_cnt--;
        } else {
            prev = r;
        }
        r = n;
    }
}

static void reassemble_data(unsigned int seq, unsigned int len,
                            const unsigned char *data)
{
    if (seq < g_next_seq)
        return;                          /* 已发出的重复段 */
    if (seq == g_next_seq) {
        pend_append(data, len);          /* 顺序段：直接发出 */
        g_next_seq += len;
        struct pend_rec *r;
        while ((r = pend_find(g_next_seq)) != NULL) {
            /* 乱序段追上：先拷贝数据（pend_append 做 memcpy），再释放 */
            pend_append(r->data, r->len);
            g_next_seq += r->len;
            pend_remove(r);
        }
    } else {
        pend_insert(seq, len, data);     /* 前序未到：暂存 */
        if (g_pending > 4096) {
            fprintf(stderr, "[reasm] 待发乱序记录超限 %u（有丢段？），退出\n",
                    g_pending);
            g_run = 0;
        }
    }
}

static void handle_data(unsigned int sport, unsigned int seq, unsigned int len,
                        const unsigned char *data)
{
    /* 该连接的 SYN 锚点还没到：暂存，等锚点确定起点 */
    if (!g_syn_seen || sport != g_cur_sport) {
        pre_hold(sport, seq, len, data);
        return;
    }
    reassemble_data(seq, len, data);
}

static void handle_syn(unsigned int sport, unsigned int first_seq)
{
    if (!g_syn_seen || sport != g_cur_sport) {
        /* 新连接（或首个连接）：上一连接宣告结束 */
        pend_clear();                    /* 丢弃上一连接残留乱序记录 */
        pre_clear_others(sport);         /* 丢弃其它连接遗留暂存段 */
        g_syn_seen = 1;
        g_cur_sport = sport;
        g_next_seq = first_seq;          /* 精确锚定流起点 */
    }
    pre_flush(sport);                    /* 补发该连接在 SYN 前到达的数据段 */
}

/*=============================================================================
 *  Ring 消费回调：采样记录 → 按 seq 重组 → 待发缓冲（绝不阻塞）
 *============================================================================*/

static int on_event(void *ctx, void *data, size_t size)
{
    (void)ctx;
    /* size 不判界：BPF 侧 output 的就是 sizeof(rec) + len（见 .bpf.c 的
     * push_rec），size 恒等于 16 + rec->len，没有越界可防。REC_DATA 的
     * len 也恒 > 0 —— push_rec 只在 payload < data_end 时才发 DATA。 */
    (void)size;
    const struct sock_rec *rec = data;
    const unsigned char *payload =
        (const unsigned char *)data + sizeof(*rec);
    if (rec->type == REC_SYN)
        handle_syn(rec->sport, rec->seq);
    else
        handle_data(rec->sport, rec->seq, rec->len, payload);
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

/*=============================================================================
 *  挂 XDP 到 IFACE
 *============================================================================*/

static int attach_xdp(int ifindex, struct bpf_program *prog)
{
    int prog_fd = bpf_program__fd(prog);
    if (bpf_xdp_attach(ifindex, prog_fd, XDP_FLAGS_DRV_MODE, NULL) == 0)
        return 0;
    if (bpf_xdp_attach(ifindex, prog_fd, XDP_FLAGS_SKB_MODE, NULL) == 0)
        return 0;
    fprintf(stderr, "[xdp] attach %s 失败（DRV+SKB 都不行）\n", IFACE);
    return -1;
}

/* 读 per-CPU u64 计数并跨 CPU 求和（drops 用，调用点保证 map_fd >= 0）。
 * per-CPU map 的 LOOKUP_ELEM 会写 value_size*可用CPU数 字节到用户 buffer
 * （只给 8 字节会越界踩栈，历史 bug），必须按 libbpf_num_possible_cpus()
 * 分配数组再逐 CPU 求和。 */
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
        fprintf(stderr, "用法: %s <repl-xdp-sock.bpf.o>\n", argv[0]);
        return 1;
    }
    const char *bpf_path = argv[1];

    signal(SIGINT, on_sigint);
    signal(SIGPIPE, SIG_IGN);               /* 从机断连时 write 触发 EPIPE 而非自杀 */

    /* 1. 打开 + 加载 BPF 对象 */
    struct bpf_object *obj = bpf_object__open_file(bpf_path, NULL);
    if (!obj) {
        fprintf(stderr, "[xdp] open %s: %s\n", bpf_path, strerror(errno));
        return 1;
    }
    if (bpf_object__load(obj)) {
        fprintf(stderr, "[xdp] load 失败\n");
        bpf_object__close(obj);
        return 1;
    }

    struct bpf_program *prog =
        bpf_object__find_program_by_name(obj, "xdp_sock_repl");
    if (!prog) {
        fprintf(stderr, "[xdp] 找不到 xdp_sock_repl 程序\n");
        bpf_object__close(obj);
        return 1;
    }

    int ifindex = if_nametoindex(IFACE);
    if (ifindex == 0) {
        fprintf(stderr, "[xdp] if_nametoindex(%s): %s\n",
                IFACE, strerror(errno));
        bpf_object__close(obj);
        return 1;
    }

    /* 2. 挂 XDP */
    if (attach_xdp(ifindex, prog)) {
        bpf_object__close(obj);
        return 1;
    }
    fprintf(stderr, "[xdp] 已挂 %s\n", IFACE);

    /* 3. 连从机 */
    g_sock = connect_slave();
    if (g_sock < 0) {
        bpf_xdp_detach(ifindex, 0, NULL);
        bpf_object__close(obj);
        return 1;
    }

    /* 4. ring 消费者 */
    int rb_fd = bpf_object__find_map_fd_by_name(obj, "events");
    if (rb_fd < 0) {
        fprintf(stderr, "[xdp] 找不到 events map\n");
        return 1;
    }
    struct ring_buffer *rb = ring_buffer__new(rb_fd, on_event, NULL, NULL);
    if (!rb) {
        fprintf(stderr, "[xdp] ring_buffer__new 失败\n");
        return 1;
    }

    /* 退出汇总用的丢采样计数 map（旧 bpf.o 无 drops 时 fd<0，调用点兜底为 0） */
    int drops_fd = bpf_object__find_map_fd_by_name(obj, "drops");

    fprintf(stderr, "[sock] 开始镜像（Ctrl-C 退出）\n");

    while (g_run) {
        ring_buffer__poll(rb, 20);          /* 消费 ring → 进待发缓冲 */
        pend_flush();                       /* 尽力发往从机 */
        drain_responses();                  /* 读掉从机应答，绝不让它堵 */
    }

    /* 5. 清理 + 汇总 */
    ring_buffer__free(rb);
    close(g_sock);
    bpf_xdp_detach(ifindex, 0, NULL);

    unsigned long drops = drops_fd >= 0 ? read_percpu_sum(drops_fd) : 0;

    bpf_object__close(obj);

    fprintf(stderr, "[sock] 退出：镜像 %lu 字节，待发峰值 %lu 字节，"
            "BPF ring 丢采样 %lu 条\n",
            g_mirrored, g_pend_peak, drops);
    return 0;
}
