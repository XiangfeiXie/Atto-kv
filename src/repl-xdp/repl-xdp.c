// SPDX-License-Identifier: GPL-2.0
/*=============================================================================
 *  repl-xdp.c — XDP 镜像的用户态进程
 *  
 *  职责（只做镜像连接发送端的事）：
 *  1. 加载并挂载 repl-xdp.bpf.o 到 IFACE（原生 XDP，失败回退 skb）
 *  2. raw socket (IP_HDRINCL) 作发送端：握手、数据段、应答 ACK、重传
 *  3. 消费 ring buffer：客户端负载 → 分配 seq → 组段发出 + 入重传缓存
 *  4. 从机控制帧 → 握手 / ACK 推进 / 应答 ACK+丢弃
 *  5. RTO：超过 RTO_MS 无 ACK 前进 → 重传 [snd_una, snd_nxt)
 *  
 *  可靠性分工：
 *  - 从机内核 TCP 栈：接收可靠（有序/去重/ACK）
 *  - 本进程：发送可靠（seq 分配、RTO 超时重传、应答 ACK+丢弃）
 *============================================================================*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <time.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <net/if.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <netinet/if_ether.h>
#include <arpa/inet.h>

#include <bpf/libbpf.h>
#include <bpf/bpf.h>
#include <linux/if_link.h>

#include "repl-xdp.h"

/*=============================================================================
 *  运行状态
 *============================================================================*/

static volatile sig_atomic_t g_run = 1;
static void on_sig(int s) { (void)s; g_run = 0; }

/*=============================================================================
 *  镜像连接发送端状态（单线程维护）
 *============================================================================*/

static uint32_t s_snd_nxt;        /* 下一个待分配的 seq（初始 MIRROR_ISN+1） */
static uint32_t s_snd_una;        /* 已被从机累计 ACK 的边界 */
static uint32_t s_rcv_nxt;        /* 本端期望的下一个从机数据 seq（应答 ACK 用） */
static bool     g_established;    /* 握手是否完成 */

/*=============================================================================
 *  重传缓存
 *  seq % CACHE_SLOTS 槽位，seq 校验防槽位冲突误用
 *============================================================================*/

struct cache_slot {
    uint32_t seq;                 /* 数据段的 seq */
    uint16_t len;                 /* 数据长度 */
    unsigned char data[MAX_REC];  /* 负载数据 */
};
static struct cache_slot s_cache[CACHE_SLOTS];

/*=============================================================================
 *  全局状态
 *============================================================================*/

static uint32_t          g_if_ip;     /* 接口 IP（网络序），镜像段源地址 */
static uint32_t          g_slave_ip;  /* 从机地址（网络序） */
static int               g_tx_fd = -1;
static int               g_ifindex;
static struct ring_buffer *g_rb;
static uint64_t          g_progress_ms;   /* 最近一次 ACK 前进/重传的时刻（RTO 基准） */

/*=============================================================================
 *  TCP 标志位
 *============================================================================*/

#define F_SYN 0x02
#define F_RST 0x04
#define F_PSH 0x08
#define F_ACK 0x10

/*=============================================================================
 *  工具函数
 *============================================================================*/

static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/*=============================================================================
 *  TCP 校验和计算
 *  伪头 + TCP 头 + 负载
 *============================================================================*/

static uint16_t tcp_checksum(uint32_t saddr, uint32_t daddr,
                             const void *tcp, unsigned int len)
{
    const uint16_t *p = tcp;
    uint32_t sum = (saddr >> 16) + (saddr & 0xffff) +
                   (daddr >> 16) + (daddr & 0xffff) +
                   htons(IPPROTO_TCP) + htons(len);
    unsigned int i;
    for (i = 0; i + 1 < len; i += 2)
        sum += *p++;
    if (len & 1)
        sum += ((const uint8_t *)tcp)[len - 1];
    while (sum >> 16)
        sum = (sum & 0xffff) + (sum >> 16);
    return (uint16_t)~sum;
}

/*=============================================================================
 *  发送 TCP 段
 *============================================================================*/

/**
 * 组一个镜像连接段并发出
 * 
 * @param seq TCP 序列号
 * @param ack TCP 确认号
 * @param flags TCP 标志位（SYN/RST/PSH/ACK）
 * @param payload 负载数据（可为空）
 * @param plen 负载长度
 * @param cache_it 是否存入重传缓存（数据段 true，握手/ACK 段 false）
 * @return 0 成功，-1 失败
 * 
 * 设计要点：
 * - cache_it=1 时把 (seq,len,payload) 记入重传缓存
 * - payload 可为空（纯 ACK/握手）
 * - 单段负载不超 MTU，避免 IP 分片
 */
static int send_seg(uint32_t seq, uint32_t ack, unsigned int flags,
                    const void *payload, unsigned int plen, bool cache_it)
{
    static unsigned char pkt[1600];
    struct iphdr *ip = (struct iphdr *)pkt;
    struct tcphdr *tc = (struct tcphdr *)(pkt + sizeof(struct iphdr));
    struct sockaddr_in dst;

    if (plen > 1460)
        plen = 1460;

    /* IP 头：check 由内核补（IP_HDRINCL 约定） */
    ip->version  = 4;
    ip->ihl      = 5;
    ip->tos      = 0;
    ip->tot_len  = htons(sizeof(struct iphdr) + sizeof(struct tcphdr) + plen);
    ip->id       = 0;
    ip->frag_off = 0;
    ip->ttl      = 64;
    ip->protocol = IPPROTO_TCP;
    ip->check    = 0;
    ip->saddr    = g_if_ip;
    ip->daddr    = g_slave_ip;

    /* TCP 头 */
    memset(tc, 0, sizeof(struct tcphdr));
    tc->source  = htons(MIRROR_PORT);
    tc->dest    = htons(SLAVE_PORT);
    tc->seq     = htonl(seq);
    tc->ack_seq = htonl(ack);
    tc->doff    = 5;
    tc->syn     = (flags & F_SYN) ? 1 : 0;
    tc->rst     = (flags & F_RST) ? 1 : 0;
    tc->psh     = (flags & F_PSH) ? 1 : 0;
    tc->ack     = (flags & F_ACK) ? 1 : 0;
    tc->window  = htons(65535);
    tc->urg_ptr = 0;
    
    memcpy(pkt + sizeof(struct iphdr) + sizeof(struct tcphdr), payload, plen);
    tc->check = tcp_checksum(ip->saddr, ip->daddr, tc,
                             sizeof(struct tcphdr) + plen);

    /* 存入重传缓存 */
    if (cache_it) {
        struct cache_slot *c = &s_cache[seq % CACHE_SLOTS];
        c->seq = seq;
        c->len = (uint16_t)plen;
        memcpy(c->data, payload, plen);
    }

    dst.sin_family      = AF_INET;
    dst.sin_port        = htons(SLAVE_PORT);
    dst.sin_addr.s_addr = g_slave_ip;
    if (sendto(g_tx_fd, pkt, sizeof(struct iphdr) + sizeof(struct tcphdr) + plen,
               0, (struct sockaddr *)&dst, sizeof(dst)) < 0) {
        perror("sendto");
        return -1;
    }
    return 0;
}

/*=============================================================================
 *  Ring Buffer 事件处理
 *============================================================================*/

/**
 * 处理客户端负载
 * 客户端负载 → 组数据段发出并入缓存
 * 镜像连接未就绪时丢弃
 */
static void handle_client(const unsigned char *payload, unsigned int plen)
{
    uint32_t seq;

    if (!g_established || plen == 0)
        return;
    seq = s_snd_nxt;
    s_snd_nxt += plen;
    send_seg(seq, s_rcv_nxt, F_ACK | F_PSH, payload, plen, true);
}

/**
 * 处理从机方向控制帧
 * 从机方向控制帧 → 握手 / ACK 推进 / 应答 ACK+丢弃
 */
static void handle_ctrl(const unsigned char *frame, unsigned int flen)
{
    struct ethhdr *eth = (struct ethhdr *)frame;
    struct iphdr *ip;
    struct tcphdr *tc;
    unsigned int iph, tcp_off, plen;
    uint32_t seq, ack;

    /* 解析以太网/IP/TCP 头 */
    if (flen < sizeof(struct ethhdr) + sizeof(struct iphdr))
        return;
    if (eth->h_proto != htons(ETH_P_IP))
        return;
    ip = (struct iphdr *)(frame + sizeof(struct ethhdr));
    if (ip->protocol != IPPROTO_TCP)
        return;
    iph = ip->ihl * 4;
    if (flen < sizeof(struct ethhdr) + iph + sizeof(struct tcphdr))
        return;
    tc = (struct tcphdr *)((unsigned char *)ip + iph);
    tcp_off = tc->doff * 4;
    if (tcp_off < sizeof(struct tcphdr))
        return;
    plen = flen - (sizeof(struct ethhdr) + iph + tcp_off);

    seq = ntohl(tc->seq);
    ack = ntohl(tc->ack_seq);

    /* RST：连接重置 */
    if (tc->rst) {
        fprintf(stderr, "[repl-xdp] mirror conn reset by slave\n");
        g_run = 0;
        return;
    }

    /* 握手：SYN-ACK → 记从机 seq 基线，本端进入可发状态 */
    if (tc->syn && tc->ack && !g_established) {
        s_rcv_nxt = seq + 1;
        g_established = true;
        printf("[repl-xdp] handshake: slave seq base %u\n", seq);
        return;
    }
    if (!g_established)
        return;

    /**
     * 从机数据（+OK 应答）：ACK 掉并丢弃
     * 
     * 为什么必须 ACK？
     * 不 ACK 会积压从机应答，一旦从机 reactor 被满缓冲写卡住，
     * 整条镜像流随之停摆
     */
    if (plen > 0) {
        s_rcv_nxt = seq + plen;
        send_seg(s_snd_nxt, s_rcv_nxt, F_ACK, NULL, 0, false);
        g_progress_ms = now_ms();
    }
    
    /* ACK 推进发送窗口 */
    if (tc->ack && (int32_t)(ack - s_snd_una) > 0) {
        s_snd_una = ack;
        g_progress_ms = now_ms();
    }
}

/**
 * Ring buffer 事件回调
 * 根据类型分发给 client/ctrl 处理器
 */
static int on_event(void *ctx, void *data, size_t size)
{
    struct mirror_rec *rec = data;

    (void)ctx;
    if (size < sizeof(struct mirror_rec))
        return 0;
    if (rec->len > size - sizeof(struct mirror_rec))   /* 越界防御 */
        return 0;
    if (rec->type == MIRROR_CLIENT)
        handle_client((unsigned char *)data + sizeof(struct mirror_rec), rec->len);
    else if (rec->type == MIRROR_CTRL)
        handle_ctrl((unsigned char *)data + sizeof(struct mirror_rec), rec->len);
    return 0;
}

/*=============================================================================
 *  RTO 重传
 *============================================================================*/

/**
 * RTO 重传
 * 从缓存重发 [snd_una, snd_nxt) 之间仍未确认的段
 */
static void retransmit(void)
{
    unsigned int i;

    for (i = 0; i < CACHE_SLOTS; i++) {
        struct cache_slot *c = &s_cache[i];
        if (c->len && (int32_t)(c->seq - s_snd_una) >= 0 &&
                       (int32_t)(c->seq - s_snd_nxt) < 0)
            send_seg(c->seq, s_rcv_nxt, F_ACK | F_PSH, c->data, c->len, false);
    }
}

/*=============================================================================
 *  握手建立
 *============================================================================*/

/**
 * 建立镜像连接
 * SYN →（ring 收 SYN-ACK）→ ACK
 * SYN 丢了就重发
 */
static int do_handshake(void)
{
    uint64_t t0 = now_ms();

    send_seg(MIRROR_ISN, 0, F_SYN, NULL, 0, false);
    s_snd_nxt = MIRROR_ISN + 1;            /* SYN 占一个序号 */
    g_progress_ms = now_ms();

    while (g_run && !g_established) {
        ring_buffer__poll(g_rb, 20);
        if (now_ms() - t0 > 3000) {
            fprintf(stderr, "[repl-xdp] handshake timeout\n");
            return -1;
        }
        if (now_ms() - g_progress_ms > RTO_MS) {   /* 重发 SYN */
            send_seg(MIRROR_ISN, 0, F_SYN, NULL, 0, false);
            g_progress_ms = now_ms();
        }
    }
    if (g_established)                       /* ACK SYN-ACK */
        send_seg(s_snd_nxt, s_rcv_nxt, F_ACK, NULL, 0, false);
    return g_established ? 0 : -1;
}

/*=============================================================================
 *  主函数
 *============================================================================*/

int main(int argc, char *argv[])
{
    const char *bpf_path = (argc > 1) ? argv[1] : "repl-xdp.bpf.o";
    struct bpf_object *obj;
    struct bpf_program *prog;
    struct ifreq ifr = {0};
    int prog_fd, rb_fd, one = 1, ds;
    struct in_addr ia;

    setvbuf(stdout, NULL, _IONBF, 0);   /* 调试日志实时落盘 */

    /*-------------------------------------------------------------------------
     *  1. 加载 BPF 程序
     *-------------------------------------------------------------------------*/
    obj = bpf_object__open_file(bpf_path, NULL);
    if (!obj) {
        fprintf(stderr, "[repl-xdp] open %s: %s\n", bpf_path, strerror(errno));
        return 1;
    }
    if (bpf_object__load(obj)) {
        fprintf(stderr, "[repl-xdp] load bpf failed\n");
        return 1;
    }
    prog = bpf_object__find_program_by_name(obj, "xdp_repl");
    if (!prog) {
        fprintf(stderr, "[repl-xdp] find prog xdp_repl\n");
        return 1;
    }
    prog_fd = bpf_program__fd(prog);

    /*-------------------------------------------------------------------------
     *  2. 挂载 XDP 到网卡
     *     原生优先，失败回退 skb
     *-------------------------------------------------------------------------*/
    g_ifindex = if_nametoindex(IFACE);
    if (!g_ifindex) {
        fprintf(stderr, "[repl-xdp] iface %s not found\n", IFACE);
        return 1;
    }
    if (bpf_xdp_attach(g_ifindex, prog_fd, XDP_FLAGS_DRV_MODE, NULL) < 0 &&
        bpf_xdp_attach(g_ifindex, prog_fd, XDP_FLAGS_SKB_MODE, NULL) < 0) {
        fprintf(stderr, "[repl-xdp] attach xdp to %s: %s\n", IFACE, strerror(errno));
        return 1;
    }

    /*-------------------------------------------------------------------------
     *  3. 创建 ring buffer 消费者
     *-------------------------------------------------------------------------*/
    rb_fd = bpf_object__find_map_fd_by_name(obj, "events");
    if (rb_fd < 0) {
        fprintf(stderr, "[repl-xdp] find map events\n");
        goto out;
    }
    g_rb = ring_buffer__new(rb_fd, on_event, NULL, NULL);
    if (!g_rb) {
        fprintf(stderr, "[repl-xdp] ring_buffer__new\n");
        goto out;
    }

    /*-------------------------------------------------------------------------
     *  4. 创建 raw socket（IP_HDRINCL 自组 IP/TCP 头）
     *-------------------------------------------------------------------------*/
    g_tx_fd = socket(AF_INET, SOCK_RAW, IPPROTO_RAW);
    if (g_tx_fd < 0) {
        perror("socket(RAW)");
        goto out;
    }
    setsockopt(g_tx_fd, IPPROTO_IP, IP_HDRINCL, &one, sizeof(one));

    /*-------------------------------------------------------------------------
     *  5. 获取接口 IP（作为镜像段源地址）
     *-------------------------------------------------------------------------*/
    ds = socket(AF_INET, SOCK_DGRAM, 0);
    strncpy(ifr.ifr_name, IFACE, IFNAMSIZ - 1);
    if (ioctl(ds, SIOCGIFADDR, &ifr) < 0) {
        perror("SIOCGIFADDR");
        close(ds);
        goto out;
    }
    close(ds);
    g_if_ip = ((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr.s_addr;

    if (inet_pton(AF_INET, SLAVE_IP, &g_slave_ip) != 1) {
        fprintf(stderr, "[repl-xdp] bad SLAVE_IP\n");
        goto out;
    }

    /*-------------------------------------------------------------------------
     *  6. 信号处理
     *-------------------------------------------------------------------------*/
    signal(SIGINT, on_sig);
    signal(SIGTERM, on_sig);

    /*-------------------------------------------------------------------------
     *  7. 握手建立
     *-------------------------------------------------------------------------*/
    if (do_handshake() < 0)
        goto out;

    ia.s_addr = g_if_ip;
    printf("[repl-xdp] %s(%s) -> %s:%d mirror_port=%d snd_nxt=%u\n",
           IFACE, inet_ntoa(ia), SLAVE_IP, SLAVE_PORT, MIRROR_PORT, s_snd_nxt);

    /*-------------------------------------------------------------------------
     *  8. 主循环：消费 ring buffer + RTO 重传
     *-------------------------------------------------------------------------*/
    while (g_run) {
        ring_buffer__poll(g_rb, 20);
        /* RTO：有未确认数据且超时无 ACK 前进 → 重传 */
        if (g_established && (int32_t)(s_snd_nxt - s_snd_una) > 0 &&
            now_ms() - g_progress_ms > RTO_MS) {
            retransmit();
            g_progress_ms = now_ms();
        }
    }

out:
    bpf_xdp_detach(g_ifindex, 0, NULL);   /* 卸载 XDP */
    if (g_rb)
        ring_buffer__free(g_rb);
    if (g_tx_fd >= 0)
        close(g_tx_fd);
    bpf_object__close(obj);
    return 0;
}