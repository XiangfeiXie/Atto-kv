// SPDX-License-Identifier: GPL-2.0
/*=============================================================================
 *  repl-xdp-sock.bpf.c — XDP 采样（最小 demo）
 *
 *  只做一件事：客户端 → 主服务(MASTER_PORT) 的 TCP 负载采样进 ring buffer，
 *  原包一律 XDP_PASS。
 *
 *  与 repl-xdp（raw socket 版）的区别：
 *    - 不采样从机方向控制帧（MIRROR_CTRL），没有 XDP_DROP 分支。
 *      用户态用内核 TCP socket 连从机，从机的 ACK/+OK 应答走内核栈正常回
 *      到该 socket，XDP 必须全 PASS，不能 DROP。
 *    - 可靠传输（seq/ACK/重传/窗口）全部交给内核 TCP 栈，用户态只做
 *      ring → write()。
 *============================================================================*/

#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

/*=============================================================================
 *  协议常量（vmlinux.h 不带宏，手动补齐）
 *============================================================================*/

#ifndef ETH_P_IP
#define ETH_P_IP 0x0800
#endif
#ifndef IPPROTO_TCP
#define IPPROTO_TCP 6
#endif

/*=============================================================================
 *  配置（与现有 repl-xdp.h 保持同值，避免额外 header 依赖）
 *============================================================================*/

#ifndef MASTER_PORT
#define MASTER_PORT 2000   /* kvstore master 端口，据此识别客户端负载 */
#endif
#ifndef MAX_REC
#define MAX_REC     2048   /* 单条采样上限（veth MTU 1500 → 负载 ≤1460） */
#endif
#ifndef RING_SIZE
#define RING_SIZE   (4 << 20)
#endif

/*=============================================================================
 *  Ring 记录：16 字节固定头（struct sock_rec）+ 变长负载
 *============================================================================*/

struct sock_rec {
    unsigned int type;             /* REC_DATA / REC_SYN */
    unsigned int seq;              /* DATA: 首个负载字节 seq；SYN: ISN+1 */
    unsigned int len;              /* DATA: 负载字节数；SYN: 0 */
    unsigned int sport;            /* 客户端源端口（连接标识） */
};

#define REC_DATA 0
#define REC_SYN  1

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, RING_SIZE);
} events SEC(".maps");

/* 采样丢弃计数（ring 满时 bpf_ringbuf_output 失败）——验证"0 丢"用 */
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u64);
} drops SEC(".maps");

/*=============================================================================
 *  Per-CPU 中转缓冲（bpf_ringbuf_reserve 尺寸须为编译期常量，
 *  运行期变长只能经 output 拷入）
 *============================================================================*/

struct scratch_rec {
    struct sock_rec hdr;
    unsigned char data[MAX_REC];
};

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct scratch_rec);
} scratch SEC(".maps");

static __always_inline void push_rec(unsigned int type, unsigned int sport,
                                     unsigned int seq, long len,
                                     const void *data)
{
    if (len < 0 || len > MAX_REC)
        return;

    __u32 zero = 0;
    struct scratch_rec *rec = bpf_map_lookup_elem(&scratch, &zero);
    if (!rec)
        return;

    rec->hdr.type = type;
    rec->hdr.sport = sport;
    rec->hdr.seq = seq;
    rec->hdr.len = (unsigned int)len;
    if (len > 0)
        bpf_probe_read_kernel(rec->data, len, data);
    if (bpf_ringbuf_output(&events, rec, sizeof(struct sock_rec) + len, 0)) {
        /* ring 满：本条采样丢失，用户态按 seq 重组会卡住 → 靠上限退出 */
        __u32 zero = 0;
        __u64 *d = bpf_map_lookup_elem(&drops, &zero);
        if (d)
            __sync_fetch_and_add(d, 1);
    }
}

SEC("xdp")
int xdp_sock_repl(struct xdp_md *ctx)
{
    void *data = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;

    /* 1. 以太网头 */
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end)
        return XDP_PASS;
    if (eth->h_proto != bpf_htons(ETH_P_IP))
        return XDP_PASS;

    /* 2. IP 头 */
    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end)
        return XDP_PASS;
    if (ip->protocol != IPPROTO_TCP)
        return XDP_PASS;

    unsigned int iph = ip->ihl * 4;
    if (iph < sizeof(struct iphdr))
        return XDP_PASS;

    /* 3. TCP 头 */
    struct tcphdr *tcp = (void *)ip + iph;
    if ((void *)(tcp + 1) > data_end)
        return XDP_PASS;

    /* 4. 客户端 → 主服务：采样 TCP 负载，原包 PASS */
    if (tcp->dest == bpf_htons(MASTER_PORT)) {
        unsigned int tcp_off = tcp->doff * 4;
        unsigned int sport = bpf_ntohs(tcp->source);
        if (tcp->syn) {
            /* SYN：发连接锚点。seq 语义是首个负载字节的序号；SYN 消耗
             * 1 个序号，故锚点 = ISN+1 = 首个数据段 seq */
            push_rec(REC_SYN, sport, bpf_ntohl(tcp->seq) + 1, 0, NULL);
        } else {
            const void *payload = (const void *)tcp + tcp_off;
            if (tcp_off >= sizeof(struct tcphdr) && payload < data_end)
                /* seq 即首个负载字节序号，直接作重组 key（勿加 tcp_off） */
                push_rec(REC_DATA, sport, bpf_ntohl(tcp->seq),
                         (char *)data_end - (char *)payload, payload);
        }
    }

    return XDP_PASS;
}

char LICENSE[] SEC("license") = "GPL";
