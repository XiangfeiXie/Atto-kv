// SPDX-License-Identifier: GPL-2.0
/*=============================================================================
 *  repl-xdp.bpf.c — XDP 筛子
 *  
 *  功能：在网卡入方向拦截数据包，进行采样和分流
 *  
 *  只做两件事，返回后进程负责一切镜像/可靠性逻辑：
 *   a) 客户端 → 主服务的 TCP 负载 → 采样进 ring（MIRROR_CLIENT），原包 PASS
 *   b) 从机 → 镜像端口的 TCP 包（ACK/应答）→ 整帧采样进 ring（MIRROR_CTRL），DROP
 *  
 *  为什么 DROP 从机方向包？
 *  不能 PASS，主内核栈没有这个 socket，PASS 会被 RST
 *============================================================================*/

#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>
#include "repl-xdp.h"

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
 *  BPF Ring Buffer
 *  客户端负载 + 从机控制帧共用一条
 *============================================================================*/

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, RING_SIZE);
} events SEC(".maps");

/*=============================================================================
 *  Per-CPU 中转缓冲
 *  
 *  组好"记录头 + 数据"再整段 ringbuf_output
 *  
 *  为什么不用 bpf_ringbuf_reserve？
 *  它的尺寸参数必须是编译期常量（ARG_CONST_ALLOC_SIZE_OR_ZERO）
 *  运行期变长只能经 output 拷入
 *============================================================================*/

struct scratch_rec {
    struct mirror_rec hdr;          /* 8 字节头部 */
    unsigned char data[MAX_REC];    /* 变长数据 */
};

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct scratch_rec);
} scratch SEC(".maps");

/*=============================================================================
 *  输出到 Ring Buffer
 *============================================================================*/

/**
 * 把 [data, data+len) 连同类型头拷入中转缓冲，再整体进 ring
 * 
 * @param type 记录类型（MIRROR_CLIENT / MIRROR_CTRL）
 * @param data 数据指针
 * @param len 数据长度
 * 
 * 设计要点：
 * - len 用 long 传：让 64 位比较 `len <= 0 || len > MAX_REC` 的边界约束
 *   作用到传给 helper 的同一个寄存器上
 *   用 unsigned int 会被 clang 拆成两个寄存器
 *   verifier 会把 helper 的 size 参数判成"可能为负"
 * - ring 满则丢弃该段（demo 规模不触发；进程应消费得足够快）
 * - bpf_probe_read_kernel 从内核内存读取数据
 */
static __always_inline void push_rec(unsigned char type,
                                     const void *data, long len)
{
    if (len <= 0 || len > MAX_REC)
        return;
    
    __u32 zero = 0;
    struct scratch_rec *rec = bpf_map_lookup_elem(&scratch, &zero);
    if (!rec)
        return;
    
    /* 组装记录：头部 + 数据 */
    rec->hdr.type = type;
    rec->hdr.len = (unsigned int)len;
    bpf_probe_read_kernel(rec->data, len, data);
    bpf_ringbuf_output(&events, rec, sizeof(struct mirror_rec) + len, 0);
}

/*=============================================================================
 *  XDP 入口程序
 *============================================================================*/

SEC("xdp")
int xdp_repl(struct xdp_md *ctx)
{
    void *data = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;

    /*-------------------------------------------------------------------------
     *  1. 解析以太网头
     *-------------------------------------------------------------------------*/
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end)
        return XDP_PASS;
    if (eth->h_proto != bpf_htons(ETH_P_IP))
        return XDP_PASS;

    /*-------------------------------------------------------------------------
     *  2. 解析 IP 头
     *-------------------------------------------------------------------------*/
    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end)
        return XDP_PASS;
    if (ip->protocol != IPPROTO_TCP)
        return XDP_PASS;
    
    unsigned int iph = ip->ihl * 4;
    if (iph < sizeof(struct iphdr))
        return XDP_PASS;

    /*-------------------------------------------------------------------------
     *  3. 解析 TCP 头
     *-------------------------------------------------------------------------*/
    struct tcphdr *tcp = (void *)ip + iph;
    if ((void *)(tcp + 1) > data_end)
        return XDP_PASS;

    /*-------------------------------------------------------------------------
     *  4. 从机 → 镜像端口方向（dst == MIRROR_PORT）
     *     整帧进 ring，DROP
     *     不能 PASS，主内核栈没有这个 socket
     *-------------------------------------------------------------------------*/
    if (tcp->dest == bpf_htons(MIRROR_PORT)) {
        push_rec(MIRROR_CTRL, data, (char *)data_end - (char *)data);
        return XDP_DROP;
    }

    /*-------------------------------------------------------------------------
     *  5. 客户端 → 主服务方向（dst == MASTER_PORT）
     *     TCP 负载进 ring（镜像数据），原包 PASS
     *-------------------------------------------------------------------------*/
    if (tcp->dest == bpf_htons(MASTER_PORT)) {
        unsigned int tcp_off = tcp->doff * 4;
        const void *payload = (const void *)tcp + tcp_off;
        if (tcp_off >= sizeof(struct tcphdr) && payload < data_end)
            push_rec(MIRROR_CLIENT, payload,
                     (char *)data_end - (char *)payload);
    }

    return XDP_PASS;
}

/*=============================================================================
 *  License
 *  GPL 许可证（与内核兼容）
 *============================================================================*/

char LICENSE[] SEC("license") = "GPL";