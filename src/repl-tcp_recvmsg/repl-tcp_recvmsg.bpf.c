// SPDX-License-Identifier: GPL-2.0
/*=============================================================================
 *  repl-tcp_recvmsg.bpf.c — kprobe 镜像 master 进程 recvmsg 收到的字节（最小 demo）
 *
 *  功能：挂在 tcp_recvmsg_locked（TCP 读路径统一收口）入口/出口，把目标
 *  master 进程每次读到的字节镜像进 BPF ring buffer。用户态 loader 转发到从机。
 *
 *  与 repl-xdp（XDP 采样）的区别：
 *    - 采样点是"内核重组完成后、拷进应用缓冲那一刻"：拿到的就是按序无重复
 *      的字节流，不需要 TCP 层 seq 重组 / SYN 锚点 / pend 预排。
 *    - 目标用 tgid 过滤（argv 传 master PID），不碰网卡；演示全程 127.0.0.1，
 *      无需 netns/veth（XDP 在 lo 上采不到分段负载的局限这里不存在）。
 *
 *  与 repl-recv（uprobe libc recv）的区别：
 *    - tcp_recvmsg_locked 是 read/recv/recvmsg 与 io_uring recvmsg 的共同收口，
 *      同时覆盖 reactor 与 proactor；uprobe libc recv 只覆盖单缓冲 recv 且
 *      io_uring 根本不经过 libc。
 *
 *  kvstore 读路径形态（决定本程序结构）：
 *    引擎用 recvmsg/io_uring_recvmsg 做"多段 gather"——msg_iter 是 ITER_IOVEC，
 *    最多 RBUF_MAX_IOV=16 段、每段 ≤ 1KB 的 rbuf 节点，一次读出的字节散在多个
 *    用户段里。因此出口要按 msg_iter 逐段把用户内存拷进 ring，一段 = 一条记录，
 *    消费侧按记录序拼接即得完整字节流。
 *
 *  过滤在 tgid：本程序对全系统 TCP 读都触发（入口一个数组查找即退），只对
 *  目标 master 进程的读做镜像。master 作为服务器只读客户端连接 → 采到的就是
 *  客户端写入的命令字节流。
 *
 *  顺序保证：ring 是 per-CPU 的。演示用 taskset 把 master 钉在单 CPU，生产都在
 *  同一 subring，ring_buffer__poll 按序读回；每条记录仍带全局单调 seq，乱序即
 *  报错退出（fail-closed），不静默错序。
 *============================================================================*/

#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

/*=============================================================================
 *  配置常量（与 repl-recv/repl-xdp 惯例：源码默认值，换端口走 -D 覆盖）
 *============================================================================*/

#ifndef RING_SIZE
#define RING_SIZE  (4 << 20)     /* events ring 总大小 */
#endif
#ifndef MAX_IOV
#define MAX_IOV    16            /* 对齐 RBUF_MAX_IOV（rbuf.h） */
#endif
/* 单条记录负载上限 = scratch 中转缓冲容量（data[MAX_REC]）。
 *
 * 为什么是 1KB 不是 32KB：引擎 rbuf 每段 iov_len = node_cap = RBUF_NODE_SIZE
 * （rbuf.h，两引擎均 rbuf_init(&c->rq, RBUF_NODE_SIZE)），一次读的每个用户段
 * ≤ 1KB → 一段一条记录，单段拷贝 ≤ 1KB 即够。scratch 是 PERCPU_ARRAY，value
 * 过大（>~32KB）创建会 E2BIG（per-CPU map 值大小限制），故必须收小；
 * 若日后 node_cap 上调，此处须同步加大。 */
#ifndef MAX_REC
#define MAX_REC    1024
#endif

/* ITER_IOVEC 值（enum iter_type，见 vmlinux.h；显式给出防依赖漂移）。
 * 入口只放行 IOVEC：目标进程（kvstore master）读客户端命令恒经
 * import_iovec → IOVEC，read()/recv() 单缓冲的 ITER_UBUF 不镜像。 */
#ifndef ITER_IOVEC
#define ITER_IOVEC 1
#endif

/*=============================================================================
 *  Ring 记录：16B 头 + 变长负载（一条 = 一次读中灌满的一个用户段）
 *============================================================================*/

struct tcp_rec {
    __u64 seq;                   /* 全局单调记录序号（乱序/丢段检测） */
    __u32 len;                   /* 负载字节数 */
    __u32 __pad;
};

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, RING_SIZE);
} events SEC(".maps");

/* 配置：idx0 = 目标 master 进程 tgid（用户态 argv 写入） */
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u64);
} cfg SEC(".maps");

/* 记录序号源：每条成功进 ring 的记录 +1（原子），用户态按序核对 */
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u64);
} seqc SEC(".maps");

/* 采样丢弃计数（ring 满 → bpf_ringbuf_output 失败） */
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u64);
} drops SEC(".maps");

/* 成功产出字节计数（= ring 记录字节，与用户态消费同口径，速率监测用） */
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u64);
} produced SEC(".maps");

/*=============================================================================
 *  Per-CPU 中转缓冲（用户内存 → 中转 → ring）
 *============================================================================*/

struct scratch_rec {
    struct tcp_rec hdr;
    unsigned char data[MAX_REC];
};

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct scratch_rec);
} scratch SEC(".maps");

/*=============================================================================
 *  pending：入口暂存的读目标快照（按 tid 索引，出口取用）
 *
 *  为什么需要：kretprobe 拿不到入口参数。tcp_recvmsg_locked 执行中会把数据
 *  拷进 msg_iter 指向的用户段并推进迭代器，所以必须在入口把"本次读要灌进哪
 *  些段"照下来，出口才能知道去哪些用户地址拷。
 *============================================================================*/

struct pending_rec {
    __u32 nr_segs;               /* 有效段数（≤ MAX_IOV） */
    __u64 iovp;                  /* 内核 iov 数组指针（import_iovec 的内核副本） */
    __u64 iov_off;               /* msg_iter.iov_offset（kvstore 读恒 0） */
};

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 4096);
    __type(key, __u64);              /* tid */
    __type(value, struct pending_rec);
} pending SEC(".maps");

/*=============================================================================
 *  入口：只镜像目标 master 进程的读，快照 msg_iter
 *============================================================================*/

SEC("kprobe")
int kprobe_tcp_recvmsg_locked(struct pt_regs *ctx)
{
    __u64 tgid = bpf_get_current_pid_tgid() >> 32;
    __u32 zero = 0;
    __u64 *want = bpf_map_lookup_elem(&cfg, &zero);
    if (!want || tgid != *want)
        return 0;                        /* 非目标进程：全系统读都会触发，这里放行 */

    struct msghdr *msg = (struct msghdr *)PT_REGS_PARM2(ctx);
    if (!msg)
        return 0;

    struct pending_rec p = {};
    /* 只处理 ITER_IOVEC：kvstore 引擎的 recvmsg/io_uring_recvmsg 恒经
     * import_iovec → msg_iter 是 IOVEC（read()/recv() 单缓冲属 ITER_UBUF，
     * 目标进程不用 → 不镜像，见文件头注释）。其它形态（pipe/bvec…）亦不适用。 */
    __u8 it = 0;
    bpf_probe_read_kernel(&it, 1, &msg->msg_iter.iter_type);
    if (it != ITER_IOVEC)
        return 0;
    /* 快照 msg_iter：只取出口需要的字段（msg_iter 在函数体内会被推进） */
    bpf_probe_read_kernel(&p.iov_off, 8, &msg->msg_iter.iov_offset);
    bpf_probe_read_kernel(&p.iovp, 8, &msg->msg_iter.__iov);
    long ns = 0;
    bpf_probe_read_kernel(&ns, 8, &msg->msg_iter.nr_segs);
    if (ns <= 0)
        return 0;
    p.nr_segs = ns > MAX_IOV ? MAX_IOV : (__u32)ns;

    __u64 tid = bpf_get_current_pid_tgid();
    bpf_map_update_elem(&pending, &tid, &p, BPF_ANY);
    return 0;
}

/*=============================================================================
 *  出口：按返回值把本次读到的字节从各用户段拷进 ring（一段一条记录）
 *============================================================================*/

/*=============================================================================
 *  出口：按返回值把本次读到的字节从各用户段拷进 ring（一段一条记录）
 *============================================================================*/

/* 把一段用户内存拷进 scratch 并作为一条记录进 ring。
 * 返回 0=进 ring 成功；1=ring 满丢采样；-1=参数非法/用户内存不可读（放弃）。
 * n 受 MAX_REC 上限约束（scratch.data 容量）。 */
static __always_inline int emit_seg(struct scratch_rec *scr, __u64 *seqp,
                                    const void *ub, long n)
{
    if (n <= 0 || n > MAX_REC)
        return -1;
    if (bpf_probe_read_user(scr->data, n, ub))
        return -1;

    scr->hdr.len = (__u32)n;
    scr->hdr.seq = __sync_fetch_and_add(seqp, 1);
    __u32 zero = 0;
    if (bpf_ringbuf_output(&events, scr, sizeof(struct tcp_rec) + n, 0)) {
        __u64 *d = bpf_map_lookup_elem(&drops, &zero);
        if (d)
            __sync_fetch_and_add(d, 1);
        return 1;
    }
    __u64 *p = bpf_map_lookup_elem(&produced, &zero);
    if (p)
        __sync_fetch_and_add(p, sizeof(struct tcp_rec) + n);
    return 0;
}

SEC("kretprobe")
int kretprobe_tcp_recvmsg_locked(struct pt_regs *ctx)
{
    __u64 tid = bpf_get_current_pid_tgid();
    struct pending_rec *p = bpf_map_lookup_elem(&pending, &tid);
    if (!p)
        return 0;
    bpf_map_delete_elem(&pending, &tid);

    /* tcp_recvmsg_locked 返回 int：EAX 高 32 位在 x86-64 上被 32 位写清零，
     * 直接按 64 位无符号读会把 -EAGAIN(-11) 误读成 4294967285（绕过下面守卫、
     * 把没数据的段当 16KB 垃圾拷进 ring）。必须先截断到有符号 int 再符号扩展。 */
    long ret = (long)(int)PT_REGS_RC(ctx);
    if (ret <= 0)
        return 0;                        /* EAGAIN / EOF / 错误：无字节可镜像 */

    __u32 zero = 0;
    __u64 *seqp = bpf_map_lookup_elem(&seqc, &zero);
    struct scratch_rec *scr = bpf_map_lookup_elem(&scratch, &zero);
    if (!seqp || !scr)
        return 0;

    /* 逐段消费 ret 个字节。段 i 能被灌满的上限 = 去首段 iov_offset 后的本段
     * iov_len（≤ node_cap = 1KB = MAX_REC），与 remain 取小 → 一段一条记录。 */
    long remain = ret;
    __u64 base_off = p->iov_off;         /* iov_offset 只作用于首段 */

    for (__u32 i = 0; i < MAX_IOV && remain > 0; i++) {
        if (i >= p->nr_segs)
            break;
        /* iov 描述符数组在内核（msg_iter.iov = import_iovec 的内核副本），
         * 每个元素 { void *iov_base; size_t iov_len; } */
        __u64 dv = p->iovp + (__u64)i * 16;
        __u64 sbase = 0;
        long slen = 0;
        bpf_probe_read_kernel(&sbase, 8, (void *)dv);
        bpf_probe_read_kernel(&slen, 8, (void *)(dv + 8));
        if (i == 0) {
            sbase += base_off;
            slen  -= (long)base_off;
        }
        if (slen <= 0)
            continue;

        long n = remain < slen ? remain : slen;
        if (n <= 0)
            break;
        emit_seg(scr, seqp, (const void *)sbase, n);
        remain -= n;
    }
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
