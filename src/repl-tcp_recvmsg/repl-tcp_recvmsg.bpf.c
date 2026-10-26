// SPDX-License-Identifier: GPL-2.0
/*=============================================================================
 *  repl-tcp_recvmsg.bpf.c — kprobe 镜像 master 进程读到的字节
 *
 *  功能：挂在 tcp_recvmsg_locked（TCP 读路径统一收口）入口/出口，把目标
 *  master 进程每次 recv 读到的字节镜像进 BPF ring buffer，用户态转发从机。
 *
 *  读路径形态（决定本程序结构）：
 *    三引擎（reactor/proactor/ntyco）已统一为「rbuf_reserve 取一段连续空间
 *    → 单缓冲 recv / io_uring_prep_recv」，msg_iter 恒为 ITER_UBUF（实测
 *    reactor 与 proactor 的 iter_type 全部为 0）。于是**一次 recv = 一条
 *    记录**，出口只需把那一段连续用户内存拷进 ring，没有多段 gather 可走。
 *
 *  规模上界：一次 recv 的字节数 ≤ rbuf 节点容量 RBUF_NODE_SIZE（rbuf.h，
 *    三引擎均 rbuf_init(&c->rq, RBUF_NODE_SIZE)），故 MAX_REC 必须 ≥ 它。
 *    越界不静默丢弃，按丢采样计数 —— 用户态据此 fail-closed 退出。
 *    scratch 是 PERCPU_ARRAY，value 过大（>~32KB）创建会 E2BIG，所以不能
 *    靠无限加大 MAX_REC 买保险。
 *  （为什么不用 bpf_ringbuf_reserve 直灌 ring 内存来省掉 scratch 中转：
 *    reserve 的 size 必须是编译期常量，实测传运行期长度被验证器拒
 *    —— "R2 is not a known constant"。所以中转 map 是必需品。）
 *
 *  与 repl-xdp 的定位差异：
 *    - 采样点是"内核重组完成后、拷进应用缓冲那一刻"：拿到的就是按序无重复
 *      的字节流，不需要 TCP 层 seq 重组 / SYN 锚点 / pend 预排。
 *    - tcp_recvmsg_locked 是 read/recv/recvmsg 与 io_uring recvmsg 的共同
 *      收口，同时覆盖 reactor 与 proactor；XDP 版需要 veth/netns。
 *
 *  过滤在 tgid：本程序对全系统 TCP 读都触发（入口一次数组查找即退），只对
 *  目标 master 进程的读做镜像。master 作为服务器只读客户端连接 → 采到的就是
 *  客户端写入的命令字节流。
 *
 *  顺序保证：ring 是 per-CPU 的，master 线程跨核迁移会把记录打散到不同
 *  subring，而用户态是"逐 subring 读干净"，到达序会乱。所以 **master 必须
 *  taskset 固定单核**（单线程 + 单核 ⇒ 只进一个 subring ⇒ 到达序 == 排空序）。
 *  BPF 侧只负责给每条记录打**全局单调 seq**，用户态按它核对，对不上就退出
 *  （见 repl-tcp_recvmsg.c）。不做重排：那是罕见路径，写出来也几乎测不到。
 *============================================================================*/

#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

/*=============================================================================
 *  配置常量（与 repl-xdp 同惯例：源码默认值，换端口走 -D 覆盖）
 *============================================================================*/

#ifndef RING_SIZE
#define RING_SIZE  (4 << 20)     /* events ring 总大小 */
#endif
/* 单条记录负载上限 = scratch 中转缓冲容量（data[MAX_REC]）。
 * 必须 ≥ rbuf 节点容量（RBUF_NODE_SIZE = 1024），见文件头上界说明。
 * 若日后 node_cap 上调，此处必须同步加大 —— 否则 emit 会因越界丢采样，
 * 用户态 fail-closed 退出（不会静默错位，但会直接停）。 */
#ifndef MAX_REC
#define MAX_REC    1024
#endif

/* ITER_UBUF 的值（enum iter_type）。入口只放行单段用户缓冲 —— 三引擎读路径
 * 统一为单缓冲 recv / io_uring_prep_recv 后的唯一形态；显式给出防依赖漂移，
 * 实测已验证（reactor/proactor 的 iter_type 全为 0）。 */
#ifndef ITER_UBUF
#define ITER_UBUF  0
#endif

/*=============================================================================
 *  Ring 记录：16B 头 + 变长负载（一条 = 一次 recv）
 *============================================================================*/

struct tcp_rec {
    __u64 seq;                   /* 全局单调记录序号（乱序/丢段检测） */
    __u32 len;                   /* 负载字节数；0 = 毒丸（见 emit_rec） */
    __u32 __pad;
};

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, RING_SIZE);
} events SEC(".maps");

/* 配置：idx0 = 目标 master 进程 tgid（用户态写入） */
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u64);
} cfg SEC(".maps");

/* 记录序号源：每条成功进 ring 的记录 +1。master 钉单核，per-CPU 值即全局值，
 * 不需要原子操作。 */
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u64);
} seqc SEC(".maps");

/* 采样丢弃计数（ring 满 / 长度越界 → 记录没能进 ring） */
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u64);
} drops SEC(".maps");

/* 不再需要 scratch 中转：emit_rec 直接 bpf_ringbuf_reserve(常量 MAX_REC)，
 * 用户内存只拷一次进 ring。 */

/*=============================================================================
 *  pending：入口暂存的本次读目标（per-CPU 单槽，出口取用）
 *
 *  为什么需要：kretprobe 拿不到入口参数。且 msg_iter.ubuf 在
 *  tcp_recvmsg_locked 执行中会被 iov_iter_advance 推进（ubuf += n），出口
 *  读到的是 base + ret，所以必须在入口把基址照下来。
 *
 *  master 被 taskset 钉在单核，入口/出口在同一 CPU 上，用 per-CPU 单槽即可，
 *  不必按 tid 哈希；入口先清 valid，非目标读的出口不会取到上一条。
 *============================================================================*/

struct pending_rec {
    __u64 ub;                        /* 本次读的用户缓冲基址 */
    __u64 valid;
};

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct pending_rec);
} pending SEC(".maps");

/*=============================================================================
 *  入口：只镜像目标 master 进程的读，快照用户缓冲基址
 *============================================================================*/

SEC("kprobe")
int kprobe_tcp_recvmsg_locked(struct pt_regs *ctx)
{
    __u32 zero = 0;
    struct pending_rec *p = bpf_map_lookup_elem(&pending, &zero);
    if (p)
        p->valid = 0;                    /* 先清：非目标读的出口不会取到上一条 */

    __u64 tgid = bpf_get_current_pid_tgid() >> 32;
    __u64 *want = bpf_map_lookup_elem(&cfg, &zero);
    if (!want || tgid != *want)
        return 0;                        /* 非目标进程：全系统读都会触发，这里放行 */

    /* msg 是调用方（read/recv/io_uring）传下来的 msghdr，内核路径上不为 NULL，
     * 不必判；两次 bpf_probe_read_kernel 失败时也只是保持初值 —— it 初值 0 恰好
     * 是 ITER_UBUF，ub 初值 0 只会在 pending 里留个 0。而 ub=0 的读在出口必然返回
     * 负错误码（-EFAULT），进不了 emit 分支。空指针判在这条链上没有活干。 */
    struct msghdr *msg = (struct msghdr *)PT_REGS_PARM2(ctx);

    /* 只处理 ITER_UBUF（单段连续用户缓冲）。exit 仅镜像这一种形态：
     * 三引擎读路径已统一为单缓冲 recv / io_uring_prep_recv，其它形态
     * （gather/pvec/pipe…）在本项目里不出现。 */
    __u8 it = 0;
    bpf_probe_read_kernel(&it, 1, &msg->msg_iter.iter_type);
    if (it != ITER_UBUF)
        return 0;

    __u64 ub = 0;
    bpf_probe_read_kernel(&ub, 8, &msg->msg_iter.ubuf);

    if (p) {
        p->ub = ub;
        p->valid = 1;
    }
    return 0;
}

/*=============================================================================
 *  出口：按返回值把本次读到的字节拷进 ring（一次读 = 一条记录）
 *============================================================================*/

/* 把 ub 处的 n 字节作为一条记录发进 ring。
 *
 * 调用方已保证 1 ≤ n ≤ MAX_REC（见出口的判界），所以这里不再复查长度：越界的
 * 那条在出口就记成丢采样了，轮不到这里。n 也因此在验证器眼里是 [1, MAX_REC]
 * 的定界标量，bpf_probe_read_user 的长度和 scr->data 的容量天然对齐。
 *
 * n 必须声明为**无符号**：若写成 long，clang 会把 (long)(int)PT_REGS_RC() 的
 * 符号扩展值留在判正负的分支上，而把**原始 RAX** 当长度传进来 —— 验证器眼里
 * 那个值符号未知，直接以 "R2 min value is negative, either use unsigned or
 * 'var &= const'" 拒收（实测踩过）。*/
static __always_inline void emit_rec(__u64 *seqp, const void *ub, __u32 n)
{
    __u32 zero = 0;

    /* 直接 reserve ring 内存：size 用编译期常量（MAX_REC），用户内存只拷一次，
     * 省掉 scratch 中转和第二次拷贝。 */
    struct tcp_rec *rec = bpf_ringbuf_reserve(&events,
                                              sizeof(struct tcp_rec) + MAX_REC, 0);
    if (!rec) {
        /* ring 满：seq 尚未递增，用户态不会等这条；计丢采样 */
        __u64 *d = bpf_map_lookup_elem(&drops, &zero);
        if (d)
            (*d)++;
        return;
    }

    /* 用户内存读不动：发一条 len=0 的记录当"毒丸"。正常路径 ret > 0 ⇒ len
     * 恒 > 0，所以 len==0 是非法值，用户态见到即 fail-closed 退出。不这么做
     * 的话，这条记录会凭空消失而 seq 照常递增 —— 从库静默错位，最坏的一种。 */
    if (bpf_probe_read_user((char *)rec + sizeof(*rec), n, ub))
        n = 0;

    rec->len = (__u32)n;
    rec->seq = (*seqp)++;
    bpf_ringbuf_submit(rec, 0);
}

SEC("kretprobe")
int kretprobe_tcp_recvmsg_locked(struct pt_regs *ctx)
{
    __u32 zero = 0;
    struct pending_rec *p = bpf_map_lookup_elem(&pending, &zero);
    if (!p || !p->valid)
        return 0;                        /* 入口没放行（非目标进程 / 非 UBUF） */
    __u64 ub = p->ub;
    p->valid = 0;

    /* 长度判界。tcp_recvmsg_locked 返回 int，先用 64 位无符号比较做粗筛
     * （挡掉 EOF 与所有错误码），真伪由下面那个 if 按 32 位定。
     *
     * **不要**把这套判界换成 `(long)(int)PT_REGS_RC(ctx)` 那种"截断到 int
     * 再转回来"的形态、再拿它喂 bpf_probe_read_user 当长度：那会让同一个值
     * 在 IR 里存在两种形态，clang 在判界处用有符号副本、在 helper 调用处换回
     * 符号未知的原始 RAX，验证器只认后者，直接以 "R2 min value is negative,
     * either use unsigned or 'var &= const'" 拒收（实测踩了三轮；barrier_var
     * 也挡不住这个替换）。判界一律走 __u64 的 raw，喂 helper 只用 (__u32)raw。 */
    __u64 raw = PT_REGS_RC(ctx);
    if (raw > MAX_REC) {
        /* 必须按 32 位截断再判。**不能假设错误码的高 32 位已符号扩展**：
         * 实测内核在这里返回的 -EAGAIN 是 0x00000000FFFFFFF5，按 64 位无符号
         * 看是个巨大的正数。早先按 (long)raw > 0 判"是不是读到了超长数据"，
         * 于是每次 EAGAIN 都被记成一条丢采样（实测虚报了 6 条）。
         * 截断成 int 后：错误码为负、EOF 为 0，都进不了这个分支；只有真读到
         * 超过 MAX_REC 字节才命中 —— 那是配置漂移（MAX_REC < rbuf 节点容量），
         * 一条本该镜像的记录没能镜像，计丢采样。 */
        if ((int)raw > MAX_REC) {
            __u64 *d = bpf_map_lookup_elem(&drops, &zero);
            if (d)
                (*d)++;
        }
        return 0;
    }
    if (raw == 0)
        return 0;                        /* EOF：对端关闭，无字节可镜像 */

    __u64 *seqp = bpf_map_lookup_elem(&seqc, &zero);
    if (!seqp)
        return 0;

    emit_rec(seqp, (const void *)ub, (__u32)raw);
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
