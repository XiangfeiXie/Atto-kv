// SPDX-License-Identifier: GPL-2.0
/* repl-recv.bpf.c — uprobe/uretprobe 镜像 libc recv 收到的字节 */

#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include "repl-recv.h"

/* ring buffer：内核态 → 用户态转发通道（每条记录即一次 recv 的原始字节，
 * 长度由 ring buffer 自带帧头给出，用户态无需再打包） */
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1 << 20);       /* 1 MB */
} events SEC(".maps");

/* uprobe 入口暂存的 recv 参数（buf 指针 + 请求长度），按线程索引 */
struct recv_args {
    void *buf;
    __u32 len;
};

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 4096);
    __type(key, __u64);
    __type(value, struct recv_args);
} pending SEC(".maps");

/* 每 CPU 中转缓冲：用户内存 → 这里 → ring buffer */
struct scratch_buf {
    __u8 data[MAX_MIRROR];
};

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct scratch_buf);
} scratch SEC(".maps");

static __always_inline int is_target(void) {
    return (bpf_get_current_pid_tgid() >> 32) == (__u32)TARGET_PID;
}

/* recv(fd, buf, len, flags) 入口：只记 buf 指针与请求长度 */
SEC("uprobe")
int uprobe_recv(struct pt_regs *ctx) {
    if (!is_target())
        return 0;

    __u64 tid = bpf_get_current_pid_tgid();
    struct recv_args a = {
        .buf = (void *)PT_REGS_PARM2(ctx),   /* rsi: buf */
        .len = (__u32)PT_REGS_PARM3(ctx),    /* rdx: len */
    };
    bpf_map_update_elem(&pending, &tid, &a, BPF_ANY);
    return 0;
}

/* recv 出口：按返回值长度把用户缓冲里的字节拷进 ring buffer */
SEC("uretprobe")
int uretprobe_recv(struct pt_regs *ctx) {
    if (!is_target())
        return 0;

    __u64 tid = bpf_get_current_pid_tgid();
    struct recv_args *a = bpf_map_lookup_elem(&pending, &tid);
    if (!a)
        return 0;
    bpf_map_delete_elem(&pending, &tid);

    long ret = PT_REGS_RC(ctx);              /* rax: recv 返回值 */
    if (ret <= 0)
        return 0;

    __u32 n = (__u32)ret;
    if (n > a->len)
        n = a->len;
    if (n > MAX_MIRROR)
        n = MAX_MIRROR;

    __u32 zero = 0;
    __u8 *buf = bpf_map_lookup_elem(&scratch, &zero);
    if (!buf)
        return 0;

    bpf_probe_read_user(buf, n, a->buf);     /* 用户缓冲 → per-CPU 中转 */
    bpf_ringbuf_output(&events, buf, n, 0);  /* 中转 → ring buffer */
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
