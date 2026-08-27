// SPDX-License-Identifier: GPL-2.0
/*=============================================================================
 *  repl-recv.bpf.c — uprobe/uretprobe 镜像 libc recv 收到的字节
 *  
 *  功能：通过 eBPF uprobe/uretprobe 拦截目标进程的 recv 系统调用
 *  将收到的数据镜像到 BPF ring buffer，供用户态程序读取并转发
 *  
 *  设计要点：
 *  1. uprobe 入口记录 recv 参数（buf 指针 + 请求长度）
 *  2. uretprobe 出口根据返回值读取用户内存并输出到 ring buffer
 *  3. per-CPU 中转缓冲绕过 ring buffer 的编译期常量限制
 *  4. 目标进程过滤在挂载时按 pid 做，BPF 侧无过滤
 *============================================================================*/

#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include "repl-recv.h"

/*=============================================================================
 *  BPF Ring Buffer
 *  内核态 → 用户态转发通道
 *  
 *  每条记录即一次 recv 的原始字节
 *  长度由 ring buffer 自带帧头给出，用户态无需再打包
 *============================================================================*/

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1 << 20);       /* 1 MB */
} events SEC(".maps");

/*=============================================================================
 *  pending 哈希表
 *  
 *  存储 uprobe 入口暂存的 recv 参数（buf 指针 + 请求长度）
 *  按线程 ID 索引，供 uretprobe 出口使用
 *  
 *  设计原因：uretprobe 无法直接访问 uprobe 入口的参数
 *  需要通过 map 传递
 *============================================================================*/

struct recv_args {
    void *buf;      /* 用户态缓冲区指针 */
    __u32 len;      /* 请求读取的字节数 */
};

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 4096);
    __type(key, __u64);            /* 线程 ID */
    __type(value, struct recv_args);
} pending SEC(".maps");

/*=============================================================================
 *  Per-CPU 中转缓冲
 *  
 *  限制原因：
 *  bpf_ringbuf_output 的 size 参数必须是编译期常量
 *  （ARG_CONST_ALLOC_SIZE_OR_ZERO）
 *  运行期字节数只能经中转缓冲拷入 ring buffer
 *  
 *  工作流程：
 *  用户内存 → per-CPU 中转 → ring buffer
 *  
 *  最大容量：MAX_MIRROR（默认 32KB）
 *============================================================================*/

struct scratch_buf {
    __u8 data[MAX_MIRROR];
};

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct scratch_buf);
} scratch SEC(".maps");

/*=============================================================================
 *  uprobe 入口
 *  
 *  挂载点：libc recv 函数入口
 *  函数原型：ssize_t recv(int sockfd, void *buf, size_t len, int flags)
 *  
 *  x86-64 调用约定：
 *  - rdi: sockfd (参数1)
 *  - rsi: buf     (参数2) ← PT_REGS_PARM2
 *  - rdx: len     (参数3) ← PT_REGS_PARM3
 *  - rcx: flags   (参数4)
 *  
 *  行为：记录 buf 指针和 len 到 pending map
 *  
 *  目标进程过滤：在挂载时按 pid 做
 *  BPF 侧无过滤，只对目标进程生效
 *============================================================================*/

SEC("uprobe")
int uprobe_recv(struct pt_regs *ctx) {
    __u64 tid = bpf_get_current_pid_tgid();
    struct recv_args a = {
        .buf = (void *)PT_REGS_PARM2(ctx),   /* rsi: buf 指针 */
        .len = (__u32)PT_REGS_PARM3(ctx),    /* rdx: 请求长度 */
    };
    bpf_map_update_elem(&pending, &tid, &a, BPF_ANY);
    return 0;
}

/*=============================================================================
 *  uretprobe 出口
 *  
 *  挂载点：libc recv 函数返回
 *  
 *  x86-64 调用约定：
 *  - rax: recv 返回值 ← PT_REGS_RC
 *  
 *  行为：
 *  1. 从 pending map 查找对应线程的 recv 参数
 *  2. 获取 recv 返回值（实际收到的字节数）
 *  3. 限制拷贝长度（不超过请求长度和 MAX_MIRROR）
 *  4. bpf_probe_read_user 从用户内存读取数据到 per-CPU 中转
 *  5. bpf_ringbuf_output 输出到 ring buffer
 *  
 *  安全设计：
 *  - 返回值 <= 0 时跳过（无数据或错误）
 *  - 限制长度防止越界
 *  - bpf_probe_read_user 安全读取用户内存
 *============================================================================*/

SEC("uretprobe")
int uretprobe_recv(struct pt_regs *ctx) {
    __u64 tid = bpf_get_current_pid_tgid();
    struct recv_args *a = bpf_map_lookup_elem(&pending, &tid);
    if (!a)
        return 0;
    bpf_map_delete_elem(&pending, &tid);

    long ret = PT_REGS_RC(ctx);              /* rax: recv 返回值 */
    if (ret <= 0)
        return 0;

    /* 限制拷贝长度 */
    __u32 n = (__u32)ret;
    if (n > a->len)
        n = a->len;
    if (n > MAX_MIRROR)
        n = MAX_MIRROR;

    /* 获取 per-CPU 中转缓冲 */
    __u32 zero = 0;
    __u8 *buf = bpf_map_lookup_elem(&scratch, &zero);
    if (!buf)
        return 0;

    /* 用户内存 → per-CPU 中转缓冲 */
    bpf_probe_read_user(buf, n, a->buf);
    
    /* 中转缓冲 → ring buffer（用户态程序读取） */
    bpf_ringbuf_output(&events, buf, n, 0);
    return 0;
}

/*=============================================================================
 *  License
 *  GPL 许可证（与内核兼容）
 *============================================================================*/

char LICENSE[] SEC("license") = "GPL";