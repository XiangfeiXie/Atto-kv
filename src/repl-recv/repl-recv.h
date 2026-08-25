#ifndef REPL_RECV_H
#define REPL_RECV_H

/* =========================================================================
 * repl-recv — 独立进程，与 kvstore 零耦合
 *
 *   挂 uprobe/uretprobe 到 kvstore master 进程的 libc `recv`，把收到的
 *   字节镜像进 BPF ring buffer，送回用户态后原样 TCP 转发到从机。
 *
 *   改下面四个宏后 `make clean && make` 即可，无需改动其它代码。
 * ========================================================================= */

#define TARGET_PID   12345                            /* kvstore master 的 PID */

#define LIBC_PATH    "/usr/lib/x86_64-linux-gnu/libc.so.6"
#define PEER_IP      "192.168.30.136"                 /* 从机 IP */
#define PEER_PORT    2002                             /* 从机端口 */

/* 单次 recv 超过该字节数的部分会被丢弃。
 * eBPF verifier 要求 bpf_probe_read_user 的长度有编译期上界，故设此常量。
 * 演示用 64KB，正常 RESP 命令流远小于此；若需精确镜像大 value 再讨论。 */
#define MAX_MIRROR   (64 * 1024)

#endif /* REPL_RECV_H */
