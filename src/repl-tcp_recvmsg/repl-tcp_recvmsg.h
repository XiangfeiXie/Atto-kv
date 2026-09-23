// SPDX-License-Identifier: GPL-2.0
/*=============================================================================
 *  repl-tcp_recvmsg.h — kprobe 镜像组件的用户态配置
 *
 *  挂载点：tcp_recvmsg_locked（kprobe/kretprobe），BPF 按 tgid 过滤 master
 *  进程（靠下面的 MASTER_PORT 反查，见 .c 的自动定位）。组件把 master 进程
 *  读到的命令字节转发到从机。
 *
 *  演示全程 127.0.0.1（无需 netns/veth）：
 *    master 127.0.0.1:2000 ← fill 客户端写入
 *    本组件把 master 读到的字节转发到从机 127.0.0.1:2000
 *
 *  这几个值都写死在这里。注意：字符串宏 SLAVE_IP 经 CMake -D 会粘参
 *  （与 repl-xdp 同坑），覆盖须改本默认值。
 *============================================================================*/

#ifndef KVS_REPL_TCP_RECVMSG_H
#define KVS_REPL_TCP_RECVMSG_H

/* master 的主端口。组件靠它反查 master 进程：先找出监听该端口的 socket inode，
 * 再找持有该 inode 的进程。所以命令行不必再传 conf 或 PID。 */
#define MASTER_PORT  2000

#define SLAVE_IP     "192.168.30.136"   /* 从机 IP（演示与 master 同机） */
#define SLAVE_PORT   2000          /* 从机 kvstore 端口 */

#endif /* KVS_REPL_TCP_RECVMSG_H */
