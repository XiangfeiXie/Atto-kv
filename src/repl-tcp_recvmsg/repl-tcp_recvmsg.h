// SPDX-License-Identifier: GPL-2.0
/*=============================================================================
 *  repl-tcp_recvmsg.h — kprobe 镜像组件的用户态配置
 *
 *  挂载点：tcp_recvmsg_locked（kprobe/kretprobe），BPF 按 tgid 过滤 master
 *  进程（argv 传 PID）。组件把 master 进程读到的命令字节转发到从机。
 *
 *  演示全程 127.0.0.1（无需 netns/veth）：
 *    master 127.0.0.1:2000 ← fill 客户端写入
 *    本组件把 master 读到的字节转发到从机 127.0.0.1:2001
 *
 *  换从机地址/端口改这里即可。注意：字符串宏 SLAVE_IP 经 CMake -D 会粘参
 *  （与 repl-xdp 同坑），覆盖须改本默认值。
 *============================================================================*/

#ifndef KVS_REPL_TCP_RECVMSG_H
#define KVS_REPL_TCP_RECVMSG_H

#define SLAVE_IP     "127.0.0.1"   /* 从机 IP（演示与 master 同机） */
#define SLAVE_PORT   2001          /* 从机 kvstore 端口 */

#endif /* KVS_REPL_TCP_RECVMSG_H */
