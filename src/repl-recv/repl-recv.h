/*=============================================================================
 *  repl-recv — 独立进程，与 kvstore 零耦合
 *  
 *  功能：通过 eBPF uprobe 拦截 kvstore master 进程的 recv 系统调用
 *  将收到的字节镜像到 BPF ring buffer，然后 TCP 转发到从机
 *  
 *  设计原则：
 *  1. 零耦合：独立进程，不依赖 kvstore 代码
 *  2. 动态挂载：master PID 由命令行参数传入
 *  3. 按需编译：配置通过宏定义，修改后重新编译
 *  
 *  适用引擎：
 *  - Reactor (epoll)：recv 经过 libc recv，可挂载
 *  - Proactor (io_uring)：recv 不经 libc recv，不适用
 *  
 *  同步方式：
 *  从机把这条链路当普通客户端连接
 *  收到的 RESP 写命令直接落库，即增量同步
 *============================================================================*/

#ifndef REPL_RECV_H
#define REPL_RECV_H

/*=============================================================================
 *  挂载配置
 *============================================================================*/

/**
 * libc 库路径
 * 
 * 用于 uprobe 挂载 libc 的 recv 符号
 * 
 * 不同系统的路径可能不同：
 * - Ubuntu/Debian：/usr/lib/x86_64-linux-gnu/libc.so.6
 * - CentOS/RHEL：/usr/lib64/libc.so.6
 * 
 * 编译时可通过 -DLIBC_PATH=/path/to/libc.so.6 覆盖
 */
#ifndef LIBC_PATH
#define LIBC_PATH    "/usr/lib/x86_64-linux-gnu/libc.so.6"
#endif

/*=============================================================================
 *  转发目标配置
 *============================================================================*/

/**
 * 从机地址（转发目标）
 * 
 * 从机主服务端口（默认 2000）
 * 从机按普通客户端命令落库
 * 
 * 编译时可通过 -DPEER_IP=192.168.1.100 覆盖
 */
#ifndef PEER_IP
#define PEER_IP      "127.0.0.1"
#endif

/**
 * 从机端口
 * 
 * 默认 2000（与 kvstore 默认端口一致）
 * 
 * 编译时可通过 -DPEER_PORT=2000 覆盖
 */
#ifndef PEER_PORT
#define PEER_PORT    2000
#endif

/*=============================================================================
 *  镜像大小限制
 *============================================================================*/

/**
 * 单次 recv 镜像最大字节数
 * 
 * 限制原因：
 * 1. eBPF verifier 要求 bpf_probe_read_user 的长度有编译期上界
 * 2. 内核 PERCPU_ARRAY map value_size 上限为 32KB
 *    超过加载时 E2BIG
 * 
 * 实际影响：
 * - 正常 RESP 命令流远小于此（通常 < 1KB）
 * - 超过 32KB 的部分会被丢弃
 * 
 * 如需镜像更大 value，需重新设计 eBPF 程序
 * 
 * 编译时可通过 -DMAX_MIRROR=65536 覆盖
 */
#ifndef MAX_MIRROR
#define MAX_MIRROR   (32 * 1024)
#endif

#endif /* REPL_RECV_H */