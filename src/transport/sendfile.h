/*=============================================================================
 *  sendfile.h — sendfile（TCP 零拷贝）传输方式
 *  
 *  功能：基于 Linux sendfile 系统调用的全量同步传输实现
 *  
 *  设计原则：
 *  1. 零拷贝：数据从内核直接发送到 socket，绕过用户态
 *  2. 服务端/客户端都在本文件实现（sendfile.c）
 *  3. 由 repl_fullsync.c 按配置开关直接调用
 *  
 *  适用场景：本地网络或低延迟要求不高的环境
 *  优势：实现简单，兼容性好，无需特殊硬件
 *============================================================================*/

#ifndef __SENDFILE_H__
#define __SENDFILE_H__

#include <stdint.h>

/*=============================================================================
 *  服务端入口
 *  
 *  功能：
 *  1. 在 fullsync_port 上监听 TCP 连接
 *  2. 接受客户端连接
 *  3. 将 token（快照 offset）作为流头发送
 *  4. 通过 sendfile 零拷贝发送 RDB 快照数据
 *  
 *  @param token 上层会话元数据（全量同步中 = 快照 offset）
 *  @param ready_fd 就绪管道 fd：listen 成功后写 1 字节通知父进程
 *  @return 0 成功，-1 失败
 *  
 *  调用者：repl_fullsync_master_trigger 中 fork 的子进程
 *  
 *  实现要点：
 *  - 使用 sendfile 避免用户态到内核态的数据拷贝
 *  - 支持大文件分块传输
 *  - 连接建立后立即发送 token，再发数据
 */
int sendfile_server_run(uint64_t token, int ready_fd);

/*=============================================================================
 *  客户端入口
 *  
 *  功能：
 *  1. 连接服务端的 fullsync_port
 *  2. 接收流头 token（快照 offset）
 *  3. 接收 RDB 快照数据并加载
 *  
 *  @param out_token 输出：从流头解出的 token（快照 offset）
 *  @return 0 成功，-1 失败
 *  
 *  调用者：repl_fullsync_slave_start
 *  
 *  实现要点：
 *  - 先接收 8 字节 token，再接收数据流
 *  - 数据通过 RDB 流式加载器（rdb_load_stream_begin/append）
 *  - 支持断线重连
 */
int sendfile_client_run(uint64_t *out_token);

#endif /* __SENDFILE_H__ */