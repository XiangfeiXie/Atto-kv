#ifndef __REPL_INCR_H__
#define __REPL_INCR_H__

#include <stddef.h>
#include "dynbuf.h"
#include "net_common.h"    /* conn_kind_t */

/* =========================================================================
 * 主从增量同步
 *
 *  Backlog: 1MB 环形缓冲，存原始 RESP 写命令，供从库断线后按 offset 补发。
 *  协议:    简化 PSYNC（只有 offset，无 replid）。
 *  传输:    独立 TCP 端口（默认 2001），与全量同步分离。
 *
 *  fd 归属: 复制 fd 交给网络引擎（conn->kind + 下面的回调）调度，本模块
 *  只保留协议/状态机逻辑，不自轮询。
 * ========================================================================= */

#define REPL_BACKLOG_SIZE      (1024 * 1024)   /* 1 MB */

/* repl_incr_slave_start() 返回值 */
#define REPL_INCR_CONTINUE     0     /* +CONTINUE，进入流式接收 */
#define REPL_INCR_ERROR       -1     /* 连接/握手失败 */
#define REPL_INCR_FULLRESYNC  -2     /* offset 已过期，需全量同步 */

/* 快照 offset 滚出 backlog 后，重做全量同步的上限（防死循环） */
#define REPL_FULLSYNC_MAX_RETRY 3

/* ── Backlog ─────────────────────────────────────────────────────────── */

typedef struct {
    char   buf[REPL_BACKLOG_SIZE];
    size_t head;            /* 绝对写偏移（单调递增，不重置） */
    size_t used;            /* 当前存量字节数（<= REPL_BACKLOG_SIZE） */
} repl_backlog_t;

void   repl_backlog_init(void);
void   repl_backlog_append(const char *resp_data, int len);

/* 从 start_offset 起拷贝最多 max_len 字节（可能截断命令，由 slave 端重组）。
 * 返回写入字节数，*out_end_offset 为拷贝后的新偏移；start_offset 不在
 * 窗口内返回 -1。 */
int    repl_backlog_fetch(size_t start_offset, char *dst, int max_len,
                          size_t *out_end_offset);

size_t repl_backlog_offset(void);         /* 当前 head（绝对偏移） */
int    repl_backlog_valid(size_t offset); /* offset 是否在窗口内 */

/* ── 从库连接（Master 侧）────────────────────────────────────────────── */

typedef enum {
    REPL_STATE_NONE,        /* 槽位空闲 */
    REPL_STATE_CONNECTED,   /* 已接受，等待 PSYNC */
    REPL_STATE_STREAMING    /* 已发 +CONTINUE，推送命令流 */
} repl_slave_state_t;

/* 仅保存复制状态——socket fd 与其原始 I/O 缓冲在引擎自己的连接结构里。
 * 设计上只有一台从机，故为单实例而非数组。 */
typedef struct {
    int                fd;          /* 引擎连接表的索引键 */
    int                active;
    repl_slave_state_t state;
    DynBuf             write_buf;   /* 待推送给该从库的字节 */
} repl_slave_t;

/* ── 全量同步桥接的全局 offset ─────────────────────────────────────────
 * 全量同步协议在 batch_hdr.repl_offset 里带出快照 offset，slave 加载完
 * RDB 后存到这里，增量模块据此续传。 */
extern size_t g_repl_snapshot_offset;   /* 0 = 尚未全量同步 */

/* ── 引擎集成 hook（引擎启动时设置，repl_incr_attach 前生效）─────────── */
extern int  (*repl_engine_register)(int fd, conn_kind_t kind);
extern void (*repl_engine_writable)(int fd, int on);   /* 开/关 EPOLLOUT */
extern void (*repl_engine_close)(int fd);              /* 释放连接槽 */

/* ── 启动（引擎循环前调用）──────────────────────────────────────────── */

int  repl_incr_master_start(int port);                    /* 建监听 socket */
int  repl_incr_slave_start(const char *master_ip, int master_port);

/* 把 repl fd 注册进引擎事件循环，引擎就绪后调用一次。 */
void repl_incr_attach(void);

/* ── Master 侧回调（fd 已注册）──────────────────────────────────────── */

int  repl_incr_master_accept(int listen_fd);   /* 接受所有待连从库 */
int  repl_incr_master_read(int fd);            /* PSYNC 握手 / 检测断开 */
int  repl_incr_master_write(int fd);           /* 冲刷 write_buf */
void repl_incr_master_close(int fd);           /* 拆除一条从库链路 */

/* 引擎对该 fd 应等待的就绪事件：等待 PSYNC 时 POLLIN，有流式数据待发时
 * POLLOUT，空闲时 0。 */
int  repl_incr_master_poll_mask(int fd);

/* ── Slave 侧回调 ───────────────────────────────────────────────────── */

int  repl_incr_slave_read(int fd);             /* 读命令流并回放 */
void repl_incr_slave_close(int fd);            /* 拆除 master 链路 */

/* 断线重连：引擎主循环周期调用（有连接或已放弃时为 no-op）；全量同步
 * 完成后调用，把续传 offset 对齐到快照 offset。 */
void repl_incr_slave_tick(void);
void repl_incr_slave_reset_offset(void);

/* ── 写命令传播（master 热路径）─────────────────────────────────────── */

void repl_incr_propagate(const char *resp_data, int len);

/* ── fd getter（repl_incr_attach 用）────────────────────────────────── */

int  repl_incr_master_listen_fd(void);
int  repl_incr_slave_link_fd(void);

#endif /* __REPL_INCR_H__ */
