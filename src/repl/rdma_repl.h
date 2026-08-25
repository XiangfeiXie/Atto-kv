#ifndef __RDMA_REPL_H__
#define __RDMA_REPL_H__

#include <stdint.h>
#include <sys/types.h>

/* RDMA 全量同步 — librdmacm + libibverbs (RoCEv2 / Soft-RoCE)
 *
 *   1. Slave 通过 TCP 发 FULLSYNC → Master fork → rdma_listen
 *   2. Slave rdma_connect → 上报目标 MR → Master RDMA_WRITE_WITH_IMM
 *   3. Slave 边收边解析 RDB 流 → ACK 释放 buffer → 环形缓冲流水线
 */

#define FULLSYNC_BUF_SIZE   (64ULL * 1024 * 1024)   /* 单个环形 buffer */
#define FULLSYNC_MAGIC      0x52444233u
#define FULLSYNC_IMM_DONE   0xFFFFFFFFu

#define FULLSYNC_NUM_BUFS   8     /* slave 环形 buffer 数量 */
#define FULLSYNC_QP_SEND_WR 512
#define FULLSYNC_QP_RECV_WR 16

/* ── 握手 / ACK 结构 ──────────────────────────────────────────────── */

struct buf_info {
    uint64_t addr[FULLSYNC_NUM_BUFS];
    uint32_t rkey[FULLSYNC_NUM_BUFS];
    uint32_t buf_size;
    uint32_t num_bufs;
} __attribute__((packed));

struct batch_hdr {
    uint32_t magic;
    uint64_t repl_offset;
} __attribute__((packed));

struct ack_msg {
    uint32_t buf_idx;
} __attribute__((packed));

/* ── 公共 API ─────────────────────────────────────────────────────── */

int  rdma_repl_master_start(void);
int  rdma_repl_master_trigger_sync(void);
int  rdma_repl_slave_start(void);
void session_add(pid_t pid);
void session_kill_all(void);
int  master_fullsync_child(uint64_t repl_offset, int ready_fd);
int  rdma_repl_slave_fullsync(void);

#endif
