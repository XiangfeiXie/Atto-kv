#ifndef __RDMA_REPL_H__
#define __RDMA_REPL_H__

#include <stdint.h>
#include <sys/types.h>

/* =========================================================================
 * RDMA 全量同步 — librdmacm + libibverbs (RoCEv2 / Soft-RoCE)
 *
 *   1. Slave TCP "FULLSYNC" → Master fork → rdma_listen
 *   2. Slave rdma_connect → 发目标 MR → Master RDMA_WRITE_WITH_IMM
 *   3. Slave 边收边解析 → ACK → rebuild_encodings
 * ========================================================================= */

/* ── 共享常量 ──────────────────────────────────────────────────────── */

#define FULLSYNC_BUF_SIZE   (256ULL * 1024 * 1024)
#define FULLSYNC_MAGIC      0x52444233u
#define FULLSYNC_VERSION    3
#define FULLSYNC_IMM_DONE   0xFFFFFFFFu
#define FULLSYNC_NUM_BUFS   2

/* ── 握手 & ACK 结构 ──────────────────────────────────────────────── */

struct buf_info {
    uint64_t addr[2];
    uint32_t rkey[2];
    uint32_t buf_size;
} __attribute__((packed));

struct batch_hdr {
    uint32_t magic;
    uint16_t version;
    uint16_t flags;
    uint64_t base_seq;
} __attribute__((packed));

struct ack_msg {
    uint32_t buf_idx;
    uint32_t entries;
} __attribute__((packed));

/* ── 公共 API ─────────────────────────────────────────────────────── */

int  rdma_repl_master_start(void);
int  rdma_repl_master_trigger_sync(void);
int  rdma_repl_slave_start(void);
void session_add(pid_t pid);
void session_kill_all(void);
int  master_fullsync_child(uint64_t base_seq, int ready_fd);
int  rdma_repl_slave_fullsync(void);

#endif
