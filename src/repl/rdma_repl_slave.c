/**
 * rdma_repl_slave.c — Slave 全量同步（rdma_connect + WRITE_WITH_IMM 接收）
 *
 * Slave 主动 connect Master，注册 N 个目标环形缓冲，
 * 边收边通过 RDB stream 解析器加载（与 sendfile 路径统一格式）。
 *
 * P1: 环形缓冲池 (≤8) 提升流水线深度；dict_reserve + dict_arena_init
 *     预分配消除 60K+ 次 rehash + malloc。
 */

#define _GNU_SOURCE
#include "rdma_repl.h"
#include "rdma_transport.h"
#include "kvstore.h"
#include "kv_dict.h"
#include "persistence.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

#define SLAVE_NUM_BUFS  8    /* ring buffer count */

static void die(const char *msg) { perror(msg); exit(1); }

/* ── 用 RDB stream parser 加载一批数据 ──
 *
 * is_first: 首批携带 batch_hdr + RDB header + entries
 *           后续批次只携带 entries
 * 返回本批成功加载的 entry 数（-1 表示致命错误）                        */

static int load_rdb_batch(const char *data, uint32_t data_len,
                          int is_first, int *stream_began) {
    if (is_first) {
        const struct batch_hdr *bh = (const struct batch_hdr *)data;
        if (ntohl(bh->magic) != FULLSYNC_MAGIC) {
            fprintf(stderr, "[slave] bad magic 0x%x\n", ntohl(bh->magic));
            return -1;
        }
        kvs_flush_all();   /* clears dict + frees old arena */
        data     += sizeof(*bh);
        data_len -= sizeof(*bh);

        /* Parse RDB header (16 bytes), get expected key count */
        int expected = rdb_load_stream_begin(data,
                                             data_len >= 16 ? 16 : data_len);
        if (expected < 0) return -1;
        *stream_began = 1;

        /* P1: pre-allocate dict to avoid progressive rehash + malloc
         * during bulk insert.  ht[0] is sized for 'expected' keys;
         * the arena carves entries from a contiguous block.            */
        if (expected > 0) {
            dict_reserve(&g_dict, (uint32_t)expected);
            /* ~80 bytes per entry: header(32) + avg key(12) + payload(36) */
            dict_arena_init(&g_dict, (size_t)expected * 80);
        }

        /* 首批中 RDB header 之后的 entry 数据 */
        if (data_len > 16) {
            int n = rdb_load_stream_append(data + 16, data_len - 16);
            return (n >= 0) ? n : -1;
        }
        return 0;
    } else {
        if (!*stream_began) {
            fprintf(stderr, "[slave] entry batch before header\n");
            return -1;
        }
        if (data_len == 0) return 0;
        int n = rdb_load_stream_append(data, data_len);
        return (n >= 0) ? n : -1;
    }
}

/* ═════════════════════════════════════════════════════════════════════ */

int rdma_repl_slave_fullsync(void) {
    setlinebuf(stdout); setlinebuf(stderr);
    const char *ip = kvs_config.rdma_master_ip;
    char port[16]; snprintf(port, sizeof(port), "%d", kvs_config.rdma_master_port);

    /* ── create ep + 注册环形目标缓冲（在 connect 之前）── */
    struct rdma_addrinfo hints = {0}, *res;
    hints.ai_port_space = RDMA_PS_TCP;
    if (rdma_getaddrinfo(ip, port, &hints, &res)) die("rdma_getaddrinfo");

    struct ibv_qp_init_attr qa = {0};
    qa.cap.max_send_wr  = FULLSYNC_QP_SEND_WR;
    qa.cap.max_recv_wr  = FULLSYNC_QP_RECV_WR;
    qa.cap.max_send_sge = 1;
    qa.cap.max_recv_sge = 1;
    qa.sq_sig_all       = 0;   /* 选择性信号: transport 控制 CQE 生成 */

    struct rdma_cm_id *id;
    if (rdma_create_ep(&id, res, NULL, &qa)) die("rdma_create_ep");
    rdma_freeaddrinfo(res);

    /* ── 创建传输层（包装已有 QP）── */
    rdma_xport_t *xport = rdma_xport_create(id);
    if (!xport) die("rdma_xport_create");

    /* Allocate N ring buffers */
    char *bufs[SLAVE_NUM_BUFS]      = {0};
    struct ibv_mr *mr[SLAVE_NUM_BUFS] = {0};
    for (int i = 0; i < SLAVE_NUM_BUFS; i++) {
        bufs[i] = malloc(FULLSYNC_BUF_SIZE);
        if (!bufs[i]) die("malloc");
        memset(bufs[i], 0, FULLSYNC_BUF_SIZE);
        mr[i] = rdma_reg_write(id, bufs[i], FULLSYNC_BUF_SIZE);
        if (!mr[i]) die("rdma_reg_write");
    }

    /* Build buf_info to send to master */
    struct buf_info binfo;
    memset(&binfo, 0, sizeof(binfo));
    binfo.buf_size = FULLSYNC_BUF_SIZE;
    binfo.num_bufs = SLAVE_NUM_BUFS;
    for (int i = 0; i < SLAVE_NUM_BUFS; i++) {
        binfo.addr[i] = (uintptr_t)bufs[i];
        binfo.rkey[i] = mr[i]->rkey;
    }
    struct ibv_mr *binfo_mr = rdma_reg_msgs(id, &binfo, sizeof(binfo));
    if (!binfo_mr) die("rdma_reg_msgs(binfo)");

    /* ── 预投 dummy recv × N 用于捕获 IMM ── */
    char *dummy = calloc(SLAVE_NUM_BUFS, 1);
    if (!dummy) die("calloc(dummy)");
    struct ibv_mr *dummy_mr[SLAVE_NUM_BUFS];
    for (int i = 0; i < SLAVE_NUM_BUFS; i++) {
        dummy_mr[i] = rdma_reg_msgs(id, &dummy[i], 1);
        if (!dummy_mr[i]) die("rdma_reg_msgs(dummy)");
        struct ibv_sge sge = {
            .addr   = (uintptr_t)&dummy[i],
            .length = 1,
            .lkey   = dummy_mr[i]->lkey
        };
        struct ibv_recv_wr wr = {0}, *bad = NULL;
        wr.wr_id   = i;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        if (ibv_post_recv(id->qp, &wr, &bad)) die("ibv_post_recv");
    }

    /* ACK buffer */
    struct ack_msg ack_buf;
    struct ibv_mr *ack_mr = rdma_reg_msgs(id, &ack_buf, sizeof(ack_buf));
    if (!ack_mr) die("rdma_reg_msgs(ack)");

    /* ── connect + 发目标 MR ── */
    uint64_t base_seq = 0;
    struct rdma_conn_param cp = {
        .private_data     = &base_seq,
        .private_data_len = 8
    };
    if (rdma_connect(id, &cp)) die("rdma_connect");
    rdma_post_send(id, NULL, &binfo, sizeof(binfo), binfo_mr, IBV_SEND_SIGNALED);
    if (rdma_xport_poll_send(xport) < 0) die("poll_send(buf_info)");

    /* ── 接收循环（环形缓冲）── */
    int buf_cur = 0, is_first = 1, stream_began = 0, total_e = 0;
    int nr_b = 0, done = 0;
    size_t total_bytes = 0;

    printf("[slave] xfer-begin (ring=%d)\n", SLAVE_NUM_BUFS);
    struct timespec t1, t2;
    clock_gettime(CLOCK_MONOTONIC, &t1);

    while (!done) {
        /* 1. 等待 RDMA WRITE (IMM) */
        struct ibv_wc wc;
        if (rdma_xport_poll_recv(xport, &wc) < 0) {
            fprintf(stderr, "[slave] recv CQ error\n");
            break;
        }

        uint32_t imm = ntohl(wc.imm_data);
        if (imm == FULLSYNC_IMM_DONE) done = 1;

        int cur       = buf_cur;
        buf_cur       = (buf_cur + 1) % SLAVE_NUM_BUFS;  /* advance ring */
        uint32_t data_len = ntohl(*(uint32_t *)bufs[cur]);

        /* 2. 处理数据 */
        int ne = load_rdb_batch(bufs[cur] + 4, data_len,
                                is_first, &stream_began);
        if (ne < 0) break;
        total_e     += ne;
        total_bytes += data_len + 4;
        nr_b++;
        is_first     = 0;

        /* 3. ACK（处理完成后才释放 buffer） */
        ack_buf.buf_idx = (uint32_t)cur;
        ack_buf.entries = (uint32_t)(ne >= 0 ? ne : 0);
        rdma_post_send(id, NULL, &ack_buf, sizeof(ack_buf), ack_mr, IBV_SEND_SIGNALED);
        if (rdma_xport_poll_send(xport) < 0) die("poll_send(ack)");

        /* 4. Re-post recv for this dummy slot */
        struct ibv_sge sge = {
            .addr   = (uintptr_t)&dummy[cur],
            .length = 1,
            .lkey   = dummy_mr[cur]->lkey
        };
        struct ibv_recv_wr wr = {0}, *bad = NULL;
        wr.wr_id   = (uint64_t)cur;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        ibv_post_recv(id->qp, &wr, &bad);
    }

    clock_gettime(CLOCK_MONOTONIC, &t2);

    /* ── 重建编码（RDB stream load 后所有 entry 是 RAW）── */
    rdb_rebuild_all_encodings();

    double ms = (t2.tv_sec - t1.tv_sec) * 1000.0
              + (t2.tv_nsec - t1.tv_nsec) / 1e6;
    double mbps = (ms > 0)
                ? (total_bytes / (ms / 1000.0) / (1024.0 * 1024.0))
                : 0;
    printf("[slave] xfer-end: entries=%d bytes=%zu ms=%.1f mbps=%.2f batches=%d\n",
           total_e, total_bytes, ms, mbps, nr_b);

    /* Cleanup */
    rdma_dereg_mr(ack_mr);
    rdma_dereg_mr(binfo_mr);
    rdma_xport_destroy(xport);
    for (int i = 0; i < SLAVE_NUM_BUFS; i++) rdma_dereg_mr(dummy_mr[i]);
    free(dummy);
    for (int i = 0; i < SLAVE_NUM_BUFS; i++) {
        rdma_dereg_mr(mr[i]);
        free(bufs[i]);
    }
    rdma_disconnect(id);
    rdma_destroy_ep(id);
    return 0;
}
