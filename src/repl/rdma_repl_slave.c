/**
 * rdma_repl_slave.c — Slave 全量同步（rdma_connect + WRITE_WITH_IMM 接收）
 *
 * Slave 主动 connect Master，注册目标双缓冲，边收边解析插入。
 */

#define _GNU_SOURCE
#include "rdma_repl.h"
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

static void die(const char *msg) { perror(msg); exit(1); }

static inline void poll_cq(struct ibv_cq *cq) {
    struct ibv_wc wc; int n;
    do { n = ibv_poll_cq(cq, 1, &wc); } while (n == 0);
    if (n < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "[slave] CQ err n=%d status=%d\n", n, wc.status);
        exit(1);
    }
}

static int parse_and_insert(const char *buf, size_t len, int is_first, int *arena_done) {
    const char *p=buf, *end=buf+len;
    int n=0;

    if (is_first) {
        const struct batch_hdr *bh = (const struct batch_hdr *)p;
        if (ntohl(bh->magic) != FULLSYNC_MAGIC) {
            fprintf(stderr, "[slave] bad magic 0x%x\n", ntohl(bh->magic));
            return -1;
        }
        kvs_flush_all();
        p += sizeof(*bh);
    }

    while (p+24 <= end) {
        uint64_t hash = be64toh(*(const uint64_t *)p);  p+=8;
        uint32_t klen = ntohl(*(const uint32_t *)p);    p+=4;
        if (p+klen > end) break;
        const char *key = p; p+=klen;
        if (p+4 > end) break;
        uint32_t dlen = ntohl(*(const uint32_t *)p);    p+=4;
        if (p+dlen+8 > end) break;
        const char *dat = p; p+=dlen;
        uint64_t exp = be64toh(*(const uint64_t *)p);   p+=8;
        kv_entry_t *e = dict_add(&g_dict, key, klen, dat, dlen, hash);
        if (e) e->expire = (long long)exp;
        n++;
    }
    if (!*arena_done && n>0) {
        kv_dict_t *dict = &g_dict;
        if (!dict->arena) {
            size_t est = (size_t)n*320;
            if (est > 64*1024*1024) dict_arena_init(dict, est);
        }
        *arena_done = 1;
    }
    return n;
}

int rdma_repl_slave_fullsync(void) {
    setlinebuf(stdout); setlinebuf(stderr);
    const char *ip = kvs_config.rdma_master_ip;
    char port[16]; snprintf(port, sizeof(port), "%d", kvs_config.rdma_master_port);

    /* create ep + 注册目标双缓冲（在 connect 之前） */
    struct rdma_addrinfo hints={0}, *res;
    hints.ai_port_space=RDMA_PS_TCP;
    if (rdma_getaddrinfo(ip, port, &hints, &res)) die("rdma_getaddrinfo");
    struct ibv_qp_init_attr qa={0};
    qa.cap.max_send_wr=32; qa.cap.max_recv_wr=8;
    qa.cap.max_send_sge=1; qa.cap.max_recv_sge=1; qa.sq_sig_all=1;
    struct rdma_cm_id *id;
    if (rdma_create_ep(&id, res, NULL, &qa)) die("rdma_create_ep");
    rdma_freeaddrinfo(res);

    char *bufs[2]={0}; struct ibv_mr *mr[2]={0};
    for (int i=0; i<2; i++) {
        bufs[i]=malloc(FULLSYNC_BUF_SIZE); if(!bufs[i]) die("malloc");
        memset(bufs[i], 0, FULLSYNC_BUF_SIZE);
        mr[i]=rdma_reg_write(id, bufs[i], FULLSYNC_BUF_SIZE); if(!mr[i]) die("rdma_reg_write");
    }
    struct buf_info binfo; binfo.buf_size=FULLSYNC_BUF_SIZE;
    for (int i=0; i<2; i++) { binfo.addr[i]=(uintptr_t)bufs[i]; binfo.rkey[i]=mr[i]->rkey; }
    struct ibv_mr *binfo_mr = rdma_reg_msgs(id, &binfo, sizeof(binfo));
    if (!binfo_mr) die("rdma_reg_msgs(binfo)");

    /* 预投 dummy recv ×2 用于捕获 IMM */
    char dummy[2]; struct ibv_mr *dummy_mr[2];
    for (int i=0; i<2; i++) {
        dummy_mr[i]=rdma_reg_msgs(id, &dummy[i], 1); if(!dummy_mr[i]) die("rdma_reg_msgs(dummy)");
        struct ibv_sge sge={.addr=(uintptr_t)&dummy[i],.length=1,.lkey=dummy_mr[i]->lkey};
        struct ibv_recv_wr wr={0}, *bad=NULL;
        wr.wr_id=i; wr.sg_list=&sge; wr.num_sge=1;
        if (ibv_post_recv(id->qp, &wr, &bad)) die("ibv_post_recv");
    }
    struct ack_msg ack_buf;
    struct ibv_mr *ack_mr = rdma_reg_msgs(id, &ack_buf, sizeof(ack_buf));
    if (!ack_mr) die("rdma_reg_msgs(ack)");

    /* connect + 发目标 MR */
    uint64_t base_seq=0;
    struct rdma_conn_param cp={.private_data=&base_seq, .private_data_len=8};
    if (rdma_connect(id, &cp)) die("rdma_connect");
    rdma_post_send(id, NULL, &binfo, sizeof(binfo), binfo_mr, 0);
    poll_cq(id->send_cq);

    /* 接收循环 */
    int buf_cur=0, is_first=1, arena_done=0, total_e=0, nr_b=0, done=0;
    size_t total_bytes=0;
    printf("[slave] xfer-begin\n");
    struct timespec t1,t2; clock_gettime(CLOCK_MONOTONIC, &t1);

    while (!done) {
        struct ibv_wc wc; int n;
        do { n=ibv_poll_cq(id->recv_cq, 1, &wc); } while (n==0);
        if (n<0 || wc.status!=IBV_WC_SUCCESS) {
            fprintf(stderr, "[slave] recv CQ n=%d status=%d\n", n, wc.status); break;
        }
        uint32_t imm=ntohl(wc.imm_data);
        if (imm==FULLSYNC_IMM_DONE) done=1;

        uint32_t data_len=ntohl(*(uint32_t *)bufs[buf_cur]);
        int ne=parse_and_insert(bufs[buf_cur]+4, data_len, is_first, &arena_done);
        if (ne<0) break;
        total_e+=ne; total_bytes+=data_len+4; nr_b++; is_first=0;

        /* ACK + re-post recv */
        ack_buf.buf_idx=buf_cur; ack_buf.entries=(uint32_t)ne;
        rdma_post_send(id, NULL, &ack_buf, sizeof(ack_buf), ack_mr, 0);
        poll_cq(id->send_cq);
        struct ibv_sge sge={.addr=(uintptr_t)&dummy[buf_cur],.length=1,.lkey=dummy_mr[buf_cur]->lkey};
        struct ibv_recv_wr wr={0}, *bad=NULL;
        wr.wr_id=buf_cur; wr.sg_list=&sge; wr.num_sge=1;
        ibv_post_recv(id->qp, &wr, &bad);
        buf_cur^=1;
    }
    clock_gettime(CLOCK_MONOTONIC, &t2);
    rdb_rebuild_all_encodings();

    double ms=(t2.tv_sec-t1.tv_sec)*1000.0+(t2.tv_nsec-t1.tv_nsec)/1e6;
    double mbps=(ms>0)?(total_bytes/(ms/1000.0)/(1024.0*1024.0)):0;
    printf("[slave] xfer-end: entries=%d bytes=%zu ms=%.1f mbps=%.2f batches=%d\n",
           total_e, total_bytes, ms, mbps, nr_b);

    rdma_dereg_mr(ack_mr); rdma_dereg_mr(binfo_mr);
    for (int i=0; i<2; i++) rdma_dereg_mr(dummy_mr[i]);
    for (int i=0; i<2; i++) { rdma_dereg_mr(mr[i]); free(bufs[i]); }
    rdma_disconnect(id); rdma_destroy_ep(id);
    return 0;
}
