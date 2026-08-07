/**
 * rdma_repl_master.c — Master 全量同步（rdma_listen + RDMA_WRITE 响应推送）
 *
 * 收到 Slave 的 RDMA 连接后：收目标 MR → 遍历 dict → 序列化 → WRITE_WITH_IMM
 */

#define _GNU_SOURCE
#include "rdma_repl.h"
#include "kvstore.h"
#include "kv_dict.h"
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
        fprintf(stderr, "[master] CQ err n=%d status=%d\n", n, wc.status);
        exit(1);
    }
}

static inline size_t serialize_one(char *dst, kv_entry_t *e) {
    char *p = dst;
    uint64_t hb = htobe64(e->hash);
    uint32_t kl = htonl(e->key_len), dl = htonl(e->val_len);
    uint64_t xb = htobe64((uint64_t)e->expire);
    memcpy(p, &hb, 8); p += 8;
    memcpy(p, &kl, 4); p += 4;
    memcpy(p, entry_key(e), e->key_len); p += e->key_len;
    memcpy(p, &dl, 4); p += 4;
    memcpy(p, e->data, e->val_len); p += e->val_len;
    memcpy(p, &xb, 8); p += 8;
    return (size_t)(p - dst);
}

static void post_write(struct rdma_cm_id *id, struct ibv_mr *local_mr,
                       void *buf, uint32_t len, uint32_t imm,
                       uint64_t raddr, uint32_t rkey) {
    struct ibv_sge sge = { .addr=(uintptr_t)buf, .length=len, .lkey=local_mr->lkey };
    struct ibv_send_wr wr = {0}, *bad = NULL;
    wr.sg_list=&sge; wr.num_sge=1;
    wr.opcode=IBV_WR_RDMA_WRITE_WITH_IMM; wr.send_flags=IBV_SEND_SIGNALED;
    wr.imm_data=htonl(imm);
    wr.wr.rdma.remote_addr=raddr; wr.wr.rdma.rkey=rkey;
    if (ibv_post_send(id->qp, &wr, &bad)) die("ibv_post_send");
    poll_cq(id->send_cq);
}

int master_fullsync_child(uint64_t base_seq, int ready_fd) {
    setlinebuf(stdout); setlinebuf(stderr);
    const char *ip = kvs_config.rdma_listen_ip;
    if (!strcmp(ip, "0.0.0.0")) ip = NULL;
    char port[16]; snprintf(port, sizeof(port), "%d", kvs_config.rdma_listen_port);

    /* listener */
    struct rdma_addrinfo hints={0}, *res;
    hints.ai_flags=RAI_PASSIVE; hints.ai_port_space=RDMA_PS_TCP;
    if (rdma_getaddrinfo(ip, port, &hints, &res)) die("rdma_getaddrinfo");
    struct ibv_qp_init_attr qa={0};
    qa.cap.max_send_wr=32; qa.cap.max_recv_wr=8;
    qa.cap.max_send_sge=1; qa.cap.max_recv_sge=1; qa.sq_sig_all=1;
    struct rdma_cm_id *listen_id;
    if (rdma_create_ep(&listen_id, res, NULL, &qa)) die("rdma_create_ep");
    rdma_freeaddrinfo(res);
    if (rdma_listen(listen_id, 1)) die("rdma_listen");
    if (ready_fd >= 0) { char r=1; (void)write(ready_fd, &r, 1); close(ready_fd); }

    /* accept */
    struct rdma_cm_id *id;
    if (rdma_get_request(listen_id, &id)) die("rdma_get_request");
    printf("[master] slave connected\n");

    /* 注册 buffer + 预贴 recv（在 accept 之前） */
    char *buf = malloc(FULLSYNC_BUF_SIZE);
    if (!buf) die("malloc");
    struct ibv_mr *buf_mr = rdma_reg_msgs(id, buf, FULLSYNC_BUF_SIZE);
    if (!buf_mr) die("rdma_reg_msgs");
    struct buf_info binfo;
    struct ibv_mr *binfo_mr = rdma_reg_msgs(id, &binfo, sizeof(binfo));
    if (!binfo_mr) die("rdma_reg_msgs(binfo)");
    rdma_post_recv(id, NULL, &binfo, sizeof(binfo), binfo_mr);
    if (rdma_accept(id, NULL)) die("rdma_accept");
    poll_cq(id->recv_cq);

    /* ACK 接收槽 */
    struct ack_msg acks[4];
    struct ibv_mr *ack_mr = rdma_reg_msgs(id, acks, sizeof(acks));
    if (!ack_mr) die("rdma_reg_msgs(ack)");
    for (int i=0; i<4; i++) rdma_post_recv(id, NULL, &acks[i], sizeof(acks[i]), ack_mr);
    int ack_inflight=0;

    /* batch_header + 序列化 + 推送 */
    struct batch_hdr bh = { .magic=htonl(FULLSYNC_MAGIC),
                            .version=htons(FULLSYNC_VERSION),
                            .flags=0, .base_seq=htobe64(base_seq) };
    memcpy(buf+4, &bh, sizeof(bh));
    size_t off=4+sizeof(bh), total_bytes=0;
    int cur=0, nr_batch=0;

    printf("[master] xfer-begin\n");
    struct timespec t1, t2; clock_gettime(CLOCK_MONOTONIC, &t1);
    kv_store_iter_t it; kv_store_iter_init(&it); kv_entry_t *e;
    while ((e = kv_store_iter_next(&it)) != NULL) {
        size_t need = 8+4+e->key_len+4+e->val_len+8;
        if (off + need > FULLSYNC_BUF_SIZE) {
            if (ack_inflight >= 4) { poll_cq(id->recv_cq); ack_inflight--; }
            *(uint32_t *)buf = htonl((uint32_t)(off-4));
            post_write(id, buf_mr, buf, (uint32_t)off, 0,
                       binfo.addr[cur], binfo.rkey[cur]);
            total_bytes+=off; ack_inflight++; cur^=1; off=4; nr_batch++;
        }
        off += serialize_one(buf+off, e);
    }
    kv_store_iter_free(&it);

    /* final batch */
    if (ack_inflight >= 4) { poll_cq(id->recv_cq); ack_inflight--; }
    *(uint32_t *)buf = htonl((uint32_t)(off-4));
    post_write(id, buf_mr, buf, (uint32_t)(off>4?off:5),
               FULLSYNC_IMM_DONE, binfo.addr[cur], binfo.rkey[cur]);
    total_bytes+=(off>4?off:5); ack_inflight++; nr_batch++;
    while (ack_inflight>0) { poll_cq(id->recv_cq); ack_inflight--; }
    clock_gettime(CLOCK_MONOTONIC, &t2);

    double ms=(t2.tv_sec-t1.tv_sec)*1000.0+(t2.tv_nsec-t1.tv_nsec)/1e6;
    double mbps=(ms>0)?(total_bytes/(ms/1000.0)/(1024.0*1024.0)):0;
    printf("[master] xfer-end: bytes=%zu ms=%.1f mbps=%.2f batches=%d\n",
           total_bytes, ms, mbps, nr_batch);

    rdma_dereg_mr(ack_mr); rdma_dereg_mr(binfo_mr);
    rdma_dereg_mr(buf_mr); free(buf);
    rdma_disconnect(id); rdma_destroy_ep(id); rdma_destroy_ep(listen_id);
    return 0;
}
