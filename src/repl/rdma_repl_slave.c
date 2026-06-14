/* Slave fullsync: FI_EP_MSG batch receiver */

#include "rdma_repl.h"
#include "rdma_ctrl_protocol.h"
#include "kvstore.h"
#include "kv_dict.h"

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_errno.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

/* Wait for a single CQ completion */
static int wait_cq(struct fid_cq *cq) {
    struct fi_cq_data_entry e;
    for (;;) {
        ssize_t r = fi_cq_read(cq, &e, 1);
        if (r > 0) return 0;
        if (r < 0 && r != -FI_EAGAIN) return -1;
    }
}

/* Parse one KV entry from buffer and load into store */
static int parse_entry(const char **p, const char *end) {
    const char *ptr = *p;
    if (ptr + 4 > end) return -1;
    uint32_t klen = ntohl(*(uint32_t *)ptr); ptr += 4;
    if (ptr + klen > end) return -1;
    const char *key = ptr; ptr += klen;
    if (ptr + 4 > end) return -1;
    uint32_t vlen = ntohl(*(uint32_t *)ptr); ptr += 4;
    if (ptr + vlen > end) return -1;
    const char *val = ptr; ptr += vlen;
    if (ptr + 8 > end) return -1;
    uint64_t expire = be64toh(*(uint64_t *)ptr); ptr += 8;

    uint64_t hash = kv_hash((const uint8_t *)key, klen);
    kv_entry_t *e = dict_add(kv_dict_for(hash), key, klen, val, vlen, hash);
    if (e) e->expire = expire;
    *p = ptr;
    return 0;
}

int slave_receive_fullsync_batched(struct fid_domain *domain,
                                   struct fid_ep *ep, struct fid_cq *cq) {
    printf("[Slave] Fullsync receive start\n");
    (void)domain;

    /* Allocate receive buffer */
    char *recv_buf = malloc(REMOTE_BUF_SIZE);
    if (!recv_buf) return -1;

    /* Send HANDSHAKE */
    ctrl_msg_t hs = { .type = CTRL_MSG_HANDSHAKE };
    if (ctrl_msg_send(ep, &hs) < 0) { free(recv_buf); return -1; }
    if (wait_cq(cq) < 0) { free(recv_buf); return -1; }
    printf("[Slave] HANDSHAKE sent\n");

    /* Receive loop */
    int batches = 0, total_entries = 0;
    while (1) {
        /* Receive header */
        ctrl_msg_t msg;
        if (ctrl_msg_recv(ep, cq, &msg) < 0) break;

        if (msg.type == CTRL_MSG_EOF) {
            printf("[Slave] EOF, fullsync complete\n");
            break;
        }
        if (msg.type != CTRL_MSG_BATCH_ACK) break;

        /* Receive data */
        size_t dlen = msg.batch_size;
        if (dlen > REMOTE_BUF_SIZE) break;
        if (fi_recv(ep, recv_buf, dlen, NULL, 0, NULL) < 0) break;
        if (wait_cq(cq) < 0) break;

        /* Verify CRC */
        if (calc_crc32(recv_buf, dlen) != msg.crc32) {
            fprintf(stderr, "[Slave] CRC mismatch\n"); break;
        }

        /* Parse entries */
        const char *p = recv_buf, *end = recv_buf + dlen;
        int n = 0;
        while (p < end) { if (parse_entry(&p, end) < 0) break; n++; }
        total_entries += n;
        batches++;
        printf("[Slave] Batch %u: %d entries, %zu bytes\n", msg.batch_id, n, dlen);

        /* Send ACK */
        ctrl_msg_t ack = { .type = CTRL_MSG_BATCH_ACK, .batch_id = msg.batch_id };
        if (ctrl_msg_send(ep, &ack) < 0) break;
        if (wait_cq(cq) < 0) break;
    }

    printf("[Slave] Total: %d batches, %d entries\n", batches, total_entries);
    free(recv_buf);
    return 0;
}
