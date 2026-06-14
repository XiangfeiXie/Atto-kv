/* Master fullsync: FI_EP_MSG batch transfer */

#include "rdma_repl.h"
#include "rdma_ctrl_protocol.h"
#include "kvstore.h"
#include "kv_dict.h"

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_errno.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <arpa/inet.h>

typedef struct {
    struct fid_ep  *ep;
    struct fid_cq  *cq;
    char   *batch_buf;
    size_t  offset;
    uint32_t batch_id;
    uint32_t entries;
    uint64_t bytes;
} master_ctx_t;

static int serialize_entry(master_ctx_t *ctx, const char *key, size_t klen,
                           const char *val, size_t vlen, uint64_t expire)
{
    size_t need = 4 + klen + 4 + vlen + 8;
    if (ctx->offset + need > LOCAL_BATCH_SIZE) return -1;
    char *p = ctx->batch_buf + ctx->offset;
    *(uint32_t *)p = htonl((uint32_t)klen); p += 4;
    memcpy(p, key, klen); p += klen;
    *(uint32_t *)p = htonl((uint32_t)vlen); p += 4;
    memcpy(p, val, vlen); p += vlen;
    *(uint64_t *)p = htobe64(expire);
    ctx->offset += need;
    ctx->entries++;
    ctx->bytes += need;
    return 0;
}

/* Wait for a CQ completion */
static int wait_cq(struct fid_cq *cq) {
    struct fi_cq_data_entry e;
    for (;;) {
        ssize_t r = fi_cq_read(cq, &e, 1);
        if (r > 0) return 0;
        if (r < 0 && r != -FI_EAGAIN) return -1;
    }
}

/* Send batch: header → data → wait ACK */
static int flush_batch(master_ctx_t *ctx) {
    if (ctx->offset == 0) return 0;
    struct fid_ep *ep = ctx->ep;
    struct fid_cq *cq = ctx->cq;

    /* Header */
    ctrl_msg_t hdr = {
        .type = CTRL_MSG_BATCH_ACK, .batch_id = ctx->batch_id,
        .batch_size = ctx->offset,
        .crc32 = calc_crc32(ctx->batch_buf, ctx->offset)
    };
    if (ctrl_msg_send(ep, &hdr) < 0) return -1;
    if (wait_cq(cq) < 0) return -1;

    /* Data */
    if (fi_send(ep, ctx->batch_buf, ctx->offset, NULL, 0, NULL) < 0) return -1;
    if (wait_cq(cq) < 0) return -1;

    /* Wait ACK */
    ctrl_msg_t ack;
    if (ctrl_msg_recv(ep, cq, &ack) < 0) return -1;
    if (ack.type != CTRL_MSG_BATCH_ACK || ack.batch_id != ctx->batch_id) return -1;

    printf("[Master] Batch %u: %zu bytes\n", ctx->batch_id, ctx->offset);
    ctx->offset = 0;
    ctx->batch_id++;
    return 0;
}

/* Send EOF */
static int send_eof(struct fid_ep *ep, struct fid_cq *cq) {
    ctrl_msg_t m = { .type = CTRL_MSG_EOF };
    if (ctrl_msg_send(ep, &m) < 0) return -1;
    return wait_cq(cq);
}

int master_fullsync_planB(struct fid_domain *domain, struct fid_ep *ep,
                          struct fid_cq *cq) {
    printf("[Master] Fullsync start\n");
    master_ctx_t ctx = { .ep = ep, .cq = cq, .batch_id = 1 };
    ctx.batch_buf = malloc(LOCAL_BATCH_SIZE);
    if (!ctx.batch_buf) return -1;

    /* Receive HANDSHAKE */
    printf("[Master] Waiting HANDSHAKE...\n");
    ctrl_msg_t hs;
    if (ctrl_msg_recv(ep, cq, &hs) < 0 || hs.type != CTRL_MSG_HANDSHAKE) {
        fprintf(stderr, "[Master] HANDSHAKE failed\n");
        goto fail;
    }
    printf("[Master] HANDSHAKE ok\n");

    /* Time the transfer */
    struct timespec t1, t2;
    clock_gettime(CLOCK_MONOTONIC, &t1);

    /* Iterate KV store */
    kv_store_iter_t it;
    kv_store_iter_init(&it);
    kv_entry_t *e;
    while ((e = kv_store_iter_next(&it)) != NULL) {
        const char *key = entry_key(e);
        const char *val = entry_type(e) == KV_TYPE_STRING
                          ? entry_payload(e) : e->data;
        size_t vlen = entry_type(e) == KV_TYPE_STRING
                      ? entry_payload_len(e) : e->val_len;
        if (serialize_entry(&ctx, key, e->key_len, val, vlen, e->expire) < 0) {
            if (flush_batch(&ctx) < 0) { kv_store_iter_free(&it); goto fail; }
            serialize_entry(&ctx, key, e->key_len, val, vlen, e->expire);
        }
    }
    kv_store_iter_free(&it);

    if (flush_batch(&ctx) < 0) goto fail;
    if (send_eof(ep, cq) < 0) goto fail;

    clock_gettime(CLOCK_MONOTONIC, &t2);
    double ms = (t2.tv_sec - t1.tv_sec) * 1000.0 + (t2.tv_nsec - t1.tv_nsec) / 1e6;
    double mbps = ms > 0 ? ctx.bytes / (ms / 1000.0) / (1024.0 * 1024.0) : 0;
    printf("[Master] Done: %u entries, %lu bytes, %u batches, %.1f ms, %.2f MB/s\n",
           ctx.entries, ctx.bytes, ctx.batch_id - 1, ms, mbps);

    free(ctx.batch_buf);
    return 0;

fail:
    free(ctx.batch_buf);
    return -1;
}
