/* af_xdp_repl.c — AF_XDP socket manager implementation
 *
 * Zero-copy packet transmission via AF_XDP for WAL replication.
 * Integrates with XDP program (wal_xdp.kern.c) for multicast forwarding.
 */

#define _GNU_SOURCE
#include "af_xdp_repl.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <linux/if_link.h>
#include <net/if.h>

#include <xdp/xsk.h>
#include <xdp/libxdp.h>
#include <bpf/libbpf.h>

/* ── Frame management helpers ── */

static int frame_stack_init(af_xdp_ctx_t *ctx)
{
    ctx->free_frames = (uint64_t *)malloc(AF_XDP_NUM_FRAMES * sizeof(uint64_t));
    if (!ctx->free_frames) {
        fprintf(stderr, "[AF_XDP] OOM for frame stack\n");
        return -1;
    }
    
    for (uint32_t i = 0; i < AF_XDP_NUM_FRAMES; i++) {
        ctx->free_frames[i] = i * ctx->frame_size;
    }
    ctx->free_count = AF_XDP_NUM_FRAMES;
    
    return 0;
}

static void frame_stack_free(af_xdp_ctx_t *ctx)
{
    free(ctx->free_frames);
    ctx->free_frames = NULL;
    ctx->free_count = 0;
}

static uint64_t frame_alloc(af_xdp_ctx_t *ctx)
{
    if (ctx->free_count == 0) {
        ctx->tx_dropped++;
        return UINT64_MAX;
    }
    return ctx->free_frames[--ctx->free_count];
}

static void frame_free(af_xdp_ctx_t *ctx, uint64_t addr)
{
    if (ctx->free_count < AF_XDP_NUM_FRAMES) {
        ctx->free_frames[ctx->free_count++] = addr;
    }
}

/* ── Lifecycle ── */

int af_xdp_init(af_xdp_ctx_t *ctx, const char *ifname, 
                int queue_id, uint32_t xdp_flags)
{
    if (!ctx || !ifname) {
        fprintf(stderr, "[AF_XDP] invalid arguments\n");
        return -1;
    }
    
    memset(ctx, 0, sizeof(*ctx));
    strncpy(ctx->ifname, ifname, sizeof(ctx->ifname) - 1);
    ctx->queue_id = queue_id;
    ctx->frame_size = AF_XDP_FRAME_SIZE;
    
    ctx->ifindex = if_nametoindex(ifname);
    if (ctx->ifindex == 0) {
        fprintf(stderr, "[AF_XDP] interface %s not found: %s\n", 
                ifname, strerror(errno));
        return -1;
    }
    
    ctx->umem_size = AF_XDP_UMEM_SIZE;
    ctx->umem_area = mmap(NULL, ctx->umem_size,
                          PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
                          -1, 0);
    
    if (ctx->umem_area == MAP_FAILED) {
        fprintf(stderr, "[AF_XDP] mmap with hugepages failed, trying normal pages\n");
        ctx->umem_area = mmap(NULL, ctx->umem_size,
                              PROT_READ | PROT_WRITE,
                              MAP_PRIVATE | MAP_ANONYMOUS,
                              -1, 0);
        if (ctx->umem_area == MAP_FAILED) {
            fprintf(stderr, "[AF_XDP] mmap failed: %s\n", strerror(errno));
            return -1;
        }
    }
    
    if (frame_stack_init(ctx) < 0) {
        munmap(ctx->umem_area, ctx->umem_size);
        return -1;
    }
    
    ctx->fq = (struct xsk_ring_prod *)malloc(sizeof(struct xsk_ring_prod));
    ctx->cq = (struct xsk_ring_cons *)malloc(sizeof(struct xsk_ring_cons));
    ctx->tx = (struct xsk_ring_prod *)malloc(sizeof(struct xsk_ring_prod));
    ctx->rx = (struct xsk_ring_cons *)malloc(sizeof(struct xsk_ring_cons));
    
    if (!ctx->fq || !ctx->cq || !ctx->tx || !ctx->rx) {
        fprintf(stderr, "[AF_XDP] OOM for ring structures\n");
        goto err_cleanup;
    }
    
    struct xsk_umem_config umem_cfg = {
        .fill_size = AF_XDP_FILL_RING_SIZE,
        .comp_size = AF_XDP_COMP_RING_SIZE,
        .frame_size = ctx->frame_size,
        .frame_headroom = XSK_UMEM__DEFAULT_FRAME_HEADROOM,
        .flags = 0,
    };
    
    int ret = xsk_umem__create(&ctx->umem, ctx->umem_area, ctx->umem_size,
                               ctx->fq, ctx->cq, &umem_cfg);
    if (ret) {
        fprintf(stderr, "[AF_XDP] xsk_umem__create failed: %s\n", strerror(-ret));
        goto err_cleanup;
    }
    
    struct xsk_socket_config xsk_cfg = {
        .rx_size = AF_XDP_RX_RING_SIZE,
        .tx_size = AF_XDP_TX_RING_SIZE,
        .xdp_flags = xdp_flags,
        .bind_flags = XDP_USE_NEED_WAKEUP,
    };
    
    ret = xsk_socket__create(&ctx->xsk, ifname, queue_id, ctx->umem,
                             ctx->rx, ctx->tx, &xsk_cfg);
    if (ret) {
        fprintf(stderr, "[AF_XDP] xsk_socket__create failed: %s\n", strerror(-ret));
        goto err_umem;
    }
    
    uint32_t idx;
    ret = xsk_ring_prod__reserve(ctx->fq, AF_XDP_FILL_RING_SIZE, &idx);
    for (uint32_t i = 0; i < ret; i++) {
        *xsk_ring_prod__fill_addr(ctx->fq, idx++) = i * ctx->frame_size;
    }
    xsk_ring_prod__submit(ctx->fq, ret);
    
    ctx->initialized = 1;
    printf("[AF_XDP] initialized on %s (queue %d, ifindex %d, flags 0x%x)\n",
           ifname, queue_id, ctx->ifindex, xdp_flags);
    
    return 0;

err_umem:
    xsk_umem__delete(ctx->umem);
err_cleanup:
    free(ctx->fq);
    free(ctx->cq);
    free(ctx->tx);
    free(ctx->rx);
    frame_stack_free(ctx);
    munmap(ctx->umem_area, ctx->umem_size);
    return -1;
}

void af_xdp_cleanup(af_xdp_ctx_t *ctx)
{
    if (!ctx || !ctx->initialized) return;
    
    if (ctx->xsk) {
        xsk_socket__delete(ctx->xsk);
        ctx->xsk = NULL;
    }
    
    if (ctx->umem) {
        xsk_umem__delete(ctx->umem);
        ctx->umem = NULL;
    }
    
    free(ctx->fq);
    free(ctx->cq);
    free(ctx->tx);
    free(ctx->rx);
    
    frame_stack_free(ctx);
    
    if (ctx->umem_area) {
        munmap(ctx->umem_area, ctx->umem_size);
        ctx->umem_area = NULL;
    }
    
    ctx->initialized = 0;
    printf("[AF_XDP] cleaned up %s\n", ctx->ifname);
}

/* ── Packet transmission ── */

int af_xdp_send(af_xdp_ctx_t *ctx, const void *data, size_t len)
{
    if (!ctx || !ctx->initialized || !data || len == 0) {
        return -1;
    }
    
    if (len > ctx->frame_size) {
        fprintf(stderr, "[AF_XDP] packet too large: %zu > %u\n", 
                len, ctx->frame_size);
        ctx->tx_errors++;
        return -1;
    }
    
    uint64_t frame_addr = frame_alloc(ctx);
    if (frame_addr == UINT64_MAX) {
        af_xdp_reclaim_frames(ctx);
        frame_addr = frame_alloc(ctx);
        if (frame_addr == UINT64_MAX) {
            return -1;
        }
    }
    
    uint32_t idx;
    if (xsk_ring_prod__reserve(ctx->tx, 1, &idx) != 1) {
        frame_free(ctx, frame_addr);
        return -1;
    }
    
    void *pkt = xsk_umem__get_data(ctx->umem_area, frame_addr);
    memcpy(pkt, data, len);
    
    struct xdp_desc *desc = xsk_ring_prod__tx_desc(ctx->tx, idx);
    desc->addr = frame_addr;
    desc->len = len;
    
    xsk_ring_prod__submit(ctx->tx, 1);
    
    ctx->tx_packets++;
    ctx->tx_bytes += len;
    
    return 0;
}

int af_xdp_tx_flush(af_xdp_ctx_t *ctx)
{
    if (!ctx || !ctx->initialized) {
        return -1;
    }
    
    if (xsk_ring_prod__needs_wakeup(ctx->tx)) {
        int ret = sendto(xsk_socket__fd(ctx->xsk), NULL, 0, MSG_DONTWAIT, NULL, 0);
        if (ret < 0 && errno != EAGAIN && errno != EBUSY) {
            fprintf(stderr, "[AF_XDP] sendto failed: %s\n", strerror(errno));
            ctx->tx_errors++;
            return -1;
        }
    }
    
    return 0;
}

/* ── Frame management ── */

int af_xdp_reclaim_frames(af_xdp_ctx_t *ctx)
{
    if (!ctx || !ctx->initialized) {
        return 0;
    }
    
    uint32_t idx;
    uint32_t reclaimed = xsk_ring_cons__peek(ctx->cq, AF_XDP_BATCH_SIZE, &idx);
    
    for (uint32_t i = 0; i < reclaimed; i++) {
        uint64_t addr = *xsk_ring_cons__comp_addr(ctx->cq, idx++);
        frame_free(ctx, addr);
    }
    
    if (reclaimed > 0) {
        xsk_ring_cons__release(ctx->cq, reclaimed);
    }
    
    return reclaimed;
}

int af_xdp_refill_fq(af_xdp_ctx_t *ctx)
{
    if (!ctx || !ctx->initialized) {
        return 0;
    }
    
    uint32_t idx;
    uint32_t stock = xsk_prod_nb_free(ctx->fq, AF_XDP_BATCH_SIZE);
    
    if (stock == 0) {
        return 0;
    }
    
    uint32_t reserved = xsk_ring_prod__reserve(ctx->fq, stock, &idx);
    
    for (uint32_t i = 0; i < reserved; i++) {
        uint64_t frame_addr = frame_alloc(ctx);
        if (frame_addr == UINT64_MAX) {
            break;
        }
        *xsk_ring_prod__fill_addr(ctx->fq, idx++) = frame_addr;
    }
    
    xsk_ring_prod__submit(ctx->fq, reserved);
    
    return reserved;
}

/* ── Statistics ── */

void af_xdp_get_stats(const af_xdp_ctx_t *ctx,
                      uint64_t *tx_packets,
                      uint64_t *tx_bytes,
                      uint64_t *tx_errors,
                      uint64_t *tx_dropped)
{
    if (!ctx) return;
    
    if (tx_packets) *tx_packets = ctx->tx_packets;
    if (tx_bytes)   *tx_bytes   = ctx->tx_bytes;
    if (tx_errors)  *tx_errors  = ctx->tx_errors;
    if (tx_dropped) *tx_dropped = ctx->tx_dropped;
}

void af_xdp_reset_stats(af_xdp_ctx_t *ctx)
{
    if (!ctx) return;
    
    ctx->tx_packets = 0;
    ctx->tx_bytes = 0;
    ctx->tx_errors = 0;
    ctx->tx_dropped = 0;
}

/* ── Utility ── */

int af_xdp_get_ifindex(const char *ifname)
{
    if (!ifname) return -1;
    
    unsigned int idx = if_nametoindex(ifname);
    return (idx > 0) ? (int)idx : -1;
}

int af_xdp_check_drv_support(const char *ifname)
{
    if (!ifname) return 0;
    
    int ifindex = if_nametoindex(ifname);
    if (ifindex == 0) return 0;
    
    DECLARE_LIBXDP_OPTS(xdp_program_opts, opts, .open_filename = "xdp");
    struct xdp_program *prog = xdp_program__create(&opts);
    if (!prog) return 0;

    int ret = xdp_program__attach(prog, ifindex, XDP_MODE_NATIVE, 0);
    int supported = (ret == 0);
    
    if (supported) {
        xdp_program__detach(prog, ifindex, XDP_MODE_NATIVE, 0);
    }
    
    xdp_program__close(prog);
    
    return supported;
}
