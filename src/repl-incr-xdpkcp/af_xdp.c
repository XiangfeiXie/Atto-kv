/* af_xdp.c — raw AF_XDP datapath (see af_xdp.h).
 *
 * Single socket, single NIC queue (queue 0).  Uses the raw <linux/if_xdp.h>
 * interface directly (libxdp is not installed here); only the XDP program is
 * loaded through libbpf (which IS present, v1.3.0).
 *
 * UMEM is partitioned into two disjoint pools so that Tx allocation never
 * races the kernel's Rx fill-ring consumption:
 *   Rx pool: frames [0, NUM_RX_FRAMES)     — cycled through Fill → Rx → Fill
 *   Tx pool: frames [NUM_RX_FRAMES, NUM_FRAMES) — cycled through free stack →
 *           Tx → Completion → free stack
 */

#include "af_xdp.h"

#include <linux/if_xdp.h>
#include <linux/if_link.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <bpf/libbpf.h>
#include <bpf/bpf.h>

/* ── UMEM geometry ────────────────────────────────────────────────────── */

#define FRAME_SIZE      4096
#define FRAME_HEADROOM  0
#define NUM_RX_FRAMES   2048
#define NUM_TX_FRAMES   2048
#define NUM_FRAMES      (NUM_RX_FRAMES + NUM_TX_FRAMES)

#define FILL_RING_SIZE  NUM_RX_FRAMES   /* power of two */
#define COMP_RING_SIZE  NUM_TX_FRAMES
#define RX_RING_SIZE    2048
#define TX_RING_SIZE    2048

/* ── Ring accessors (faithful port of libbpf's xsk.h helpers) ────────── */

typedef struct xsk_ring {
    __u32 *producer;
    __u32 *consumer;
    __u32 *flags;
    __u8  *desc;
    __u32  size;
    __u32  mask;
    __u32  cached_prod;
    __u32  cached_cons;
    size_t desc_size;
} xsk_ring_t;

static inline __u32 ring_prod_nb_free(xsk_ring_t *r, __u32 nb)
{
    __u32 free_entries = r->cached_cons - r->cached_prod;
    if (free_entries >= nb)
        return free_entries;
    /* Refresh the local consumer; stored as kernel_consumer + size so the
     * subtraction yields free entries directly across the 32-bit wrap. */
    r->cached_cons = __atomic_load_n(r->consumer, __ATOMIC_ACQUIRE) + r->size;
    return r->cached_cons - r->cached_prod;
}

static inline __u32 ring_cons_nb_avail(xsk_ring_t *r, __u32 nb)
{
    __u32 entries = r->cached_prod - r->cached_cons;
    if (entries == 0) {
        r->cached_prod = __atomic_load_n(r->producer, __ATOMIC_ACQUIRE);
        entries = r->cached_prod - r->cached_cons;
    }
    return (entries > nb) ? nb : entries;
}

static inline __u32 ring_prod_reserve(xsk_ring_t *r, __u32 nb, __u32 *idx)
{
    if (ring_prod_nb_free(r, nb) < nb)
        return 0;
    *idx = r->cached_prod;
    r->cached_prod += nb;
    return nb;
}

static inline void ring_prod_submit(xsk_ring_t *r)
{
    __atomic_store_n(r->producer, r->cached_prod, __ATOMIC_RELEASE);
}

static inline __u32 ring_cons_peek(xsk_ring_t *r, __u32 nb, __u32 *idx)
{
    __u32 entries = ring_cons_nb_avail(r, nb);
    if (entries > 0) {
        *idx = r->cached_cons;
        r->cached_cons += entries;
    }
    return entries;
}

static inline void ring_cons_release(xsk_ring_t *r)
{
    __atomic_store_n(r->consumer, r->cached_cons, __ATOMIC_RELEASE);
}

static inline void *ring_desc(xsk_ring_t *r, __u32 idx)
{
    return r->desc + (idx & r->mask) * r->desc_size;
}

/* ── Context ──────────────────────────────────────────────────────────── */

struct af_xdp_ctx {
    int      fd;
    int      ifindex;
    uint8_t  local_mac[6];

    void    *umem;
    uint32_t frame_size;

    xsk_ring_t fill, comp, rx, tx;

    uint64_t *tx_free;      /* Tx-pool frame offsets (stack) */
    uint32_t  tx_free_top;
    uint32_t  tx_free_cap;

    struct bpf_object *obj;
    struct bpf_link  *link;
    int xsks_map_fd;

    uint64_t rx_pkts, tx_pkts, tx_drops, rx_refill_fail;
};

static void *xsk_mmap_ring(int fd, const struct xdp_ring_offset *off,
                           uint64_t pgoff, __u32 size, size_t desc_size,
                           xsk_ring_t *ring)
{
    size_t map_size = (size_t)off->desc + (size_t)size * desc_size;
    long   ps = getpagesize();
    map_size = (map_size + (size_t)ps - 1) & ~((size_t)ps - 1);

    void *base = mmap(NULL, map_size, PROT_READ | PROT_WRITE,
                      MAP_SHARED | MAP_POPULATE, fd, (off_t)pgoff);
    if (base == MAP_FAILED)
        return NULL;

    ring->producer = (__u32 *)((char *)base + off->producer);
    ring->consumer = (__u32 *)((char *)base + off->consumer);
    ring->flags    = (__u32 *)((char *)base + off->flags);
    ring->desc     = (__u8  *)((char *)base + off->desc);
    ring->size     = size;
    ring->mask     = size - 1;
    ring->cached_prod = 0;
    ring->cached_cons = 0;
    ring->desc_size   = desc_size;
    return base;
}

static int get_local_mac(const char *ifname, uint8_t mac[6])
{
    int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s < 0)
        return -1;
    struct ifreq ifr;
    memset(&ifr, 0, sizeof(ifr));
    strncpy(ifr.ifr_name, ifname, IFNAMSIZ - 1);
    int rc = ioctl(s, SIOCGIFHWADDR, &ifr);
    if (rc == 0)
        memcpy(mac, ifr.ifr_hwaddr.sa_data, 6);
    close(s);
    return rc;
}

static int load_and_attach_xdp(struct af_xdp_ctx *ctx, const char *obj_path,
                               int ifindex)
{
    ctx->obj = bpf_object__open_file(obj_path, NULL);
    if (!ctx->obj) {
        fprintf(stderr, "[AF_XDP] bpf_object__open_file(%s) failed: %s\n",
                obj_path, strerror(errno));
        return -1;
    }

    struct bpf_program *prog =
        bpf_object__find_program_by_name(ctx->obj, "xdp_kcp_prog");
    struct bpf_map *xsks =
        bpf_object__find_map_by_name(ctx->obj, "xsks_map");
    if (!prog || !xsks) {
        fprintf(stderr, "[AF_XDP] xdp_kcp_prog / xsks_map not found in BPF object\n");
        return -1;
    }

    if (bpf_object__load(ctx->obj)) {
        fprintf(stderr, "[AF_XDP] bpf_object__load failed: %s\n",
                strerror(errno));
        return -1;
    }

    ctx->xsks_map_fd = bpf_map__fd(xsks);

    /* Insert this XSK under its queue so bpf_redirect_map finds it. */
    __u32 qid = 0;
    if (bpf_map_update_elem(ctx->xsks_map_fd, &qid, &ctx->fd, 0)) {
        fprintf(stderr, "[AF_XDP] xsks_map update failed: %s\n",
                strerror(errno));
        return -1;
    }

    /* Attach.  bpf_program__attach_xdp() uses BPF_XDP link attach, which the
     * kernel resolves to native (DRV) or generic (SKB) as the driver allows. */
    ctx->link = bpf_program__attach_xdp(prog, ifindex);
    if (!ctx->link) {
        fprintf(stderr, "[AF_XDP] bpf_program__attach_xdp failed: %s\n",
                strerror(errno));
        return -1;
    }
    printf("[AF_XDP] XDP program attached (ifindex=%d)\n", ifindex);
    return 0;
}

af_xdp_ctx_t *af_xdp_init(const char *ifname, const char *bpf_obj_path)
{
    af_xdp_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx)
        return NULL;
    ctx->fd = -1;
    ctx->frame_size = FRAME_SIZE;

    ctx->ifindex = (int)if_nametoindex(ifname);
    if (ctx->ifindex == 0) {
        fprintf(stderr, "[AF_XDP] unknown interface '%s'\n", ifname);
        goto fail;
    }
    if (get_local_mac(ifname, ctx->local_mac) != 0) {
        fprintf(stderr, "[AF_XDP] SIOCGIFHWADDR(%s) failed: %s\n",
                ifname, strerror(errno));
        goto fail;
    }

    /* ── UMEM ── */
    ctx->umem = mmap(NULL, NUM_FRAMES * FRAME_SIZE,
                     PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
    if (ctx->umem == MAP_FAILED) {
        fprintf(stderr, "[AF_XDP] umem mmap failed: %s\n", strerror(errno));
        goto fail;
    }

    ctx->fd = socket(AF_XDP, SOCK_RAW, 0);
    if (ctx->fd < 0) {
        fprintf(stderr, "[AF_XDP] socket(AF_XDP) failed: %s\n", strerror(errno));
        goto fail;
    }

    struct xdp_umem_reg reg = {
        .addr = (uint64_t)(uintptr_t)ctx->umem,
        .len  = NUM_FRAMES * FRAME_SIZE,
        .chunk_size = FRAME_SIZE,
        .headroom   = FRAME_HEADROOM,
        .flags      = 0,
        .tx_metadata_len = 0,
    };
    if (setsockopt(ctx->fd, SOL_XDP, XDP_UMEM_REG, &reg, sizeof(reg))) {
        fprintf(stderr, "[AF_XDP] XDP_UMEM_REG failed: %s\n", strerror(errno));
        goto fail;
    }

    struct xdp_mmap_offsets off;
    socklen_t offlen = sizeof(off);
    if (getsockopt(ctx->fd, SOL_XDP, XDP_MMAP_OFFSETS, &off, &offlen)) {
        fprintf(stderr, "[AF_XDP] XDP_MMAP_OFFSETS failed: %s\n", strerror(errno));
        goto fail;
    }

    __u32 fill_sz = FILL_RING_SIZE, comp_sz = COMP_RING_SIZE;
    __u32 rx_sz   = RX_RING_SIZE,   tx_sz   = TX_RING_SIZE;
    if (setsockopt(ctx->fd, SOL_XDP, XDP_UMEM_FILL_RING, &fill_sz, sizeof(fill_sz)) ||
        setsockopt(ctx->fd, SOL_XDP, XDP_UMEM_COMPLETION_RING, &comp_sz, sizeof(comp_sz)) ||
        setsockopt(ctx->fd, SOL_XDP, XDP_RX_RING, &rx_sz, sizeof(rx_sz)) ||
        setsockopt(ctx->fd, SOL_XDP, XDP_TX_RING, &tx_sz, sizeof(tx_sz))) {
        fprintf(stderr, "[AF_XDP] ring setsockopt failed: %s\n", strerror(errno));
        goto fail;
    }

    if (!xsk_mmap_ring(ctx->fd, &off.fr, XDP_UMEM_PGOFF_FILL_RING,
                       fill_sz, sizeof(__u64), &ctx->fill) ||
        !xsk_mmap_ring(ctx->fd, &off.cr, XDP_UMEM_PGOFF_COMPLETION_RING,
                       comp_sz, sizeof(__u64), &ctx->comp) ||
        !xsk_mmap_ring(ctx->fd, &off.rx, XDP_PGOFF_RX_RING,
                       rx_sz, sizeof(struct xdp_desc), &ctx->rx) ||
        !xsk_mmap_ring(ctx->fd, &off.tx, XDP_PGOFF_TX_RING,
                       tx_sz, sizeof(struct xdp_desc), &ctx->tx)) {
        fprintf(stderr, "[AF_XDP] ring mmap failed: %s\n", strerror(errno));
        goto fail;
    }

    /* Init the fill ring: offer every Rx-pool frame to the kernel. */
    ctx->fill.cached_cons = ctx->fill.size;
    for (__u32 i = 0; i < fill_sz; i++) {
        __u32 idx;
        ring_prod_reserve(&ctx->fill, 1, &idx);
        *(__u64 *)ring_desc(&ctx->fill, idx) = (__u64)i * FRAME_SIZE;
    }
    ring_prod_submit(&ctx->fill);

    /* Init the Tx free stack from the Tx pool. */
    ctx->tx_free_cap = NUM_TX_FRAMES;
    ctx->tx_free = malloc(sizeof(uint64_t) * ctx->tx_free_cap);
    if (!ctx->tx_free)
        goto fail;
    ctx->tx_free_top = 0;
    for (__u32 i = 0; i < NUM_TX_FRAMES; i++)
        ctx->tx_free[ctx->tx_free_top++] =
            (__u64)(NUM_RX_FRAMES + i) * FRAME_SIZE;

    /* Init Tx/Comp cached_cons (prod rings) to size. */
    ctx->tx.cached_cons = ctx->tx.size;
    ctx->comp.cached_cons = 0;  /* consume ring */
    ctx->rx.cached_cons = 0;    /* consume ring */

    /* ── bind ── */
    struct sockaddr_xdp sxdp = {
        .sxdp_family   = AF_XDP,
        .sxdp_flags    = XDP_COPY | XDP_USE_NEED_WAKEUP,
        .sxdp_ifindex  = (__u32)ctx->ifindex,
        .sxdp_queue_id = 0,
    };
    if (bind(ctx->fd, (struct sockaddr *)&sxdp, sizeof(sxdp))) {
        fprintf(stderr, "[AF_XDP] bind(%s q0) failed: %s\n", ifname, strerror(errno));
        goto fail;
    }

    if (load_and_attach_xdp(ctx, bpf_obj_path, ctx->ifindex) != 0)
        goto fail;

    printf("[AF_XDP] ready on %s (ifindex=%d, queue=0, umem=%u frames)\n",
           ifname, ctx->ifindex, NUM_FRAMES);
    return ctx;

fail:
    af_xdp_destroy(ctx);
    return NULL;
}

void af_xdp_destroy(af_xdp_ctx_t *ctx)
{
    if (!ctx)
        return;
    if (ctx->obj) {
        bpf_link__destroy(ctx->link);
        bpf_object__close(ctx->obj);
    }
    if (ctx->fd >= 0)
        close(ctx->fd);
    if (ctx->umem && ctx->umem != MAP_FAILED)
        munmap(ctx->umem, NUM_FRAMES * FRAME_SIZE);
    free(ctx->tx_free);
    free(ctx);
}

int af_xdp_send_frame(af_xdp_ctx_t *ctx, const void *frame, size_t len)
{
    if (len > ctx->frame_size)
        return -1;

    if (ctx->tx_free_top == 0) {
        af_xdp_poll_tx(ctx);            /* recycle any completed frames */
        if (ctx->tx_free_top == 0) {
            ctx->tx_drops++;
            return -1;
        }
    }

    uint64_t addr = ctx->tx_free[--ctx->tx_free_top];
    memcpy((char *)ctx->umem + addr, frame, len);

    __u32 idx;
    if (ring_prod_reserve(&ctx->tx, 1, &idx) == 0) {
        ctx->tx_free[ctx->tx_free_top++] = addr;   /* put it back */
        ctx->tx_drops++;
        return -1;
    }

    struct xdp_desc *d = ring_desc(&ctx->tx, idx);
    d->addr    = addr;
    d->len     = (__u32)len;
    d->options = 0;
    ring_prod_submit(&ctx->tx);

    if (*ctx->tx.flags & XDP_RING_NEED_WAKEUP)
        sendto(ctx->fd, NULL, 0, MSG_DONTWAIT, NULL, 0);

    ctx->tx_pkts++;
    return 0;
}

void af_xdp_poll_tx(af_xdp_ctx_t *ctx)
{
    __u32 idx, nb = ring_cons_peek(&ctx->comp, ctx->comp.size, &idx);
    for (__u32 i = 0; i < nb; i++) {
        __u64 addr = *(__u64 *)ring_desc(&ctx->comp, idx + i);
        if (ctx->tx_free_top < ctx->tx_free_cap)
            ctx->tx_free[ctx->tx_free_top++] = addr;
    }
    if (nb)
        ring_cons_release(&ctx->comp);
}

int af_xdp_poll_rx(af_xdp_ctx_t *ctx,
                   void (*cb)(const void *frame, size_t len, void *arg),
                   void *arg, int max_frames)
{
    int delivered = 0;
    __u32 idx, nb = ring_cons_peek(&ctx->rx, (__u32)max_frames, &idx);

    for (__u32 i = 0; i < nb; i++) {
        struct xdp_desc *d = ring_desc(&ctx->rx, idx + i);
        void *frame = (char *)ctx->umem + d->addr;
        cb(frame, d->len, arg);
        delivered++;

        /* Return the frame to the fill ring. */
        __u32 fidx;
        if (ring_prod_reserve(&ctx->fill, 1, &fidx))
            *(__u64 *)ring_desc(&ctx->fill, fidx) = d->addr;
        else
            ctx->rx_refill_fail++;
    }

    if (nb) {
        ring_prod_submit(&ctx->fill);   /* make refilled frames visible */
        ring_cons_release(&ctx->rx);
        if (*ctx->fill.flags & XDP_RING_NEED_WAKEUP)
            recvfrom(ctx->fd, NULL, 0, MSG_DONTWAIT, NULL, NULL);
        ctx->rx_pkts += nb;
    }
    return delivered;
}

int af_xdp_local_mac(af_xdp_ctx_t *ctx, uint8_t out[6])
{
    if (!ctx)
        return -1;
    memcpy(out, ctx->local_mac, 6);
    return 0;
}

int af_xdp_ifindex(af_xdp_ctx_t *ctx)
{
    return ctx ? ctx->ifindex : -1;
}
