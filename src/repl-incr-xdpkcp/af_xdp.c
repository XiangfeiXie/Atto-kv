/* af_xdp.c — AF_XDP datapath (see af_xdp.h).
 *
 * Built on libxdp's xsk.h (the maintained AF_XDP user-space library), which
 * provides UMEM/socket/ring management and the ring accessors that a raw
 * <linux/if_xdp.h> implementation would otherwise have to hand-roll.  Only the
 * XDP program load/attach is done through libbpf (bpf_program__attach_xdp,
 * link-based with automatic DRV → generic fallback); the socket is created
 * with XSK_LIBBPF_FLAGS__INHIBIT_PROG_LOAD so it does not load its own program.
 *
 * UMEM is partitioned into two disjoint pools so Tx allocation never races
 * the kernel's Rx fill-ring consumption:
 *   Rx pool: frames [0, NUM_RX_FRAMES)          — cycled Fill → Rx → Fill
 *   Tx pool: frames [NUM_RX_FRAMES, NUM_FRAMES) — cycled free stack → Tx →
 *             Completion → free stack
 */

#include "af_xdp.h"

#include <linux/if_xdp.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <bpf/libbpf.h>
#include <bpf/bpf.h>
#include <xdp/xsk.h>

/* ── UMEM geometry ────────────────────────────────────────────────────── */

#define FRAME_SIZE      4096
#define NUM_RX_FRAMES   2048
#define NUM_TX_FRAMES   2048
#define NUM_FRAMES      (NUM_RX_FRAMES + NUM_TX_FRAMES)

#define FILL_RING_SIZE  2048   /* power of two */
#define COMP_RING_SIZE  2048
#define RX_RING_SIZE    2048
#define TX_RING_SIZE    2048

/* ── Context ──────────────────────────────────────────────────────────── */

struct af_xdp_ctx {
    int      fd;              /* cached xsk_socket__fd(xsk) */
    int      ifindex;
    uint8_t  local_mac[6];

    void             *umem_area;   /* UMEM buffer base (we own the mmap) */
    struct xsk_umem   *umem;
    struct xsk_socket *xsk;

    struct xsk_ring_prod fill, tx;
    struct xsk_ring_cons comp, rx;

    uint64_t *tx_free;      /* Tx-pool frame offsets (stack) */
    uint32_t  tx_free_top;
    uint32_t  tx_free_cap;

    struct bpf_object *obj;
    struct bpf_link   *link;

    uint64_t rx_pkts, tx_pkts, tx_drops, rx_refill_fail;
};

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

/* Load xdp_kcp.bpf.o, insert this XSK into its XSKMAP, then attach it. */
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

    /* Insert this XSK under its queue so bpf_redirect_map finds it. */
    if (xsk_socket__update_xskmap(ctx->xsk, bpf_map__fd(xsks))) {
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
    ctx->umem_area = mmap(NULL, NUM_FRAMES * FRAME_SIZE,
                          PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
    if (ctx->umem_area == MAP_FAILED) {
        fprintf(stderr, "[AF_XDP] umem mmap failed: %s\n", strerror(errno));
        goto fail;
    }

    struct xsk_umem_config ucfg = {
        .fill_size      = FILL_RING_SIZE,
        .comp_size      = COMP_RING_SIZE,
        .frame_size     = FRAME_SIZE,
        .frame_headroom = 0,
        .flags          = 0,
    };
    if (xsk_umem__create(&ctx->umem, ctx->umem_area, NUM_FRAMES * FRAME_SIZE,
                         &ctx->fill, &ctx->comp, &ucfg)) {
        fprintf(stderr, "[AF_XDP] xsk_umem__create failed: %s\n", strerror(errno));
        goto fail;
    }

    struct xsk_socket_config scfg = {
        .rx_size      = RX_RING_SIZE,
        .tx_size      = TX_RING_SIZE,
        .libbpf_flags = XSK_LIBBPF_FLAGS__INHIBIT_PROG_LOAD,  /* we attach our own */
        .xdp_flags    = 0,
        .bind_flags   = XDP_COPY | XDP_USE_NEED_WAKEUP,
    };
    if (xsk_socket__create(&ctx->xsk, ifname, 0, ctx->umem, &ctx->rx, &ctx->tx,
                           &scfg)) {
        fprintf(stderr, "[AF_XDP] xsk_socket__create failed: %s\n", strerror(errno));
        goto fail;
    }
    ctx->fd = xsk_socket__fd(ctx->xsk);

    /* Offer every Rx-pool frame to the kernel via the fill ring. */
    __u32 idx, nb = FILL_RING_SIZE;
    if (xsk_ring_prod__reserve(&ctx->fill, nb, &idx) != nb)
        goto fail;
    for (__u32 i = 0; i < nb; i++)
        *xsk_ring_prod__fill_addr(&ctx->fill, idx + i) = (__u64)i * FRAME_SIZE;
    xsk_ring_prod__submit(&ctx->fill, nb);

    /* Init the Tx free stack from the Tx pool. */
    ctx->tx_free_cap = NUM_TX_FRAMES;
    ctx->tx_free = malloc(sizeof(uint64_t) * ctx->tx_free_cap);
    if (!ctx->tx_free)
        goto fail;
    for (__u32 i = 0; i < NUM_TX_FRAMES; i++)
        ctx->tx_free[ctx->tx_free_top++] =
            (__u64)(NUM_RX_FRAMES + i) * FRAME_SIZE;

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
    if (ctx->xsk)
        xsk_socket__delete(ctx->xsk);
    if (ctx->umem)
        xsk_umem__delete(ctx->umem);
    if (ctx->umem_area && ctx->umem_area != MAP_FAILED)
        munmap(ctx->umem_area, NUM_FRAMES * FRAME_SIZE);
    free(ctx->tx_free);
    free(ctx);
}

int af_xdp_send_frame(af_xdp_ctx_t *ctx, const void *frame, size_t len)
{
    if (len > FRAME_SIZE)
        return -1;

    if (ctx->tx_free_top == 0) {
        af_xdp_poll_tx(ctx);            /* recycle any completed frames */
        if (ctx->tx_free_top == 0) {
            ctx->tx_drops++;
            return -1;
        }
    }

    uint64_t addr = ctx->tx_free[--ctx->tx_free_top];
    memcpy(xsk_umem__get_data(ctx->umem_area, addr), frame, len);

    __u32 idx;
    if (xsk_ring_prod__reserve(&ctx->tx, 1, &idx) == 0) {
        ctx->tx_free[ctx->tx_free_top++] = addr;   /* put it back */
        ctx->tx_drops++;
        return -1;
    }

    struct xdp_desc *d = xsk_ring_prod__tx_desc(&ctx->tx, idx);
    d->addr    = addr;
    d->len     = (__u32)len;
    d->options = 0;
    xsk_ring_prod__submit(&ctx->tx, 1);

    if (xsk_ring_prod__needs_wakeup(&ctx->tx))
        sendto(ctx->fd, NULL, 0, MSG_DONTWAIT, NULL, 0);

    ctx->tx_pkts++;
    return 0;
}

void af_xdp_poll_tx(af_xdp_ctx_t *ctx)
{
    __u32 idx, nb = xsk_ring_cons__peek(&ctx->comp, COMP_RING_SIZE, &idx);
    for (__u32 i = 0; i < nb; i++) {
        __u64 addr = *xsk_ring_cons__comp_addr(&ctx->comp, idx + i);
        if (ctx->tx_free_top < ctx->tx_free_cap)
            ctx->tx_free[ctx->tx_free_top++] = addr;
    }
    if (nb)
        xsk_ring_cons__release(&ctx->comp, nb);
}

int af_xdp_poll_rx(af_xdp_ctx_t *ctx,
                   void (*cb)(const void *frame, size_t len, void *arg),
                   void *arg, int max_frames)
{
    int delivered = 0;
    __u32 refilled = 0;
    __u32 idx, nb = xsk_ring_cons__peek(&ctx->rx, (__u32)max_frames, &idx);

    for (__u32 i = 0; i < nb; i++) {
        const struct xdp_desc *d = xsk_ring_cons__rx_desc(&ctx->rx, idx + i);
        cb(xsk_umem__get_data(ctx->umem_area, d->addr), d->len, arg);
        delivered++;

        /* Return the frame to the fill ring. */
        __u32 fidx;
        if (xsk_ring_prod__reserve(&ctx->fill, 1, &fidx)) {
            *xsk_ring_prod__fill_addr(&ctx->fill, fidx) = d->addr;
            refilled++;
        } else {
            ctx->rx_refill_fail++;
        }
    }

    if (nb) {
        if (refilled)
            xsk_ring_prod__submit(&ctx->fill, refilled);
        xsk_ring_cons__release(&ctx->rx, nb);
        if (xsk_ring_prod__needs_wakeup(&ctx->fill))
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
