#include "xdp_repl_manager.h"
#include "xdp_incr_repl.h"
#include "dynbuf.h"
#include "config.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifdef HAVE_LIBXDP
#include <arpa/inet.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <linux/if_ether.h>
#include <linux/if_link.h>
#include <linux/ip.h>
#include <linux/udp.h>
#include <net/if.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <xdp/libxdp.h>
#include <xdp/xsk.h>
#ifndef IPPROTO_UDP
#define IPPROTO_UDP 17
#endif
#define UMEM_SIZE (64u * 1024u * 1024u)
#define FRAME_SIZE 4096u
#define NUM_FRAMES (UMEM_SIZE / FRAME_SIZE)
#define RING_SIZE 2048u
#define BATCH 64u

typedef struct {
    int started, ifindex, queue_id, udp_port, zero_copy, attached;
    char ifname[64];
    void *umem_area;
    size_t umem_size;
    struct xsk_umem *umem;
    struct xsk_socket *xsk;
    struct xsk_ring_prod fq, tx;
    struct xsk_ring_cons cq, rx;
    uint64_t *free_frames;
    uint32_t free_count;
    struct bpf_object *obj;
    int prog_fd, xsks_map_fd, port_map_fd;
    uint32_t xdp_flags;
    uint64_t tx_packets, tx_bytes, tx_errors, rx_packets, rx_bytes, rx_errors;
} repl_xdp_ctx_t;

static repl_xdp_ctx_t g;

static uint16_t ip_csum(const void *data, size_t len)
{
    const uint16_t *p = data;
    uint32_t sum = 0;
    while (len > 1) { sum += *p++; len -= 2; }
    if (len) sum += *(const uint8_t *)p;
    while (sum >> 16) sum = (sum & 0xffffu) + (sum >> 16);
    return (uint16_t)~sum;
}

static uint64_t frame_alloc(void)
{
    return g.free_count ? g.free_frames[--g.free_count] : UINT64_MAX;
}

static void frame_free(uint64_t addr)
{
    if (g.free_count < NUM_FRAMES) g.free_frames[g.free_count++] = addr;
}

static int frames_init(void)
{
    g.free_frames = malloc(sizeof(uint64_t) * NUM_FRAMES);
    if (!g.free_frames) return -1;
    for (uint32_t i = 0; i < NUM_FRAMES; i++) g.free_frames[i] = (uint64_t)i * FRAME_SIZE;
    g.free_count = NUM_FRAMES;
    return 0;
}

static int tx_reclaim(void)
{
    uint32_t idx = 0, n = xsk_ring_cons__peek(&g.cq, BATCH, &idx);
    for (uint32_t i = 0; i < n; i++) frame_free(*xsk_ring_cons__comp_addr(&g.cq, idx++));
    if (n) xsk_ring_cons__release(&g.cq, n);
    return (int)n;
}

static int rx_refill(void)
{
    uint32_t idx = 0, want = xsk_prod_nb_free(&g.fq, BATCH);
    if (!want) return 0;
    uint32_t n = xsk_ring_prod__reserve(&g.fq, want, &idx), filled = 0;
    for (; filled < n; filled++) {
        uint64_t addr = frame_alloc();
        if (addr == UINT64_MAX) break;
        *xsk_ring_prod__fill_addr(&g.fq, idx++) = addr;
    }
    if (filled) xsk_ring_prod__submit(&g.fq, filled);
    return (int)filled;
}

static int bpf_load_attach(void)
{
    g.obj = bpf_object__open_file("build/xdp_repl.kern.o", NULL);
    if (!g.obj || bpf_object__load(g.obj) != 0) return -1;
    struct bpf_program *prog = bpf_object__find_program_by_name(g.obj, "xdp_kvs_repl");
    g.prog_fd = prog ? bpf_program__fd(prog) : -1;
    g.xsks_map_fd = bpf_object__find_map_fd_by_name(g.obj, "kvs_repl_xsks");
    g.port_map_fd = bpf_object__find_map_fd_by_name(g.obj, "kvs_repl_port");
    if (g.prog_fd < 0 || g.xsks_map_fd < 0 || g.port_map_fd < 0) return -1;
    uint32_t key = 0;
    uint16_t port = (uint16_t)g.udp_port;
    if (bpf_map_update_elem(g.port_map_fd, &key, &port, BPF_ANY) != 0) return -1;
    if (bpf_xdp_attach(g.ifindex, g.prog_fd, g.xdp_flags, NULL) != 0) return -1;
    g.attached = 1;
    return 0;
}

static int xsk_init(void)
{
    g.umem_size = UMEM_SIZE;
    g.umem_area = mmap(NULL, g.umem_size, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (g.umem_area == MAP_FAILED)
        g.umem_area = mmap(NULL, g.umem_size, PROT_READ | PROT_WRITE,
                           MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (g.umem_area == MAP_FAILED || frames_init() != 0) return -1;
    struct xsk_umem_config uc = {.fill_size=RING_SIZE,.comp_size=RING_SIZE,.frame_size=FRAME_SIZE,.frame_headroom=XSK_UMEM__DEFAULT_FRAME_HEADROOM,.flags=0};
    int rc = xsk_umem__create(&g.umem, g.umem_area, g.umem_size, &g.fq, &g.cq, &uc);
    if (rc) return -1;
    struct xsk_socket_config xc = {
        .rx_size = RING_SIZE,
        .tx_size = RING_SIZE,
        .libbpf_flags = XSK_LIBBPF_FLAGS__INHIBIT_PROG_LOAD,
        .xdp_flags = g.xdp_flags,
        .bind_flags = XDP_USE_NEED_WAKEUP | (g.zero_copy ? XDP_ZEROCOPY : XDP_COPY)
    };
    rc = xsk_socket__create(&g.xsk, g.ifname, g.queue_id, g.umem, &g.rx, &g.tx, &xc);
    if (rc) return -1;
    int fd = xsk_socket__fd(g.xsk);
    uint32_t qid = (uint32_t)g.queue_id;
    if (bpf_map_update_elem(g.xsks_map_fd, &qid, &fd, BPF_ANY) != 0) return -1;
    rx_refill();
    return 0;
}

int xdp_repl_manager_init(const char *ifname, int queue_id, int udp_port, int zero_copy_required)
{
    if (!ifname || queue_id < 0 || udp_port <= 0 || udp_port > 65535) return -1;
    if (g.started) return 0;
    memset(&g, 0, sizeof(g));
    snprintf(g.ifname, sizeof(g.ifname), "%s", ifname);
    g.queue_id = queue_id; g.udp_port = udp_port; g.zero_copy = zero_copy_required;
    g.ifindex = (int)if_nametoindex(ifname);
    g.xdp_flags = XDP_FLAGS_UPDATE_IF_NOEXIST |
                  (kvs_config.xdp_driver_mode ? XDP_FLAGS_DRV_MODE : XDP_FLAGS_SKB_MODE);
    if (g.ifindex <= 0 || bpf_load_attach() != 0 || xsk_init() != 0) {
        fprintf(stderr, "[XDP-REPL] AF_XDP init failed on %s: %s\n", ifname, strerror(errno));
        xdp_repl_manager_stop();
        return -1;
    }
    g.started = 1;
    printf("[XDP-REPL] AF_XDP ready iface=%s queue=%d port=%d mode=%s\n", g.ifname, g.queue_id, g.udp_port, g.zero_copy ? "zerocopy" : "copy");
    return 0;
}

static int build_packet(void *dst, size_t cap, const kvs_repl_frame_hdr_t *hdr, const char *payload, uint32_t len, size_t *out)
{
    size_t total = sizeof(struct ethhdr) + sizeof(struct iphdr) + sizeof(struct udphdr) + sizeof(*hdr) + len;
    if (total > cap) return -1;
    memset(dst, 0, total);
    struct ethhdr *eth = dst;
    memset(eth->h_dest, 0xff, ETH_ALEN);
    eth->h_proto = htons(ETH_P_IP);
    struct iphdr *ip = (struct iphdr *)(eth + 1);
    ip->version = 4; ip->ihl = 5; ip->ttl = 64; ip->protocol = IPPROTO_UDP;
    ip->tot_len = htons((uint16_t)(total - sizeof(*eth)));
    ip->saddr = htonl(0x7f000001u); ip->daddr = htonl(0x7f000001u);
    ip->check = ip_csum(ip, sizeof(*ip));
    struct udphdr *udp = (struct udphdr *)(ip + 1);
    udp->source = htons((uint16_t)g.udp_port); udp->dest = htons((uint16_t)g.udp_port);
    udp->len = htons((uint16_t)(sizeof(*udp) + sizeof(*hdr) + len));
    char *p = (char *)(udp + 1);
    memcpy(p, hdr, sizeof(*hdr));
    if (len) memcpy(p + sizeof(*hdr), payload, len);
    *out = total;
    return 0;
}

int xdp_repl_send_fragment(const kvs_repl_frame_hdr_t *hdr, const char *payload, uint32_t len)
{
    if (!g.started || !hdr || (!payload && len)) return -1;
    tx_reclaim();
    uint64_t addr = frame_alloc();
    if (addr == UINT64_MAX) { g.tx_errors++; return -1; }
    uint32_t idx = 0;
    if (xsk_ring_prod__reserve(&g.tx, 1, &idx) != 1) { frame_free(addr); g.tx_errors++; return -1; }
    void *pkt = xsk_umem__get_data(g.umem_area, addr);
    size_t pkt_len = 0;
    if (build_packet(pkt, FRAME_SIZE, hdr, payload, len, &pkt_len) != 0) { frame_free(addr); g.tx_errors++; return -1; }
    struct xdp_desc *d = xsk_ring_prod__tx_desc(&g.tx, idx);
    d->addr = addr; d->len = (uint32_t)pkt_len;
    xsk_ring_prod__submit(&g.tx, 1);
    if (xsk_ring_prod__needs_wakeup(&g.tx)) sendto(xsk_socket__fd(g.xsk), NULL, 0, MSG_DONTWAIT, NULL, 0);
    g.tx_packets++; g.tx_bytes += pkt_len;
    return 0;
}

int xdp_repl_poll(void)
{
    if (!g.started) return -1;
    rx_refill();
    uint32_t idx = 0, n = xsk_ring_cons__peek(&g.rx, BATCH, &idx);
    for (uint32_t i = 0; i < n; i++) {
        const struct xdp_desc *d = xsk_ring_cons__rx_desc(&g.rx, idx++);
        if (d->len >= sizeof(struct ethhdr) + sizeof(struct iphdr) + sizeof(struct udphdr) + sizeof(kvs_repl_frame_hdr_t)) {
            const char *pkt = xsk_umem__get_data(g.umem_area, d->addr);
            const struct ethhdr *eth = (const struct ethhdr *)pkt;
            const struct iphdr *ip = (const struct iphdr *)(eth + 1);
            const struct udphdr *udp = (const struct udphdr *)((const char *)ip + ip->ihl * 4);
            const kvs_repl_frame_hdr_t *h = (const kvs_repl_frame_hdr_t *)(udp + 1);
            uint32_t udp_len = ntohs(udp->len);
            uint32_t repl_len = udp_len >= sizeof(*udp) + sizeof(*h)
                ? udp_len - (uint32_t)sizeof(*udp) - (uint32_t)sizeof(*h)
                : 0;
            const char *payload = (const char *)(h + 1);
            if (ntohs(eth->h_proto) == ETH_P_IP && ip->protocol == IPPROTO_UDP && ntohs(udp->dest) == (uint16_t)g.udp_port && h->magic == KVS_REPL_FRAME_MAGIC) {
                if (h->type == KVS_REPL_FRAME_ACK) {
                    xdp_incr_handle_ack(h->seq);
                    g.rx_packets++; g.rx_bytes += d->len;
                } else {
                    DynBuf wb;
                    dynbuf_init(&wb, 4096);
                    if (xdp_incr_apply_frame(h, payload, repl_len, &wb) == 0) {
                        g.rx_packets++; g.rx_bytes += d->len;
                    } else {
                        g.rx_errors++;
                    }
                    dynbuf_free(&wb);
                }
            } else g.rx_errors++;
        } else g.rx_errors++;
        frame_free(d->addr);
    }
    if (n) xsk_ring_cons__release(&g.rx, n);
    rx_refill();
    return (int)n;
}

void xdp_repl_manager_stop(void)
{
    if (g.xsks_map_fd >= 0) { uint32_t qid = (uint32_t)g.queue_id; bpf_map_delete_elem(g.xsks_map_fd, &qid); }
    if (g.xsk) xsk_socket__delete(g.xsk);
    if (g.umem) xsk_umem__delete(g.umem);
    if (g.attached) bpf_xdp_detach(g.ifindex, g.xdp_flags, NULL);
    if (g.obj) bpf_object__close(g.obj);
    if (g.umem_area && g.umem_area != MAP_FAILED) munmap(g.umem_area, g.umem_size);
    free(g.free_frames);
    if (g.started) printf("[XDP-REPL] stopped tx=%llu rx=%llu\n", (unsigned long long)g.tx_packets, (unsigned long long)g.rx_packets);
    memset(&g, 0, sizeof(g));
}

#else
static int g_started;
int xdp_repl_manager_init(const char *ifname, int queue_id, int udp_port, int zero_copy_required)
{
    if (!ifname || udp_port <= 0) return -1;
    g_started = 1;
    printf("[XDP-REPL] libxdp unavailable; staged manager active iface=%s queue=%d port=%d zero_copy=%s\n", ifname, queue_id, udp_port, zero_copy_required ? "require" : "auto");
    return 0;
}
int xdp_repl_send_fragment(const kvs_repl_frame_hdr_t *hdr, const char *payload, uint32_t len)
{
    if (!g_started || !hdr || (!payload && len)) return -1;
    return 0;
}
int xdp_repl_poll(void) { return g_started ? 0 : -1; }
void xdp_repl_manager_stop(void) { if (g_started) printf("[XDP-REPL] staged manager stopped\n"); g_started = 0; }
#endif
