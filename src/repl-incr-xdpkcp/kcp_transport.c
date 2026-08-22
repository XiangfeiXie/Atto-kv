/* kcp_transport.c — see kcp_transport.h. */

#include "kcp_transport.h"
#include "af_xdp.h"
#include "ikcp.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

#define ETH_HDR  14
#define IP_HDR   20
#define UDP_HDR  8
#define HDR_TOTAL (ETH_HDR + IP_HDR + UDP_HDR)

#define KCP_CONV  0x11223344u

struct kcp_transport {
    af_xdp_ctx_t *xsk;
    ikcpcb       *kcp;

    uint8_t  local_mac[6];
    uint8_t  peer_mac[6];
    uint32_t local_ip;      /* network byte order */
    uint32_t peer_ip;       /* network byte order */
    uint16_t sync_port;     /* host byte order */
    int      is_master;

    uint8_t  hdr_tmpl[HDR_TOTAL]; /* Eth+IPv4+UDP header with constant fields */

    uint64_t kcp_sent, kcp_recv, rx_invalid, tx_fail;
};

static inline uint16_t rd16(const uint8_t *p)
{
    return (uint16_t)(((uint16_t)p[0] << 8) | p[1]);
}

static inline uint32_t rd32(const uint8_t *p)
{
    return ((uint32_t)rd16(p) << 16) | rd16(p + 2);
}

/* IPv4 header checksum (RFC 791).  `ip` is 20 bytes; checksum field (bytes
 * 10-11) must be zero before the call. */
static uint16_t ip_cksum(const uint8_t *ip)
{
    uint32_t sum = 0;
    for (int i = 0; i < IP_HDR; i += 2)
        sum += rd16(ip + i);
    while (sum >> 16)
        sum = (sum & 0xffff) + (sum >> 16);
    return (uint16_t)~sum;
}

static void wr16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)(v & 0xff);
}

static void wr32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)(v & 0xff);
}

/* Assemble Eth + IPv4 + UDP around one KCP datagram and push it out.
 *
 * The constant header fields are pre-filled once in t->hdr_tmpl (see
 * kcp_transport_init); here we copy that template straight into the UMEM Tx
 * frame (no intermediate stack buffer → no second memcpy), patch the three
 * length/checksum fields, then commit.  The kernel is woken once per flush by
 * kcp_flush(), not per frame. */
static int send_kcp_datagram(kcp_transport_t *t, const void *datagram, size_t dlen)
{
    size_t iplen = IP_HDR + UDP_HDR + dlen;
    size_t total = ETH_HDR + iplen;
    if (total > 1500)                   /* Ethernet MTU */
        return -1;

    uint64_t addr;
    uint8_t *frame = af_xdp_tx_acquire(t->xsk, &addr);
    if (!frame) {
        t->tx_fail++;
        return -1;
    }

    uint8_t *ip  = frame + ETH_HDR;
    uint8_t *udp = frame + ETH_HDR + IP_HDR;

    memcpy(frame, t->hdr_tmpl, HDR_TOTAL);

    wr16(ip + 2, (uint16_t)iplen);      /* IPv4 total length */
    wr16(ip + 10, 0);                   /* checksum (zero for now) */
    wr16(ip + 10, ip_cksum(ip));
    wr16(udp + 4, (uint16_t)(UDP_HDR + dlen));

    memcpy(udp + UDP_HDR, datagram, dlen);

    if (af_xdp_tx_commit(t->xsk, addr, total) != 0) {
        t->tx_fail++;
        return -1;
    }
    return 0;
}

/* KCP output callback: a KCP datagram must go on the wire. */
static int kcp_output_cb(const char *buf, int len, ikcpcb *kcp, void *user)
{
    (void)kcp;
    kcp_transport_t *t = user;
    return send_kcp_datagram(t, buf, (size_t)len);
}

/* Parse an incoming frame; if it is UDP:sync_port, hand the payload (KCP
 * datagram) to ikcp_input. */
static void rx_frame_cb(const void *frame, size_t len, void *arg)
{
    kcp_transport_t *t = arg;
    const uint8_t *p = frame;

    if (len < HDR_TOTAL)
        return;
    if (rd16(p + 12) != 0x0800)         /* not IPv4 */
        return;

    const uint8_t *ip = p + ETH_HDR;
    int ihl = (ip[0] & 0x0f) * 4;
    if (ihl < IP_HDR || (size_t)(ETH_HDR + ihl + UDP_HDR) > len)
        return;
    if (ip[9] != 17)                    /* not UDP */
        return;

    const uint8_t *udp = ip + ihl;
    uint16_t dport = rd16(udp + 2);
    if (dport != (uint16_t)t->sync_port) {
        t->rx_invalid++;
        return;
    }

    const uint8_t *payload = udp + UDP_HDR;
    size_t plen = len - (size_t)(payload - p);

    if (plen > 0)
        ikcp_input(t->kcp, (const char *)payload, (long)plen);
}

kcp_transport_t *kcp_transport_init(const kcp_transport_cfg_t *cfg)
{
    kcp_transport_t *t = calloc(1, sizeof(*t));
    if (!t)
        return NULL;

    t->sync_port = cfg->sync_port;
    t->local_ip  = cfg->local_ip;
    t->peer_ip   = cfg->peer_ip;
    t->is_master = cfg->is_master;
    memcpy(t->peer_mac, cfg->peer_mac, 6);

    t->xsk = af_xdp_init(cfg->iface, cfg->bpf_obj_path);
    if (!t->xsk)
        goto fail;
    af_xdp_local_mac(t->xsk, t->local_mac);

    /* Pre-fill the constant Ethernet + IPv4 + UDP header.  Per-frame we only
     * patch total-length, IP checksum and UDP length (send_kcp_datagram). */
    {
        uint8_t *eth = t->hdr_tmpl;
        uint8_t *ip  = t->hdr_tmpl + ETH_HDR;
        uint8_t *udp = t->hdr_tmpl + ETH_HDR + IP_HDR;

        memcpy(eth, t->peer_mac, 6);
        memcpy(eth + 6, t->local_mac, 6);
        eth[12] = 0x08; eth[13] = 0x00;   /* EtherType IPv4 */

        ip[0] = 0x45;                      /* version 4, IHL 5 */
        wr16(ip + 4, 0);                   /* identification */
        wr16(ip + 6, 0x4000);              /* flags/offset: DF */
        ip[8]  = 64;                       /* TTL */
        ip[9]  = 17;                       /* UDP */
        wr32(ip + 12, t->local_ip);        /* src */
        wr32(ip + 16, t->peer_ip);         /* dst */

        wr16(udp + 0, (uint16_t)t->sync_port);  /* src port */
        wr16(udp + 2, (uint16_t)t->sync_port);  /* dst port */
        wr16(udp + 6, 0);                  /* UDP checksum = 0 */
    }

    t->kcp = ikcp_create(KCP_CONV, t);
    if (!t->kcp)
        goto fail;
    ikcp_setoutput(t->kcp, kcp_output_cb);
    ikcp_nodelay(t->kcp, 1, 10, 2, 1);   /* nodelay, 10ms interval, fastresend, no cwnd */
    ikcp_wndsize(t->kcp, 256, 256);
    ikcp_setmtu(t->kcp, 1400);
    t->kcp->stream = 1;                  /* byte-stream semantics */

    printf("[KCP] transport up: role=%s peer=%u.%u.%u.%u:%u\n",
           t->is_master ? "master" : "slave",
           (t->peer_ip >> 24) & 0xff, (t->peer_ip >> 16) & 0xff,
           (t->peer_ip >> 8) & 0xff, (t->peer_ip) & 0xff,
           t->sync_port);
    return t;

fail:
    kcp_transport_destroy(t);
    return NULL;
}

void kcp_transport_destroy(kcp_transport_t *t)
{
    if (!t)
        return;
    if (t->kcp)
        ikcp_release(t->kcp);
    if (t->xsk)
        af_xdp_destroy(t->xsk);
    free(t);
}

int kcp_send_queue(kcp_transport_t *t, const void *data, size_t len)
{
    int r = ikcp_send(t->kcp, data, (int)len);
    if (r < 0)
        return -1;
    t->kcp_sent += (uint64_t)len;
    return 0;
}

void kcp_flush(kcp_transport_t *t)
{
    ikcp_flush(t->kcp);                  /* output callback fires now */
    af_xdp_tx_kick(t->xsk);              /* one wakeup for the whole batch */
}

int kcp_recv_stream(kcp_transport_t *t, void *buf, size_t cap)
{
    int total = 0;
    char *dst = buf;

    while ((size_t)total < cap) {
        int peek = ikcp_peeksize(t->kcp);
        if (peek <= 0)
            break;
        if (peek > (int)(cap - (size_t)total))
            break;                       /* leave it queued; caller retries */
        int n = ikcp_recv(t->kcp, dst + total, peek);
        if (n < 0)
            break;
        total += n;
    }
    if (total > 0)
        t->kcp_recv += (uint64_t)total;
    return total;
}

void kcp_poll(kcp_transport_t *t, uint64_t now_ms)
{
    /* Advance the KCP clock first so `current` is fresh for ikcp_input's RTT
     * calc and for the flush below. */
    ikcp_update(t->kcp, (IUINT32)now_ms);
    /* ikcp_input queues ACKs for any data segments received, but does not
     * flush them — the only flush inside the KCP library is gated by the
     * `interval` tick (10ms).  Flush immediately so the peer's send window
     * advances at RTT speed instead of one window per 10ms. */
    af_xdp_poll_rx(t->xsk, rx_frame_cb, t, 256);
    kcp_flush(t);
    af_xdp_poll_tx(t->xsk);
}

void kcp_transport_stats(const kcp_transport_t *t, kcp_transport_stats_t *out)
{
    if (!t || !out)
        return;
    out->kcp_sent   = t->kcp_sent;
    out->kcp_recv   = t->kcp_recv;
    out->tx_fail    = t->tx_fail;
    out->rx_invalid = t->rx_invalid;
    af_xdp_stats(t->xsk, &out->xdp);
}
