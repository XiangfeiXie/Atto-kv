/* gateway.c — the kernel-bypass gateway process (AF_XDP + KCP + SHM).
 *
 *
 *   master gateway: SHM (engine→gateway) dequeue → ikcp_send → AF_XDP Tx
 *   slave  gateway: AF_XDP Rx → ikcp_input → ikcp_recv → SHM (gateway→engine)
 *
 * Both directions run kcp_poll() every iteration (ACKs must flow back), and
 * both gateways therefore need TX *and* RX, with the XDP program attached on
 * each machine's NIC to redirect UDP:sync_port into the XSK.
 *
 * Usage:
 *   kvstore-xdp-gateway --master --iface ens33 --bpf xdp_kcp.bpf.o \
 *       --local-ip 192.168.30.137 --peer-ip 192.168.30.136 \
 *       --peer-mac 00:0c:29:xx:xx:xx [--port 8888]
 */

#include "shm_ipc.h"
#include "kcp_transport.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <x86intrin.h>

/* How long the gateway waits (ms) for the engine to create the SHM region
 * before giving up.  The engine must be started first. */
#define SHM_WAIT_TIMEOUT_MS  30000

static volatile sig_atomic_t g_stop = 0;

static void on_signal(int sig)
{
    (void)sig;
    g_stop = 1;
}

static uint64_t get_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)(ts.tv_nsec / 1000000);
}

static int parse_mac(const char *s, uint8_t mac[6])
{
    unsigned int m[6];
    if (sscanf(s, "%x:%x:%x:%x:%x:%x", &m[0], &m[1], &m[2], &m[3], &m[4], &m[5]) != 6)
        return -1;
    for (int i = 0; i < 6; i++)
        mac[i] = (uint8_t)m[i];
    return 0;
}

static void usage(const char *argv0)
{
    fprintf(stderr,
        "usage: %s --master|--slave --iface IF --bpf OBJ --local-ip IP --peer-ip IP --peer-mac MAC [--port N]\n",
        argv0);
}

int main(int argc, char **argv)
{
    /* Line-buffer stdout so that progress markers ("transport up", stats) are
     * flushed to a redirected log file, not held in a stdio buffer. */
    setvbuf(stdout, NULL, _IOLBF, 0);

    int    is_master = -1;
    const char *iface = NULL, *bpf = "xdp_kcp.bpf.o";
    const char *local_ip = NULL, *peer_ip = NULL, *peer_mac_s = NULL;
    uint16_t sync_port = 8888;

    for (int i = 1; i < argc; i++) {
        const char *a = argv[i];
        const char *v = (i + 1 < argc) ? argv[i + 1] : NULL;
        if      (!strcmp(a, "--master")) { is_master = 1; }
        else if (!strcmp(a, "--slave"))  { is_master = 0; }
        else if (!strcmp(a, "--iface") && v)    { iface = v; i++; }
        else if (!strcmp(a, "--bpf") && v)      { bpf = v; i++; }
        else if (!strcmp(a, "--local-ip") && v) { local_ip = v; i++; }
        else if (!strcmp(a, "--peer-ip") && v)  { peer_ip = v; i++; }
        else if (!strcmp(a, "--peer-mac") && v) { peer_mac_s = v; i++; }
        else if (!strcmp(a, "--port") && v)     { sync_port = (uint16_t)atoi(v); i++; }
        else { usage(argv[0]); return 2; }
    }

    if (is_master < 0 || !iface || !local_ip || !peer_ip || !peer_mac_s) {
        usage(argv[0]);
        return 2;
    }

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    kcp_transport_cfg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.iface        = iface;
    cfg.bpf_obj_path = bpf;
    cfg.sync_port    = sync_port;
    cfg.is_master    = is_master;
    if (inet_pton(AF_INET, local_ip, &cfg.local_ip) != 1 ||
        inet_pton(AF_INET, peer_ip,  &cfg.peer_ip) != 1) {
        fprintf(stderr, "[GW] invalid local/peer IP\n");
        return 2;
    }
    if (parse_mac(peer_mac_s, cfg.peer_mac) != 0) {
        fprintf(stderr, "[GW] invalid peer MAC '%s'\n", peer_mac_s);
        return 2;
    }

    /* The engine creates the SHM region.  Announce once, then poll silently
     * (the missing region is the normal "engine not up yet" state). */
    shm_region_t *shm = NULL;
    {
        uint64_t wait_start = get_ms();
        int announced = 0;
        while (!g_stop && !(shm = shm_region_attach())) {
            if (!announced) {
                fprintf(stderr, "[GW] waiting for engine SHM region (%s)...\n",
                        shm_region_name());
                announced = 1;
            }
            if (get_ms() - wait_start > SHM_WAIT_TIMEOUT_MS) {
                fprintf(stderr, "[GW] timeout: SHM region (%s) never appeared — "
                        "start the kvstore engine (xdp_sync on) first\n",
                        shm_region_name());
                return 1;
            }
            sleep(1);
        }
        if (!shm)
            return 1;
    }

    kcp_transport_t *t = kcp_transport_init(&cfg);
    if (!t) {
        shm_region_destroy(shm, 0);
        return 1;
    }

    printf("[GW] %s gateway: iface=%s bpf=%s local=%s peer=%s:%u shm=%s\n",
           is_master ? "master" : "slave", iface, bpf, local_ip, peer_ip,
           sync_port, shm_region_name());

    /* Slave-side pending buffer: KCP ordered bytes awaiting SHM space.
     * Master-side: one dequeued item held across passes when KCP is busy. */
    enum { PENDING_CAP = 1 << 20 };
    static char pending[PENDING_CAP];
    int pending_len = 0;

    char     mbuf[SHM_MAX_PAYLOAD];
    uint32_t mlen = 0;
    int      have = 0;

    uint64_t shm_rx = 0, shm_tx = 0, drops = 0;
    int      idle = 0;

    while (!g_stop) {
        uint64_t now = get_ms();

        if (is_master) {
            int drained = 0;
            while (drained < 256) {
                if (!have) {
                    if (spsc_dequeue(&shm->q, mbuf, &mlen) != 0)
                        break;               /* SHM empty */
                    have = 1;
                }
                if (kcp_send_stream(t, mbuf, mlen) != 0)
                    break;                   /* KCP busy — keep item, retry next pass */
                have = 0;
                shm_rx++;
                drained++;
            }
        } else {
            char buf[65536];
            int n = kcp_recv_stream(t, buf, sizeof(buf));
            if (n > 0) {
                if (pending_len + n <= PENDING_CAP) {
                    memcpy(pending + pending_len, buf, (size_t)n);
                    pending_len += n;
                } else {
                    drops += (uint64_t)n;   /* engine stalled past our cap */
                }
            }
            int off = 0;
            while (off < pending_len) {
                int chunk = pending_len - off;
                if (chunk > SHM_MAX_PAYLOAD)
                    chunk = SHM_MAX_PAYLOAD;
                if (spsc_enqueue(&shm->q, pending + off, (uint32_t)chunk) != 0)
                    break;               /* SHM full — engine will drain */
                off += chunk;
            }
            if (off > 0) {
                memmove(pending, pending + off, (size_t)(pending_len - off));
                pending_len -= off;
                shm_tx += (uint64_t)off;
            }
        }

        kcp_poll(t, now);

        if (idle > 64) {
            usleep(1);
            idle = 0;
        } else {
            idle++;
            _mm_pause();
        }
    }

    printf("[GW] shutting down: shm_in=%llu shm_out=%llu drops=%llu\n",
           (unsigned long long)shm_rx,
           (unsigned long long)shm_tx,
           (unsigned long long)drops);
    {
        kcp_transport_stats_t ks;
        kcp_transport_stats(t, &ks);
        printf("[GW] kcp: sent=%llu recv=%llu tx_fail=%llu rx_invalid=%llu\n",
               (unsigned long long)ks.kcp_sent,
               (unsigned long long)ks.kcp_recv,
               (unsigned long long)ks.tx_fail,
               (unsigned long long)ks.rx_invalid);
        printf("[GW] af_xdp: tx_pkts=%llu tx_drops=%llu rx_pkts=%llu rx_refill_fail=%llu\n",
               (unsigned long long)ks.xdp.tx_pkts,
               (unsigned long long)ks.xdp.tx_drops,
               (unsigned long long)ks.xdp.rx_pkts,
               (unsigned long long)ks.xdp.rx_refill_fail);
    }

    kcp_transport_destroy(t);
    shm_region_destroy(shm, 0);
    return 0;
}
