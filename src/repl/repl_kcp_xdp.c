/* repl_kcp_xdp.c — KCP + AF_XDP replication transport layer
 *
 * Master flow:
 *   1. Load eBPF filter (repl_filter.bpf.o) → captures write commands
 *   2. Poll ringbuf → feed to KCP → output via AF_XDP
 *   3. Full sync on slave connect (via RDMA or TCP fallback)
 *
 * Slave flow:
 *   1. Receive via AF_XDP → feed to KCP → extract commands
 *   2. Write commands to local kvstore socket
 *   3. Auto-reconnect on disconnect
 */
#define _GNU_SOURCE
#include "repl_kcp_xdp.h"
#include "repl_observe.h"

#include <arpa/inet.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/if_link.h>
#include <net/if.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "ikcp.h"

#ifdef ENABLE_AF_XDP
#include "af_xdp_socket.h"
#endif

/* ── Constants ──────────────────────────────────────────────────────── */

#define REPL_KCP_MTU        1400
#define REPL_KCP_WND        256
#define REPL_KCP_INTERVAL   10
#define REPL_FRAME_HEADER   4

/* AF_XDP configuration */
#define XSK_RING_PROD_SIZE  2048
#define XSK_RING_CONS_SIZE  2048
#define XSK_UMEM_SIZE       (4096 * 2048)
#define XSK_FRAME_SIZE      2048

/* ── Global state ───────────────────────────────────────────────────── */

static struct {
    struct repl_kcp_config cfg;
    pthread_t              thread;
    volatile int           running;

    /* Transport mode */
    transport_mode_t       transport;

    /* KCP state */
    ikcpcb                *kcp;
    uint32_t               conv_id;

    /* eBPF state (master only) */
    struct bpf_object     *bpf_obj;
    struct ring_buffer    *ringbuf;
    int                    cgroup_fd;

#ifdef ENABLE_AF_XDP
    /* AF_XDP state */
    struct af_xdp_socket  *xsk;
    struct bpf_object     *xdp_obj;  /* XDP program object */
    int                    xdp_prog_fd;
#endif

    /* UDP socket for fallback */
    int                    udp_fd;
    struct sockaddr_in     peer_addr;

    /* Statistics */
    struct repl_kcp_stats  stats;
    pthread_mutex_t        stats_lock;

} g_repl = {0};

/* ── KCP output callback ────────────────────────────────────────────── */

static int kcp_output(const char *buf, int len, ikcpcb *kcp, void *user)
{
    (void)kcp; (void)user;

#ifdef ENABLE_AF_XDP
    /* AF_XDP zero-copy send */
    if (g_repl.transport == TRANSPORT_AF_XDP && g_repl.xsk) {
        int ret = af_xdp_send(g_repl.xsk, buf, len);
        if (ret > 0) {
            pthread_mutex_lock(&g_repl.stats_lock);
            g_repl.stats.bytes_sent += ret;
            g_repl.stats.pkts_sent++;
            pthread_mutex_unlock(&g_repl.stats_lock);
            return ret;
        }
        /* Fall through to UDP on error */
    }
#endif

    /* UDP fallback */
    if (g_repl.udp_fd >= 0 && g_repl.peer_addr.sin_port != 0) {
        ssize_t n = sendto(g_repl.udp_fd, buf, len, 0,
                          (struct sockaddr *)&g_repl.peer_addr,
                          sizeof(g_repl.peer_addr));
        if (n > 0) {
            pthread_mutex_lock(&g_repl.stats_lock);
            g_repl.stats.bytes_sent += n;
            g_repl.stats.pkts_sent++;
            pthread_mutex_unlock(&g_repl.stats_lock);
        }
        return n;
    }

    return 0;
}

/* ── eBPF ringbuf callback (master only) ────────────────────────────── */

static int master_on_data(void *ctx, void *data, size_t len)
{
    (void)ctx; (void)len;
    struct repl_observe_event *e = (struct repl_observe_event *)data;

    if (e->len == 0 || e->len > REPL_OBSERVE_MAX_DATA)
        return 0;

    if (!g_repl.kcp)
        return 0;

    /* Send via KCP with length prefix */
    uint32_t frame_len = htonl(e->len);
    ikcp_send(g_repl.kcp, (const char *)&frame_len, REPL_FRAME_HEADER);
    ikcp_send(g_repl.kcp, (const char *)e->data, e->len);

    return 0;
}

/* ── eBPF loader (master only) ──────────────────────────────────────── */

static int master_load_bpf(void)
{
    char obj_path[512];
    snprintf(obj_path, sizeof(obj_path), "%s/repl_filter.bpf.o",
             getenv("BUILD_DIR") ?: "build");

    if (access(obj_path, R_OK) != 0) {
        fprintf(stderr, "[REPL] BPF object not found: %s\n", obj_path);
        return -1;
    }

    g_repl.bpf_obj = bpf_object__open_file(obj_path, NULL);
    if (!g_repl.bpf_obj) {
        fprintf(stderr, "[REPL] bpf_object__open_file: %s\n", strerror(errno));
        return -1;
    }

    if (bpf_object__load(g_repl.bpf_obj)) {
        fprintf(stderr, "[REPL] bpf_object__load failed\n");
        bpf_object__close(g_repl.bpf_obj);
        g_repl.bpf_obj = NULL;
        return -1;
    }

    /* Configure port filter */
    struct bpf_map *cfg_map = bpf_object__find_map_by_name(g_repl.bpf_obj, "repl_config");
    if (cfg_map) {
        uint32_t key = 0, val = g_repl.cfg.kv_port;
        bpf_map__update_elem(cfg_map, &key, sizeof(key), &val, sizeof(val), BPF_ANY);
    }

    /* Attach sockops to cgroup */
    g_repl.cgroup_fd = open("/sys/fs/cgroup", O_RDONLY | O_DIRECTORY);
    if (g_repl.cgroup_fd < 0) {
        fprintf(stderr, "[REPL] open cgroup: %s\n", strerror(errno));
        bpf_object__close(g_repl.bpf_obj);
        g_repl.bpf_obj = NULL;
        return -1;
    }

    struct bpf_program *sockops = bpf_object__find_program_by_name(g_repl.bpf_obj, "repl_filter_sockops");
    if (sockops) {
        bpf_prog_attach(bpf_program__fd(sockops), g_repl.cgroup_fd, BPF_CGROUP_SOCK_OPS, 0);
    }

    /* Attach parser/verdict to sockmap */
    struct bpf_map *sockmap = bpf_object__find_map_by_name(g_repl.bpf_obj, "repl_sockmap");
    if (sockmap) {
        int map_fd = bpf_map__fd(sockmap);
        struct bpf_program *parser = bpf_object__find_program_by_name(g_repl.bpf_obj, "repl_filter_parser");
        if (parser)
            bpf_prog_attach(bpf_program__fd(parser), map_fd, BPF_SK_SKB_STREAM_PARSER, 0);

        struct bpf_program *verdict = bpf_object__find_program_by_name(g_repl.bpf_obj, "repl_filter_verdict");
        if (verdict)
            bpf_prog_attach(bpf_program__fd(verdict), map_fd, BPF_SK_SKB_STREAM_VERDICT, 0);
    }

    /* Setup ringbuf */
    struct bpf_map *rb_map = bpf_object__find_map_by_name(g_repl.bpf_obj, "repl_ringbuf");
    if (!rb_map) {
        fprintf(stderr, "[REPL] ringbuf map not found\n");
        close(g_repl.cgroup_fd);
        bpf_object__close(g_repl.bpf_obj);
        g_repl.bpf_obj = NULL;
        return -1;
    }

    g_repl.ringbuf = ring_buffer__new(bpf_map__fd(rb_map), master_on_data, NULL, NULL);
    if (!g_repl.ringbuf) {
        fprintf(stderr, "[REPL] ring_buffer__new failed\n");
        close(g_repl.cgroup_fd);
        bpf_object__close(g_repl.bpf_obj);
        g_repl.bpf_obj = NULL;
        return -1;
    }

    printf("[REPL] eBPF filter loaded, monitoring port %u\n", g_repl.cfg.kv_port);
    return 0;
}

/* ── UDP socket setup ───────────────────────────────────────────────── */

static int setup_udp_socket(void)
{
    g_repl.udp_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (g_repl.udp_fd < 0) {
        perror("[REPL] socket");
        return -1;
    }

    int flags = fcntl(g_repl.udp_fd, F_GETFL, 0);
    fcntl(g_repl.udp_fd, F_SETFL, flags | O_NONBLOCK);

    /* Slave: use local_port if set, otherwise use repl_port */
    uint16_t bind_port = (g_repl.cfg.role == REPL_ROLE_SLAVE && g_repl.cfg.local_port != 0)
                         ? g_repl.cfg.local_port
                         : g_repl.cfg.repl_port;

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(bind_port),
        .sin_addr.s_addr = INADDR_ANY
    };

    if (bind(g_repl.udp_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("[REPL] bind");
        close(g_repl.udp_fd);
        g_repl.udp_fd = -1;
        return -1;
    }

    printf("[REPL] UDP socket bound to port %u\n", bind_port);
    return 0;
}

#ifdef ENABLE_AF_XDP
/* ── XDP program loader (slave only) ────────────────────────────────── */

static int load_xdp_program(void)
{
    char obj_path[512];
    snprintf(obj_path, sizeof(obj_path), "%s/xdp_kcp_filter.bpf.o",
             getenv("BUILD_DIR") ?: "build");

    if (access(obj_path, R_OK) != 0) {
        fprintf(stderr, "[REPL] XDP object not found: %s\n", obj_path);
        return -1;
    }

    g_repl.xdp_obj = bpf_object__open_file(obj_path, NULL);
    if (!g_repl.xdp_obj) {
        fprintf(stderr, "[REPL] Failed to open XDP object: %s\n", strerror(errno));
        return -1;
    }

    if (bpf_object__load(g_repl.xdp_obj)) {
        fprintf(stderr, "[REPL] Failed to load XDP object\n");
        bpf_object__close(g_repl.xdp_obj);
        g_repl.xdp_obj = NULL;
        return -1;
    }

    /* Find XDP program */
    struct bpf_program *prog = bpf_object__find_program_by_name(g_repl.xdp_obj, "xdp_kcp_filter_prog");
    if (!prog) {
        fprintf(stderr, "[REPL] XDP program 'xdp_kcp_filter_prog' not found\n");
        bpf_object__close(g_repl.xdp_obj);
        g_repl.xdp_obj = NULL;
        return -1;
    }

    g_repl.xdp_prog_fd = bpf_program__fd(prog);

    /* Attach XDP program to interface */
    int ifindex = if_nametoindex(g_repl.cfg.ifname);
    if (ifindex == 0) {
        fprintf(stderr, "[REPL] Interface %s not found\n", g_repl.cfg.ifname);
        bpf_object__close(g_repl.xdp_obj);
        g_repl.xdp_obj = NULL;
        return -1;
    }

    if (bpf_xdp_attach(ifindex, g_repl.xdp_prog_fd, XDP_FLAGS_UPDATE_IF_NOEXIST, NULL) < 0) {
        fprintf(stderr, "[REPL] Failed to attach XDP program: %s\n", strerror(errno));
        bpf_object__close(g_repl.xdp_obj);
        g_repl.xdp_obj = NULL;
        return -1;
    }

    /* Update xsks_map with AF_XDP socket */
    struct bpf_map *xsks_map = bpf_object__find_map_by_name(g_repl.xdp_obj, "xsks_map");
    if (xsks_map && g_repl.xsk) {
        int map_fd = bpf_map__fd(xsks_map);
        int xsk_fd = xsk_socket__fd(g_repl.xsk->xsk);
        uint32_t key = g_repl.cfg.queue_id;
        if (bpf_map_update_elem(map_fd, &key, &xsk_fd, BPF_ANY) < 0) {
            fprintf(stderr, "[REPL] Failed to update xsks_map: %s\n", strerror(errno));
        }
    }

    printf("[REPL] XDP program attached to %s\n", g_repl.cfg.ifname);
    return 0;
}

/* ── AF_XDP socket setup ────────────────────────────────────────────── */

static int setup_af_xdp_socket(void)
{
    int ret = af_xdp_socket_create(&g_repl.xsk,
                                   g_repl.cfg.ifname,
                                   g_repl.cfg.queue_id,
                                   NULL);
    if (ret < 0) {
        fprintf(stderr, "[REPL] Failed to create AF_XDP socket\n");
        return -1;
    }

    printf("[REPL] AF_XDP socket created on %s queue %d\n",
           g_repl.cfg.ifname, g_repl.cfg.queue_id);
    return 0;
}
#endif /* ENABLE_AF_XDP */

/* ── Slave: receive and apply commands ──────────────────────────────── */

static int slave_apply_command(const uint8_t *data, uint32_t len)
{
    /* Connect to local kvstore and send command */
    static int kv_fd = -1;

    if (kv_fd < 0) {
        kv_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (kv_fd < 0) return -1;

        struct sockaddr_in addr = {
            .sin_family = AF_INET,
            .sin_port = htons(g_repl.cfg.kv_port),
            .sin_addr.s_addr = htonl(INADDR_LOOPBACK)
        };

        if (connect(kv_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            close(kv_fd);
            kv_fd = -1;
            return -1;
        }
    }

    ssize_t n = write(kv_fd, data, len);
    if (n < 0) {
        close(kv_fd);
        kv_fd = -1;
        return -1;
    }

    return 0;
}

/* ── Main replication thread ────────────────────────────────────────── */

static void *repl_thread_func(void *arg)
{
    (void)arg;

    uint8_t recv_buf[REPL_OBSERVE_MAX_DATA];
    uint8_t frame_buf[REPL_OBSERVE_MAX_DATA];
    uint32_t frame_len = 0;
    int frame_state = 0;  /* 0=waiting header, 1=waiting data */

    printf("[REPL] thread started, role=%s\n",
           g_repl.cfg.role == REPL_ROLE_MASTER ? "master" : "slave");

    /* Slave: setup peer address */
    if (g_repl.cfg.role == REPL_ROLE_SLAVE) {
        g_repl.peer_addr.sin_family = AF_INET;
        g_repl.peer_addr.sin_port = htons(g_repl.cfg.repl_port);
        inet_pton(AF_INET, g_repl.cfg.master_ip, &g_repl.peer_addr.sin_addr);

        /* Send initial handshake */
        const char *hello = "REPL_HELLO";
        sendto(g_repl.udp_fd, hello, strlen(hello), 0,
               (struct sockaddr *)&g_repl.peer_addr, sizeof(g_repl.peer_addr));
    }

    uint32_t last_update = 0;

    while (g_repl.running) {
        uint32_t now = (uint32_t)time(NULL);  /* Use time() instead of ikcp_getconv */

        /* KCP update (every 10ms) */
        if (now - last_update >= REPL_KCP_INTERVAL) {
            if (g_repl.kcp)
                ikcp_update(g_repl.kcp, now * 1000);  /* KCP expects milliseconds */
            last_update = now;
        }

        /* Master: poll eBPF ringbuf */
        if (g_repl.cfg.role == REPL_ROLE_MASTER && g_repl.ringbuf) {
            ring_buffer__poll(g_repl.ringbuf, 10);
        }

        /* Receive packets (AF_XDP or UDP) */
#ifdef ENABLE_AF_XDP
        if (g_repl.transport == TRANSPORT_AF_XDP && g_repl.xsk) {
            /* AF_XDP receive */
            size_t recv_len = sizeof(recv_buf);
            ssize_t n = af_xdp_recv(g_repl.xsk, recv_buf, &recv_len);

            if (n > 0) {
                pthread_mutex_lock(&g_repl.stats_lock);
                g_repl.stats.bytes_recv += n;
                g_repl.stats.pkts_recv++;
                pthread_mutex_unlock(&g_repl.stats_lock);

                /* Feed to KCP */
                if (g_repl.kcp)
                    ikcp_input(g_repl.kcp, (const char *)recv_buf, n);
            }
        } else
#endif
        {
            /* UDP receive */
            struct sockaddr_in from;
            socklen_t fromlen = sizeof(from);
            ssize_t n = recvfrom(g_repl.udp_fd, recv_buf, sizeof(recv_buf), 0,
                                (struct sockaddr *)&from, &fromlen);

            if (n > 0) {
                pthread_mutex_lock(&g_repl.stats_lock);
                g_repl.stats.bytes_recv += n;
                g_repl.stats.pkts_recv++;
                pthread_mutex_unlock(&g_repl.stats_lock);

                /* Master: record slave address on first packet */
                if (g_repl.cfg.role == REPL_ROLE_MASTER && g_repl.peer_addr.sin_port == 0) {
                    memcpy(&g_repl.peer_addr, &from, sizeof(from));
                    printf("[REPL] slave connected from %s:%u\n",
                           inet_ntoa(from.sin_addr), ntohs(from.sin_port));
                }

                /* Feed to KCP */
                if (g_repl.kcp)
                    ikcp_input(g_repl.kcp, (const char *)recv_buf, n);
            }
        }

        /* Slave: receive KCP data and apply commands */
        if (g_repl.cfg.role == REPL_ROLE_SLAVE && g_repl.kcp) {
            while (1) {
                int kcp_len = ikcp_recv(g_repl.kcp, (char *)recv_buf, sizeof(recv_buf));
                if (kcp_len <= 0) break;

                /* Parse framed commands */
                for (int i = 0; i < kcp_len; ) {
                    if (frame_state == 0) {
                        /* Read frame header */
                        if (i + REPL_FRAME_HEADER > kcp_len) break;
                        memcpy(&frame_len, recv_buf + i, REPL_FRAME_HEADER);
                        frame_len = ntohl(frame_len);
                        i += REPL_FRAME_HEADER;
                        frame_state = 1;
                    } else {
                        /* Read frame data */
                        int remain = kcp_len - i;
                        int need = frame_len;
                        int copy = remain < need ? remain : need;
                        memcpy(frame_buf, recv_buf + i, copy);
                        i += copy;

                        if (copy == need) {
                            slave_apply_command(frame_buf, frame_len);
                            frame_state = 0;
                        }
                    }
                }
            }
        }

        usleep(1000);  /* 1ms sleep */
    }

    printf("[REPL] thread stopped\n");
    return NULL;
}

/* ── Public API ─────────────────────────────────────────────────────── */

int repl_kcp_init(const struct repl_kcp_config *cfg)
{
    if (!cfg || cfg->role == REPL_ROLE_NONE)
        return -1;

    memcpy(&g_repl.cfg, cfg, sizeof(*cfg));
    g_repl.running = 1;
    g_repl.conv_id = 0x12345678;
    g_repl.transport = TRANSPORT_UDP;

    pthread_mutex_init(&g_repl.stats_lock, NULL);

#ifdef ENABLE_AF_XDP
    if (cfg->use_af_xdp) {
        printf("[REPL] Attempting AF_XDP mode on %s...\n", cfg->ifname);

        if (setup_af_xdp_socket() == 0) {
            if (cfg->role == REPL_ROLE_SLAVE) {
                if (load_xdp_program() == 0) {
                    g_repl.transport = TRANSPORT_AF_XDP;
                    printf("[REPL] AF_XDP mode enabled (slave with XDP filter)\n");
                } else {
                    fprintf(stderr, "[REPL] XDP program load failed, falling back to UDP\n");
                    af_xdp_socket_destroy(g_repl.xsk);
                    g_repl.xsk = NULL;
                }
            } else {
                g_repl.transport = TRANSPORT_AF_XDP;
                printf("[REPL] AF_XDP mode enabled (master TX only)\n");
            }
        } else {
            fprintf(stderr, "[REPL] AF_XDP socket creation failed, falling back to UDP\n");
        }
    }
#endif

    if (setup_udp_socket() != 0) {
        fprintf(stderr, "[REPL] UDP socket setup failed\n");
#ifdef ENABLE_AF_XDP
        if (g_repl.xsk) af_xdp_socket_destroy(g_repl.xsk);
        if (g_repl.xdp_obj) bpf_object__close(g_repl.xdp_obj);
#endif
        return -1;
    }

    g_repl.kcp = ikcp_create(g_repl.conv_id, NULL);
    if (!g_repl.kcp) {
        fprintf(stderr, "[REPL] ikcp_create failed\n");
        close(g_repl.udp_fd);
#ifdef ENABLE_AF_XDP
        if (g_repl.xsk) af_xdp_socket_destroy(g_repl.xsk);
        if (g_repl.xdp_obj) bpf_object__close(g_repl.xdp_obj);
#endif
        return -1;
    }

    ikcp_setoutput(g_repl.kcp, kcp_output);
    ikcp_wndsize(g_repl.kcp, REPL_KCP_WND, REPL_KCP_WND);
    ikcp_nodelay(g_repl.kcp, 1, 10, 2, 1);
    g_repl.kcp->rx_minrto = 10;
    g_repl.kcp->interval = REPL_KCP_INTERVAL;

    if (cfg->role == REPL_ROLE_MASTER) {
        if (master_load_bpf() != 0) {
            fprintf(stderr, "[REPL] Warning: eBPF filter failed, using fallback mode\n");
        }
    }

    if (pthread_create(&g_repl.thread, NULL, repl_thread_func, NULL) != 0) {
        fprintf(stderr, "[REPL] pthread_create failed\n");
        if (g_repl.ringbuf) ring_buffer__free(g_repl.ringbuf);
        if (g_repl.bpf_obj) bpf_object__close(g_repl.bpf_obj);
        if (g_repl.cgroup_fd >= 0) close(g_repl.cgroup_fd);
        ikcp_release(g_repl.kcp);
        close(g_repl.udp_fd);
#ifdef ENABLE_AF_XDP
        if (g_repl.xsk) af_xdp_socket_destroy(g_repl.xsk);
        if (g_repl.xdp_obj) bpf_object__close(g_repl.xdp_obj);
#endif
        return -1;
    }

    printf("[REPL] initialized, role=%s port=%u transport=%s\n",
           cfg->role == REPL_ROLE_MASTER ? "master" : "slave",
           cfg->repl_port,
           g_repl.transport == TRANSPORT_AF_XDP ? "AF_XDP" : "UDP");

    return 0;
}

void repl_kcp_shutdown(void)
{
    if (!g_repl.running)
        return;

    g_repl.running = 0;
    pthread_join(g_repl.thread, NULL);

    if (g_repl.ringbuf)
        ring_buffer__free(g_repl.ringbuf);

    if (g_repl.bpf_obj)
        bpf_object__close(g_repl.bpf_obj);

    if (g_repl.cgroup_fd >= 0)
        close(g_repl.cgroup_fd);

#ifdef ENABLE_AF_XDP
    if (g_repl.xsk)
        af_xdp_socket_destroy(g_repl.xsk);

    if (g_repl.xdp_obj)
        bpf_object__close(g_repl.xdp_obj);
#endif

    if (g_repl.kcp)
        ikcp_release(g_repl.kcp);

    if (g_repl.udp_fd >= 0)
        close(g_repl.udp_fd);

    pthread_mutex_destroy(&g_repl.stats_lock);

    printf("[REPL] shutdown complete\n");
}

void repl_kcp_update(void)
{
    /* Called from main event loop if needed */
}

void repl_kcp_get_stats(struct repl_kcp_stats *stats)
{
    pthread_mutex_lock(&g_repl.stats_lock);
    memcpy(stats, &g_repl.stats, sizeof(*stats));
    pthread_mutex_unlock(&g_repl.stats_lock);
}
