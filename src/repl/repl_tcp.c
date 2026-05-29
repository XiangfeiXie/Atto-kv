/* repl_tcp.c — TCP-based replication transport layer
 *
 * Master flow:
 *   1. Load eBPF filter (repl_filter.bpf.o) → captures write commands
 *   2. Poll ringbuf → send via TCP with length framing
 *   3. Accept slave connections on repl_port
 *
 * Slave flow:
 *   1. Connect to master via TCP
 *   2. Receive length-framed commands
 *   3. Write commands to local kvstore socket
 *   4. Auto-reconnect on disconnect
 */
#define _GNU_SOURCE
#include "repl_tcp.h"
#include "repl_incr.h"

#include <arpa/inet.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* ── eBPF event structure (from repl_observe.h) ─────────────────────── */

#define REPL_OBSERVE_MAX_DATA  512
#define REPL_OBSERVE_MAX_SOCKS 65536
#define REPL_OBSERVE_RINGBUF_SZ (2 * 1024 * 1024)

struct repl_observe_event {
    uint32_t len;
    uint32_t padding;
    uint8_t  data[REPL_OBSERVE_MAX_DATA];
};

/* ── Constants ──────────────────────────────────────────────────────── */

#define REPL_FRAME_HEADER   4
#define REPL_MAX_FRAME_SIZE (64 * 1024)  /* 64KB max frame */
#define RECONNECT_INTERVAL  3            /* seconds */

/* ── Global state ───────────────────────────────────────────────────── */

static struct {
    struct repl_tcp_config cfg;
    pthread_t              thread;
    volatile int           running;

    /* eBPF state (master only) */
    struct bpf_object     *bpf_obj;
    struct ring_buffer    *ringbuf;
    int                    cgroup_fd;

    /* TCP state */
    int                    listen_fd;   /* master: listen socket */
    int                    peer_fd;     /* connected peer socket */
    int                    kv_fd;       /* slave: local kvstore connection */

    /* Statistics */
    struct repl_tcp_stats  stats;
    pthread_mutex_t        stats_lock;

} g_repl = {0};

/* ── TCP helpers ────────────────────────────────────────────────────── */

static int tcp_set_options(int fd)
{
    if (g_repl.cfg.enable_nodelay) {
        int opt = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    }

    if (g_repl.cfg.sndbuf_size > 0) {
        setsockopt(fd, SOL_SOCKET, SO_SNDBUF,
                   &g_repl.cfg.sndbuf_size, sizeof(g_repl.cfg.sndbuf_size));
    }

    if (g_repl.cfg.rcvbuf_size > 0) {
        setsockopt(fd, SOL_SOCKET, SO_RCVBUF,
                   &g_repl.cfg.rcvbuf_size, sizeof(g_repl.cfg.rcvbuf_size));
    }

    return 0;
}

static int tcp_listen(uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("[REPL-TCP] socket");
        return -1;
    }

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr.s_addr = INADDR_ANY
    };

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("[REPL-TCP] bind");
        close(fd);
        return -1;
    }

    if (listen(fd, 5) < 0) {
        perror("[REPL-TCP] listen");
        close(fd);
        return -1;
    }

    printf("[REPL-TCP] Listening on port %u\n", port);
    return fd;
}

static int tcp_connect(const char *ip, uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("[REPL-TCP] socket");
        return -1;
    }

    tcp_set_options(fd);

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port)
    };

    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "[REPL-TCP] Invalid IP: %s\n", ip);
        close(fd);
        return -1;
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("[REPL-TCP] connect");
        close(fd);
        return -1;
    }

    printf("[REPL-TCP] Connected to %s:%u\n", ip, port);
    return fd;
}

static int tcp_send_frame(int fd, const void *data, uint32_t len)
{
    if (fd < 0 || len > REPL_MAX_FRAME_SIZE)
        return -1;

    /* Send length header (network byte order) */
    uint32_t net_len = htonl(len);
    ssize_t n = send(fd, &net_len, REPL_FRAME_HEADER, MSG_NOSIGNAL);
    if (n != REPL_FRAME_HEADER)
        return -1;

    /* Send data */
    size_t sent = 0;
    while (sent < len) {
        n = send(fd, (const char *)data + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0)
            return -1;
        sent += n;
    }

    return 0;
}

static int tcp_recv_exact(int fd, void *buf, size_t len)
{
    size_t received = 0;
    while (received < len) {
        ssize_t n = recv(fd, (char *)buf + received, len - received, 0);
        if (n <= 0)
            return -1;
        received += n;
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

    /* Append to backlog for incremental recovery */
    repl_incr_append((const char *)e->data, e->len);

    if (g_repl.peer_fd < 0)
        return 0;  /* No slave connected */

    /* Send frame to slave */
    if (tcp_send_frame(g_repl.peer_fd, e->data, e->len) == 0) {
        pthread_mutex_lock(&g_repl.stats_lock);
        g_repl.stats.bytes_sent += e->len + REPL_FRAME_HEADER;
        g_repl.stats.frames_sent++;
        pthread_mutex_unlock(&g_repl.stats_lock);
    } else {
        /* Connection lost */
        close(g_repl.peer_fd);
        g_repl.peer_fd = -1;
        pthread_mutex_lock(&g_repl.stats_lock);
        g_repl.stats.connected = 0;
        pthread_mutex_unlock(&g_repl.stats_lock);
        printf("[REPL-TCP] Slave disconnected\n");
    }

    return 0;
}

/* ── eBPF loader (master only) ──────────────────────────────────────── */

static int master_load_bpf(void)
{
    char obj_path[512];
    snprintf(obj_path, sizeof(obj_path), "%s/repl_filter.bpf.o",
             getenv("BUILD_DIR") ?: "build");

    if (access(obj_path, R_OK) != 0) {
        fprintf(stderr, "[REPL-TCP] BPF object not found: %s\n", obj_path);
        return -1;
    }

    g_repl.bpf_obj = bpf_object__open_file(obj_path, NULL);
    if (!g_repl.bpf_obj) {
        fprintf(stderr, "[REPL-TCP] bpf_object__open_file: %s\n", strerror(errno));
        return -1;
    }

    if (bpf_object__load(g_repl.bpf_obj)) {
        fprintf(stderr, "[REPL-TCP] bpf_object__load failed\n");
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
        fprintf(stderr, "[REPL-TCP] open cgroup: %s\n", strerror(errno));
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
        fprintf(stderr, "[REPL-TCP] ringbuf map not found\n");
        close(g_repl.cgroup_fd);
        bpf_object__close(g_repl.bpf_obj);
        g_repl.bpf_obj = NULL;
        return -1;
    }

    g_repl.ringbuf = ring_buffer__new(bpf_map__fd(rb_map), master_on_data, NULL, NULL);
    if (!g_repl.ringbuf) {
        fprintf(stderr, "[REPL-TCP] ring_buffer__new failed\n");
        close(g_repl.cgroup_fd);
        bpf_object__close(g_repl.bpf_obj);
        g_repl.bpf_obj = NULL;
        return -1;
    }

    printf("[REPL-TCP] eBPF filter loaded, monitoring port %u\n", g_repl.cfg.kv_port);
    return 0;
}

/* ── Slave: receive and apply commands ──────────────────────────────── */

static int slave_apply_command(const uint8_t *data, uint32_t len)
{
    /* Connect to local kvstore if not connected */
    if (g_repl.kv_fd < 0) {
        g_repl.kv_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (g_repl.kv_fd < 0)
            return -1;

        struct sockaddr_in addr = {
            .sin_family = AF_INET,
            .sin_port = htons(g_repl.cfg.kv_port),
            .sin_addr.s_addr = htonl(INADDR_LOOPBACK)
        };

        if (connect(g_repl.kv_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            close(g_repl.kv_fd);
            g_repl.kv_fd = -1;
            return -1;
        }
    }

    /* Write command to local kvstore */
    ssize_t n = send(g_repl.kv_fd, data, len, MSG_NOSIGNAL);
    if (n < 0) {
        close(g_repl.kv_fd);
        g_repl.kv_fd = -1;
        return -1;
    }

    return 0;
}

/* ── Master thread ──────────────────────────────────────────────────── */

static void *master_thread_func(void *arg)
{
    (void)arg;

    printf("[REPL-TCP] Master thread started\n");

    /* Set listen socket to non-blocking */
    int flags = fcntl(g_repl.listen_fd, F_GETFL, 0);
    fcntl(g_repl.listen_fd, F_SETFL, flags | O_NONBLOCK);

    while (g_repl.running) {
        /* Poll eBPF ringbuf for write commands */
        if (g_repl.ringbuf) {
            ring_buffer__poll(g_repl.ringbuf, 1);  /* 1ms timeout */
        }

        /* Accept new slave connections */
        if (g_repl.peer_fd < 0) {
            struct sockaddr_in slave_addr;
            socklen_t addrlen = sizeof(slave_addr);
            int fd = accept(g_repl.listen_fd, (struct sockaddr *)&slave_addr, &addrlen);

            if (fd >= 0) {
                tcp_set_options(fd);
                g_repl.peer_fd = fd;

                pthread_mutex_lock(&g_repl.stats_lock);
                g_repl.stats.connected = 1;
                pthread_mutex_unlock(&g_repl.stats_lock);

                printf("[REPL-TCP] Slave connected from %s:%u\n",
                       inet_ntoa(slave_addr.sin_addr), ntohs(slave_addr.sin_port));
            }
        }

        usleep(1000);  /* 1ms sleep */
    }

    printf("[REPL-TCP] Master thread stopped\n");
    return NULL;
}

/* ── Slave thread ───────────────────────────────────────────────────── */

static void *slave_thread_func(void *arg)
{
    (void)arg;

    printf("[REPL-TCP] Slave thread started\n");

    uint8_t frame_buf[REPL_MAX_FRAME_SIZE];

    while (g_repl.running) {
        /* Connect to master if not connected */
        if (g_repl.peer_fd < 0) {
            printf("[REPL-TCP] Connecting to master %s:%u...\n",
                   g_repl.cfg.master_ip, g_repl.cfg.repl_port);

            g_repl.peer_fd = tcp_connect(g_repl.cfg.master_ip, g_repl.cfg.repl_port);

            if (g_repl.peer_fd >= 0) {
                pthread_mutex_lock(&g_repl.stats_lock);
                g_repl.stats.connected = 1;
                g_repl.stats.reconnect_count++;
                pthread_mutex_unlock(&g_repl.stats_lock);
            } else {
                sleep(RECONNECT_INTERVAL);
                continue;
            }
        }

        /* Receive frame header */
        uint32_t frame_len;
        if (tcp_recv_exact(g_repl.peer_fd, &frame_len, REPL_FRAME_HEADER) != 0) {
            close(g_repl.peer_fd);
            g_repl.peer_fd = -1;
            pthread_mutex_lock(&g_repl.stats_lock);
            g_repl.stats.connected = 0;
            pthread_mutex_unlock(&g_repl.stats_lock);
            printf("[REPL-TCP] Connection lost, reconnecting...\n");
            continue;
        }

        frame_len = ntohl(frame_len);
        if (frame_len == 0 || frame_len > REPL_MAX_FRAME_SIZE) {
            fprintf(stderr, "[REPL-TCP] Invalid frame length: %u\n", frame_len);
            close(g_repl.peer_fd);
            g_repl.peer_fd = -1;
            pthread_mutex_lock(&g_repl.stats_lock);
            g_repl.stats.connected = 0;
            pthread_mutex_unlock(&g_repl.stats_lock);
            continue;
        }

        /* Receive frame data */
        if (tcp_recv_exact(g_repl.peer_fd, frame_buf, frame_len) != 0) {
            close(g_repl.peer_fd);
            g_repl.peer_fd = -1;
            pthread_mutex_lock(&g_repl.stats_lock);
            g_repl.stats.connected = 0;
            pthread_mutex_unlock(&g_repl.stats_lock);
            printf("[REPL-TCP] Connection lost, reconnecting...\n");
            continue;
        }

        /* Update statistics */
        pthread_mutex_lock(&g_repl.stats_lock);
        g_repl.stats.bytes_recv += frame_len + REPL_FRAME_HEADER;
        g_repl.stats.frames_recv++;
        g_repl.stats.slave_offset += frame_len;
        pthread_mutex_unlock(&g_repl.stats_lock);

        /* Apply command to local kvstore */
        slave_apply_command(frame_buf, frame_len);

        /* Acknowledge offset to master every 100 frames */
        if (g_repl.stats.frames_recv % 100 == 0) {
            repl_incr_ack_offset(g_repl.stats.slave_offset);
        }
    }

    printf("[REPL-TCP] Slave thread stopped\n");
    return NULL;
}

/* ── Public API ─────────────────────────────────────────────────────── */

int repl_tcp_init(const struct repl_tcp_config *cfg)
{
    if (!cfg || cfg->role == REPL_ROLE_NONE)
        return -1;

    memcpy(&g_repl.cfg, cfg, sizeof(*cfg));
    g_repl.running = 1;
    g_repl.peer_fd = -1;
    g_repl.kv_fd = -1;
    g_repl.listen_fd = -1;

    /* Set default TCP options */
    if (g_repl.cfg.enable_nodelay == 0)
        g_repl.cfg.enable_nodelay = 1;  /* Enable by default */

    /* Set larger buffer sizes for better throughput */
    if (g_repl.cfg.sndbuf_size == 0)
        g_repl.cfg.sndbuf_size = 2 * 1024 * 1024;  /* 2MB send buffer */
    if (g_repl.cfg.rcvbuf_size == 0)
        g_repl.cfg.rcvbuf_size = 2 * 1024 * 1024;  /* 2MB recv buffer */

    pthread_mutex_init(&g_repl.stats_lock, NULL);

    if (cfg->role == REPL_ROLE_MASTER) {
        /* Master: setup listen socket */
        g_repl.listen_fd = tcp_listen(cfg->repl_port);
        if (g_repl.listen_fd < 0) {
            fprintf(stderr, "[REPL-TCP] Failed to create listen socket\n");
            return -1;
        }

        /* Load eBPF filter */
        if (master_load_bpf() != 0) {
            fprintf(stderr, "[REPL-TCP] Warning: eBPF filter failed, using fallback mode\n");
        }

        /* Start master thread */
        if (pthread_create(&g_repl.thread, NULL, master_thread_func, NULL) != 0) {
            fprintf(stderr, "[REPL-TCP] pthread_create failed\n");
            if (g_repl.ringbuf) ring_buffer__free(g_repl.ringbuf);
            if (g_repl.bpf_obj) bpf_object__close(g_repl.bpf_obj);
            if (g_repl.cgroup_fd >= 0) close(g_repl.cgroup_fd);
            close(g_repl.listen_fd);
            return -1;
        }

        printf("[REPL-TCP] Master initialized on port %u\n", cfg->repl_port);
    } else {
        /* Slave: start slave thread */
        if (pthread_create(&g_repl.thread, NULL, slave_thread_func, NULL) != 0) {
            fprintf(stderr, "[REPL-TCP] pthread_create failed\n");
            return -1;
        }

        printf("[REPL-TCP] Slave initialized, connecting to %s:%u\n",
               cfg->master_ip, cfg->repl_port);
    }

    return 0;
}

void repl_tcp_shutdown(void)
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

    if (g_repl.listen_fd >= 0)
        close(g_repl.listen_fd);

    if (g_repl.peer_fd >= 0)
        close(g_repl.peer_fd);

    if (g_repl.kv_fd >= 0)
        close(g_repl.kv_fd);

    pthread_mutex_destroy(&g_repl.stats_lock);

    printf("[REPL-TCP] Shutdown complete\n");
}

void repl_tcp_get_stats(struct repl_tcp_stats *stats)
{
    pthread_mutex_lock(&g_repl.stats_lock);
    memcpy(stats, &g_repl.stats, sizeof(*stats));
    pthread_mutex_unlock(&g_repl.stats_lock);
}
