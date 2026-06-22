/* repl_tcp.c — TCP transport + heartbeat + public API
 *
 * This file contains:
 *   1. Global state (g_repl, g_reassembly)
 *   2. TCP helpers (set_options, listen, connect, send_frame, recv_exact)
 *   3. Heartbeat (ping, timeout, thread)
 *   4. Public API (init, shutdown, get_stats)
 *
 * Protocol types: see repl_tcp_internal.h
 * Master logic:  see repl_tcp_master.c
 * Slave logic:   see repl_tcp_slave.c
 */
#define _GNU_SOURCE
#include "repl_tcp_internal.h"

/* ── Global state ───────────────────────────────────────────────────── */

struct repl_tcp_global g_repl = {0};

/* ── TCP helpers ────────────────────────────────────────────────────── */

int tcp_set_options(int fd)
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

int tcp_listen(uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("[REPL-TCP] socket"); return -1; }

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr.s_addr = INADDR_ANY
    };
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("[REPL-TCP] bind"); close(fd); return -1;
    }
    if (listen(fd, 5) < 0) {
        perror("[REPL-TCP] listen"); close(fd); return -1;
    }
    printf("[REPL-TCP] Listening on port %u\n", port);
    return fd;
}

int tcp_connect(const char *ip, uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("[REPL-TCP] socket"); return -1; }
    tcp_set_options(fd);

    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(port) };
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "[REPL-TCP] Invalid IP: %s\n", ip);
        close(fd); return -1;
    }
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("[REPL-TCP] connect"); close(fd); return -1;
    }
    printf("[REPL-TCP] Connected to %s:%u\n", ip, port);
    return fd;
}

int tcp_send_frame(int fd, const void *data, uint32_t len)
{
    if (fd < 0 || len > REPL_MAX_FRAME_SIZE) return -1;

    uint32_t net_len = htonl(len);
    if (send(fd, &net_len, REPL_FRAME_HEADER, MSG_NOSIGNAL) != REPL_FRAME_HEADER)
        return -1;

    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, (const char *)data + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) return -1;
        sent += n;
    }
    return 0;
}

int tcp_send_data_frame(int fd, const void *data, uint32_t len)
{
    if (fd < 0 || len > REPL_MAX_FRAME_SIZE) return -1;

    struct ctrl_msg_header hdr = {
        .magic = htonl(CTRL_MAGIC),
        .type = htonl(CTRL_MSG_DATA_FRAME),
        .timestamp = htobe64(time(NULL) * 1000),
    };
    uint32_t net_len = htonl(len);
    struct iovec iov[3] = {
        {.iov_base = &hdr,     .iov_len = sizeof(hdr)},
        {.iov_base = &net_len, .iov_len = REPL_FRAME_HEADER},
        {.iov_base = (void *)data, .iov_len = len},
    };
    struct msghdr msg = {.msg_iov = iov, .msg_iovlen = 3};
    if (sendmsg(fd, &msg, MSG_NOSIGNAL) != (ssize_t)(sizeof(hdr) + REPL_FRAME_HEADER + len))
        return -1;
    return 0;
}

int tcp_recv_exact(int fd, void *buf, size_t len)
{
    size_t received = 0;
    while (received < len) {
        ssize_t n = recv(fd, (char *)buf + received, len - received, 0);
        if (n <= 0) return -1;
        received += n;
    }
    return 0;
}

/* ── Heartbeat helpers ──────────────────────────────────────────────── */

int send_heartbeat_ping(int fd) {
    struct heartbeat_msg msg;
    msg.hdr.magic = htonl(CTRL_MAGIC);
    msg.hdr.type = htonl(CTRL_MSG_PING);
    msg.hdr.timestamp = htobe64(time(NULL) * 1000);
    msg.master_offset = htobe64(repl_incr_get_master_offset());

    ssize_t n = send(fd, &msg, sizeof(msg), MSG_NOSIGNAL);
    return (n == sizeof(msg)) ? 0 : -1;
}

void update_last_heartbeat(void) {
    pthread_mutex_lock(&g_repl.heartbeat_lock);
    g_repl.last_heartbeat = time(NULL);
    pthread_mutex_unlock(&g_repl.heartbeat_lock);
}

int check_heartbeat_timeout(void) {
    pthread_mutex_lock(&g_repl.heartbeat_lock);
    time_t now = time(NULL);
    int timeout = (now - g_repl.last_heartbeat > HEARTBEAT_TIMEOUT);
    pthread_mutex_unlock(&g_repl.heartbeat_lock);
    return timeout;
}

/* ── Heartbeat thread (master only) ─────────────────────────────────── */

void *heartbeat_thread_func(void *arg) {
    (void)arg;
    printf("[REPL-TCP] Heartbeat thread started\n");

    while (g_repl.running) {
        sleep(HEARTBEAT_INTERVAL);
        if (g_repl.peer_fd < 0) continue;

        if (check_heartbeat_timeout()) {
            fprintf(stderr, "[REPL-TCP] Heartbeat timeout, closing connection\n");
            close(g_repl.peer_fd);
            g_repl.peer_fd = -1;
            pthread_mutex_lock(&g_repl.stats_lock);
            g_repl.stats.connected = 0;
            pthread_mutex_unlock(&g_repl.stats_lock);
            continue;
        }

        if (send_heartbeat_ping(g_repl.peer_fd) < 0) {
            fprintf(stderr, "[REPL-TCP] Failed to send PING, closing connection\n");
            close(g_repl.peer_fd);
            g_repl.peer_fd = -1;
            pthread_mutex_lock(&g_repl.stats_lock);
            g_repl.stats.connected = 0;
            pthread_mutex_unlock(&g_repl.stats_lock);
        } else {
            update_last_heartbeat();
        }
    }

    printf("[REPL-TCP] Heartbeat thread stopped\n");
    return NULL;
}

/* ── Public API ─────────────────────────────────────────────────────── */

int repl_tcp_init(const struct repl_tcp_config *cfg)
{
    if (!cfg || cfg->role == REPL_ROLE_NONE) return -1;

    memcpy(&g_repl.cfg, cfg, sizeof(*cfg));
    g_repl.running = 1;
    g_repl.peer_fd = -1;
    g_repl.kv_fd = -1;
    g_repl.listen_fd = -1;

    if (g_repl.cfg.enable_nodelay == 0)  g_repl.cfg.enable_nodelay = 1;
    if (g_repl.cfg.sndbuf_size == 0)     g_repl.cfg.sndbuf_size = 2 * 1024 * 1024;
    if (g_repl.cfg.rcvbuf_size == 0)     g_repl.cfg.rcvbuf_size = 2 * 1024 * 1024;

    pthread_mutex_init(&g_repl.stats_lock, NULL);
    pthread_mutex_init(&g_repl.heartbeat_lock, NULL);
    g_repl.last_heartbeat = time(NULL);

    if (cfg->role == REPL_ROLE_MASTER) {
        g_repl.listen_fd = tcp_listen(cfg->repl_port);
        if (g_repl.listen_fd < 0) {
            fprintf(stderr, "[REPL-TCP] Failed to create listen socket\n");
            return -1;
        }

        if (master_load_bpf() != 0)
            fprintf(stderr, "[REPL-TCP] Warning: eBPF filter failed, using fallback mode\n");

        if (pthread_create(&g_repl.thread, NULL, master_thread_func, NULL) != 0) {
            fprintf(stderr, "[REPL-TCP] pthread_create failed\n");
            if (g_repl.ringbuf) ring_buffer__free(g_repl.ringbuf);
            if (g_repl.bpf_obj) bpf_object__close(g_repl.bpf_obj);
            if (g_repl.cgroup_fd >= 0) close(g_repl.cgroup_fd);
            close(g_repl.listen_fd);
            return -1;
        }

        if (pthread_create(&g_repl.heartbeat_thread, NULL, heartbeat_thread_func, NULL) != 0) {
            fprintf(stderr, "[REPL-TCP] Failed to create heartbeat thread\n");
            g_repl.running = 0;
            pthread_join(g_repl.thread, NULL);
            if (g_repl.ringbuf) ring_buffer__free(g_repl.ringbuf);
            if (g_repl.bpf_obj) bpf_object__close(g_repl.bpf_obj);
            if (g_repl.cgroup_fd >= 0) close(g_repl.cgroup_fd);
            close(g_repl.listen_fd);
            return -1;
        }

        printf("[REPL-TCP] Master initialized on port %u\n", cfg->repl_port);
    } else {
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
    if (!g_repl.running) return;

    g_repl.running = 0;
    pthread_join(g_repl.thread, NULL);

    if (g_repl.cfg.role == REPL_ROLE_MASTER)
        pthread_join(g_repl.heartbeat_thread, NULL);

    if (g_repl.ringbuf)       ring_buffer__free(g_repl.ringbuf);
    if (g_repl.bpf_obj)       bpf_object__close(g_repl.bpf_obj);
    if (g_repl.cgroup_fd >= 0) close(g_repl.cgroup_fd);
    if (g_repl.listen_fd >= 0) close(g_repl.listen_fd);
    if (g_repl.peer_fd >= 0)   close(g_repl.peer_fd);
    if (g_repl.kv_fd >= 0)     close(g_repl.kv_fd);

    pthread_mutex_destroy(&g_repl.stats_lock);
    pthread_mutex_destroy(&g_repl.heartbeat_lock);

    printf("[REPL-TCP] Shutdown complete\n");
}

void repl_tcp_get_stats(struct repl_tcp_stats *stats)
{
    pthread_mutex_lock(&g_repl.stats_lock);
    memcpy(stats, &g_repl.stats, sizeof(*stats));
    pthread_mutex_unlock(&g_repl.stats_lock);
}
