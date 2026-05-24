#define _GNU_SOURCE
#include "tcp_xdp_repl.h"
#include "repl_backlog.h"
#include "xdp_repl_manager.h"
#include "kvstore.h"
#include "config.h"
#include "protocol.h"
#include "dynbuf.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <stdatomic.h>

/* ===================================================================
 * Global context
 * =================================================================== */

typedef struct {
    int tcp_fd;
    int listen_fd;
    uint64_t master_offset;
    uint64_t slave_ack_offset;
    time_t last_ping;
    pthread_t accept_thread;
    pthread_t send_thread;
    pthread_t recv_thread;
    _Atomic int running;
    _Atomic int is_master;
    _Atomic int slave_connected;
} tcp_xdp_repl_ctx_t;

static tcp_xdp_repl_ctx_t g_ctx;

/* ===================================================================
 * TCP socket optimization
 * =================================================================== */

static int optimize_tcp_socket(int fd) {
    int flag = 1;

    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    int sndbuf = 4 * 1024 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

    int rcvbuf = 4 * 1024 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &flag, sizeof(flag));

    /* Set receive timeout to allow graceful shutdown */
    struct timeval tv;
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    return 0;
}

/* ===================================================================
 * Frame send/recv helpers
 * =================================================================== */

static int send_frame(int fd, uint8_t type, const char *payload, uint32_t len) {
    repl_frame_hdr_t hdr;
    hdr.type = type;
    hdr.flags = 0;
    hdr.reserved = 0;
    hdr.len = len;

    const char *p = (const char *)&hdr;
    size_t left = sizeof(hdr);
    while (left > 0) {
        ssize_t n = send(fd, p, left, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p += (size_t)n;
        left -= (size_t)n;
    }

    if (len > 0 && payload) {
        p = payload;
        left = len;
        while (left > 0) {
            ssize_t n = send(fd, p, left, MSG_NOSIGNAL);
            if (n < 0) {
                if (errno == EINTR) continue;
                return -1;
            }
            p += (size_t)n;
            left -= (size_t)n;
        }
    }

    return 0;
}

static int recv_frame(int fd, repl_frame_hdr_t *hdr, char *payload, size_t max_len) {
    /* Receive header with timeout to allow graceful shutdown */
    ssize_t n = recv(fd, hdr, sizeof(*hdr), MSG_WAITALL);
    if (n != sizeof(*hdr)) {
        if (n == 0 || errno == ECONNRESET || errno == EPIPE) {
            return -1;  /* Connection closed */
        }
        if (errno == EINTR || errno == EAGAIN) {
            return -1;  /* Interrupted or timeout */
        }
        return -1;
    }

    if (hdr->len > 0) {
        if (hdr->len > max_len) {
            fprintf(stderr, "[TCP-XDP-REPL] payload too large: %u > %zu\n",
                    hdr->len, max_len);
            return -1;
        }

        n = recv(fd, payload, hdr->len, MSG_WAITALL);
        if (n != (ssize_t)hdr->len) return -1;
    }

    return 0;
}

/* ===================================================================
 * Master send thread
 * =================================================================== */

static void *master_send_thread(void *arg) {
    (void)arg;
    char buf[65536];

    printf("[TCP-XDP-REPL] Master send thread started\n");

    while (atomic_load_explicit(&g_ctx.running, memory_order_acquire)) {
        size_t total = 0;

        while (total < sizeof(buf) - 8192) {
            size_t len = 0;
            int rc = repl_backlog_get_range(g_ctx.master_offset + total,
                                           buf + total, 8192, &len);
            if (rc != 0 || len == 0) break;
            total += len;
        }

        if (total > 0) {
            printf("[TCP-XDP-REPL] Sending %zu bytes from offset %llu\n",
                   total, (unsigned long long)g_ctx.master_offset);
            if (send_frame(g_ctx.tcp_fd, REPL_FRAME_DATA, buf, total) == 0) {
                g_ctx.master_offset += total;
                printf("[TCP-XDP-REPL] Sent successfully, new offset=%llu\n",
                       (unsigned long long)g_ctx.master_offset);
            } else {
                fprintf(stderr, "[TCP-XDP-REPL] send failed: %s\n", strerror(errno));
                break;
            }
        } else {
            usleep(100);
        }

        time_t now = time(NULL);
        if (now - g_ctx.last_ping >= 10) {
            send_frame(g_ctx.tcp_fd, REPL_FRAME_PING, NULL, 0);
            g_ctx.last_ping = now;
        }
    }

    printf("[TCP-XDP-REPL] Master send thread stopped\n");
    return NULL;
}

/* ===================================================================
 * Master recv thread (handle ACK)
 * =================================================================== */

static void *master_recv_thread(void *arg) {
    (void)arg;
    repl_frame_hdr_t hdr;
    char payload[1024];

    printf("[TCP-XDP-REPL] Master recv thread started\n");

    while (atomic_load_explicit(&g_ctx.running, memory_order_acquire)) {
        if (recv_frame(g_ctx.tcp_fd, &hdr, payload, sizeof(payload)) != 0) {
            fprintf(stderr, "[TCP-XDP-REPL] recv failed: %s\n", strerror(errno));
            break;
        }

        switch (hdr.type) {
        case REPL_FRAME_ACK:
            if (hdr.len == sizeof(uint64_t)) {
                uint64_t ack_offset;
                memcpy(&ack_offset, payload, sizeof(ack_offset));
                repl_backlog_ack(ack_offset);
                g_ctx.slave_ack_offset = ack_offset;
            }
            break;

        case REPL_FRAME_PONG:
            break;

        case REPL_FRAME_PSYNC:
            /* Ignore PSYNC - fullsync is handled by RDMA layer */
            break;

        default:
            fprintf(stderr, "[TCP-XDP-REPL] unknown frame type: %u\n", hdr.type);
            break;
        }
    }

    printf("[TCP-XDP-REPL] Master recv thread stopped\n");
    return NULL;
}

/* ===================================================================
 * Slave recv thread (receive DATA frames)
 * =================================================================== */

static void *slave_recv_thread(void *arg) {
    (void)arg;
    repl_frame_hdr_t hdr;
    char *payload = malloc(128 * 1024);  /* 128KB buffer */
    if (!payload) {
        fprintf(stderr, "[TCP-XDP-REPL] malloc failed\n");
        return NULL;
    }

    printf("[TCP-XDP-REPL] Slave recv thread started\n");

    uint64_t recv_offset = 0;
    int ack_counter = 0;

    while (atomic_load_explicit(&g_ctx.running, memory_order_acquire)) {
        if (recv_frame(g_ctx.tcp_fd, &hdr, payload, 128 * 1024) != 0) {
            fprintf(stderr, "[TCP-XDP-REPL] recv failed: %s\n", strerror(errno));
            break;
        }

        switch (hdr.type) {
        case REPL_FRAME_DATA:
            /* Parse and apply journal records */
            {
                printf("[TCP-XDP-REPL] Slave received %u bytes of data\n", hdr.len);
                const char *p = payload;
                size_t remain = hdr.len;
                int cmd_count = 0;

                while (remain > 0) {
                    kvs_token_t tokens[KVS_MAX_TOKENS];
                    int argc = 0;
                    int cmd_len = resp_parse_slices(p, (int)remain, tokens, KVS_MAX_TOKENS, &argc);

                    if (cmd_len <= 0) break;

                    DynBuf wb;
                    dynbuf_init(&wb, 4096);
                    kvs_dispatch(tokens, argc, p, cmd_len, &wb, NULL);
                    dynbuf_free(&wb);

                    p += cmd_len;
                    remain -= cmd_len;
                    cmd_count++;
                }

                recv_offset += hdr.len;
                ack_counter++;
                printf("[TCP-XDP-REPL] Applied %d commands, recv_offset=%llu\n",
                       cmd_count, (unsigned long long)recv_offset);

                /* Send ACK every 100 frames */
                if (ack_counter >= 100) {
                    send_frame(g_ctx.tcp_fd, REPL_FRAME_ACK,
                              (char *)&recv_offset, sizeof(recv_offset));
                    ack_counter = 0;
                }
            }
            break;

        case REPL_FRAME_PING:
            send_frame(g_ctx.tcp_fd, REPL_FRAME_PONG, NULL, 0);
            break;

        default:
            fprintf(stderr, "[TCP-XDP-REPL] unknown frame type: %u\n", hdr.type);
            break;
        }
    }

    free(payload);
    printf("[TCP-XDP-REPL] Slave recv thread stopped\n");
    return NULL;
}

/* ===================================================================
 * Master API
 * =================================================================== */

static void *master_accept_thread(void *arg) {
    (void)arg;

    printf("[TCP-XDP-REPL] Waiting for slave connection...\n");

    /* Accept slave connection (blocking) */
    g_ctx.tcp_fd = accept(g_ctx.listen_fd, NULL, NULL);
    if (g_ctx.tcp_fd < 0) {
        fprintf(stderr, "[TCP-XDP-REPL] accept failed: %s\n", strerror(errno));
        return NULL;
    }

    optimize_tcp_socket(g_ctx.tcp_fd);

    /* Initialize master_offset to current backlog position */
    g_ctx.master_offset = repl_backlog_get_master_offset();
    printf("[TCP-XDP-REPL] Slave connected, master_offset=%llu\n",
           (unsigned long long)g_ctx.master_offset);

    atomic_store_explicit(&g_ctx.slave_connected, 1, memory_order_release);

    /* Start send/recv threads */
    pthread_create(&g_ctx.send_thread, NULL, master_send_thread, NULL);
    pthread_create(&g_ctx.recv_thread, NULL, master_recv_thread, NULL);

    return NULL;
}

int tcp_xdp_repl_master_start(void) {
    /* Check if already started */
    if (atomic_load_explicit(&g_ctx.running, memory_order_acquire)) {
        printf("[TCP-XDP-REPL] Master already started\n");
        return 0;
    }

    memset(&g_ctx, 0, sizeof(g_ctx));

    /* Initialize backlog */
    if (repl_backlog_init() != 0) {
        fprintf(stderr, "[TCP-XDP-REPL] backlog init failed\n");
        return -1;
    }

    /* Create listen socket */
    g_ctx.listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_ctx.listen_fd < 0) {
        fprintf(stderr, "[TCP-XDP-REPL] socket failed: %s\n", strerror(errno));
        return -1;
    }

    int reuse = 1;
    setsockopt(g_ctx.listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(kvs_config.xdp_repl_tcp_port),
        .sin_addr.s_addr = INADDR_ANY
    };

    if (bind(g_ctx.listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "[TCP-XDP-REPL] bind failed: %s\n", strerror(errno));
        close(g_ctx.listen_fd);
        return -1;
    }

    if (listen(g_ctx.listen_fd, 5) < 0) {
        fprintf(stderr, "[TCP-XDP-REPL] listen failed: %s\n", strerror(errno));
        close(g_ctx.listen_fd);
        return -1;
    }

    printf("[TCP-XDP-REPL] Master listening on port %d\n", kvs_config.xdp_repl_tcp_port);

    /* Start accept thread (non-blocking) */
    atomic_store_explicit(&g_ctx.running, 1, memory_order_release);
    atomic_store_explicit(&g_ctx.is_master, 1, memory_order_release);
    atomic_store_explicit(&g_ctx.slave_connected, 0, memory_order_release);
    g_ctx.last_ping = time(NULL);

    pthread_create(&g_ctx.accept_thread, NULL, master_accept_thread, NULL);

    return 0;
}

void tcp_xdp_repl_master_stop(void) {
    atomic_store_explicit(&g_ctx.running, 0, memory_order_release);

    /* Shutdown sockets first to unblock recv/send calls */
    if (g_ctx.tcp_fd > 0) {
        shutdown(g_ctx.tcp_fd, SHUT_RDWR);
    }

    if (g_ctx.listen_fd > 0) {
        shutdown(g_ctx.listen_fd, SHUT_RDWR);
    }

    /* Wait for threads to exit */
    if (atomic_load_explicit(&g_ctx.slave_connected, memory_order_acquire)) {
        pthread_join(g_ctx.send_thread, NULL);
        pthread_join(g_ctx.recv_thread, NULL);
    }
    pthread_join(g_ctx.accept_thread, NULL);

    /* Close file descriptors */
    if (g_ctx.tcp_fd > 0) {
        close(g_ctx.tcp_fd);
        g_ctx.tcp_fd = -1;
    }

    if (g_ctx.listen_fd > 0) {
        close(g_ctx.listen_fd);
        g_ctx.listen_fd = -1;
    }

    repl_backlog_free();

    printf("[TCP-XDP-REPL] Master stopped\n");
}

int tcp_xdp_repl_master_publish(const kvs_journal_record_t *rec) {
    if (!rec) return -1;
    if (!atomic_load_explicit(&g_ctx.running, memory_order_acquire)) return 0;

    return repl_backlog_append(rec->payload, rec->len);
}

/* ===================================================================
 * Slave API
 * =================================================================== */

int tcp_xdp_repl_slave_start(const char *master_ip, int master_port) {
    memset(&g_ctx, 0, sizeof(g_ctx));

    /* Connect to master */
    g_ctx.tcp_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_ctx.tcp_fd < 0) {
        fprintf(stderr, "[TCP-XDP-REPL] socket failed: %s\n", strerror(errno));
        return -1;
    }

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(master_port)
    };

    if (inet_pton(AF_INET, master_ip, &addr.sin_addr) <= 0) {
        fprintf(stderr, "[TCP-XDP-REPL] invalid IP: %s\n", master_ip);
        close(g_ctx.tcp_fd);
        return -1;
    }

    if (connect(g_ctx.tcp_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "[TCP-XDP-REPL] connect failed: %s\n", strerror(errno));
        close(g_ctx.tcp_fd);
        return -1;
    }

    optimize_tcp_socket(g_ctx.tcp_fd);

    printf("[TCP-XDP-REPL] Slave connected to %s:%d\n", master_ip, master_port);

    /* Send PSYNC request */
    send_frame(g_ctx.tcp_fd, REPL_FRAME_PSYNC, NULL, 0);

    /* Start recv thread */
    atomic_store_explicit(&g_ctx.running, 1, memory_order_release);
    atomic_store_explicit(&g_ctx.is_master, 0, memory_order_release);

    pthread_create(&g_ctx.recv_thread, NULL, slave_recv_thread, NULL);

    return 0;
}

void tcp_xdp_repl_slave_stop(void) {
    atomic_store_explicit(&g_ctx.running, 0, memory_order_release);

    /* Shutdown socket first to unblock recv calls */
    if (g_ctx.tcp_fd > 0) {
        shutdown(g_ctx.tcp_fd, SHUT_RDWR);
    }

    /* Wait for thread to exit */
    pthread_join(g_ctx.recv_thread, NULL);

    /* Close file descriptor */
    if (g_ctx.tcp_fd > 0) {
        close(g_ctx.tcp_fd);
        g_ctx.tcp_fd = -1;
    }

    printf("[TCP-XDP-REPL] Slave stopped\n");
}

/* ===================================================================
 * Status API
 * =================================================================== */

uint64_t tcp_xdp_repl_get_master_offset(void) {
    return g_ctx.master_offset;
}

uint64_t tcp_xdp_repl_get_slave_offset(void) {
    return g_ctx.slave_ack_offset;
}
