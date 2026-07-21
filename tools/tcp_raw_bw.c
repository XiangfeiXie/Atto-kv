/*
 * tcp_raw_bw.c — 原生 TCP socket 单向传输带宽基准
 *
 * 与 libfabric_bw.c 功能对等、接口一致，仅传输层用原生 socket:
 *   libfabric_bw:  fi_passive_ep / fi_send / fi_recv
 *   tcp_raw_bw:    socket/bind/listen / send / recv
 *
 * 跨 VM 用法 (与 libfabric_bw 完全相同):
 *   主机: ./tcp_raw_bw --server --port 2101 --size 100M
 *   从机: ./tcp_raw_bw --client 192.168.30.135 --port 2101 --size 100M
 *
 * 本地: ./tcp_raw_bw --server --port 2101 &
 *       ./tcp_raw_bw --client 127.0.0.1 --port 2101 --size 100M
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <getopt.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

static const char *g_node   = "127.0.0.1";
static int         g_port   = 2101;
static int         g_server = 0;
static size_t      g_total  = 100UL * 1024 * 1024;
static size_t      g_batch  = 1024UL * 1024;

static long long now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

static void die(const char *msg) { perror(msg); exit(1); }

static int tcp_listen(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) die("socket");
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port),
                            .sin_addr.s_addr = INADDR_ANY};
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) < 0) die("bind");
    if (listen(fd, 5) < 0) die("listen");
    return fd;
}

static int tcp_connect(const char *ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) die("socket");
    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    if (inet_pton(AF_INET, ip, &a.sin_addr) != 1) die("inet_pton");
    if (connect(fd, (struct sockaddr *)&a, sizeof(a)) < 0) die("connect");
    return fd;
}

static size_t recv_full(int fd, void *buf, size_t len) {
    size_t got = 0;
    while (got < len) {
        ssize_t n = recv(fd, (char *)buf + got, len - got, 0);
        if (n <= 0) return got;
        got += (size_t)n;
    }
    return got;
}

/* ── server ────────────────────────────────────────────────────────── */

static int run_server(void) {
    int lfd = tcp_listen(g_port);
    printf("[server] listening on 0.0.0.0:%d\n", g_port);

    int cfd = accept(lfd, NULL, NULL);
    if (cfd < 0) die("accept");
    close(lfd);
    printf("[server] client connected\n");

    char *buf = malloc(g_batch);
    size_t total = 0;
    long long t0 = now_us();

    while (total < g_total) {
        size_t chunk = g_batch;
        if (chunk > g_total - total) chunk = g_total - total;
        size_t got = recv_full(cfd, buf, chunk);
        if (got == 0) break;
        total += got;
    }

    long long t1 = now_us();
    close(cfd);
    free(buf);

    double ms = (t1 - t0) / 1000.0;
    double mbps = (ms > 0) ? (total / (ms / 1000.0) / (1024.0 * 1024.0)) : 0;
    printf("[server] recv: %zu bytes in %.1f ms  →  %.2f MB/s\n",
           total, ms, mbps);
    return 0;
}

/* ── client ────────────────────────────────────────────────────────── */

static int run_client(void) {
    int fd = tcp_connect(g_node, g_port);
    printf("[client] connected, sending %zu MB...\n", g_total / (1024*1024));

    char *buf = malloc(g_batch);
    memset(buf, 'A', g_batch);

    size_t sent = 0;
    long long t0 = now_us();

    while (sent < g_total) {
        size_t chunk = g_batch;
        if (sent + chunk > g_total) chunk = g_total - sent;
        ssize_t n = send(fd, buf, chunk, MSG_NOSIGNAL);
        if (n < 0) die("send");
        sent += (size_t)n;
    }

    shutdown(fd, SHUT_WR);
    /* drain any final ack */
    char dummy[64];
    recv(fd, dummy, sizeof(dummy), 0);

    long long t1 = now_us();
    close(fd);
    free(buf);

    double ms = (t1 - t0) / 1000.0;
    double mbps = (ms > 0) ? (sent / (ms / 1000.0) / (1024.0 * 1024.0)) : 0;
    printf("[client] send: %zu bytes in %.1f ms  →  %.2f MB/s\n",
           sent, ms, mbps);
    return 0;
}

/* ── main ──────────────────────────────────────────────────────────── */

static size_t parse_size(const char *s) {
    char *end;
    size_t v = (size_t)strtoull(s, &end, 10);
    if (*end == 'G' || *end == 'g') v *= 1024*1024*1024;
    else if (*end == 'M' || *end == 'm') v *= 1024*1024;
    else if (*end == 'K' || *end == 'k') v *= 1024;
    return v;
}

int main(int argc, char **argv) {
    static struct option opts[] = {
        {"server", no_argument,       NULL, 's'},
        {"client", required_argument, NULL, 'c'},
        {"port",   required_argument, NULL, 'P'},
        {"size",   required_argument, NULL, 'S'},
        {"batch",  required_argument, NULL, 'B'},
        {"help",   no_argument,       NULL, 'h'},
        {0,0,0,0}
    };
    int opt;
    while ((opt = getopt_long(argc, argv, "sc:P:S:B:h", opts, NULL)) != -1) {
        switch (opt) {
        case 's': g_server = 1; break;
        case 'c': g_node = optarg; g_server = 0; break;
        case 'P': g_port = atoi(optarg); break;
        case 'S': g_total = parse_size(optarg); break;
        case 'B': g_batch = parse_size(optarg); break;
        case 'h':
            printf("Usage: tcp_raw_bw --server|--client IP --port P --size N[M|G] [--batch N[K|M]]\n");
            return 0;
        default:  break;
        }
    }

    printf("=== raw TCP BW: size=%zu MB  batch=%zu KB ===\n",
           g_total/(1024*1024), g_batch/1024);
    return g_server ? run_server() : run_client();
}
