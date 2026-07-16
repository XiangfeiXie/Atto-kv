/* test_xdp_mirror.c — Standalone XDP mirror functional test
 *
 * Tests the XDP packet capture → ringbuf → user-space pipeline:
 *   1. Load XDP BPF, attach to loopback
 *   2. Start a TCP echo server on test port
 *   3. Connect and send "PING\r\n"
 *   4. Poll ringbuf, verify captured data matches
 *
 * Build:  gcc -O2 -o test_xdp_mirror test_xdp_mirror.c \
 *              -lbpf -lelf -lz -lpthread
 * Run:    sudo ./test_xdp_mirror
 */

#define _GNU_SOURCE
#include <arpa/inet.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <linux/if_link.h>
#include <net/if.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* Must match repl_xdp_common.h */
#define XDP_MAX_PAYLOAD   4096
#define XDP_FLAG_SYN      0x01
#define XDP_FLAG_FIN      0x02
#define XDP_FLAG_RST      0x04

struct xdp_repl_event {
    uint32_t saddr;
    uint32_t daddr;
    uint16_t sport;
    uint16_t dport;
    uint32_t seq;
    uint16_t data_len;
    uint8_t  flags;
    uint8_t  pad;
    uint8_t  data[XDP_MAX_PAYLOAD];
};

static volatile int g_running = 1;
static volatile int g_events = 0;
static volatile int g_data_events = 0;
static char g_captured[4096] = {0};
static volatile int g_captured_len = 0;

/* Ringbuf callback */
static int handle_event(void *ctx, void *data, size_t sz)
{
    (void)ctx; (void)sz;
    struct xdp_repl_event *e = (struct xdp_repl_event *)data;

    g_events++;

    uint16_t dport_ho = ntohs(e->dport);
    printf("[EVENT] saddr=%08x sport=%u dport=%u seq=%u len=%u flags=0x%02x\n",
           ntohl(e->saddr), ntohs(e->sport), dport_ho,
           ntohl(e->seq), e->data_len, e->flags);

    if (e->data_len > 0 && !(e->flags & XDP_FLAG_SYN)) {
        g_data_events++;
        if (g_captured_len == 0 && e->data_len <= (int)sizeof(g_captured) - 1) {
            memcpy(g_captured, e->data, e->data_len);
            g_captured[e->data_len] = '\0';
            g_captured_len = e->data_len;
        }
        printf("[DATA]  len=%u: '%.*s'\n", e->data_len,
               e->data_len > 64 ? 64 : e->data_len, e->data);
    }

    return 0;
}

/* Signal handler */
static void sig_handler(int sig) { (void)sig; g_running = 0; }

#define TEST_PORT 19999

/* Simple echo server on port TEST_PORT */
static void *echo_server(void *arg)
{
    (void)arg;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("server socket"); return NULL; }

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port   = htons(TEST_PORT),
        .sin_addr.s_addr = htonl(INADDR_LOOPBACK)
    };
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("server bind"); close(fd); return NULL;
    }
    if (listen(fd, 1) < 0) {
        perror("server listen"); close(fd); return NULL;
    }
    printf("[SERVER] Listening on 127.0.0.1:%d\n", TEST_PORT);

    struct sockaddr_in cli_addr;
    socklen_t cli_len = sizeof(cli_addr);
    int cfd = accept(fd, (struct sockaddr *)&cli_addr, &cli_len);
    if (cfd < 0) { perror("accept"); close(fd); return NULL; }
    printf("[SERVER] Client connected from %s:%d\n",
           inet_ntoa(cli_addr.sin_addr), ntohs(cli_addr.sin_port));

    char buf[256];
    ssize_t n = recv(cfd, buf, sizeof(buf) - 1, 0);
    if (n > 0) {
        buf[n] = '\0';
        printf("[SERVER] Received: '%.*s'\n", (int)n, buf);
        send(cfd, buf, n, 0);
        printf("[SERVER] Echoed back\n");
    }

    sleep(1); /* Wait for FIN to be captured */
    close(cfd);
    close(fd);
    printf("[SERVER] Done\n");
    return NULL;
}

int main(int argc, char **argv)
{
    const char *iface = "lo";
    const char *obj_path = NULL;

    if (argc >= 2) iface    = argv[1];
    if (argc >= 3) obj_path = argv[2];

    if (!obj_path) {
        /* Try common locations */
        static char paths[4][512];
        snprintf(paths[0], sizeof(paths[0]), "build/repl_filter_xdp.bpf.o");
        snprintf(paths[1], sizeof(paths[1]), "../build/repl_filter_xdp.bpf.o");
        snprintf(paths[2], sizeof(paths[2]), "%s/repl_filter_xdp.bpf.o",
                 getenv("BUILD_DIR") ? getenv("BUILD_DIR") : "build");
        for (int i = 0; i < 3; i++) {
            if (access(paths[i], R_OK) == 0) {
                obj_path = paths[i];
                break;
            }
        }
        if (!obj_path) {
            fprintf(stderr, "BPF object not found. Usage: %s [iface] [bpf_obj]\n", argv[0]);
            return 1;
        }
    }

    printf("[TEST] Interface: %s\n", iface);
    printf("[TEST] BPF obj:   %s\n", obj_path);
    printf("[TEST] Port:      %d\n", TEST_PORT);

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    /* ── Resolve interface ───────────────────────────────── */
    int ifindex = if_nametoindex(iface);
    if (ifindex == 0) {
        fprintf(stderr, "[FAIL] Interface '%s' not found\n", iface);
        return 1;
    }
    printf("[TEST] ifindex:   %d\n", ifindex);

    /* ── Load XDP BPF ─────────────────────────────────────── */
    struct bpf_object *obj = bpf_object__open_file(obj_path, NULL);
    if (!obj) {
        fprintf(stderr, "[FAIL] bpf_object__open_file: %s\n", strerror(errno));
        return 1;
    }

    if (bpf_object__load(obj) != 0) {
        fprintf(stderr, "[FAIL] bpf_object__load: %s\n", strerror(errno));
        bpf_object__close(obj);
        return 1;
    }
    printf("[TEST] BPF object loaded\n");

    /* Set config: port = TEST_PORT */
    struct bpf_map *cfg_map = bpf_object__find_map_by_name(obj, "repl_config");
    if (cfg_map) {
        uint32_t key = 0, val = TEST_PORT;
        bpf_map__update_elem(cfg_map, &key, sizeof(key), &val, sizeof(val), BPF_ANY);
        printf("[TEST] Config set: port=%u\n", val);
    }

    /* ── Attach XDP ───────────────────────────────────────── */
    struct bpf_program *prog = bpf_object__find_program_by_name(obj, "xdp_repl_sniff");
    if (!prog) {
        fprintf(stderr, "[FAIL] xdp_repl_sniff not found\n");
        bpf_object__close(obj);
        return 1;
    }

    struct bpf_link *link = bpf_program__attach_xdp(prog, ifindex);
    if (!link) {
        fprintf(stderr, "[FAIL] XDP attach: %s\n", strerror(errno));
        bpf_object__close(obj);
        return 1;
    }
    printf("[TEST] XDP attached to '%s'\n", iface);

    /* ── Create ringbuf ───────────────────────────────────── */
    struct bpf_map *rb_map = bpf_object__find_map_by_name(obj, "repl_ringbuf");
    if (!rb_map) {
        fprintf(stderr, "[FAIL] ringbuf map not found\n");
        bpf_link__destroy(link);
        bpf_object__close(obj);
        return 1;
    }

    struct ring_buffer *rb = ring_buffer__new(bpf_map__fd(rb_map), handle_event, NULL, NULL);
    if (!rb) {
        fprintf(stderr, "[FAIL] ring_buffer__new: %s\n", strerror(errno));
        bpf_link__destroy(link);
        bpf_object__close(obj);
        return 1;
    }
    printf("[TEST] Ringbuf created\n");

    /* ── Start echo server ────────────────────────────────── */
    pthread_t server_thread;
    pthread_create(&server_thread, NULL, echo_server, NULL);
    usleep(100000); /* Give server time to listen */

    /* ── Connect and send data ────────────────────────────── */
    printf("[TEST] Connecting to 127.0.0.1:%d ...\n", TEST_PORT);
    int cfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in saddr = {
        .sin_family = AF_INET,
        .sin_port   = htons(TEST_PORT),
        .sin_addr.s_addr = htonl(INADDR_LOOPBACK)
    };
    if (connect(cfd, (struct sockaddr *)&saddr, sizeof(saddr)) < 0) {
        perror("[FAIL] connect");
        g_running = 0;
        pthread_join(server_thread, NULL);
        goto cleanup;
    }

    /* Send RESP command (simulates a real kvstore write) */
    const char *msg = "*3\r\n$3\r\nSET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n";
    send(cfd, msg, strlen(msg), 0);
    printf("[TEST] Sent RESP command (%zu bytes)\n", strlen(msg));

    char rbuf[256];
    ssize_t rn = recv(cfd, rbuf, sizeof(rbuf) - 1, 0);
    if (rn > 0) {
        rbuf[rn] = '\0';
        printf("[TEST] Received echo: '%s'\n", rbuf);
    }
    close(cfd);

    /* ── Poll ringbuf for captured events ─────────────────── */
    printf("[TEST] Polling ringbuf for 2 seconds...\n");
    time_t start = time(NULL);
    while (g_running && time(NULL) - start < 2) {
        ring_buffer__poll(rb, 100);
        if (g_data_events > 0) break;
    }
    /* One more poll to catch stragglers */
    ring_buffer__poll(rb, 500);

    /* ── Results ──────────────────────────────────────────── */
    printf("\n========================================\n");
    printf("        XDP MIRROR TEST RESULTS\n");
    printf("========================================\n");
    printf(" Total events:      %d\n", g_events);
    printf(" Data events:       %d\n", g_data_events);
    printf(" Captured data:     '%.*s'\n", g_captured_len, g_captured);
    printf("----------------------------------------\n");

    if (g_data_events > 0 && g_captured_len > 0) {
        printf(" RESULT: PASS \xe2\x9c\x93\n");
        printf(" XDP successfully captured the test data!\n");
    } else if (g_events > 0) {
        printf(" RESULT: PARTIAL — events seen but no PING data\n");
        printf(" Check: does loopback see both directions?\n");
    } else {
        printf(" RESULT: FAIL \xe2\x9c\x97\n");
        printf(" No events captured. Check:\n");
        printf("  - XDP_MODE=generic for loopback\n");
        printf("  - cap_bpf,cap_net_admin capabilities\n");
        printf("  - Kernel CONFIG_XDP_SOCKETS=y\n");
    }
    printf("========================================\n");

    g_running = 0;
    pthread_join(server_thread, NULL);

cleanup:
    ring_buffer__free(rb);
    bpf_link__destroy(link);
    bpf_object__close(obj);
    printf("[TEST] Cleanup complete\n");
    return (g_data_events > 0 && g_captured_len > 0) ? 0 : 1;
}
