/* repl_tcp.c — TCP-based replication transport layer with RESP reassembly
 *
 * Master flow:
 *   1. Load eBPF filter (repl_filter.bpf.o) → captures write commands
 *   2. Poll ringbuf → reassemble RESP commands in userspace
 *   3. Send complete commands via TCP with length framing
 *   4. Accept slave connections on repl_port
 *
 * Slave flow:
 *   1. Connect to master via TCP
 *   2. Receive length-framed commands (already complete)
 *   3. Write commands to local kvstore socket
 *   4. Auto-reconnect on disconnect
 *
 * RESP Reassembly:
 *   - eBPF captures TCP segments (may be incomplete RESP commands)
 *   - Userspace maintains per-connection reassembly buffers
 *   - Only complete RESP commands are replicated
 *   - Stale connections are cleaned up every 10 seconds
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

#define REPL_OBSERVE_MAX_DATA  4096
#define REPL_OBSERVE_MAX_SOCKS 65536
#define REPL_OBSERVE_RINGBUF_SZ (2 * 1024 * 1024)

struct repl_observe_event {
    uint64_t sock_cookie;
    uint32_t len;
    uint32_t padding;
    uint8_t  data[REPL_OBSERVE_MAX_DATA];
};

/* ── Constants ──────────────────────────────────────────────────────── */

#define REPL_FRAME_HEADER   4
#define REPL_MAX_FRAME_SIZE (64 * 1024)  /* 64KB max frame */
#define RECONNECT_INTERVAL  3            /* seconds */

/* ── Sync Protocol ──────────────────────────────────────────────────── */

#define SYNC_MAGIC          0x53594E43   /* "SYNC" */
#define SYNC_STATUS_OK      0
#define SYNC_STATUS_FULLSYNC_REQUIRED 1

/* Sync request from slave to master */
struct sync_request {
    uint32_t magic;          /* SYNC_MAGIC */
    uint64_t last_offset;    /* Last received offset */
} __attribute__((packed));

/* Sync response from master to slave */
struct sync_response {
    uint32_t status;         /* SYNC_STATUS_OK or SYNC_STATUS_FULLSYNC_REQUIRED */
    uint64_t start_offset;   /* Starting offset for catch-up data */
    uint64_t data_len;       /* Length of catch-up data to follow */
} __attribute__((packed));

/* ── RESP Protocol Reassembly ───────────────────────────────────────── */

#define REASSEMBLY_BUF_SIZE  16384       /* 16KB reassembly buffer per connection */
#define REASSEMBLY_TIMEOUT   30          /* 30 seconds timeout for stale connections */
#define MAX_CONNECTIONS      1024        /* Max tracked connections */

/* Reassembly context for each connection */
struct reassembly_ctx {
    uint64_t sock_cookie;
    uint8_t  buf[REASSEMBLY_BUF_SIZE];
    size_t   buf_len;
    time_t   last_active;
    struct reassembly_ctx *next;  /* For hash table chaining */
};

/* Hash table for connection tracking */
static struct {
    struct reassembly_ctx *table[MAX_CONNECTIONS];
    pthread_mutex_t lock;
} g_reassembly = {0};

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

/* ── RESP Protocol Parser ───────────────────────────────────────────── */

/* Find complete RESP command in buffer
 * Returns: length of complete command, or -1 if incomplete
 */
static int find_complete_resp_command(const uint8_t *data, size_t len)
{
    if (len < 4 || data[0] != '*')
        return -1;

    /* Parse array count: *N\r\n */
    size_t i = 1;
    int argc = 0;
    while (i < len && data[i] >= '0' && data[i] <= '9') {
        argc = argc * 10 + (data[i] - '0');
        i++;
    }
    if (i + 1 >= len || data[i] != '\r' || data[i+1] != '\n')
        return -1;
    i += 2;

    /* Parse each argument: $L\r\n<data>\r\n */
    for (int arg = 0; arg < argc; arg++) {
        if (i >= len || data[i] != '$')
            return -1;
        i++;

        /* Parse argument length */
        int arg_len = 0;
        while (i < len && data[i] >= '0' && data[i] <= '9') {
            arg_len = arg_len * 10 + (data[i] - '0');
            i++;
        }
        if (i + 1 >= len || data[i] != '\r' || data[i+1] != '\n')
            return -1;
        i += 2;

        /* Skip argument data + \r\n */
        i += arg_len + 2;
        if (i > len)
            return -1;  /* Incomplete data */
    }

    return (int)i;  /* Complete command length */
}

/* ── Reassembly Context Management ──────────────────────────────────── */

static inline uint32_t hash_cookie(uint64_t cookie)
{
    return (uint32_t)(cookie % MAX_CONNECTIONS);
}

static struct reassembly_ctx *get_or_create_ctx(uint64_t cookie)
{
    uint32_t hash = hash_cookie(cookie);

    pthread_mutex_lock(&g_reassembly.lock);

    /* Search for existing context */
    struct reassembly_ctx *ctx = g_reassembly.table[hash];
    while (ctx) {
        if (ctx->sock_cookie == cookie) {
            pthread_mutex_unlock(&g_reassembly.lock);
            return ctx;
        }
        ctx = ctx->next;
    }

    /* Create new context */
    ctx = (struct reassembly_ctx *)calloc(1, sizeof(*ctx));
    if (!ctx) {
        pthread_mutex_unlock(&g_reassembly.lock);
        return NULL;
    }

    ctx->sock_cookie = cookie;
    ctx->buf_len = 0;
    ctx->last_active = time(NULL);

    /* Insert at head of chain */
    ctx->next = g_reassembly.table[hash];
    g_reassembly.table[hash] = ctx;

    pthread_mutex_unlock(&g_reassembly.lock);
    return ctx;
}

static void cleanup_stale_contexts(void)
{
    time_t now = time(NULL);

    pthread_mutex_lock(&g_reassembly.lock);

    for (int i = 0; i < MAX_CONNECTIONS; i++) {
        struct reassembly_ctx **prev = &g_reassembly.table[i];
        struct reassembly_ctx *ctx = *prev;

        while (ctx) {
            if (now - ctx->last_active > REASSEMBLY_TIMEOUT) {
                /* Remove stale context */
                *prev = ctx->next;
                free(ctx);
                ctx = *prev;
            } else {
                prev = &ctx->next;
                ctx = ctx->next;
            }
        }
    }

    pthread_mutex_unlock(&g_reassembly.lock);
}

static void cleanup_all_contexts(void)
{
    pthread_mutex_lock(&g_reassembly.lock);

    for (int i = 0; i < MAX_CONNECTIONS; i++) {
        struct reassembly_ctx *ctx = g_reassembly.table[i];
        while (ctx) {
            struct reassembly_ctx *next = ctx->next;
            free(ctx);
            ctx = next;
        }
        g_reassembly.table[i] = NULL;
    }

    pthread_mutex_unlock(&g_reassembly.lock);
}

/* ── eBPF ringbuf callback (master only) ────────────────────────────── */

static int master_on_data(void *ctx, void *data, size_t len)
{
    (void)ctx; (void)len;
    struct repl_observe_event *e = (struct repl_observe_event *)data;

    if (e->len == 0 || e->len > REPL_OBSERVE_MAX_DATA)
        return 0;

    /* Get or create reassembly context for this connection */
    struct reassembly_ctx *rctx = get_or_create_ctx(e->sock_cookie);
    if (!rctx) {
        fprintf(stderr, "[REPL-TCP] Failed to get reassembly context\n");
        return 0;
    }

    /* Check buffer overflow */
    if (rctx->buf_len + e->len > REASSEMBLY_BUF_SIZE) {
        fprintf(stderr, "[REPL-TCP] Reassembly buffer overflow, resetting (cookie=%lu)\n",
                e->sock_cookie);
        rctx->buf_len = 0;
    }

    /* Append data to reassembly buffer */
    memcpy(rctx->buf + rctx->buf_len, e->data, e->len);
    rctx->buf_len += e->len;
    rctx->last_active = time(NULL);

    /* Try to extract complete RESP commands */
    while (rctx->buf_len > 0) {
        int cmd_len = find_complete_resp_command(rctx->buf, rctx->buf_len);
        if (cmd_len <= 0)
            break;  /* Incomplete command, wait for more data */

        /* Append complete command to backlog for incremental recovery */
        repl_incr_append((const char *)rctx->buf, cmd_len);

        /* Send complete command to slave if connected */
        if (g_repl.peer_fd >= 0) {
            if (tcp_send_frame(g_repl.peer_fd, rctx->buf, cmd_len) == 0) {
                pthread_mutex_lock(&g_repl.stats_lock);
                g_repl.stats.bytes_sent += cmd_len + REPL_FRAME_HEADER;
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
        }

        /* Remove processed command from buffer */
        memmove(rctx->buf, rctx->buf + cmd_len, rctx->buf_len - cmd_len);
        rctx->buf_len -= cmd_len;
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

static int master_handle_sync_request(int fd)
{
    struct sync_request req;
    struct sync_response resp;

    /* Receive sync request */
    if (tcp_recv_exact(fd, &req, sizeof(req)) != 0) {
        fprintf(stderr, "[REPL-TCP] Failed to receive sync request\n");
        return -1;
    }

    /* Validate magic */
    if (ntohl(req.magic) != SYNC_MAGIC) {
        fprintf(stderr, "[REPL-TCP] Invalid sync magic: 0x%x\n", ntohl(req.magic));
        return -1;
    }

    uint64_t last_offset = be64toh(req.last_offset);
    printf("[REPL-TCP] Slave requests sync from offset %lu\n", last_offset);

    /* Check if offset is available in backlog */
    if (repl_incr_has_offset(last_offset)) {
        /* Partial resync - send missing data */
        resp.status = htonl(SYNC_STATUS_OK);
        resp.start_offset = htobe64(last_offset);

        /* Get missing data from backlog */
        char *catchup_buf = malloc(REPL_MAX_FRAME_SIZE);
        if (!catchup_buf) {
            fprintf(stderr, "[REPL-TCP] malloc failed for catchup buffer\n");
            return -1;
        }

        size_t catchup_len = 0;
        if (repl_incr_get_range(last_offset, catchup_buf, REPL_MAX_FRAME_SIZE, &catchup_len) == 0) {
            resp.data_len = htobe64(catchup_len);

            /* Send response header */
            if (send(fd, &resp, sizeof(resp), MSG_NOSIGNAL) != sizeof(resp)) {
                free(catchup_buf);
                return -1;
            }

            /* Send catchup data if any */
            if (catchup_len > 0) {
                if (send(fd, catchup_buf, catchup_len, MSG_NOSIGNAL) != (ssize_t)catchup_len) {
                    free(catchup_buf);
                    return -1;
                }
                printf("[REPL-TCP] Sent %zu bytes of catchup data\n", catchup_len);
            } else {
                printf("[REPL-TCP] Slave is up-to-date\n");
            }
        } else {
            /* Failed to get range, require full sync */
            resp.status = htonl(SYNC_STATUS_FULLSYNC_REQUIRED);
            resp.start_offset = 0;
            resp.data_len = 0;
            send(fd, &resp, sizeof(resp), MSG_NOSIGNAL);
            printf("[REPL-TCP] Offset available but get_range failed, requiring full sync\n");
        }

        free(catchup_buf);
    } else {
        /* Offset too old - require full sync */
        resp.status = htonl(SYNC_STATUS_FULLSYNC_REQUIRED);
        resp.start_offset = 0;
        resp.data_len = 0;

        if (send(fd, &resp, sizeof(resp), MSG_NOSIGNAL) != sizeof(resp)) {
            return -1;
        }

        printf("[REPL-TCP] Offset %lu too old, requiring full sync\n", last_offset);
    }

    return 0;
}

static void *master_thread_func(void *arg)
{
    (void)arg;

    printf("[REPL-TCP] Master thread started\n");

    /* Set listen socket to non-blocking */
    int flags = fcntl(g_repl.listen_fd, F_GETFL, 0);
    fcntl(g_repl.listen_fd, F_SETFL, flags | O_NONBLOCK);

    int cleanup_counter = 0;

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

                /* Handle sync request from slave */
                if (master_handle_sync_request(fd) == 0) {
                    g_repl.peer_fd = fd;

                    pthread_mutex_lock(&g_repl.stats_lock);
                    g_repl.stats.connected = 1;
                    pthread_mutex_unlock(&g_repl.stats_lock);

                    printf("[REPL-TCP] Slave connected from %s:%u\n",
                           inet_ntoa(slave_addr.sin_addr), ntohs(slave_addr.sin_port));
                } else {
                    fprintf(stderr, "[REPL-TCP] Sync handshake failed, closing connection\n");
                    close(fd);
                }
            }
        }

        /* Cleanup stale reassembly contexts every 10 seconds */
        if (++cleanup_counter >= 10000) {  /* 10000 * 1ms = 10s */
            cleanup_stale_contexts();
            cleanup_counter = 0;
        }

        usleep(1000);  /* 1ms sleep */
    }

    printf("[REPL-TCP] Master thread stopped\n");
    return NULL;
}

/* ── Slave thread ───────────────────────────────────────────────────── */

static int slave_send_sync_request(int fd, uint64_t last_offset)
{
    struct sync_request req;
    struct sync_response resp;

    /* Send sync request */
    req.magic = htonl(SYNC_MAGIC);
    req.last_offset = htobe64(last_offset);

    if (send(fd, &req, sizeof(req), MSG_NOSIGNAL) != sizeof(req)) {
        fprintf(stderr, "[REPL-TCP] Failed to send sync request\n");
        return -1;
    }

    /* Receive sync response */
    if (tcp_recv_exact(fd, &resp, sizeof(resp)) != 0) {
        fprintf(stderr, "[REPL-TCP] Failed to receive sync response\n");
        return -1;
    }

    uint32_t status = ntohl(resp.status);
    uint64_t data_len = be64toh(resp.data_len);

    if (status == SYNC_STATUS_FULLSYNC_REQUIRED) {
        fprintf(stderr, "[REPL-TCP] Master requires full sync (offset too old)\n");
        return -2;  /* Special code for full sync required */
    }

    if (status != SYNC_STATUS_OK) {
        fprintf(stderr, "[REPL-TCP] Unknown sync status: %u\n", status);
        return -1;
    }

    /* Receive catchup data if any */
    if (data_len > 0) {
        char *catchup_buf = malloc(data_len);
        if (!catchup_buf) {
            fprintf(stderr, "[REPL-TCP] malloc failed for catchup buffer\n");
            return -1;
        }

        if (tcp_recv_exact(fd, catchup_buf, data_len) != 0) {
            fprintf(stderr, "[REPL-TCP] Failed to receive catchup data\n");
            free(catchup_buf);
            return -1;
        }

        printf("[REPL-TCP] Received %lu bytes of catchup data, applying...\n", data_len);

        /* Apply catchup data frame by frame */
        size_t pos = 0;
        while (pos < data_len) {
            if (pos + 4 > data_len) break;

            uint32_t frame_len;
            memcpy(&frame_len, catchup_buf + pos, 4);
            frame_len = ntohl(frame_len);
            pos += 4;

            if (pos + frame_len > data_len) break;

            slave_apply_command((uint8_t *)(catchup_buf + pos), frame_len);
            pos += frame_len;

            pthread_mutex_lock(&g_repl.stats_lock);
            g_repl.stats.slave_offset += frame_len;
            pthread_mutex_unlock(&g_repl.stats_lock);
        }

        free(catchup_buf);
        printf("[REPL-TCP] Catchup complete, now in sync\n");
    } else {
        printf("[REPL-TCP] Already in sync with master\n");
    }

    return 0;
}

static void *slave_thread_func(void *arg)
{
    (void)arg;

    printf("[REPL-TCP] Slave thread started\n");

    uint8_t frame_buf[REPL_MAX_FRAME_SIZE];
    uint64_t last_saved_offset = repl_incr_load_offset();

    while (g_repl.running) {
        /* Connect to master if not connected */
        if (g_repl.peer_fd < 0) {
            printf("[REPL-TCP] Connecting to master %s:%u...\n",
                   g_repl.cfg.master_ip, g_repl.cfg.repl_port);

            g_repl.peer_fd = tcp_connect(g_repl.cfg.master_ip, g_repl.cfg.repl_port);

            if (g_repl.peer_fd >= 0) {
                /* Send sync request with last known offset */
                int sync_result = slave_send_sync_request(g_repl.peer_fd, last_saved_offset);

                if (sync_result == -2) {
                    /* Full sync required */
                    close(g_repl.peer_fd);
                    g_repl.peer_fd = -1;
                    fprintf(stderr, "[REPL-TCP] Full sync required but not implemented in TCP layer\n");
                    fprintf(stderr, "[REPL-TCP] Please restart slave to trigger RDMA full sync\n");
                    sleep(10);
                    continue;
                } else if (sync_result != 0) {
                    /* Sync failed */
                    close(g_repl.peer_fd);
                    g_repl.peer_fd = -1;
                    sleep(RECONNECT_INTERVAL);
                    continue;
                }

                pthread_mutex_lock(&g_repl.stats_lock);
                g_repl.stats.connected = 1;
                g_repl.stats.reconnect_count++;
                g_repl.stats.partial_resync = (sync_result == 0 && last_saved_offset > 0) ? 1 : 0;
                pthread_mutex_unlock(&g_repl.stats_lock);

                printf("[REPL-TCP] Connected and synced with master\n");
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

        /* Save offset to disk every 1000 frames */
        if (g_repl.stats.frames_recv % 1000 == 0) {
            last_saved_offset = g_repl.stats.slave_offset;
            repl_incr_save_offset(last_saved_offset);
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
        /* Initialize reassembly hash table */
        pthread_mutex_init(&g_reassembly.lock, NULL);
        memset(g_reassembly.table, 0, sizeof(g_reassembly.table));

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

    /* Cleanup reassembly contexts */
    cleanup_all_contexts();
    pthread_mutex_destroy(&g_reassembly.lock);

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
