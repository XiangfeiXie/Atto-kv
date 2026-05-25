/* repl_agent.c — Independent replication agent.
 *
 * Protocol:
 *   Master accepts TCP connection from slave, then:
 *    1. Full sync: dumps all existing keys as RESP commands
 *    2. Sends marker frame (len=0xFFFFFFFF)
 *    3. Incremental sync: relays ring-buffer data
 *   Slave:
 *    1. Receives full-sync frames → writes to local kvstore
 *    2. On marker, switches to incremental mode
 *    3. On disconnect, reconnects → starts over
 *
 * eBPF observes kvstore traffic at socket layer (SK_PASS, zero overhead).
 * TCP provides reliable ordered transport — no seq/ACK/retransmit needed.
 */
#define _GNU_SOURCE
#include "repl_observe.h"

#include <arpa/inet.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#define REPL_PORT            6483
#define REPL_MARKER_FULLSYNC 0xFFFFFFFFu

static volatile int g_running = 1;
static int g_peer_fd = -1;
static int g_kv_fd   = -1;

static void sig_handler(int sig) { (void)sig; g_running = 0; }

/* ── TCP helpers ───────────────────────────────────────────────────── */

static int tcp_listen(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    struct sockaddr_in a = { .sin_family = AF_INET,
        .sin_port = htons((uint16_t)port),
        .sin_addr.s_addr = htonl(0x7f000001u) };
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) < 0) { perror("bind"); close(fd); return -1; }
    if (listen(fd, 1) < 0) { perror("listen"); close(fd); return -1; }
    return fd;
}

static int tcp_connect(const char *ip, int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }
    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    struct sockaddr_in a = { .sin_family = AF_INET,
        .sin_port = htons((uint16_t)port) };
    if (inet_pton(AF_INET, ip, &a.sin_addr) != 1) { close(fd); return -1; }
    if (connect(fd, (struct sockaddr *)&a, sizeof(a)) < 0) { perror("connect"); close(fd); return -1; }
    return fd;
}

static int read_exact(int fd, void *buf, size_t n)
{
    size_t off = 0;
    while (off < n && g_running) {
        ssize_t r = read(fd, (char *)buf + off, n - off);
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        if (r == 0) return -1;
        off += (size_t)r;
    }
    return g_running ? 0 : -1;
}

static int frame_send(int fd, const void *data, uint32_t len)
{
    uint32_t nl = htonl(len);
    struct iovec iov[2] = { { .iov_base = &nl, .iov_len = 4 },
                            { .iov_base = (void *)data, .iov_len = len } };
    struct msghdr msg = { .msg_iov = iov, .msg_iovlen = 2 };
    ssize_t total = (ssize_t)(4 + len), sent = sendmsg(fd, &msg, MSG_NOSIGNAL);
    if (sent < 0) return -1;
    while (sent < total) {
        ssize_t n = send(fd, (char *)data + sent - 4, total - sent, MSG_NOSIGNAL);
        if (n <= 0) return -1;
        sent += n;
    }
    return 0;
}

static int frame_recv(int fd, uint8_t *buf, uint32_t sz, uint32_t *olen)
{
    uint32_t nl;
    if (read_exact(fd, &nl, 4) != 0) return -1;
    *olen = ntohl(nl);
    if (*olen == REPL_MARKER_FULLSYNC) return 0;  /* marker frame */
    if (*olen > sz) { fprintf(stderr, "frame too big %u\n", *olen); return -1; }
    if (*olen == 0) return 0;
    return read_exact(fd, buf, *olen);
}

/* ═══════════════════════════════════════════════════════════════════
 * RESP helpers for full sync
 * ═══════════════════════════════════════════════════════════════════ */

/* read a CRLF-terminated line from socket.  returns line without \r\n,
 * null-terminates it.  max 256 bytes for safety. */
static int resp_line(int fd, char *buf, int max)
{
    int i = 0;
    while (i < max - 1) {
        char c;
        if (read_exact(fd, &c, 1) != 0) return -1;
        if (c == '\r') {
            if (read_exact(fd, &c, 1) != 0) return -1;
            if (c == '\n') { buf[i] = 0; return i; }
            return -1;
        }
        buf[i++] = c;
    }
    return -1;
}

/* read a RESP simple string, returns pointer into buf */
static char *resp_read(int fd, char *buf, int max)
{
    char c;
    if (read_exact(fd, &c, 1) != 0) return NULL;
    if (c == '+') {              /* simple string */
        int n = resp_line(fd, buf, max);
        if (n < 0) return NULL;
        return buf;
    } else if (c == '-') {       /* error */
        int n = resp_line(fd, buf, max);
        if (n < 0) return NULL;
        fprintf(stderr, "kvstore ERR: %s\n", buf);
        return NULL;
    } else if (c == ':') {       /* integer — return as string */
        int n = resp_line(fd, buf, max);
        if (n < 0) return NULL;
        return buf;
    } else if (c == '$') {       /* bulk string */
        char lenstr[32];
        if (resp_line(fd, lenstr, sizeof(lenstr)) < 0) return NULL;
        int slen = atoi(lenstr);
        if (slen < 0) return NULL;  /* null bulk */
        if (slen >= max) { fprintf(stderr, "bulk string too long: %d\n", slen); return NULL; }
        if (read_exact(fd, buf, slen) != 0) return NULL;
        buf[slen] = 0;
        char crlf[2];
        if (read_exact(fd, crlf, 2) != 0) return NULL;
        return buf;
    } else if (c == '*') {       /* array */
        char lenstr[32];
        if (resp_line(fd, lenstr, sizeof(lenstr)) < 0) return NULL;
        int count = atoi(lenstr);
        /* return count as text for caller to parse */
        snprintf(buf, max, "%d", count);
        return buf;
    }
    return NULL;
}

/* read N elements from an array response (caller already read '*N\r\n').
 * fills strings[0..n-1] with pointers into the provided buf.
 * n = array count.  each string is placed adjacently in buf.
 * returns 0 on success */
static int resp_read_array(int fd, int n, char *buf, int bufsz, char **strings)
{
    char *wr = buf;
    for (int i = 0; i < n; i++) {
        char *v = resp_read(fd, wr, bufsz - (int)(wr - buf));
        if (!v) return -1;
        strings[i] = v;
        wr += strlen(v) + 1;
    }
    return 0;
}

/* send RESP command to kvstore, get full response.
 * cmd: null-terminated RESP command (e.g. "*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n")
 * out: buffer for response
 * sz:  size of out
 * returns response length, or -1 on error */
static int kv_raw_cmd(int fd, const char *cmd, char *out, int sz)
{
    ssize_t n = write(fd, cmd, strlen(cmd));
    if (n < 0) return -1;
    char c;
    if (read_exact(fd, &c, 1) != 0) return -1;

    /* read first line to get prefix */
    if (c == '+') {
        return resp_line(fd, out, sz);
    } else if (c == ':') {
        return resp_line(fd, out, sz);
    } else if (c == '-') {
        int r = resp_line(fd, out, sz);
        fprintf(stderr, "kvstore ERR: %s\n", out);
        return r;
    } else if (c == '$') {
        char lenstr[32];
        int r = resp_line(fd, lenstr, sizeof(lenstr));
        if (r < 0) return -1;
        int slen = atoi(lenstr);
        if (slen < 0) { out[0] = 0; return 0; }  /* null */
        if (slen >= sz) return -1;
        if (read_exact(fd, out, slen) != 0) return -1;
        out[slen] = 0;
        char crlf[2];
        read_exact(fd, crlf, 2);
        return slen;
    } else if (c == '*') {
        char lenstr[32];
        int r = resp_line(fd, lenstr, sizeof(lenstr));
        if (r < 0) return -1;
        return atoi(lenstr);  /* return count */
    }
    return -1;
}

/* build a RESP bulk string array command.
 * writes to cmd_buf.  returns length. */
static int resp_build(char *buf, int sz, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    /* count args */
    int nargs = 0;
    const char *p = fmt;
    while (*p) { if (*p++ == '%') nargs++; }

    /* build command: *N\r\n $L1\r\n arg1 \r\n ... */
    char *wr = buf, *end = buf + sz;

    /* build header and gather arg lengths */
    int *lens = alloca(nargs * sizeof(int));
    const char **args = alloca(nargs * sizeof(const char *));
    int ai = 0;
    p = fmt;
    while (*p) {
        if (*p == '%') {
            p++;
            if (*p == 's') args[ai] = va_arg(ap, const char *);
            else if (*p == 'b') { args[ai] = va_arg(ap, const char *); }
            lens[ai] = (int)strlen(args[ai]);
            ai++; p++;
        } else p++;
    }
    va_end(ap);

    /* count = name(args) + extra count for each hash arg pair */
    int n = nargs;

    wr += snprintf(wr, end - wr, "*%d\r\n", n);
    for (int i = 0; i < nargs && wr < end; i++) {
        wr += snprintf(wr, end - wr, "$%d\r\n", lens[i]);
        if (wr + lens[i] < end) { memcpy(wr, args[i], lens[i]); wr += lens[i]; }
        wr += snprintf(wr, end - wr, "\r\n");
    }
    return (int)(wr - buf);
}

/* ═══════════════════════════════════════════════════════════════════
 * FULL SYNC (master side)
 * ═══════════════════════════════════════════════════════════════════ */

#define FS_CMD_MAX  2048
#define FS_BUF_MAX  65536

static int master_full_sync(int peer_fd, int kv_port)
{
    int kv = tcp_connect("127.0.0.1", kv_port);
    if (kv < 0) return -1;

    printf("[AGENT-M] full sync: dumping keys...\n");

    /* 1. KEYS * */
    {
        const char *keys_cmd = "*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n";
        if (write(kv, keys_cmd, strlen(keys_cmd)) < 0) { close(kv); return -1; }
    }

    /* read response: *N\r\n */
    char buf[FS_BUF_MAX];
    static char *keys[4096];
    char *bp = buf;
    int nkeys = 0;

    {
        char c;
        read_exact(kv, &c, 1);
        if (c != '*') { fprintf(stderr, "expected *, got %c\n", c); close(kv); return -1; }
        char lenstr[16];
        resp_line(kv, lenstr, sizeof(lenstr));
        nkeys = atoi(lenstr);
        if (nkeys < 0 || nkeys > 4096) { close(kv); return -1; }
    }

    /* read each key */
    for (int i = 0; i < nkeys; i++) {
        char *v = resp_read(kv, bp, FS_BUF_MAX - (int)(bp - buf));
        if (!v) { close(kv); return -1; }
        keys[i] = v;
        bp += strlen(v) + 1;
    }

    printf("[AGENT-M] full sync: %d keys, getting values...\n", nkeys);

    /* 2. for each key, get type and value, send to slave */
    char cmd[FS_CMD_MAX];
    char val[FS_BUF_MAX];
    int synced = 0;

    for (int i = 0; i < nkeys; i++) {
        if (!g_running) break;

        /* TYPE key */
        snprintf(cmd, sizeof(cmd), "*2\r\n$4\r\nTYPE\r\n$%d\r\n%s\r\n",
                 (int)strlen(keys[i]), keys[i]);
        int rc = kv_raw_cmd(kv, cmd, val, sizeof(val));
        if (rc < 0) { close(kv); return -1; }

        if (strcmp(val, "string") == 0) {
            /* GET key → SET key value */
            snprintf(cmd, sizeof(cmd), "*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n",
                     (int)strlen(keys[i]), keys[i]);
            int rc = kv_raw_cmd(kv, cmd, val, sizeof(val));
            if (rc < 0) continue;

            int klen = (int)strlen(keys[i]);
            int vlen = (int)strlen(val);
            snprintf(cmd, sizeof(cmd), "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
                     klen, keys[i], vlen, val);
            frame_send(peer_fd, cmd, (uint32_t)strlen(cmd));
            synced++;

        } else if (strcmp(val, "hash") == 0) {
            /* HGETALL key → HSET key f1 v1 ... */
            snprintf(cmd, sizeof(cmd), "*2\r\n$7\r\nHGETALL\r\n$%d\r\n%s\r\n",
                     (int)strlen(keys[i]), keys[i]);
            rc = write(kv, cmd, strlen(cmd));
            if (rc < 0) continue;

            /* read array response: *N\r\n */
            char c;
            if (read_exact(kv, &c, 1) != 0 || c != '*') continue;
            char lenstr[16];
            if (resp_line(kv, lenstr, sizeof(lenstr)) < 0) continue;
            int npairs = atoi(lenstr) / 2;
            if (npairs < 0) continue;

            /* read field-value pairs */
            static char *fv[256];  /* up to 256 field-value pairs */
            int nfv = 0;
            char *lbp = bp;  /* reuse buffer space */
            for (int j = 0; j < npairs * 2; j++) {
                char *fv_str = resp_read(kv, lbp, FS_BUF_MAX - (int)(lbp - buf));
                if (!fv_str) break;
                fv[nfv++] = fv_str;
                lbp += strlen(fv_str) + 1;
            }

            /* build HSET command */
            char *wr = cmd, *end = cmd + sizeof(cmd);
            int nargs = 2 + nfv;
            wr += snprintf(wr, end - wr, "*%d\r\n$4\r\nHSET\r\n$%d\r\n%s\r\n",
                           nargs, (int)strlen(keys[i]), keys[i]);
            for (int j = 0; j < nfv && wr < end; j++) {
                int fl = (int)strlen(fv[j]);
                wr += snprintf(wr, end - wr, "$%d\r\n", fl);
                if (wr + fl < end) { memcpy(wr, fv[j], fl); wr += fl; }
                if (wr + 2 < end) { *wr++ = '\r'; *wr++ = '\n'; }
            }
            frame_send(peer_fd, cmd, (uint32_t)(wr - cmd));
            synced++;
        }
        /* skip other types (none, expired, etc.) */
    }

    close(kv);
    printf("[AGENT-M] full sync: %d keys synced\n", synced);

    /* send marker */
    uint32_t marker = htonl(REPL_MARKER_FULLSYNC);
    if (write(peer_fd, &marker, 4) < 0) {}
    printf("[AGENT-M] full sync done, switching to incremental\n");
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * INCREMENTAL SYNC (ring buffer → TCP)
 * ═══════════════════════════════════════════════════════════════════ */

static int master_on_data(void *ctx, void *data, size_t len)
{
    (void)ctx; (void)len;
    struct repl_observe_event *e = (struct repl_observe_event *)data;
    if (e->len == 0 || e->len > REPL_OBSERVE_MAX_DATA) return 0;
    if (g_peer_fd >= 0) frame_send(g_peer_fd, e->data, e->len);
    return 0;
}

static int master_bpf_load(int kv_port)
{
    char obj_path[512] = {};
    ssize_t n = readlink("/proc/self/exe", obj_path, sizeof(obj_path) - 1);
    if (n > 0) {
        char *slash = strrchr(obj_path, '/');
        if (slash) *slash = '\0';
        strcat(obj_path, "/repl_observe.bpf.o");
    } else strcpy(obj_path, "build/repl_observe.bpf.o");

    if (access(obj_path, R_OK) != 0) strcpy(obj_path, "repl_observe.bpf.o");
    if (access(obj_path, R_OK) != 0) {
        fprintf(stderr, "[AGENT-M] BPF object not found\n"); return -1;
    }

    struct bpf_object *obj = bpf_object__open_file(obj_path, NULL);
    if (!obj) { fprintf(stderr, "[AGENT-M] open: %s\n", strerror(errno)); return -1; }
    if (bpf_object__load(obj)) { fprintf(stderr, "[AGENT-M] load failed\n"); bpf_object__close(obj); return -1; }

    struct bpf_map *cfg = bpf_object__find_map_by_name(obj, "repl_config");
    if (cfg) { uint32_t k = 0, v = (uint32_t)kv_port;
               bpf_map__update_elem(cfg, &k, sizeof(k), &v, sizeof(v), BPF_ANY); }

    int cgfd = open("/sys/fs/cgroup", O_RDONLY | O_DIRECTORY);
    if (cgfd < 0) { bpf_object__close(obj); return -1; }

    struct bpf_program *sock_prog = bpf_object__find_program_by_name(obj, "repl_observe_sockops");
    if (sock_prog) bpf_prog_attach(bpf_program__fd(sock_prog), cgfd, BPF_CGROUP_SOCK_OPS, 0);

    struct bpf_map *sockmap = bpf_object__find_map_by_name(obj, "repl_sockmap");
    if (sockmap) {
        int mfd = bpf_map__fd(sockmap);
        struct bpf_program *p = bpf_object__find_program_by_name(obj, "repl_observe_parser");
        if (p) bpf_prog_attach(bpf_program__fd(p), mfd, BPF_SK_SKB_STREAM_PARSER, 0);
        p = bpf_object__find_program_by_name(obj, "repl_observe_verdict");
        if (p) bpf_prog_attach(bpf_program__fd(p), mfd, BPF_SK_SKB_STREAM_VERDICT, 0);
    }

    struct bpf_map *rb = bpf_object__find_map_by_name(obj, "repl_ringbuf");
    struct ring_buffer *ring = rb ? ring_buffer__new(bpf_map__fd(rb), master_on_data, NULL, NULL) : NULL;
    if (!ring) { close(cgfd); bpf_object__close(obj); return -1; }

    printf("[AGENT-M] incremental sync active\n");

    while (g_running) {
        int rc = ring_buffer__poll(ring, 100);
        if (rc < 0) break;

        /* check if slave disconnected */
        char c;
        ssize_t r = recv(g_peer_fd, &c, 1, MSG_PEEK | MSG_DONTWAIT);
        if (r == 0) break;  /* peer closed */
        if (r < 0 && errno != EAGAIN && errno != EWOULDBLOCK) break;
    }

    ring_buffer__free(ring);
    bpf_object__close(obj);
    close(cgfd);
    return 0;
}

static int master_run(int kv_port, int repl_port)
{
    printf("[AGENT-M] listening on 127.0.0.1:%d, kv_port=%d\n", repl_port, kv_port);

    int listen_fd = tcp_listen(repl_port);
    if (listen_fd < 0) return 1;

    while (g_running) {
        printf("[AGENT-M] waiting for slave...\n");
        g_peer_fd = accept(listen_fd, NULL, NULL);
        if (g_peer_fd < 0) { if (errno == EINTR) continue; perror("accept"); break; }
        printf("[AGENT-M] slave connected\n");

        if (master_full_sync(g_peer_fd, kv_port) != 0) {
            close(g_peer_fd); g_peer_fd = -1;
            continue;
        }

        if (master_bpf_load(kv_port) != 0) {
            close(g_peer_fd); g_peer_fd = -1;
            continue;
        }

        close(g_peer_fd);
        g_peer_fd = -1;
        printf("[AGENT-M] slave disconnected\n");
    }

    close(listen_fd);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * SLAVE
 * ═══════════════════════════════════════════════════════════════════ */

static int slave_run(const char *master_ip, int kv_port, int repl_port)
{
    while (g_running) {
        printf("[AGENT-S] connecting to %s:%d\n", master_ip, repl_port);
        g_peer_fd = tcp_connect(master_ip, repl_port);
        if (g_peer_fd < 0) { sleep(1); continue; }

        g_kv_fd = tcp_connect("127.0.0.1", kv_port);
        if (g_kv_fd < 0) { close(g_peer_fd); sleep(1); continue; }

        printf("[AGENT-S] receiving full sync...\n");
        uint8_t buf[REPL_OBSERVE_MAX_DATA];
        int fsync = 1;

        while (g_running) {
            uint32_t dlen;
            if (frame_recv(g_peer_fd, buf, sizeof(buf), &dlen) != 0)
                break;

            if (fsync && dlen == REPL_MARKER_FULLSYNC) {
                printf("[AGENT-S] full sync done, incremental mode\n");
                fsync = 0;
                continue;
            }

            if (dlen > 0) {
                ssize_t w = write(g_kv_fd, buf, dlen);
                if (w < 0) { perror("[AGENT-S] write kvstore"); break; }
            }
        }

        close(g_kv_fd); g_kv_fd = -1;
        close(g_peer_fd); g_peer_fd = -1;
        printf("[AGENT-S] disconnected, will reconnect...\n");
        sleep(1);
    }
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * MAIN
 * ═══════════════════════════════════════════════════════════════════ */

static void usage(const char *p)
{
    fprintf(stderr,
        "Usage:\n"
        "  %s master <kv_port>            [repl_port]\n"
        "  %s slave  <master_ip> <kv_port> [repl_port]\n"
        "  repl_port default: %d\n", p, p, REPL_PORT);
}

int main(int argc, char *argv[])
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    if (argc < 2) { usage(argv[0]); return 1; }
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    if (strcmp(argv[1], "master") == 0) {
        if (argc < 3) { usage(argv[0]); return 1; }
        return master_run(atoi(argv[2]), argc >= 4 ? atoi(argv[3]) : REPL_PORT);
    } else if (strcmp(argv[1], "slave") == 0) {
        if (argc < 4) { usage(argv[0]); return 1; }
        return slave_run(argv[2], atoi(argv[3]), argc >= 5 ? atoi(argv[4]) : REPL_PORT);
    } else {
        usage(argv[0]); return 1;
    }
}
