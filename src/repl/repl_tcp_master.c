/* repl_tcp_master.c — Master-side replication with linear stream reassembly
 *
 *   - eBPF ringbuf → linear per-socket buffer (ordered delivery, no OOO)
 *   - RESP command extraction + write-command filter
 *   - Backlog append + slave forwarding
 *   - eBPF loader + sync request handler + master thread
 *
 * Key design (vs old FIRST/LAST/offset approach):
 *   - eBPF does zero protocol parsing — just copies raw skb bytes
 *   - User-space appends each event to a linear buffer, then loops:
 *       extract complete RESP cmd → if write → backlog + forward
 *     Pipelines naturally handled: multiple cmds per segment work too.
 *   - Read commands are consumed and discarded (no forwarding).
 */

#define _GNU_SOURCE
#include "repl_tcp_internal.h"
#include "repl_observe.h"

#include <stdlib.h>

/* ── Global stream reassembly state ─────────────────────────────────── */

static struct {
    struct sock_stream slots[MAX_STREAM_BUFFERS];
} g_streams;

/* ── Write-command filter ─────────────────────────────────────────────
 *
 * Checks the first 3 chars of the command name (after RESP header).
 * False positives (e.g. SUBSTR matching 'S') are harmless — they just
 * result in an extra backlog append that the slave replays harmlessly.
 * False negatives would be catastrophic, so we err on the inclusive side. */

int is_write_command(const uint8_t *data, size_t len)
{
    if (!data || len < 12 || data[0] != '*') return 0;

    /* Skip *N\r\n */
    size_t i = 1;
    while (i < len && data[i] >= '0' && data[i] <= '9') i++;
    if (i + 1 >= len || data[i] != '\r' || data[i + 1] != '\n') return 0;
    i += 2;

    /* Skip $L\r\n to reach command name */
    if (i >= len || data[i] != '$') return 0;
    i++;
    while (i < len && data[i] >= '0' && data[i] <= '9') i++;
    if (i + 1 >= len || data[i] != '\r' || data[i + 1] != '\n') return 0;
    i += 2;

    if (i + 2 >= len) return 0;

    /* Uppercase first 3 chars */
    char c0 = (char)data[i];
    char c1 = (char)data[i + 1];
    char c2 = (char)data[i + 2];
    if (c0 >= 'a' && c0 <= 'z') c0 -= 32;
    if (c1 >= 'a' && c1 <= 'z') c1 -= 32;
    if (c2 >= 'a' && c2 <= 'z') c2 -= 32;

    /* Whitelist of write-command prefixes */
    if (c0 == 'S' && c1 == 'E' && c2 == 'T') return 1;  /* SET */
    if (c0 == 'D' && c1 == 'E' && c2 == 'L') return 1;  /* DEL */
    if (c0 == 'D' && c1 == 'E' && c2 == 'C') return 1;  /* DECR */
    if (c0 == 'L' && c1 == 'P')              return 1;  /* LPUSH/LPOP */
    if (c0 == 'R' && c1 == 'P')              return 1;  /* RPUSH/RPOP */
    if (c0 == 'H' && (c1 == 'S' || c1 == 'D')) return 1; /* HSET/HDEL */
    if (c0 == 'S' && (c1 == 'A' || c1 == 'R')) return 1; /* SADD/SREM */
    if (c0 == 'Z' && (c1 == 'A' || c1 == 'R')) return 1; /* ZADD/ZREM */
    if (c0 == 'I' && c1 == 'N' && c2 == 'C') return 1;  /* INCR */
    if (c0 == 'A' && c1 == 'P' && c2 == 'P') return 1;  /* APPEND */
    if (c0 == 'M' && c1 == 'S' && c2 == 'E') return 1;  /* MSET */
    if (c0 == 'F' && c1 == 'L' && c2 == 'U') return 1;  /* FLUSHALL/FLUSHDB */

    return 0;
}

/* ── RESP parser ──────────────────────────────────────────────────────
 *
 * Returns:
 *   > 0  — complete command length
 *   = 0  — need more data (incomplete)
 *   < 0  — protocol error (should discard) */

int find_complete_resp_command(const uint8_t *data, size_t len)
{
    if (len < 4 || data[0] != '*') return -1;

    /* Parse array count: *N\r\n */
    size_t i = 1;
    int argc = 0;
    while (i < len && data[i] >= '0' && data[i] <= '9') {
        argc = argc * 10 + (data[i] - '0');
        if (argc > 64) return -1;  /* sanity: max 64 args */
        i++;
    }
    if (i == 1) return -1;  /* no digits after '*' */
    if (i + 1 >= len) return 0;  /* CRLF not fully arrived */
    if (data[i] != '\r' || data[i + 1] != '\n') return -1;
    i += 2;

    /* Parse each argument: $L\r\n<data>\r\n */
    for (int arg = 0; arg < argc; arg++) {
        if (i >= len) return 0;
        if (data[i] != '$') return -1;
        i++;

        int arg_len = 0;
        while (i < len && data[i] >= '0' && data[i] <= '9') {
            arg_len = arg_len * 10 + (data[i] - '0');
            if (arg_len > REPL_OBSERVE_MAX_CMD) return -1;
            i++;
        }
        if (i + 1 >= len) return 0;
        if (data[i] != '\r' || data[i + 1] != '\n') return -1;
        i += 2;

        /* Jump over bulk data + trailing \r\n */
        if (i + (size_t)arg_len + 2 > len) return 0;
        i += (size_t)arg_len + 2;
    }

    return (int)i;
}

/* ── Stream management ──────────────────────────────────────────────── */

static struct sock_stream *stream_get(uint64_t cookie)
{
    for (int i = 0; i < MAX_STREAM_BUFFERS; i++)
        if (g_streams.slots[i].sock_cookie == cookie)
            return &g_streams.slots[i];
    return NULL;
}

static struct sock_stream *stream_get_or_create(uint64_t cookie)
{
    struct sock_stream *s = stream_get(cookie);
    if (s) return s;

    /* Find free slot */
    for (int i = 0; i < MAX_STREAM_BUFFERS; i++) {
        if (g_streams.slots[i].sock_cookie == 0) {
            s = &g_streams.slots[i];
            memset(s, 0, sizeof(*s));
            s->sock_cookie = cookie;
            return s;
        }
    }

    /* Recycle stale */
    time_t now = time(NULL);
    for (int i = 0; i < MAX_STREAM_BUFFERS; i++) {
        struct sock_stream *c = &g_streams.slots[i];
        if (now - c->last_active > STREAM_GC_TIMEOUT) {
            free(c->buf);
            memset(c, 0, sizeof(*c));
            c->sock_cookie = cookie;
            return c;
        }
    }

    /* Force-recycle oldest */
    struct sock_stream *oldest = &g_streams.slots[0];
    for (int i = 1; i < MAX_STREAM_BUFFERS; i++)
        if (g_streams.slots[i].last_active < oldest->last_active)
            oldest = &g_streams.slots[i];

    fprintf(stderr, "[REPL-TCP] Stream slots exhausted, evicting cookie=%lu\n",
            (unsigned long)oldest->sock_cookie);
    free(oldest->buf);
    memset(oldest, 0, sizeof(*oldest));
    oldest->sock_cookie = cookie;
    return oldest;
}

static int stream_ensure_capacity(struct sock_stream *s, uint32_t needed)
{
    if (needed <= s->cap) return 0;

    uint32_t new_cap = s->cap ? s->cap : 4096;
    while (new_cap < needed) {
        if (new_cap > REPL_OBSERVE_MAX_CMD) return -1;
        new_cap *= 2;
    }
    uint8_t *p = realloc(s->buf, new_cap);
    if (!p) return -1;
    s->buf = p;
    s->cap = new_cap;
    return 0;
}

static void stream_cleanup_stale(void)
{
    time_t now = time(NULL);
    for (int i = 0; i < MAX_STREAM_BUFFERS; i++) {
        struct sock_stream *s = &g_streams.slots[i];
        if (s->sock_cookie && now - s->last_active > STREAM_GC_TIMEOUT) {
            free(s->buf);
            memset(s, 0, sizeof(*s));
        }
    }
}

/* ── Forward a verified write command to slave ──────────────────────── */

static void forward_to_slave(const uint8_t *cmd, uint32_t cmd_len)
{
    repl_incr_append((const char *)cmd, (size_t)cmd_len);

    if (g_repl.peer_fd < 0) return;

    if (tcp_send_data_frame(g_repl.peer_fd, cmd, cmd_len) == 0) {
        pthread_mutex_lock(&g_repl.stats_lock);
        g_repl.stats.bytes_sent += cmd_len + REPL_FRAME_HEADER;
        g_repl.stats.frames_sent++;
        pthread_mutex_unlock(&g_repl.stats_lock);
    } else {
        close(g_repl.peer_fd);
        g_repl.peer_fd = -1;
        pthread_mutex_lock(&g_repl.stats_lock);
        g_repl.stats.connected = 0;
        pthread_mutex_unlock(&g_repl.stats_lock);
        printf("[REPL-TCP] Slave disconnected\n");
    }
}

/* ── eBPF ringbuf callback ────────────────────────────────────────────
 *
 * Linear reassembly + command extraction + write filter + forward.
 *
 * For each event:
 *   1. Append raw bytes to per-socket linear buffer
 *   2. Loop: extract complete RESP commands from buffer head
 *      - Write command → backlog + forward to slave
 *      - Read command  → skip (consumed, not forwarded)
 *   3. Compact remaining partial data to buffer head
 *
 * Pipelined commands (multiple cmds in one TCP segment) are naturally
 * handled by the extraction loop. */

int master_on_data(void *ctx, void *data, size_t data_sz)
{
    (void)ctx; (void)data_sz;
    struct repl_observe_event *e = (struct repl_observe_event *)data;

    if (e->len == 0 || e->len > REPL_OBSERVE_MAX_DATA)
        return 0;

    struct sock_stream *s = stream_get_or_create(e->sock_cookie);
    if (!s) return 0;

    /* Grow if needed (cap exponential growth) */
    if (stream_ensure_capacity(s, s->len + e->len) != 0) {
        /* Buffer too large — likely garbage, reset */
        s->len = 0;
        return 0;
    }

    /* Append to linear buffer */
    memcpy(s->buf + s->len, e->data, e->len);
    s->len += e->len;
    s->last_active = time(NULL);

    /* Extract all complete commands from the head of the buffer.
     * Typical case: 1 command.  Pipeline case: N commands. */
    int extracted = 0;
    while (1) {
        int cmd_len = find_complete_resp_command(s->buf, s->len);
        if (cmd_len <= 0) {
            /* 0 = incomplete (wait for more data)
             * -1 = parse error (garbage — discard accumulated data) */
            if (cmd_len < 0 && s->len > REPL_OBSERVE_MAX_CMD) {
                /* Stale/garbage data accumulated, reset */
                s->len = 0;
            }
            break;
        }

        /* Check if write command — if so, replicate */
        if (is_write_command(s->buf, (size_t)cmd_len))
            forward_to_slave(s->buf, (uint32_t)cmd_len);

        extracted++;

        /* Consume this command from the buffer */
        s->len -= (uint32_t)cmd_len;
        if (s->len > 0)
            memmove(s->buf, s->buf + cmd_len, s->len);
    }

    /* Keep small buffers (<64KB) to avoid constant realloc.
     * Free larger ones to bound memory. */
    if (s->len == 0 && s->cap > 65536) {
        free(s->buf);
        s->buf = NULL;
        s->cap = 0;
    }

    return 0;
}

/* ── eBPF loader ────────────────────────────────────────────────────── */

int master_load_bpf(void)
{
    char obj_path[512];
    const char *build_dir = getenv("BUILD_DIR");
    if (!build_dir) {
        char self[256];
        ssize_t n = readlink("/proc/self/exe", self, sizeof(self) - 1);
        if (n > 0) {
            self[n] = '\0';
            char *slash = strrchr(self, '/');
            if (slash) *slash = '\0';
            snprintf(obj_path, sizeof(obj_path), "%s/repl_filter.bpf.o", self);
            if (access(obj_path, R_OK) != 0)
                snprintf(obj_path, sizeof(obj_path), "build/repl_filter.bpf.o");
        } else {
            snprintf(obj_path, sizeof(obj_path), "build/repl_filter.bpf.o");
        }
    } else {
        snprintf(obj_path, sizeof(obj_path), "%s/repl_filter.bpf.o", build_dir);
    }

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

    struct bpf_map *cfg_map = bpf_object__find_map_by_name(g_repl.bpf_obj, "repl_config");
    if (cfg_map) {
        uint32_t key = 0, val = g_repl.cfg.kv_port;
        bpf_map__update_elem(cfg_map, &key, sizeof(key), &val, sizeof(val), BPF_ANY);
    }

    g_repl.cgroup_fd = open("/sys/fs/cgroup", O_RDONLY | O_DIRECTORY);
    if (g_repl.cgroup_fd < 0) {
        fprintf(stderr, "[REPL-TCP] open cgroup: %s\n", strerror(errno));
        bpf_object__close(g_repl.bpf_obj);
        g_repl.bpf_obj = NULL;
        return -1;
    }

    struct bpf_program *sockops =
        bpf_object__find_program_by_name(g_repl.bpf_obj, "repl_filter_sockops");
    if (sockops)
        bpf_prog_attach(bpf_program__fd(sockops), g_repl.cgroup_fd,
                        BPF_CGROUP_SOCK_OPS, 0);

    struct bpf_map *sockmap =
        bpf_object__find_map_by_name(g_repl.bpf_obj, "repl_sockmap");
    if (sockmap) {
        int map_fd = bpf_map__fd(sockmap);
        struct bpf_program *parser =
            bpf_object__find_program_by_name(g_repl.bpf_obj, "repl_filter_parser");
        if (parser)
            bpf_prog_attach(bpf_program__fd(parser), map_fd,
                            BPF_SK_SKB_STREAM_PARSER, 0);
        struct bpf_program *verdict =
            bpf_object__find_program_by_name(g_repl.bpf_obj, "repl_filter_verdict");
        if (verdict)
            bpf_prog_attach(bpf_program__fd(verdict), map_fd,
                            BPF_SK_SKB_STREAM_VERDICT, 0);
    }

    struct bpf_map *rb_map =
        bpf_object__find_map_by_name(g_repl.bpf_obj, "repl_ringbuf");
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

    printf("[REPL-TCP] eBPF filter loaded, monitoring port %u\n",
           g_repl.cfg.kv_port);
    return 0;
}

/* ── Master sync request handler ────────────────────────────────────── */

static int master_handle_sync_request(int fd)
{
    struct sync_request req;
    struct sync_response resp;

    if (tcp_recv_exact(fd, &req, sizeof(req)) != 0) {
        fprintf(stderr, "[REPL-TCP] Failed to receive sync request\n");
        return -1;
    }
    if (ntohl(req.hdr.magic) != CTRL_MAGIC ||
        ntohl(req.hdr.type) != CTRL_MSG_SYNC_REQUEST) {
        fprintf(stderr, "[REPL-TCP] Invalid sync message\n");
        return -1;
    }

    uint64_t last_offset = be64toh(req.last_offset);
    printf("[REPL-TCP] Slave requests sync from offset %lu\n", last_offset);

    extern uint32_t kv_store_count(void);
    uint32_t entry_count = kv_store_count();
    if (last_offset == 0 && entry_count > 0) {
        printf("[REPL-TCP] Slave offset=0 but master has %u entries,"
               " requiring full sync\n", entry_count);
        goto fullsync;
    }

    if (repl_incr_has_offset(last_offset)) {
        /* Partial resync */
        resp.hdr.magic      = htonl(CTRL_MAGIC);
        resp.hdr.type       = htonl(CTRL_MSG_SYNC_RESPONSE);
        resp.hdr.timestamp  = htobe64(time(NULL) * 1000);
        resp.status         = htonl(SYNC_STATUS_OK);
        resp.start_offset   = htobe64(last_offset);
        resp.base_seq       = 0;
        resp.rdma_port      = 0;
        resp.padding        = 0;

        char *catchup_buf = malloc(REPL_MAX_FRAME_SIZE);
        if (!catchup_buf) {
            fprintf(stderr, "[REPL-TCP] malloc failed\n");
            return -1;
        }

        size_t catchup_len = 0;
        if (repl_incr_get_range(last_offset, catchup_buf,
                                 REPL_MAX_FRAME_SIZE, &catchup_len) == 0) {
            resp.data_len = htobe64(catchup_len);
            if (send(fd, &resp, sizeof(resp), MSG_NOSIGNAL) != sizeof(resp)) {
                free(catchup_buf);
                return -1;
            }
            if (catchup_len > 0) {
                send(fd, catchup_buf, catchup_len, MSG_NOSIGNAL);
                printf("[REPL-TCP] Sent %zu bytes of catchup data\n", catchup_len);
            } else {
                printf("[REPL-TCP] Slave is up-to-date\n");
            }
        } else {
            resp.status  = htonl(SYNC_STATUS_FULLSYNC_REQUIRED);
            resp.data_len = 0;
            send(fd, &resp, sizeof(resp), MSG_NOSIGNAL);
        }
        free(catchup_buf);
    } else {
fullsync:;
        uint64_t base_seq = repl_incr_get_master_offset();
        printf("[REPL-TCP] Triggering full sync (base_seq=%lu)\n", base_seq);

        extern int master_fullsync_child(uint64_t base_seq);
        extern void session_add(pid_t pid);

        pid_t child_pid = fork();
        if (child_pid < 0) { perror("[REPL-TCP] fork"); return -1; }

        if (child_pid == 0) {
            close(fd);
            if (g_repl.listen_fd >= 0) close(g_repl.listen_fd);
            if (g_repl.kv_fd >= 0) close(g_repl.kv_fd);
            int rc = master_fullsync_child(base_seq);
            _exit(rc == 0 ? 0 : 1);
        }

        session_add(child_pid);
        printf("[REPL-TCP] Forked fullsync child pid=%d\n", child_pid);

        uint64_t current_offset = repl_incr_get_master_offset();
        char *catchup_buf = NULL;
        size_t catchup_len = 0;
        if (current_offset > base_seq) {
            catchup_buf = malloc(REPL_MAX_FRAME_SIZE);
            if (catchup_buf && repl_incr_get_range(base_seq, catchup_buf,
                    REPL_MAX_FRAME_SIZE, &catchup_len) != 0)
                catchup_len = 0;
        }

        resp.hdr.magic      = htonl(CTRL_MAGIC);
        resp.hdr.type       = htonl(CTRL_MSG_SYNC_RESPONSE);
        resp.hdr.timestamp  = htobe64(time(NULL) * 1000);
        resp.status         = htonl(SYNC_STATUS_FULLSYNC_REQUIRED);
        resp.start_offset   = 0;
        resp.data_len       = htobe64(catchup_len);
        resp.base_seq       = htobe64(base_seq);
        resp.rdma_port      = htons(2101);
        resp.padding        = 0;

        if (send(fd, &resp, sizeof(resp), MSG_NOSIGNAL) != sizeof(resp)) {
            free(catchup_buf); return -1;
        }
        printf("[REPL-TCP] Sent fullsync response (base_seq=%lu, catchup=%zu)\n",
               base_seq, catchup_len);

        if (catchup_len > 0 && catchup_buf) {
            size_t pos = 0;
            while (pos < catchup_len) {
                int cmd_len = find_complete_resp_command(
                    (const uint8_t *)(catchup_buf + pos), catchup_len - pos);
                if (cmd_len <= 0) break;
                tcp_send_frame(fd, catchup_buf + pos, (uint32_t)cmd_len);
                pos += (size_t)cmd_len;
            }
            free(catchup_buf);
        }

        struct fullsync_done_msg done_msg;
        if (tcp_recv_exact(fd, &done_msg, sizeof(done_msg)) != 0) {
            fprintf(stderr, "[REPL-TCP] Failed to receive FULLSYNC_DONE\n");
            return -1;
        }
        printf("[REPL-TCP] Received FULLSYNC_DONE (base_seq=%lu)\n",
               be64toh(done_msg.base_seq));
    }
    return 0;
}

/* ── Master thread ──────────────────────────────────────────────────── */

void *master_thread_func(void *arg)
{
    (void)arg;
    printf("[REPL-TCP] Master thread started\n");

    int flags = fcntl(g_repl.listen_fd, F_GETFL, 0);
    fcntl(g_repl.listen_fd, F_SETFL, flags | O_NONBLOCK);

    time_t last_gc = time(NULL);

    while (g_repl.running) {
        if (g_repl.ringbuf)
            ring_buffer__poll(g_repl.ringbuf, 1);

        time_t now = time(NULL);
        if (now - last_gc >= STREAM_GC_INTERVAL) {
            stream_cleanup_stale();
            last_gc = now;
        }

        if (g_repl.peer_fd < 0) {
            struct sockaddr_in slave_addr;
            socklen_t addrlen = sizeof(slave_addr);
            int fd = accept(g_repl.listen_fd,
                            (struct sockaddr *)&slave_addr, &addrlen);
            if (fd >= 0) {
                tcp_set_options(fd);
                if (master_handle_sync_request(fd) == 0) {
                    g_repl.peer_fd = fd;
                    update_last_heartbeat();
                    pthread_mutex_lock(&g_repl.stats_lock);
                    g_repl.stats.connected = 1;
                    pthread_mutex_unlock(&g_repl.stats_lock);
                    printf("[REPL-TCP] Slave connected from %s:%u\n",
                           inet_ntoa(slave_addr.sin_addr),
                           ntohs(slave_addr.sin_port));
                } else {
                    close(fd);
                }
            }
        }

        usleep(1000);
    }

    for (int i = 0; i < MAX_STREAM_BUFFERS; i++)
        free(g_streams.slots[i].buf);

    printf("[REPL-TCP] Master thread stopped\n");
    return NULL;
}
