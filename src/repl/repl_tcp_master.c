/* repl_tcp_master.c — Master-side replication logic with stream reassembly
 *
 *   - RESP command reassembly from eBPF ringbuf events (multi-segment support)
 *   - Per-socket buffer management with GC
 *   - eBPF program loading (sockops + sockmap + ringbuf)
 *   - Sync request handling (partial resync / fullsync + fork)
 *   - Master thread (poll ringbuf + accept slaves + process notify queue)
 */
#define _GNU_SOURCE
#include "repl_tcp_internal.h"
#include "repl_observe.h"

/* Event flags (ensure they match repl_observe.h) */
#define REPL_FLAG_FIRST  0x01
#define REPL_FLAG_LAST   0x02

/* ── Global stream reassembly state ─────────────────────────────────────── */

static struct {
    struct sock_stream slots[MAX_STREAM_BUFFERS];
} g_streams = {0};

/* ── RESP Protocol Parser ───────────────────────────────────────────────── */

int find_complete_resp_command(const uint8_t *data, size_t len)
{
    if (len < 4 || data[0] != '*') return -1;

    /* Parse array count: *N\r\n */
    size_t i = 1;
    int argc = 0;
    while (i < len && data[i] >= '0' && data[i] <= '9') {
        argc = argc * 10 + (data[i] - '0');
        i++;
    }
    if (i + 1 >= len || data[i] != '\r' || data[i + 1] != '\n') return -1;
    i += 2;

    /* Parse each argument: $L\r\n<data>\r\n */
    for (int arg = 0; arg < argc; arg++) {
        if (i >= len || data[i] != '$') return -1;
        i++;
        int arg_len = 0;
        while (i < len && data[i] >= '0' && data[i] <= '9') {
            arg_len = arg_len * 10 + (data[i] - '0');
            i++;
        }
        if (i + 1 >= len || data[i] != '\r' || data[i + 1] != '\n') return -1;
        i += 2;
        i += arg_len + 2;
        if (i > len) return -1;
    }
    return (int)i;
}

/* ── Stream management helpers ──────────────────────────────────────────── */

static struct sock_stream *stream_get(uint64_t cookie)
{
    for (int i = 0; i < MAX_STREAM_BUFFERS; i++) {
        if (g_streams.slots[i].sock_cookie == cookie)
            return &g_streams.slots[i];
    }
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

    /* All slots occupied - recycle oldest timeout buffer */
    time_t now = time(NULL);
    struct sock_stream *oldest = NULL;
    for (int i = 0; i < MAX_STREAM_BUFFERS; i++) {
        if (now - g_streams.slots[i].last_active > STREAM_GC_TIMEOUT) {
            if (g_streams.slots[i].buffer) {
                free(g_streams.slots[i].buffer);
            }
            memset(&g_streams.slots[i], 0, sizeof(struct sock_stream));
            s = &g_streams.slots[i];
            s->sock_cookie = cookie;
            return s;
        }
        if (!oldest || g_streams.slots[i].last_active < oldest->last_active)
            oldest = &g_streams.slots[i];
    }

    /* Force recycle oldest if all active */
    fprintf(stderr, "[REPL-TCP] Stream buffer exhausted, dropping oldest (cookie=%lu)\n",
            (unsigned long)oldest->sock_cookie);
    if (oldest->buffer) free(oldest->buffer);
    memset(oldest, 0, sizeof(*oldest));
    oldest->sock_cookie = cookie;
    return oldest;
}

static int stream_ensure_capacity(struct sock_stream *s, uint32_t needed)
{
    if (needed <= s->capacity)
        return 0;
    uint32_t new_cap = s->capacity ? s->capacity * 2 : 4096;
    while (new_cap < needed)
        new_cap *= 2;
    uint8_t *p = realloc(s->buffer, new_cap);
    if (!p) {
        fprintf(stderr, "[REPL-TCP] Failed to allocate %u bytes\n", new_cap);
        return -1;
    }
    s->buffer = p;
    s->capacity = new_cap;
    return 0;
}

static void stream_cleanup_stale(void)
{
    time_t now = time(NULL);
    for (int i = 0; i < MAX_STREAM_BUFFERS; i++) {
        struct sock_stream *s = &g_streams.slots[i];
        if (s->sock_cookie != 0 && now - s->last_active > STREAM_GC_TIMEOUT) {
            fprintf(stderr, "[REPL-TCP] GC stale stream cookie=%lu written=%u\n",
                    (unsigned long)s->sock_cookie, s->written);
            if (s->buffer) free(s->buffer);
            memset(s, 0, sizeof(*s));
        }
    }
}

/* ── eBPF ringbuf callback (with stream reassembly) ──────────────────────── */

int master_on_data(void *ctx, void *data, size_t data_sz)
{
    (void)ctx; (void)data_sz;
    struct repl_observe_event *e = (struct repl_observe_event *)data;

    if (e->len == 0 || e->len > REPL_OBSERVE_MAX_DATA)
        return 0;

    struct sock_stream *s = stream_get_or_create(e->sock_cookie);
    if (!s) return 0;

    /* FIRST: Check for residual data from previous command boundary */
    if (e->flags & REPL_FLAG_FIRST) {
        /* If written > 0, previous LAST had residual data */
        /* Simplification: if residual exists but new command is multi-segment, reset */
        if (s->written > 0 && !(e->flags & REPL_FLAG_LAST)) {
            s->written = 0;
        }
        /* Single-segment command (FIRST+LAST) is unaffected */
    }

    if (stream_ensure_capacity(s, s->written + e->len) != 0)
        return 0;

    /* Append data: BPF offset is relative, actual position is written + offset */
    memcpy(s->buffer + s->written, e->data, e->len);
    s->written += e->len;
    s->last_active = time(NULL);

    /* LAST: Command complete, submit */
    if (e->flags & REPL_FLAG_LAST) {
        int cmd_len = find_complete_resp_command(s->buffer, s->written);
        if (cmd_len > 0) {
            repl_incr_append((const char *)s->buffer, cmd_len);

            if (g_repl.peer_fd >= 0) {
                if (tcp_send_data_frame(g_repl.peer_fd, s->buffer, cmd_len) == 0) {
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

            /* Key fix: Preserve residual data after command boundary */
            if (cmd_len < (int)s->written) {
                size_t remain = s->written - cmd_len;
                memmove(s->buffer, s->buffer + cmd_len, remain);
                s->written = remain;
            } else {
                s->written = 0;
            }
        } else {
            /* Parse failed - reset buffer */
            s->written = 0;
        }
    }

    return 0;
}

/* ── eBPF loader ────────────────────────────────────────────────────────── */

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

    struct bpf_program *sockops = bpf_object__find_program_by_name(g_repl.bpf_obj, "repl_filter_sockops");
    if (sockops)
        bpf_prog_attach(bpf_program__fd(sockops), g_repl.cgroup_fd, BPF_CGROUP_SOCK_OPS, 0);

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

    printf("[REPL-TCP] eBPF filter loaded with stream reassembly, monitoring port %u\n",
           g_repl.cfg.kv_port);
    return 0;
}

/* ── Master sync request handler ────────────────────────────────────────── */

static int master_handle_sync_request(int fd)
{
    struct sync_request req;
    struct sync_response resp;

    if (tcp_recv_exact(fd, &req, sizeof(req)) != 0) {
        fprintf(stderr, "[REPL-TCP] Failed to receive sync request\n");
        return -1;
    }
    if (ntohl(req.hdr.magic) != CTRL_MAGIC || ntohl(req.hdr.type) != CTRL_MSG_SYNC_REQUEST) {
        fprintf(stderr, "[REPL-TCP] Invalid sync message\n");
        return -1;
    }

    uint64_t last_offset = be64toh(req.last_offset);
    printf("[REPL-TCP] Slave requests sync from offset %lu\n", last_offset);

    /* If slave offset is 0 and master has data, require full sync */
    extern uint32_t kv_store_count(void);
    uint32_t entry_count = kv_store_count();
    if (last_offset == 0 && entry_count > 0) {
        printf("[REPL-TCP] Slave offset=0 but master has %u entries, requiring full sync\n",
               entry_count);
        goto fullsync;
    }

    if (repl_incr_has_offset(last_offset)) {
        /* Partial resync */
        resp.hdr.magic = htonl(CTRL_MAGIC);
        resp.hdr.type = htonl(CTRL_MSG_SYNC_RESPONSE);
        resp.hdr.timestamp = htobe64(time(NULL) * 1000);
        resp.status = htonl(SYNC_STATUS_OK);
        resp.start_offset = htobe64(last_offset);
        resp.base_seq = 0;
        resp.rdma_port = 0;
        resp.padding = 0;

        char *catchup_buf = malloc(REPL_MAX_FRAME_SIZE);
        if (!catchup_buf) { fprintf(stderr, "[REPL-TCP] malloc failed\n"); return -1; }

        size_t catchup_len = 0;
        if (repl_incr_get_range(last_offset, catchup_buf, REPL_MAX_FRAME_SIZE, &catchup_len) == 0) {
            resp.data_len = htobe64(catchup_len);
            if (send(fd, &resp, sizeof(resp), MSG_NOSIGNAL) != sizeof(resp)) {
                free(catchup_buf); return -1;
            }
            if (catchup_len > 0) {
                send(fd, catchup_buf, catchup_len, MSG_NOSIGNAL);
                printf("[REPL-TCP] Sent %zu bytes of catchup data\n", catchup_len);
            } else {
                printf("[REPL-TCP] Slave is up-to-date\n");
            }
        } else {
            resp.status = htonl(SYNC_STATUS_FULLSYNC_REQUIRED);
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

        /* Get catchup data BEFORE waiting for FULLSYNC_DONE */
        uint64_t current_offset = repl_incr_get_master_offset();
        char *catchup_buf = NULL;
        size_t catchup_len = 0;
        if (current_offset > base_seq) {
            catchup_buf = malloc(REPL_MAX_FRAME_SIZE);
            if (catchup_buf && repl_incr_get_range(base_seq, catchup_buf,
                    REPL_MAX_FRAME_SIZE, &catchup_len) != 0)
                catchup_len = 0;
        }

        resp.hdr.magic = htonl(CTRL_MAGIC);
        resp.hdr.type = htonl(CTRL_MSG_SYNC_RESPONSE);
        resp.hdr.timestamp = htobe64(time(NULL) * 1000);
        resp.status = htonl(SYNC_STATUS_FULLSYNC_REQUIRED);
        resp.start_offset = 0;
        resp.data_len = htobe64(catchup_len);
        resp.base_seq = htobe64(base_seq);
        resp.rdma_port = htons(2101);
        resp.padding = 0;

        if (send(fd, &resp, sizeof(resp), MSG_NOSIGNAL) != sizeof(resp)) {
            free(catchup_buf); return -1;
        }
        printf("[REPL-TCP] Sent fullsync response (base_seq=%lu, catchup=%zu)\n",
               base_seq, catchup_len);

        /* Send framed catchup data BEFORE waiting for fullsync */
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

/* ── Master thread (with integrated GC) ─────────────────────────────────── */

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

        /* GC stale stream buffers every STREAM_GC_INTERVAL seconds */
        time_t now = time(NULL);
        if (now - last_gc >= STREAM_GC_INTERVAL) {
            stream_cleanup_stale();
            last_gc = now;
        }

        if (g_repl.peer_fd < 0) {
            struct sockaddr_in slave_addr;
            socklen_t addrlen = sizeof(slave_addr);
            int fd = accept(g_repl.listen_fd, (struct sockaddr *)&slave_addr, &addrlen);
            if (fd >= 0) {
                tcp_set_options(fd);
                if (master_handle_sync_request(fd) == 0) {
                    g_repl.peer_fd = fd;
                    update_last_heartbeat();
                    pthread_mutex_lock(&g_repl.stats_lock);
                    g_repl.stats.connected = 1;
                    pthread_mutex_unlock(&g_repl.stats_lock);
                    printf("[REPL-TCP] Slave connected from %s:%u\n",
                           inet_ntoa(slave_addr.sin_addr), ntohs(slave_addr.sin_port));
                } else {
                    close(fd);
                }
            }
        }

        usleep(1000);
    }

    /* Cleanup all stream buffers on exit */
    for (int i = 0; i < MAX_STREAM_BUFFERS; i++) {
        if (g_streams.slots[i].buffer) {
            free(g_streams.slots[i].buffer);
        }
    }

    printf("[REPL-TCP] Master thread stopped\n");
    return NULL;
}
