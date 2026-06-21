/* repl_tcp_master.c — Master-side replication logic
 *
 *   - RESP command reassembly from eBPF ringbuf events
 *   - eBPF program loading (sockops + sockmap + ringbuf)
 *   - Sync request handling (partial resync / fullsync + fork)
 *   - Master thread (poll ringbuf + accept slaves)
 */
#define _GNU_SOURCE
#include "repl_tcp_internal.h"

/* ── RESP Protocol Parser ───────────────────────────────────────────── */

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

/* ── Reassembly Context Management ──────────────────────────────────── */

static inline uint32_t hash_cookie(uint64_t cookie)
{
    return (uint32_t)(cookie % MAX_CONNECTIONS);
}

struct reassembly_ctx *get_or_create_ctx(uint64_t cookie)
{
    uint32_t hash = hash_cookie(cookie);
    pthread_mutex_lock(&g_reassembly.lock);

    struct reassembly_ctx *ctx = g_reassembly.table[hash];
    while (ctx) {
        if (ctx->sock_cookie == cookie) {
            pthread_mutex_unlock(&g_reassembly.lock);
            return ctx;
        }
        ctx = ctx->next;
    }

    ctx = (struct reassembly_ctx *)calloc(1, sizeof(*ctx));
    if (!ctx) { pthread_mutex_unlock(&g_reassembly.lock); return NULL; }

    ctx->sock_cookie = cookie;
    ctx->buf_len = 0;
    ctx->last_active = time(NULL);

    ctx->next = g_reassembly.table[hash];
    g_reassembly.table[hash] = ctx;
    pthread_mutex_unlock(&g_reassembly.lock);
    return ctx;
}

void cleanup_stale_contexts(void)
{
    time_t now = time(NULL);
    pthread_mutex_lock(&g_reassembly.lock);

    for (int i = 0; i < MAX_CONNECTIONS; i++) {
        struct reassembly_ctx **prev = &g_reassembly.table[i];
        struct reassembly_ctx *ctx = *prev;
        while (ctx) {
            if (now - ctx->last_active > REASSEMBLY_TIMEOUT) {
                *prev = ctx->next; free(ctx); ctx = *prev;
            } else {
                prev = &ctx->next; ctx = ctx->next;
            }
        }
    }
    pthread_mutex_unlock(&g_reassembly.lock);
}

void cleanup_all_contexts(void)
{
    pthread_mutex_lock(&g_reassembly.lock);
    for (int i = 0; i < MAX_CONNECTIONS; i++) {
        struct reassembly_ctx *ctx = g_reassembly.table[i];
        while (ctx) {
            struct reassembly_ctx *next = ctx->next;
            free(ctx); ctx = next;
        }
        g_reassembly.table[i] = NULL;
    }
    pthread_mutex_unlock(&g_reassembly.lock);
}

/* ── eBPF ringbuf callback ──────────────────────────────────────────── */

int master_on_data(void *ctx, void *data, size_t len)
{
    (void)ctx; (void)len;
    struct repl_observe_event *e = (struct repl_observe_event *)data;
    if (e->len == 0 || e->len > REPL_OBSERVE_MAX_DATA) return 0;

    struct reassembly_ctx *rctx = get_or_create_ctx(e->sock_cookie);
    if (!rctx) {
        fprintf(stderr, "[REPL-TCP] Failed to get reassembly context\n");
        return 0;
    }

    if (rctx->buf_len + e->len > REASSEMBLY_BUF_SIZE) {
        fprintf(stderr, "[REPL-TCP] Reassembly buffer overflow, resetting (cookie=%lu)\n",
                e->sock_cookie);
        rctx->buf_len = 0;
    }

    memcpy(rctx->buf + rctx->buf_len, e->data, e->len);
    rctx->buf_len += e->len;
    rctx->last_active = time(NULL);

    while (rctx->buf_len > 0) {
        int cmd_len = find_complete_resp_command(rctx->buf, rctx->buf_len);
        if (cmd_len <= 0) break;

        repl_incr_append((const char *)rctx->buf, cmd_len);

        if (g_repl.peer_fd >= 0) {
            if (tcp_send_data_frame(g_repl.peer_fd, rctx->buf, cmd_len) == 0) {
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

        memmove(rctx->buf, rctx->buf + cmd_len, rctx->buf_len - cmd_len);
        rctx->buf_len -= cmd_len;
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

    printf("[REPL-TCP] eBPF filter loaded, monitoring port %u\n", g_repl.cfg.kv_port);
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
fullsync:
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

/* ── Master thread ──────────────────────────────────────────────────── */

void *master_thread_func(void *arg)
{
    (void)arg;
    printf("[REPL-TCP] Master thread started\n");

    int flags = fcntl(g_repl.listen_fd, F_GETFL, 0);
    fcntl(g_repl.listen_fd, F_SETFL, flags | O_NONBLOCK);

    int cleanup_counter = 0;

    while (g_repl.running) {
        if (g_repl.ringbuf)
            ring_buffer__poll(g_repl.ringbuf, 1);

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

        if (++cleanup_counter >= 10000) {
            cleanup_stale_contexts();
            cleanup_counter = 0;
        }
        usleep(1000);
    }

    printf("[REPL-TCP] Master thread stopped\n");
    return NULL;
}
