/* repl_tcp_master.c — Master-side replication (XDP path)
 *
 * Pipeline:
 *   XDP BPF → ringbuf → xdp_reasm_on_event()  (repl_xdp_reasm.c)
 *            → seq dedup + ordering + linear buffer
 *            → find_complete_resp_command()    (this file)
 *            → is_write_command()              (this file)
 *            → forward_to_slave()              (this file)
 *
 * This file contains:
 *   - RESP protocol parser
 *   - Write-command filter
 *   - Forward to slave (TCP data frame + backlog append)
 *   - Sync request handler
 *   - Master thread (accept + ringbuf poll + GC)
 */

#define _GNU_SOURCE
#include "repl_tcp_internal.h"
#include "rdma_repl.h"

#include <stdlib.h>

/* Max RESP command length (sanity cap) */
#define MAX_RESP_CMD_LEN  (4 * 1024 * 1024)

/* GC interval for XDP connection table (seconds) */
#define XDP_GC_INTERVAL    5

/* ── Write-command filter ─────────────────────────────────────────────
 *
 * Checks the first 3 chars of the command name (after RESP header).
 * False positives are harmless — extra backlog append, slave replays idempotently.
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
            if (arg_len > MAX_RESP_CMD_LEN) return -1;
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

/* ── Forward a verified write command to slave ──────────────────────── */

void forward_to_slave(const uint8_t *cmd, uint32_t cmd_len)
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

        /* Kill previous fullsync child to free RDMA_CM port 2101.
         * Otherwise fi_getinfo fails for the new child ("No data available"). */
        session_kill_all();

        extern int master_fullsync_child(uint64_t base_seq, int ready_fd);

        /* Pipe for child to signal readiness before parent responds */
        int pipefd[2];
        if (pipe(pipefd) < 0) { perror("[REPL-TCP] pipe"); return -1; }

        pid_t child_pid = fork();
        if (child_pid < 0) { perror("[REPL-TCP] fork"); close(pipefd[0]); close(pipefd[1]); return -1; }

        if (child_pid == 0) {
            close(pipefd[0]);   /* child only writes */
            close(fd);
            if (g_repl.listen_fd >= 0) close(g_repl.listen_fd);
            if (g_repl.kv_fd >= 0) close(g_repl.kv_fd);
            int rc = master_fullsync_child(base_seq, pipefd[1]);
            _exit(rc == 0 ? 0 : 1);
        }

        close(pipefd[1]);  /* parent only reads */

        /* Wait for child to fi_listen() before responding to slave */
        {
            char ready = 0;
            ssize_t n = read(pipefd[0], &ready, 1);
            close(pipefd[0]);
            if (n != 1 || ready != 1) {
                fprintf(stderr, "[REPL-TCP] Child failed to become ready\n");
                return -1;
            }
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
    printf("[REPL-TCP] Master thread started (XDP mode)\n");

    int flags = fcntl(g_repl.listen_fd, F_GETFL, 0);
    fcntl(g_repl.listen_fd, F_SETFL, flags | O_NONBLOCK);

    time_t last_gc = time(NULL);

    while (g_repl.running) {
        if (g_repl.ringbuf)
            ring_buffer__poll(g_repl.ringbuf, 1);

        time_t now = time(NULL);
        if (now - last_gc >= XDP_GC_INTERVAL) {
            xdp_reasm_gc();
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

    printf("[REPL-TCP] Master thread stopped\n");
    return NULL;
}
