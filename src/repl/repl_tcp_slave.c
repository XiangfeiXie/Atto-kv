/* repl_tcp_slave.c — Slave-side replication logic
 *
 *   - Connect to local kvstore and apply replicated commands
 *   - Send sync request, handle fullsync/partial-resync response
 *   - Slave thread: connect → sync → poll for data frames/heartbeat
 */
#define _GNU_SOURCE
#include "repl_tcp_internal.h"

/* ── Apply command to local kvstore ─────────────────────────────────── */

int slave_apply_command(const uint8_t *data, uint32_t len)
{
    if (g_repl.kv_fd < 0) {
        g_repl.kv_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (g_repl.kv_fd < 0) return -1;

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

    ssize_t n = send(g_repl.kv_fd, data, len, MSG_NOSIGNAL);
    if (n < 0) { close(g_repl.kv_fd); g_repl.kv_fd = -1; return -1; }
    return 0;
}

/* ── Sync request ───────────────────────────────────────────────────── */

static int slave_send_sync_request(int fd, uint64_t last_offset)
{
    struct sync_request req;
    struct sync_response resp;

    req.hdr.magic = htonl(CTRL_MAGIC);
    req.hdr.type = htonl(CTRL_MSG_SYNC_REQUEST);
    req.hdr.timestamp = htobe64(time(NULL) * 1000);
    req.last_offset = htobe64(last_offset);

    if (send(fd, &req, sizeof(req), MSG_NOSIGNAL) != sizeof(req)) {
        fprintf(stderr, "[REPL-TCP] Failed to send sync request\n");
        return -1;
    }
    if (tcp_recv_exact(fd, &resp, sizeof(resp)) != 0) {
        fprintf(stderr, "[REPL-TCP] Failed to receive sync response\n");
        return -1;
    }

    uint32_t status = ntohl(resp.status);
    uint64_t data_len = be64toh(resp.data_len);

    /* ── Fullsync path ──────────────────────────────────────────────── */
    if (status == SYNC_STATUS_FULLSYNC_REQUIRED) {
        uint64_t base_seq = be64toh(resp.base_seq);
        uint16_t rdma_port = ntohs(resp.rdma_port);
        uint64_t catchup_len = be64toh(resp.data_len);

        printf("[REPL-TCP] Master requires full sync (base_seq=%lu, rdma_port=%u, catchup=%lu)\n",
               base_seq, rdma_port, catchup_len);

        /* Receive catchup data BEFORE fullsync */
        if (catchup_len > 0) {
            char *cbuf = malloc(catchup_len);
            if (!cbuf || tcp_recv_exact(fd, cbuf, catchup_len) != 0) {
                free(cbuf);
                return -1;
            }
            size_t pos = 0;
            while (pos < catchup_len) {
                if (pos + 4 > catchup_len) break;
                uint32_t flen;
                memcpy(&flen, cbuf + pos, 4);
                flen = ntohl(flen);
                pos += 4;
                if (pos + flen > catchup_len) break;
                slave_apply_command((uint8_t *)(cbuf + pos), flen);
                pos += flen;
                pthread_mutex_lock(&g_repl.stats_lock);
                g_repl.stats.slave_offset += flen;
                pthread_mutex_unlock(&g_repl.stats_lock);
            }
            free(cbuf);
        }

        extern int rdma_repl_slave_fullsync(const char *master_ip, uint16_t rdma_port);
        if (rdma_repl_slave_fullsync(g_repl.cfg.master_ip, rdma_port) != 0) {
            fprintf(stderr, "[REPL-TCP] RDMA fullsync failed\n");
            return -1;
        }

        printf("[REPL-TCP] RDMA fullsync completed\n");

        struct fullsync_done_msg done;
        done.hdr.magic = htonl(CTRL_MAGIC);
        done.hdr.type = htonl(CTRL_MSG_FULLSYNC_DONE);
        done.hdr.timestamp = htobe64(time(NULL) * 1000);
        done.base_seq = htobe64(base_seq);

        if (send(fd, &done, sizeof(done), MSG_NOSIGNAL) != sizeof(done)) {
            fprintf(stderr, "[REPL-TCP] Failed to send FULLSYNC_DONE\n");
            return -1;
        }
        printf("[REPL-TCP] Sent FULLSYNC_DONE (base_seq=%lu)\n", base_seq);
        return -2;  /* Signal fullsync was performed */
    }

    /* ── Partial resync path ────────────────────────────────────────── */
    if (status != SYNC_STATUS_OK) {
        fprintf(stderr, "[REPL-TCP] Unknown sync status: %u\n", status);
        return -1;
    }

    if (data_len > 0) {
        char *cbuf = malloc(data_len);
        if (!cbuf || tcp_recv_exact(fd, cbuf, data_len) != 0) {
            free(cbuf); return -1;
        }

        printf("[REPL-TCP] Received %lu bytes of catchup data, applying...\n", data_len);
        size_t pos = 0;
        while (pos < data_len) {
            if (pos + 4 > data_len) break;
            uint32_t flen;
            memcpy(&flen, cbuf + pos, 4);
            flen = ntohl(flen);
            pos += 4;
            if (pos + flen > data_len) break;
            slave_apply_command((uint8_t *)(cbuf + pos), flen);
            pos += flen;
            pthread_mutex_lock(&g_repl.stats_lock);
            g_repl.stats.slave_offset += flen;
            pthread_mutex_unlock(&g_repl.stats_lock);
        }
        free(cbuf);
        printf("[REPL-TCP] Catchup complete, now in sync\n");
    } else {
        printf("[REPL-TCP] Already in sync with master\n");
    }
    return 0;
}

/* ── Slave thread ───────────────────────────────────────────────────── */

void *slave_thread_func(void *arg)
{
    (void)arg;
    printf("[REPL-TCP] Slave thread started\n");

    uint8_t frame_buf[REPL_MAX_FRAME_SIZE];
    uint64_t last_saved_offset = repl_incr_load_offset();

    extern uint32_t kv_store_count(void);
    if (last_saved_offset > 0 && kv_store_count() == 0) {
        printf("[REPL-TCP] Local store empty, resetting offset to 0 for fullsync\n");
        last_saved_offset = 0;
        repl_incr_save_offset(0);
    }

    while (g_repl.running) {
        if (g_repl.peer_fd < 0) {
            printf("[REPL-TCP] Connecting to master %s:%u...\n",
                   g_repl.cfg.master_ip, g_repl.cfg.repl_port);

            g_repl.peer_fd = tcp_connect(g_repl.cfg.master_ip, g_repl.cfg.repl_port);
            if (g_repl.peer_fd < 0) { sleep(RECONNECT_INTERVAL); continue; }

            int sync_result = slave_send_sync_request(g_repl.peer_fd, last_saved_offset);
            if (sync_result == -2) {
                /* Fullsync handled, connection remains open */
            } else if (sync_result != 0) {
                close(g_repl.peer_fd); g_repl.peer_fd = -1;
                sleep(RECONNECT_INTERVAL);
                continue;
            }

            update_last_heartbeat();
            pthread_mutex_lock(&g_repl.stats_lock);
            g_repl.stats.connected = 1;
            g_repl.stats.reconnect_count++;
            g_repl.stats.partial_resync = (sync_result == 0 && last_saved_offset > 0);
            pthread_mutex_unlock(&g_repl.stats_lock);
            printf("[REPL-TCP] Connected and synced with master\n");
        }

        struct pollfd pfd = {.fd = g_repl.peer_fd, .events = POLLIN};
        int poll_ret = poll(&pfd, 1, 1000);

        if (poll_ret < 0) {
            if (errno == EINTR) continue;
            close(g_repl.peer_fd); g_repl.peer_fd = -1;
            pthread_mutex_lock(&g_repl.stats_lock);
            g_repl.stats.connected = 0;
            pthread_mutex_unlock(&g_repl.stats_lock);
            printf("[REPL-TCP] Poll error, reconnecting...\n");
            continue;
        }

        if (poll_ret == 0) {
            if (check_heartbeat_timeout()) {
                fprintf(stderr, "[REPL-TCP] Heartbeat timeout detected\n");
                close(g_repl.peer_fd); g_repl.peer_fd = -1;
                pthread_mutex_lock(&g_repl.stats_lock);
                g_repl.stats.connected = 0;
                pthread_mutex_unlock(&g_repl.stats_lock);
            }
            continue;
        }

        struct ctrl_msg_header hdr;
        if (tcp_recv_exact(g_repl.peer_fd, &hdr, sizeof(hdr)) != 0) {
            close(g_repl.peer_fd); g_repl.peer_fd = -1;
            pthread_mutex_lock(&g_repl.stats_lock);
            g_repl.stats.connected = 0;
            pthread_mutex_unlock(&g_repl.stats_lock);
            printf("[REPL-TCP] Connection lost, reconnecting...\n");
            continue;
        }

        uint32_t msg_type = ntohl(hdr.type);

        if (msg_type == CTRL_MSG_PING) {
            uint64_t discard;
            tcp_recv_exact(g_repl.peer_fd, &discard, sizeof(discard));
            update_last_heartbeat();
            continue;
        }

        if (msg_type == CTRL_MSG_DATA_FRAME) {
            uint32_t frame_len;
            if (tcp_recv_exact(g_repl.peer_fd, &frame_len, REPL_FRAME_HEADER) != 0)
                goto disconnect;

            frame_len = ntohl(frame_len);
            if (frame_len == 0 || frame_len > REPL_MAX_FRAME_SIZE) {
                fprintf(stderr, "[REPL-TCP] Invalid frame length: %u\n", frame_len);
                goto disconnect;
            }
            if (tcp_recv_exact(g_repl.peer_fd, frame_buf, frame_len) != 0)
                goto disconnect;

            update_last_heartbeat();
            pthread_mutex_lock(&g_repl.stats_lock);
            g_repl.stats.bytes_recv += frame_len + REPL_FRAME_HEADER;
            g_repl.stats.frames_recv++;
            g_repl.stats.slave_offset += frame_len;
            pthread_mutex_unlock(&g_repl.stats_lock);

            slave_apply_command(frame_buf, frame_len);

            if (g_repl.stats.frames_recv % 100 == 0)
                repl_incr_ack_offset(g_repl.stats.slave_offset);
            if (g_repl.stats.frames_recv % 1000 == 0) {
                last_saved_offset = g_repl.stats.slave_offset;
                repl_incr_save_offset(last_saved_offset);
            }
        }
        continue;

    disconnect:
        close(g_repl.peer_fd); g_repl.peer_fd = -1;
        pthread_mutex_lock(&g_repl.stats_lock);
        g_repl.stats.connected = 0;
        pthread_mutex_unlock(&g_repl.stats_lock);
        printf("[REPL-TCP] Connection lost, reconnecting...\n");
    }

    printf("[REPL-TCP] Slave thread stopped\n");
    return NULL;
}
