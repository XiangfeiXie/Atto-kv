/* repl_tcp_internal.h — Internal types and declarations shared across repl_tcp_*.c */

#ifndef REPL_TCP_INTERNAL_H
#define REPL_TCP_INTERNAL_H

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
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

/* eBPF ringbuf event (userspace copy, must match repl_observe.h) */
#define REPL_OBSERVE_MAX_DATA  4096
#define REPL_OBSERVE_RINGBUF_SZ (8 * 1024 * 1024)

struct repl_observe_event {
    uint64_t sock_cookie;
    uint32_t len;
    uint32_t pad;
    uint8_t  data[REPL_OBSERVE_MAX_DATA];
};

/* Stream reassembly: linear per-socket buffer.
 * strparser guarantees ordered delivery (no OOO), so a single
 * linear buffer is sufficient — no rb_tree / sequence tracking needed. */
#define MAX_STREAM_BUFFERS  32
#define STREAM_GC_TIMEOUT   10
#define STREAM_GC_INTERVAL  5

struct sock_stream {
    uint64_t  sock_cookie;
    uint8_t  *buf;           /* Linear reassembly buffer (realloc'd as needed) */
    uint32_t  len;           /* Valid bytes in buf */
    uint32_t  cap;           /* Total capacity */
    time_t    last_active;
};

/* ── Constants ──────────────────────────────────────────────────────── */
#define REPL_FRAME_HEADER   4
#define REPL_MAX_FRAME_SIZE (64 * 1024)
#define RECONNECT_INTERVAL  3
#define HEARTBEAT_INTERVAL  5
#define HEARTBEAT_TIMEOUT   15

/* ── Control Protocol ───────────────────────────────────────────────── */
#define CTRL_MAGIC  0x4B565343   /* "KVSC" */

typedef enum {
    CTRL_MSG_SYNC_REQUEST  = 1,
    CTRL_MSG_SYNC_RESPONSE = 2,
    CTRL_MSG_PING          = 3,
    CTRL_MSG_DATA_FRAME    = 5,
    CTRL_MSG_FULLSYNC_DONE = 6,
} ctrl_msg_type_t;

/* ── Sync Protocol ──────────────────────────────────────────────────── */
#define SYNC_STATUS_OK                0
#define SYNC_STATUS_FULLSYNC_REQUIRED 1

struct ctrl_msg_header {
    uint32_t magic;
    uint32_t type;
    uint64_t timestamp;
} __attribute__((packed));

struct sync_request {
    struct ctrl_msg_header hdr;
    uint64_t last_offset;
} __attribute__((packed));

struct sync_response {
    struct ctrl_msg_header hdr;
    uint32_t status;
    uint64_t start_offset;
    uint64_t data_len;
    uint64_t base_seq;
    uint16_t rdma_port;
    uint16_t padding;
} __attribute__((packed));

struct fullsync_done_msg {
    struct ctrl_msg_header hdr;
    uint64_t base_seq;
} __attribute__((packed));

struct heartbeat_msg {
    struct ctrl_msg_header hdr;
    uint64_t master_offset;
} __attribute__((packed));

/* ── Global state ───────────────────────────────────────────────────── */
struct repl_tcp_global {
    struct repl_tcp_config cfg;
    pthread_t              thread;
    pthread_t              heartbeat_thread;
    volatile int           running;

    /* eBPF state (master only) */
    struct bpf_object     *bpf_obj;
    struct ring_buffer    *ringbuf;
    int                    cgroup_fd;

    /* TCP state */
    int                    listen_fd;
    int                    peer_fd;
    int                    kv_fd;

    /* Heartbeat state */
    time_t                 last_heartbeat;
    pthread_mutex_t        heartbeat_lock;

    /* Statistics */
    struct repl_tcp_stats  stats;
    pthread_mutex_t        stats_lock;
};

extern struct repl_tcp_global g_repl;

/* ── TCP helpers ────────────────────────────────────────────────────── */
int  tcp_set_options(int fd);
int  tcp_listen(uint16_t port);
int  tcp_connect(const char *ip, uint16_t port);
int  tcp_send_frame(int fd, const void *data, uint32_t len);
int  tcp_send_data_frame(int fd, const void *data, uint32_t len);
int  tcp_recv_exact(int fd, void *buf, size_t len);

/* ── RESP parser ────────────────────────────────────────────────────── */
int find_complete_resp_command(const uint8_t *data, size_t len);

/* ── Write-command filter ───────────────────────────────────────────── */
int is_write_command(const uint8_t *data, size_t len);

/* ── Heartbeat ──────────────────────────────────────────────────────── */
int  send_heartbeat_ping(int fd);
void update_last_heartbeat(void);
int  check_heartbeat_timeout(void);
void *heartbeat_thread_func(void *arg);

/* ── Master ─────────────────────────────────────────────────────────── */
int  master_load_bpf(void);
int  master_on_data(void *ctx, void *data, size_t len);
void *master_thread_func(void *arg);

/* ── Slave ──────────────────────────────────────────────────────────── */
int  slave_apply_command(const uint8_t *data, uint32_t len);
void *slave_thread_func(void *arg);

#endif /* REPL_TCP_INTERNAL_H */
