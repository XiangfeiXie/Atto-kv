#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <liburing.h>

#include "server.h"
#include "kvstore.h"
#include "config.h"
#include "repl_incr.h"
#include "protocol.h"
#include "engine_xdp.h"

/* ═══════════════════════════════════════════════════════════════════════
 *  Types & Constants
 * ═══════════════════════════════════════════════════════════════════════ */

#define ENTRIES_LENGTH   2048
#define CQE_BATCH_SIZE   128
#define SQPOLL_IDLE_MS   500     /* SQPOLL kernel thread idle timeout (ms)
                                   Lower = more responsive to new SQEs,
                                   higher = less CPU burn when truly idle */

#define EVENT_ACCEPT     0
#define EVENT_READ       1
#define EVENT_WRITE      2
#define EVENT_REPL_POLL  3

typedef struct {
    int             listen_fd;
    struct io_uring ring;
    int             accept_armed;
    unsigned        sqpoll_cpu;  /* CPU for SQPOLL kernel thread affinity */
} proactor_worker_t;

/* ═══════════════════════════════════════════════════════════════════════
 *  conn_info pack/unpack — zero malloc
 * ═══════════════════════════════════════════════════════════════════════ */

static inline uint64_t conn_info_pack(int fd, int event, uint32_t gen) {
    return ((uint64_t)(uint32_t)fd)
         | ((uint64_t)(uint8_t)event << 32)
         | ((uint64_t)(gen & 0xFFFFFFu) << 40);
}

static inline int conn_info_fd(uint64_t data) {
    return (int)(uint32_t)data;
}

static inline int conn_info_event(uint64_t data) {
    return (int)((data >> 32) & 0xFFu);
}

static inline uint32_t conn_info_gen(uint64_t data) {
    return (uint32_t)((data >> 40) & 0xFFFFFFu);
}

static inline uint32_t next_conn_gen(void) {
    static uint32_t g_gen = 1;
    uint32_t gen;
    do { gen = ++g_gen & 0xFFFFFFu; } while (gen == 0);
    return gen;
}

/* ═══════════════════════════════════════════════════════════════════════
 *  Global state
 * ═══════════════════════════════════════════════════════════════════════ */

static conn_t           *conn_pool      = NULL;
static int               conn_pool_size = 0;
static proactor_worker_t g_worker;

/* ═══════════════════════════════════════════════════════════════════════
 *  Connection pool
 * ═══════════════════════════════════════════════════════════════════════ */

static int conn_pool_init(int max_clients) {
    conn_pool_size = max_clients;
    conn_pool = calloc((size_t)max_clients, sizeof(conn_t));
    if (!conn_pool) {
        fprintf(stderr, "[proactor] failed to allocate conn pool\n");
        return -1;
    }
    for (int i = 0; i < max_clients; i++) {
        conn_pool[i].client = calloc(1, sizeof(client_t));
        if (conn_pool[i].client)
            conn_pool[i].client->conn = &conn_pool[i];
    }
    printf("[proactor] conn pool: %d slots (%zu MB)\n",
           max_clients,
           (sizeof(conn_t) * (size_t)max_clients) / (1024 * 1024));
    return 0;
}

static void conn_release(int fd) {
    if (fd < 0 || fd >= conn_pool_size) return;
    conn_t *c = &conn_pool[fd];

    if (c->client)
        memset(c->client, 0, sizeof(client_t));

    if (c->active) {
        atomic_fetch_sub_explicit(&g_kvs_connected_clients, 1,
                                   memory_order_relaxed);
    }

    /* Preserve pre-allocated buffers across reconnects */
    char *rb = c->read_buf.buf, *wb = c->write_buf.buf;
    int   rc = c->read_buf.cap,  wc = c->write_buf.cap;

    memset(c, 0, sizeof(*c));
    c->read_buf.buf  = rb;  c->read_buf.cap  = rc;
    c->write_buf.buf = wb;  c->write_buf.cap = wc;
    close(fd);
}

static void conn_pool_destroy(void) {
    if (!conn_pool) return;
    for (int i = 0; i < conn_pool_size; i++) {
        if (conn_pool[i].active)
            conn_release(i);
        dynbuf_free(&conn_pool[i].read_buf);
        dynbuf_free(&conn_pool[i].write_buf);
        free(conn_pool[i].client);
    }
    free(conn_pool);
    conn_pool = NULL;
    conn_pool_size = 0;
}

/* ═══════════════════════════════════════════════════════════════════════
 *  SQE helpers
 * ═══════════════════════════════════════════════════════════════════════ */

static int arm_recv(struct io_uring *ring, conn_t *c) {
    if (!c->active || c->recv_armed || c->status)
        return 0;

    DynBuf *rb = &c->read_buf;
    size_t tail = (size_t)(c->r_off + rb->len);
    size_t avail = (size_t)rb->cap - tail;

    /* Try compaction before growing — reclaim consumed space at buf[0..r_off).
     * Safe because arm_recv is called before drain (no inflight recv yet).
     * Without this, arm-before-drain causes r_off to advance monotonically
     * until avail < 4096 triggers realloc, doubling the buffer each time
     * until KVS_NET_BUF_MAX is hit and arm_recv fails → QPS drops to 0. */
    if (avail < 4096 && c->r_off > 0) {
        if (rb->len > 0)
            memmove(rb->buf, rb->buf + c->r_off, (size_t)rb->len);
        tail    = (size_t)rb->len;
        c->r_off = 0;
        avail   = (size_t)rb->cap - tail;
    }

    /* Grow buffer only if compaction wasn't enough */
    if (avail < 4096) {
        size_t new_cap = rb->cap ? (size_t)rb->cap * 2 : KVS_NET_BUF_INIT;
        if (new_cap > KVS_NET_BUF_MAX) return -1;
        char *new_buf = realloc(rb->buf, new_cap);
        if (!new_buf) return -1;
        rb->buf = new_buf;
        rb->cap = (int)new_cap;
        avail = new_cap - tail;
    }

    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) return -1;

    io_uring_prep_recv(sqe, c->fd, rb->buf + tail, avail, 0);
    sqe->user_data = conn_info_pack(c->fd, EVENT_READ, c->gen);
    c->recv_armed = 1;
    return 0;
}

static int arm_send(struct io_uring *ring, conn_t *c) {
    if (!c->active || c->send_armed || c->write_buf.len <= 0)
        return 0;

    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) return -1;

    io_uring_prep_send(sqe, c->fd,
                       c->write_buf.buf + c->w_off,
                       (size_t)c->write_buf.len, MSG_NOSIGNAL);
    sqe->user_data = conn_info_pack(c->fd, EVENT_WRITE, c->gen);
    c->send_armed = 1;
    return 0;
}

/* ── Inline send + async fallback (merged SQE-allocation path) ───────
 * Tries non-blocking send() to flush write_buf immediately.  On success
 * the buffer is fully drained → caller re-arms recv without an SQE→CQE
 * round-trip (equivalent to reactor's inline flush).  Only on EAGAIN
 * does it compact and arm an async send via io_uring.
 *
 * Returns: 0 = fully sent, wb cleared → caller may arm_recv
 *          1 = async send armed → caller must wait for send CQE
 *         -1 = error → caller should conn_release                       */
static int flush_write_buf(struct io_uring *ring, conn_t *c) {
    DynBuf *wb = &c->write_buf;

    while (wb->len > 0) {
        ssize_t n = send(c->fd, wb->buf + c->w_off,
                         (size_t)wb->len, MSG_NOSIGNAL);
        if (n > 0) {
            c->w_off += (int)n;
            wb->len  -= (int)n;
        } else if (n == 0) {
            return -1;                      /* peer closed */
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                goto async_fallback;
            return -1;                      /* hard error */
        }
    }

    /* Fully sent inline — like reactor's inline flush */
    c->w_off = 0;
    dynbuf_clear(wb);
    return 0;

async_fallback:
    /* Compact unsent data to buf[0] to prevent fragmentation
     * when subsequent drains append to write_buf. */
    if (c->w_off > 0) {
        memmove(wb->buf, wb->buf + c->w_off, (size_t)wb->len);
        c->w_off = 0;
    }
    return arm_send(ring, c);
}

static int arm_accept(struct io_uring *ring, int listen_fd) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) return -1;
    io_uring_prep_accept(sqe, listen_fd, NULL, NULL, SOCK_NONBLOCK);
    sqe->user_data = conn_info_pack(listen_fd, EVENT_ACCEPT, 0);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════════
 *  Replication engine hooks (callback model)
 *
 *  The repl module's callbacks (nonblocking accept/recv/send) are
 *  readiness-based, while proactor is completion-based.  IORING_OP_POLL_ADD
 *  bridges the two: each repl fd arms a poll for exactly the readiness mask
 *  it needs right now, and the resulting CQE dispatches back into the repl
 *  module.  An idle streaming slave arms nothing (mask 0), so no busy loop.
 * ═══════════════════════════════════════════════════════════════════════ */

static int repl_desired_mask(conn_t *c) {
    switch (c->kind) {
    case CONN_KIND_REPL_LISTEN: return POLLIN;                     /* accept */
    case CONN_KIND_REPL_MASTER: return POLLIN;                     /* cmd stream */
    case CONN_KIND_REPL_SLAVE:  return repl_incr_master_poll_mask(c->fd);
    default:                    return 0;
    }
}

static void repl_poll_update(conn_t *c) {
    if (!c->active) return;

    int desired = repl_desired_mask(c);

    if (desired == 0) {
        /* Nothing to wait on: drop the arm and invalidate any in-flight
         * poll so its late CQE is treated as stale. */
        if (c->repl_poll_mask != 0) {
            c->repl_poll_mask = 0;
            c->gen = next_conn_gen();
        }
        return;
    }

    if (c->repl_poll_mask == (uint32_t)desired)
        return;  /* already armed for this exact mask */

    /* Mask changed while a poll is in flight: bump gen so the old CQE is
     * discarded, then arm the new mask. */
    if (c->repl_poll_mask != 0)
        c->gen = next_conn_gen();

    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_worker.ring);
    if (!sqe) return;
    io_uring_prep_poll_add(sqe, c->fd, (unsigned)desired);
    sqe->user_data = conn_info_pack(c->fd, EVENT_REPL_POLL, c->gen);
    c->repl_poll_mask = (uint32_t)desired;
}

static int proactor_repl_register(int fd, conn_kind_t kind) {
    if (fd < 0 || fd >= conn_pool_size) return -1;

    conn_t *c = &conn_pool[fd];

    /* Reset the slot, preserving pre-allocated buffers + client pointer. */
    client_t *cli = c->client;
    char *rb = c->read_buf.buf, *wb = c->write_buf.buf;
    int   rc = c->read_buf.cap,  wc = c->write_buf.cap;
    memset(c, 0, sizeof(*c));
    c->client       = cli;
    c->read_buf.buf = rb;  c->read_buf.cap = rc;
    c->write_buf.buf = wb; c->write_buf.cap = wc;

    c->fd     = fd;
    c->active = 1;
    c->kind   = kind;
    c->gen    = next_conn_gen();

    repl_poll_update(c);
    return 0;
}

static void proactor_repl_writable(int fd, int on) {
    (void)on;  /* repl_poll_update recomputes the mask from repl state */
    if (fd < 0 || fd >= conn_pool_size) return;
    conn_t *c = &conn_pool[fd];
    repl_poll_update(c);
}

static void proactor_repl_close(int fd) {
    if (fd < 0 || fd >= conn_pool_size) return;
    conn_t *c = &conn_pool[fd];

    if (c->client)
        memset(c->client, 0, sizeof(client_t));

    /* NOTE: unlike conn_release, do NOT decrement g_kvs_connected_clients —
     * repl conns never went through handle_accept and so never incremented
     * it. */

    client_t *cli = c->client;
    char *rb = c->read_buf.buf, *wb = c->write_buf.buf;
    int   rc = c->read_buf.cap,  wc = c->write_buf.cap;

    memset(c, 0, sizeof(*c));
    c->client       = cli;
    c->read_buf.buf = rb;  c->read_buf.cap = rc;
    c->write_buf.buf = wb; c->write_buf.cap = wc;
    close(fd);
}

static void handle_repl_poll(int fd, uint32_t gen, int res) {
    if (fd < 0 || fd >= conn_pool_size) return;
    conn_t *c = &conn_pool[fd];
    if (!c->active) return;
    if (gen != (c->gen & 0xFFFFFFu)) return;  /* stale CQE */

    c->repl_poll_mask = 0;  /* poll consumed; re-armed below */

    switch (c->kind) {
    case CONN_KIND_REPL_LISTEN:
        repl_incr_master_accept(fd);
        break;
    case CONN_KIND_REPL_SLAVE:
        if (res & POLLIN)  repl_incr_master_read(fd);
        if (res & POLLOUT) repl_incr_master_write(fd);
        break;
    case CONN_KIND_REPL_MASTER:
        repl_incr_slave_read(fd);
        break;
    default:
        break;
    }

    repl_poll_update(c);
}

/* ═══════════════════════════════════════════════════════════════════════
 *  CQE handlers (always drain, no deferred processing)
 * ═══════════════════════════════════════════════════════════════════════ */

static void handle_accept(proactor_worker_t *w, struct io_uring_cqe *cqe) {
    struct io_uring *ring = &w->ring;

    /* Consume the accept SQE — re-armed after batch */
    w->accept_armed = 0;

    int connfd = cqe->res;

    if (connfd < 0) return;
    if (connfd >= conn_pool_size) {
        close(connfd);
        return;
    }

    int nd = 1;
    setsockopt(connfd, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd));

    conn_t *c = &conn_pool[connfd];

    /* Lazy-allocate or reuse buffers */
    if (!c->read_buf.buf) {
        dynbuf_init(&c->read_buf, KVS_NET_BUF_INIT);
    } else {
        c->read_buf.len = 0;
    }
    if (!c->write_buf.buf) {
        dynbuf_init(&c->write_buf, KVS_NET_BUF_INIT);
    } else {
        c->write_buf.len = 0;
    }

    c->fd         = connfd;
    c->active     = 1;
    c->r_off      = 0;
    c->w_off      = 0;
    c->recv_armed = 0;
    c->send_armed = 0;
    c->gen        = next_conn_gen();

    atomic_fetch_add_explicit(&g_kvs_total_connections, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_kvs_connected_clients, 1, memory_order_relaxed);

    if (c->client) {
        memset(c->client, 0, sizeof(client_t));
        c->client->conn = c;
    }

    arm_recv(ring, c);
}

static void handle_read(proactor_worker_t *w, struct io_uring_cqe *cqe,
                        int fd, uint32_t gen) {
    struct io_uring *ring = &w->ring;
    int res = cqe->res;

    if (fd < 0 || fd >= conn_pool_size) return;
    conn_t *c = &conn_pool[fd];
    if (!c->active) return;
    if (gen != (c->gen & 0xFFFFFFu)) return;  /* stale CQE */

    c->recv_armed = 0;

    DynBuf *rb = &c->read_buf;
    DynBuf *wb = &c->write_buf;

    if (res > 0) {
        /* Data was placed directly into read_buf by io_uring_prep_recv.
         * DO NOT reset r_off here: arm_recv placed new data at offset
         * (c->r_off + rb->len), which may be > 0 if a previous drain
         * consumed data but left r_off non-zero.  Resetting prematurely
         * causes the parser to re-scan stale buffer contents at buf[0]
         * while the new data sits unseen at a higher offset.
         * r_off is reset after kvs_drain_resp_commands_off (below). */
        rb->len += res;
    }

    if (res <= 0) {
        /* Connection closed or error — drain remaining data then release */
        if (rb->len > 0)
            kvs_drain_resp_commands_off(rb, &c->r_off, wb, c);
        if (wb->len > 0) {
            if (flush_write_buf(ring, c) == 0)
                conn_release(fd);
            /* else: async send armed, release in handle_write */
        } else {
            conn_release(fd);
        }
        return;
    }

    /* ── Pipeline IO and CPU: arm next recv BEFORE draining ─────────
     * The SQE target is buf + r_off + rb->len (current tail).
     * kvs_drain_resp_commands_off maintains the invariant
     *   new_r_off + new_rb_len == old_r_off + old_rb_len == tail_ptr
     * so the kernel writes data contiguously after the unprocessed
     * remainder.  With SQPOLL actively polling, the kernel thread
     * picks up the SQE within microseconds — while the main thread
     * is still draining current commands below.  Compaction is
     * suppressed in protocol.c when recv_armed is set.              */
    arm_recv(ring, c);

    kvs_drain_resp_commands_off(rb, &c->r_off, wb, c);

    /* Reset r_off when the buffer is fully consumed so that the next
     * arm_recv targets buf[0], matching where the parser will start.
     * Must be AFTER the drain — resetting before it (as was done
     * previously when res > 0 and rb->len == 0) would cause the
     * parser to re-scan stale data while new data sits at the old
     * r_off offset.  Matches reactor.c:209 pattern.
     *
     * With arm-before-drain, a recv may be in flight (c->recv_armed==1)
     * targeting the current tail.  Resetting r_off to 0 while the
     * kernel is writing to buf[old_tail] desynchronizes the parser:
     * the next rb->len += res appends to a reset parser position,
     * causing it to read stale data at buf[0..old_tail) while the
     * real new data sits unseen at the old tail offset. */
    if (rb->len == 0 && !c->recv_armed)
        c->r_off = 0;

    if (c->status) {
        /* Recv already armed above; stale-CQE gen check will ignore
         * any late-arriving CQE after conn_release. */
        if (wb->len > 0) {
            if (flush_write_buf(ring, c) == 0)
                conn_release(fd);
            /* else: async send armed, release in handle_write */
        } else {
            conn_release(fd);
        }
    } else if (wb->len > 0) {
        /* Inline flush: try non-blocking send() first.  recv is
         * already armed, so the kernel may complete the next recv
         * while we flush — no need to re-arm after send. */
        int ret = flush_write_buf(ring, c);
        if (ret == 0) {
            /* Fully sent inline — recv already armed, nothing to do */
        } else if (ret < 0) {
            conn_release(fd);
        }
        /* ret > 0: async send armed, recv already armed above */
    }
    /* else: wb empty, recv already armed above */
}

static void handle_write(proactor_worker_t *w, int fd,
                         uint32_t gen, int res) {
    struct io_uring *ring = &w->ring;

    if (fd < 0 || fd >= conn_pool_size) return;
    conn_t *c = &conn_pool[fd];
    if (!c->active) return;
    if (gen != (c->gen & 0xFFFFFFu)) return;

    c->send_armed = 0;
    DynBuf *wb = &c->write_buf;

    if (res <= 0) {
        conn_release(fd);
        return;
    }

    c->w_off += res;
    wb->len  -= res;

    if (wb->len > 0) {
        /* Partial send: compact unsent data to buf[0] to prevent
         * fragmentation when subsequent drains append to write_buf. */
        if (c->w_off > 0) {
            memmove(wb->buf, wb->buf + c->w_off, (size_t)wb->len);
            c->w_off = 0;
        }
        if (arm_send(ring, c) != 0)
            conn_release(fd);
    } else {
        /* Fully sent — clear write buffer and re-arm recv */
        c->w_off = 0;
        dynbuf_clear(wb);

        if (c->status) {
            conn_release(fd);
        } else {
            if (arm_recv(ring, c) != 0)
                conn_release(fd);
        }
    }
}

static void handle_cqe(proactor_worker_t *w, struct io_uring_cqe *cqe) {
    uint64_t ud = cqe->user_data;
    int fd      = conn_info_fd(ud);
    int event   = conn_info_event(ud);
    uint32_t gen = conn_info_gen(ud);

    switch (event) {
    case EVENT_ACCEPT:
        handle_accept(w, cqe);
        break;
    case EVENT_READ:
        handle_read(w, cqe, fd, gen);
        break;
    case EVENT_WRITE:
        handle_write(w, fd, gen, cqe->res);
        break;
    case EVENT_REPL_POLL:
        handle_repl_poll(fd, gen, cqe->res);
        break;
    }
}

/* ═══════════════════════════════════════════════════════════════════════
 *  Event loop — SQPOLL mode
 *
 *  With IORING_SETUP_SQPOLL the kernel thread polls the SQ ring
 *  asynchronously.  Userspace still calls io_uring_submit() to *wake*
 *  the SQPOLL thread (IORING_ENTER_SQ_WAKEUP semantic when SQPOLL is
 *  active — much cheaper than the full submission path), not to perform
 *  the actual SQE→kernel handoff.  The hot path remains:
 *     peek_batch → handle → cq_advance → submit (wake-only)
 * ═══════════════════════════════════════════════════════════════════════ */

static void proactor_loop(proactor_worker_t *w) {
    struct io_uring *ring = &w->ring;

    if (arm_accept(ring, w->listen_fd) != 0) {
        fprintf(stderr, "[proactor] failed to arm accept\n");
        return;
    }
    w->accept_armed = 1;

    /* Initial wake-up: SQPOLL kernel thread sleeps until first submit */
    io_uring_submit(ring);

    printf("[proactor] SQPOLL event loop started (wake-only submit)\n");

    while (!g_kvs_stop) {
        update_cached_time();

        /* Drain any XDP-delivered write commands (slave replay).  The idle
         * path below spins+naps at 10us granularity, so this is naturally
         * low-latency without shortening any sleep. */
        xdp_engine_poll();

        /* Flush the master's batched write commands to the gateway so a
         * partial batch drains promptly (idle path runs at 10us granularity,
         * so this bounds replication latency without any extra sleep). */
        xdp_engine_flush();

        /* Peek completed CQEs — shared-memory, no syscall */
        struct io_uring_cqe *cqes[CQE_BATCH_SIZE];
        int nready = io_uring_peek_batch_cqe(ring, cqes, CQE_BATCH_SIZE);

        if (nready == 0) {
            /* Idle: wake SQPOLL so it sees any queued SQEs.
             *
             * With arm-before-drain, the kernel recv is already in
             * flight while we were draining.  CQEs often arrive within
             * a few microseconds — spin briefly before committing to
             * sleep.  If the SQPOLL thread hasn't finished yet, sleep
             * for 10 us (vs the old 100 us) and try again. */
            io_uring_submit(ring);
            for (int spin = 0; spin < 80; spin++) {
                nready = io_uring_peek_batch_cqe(ring, cqes,
                                                  CQE_BATCH_SIZE);
                if (nready > 0) break;
            }
            if (nready == 0) {
                struct timespec ts = { .tv_sec = 0,
                                       .tv_nsec = 10000L };
                nanosleep(&ts, NULL);
                continue;
            }
            /* CQEs arrived during spin — fall through to process */
        }

        /* Process all completed CQEs in one batch */
        for (int i = 0; i < nready; i++)
            handle_cqe(w, cqes[i]);

        /* Re-arm accept if consumed */
        if (!w->accept_armed) {
            if (arm_accept(ring, w->listen_fd) == 0)
                w->accept_armed = 1;
        }

        /* Advance CQ ring */
        io_uring_cq_advance(ring, nready);

        /* Wake SQPOLL thread so it sees newly-queued SQEs (arm_recv,
         * arm_send, arm_accept).  Under SQPOLL this is a cheap wake,
         * not a full submit. */
        io_uring_submit(ring);
    }

    fprintf(stderr, "[proactor] exit stop=%d\n", (int)g_kvs_stop);
}

/* ═══════════════════════════════════════════════════════════════════════
 *  Setup
 * ═══════════════════════════════════════════════════════════════════════ */

static int setup_io_uring(proactor_worker_t *w) {
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    /* SQPOLL: kernel thread polls SQ ring, eliminating io_uring_enter syscalls
     * from the hot path.  sq_thread_idle limits CPU burn when idle. */
    params.flags          = IORING_SETUP_SQPOLL;
    params.sq_thread_idle = SQPOLL_IDLE_MS;

    /* Pin SQPOLL thread to the worker CPU if configured */
    if (w->sqpoll_cpu > 0) {
        params.flags         |= IORING_SETUP_SQ_AFF;
        params.sq_thread_cpu  = w->sqpoll_cpu;
    }

    int ret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);
    if (ret < 0) {
        fprintf(stderr, "[proactor] io_uring SQPOLL init: %s\n", strerror(-ret));
        return -1;
    }

    printf("[proactor] io_uring SQPOLL ready (sq_idle=%ums, sq_cpu=%u)\n",
           SQPOLL_IDLE_MS, w->sqpoll_cpu);
    return 0;
}

static int init_listen_socket(unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (sockfd < 0) return -1;

    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons(port);

    if (bind(sockfd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        perror("[proactor] bind");
        close(sockfd);
        return -1;
    }
    listen(sockfd, SOMAXCONN);
    return sockfd;
}

static void worker_destroy(proactor_worker_t *w) {
    io_uring_queue_exit(&w->ring);
    if (w->listen_fd >= 0) {
        close(w->listen_fd);
        w->listen_fd = -1;
    }
}

/* ═══════════════════════════════════════════════════════════════════════
 *  Entry point
 * ═══════════════════════════════════════════════════════════════════════ */

int proactor_start(unsigned short port, msg_handler handler) {
    (void)handler;

    if (conn_pool_init(kvs_config.maxclients) < 0) return -1;
    memset(&g_worker, 0, sizeof(g_worker));
    g_worker.listen_fd  = -1;
    g_worker.sqpoll_cpu = 0;  /* 0 = no CPU pinning for SQPOLL thread */

    g_worker.listen_fd = init_listen_socket(port);
    if (g_worker.listen_fd < 0) {
        conn_pool_destroy();
        return -1;
    }

    if (setup_io_uring(&g_worker) < 0) {
        close(g_worker.listen_fd);
        conn_pool_destroy();
        return -1;
    }

    /* Wire replication hooks — unify repl fds into the io_uring loop.
     * Must run after the ring is ready (repl_incr_attach arms POLL_ADD). */
    if (kvs_config.incr_sync_enabled) {
        repl_engine_register = proactor_repl_register;
        repl_engine_writable = proactor_repl_writable;
        repl_engine_close    = proactor_repl_close;
        repl_incr_attach();
    }

    printf("[proactor] single-threaded SQPOLL on port %u (sq_cpu=%u)\n",
           port, g_worker.sqpoll_cpu);

    proactor_loop(&g_worker);

    worker_destroy(&g_worker);
    conn_pool_destroy();
    return 0;
}
