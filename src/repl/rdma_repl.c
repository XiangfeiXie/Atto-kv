/* ===================================================================
 * rdma_repl.c  —  Streaming full-sync over libfabric (Soft-RoCE / verbs)
 *                 with fork() + MAP_SHARED ring buffer + parent worker.
 *
 * Architecture (Plan B):
 *   Master listener thread accepts RDMA connections. On each connect:
 *     1. Read eBPF global_seq → base_seq (fullsync cut point)
 *     2. Signal incr thread: status = INPROGRESS (pause direct send)
 *     3. Create memfd + mmap(MAP_SHARED) ring buffer + eventfd pair
 *     4. Register MR on domain BEFORE fork (MAP_SHARED → physical pages stable)
 *     5. fork()
 *     6. Child: iterates dict, serializes entries into ring slots,
 *        signals parent via eventfd. Does NOT touch libfabric.
 *     7. Parent: worker thread polls child_evt + CQ, sends chunks via
 *        RDMA in a 8-slot pipeline. Sets child_should_exit when done.
 *     8. Worker joins, child reaped, incr thread transitions to CATCHUP.
 *     9. Slave connects TCP incr, sends INCR_SYNC, catches up backlog.
 *
 * Slave:
 *   Connects to master RDMA, pre-posts 4 recv buffers (CHUNK_WIRE_SIZE),
 *   receives streaming chunks (HEADER/DATA/EOF), incrementally loads
 *   entries, then connects TCP incr with INCR_SYNC <base_seq>.
 * =================================================================== */

#define _GNU_SOURCE   /* memfd_create, eventfd_read/write, SYS_pidfd_open */
#include "rdma_repl.h"
#include "kvstore.h"
#include "config.h"
#include "server.h"
#include "wal_repl.h"
#include "repl_stream.h"
#include "persistence.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <dirent.h>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>

#include <sys/socket.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <sys/syscall.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* g_dict is declared in kvstore.h, defined in kvstore.c */

/* ========================================================== */
/* 1. libfabric PLUMBING                                      */
/* ========================================================== */

#define RDMA_CHK(call, msg) do {                                    \
        int _rc = (call);                                           \
        if (_rc) {                                                  \
            fprintf(stderr, "[RDMA-REPL] %s failed: %s (%d)\n",     \
                    msg, fi_strerror(-_rc), _rc);                   \
            goto fail;                                              \
        }                                                           \
    } while (0)

#define CLOSE_FID(f) do { if (f) { fi_close(&(f)->fid); (f) = NULL; } } while (0)

static uint64_t g_mr_key_next = 1;

static uint64_t rdma_next_mr_key(void)
{
    return __atomic_fetch_add(&g_mr_key_next, 1, __ATOMIC_RELAXED);
}

static int rdma_mr_reg(struct fid_domain *domain, void *buf, size_t len,
                       uint64_t access, struct fid_mr **mr)
{
    int rc = fi_mr_reg(domain, buf, len, access, 0, 0, 0, mr, NULL);
    if (rc != -FI_ENOKEY)
        return rc;

    for (int i = 0; i < 64; i++) {
        uint64_t key = rdma_next_mr_key();
        rc = fi_mr_reg(domain, buf, len, access, 0, key, 0, mr, NULL);
        if (rc == 0)
            return 0;
        if (rc != -FI_ENOKEY)
            return rc;
    }
    return rc;
}

static struct fi_info *rdma_build_hints(void) {
    struct fi_info *hints = fi_allocinfo();
    if (!hints) return NULL;
    hints->caps                   = FI_MSG;
    hints->ep_attr->type          = FI_EP_MSG;
    hints->mode                   = 0;
    hints->domain_attr->mr_mode   = FI_MR_LOCAL | FI_MR_ALLOCATED;
    if (kvs_config.rdma_provider[0] &&
        strcasecmp(kvs_config.rdma_provider, "auto") != 0) {
        hints->fabric_attr->prov_name = strdup(kvs_config.rdma_provider);
    }
    return hints;
}

static int rdma_wait_cq(struct fid_cq *cq) {
    struct fi_cq_data_entry ce;
    for (;;) {
        ssize_t r = fi_cq_sread(cq, &ce, 1, NULL, -1);
        if (r == 1) return 0;
        if (r == -FI_EAGAIN) continue;
        if (r == -FI_EAVAIL) {
            struct fi_cq_err_entry ee; memset(&ee, 0, sizeof(ee));
            fi_cq_readerr(cq, &ee, 0);
            fprintf(stderr, "[RDMA-REPL] CQ error: %s\n",
                    fi_cq_strerror(cq, ee.prov_errno, ee.err_data, NULL, 0));
            return -1;
        }
        fprintf(stderr, "[RDMA-REPL] fi_cq_sread: %s\n", fi_strerror(-r));
        return -1;
    }
}

static int rdma_wait_eq(struct fid_eq *eq, uint32_t *event,
                        struct fi_eq_cm_entry *entry, size_t entry_size) {
    ssize_t r = fi_eq_sread(eq, event, entry, entry_size, -1, 0);
    if (r < 0) {
        struct fi_eq_err_entry ee; memset(&ee, 0, sizeof(ee));
        fi_eq_readerr(eq, &ee, 0);
        fprintf(stderr, "[RDMA-REPL] EQ error: %s\n",
                fi_eq_strerror(eq, ee.prov_errno, ee.err_data, NULL, 0));
        return -1;
    }
    return 0;
}

/* ========================================================== */
/* 2. Streaming fullsync — data structures                    */
/* ========================================================== */

#define SHM_RING_SIZE      16
#define CHUNK_DATA_SIZE    (1024 * 1024)    /* 1 MB per slot */
#define CHUNK_HDR_SIZE     5               /* [chunk_len:4][type:1] */
#define CHUNK_WIRE_SIZE    (CHUNK_HDR_SIZE + CHUNK_DATA_SIZE)

/* Chunk types (wire protocol) */
#define CHUNK_TYPE_HEADER  0
#define CHUNK_TYPE_DATA    1
#define CHUNK_TYPE_EOF     2

/* Slot states (internal coordination) */
#define SLOT_EMPTY     0
#define SLOT_READY     1
#define SLOT_INFLIGHT  2

/* Per-slot layout: wire header + data are contiguous so a single
 * fi_send() sends the complete chunk without copying.
 *
 * Wire format of a single fi_send:
 *   [chunk_len:4][chunk_type:1][data:chunk_len]
 *
 * Slot layout (in MAP_SHARED memory):
 *   offset 0-3:   chunk_len   (wire)
 *   offset 4:     chunk_type  (wire)
 *   offset 5-...: data[]       (wire payload)
 *   ... + state:  slot state  (internal, past data[], not on wire) */
typedef struct {
    uint32_t chunk_len;                     /* wire byte 0-3  */
    uint8_t  chunk_type;                    /* wire byte 4    */
    char     data[CHUNK_DATA_SIZE];         /* wire byte 5+   */
    /* ── internal (not sent) ── */
    uint8_t  state;                         /* EMPTY/READY/INFLIGHT */
    uint8_t  _pad[3];
} shm_slot_t;

/* Verify the layout: a fi_send(slot, 5+chunk_len) sends header+data
 * and stops before 'state'. */
_Static_assert(offsetof(shm_slot_t, state) == 5 + CHUNK_DATA_SIZE,
               "slot state must be after data for clean wire format");

typedef struct {
    shm_slot_t slots[SHM_RING_SIZE];
    uint64_t   base_seq;          /* set by parent after fork */
    int        child_should_exit; /* parent→child: sync done  */
} shm_ring_t;

/* Per-session tracking (for concurrent fullsyncs + SIGCHLD) */
typedef struct fullsync_session_s {
    pid_t pid;
    struct fullsync_session_s *next;
} fullsync_session_t;

static fullsync_session_t *g_sessions = NULL;
static pthread_mutex_t      g_sessions_mtx = PTHREAD_MUTEX_INITIALIZER;

static void session_add(pid_t pid) {
    fullsync_session_t *s = (fullsync_session_t *)malloc(sizeof(*s));
    if (!s) return;
    s->pid = pid;
    pthread_mutex_lock(&g_sessions_mtx);
    s->next = g_sessions;
    g_sessions = s;
    pthread_mutex_unlock(&g_sessions_mtx);
}

static void session_remove(pid_t pid) {
    pthread_mutex_lock(&g_sessions_mtx);
    fullsync_session_t **pp = &g_sessions;
    while (*pp) {
        if ((*pp)->pid == pid) {
            fullsync_session_t *t = *pp;
            *pp = t->next;
            free(t);
            break;
        }
        pp = &(*pp)->next;
    }
    pthread_mutex_unlock(&g_sessions_mtx);
}

/* SIGCHLD handler: reap exited children and remove from session list */
static void fullsync_sigchld(int signo) {
    (void)signo;
    pid_t pid;
    int   status;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0)
        session_remove(pid);
}

static int sigchld_installed = 0;

static void fullsync_sigchld_install(void) {
    if (sigchld_installed) return;
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = fullsync_sigchld;
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);
    sigchld_installed = 1;
}

/* ========================================================== */
/* 3. Child process: serialize engine → ring slots            */
/* ========================================================== */

/* Close all FDs >= 3 except keep1 and keep2.
 * Used in the child after fork() to release libfabric resources
 * inherited from the parent without explicitly enumerating them. */
static void child_close_extra_fds(int keep1, int keep2) {
    DIR *d = opendir("/proc/self/fd");
    if (!d) return;
    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        if (ent->d_name[0] == '.') continue;
        int fd = atoi(ent->d_name);
        if (fd > 2 && fd != keep1 && fd != keep2)
            close(fd);
    }
    closedir(d);
}

/* Acquire an EMPTY slot in the ring. If none available, busy-wait
 * with a backoff — the parent worker thread is concurrently setting
 * INFLIGHT slots back to EMPTY via CQ completions. */
static shm_slot_t *child_acquire_slot(shm_ring_t *shm) {
    for (;;) {
        for (int i = 0; i < SHM_RING_SIZE; i++) {
            if (shm->slots[i].state == SLOT_EMPTY)
                return &shm->slots[i];
        }
        /* Ring full — parent hasn't drained yet. Brief sleep. */
        usleep(100);
    }
}

static void child_serialize_loop(shm_ring_t *shm,
                                 int child_evt_fd,
                                 int parent_evt_fd)
{
    /* Wait for the parent to write the post-fork base_seq into
     * shm->base_seq and signal us.  Reading base_seq after fork
     * guarantees that no write command with seq >= base_seq is
     * included in the COW snapshot — the fork() itself is the
     * serialisation point. */
    {
        uint64_t dummy;
        eventfd_read(parent_evt_fd, &dummy);
    }
    uint64_t base_seq = shm->base_seq;

    /* Acquire first slot.  HEADER is prepended to the data[] of the
     * first DATA chunk so the slave can parse it inline — no separate
     * HEADER chunk needed. */
    shm_slot_t *s = child_acquire_slot(shm);

    /* Write header at start of first slot:
     * [magic:4][version:4][db_num:4][key_count:4][base_seq:8] = 24 bytes */
    {
        int magic = 0x524442, version = 2, db_num = 0;
        int key_count = (int)kv_shards_count();
        char *p = s->data;
        memcpy(p, &magic,     4); p += 4;
        memcpy(p, &version,   4); p += 4;
        memcpy(p, &db_num,    4); p += 4;
        memcpy(p, &key_count, 4); p += 4;
        memcpy(p, &base_seq,  8); p += 8;
    }

    /* Write pointer: entries start after the 24-byte header */
    char *wp = s->data + 24;

    kv_shards_iter_t it_s; kv_shards_iter_init_nolock(&it_s);

    kv_entry_t *e;
    while ((e = kv_shards_iter_next(&it_s)) != NULL) {
        /* Try to serialize into remaining slot space */
        size_t room = (size_t)(s->data + CHUNK_DATA_SIZE - wp);
        int n = serialize_entry_inline(wp, room, e);

        if (n < 0) {
            /* Entry doesn't fit in remaining space */
            if (wp == s->data || wp == s->data + 24) {
                /* Empty slot (or header-only) and entry still too large.
                 * Truly oversized — skip it. */
                fprintf(stderr, "[RDMA-REPL] child: oversized entry, "
                        "skipping\n");
                continue;
            }

            /* Flush current slot, acquire a new one, retry this entry */
            s->chunk_len  = (uint32_t)(wp - s->data);
            s->chunk_type = CHUNK_TYPE_DATA;
            s->state      = SLOT_READY;
            eventfd_write(child_evt_fd, 1);

            s = child_acquire_slot(shm);
            wp = s->data;  /* no header in continuation slots */

            n = serialize_entry_inline(wp, CHUNK_DATA_SIZE, e);
            if (n < 0) {
                fprintf(stderr, "[RDMA-REPL] child: oversized entry in "
                        "empty slot, skipping\n");
                continue;
            }
        }

        wp += n;
    }

    /* Flush final slot if it has data */
    if (wp > s->data) {
        s->chunk_len  = (uint32_t)(wp - s->data);
        s->chunk_type = CHUNK_TYPE_DATA;
        s->state      = SLOT_READY;
        eventfd_write(child_evt_fd, 1);
    }

    kv_shards_iter_free(&it_s);

send_eof:
    {
        shm_slot_t *eof_s = child_acquire_slot(shm);
        eof_s->chunk_len  = 0;
        eof_s->chunk_type = CHUNK_TYPE_EOF;
        eof_s->state      = SLOT_READY;
        eventfd_write(child_evt_fd, 1);
    }

    /* Wait for parent worker thread to signal completion */
    printf("[RDMA-REPL] child: serialization done, waiting for parent\n");
    while (!shm->child_should_exit)
        usleep(1000);
    printf("[RDMA-REPL] child: exiting\n");
}

/* ========================================================== */
/* 4. Parent worker thread: epoll + RDMA send pipeline        */
/* ========================================================== */

struct send_thread_arg {
    struct fid_ep  *ep;
    struct fid_cq  *cq;
    struct fid_mr  *mr_shm;
    shm_ring_t     *shm;
    int            child_evt_fd;   /* read end */
    int            parent_evt_fd;  /* write end */
};

static void *parent_send_thread(void *arg) {
    struct send_thread_arg *a = (struct send_thread_arg *)arg;
    struct fid_ep  *ep    = a->ep;
    struct fid_cq  *cq    = a->cq;
    struct fid_mr  *mr    = a->mr_shm;
    shm_ring_t     *shm   = a->shm;
    int            evt_fd = a->child_evt_fd;
    int            sig_fd = a->parent_evt_fd;

    struct fi_context send_ctx[SHM_RING_SIZE];
    int    eof_sent  = 0;
    int    inflight  = 0;
    int    send_idx  = 0;   /* next slot index to scan */

    /* epoll on child_evt_fd */
    int epfd = epoll_create1(0);
    if (epfd < 0) {
        fprintf(stderr, "[RDMA-REPL] parent: epoll_create1 failed: %s\n",
                strerror(errno));
        return NULL;
    }

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN;
    ev.data.fd = evt_fd;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, evt_fd, &ev) < 0) {
        fprintf(stderr, "[RDMA-REPL] parent: epoll_ctl failed: %s\n",
                strerror(errno));
        close(epfd);
        return NULL;
    }

    printf("[RDMA-REPL] parent: send thread started\n");

    while (1) {
        /* When we have in-flight sends, poll briefly to reap CQ
         * completions (which free slots for the child).  Otherwise
         * block until the child signals new data. */
        int timeout_ms = (inflight > 0) ? 1 : -1;
        struct epoll_event ev_out;
        int n = epoll_wait(epfd, &ev_out, 1, timeout_ms);

        /* Consume eventfd counter */
        if (n > 0 && (ev_out.events & EPOLLIN)) {
            uint64_t dummy;
            eventfd_read(evt_fd, &dummy);
        }

        /* Scan ring for READY slots, submit sends */
        for (int round = 0; round < SHM_RING_SIZE; round++) {
            shm_slot_t *s = &shm->slots[send_idx];
            if (s->state == SLOT_READY) {
                size_t wire_len = (size_t)(5 + s->chunk_len);

                ssize_t sr = fi_send(ep, s, wire_len,
                                     fi_mr_desc(mr), 0, &send_ctx[send_idx]);
                if (sr == 0) {
                    s->state = SLOT_INFLIGHT;
                    inflight++;
                    if (s->chunk_type == CHUNK_TYPE_EOF)
                        eof_sent = 1;
                } else if (sr == -FI_EAGAIN) {
                    /* Queue full — retry on next iteration */
                    break;
                } else {
                    fprintf(stderr, "[RDMA-REPL] parent: fi_send failed: %s\n",
                            fi_strerror((int)-sr));
                    goto done;
                }
            }
            send_idx = (send_idx + 1) % SHM_RING_SIZE;
        }

        /* Reap CQ completions (non-blocking) */
        for (;;) {
            struct fi_cq_data_entry ce;
            ssize_t r = fi_cq_read(cq, &ce, 1);
            if (r == -FI_EAGAIN) break;
            if (r < 0) {
                if (r == -FI_EAVAIL) {
                    struct fi_cq_err_entry ee; memset(&ee, 0, sizeof(ee));
                    fi_cq_readerr(cq, &ee, 0);
                }
                break;
            }
            if (r == 1) {
                inflight--;
                /* Find slot by op_context */
                int slot_idx = -1;
                for (int i = 0; i < SHM_RING_SIZE; i++) {
                    if (&send_ctx[i] == (struct fi_context *)ce.op_context) {
                        slot_idx = i;
                        break;
                    }
                }
                if (slot_idx >= 0)
                    shm->slots[slot_idx].state = SLOT_EMPTY;
            }
        }

        /* All done: EOF sent + all completions received */
        if (eof_sent && inflight == 0) {
            int all_empty = 1;
            for (int i = 0; i < SHM_RING_SIZE; i++) {
                if (shm->slots[i].state != SLOT_EMPTY) {
                    all_empty = 0;
                    break;
                }
            }
            if (all_empty)
                break;
        }
    }

    printf("[RDMA-REPL] parent: all chunks sent and acked by transport\n");

done:
    /* Signal child that it can exit */
    shm->child_should_exit = 1;
    eventfd_write(sig_fd, 1);

    close(epfd);
    return NULL;
}

/* ==========================================================
 * 5. MASTER: serve one slave connection
 *
 * Flow:
 *   1. Build domain/CQ/EP, accept, wait CONNECTED
 *   2. Begin fullsync (base_seq), create shared memory + eventfds
 *   3. Register MR for shm BEFORE fork
 *   4. fork()
 *   5. Child: serialize to ring, _exit
 *   6. Parent: worker thread sends via RDMA, waits, signals child
 *   7. Reap child, transition incr to CATCHUP
 * ========================================================== */

static int rdma_master_serve_one(struct fid_fabric *fabric,
                                 struct fid_eq     *eq,
                                 struct fi_info    *client_info)
{
    struct fid_domain *domain = NULL;
    struct fid_ep     *ep     = NULL;
    struct fid_cq     *cq     = NULL;
    struct fid_mr     *mr_shm = NULL;
    shm_ring_t        *shm    = NULL;
    size_t            shm_size = sizeof(shm_ring_t);
    int    shm_fd            = -1;
    int    child_evt_fd      = -1;
    int    parent_evt_fd     = -1;
    pid_t  child_pid         = -1;
    int    rc                = -1;

    /* 1) Build domain, CQ, endpoint */
    struct fi_cq_attr cq_attr = {0};
    cq_attr.format   = FI_CQ_FORMAT_DATA;
    cq_attr.wait_obj = FI_WAIT_UNSPEC;

    RDMA_CHK(fi_domain(fabric, client_info, &domain, NULL), "fi_domain");
    RDMA_CHK(fi_cq_open(domain, &cq_attr, &cq, NULL),        "fi_cq_open");
    RDMA_CHK(fi_endpoint(domain, client_info, &ep, NULL),    "fi_endpoint");
    RDMA_CHK(fi_ep_bind(ep, &eq->fid, 0),                    "ep bind eq");
    RDMA_CHK(fi_ep_bind(ep, &cq->fid, FI_SEND | FI_RECV),    "ep bind cq");
    RDMA_CHK(fi_enable(ep),                                  "fi_enable");

    /* 2) Accept and wait for FI_CONNECTED */
    RDMA_CHK(fi_accept(ep, NULL, 0), "fi_accept");
    {
        uint32_t event;
        struct fi_eq_cm_entry entry;
        if (rdma_wait_eq(eq, &event, &entry, sizeof(entry)) < 0) goto fail;
        if (event != FI_CONNECTED) {
            fprintf(stderr, "[RDMA-REPL] expected FI_CONNECTED got %u\n", event);
            goto fail;
        }
    }
    printf("[RDMA-REPL] master: slave connected, starting streaming full sync\n");

    /* 3) Begin fullsync: read eBPF global_seq, set status=INPROGRESS */
    /* Begin fullsync: set INPROGRESS, read pre-fork global_seq.
     * The authoritative base_seq is read again after fork(). */
    (void)ebpf_incr_fullsync_begin();

    /* 4) Create shared memory ring: memfd + ftruncate + mmap */
    shm_fd = memfd_create("kvstore_fullsync", 0);
    if (shm_fd < 0) {
        fprintf(stderr, "[RDMA-REPL] memfd_create failed: %s\n",
                strerror(errno));
        ebpf_incr_fullsync_finish();
        goto fail;
    }
    if (ftruncate(shm_fd, (off_t)shm_size) < 0) {
        fprintf(stderr, "[RDMA-REPL] ftruncate failed: %s\n",
                strerror(errno));
        close(shm_fd);
        ebpf_incr_fullsync_finish();
        goto fail;
    }
    shm = (shm_ring_t *)mmap(NULL, shm_size, PROT_READ | PROT_WRITE,
                              MAP_SHARED, shm_fd, 0);
    close(shm_fd);
    shm_fd = -1;

    if (shm == MAP_FAILED) {
        fprintf(stderr, "[RDMA-REPL] mmap failed: %s\n", strerror(errno));
        shm = NULL;
        ebpf_incr_fullsync_finish();
        goto fail;
    }

    /* Zero-initialize the ring (all slots EMPTY).
     * base_seq is written by the parent AFTER fork. */
    memset(shm, 0, shm_size);

    /* 5) Register MR for shared memory BEFORE fork.
     *     MAP_SHARED guarantees physical pages are stable across fork. */
    RDMA_CHK(rdma_mr_reg(domain, shm, shm_size,
                         FI_SEND, &mr_shm), "mr_reg shm");

    /* 6) Create eventfd pair:
     *     child_evt_fd  — child writes, parent reads  (EPOLLIN in worker)
     *     parent_evt_fd — parent writes, child reads  (child_should_exit) */
    child_evt_fd = eventfd(0, EFD_NONBLOCK);
    parent_evt_fd = eventfd(0, 0);
    if (child_evt_fd < 0 || parent_evt_fd < 0) {
        fprintf(stderr, "[RDMA-REPL] eventfd failed: %s\n", strerror(errno));
        ebpf_incr_fullsync_finish();
        goto fail;
    }

    /* 7) fork() */
    fullsync_sigchld_install();
    kv_shards_lock_all_for_snapshot();
    child_pid = fork();

    if (child_pid < 0) {
        fprintf(stderr, "[RDMA-REPL] fork() failed: %s\n", strerror(errno));
        kv_shards_unlock_all_snapshot();
        close(child_evt_fd);
        close(parent_evt_fd);
        ebpf_incr_fullsync_finish();
        goto fail;
    }

    if (child_pid == 0) {
        /* ============================================================
         * CHILD: owns COW snapshot of engine at fork() moment.
         * Must NOT touch any libfabric resources.
         * ============================================================ */
        child_close_extra_fds(child_evt_fd, parent_evt_fd);

        child_serialize_loop(shm, child_evt_fd, parent_evt_fd);

        /* Clean up what we still hold */
        munmap(shm, shm_size);
        close(child_evt_fd);
        close(parent_evt_fd);
        _exit(0);
    }
    kv_shards_unlock_all_snapshot();

    /* ============================================================
     * PARENT: spawn worker thread to handle RDMA send pipeline
     * ============================================================ */
    session_add(child_pid);

    /* Read global_seq AFTER fork.  The fork() is the serialisation
     * point: the child's COW snapshot reflects every write with
     * seq < post_fork_seq, and the backlog will contain every write
     * with seq >= post_fork_seq.  No overlap, no duplicates. */
    {
        uint64_t post_fork_seq = ebpf_incr_read_global_seq();
        ebpf_incr_fullsync_set_base_seq(post_fork_seq);
        shm->base_seq = post_fork_seq;
        /* Wake the child — it is blocked on parent_evt_fd waiting
         * for base_seq to be ready. */
        eventfd_write(parent_evt_fd, (eventfd_t)1);
        printf("[RDMA-REPL] master: post-fork base_seq=%" PRIu64 "\n",
               post_fork_seq);
    }

    /* Parent closes the write end of child_evt (child writes, we read)
     * and the read end of parent_evt (we write, child reads).
     * But eventfd is a single fd without direction — both sides can
     * read/write. The parent keeps both fds; worker reads child_evt
     * and writes parent_evt. */
    {
        struct send_thread_arg arg;
        arg.ep           = ep;
        arg.cq           = cq;
        arg.mr_shm       = mr_shm;
        arg.shm          = shm;
        arg.child_evt_fd = child_evt_fd;
        arg.parent_evt_fd = parent_evt_fd;

        pthread_t tid;
        if (pthread_create(&tid, NULL, parent_send_thread, &arg) != 0) {
            fprintf(stderr, "[RDMA-REPL] pthread_create send thread: %s\n",
                    strerror(errno));
            shm->child_should_exit = 1;
            eventfd_write(parent_evt_fd, 1);
            rc = -1;
        } else {
            pthread_join(tid, NULL);
            rc = 0;
        }
    }

    /* Reap child process */
    if (child_pid > 0) {
        int wstatus;
        waitpid(child_pid, &wstatus, 0);
        session_remove(child_pid);
    }

    printf("[RDMA-REPL] master: streaming full sync done "
           "(base_seq=%" PRIu64 ")\n",
           ebpf_incr_fullsync_base_seq());

    /* Transition incr to IDLE (or CATCHUP if no slave connected yet) */
    if (rc == 0)
        ebpf_incr_fullsync_send_done();
    else
        ebpf_incr_fullsync_finish();

fail:
    /* Release resources */
    if (mr_shm) { CLOSE_FID(mr_shm); mr_shm = NULL; }

    /* Shut down the RDMA connection; incr sync happens over TCP */
    if (rc == 0 && ep) fi_shutdown(ep, 0);

    CLOSE_FID(ep);
    CLOSE_FID(cq);
    CLOSE_FID(domain);

    if (shm && shm != MAP_FAILED) munmap(shm, shm_size);
    if (child_evt_fd >= 0)  close(child_evt_fd);
    if (parent_evt_fd >= 0) close(parent_evt_fd);
    return rc;
}

/* ========================================================== */
/* 6. MASTER: listener thread                                 */
/* ========================================================== */

static void *rdma_master_thread(void *arg) {
    (void)arg;

    struct fi_info      *hints  = NULL, *info = NULL;
    struct fid_fabric   *fabric = NULL;
    struct fid_eq       *eq     = NULL;
    struct fid_pep      *pep    = NULL;

    hints = rdma_build_hints();
    if (!hints) { fprintf(stderr, "[RDMA-REPL] alloc hints failed\n"); return NULL; }

    char service[16];
    snprintf(service, sizeof(service), "%d", kvs_config.rdma_listen_port);

    int rc = fi_getinfo(FI_VERSION(1, 17),
                        kvs_config.rdma_listen_ip, service,
                        FI_SOURCE, hints, &info);
    if (rc) {
        fprintf(stderr, "[RDMA-REPL] fi_getinfo (master) failed: %s\n",
                fi_strerror(-rc));
        goto cleanup;
    }
    printf("[RDMA-REPL] master: using provider '%s' on %s:%s\n",
           info->fabric_attr->prov_name,
           kvs_config.rdma_listen_ip, service);

    struct fi_eq_attr eq_attr = {0};
    eq_attr.wait_obj = FI_WAIT_UNSPEC;

    RDMA_CHK(fi_fabric(info->fabric_attr, &fabric, NULL), "fi_fabric");
    RDMA_CHK(fi_eq_open(fabric, &eq_attr, &eq, NULL),     "fi_eq_open");
    RDMA_CHK(fi_passive_ep(fabric, info, &pep, NULL),     "fi_passive_ep");
    RDMA_CHK(fi_pep_bind(pep, &eq->fid, 0),               "fi_pep_bind");
    RDMA_CHK(fi_listen(pep),                              "fi_listen");

    /* Start WAL-based incremental sync module + TCP incr listener. */
    if (ebpf_incr_master_start() != 0) {
        fprintf(stderr, "[RDMA-REPL] WAL incr sync unavailable "
                        "(full sync only)\n");
    }

    printf("[RDMA-REPL] master: listening for slaves\n");

    /* Main accept loop */
    for (;;) {
        uint8_t buf[sizeof(struct fi_eq_cm_entry) + 256];
        struct fi_eq_cm_entry *entry = (struct fi_eq_cm_entry *)buf;
        uint32_t event;
        ssize_t r = fi_eq_sread(eq, &event, entry, sizeof(buf), -1, 0);
        if (r < 0) {
            if (r == -FI_EAVAIL) {
                struct fi_eq_err_entry ee; memset(&ee, 0, sizeof(ee));
                fi_eq_readerr(eq, &ee, 0);
                fprintf(stderr, "[RDMA-REPL] EQ error: %s\n",
                        fi_eq_strerror(eq, ee.prov_errno, ee.err_data, NULL, 0));
            } else {
                fprintf(stderr, "[RDMA-REPL] fi_eq_sread: %s\n",
                        fi_strerror((int)-r));
            }
            continue;
        }
        if (event != FI_CONNREQ)
            continue;

        struct fi_info *client_info = entry->info;
        if (rdma_master_serve_one(fabric, eq, client_info) != 0)
            fprintf(stderr, "[RDMA-REPL] master: session failed\n");
        fi_freeinfo(client_info);
    }

fail:
cleanup:
    CLOSE_FID(pep);
    CLOSE_FID(eq);
    CLOSE_FID(fabric);
    if (info)  fi_freeinfo(info);
    if (hints) fi_freeinfo(hints);
    return NULL;
}

int rdma_repl_master_start(void) {
    pthread_t tid;
    int rc = pthread_create(&tid, NULL, rdma_master_thread, NULL);
    if (rc) {
        fprintf(stderr, "[RDMA-REPL] pthread_create failed: %s\n", strerror(rc));
        return -1;
    }
    pthread_detach(tid);
    return 0;
}

/* ==========================================================
 * 7. SLAVE: TCP incremental recv thread (with reconnect)
 *
 *    Wire frames: [uint64_t cmd_len][cmd_len bytes]
 *    cmd_len = 0 → disconnect sentinel
 *
 *    On first connect: reads +CONTINUE or -FULLRESYNC after INCR_SYNC.
 *    On disconnect: reconnects with exponential backoff, sends
 *    INCR_SYNC <replid> <last_seq+1>, reads response, continues.
 * ========================================================== */

/* Shared state between incr thread and main slave flow */
static char     g_slave_replid[INCR_REPL_ID_LEN + 1];
static uint64_t g_slave_base_seq       = 0;
static volatile uint64_t g_slave_applied     = 0;
static volatile int    g_slave_need_fullresync = 0;

int rdma_repl_slave_need_fullresync(void)
{
    return g_slave_need_fullresync;
}

static void *incr_tcp_slave_recv_thread(void *arg)
{
    int fd = (int)(intptr_t)arg;
    int applied = (int)g_slave_applied;
    char *buf = NULL;

    /* ── Outer loop: connection lifecycle ── */
    while (1) {

        /* ── Allocate recv buffer ── */
        if (!buf) {
            buf = (char *)malloc((size_t)INCR_MAX_CMD);
            if (!buf) {
                fprintf(stderr, "[RDMA-REPL] slave incr: OOM\n");
                close(fd);
                return NULL;
            }
        }

        printf("[RDMA-REPL] slave incr: connected, receiving (applied=%d, "
               "next_seq=%" PRIu64 ")\n",
               applied, g_slave_base_seq + (uint64_t)applied);

        /* ── Inner loop: recv wire frames ── */
        while (1) {
            uint64_t cmd_len = 0;
            size_t  len_off  = 0;
            while (len_off < sizeof(cmd_len)) {
                ssize_t n = recv(fd, (char *)&cmd_len + len_off,
                                 sizeof(cmd_len) - len_off, 0);
                if (n <= 0) {
                    fprintf(stderr, "[RDMA-REPL] slave incr: connection closed "
                            "(recv len), applied=%d\n", applied);
                    goto reconnect;
                }
                len_off += (size_t)n;
            }

            if (cmd_len == 0) {
                printf("[RDMA-REPL] slave incr: received disconnect sentinel\n");
                goto done;
            }
            if (cmd_len > INCR_MAX_CMD) {
                fprintf(stderr, "[RDMA-REPL] slave incr: oversized cmd %" PRIu64 "\n",
                        cmd_len);
                goto done;
            }

            size_t remain = (size_t)cmd_len;
            char *p = buf;
            while (remain > 0) {
                ssize_t nr = recv(fd, p, remain, 0);
                if (nr <= 0) {
                    fprintf(stderr, "[RDMA-REPL] slave incr: recv payload "
                            "failed\n");
                    goto reconnect;
                }
                p += nr;
                remain -= (size_t)nr;
            }

            DynBuf wb;
            dynbuf_init(&wb, 256);
            kvs_protocol(buf, (int)cmd_len, &wb);
            dynbuf_free(&wb);
            applied++;
            g_slave_applied = (uint64_t)applied;
        }

reconnect:
        /* ── Connection lost — attempt reconnect with backoff ── */
        printf("[RDMA-REPL] slave incr: reconnecting (applied=%d, "
               "next_seq=%" PRIu64 ")\n",
               applied, g_slave_base_seq + (uint64_t)applied);
        free(buf);
        buf = NULL;
        close(fd);

        {
            int backoff_ms = 250;
            int max_backoff = 8000;
            int attempts = 0;

            while (1) {
                usleep((unsigned int)(backoff_ms * 1000));

                fd = socket(AF_INET, SOCK_STREAM, 0);
                if (fd < 0) goto retry;

                struct sockaddr_in incr_addr;
                memset(&incr_addr, 0, sizeof(incr_addr));
                incr_addr.sin_family = AF_INET;
                incr_addr.sin_port   = htons((uint16_t)kvs_config.incr_port);
                inet_pton(AF_INET, kvs_config.rdma_master_ip,
                          &incr_addr.sin_addr);

                if (connect(fd, (struct sockaddr *)&incr_addr,
                            sizeof(incr_addr)) < 0) {
                    close(fd);
                    goto retry;
                }

                /* Send INCR_SYNC <replid> <next_seq> */
                {
                    uint64_t next_seq = g_slave_base_seq + (uint64_t)applied;
                    char sync_line[512];
                    int sync_len = snprintf(sync_line, sizeof(sync_line),
                                            INCR_SYNC_CMD " %s %" PRIu64 "\r\n",
                                            g_slave_replid, next_seq);
                    if (send(fd, sync_line, (size_t)sync_len,
                             MSG_NOSIGNAL) != sync_len) {
                        close(fd);
                        goto retry;
                    }
                    printf("[RDMA-REPL] slave incr: reconnect attempt %d, "
                           "INCR_SYNC %s %" PRIu64 "\n",
                           attempts + 1, g_slave_replid, next_seq);
                }

                /* Read master response */
                {
                    char rline[256];
                    int rpos = 0;
                    while (rpos < (int)sizeof(rline) - 1) {
                        char c;
                        ssize_t n = recv(fd, &c, 1, 0);
                        if (n <= 0) break;
                        if (c == '\n') {
                            if (rpos > 0 && rline[rpos - 1] == '\r') rpos--;
                            rline[rpos] = '\0';
                            break;
                        }
                        rline[rpos++] = c;
                    }

                    if (strncmp(rline, INCR_CONTINUE,
                                strlen(INCR_CONTINUE)) == 0) {
                        /* Update replid (normally unchanged, but
                         * extracted for consistency) */
                        const char *rp = rline + strlen(INCR_CONTINUE);
                        if (*rp == ' ') rp++;
                        size_t rlen = strlen(rp);
                        if (rlen > 0 && rlen <= INCR_REPL_ID_LEN) {
                            memcpy(g_slave_replid, rp, rlen);
                            g_slave_replid[rlen] = '\0';
                        }
                        printf("[RDMA-REPL] slave incr: +CONTINUE, "
                               "resuming stream\n");
                        break;  /* Success — re-enter recv loop */
                    }

                    if (strncmp(rline, INCR_FULLRESYNC,
                                strlen(INCR_FULLRESYNC)) == 0) {
                        const char *p = rline + strlen(INCR_FULLRESYNC);
                        if (*p == ' ') p++;
                        size_t rlen = strlen(p);
                        if (rlen <= INCR_REPL_ID_LEN && rlen > 0) {
                            memcpy(g_slave_replid, p, rlen);
                            g_slave_replid[rlen] = '\0';
                        }
                        printf("[RDMA-REPL] slave incr: master requested "
                               "fullresync (new replid=%s)\n", g_slave_replid);
                        g_slave_need_fullresync = 1;
                        close(fd);
                        return NULL;
                    }
                    close(fd);
                }

retry:
                attempts++;
                if (backoff_ms < max_backoff)
                    backoff_ms *= 2;
                if (attempts >= 20) {
                    fprintf(stderr, "[RDMA-REPL] slave incr: reconnect "
                            "failed after %d attempts, requesting fullresync\n",
                            attempts);
                    g_slave_need_fullresync = 1;
                    return NULL;
                }
            }
        }
        /* Continue outer loop — buf will be re-allocated */
    }

done:
    printf("[RDMA-REPL] slave incr: thread exiting, applied %d commands\n",
           applied);
    g_slave_applied = (uint64_t)applied;
    free(buf);
    close(fd);
    return NULL;
}

/* ==========================================================
 * 8. SLAVE: streaming full-sync receiver + incr spawn
 *
 * Wire protocol (streaming, over FI_EP_MSG):
 *   Each fi_send delivers one chunk:
 *     [chunk_len:4][chunk_type:1][data:chunk_len]
 *   chunk_type = 0 HEADER  (payload: magic+version+db_num+key_count+base_seq)
 *   chunk_type = 1 DATA    (payload: serialized entries)
 *   chunk_type = 2 EOF     (chunk_len=0, end of stream)
 *
 * Slave pre-posts 4 recv buffers of CHUNK_WIRE_SIZE.
 * After EOF, closes RDMA, connects TCP incr with INCR_SYNC <base_seq>.
 * ========================================================== */

int rdma_repl_slave_run(void) {
    struct fi_info    *hints  = NULL, *info = NULL;
    struct fid_fabric *fabric = NULL;
    struct fid_domain *domain = NULL;
    struct fid_eq     *eq     = NULL;
    struct fid_cq     *cq     = NULL;
    struct fid_ep     *ep     = NULL;
    struct fid_mr     *mr_recv[SHM_RING_SIZE] = {0};
    char   *recv_buf[SHM_RING_SIZE] = {0};
    struct fi_context recv_ctx[SHM_RING_SIZE];
    uint64_t base_seq    = 0;
    int    rc_ret        = -1;
    int    eof_received  = 0;
    int    header_parsed = 0;
    int    total_keys    = 0;
    int    pending       = 0;

    hints = rdma_build_hints();
    if (!hints) return -1;

    char service[16];
    snprintf(service, sizeof(service), "%d", kvs_config.rdma_master_port);

    int rc = fi_getinfo(FI_VERSION(1, 17),
                        kvs_config.rdma_master_ip, service,
                        0, hints, &info);
    if (rc) {
        fprintf(stderr, "[RDMA-REPL] fi_getinfo (slave) failed: %s\n",
                fi_strerror(-rc));
        goto fail;
    }
    printf("[RDMA-REPL] slave: using provider '%s', connecting to %s:%s\n",
           info->fabric_attr->prov_name,
           kvs_config.rdma_master_ip, service);

    struct fi_eq_attr eq_attr = {0};
    eq_attr.wait_obj = FI_WAIT_UNSPEC;
    struct fi_cq_attr cq_attr = {0};
    cq_attr.format   = FI_CQ_FORMAT_DATA;
    cq_attr.wait_obj = FI_WAIT_UNSPEC;

    RDMA_CHK(fi_fabric(info->fabric_attr, &fabric, NULL), "fi_fabric");
    RDMA_CHK(fi_eq_open(fabric, &eq_attr, &eq, NULL),     "fi_eq_open");
    RDMA_CHK(fi_domain(fabric, info, &domain, NULL),      "fi_domain");
    RDMA_CHK(fi_cq_open(domain, &cq_attr, &cq, NULL),     "fi_cq_open");
    RDMA_CHK(fi_endpoint(domain, info, &ep, NULL),        "fi_endpoint");
    RDMA_CHK(fi_ep_bind(ep, &eq->fid, 0),                 "ep bind eq");
    RDMA_CHK(fi_ep_bind(ep, &cq->fid, FI_SEND | FI_RECV), "ep bind cq");
    RDMA_CHK(fi_enable(ep),                               "fi_enable");

    /* Allocate and pre-post recv ring (4 × CHUNK_WIRE_SIZE) */
    for (int i = 0; i < SHM_RING_SIZE; i++) {
        recv_buf[i] = (char *)kvs_malloc(CHUNK_WIRE_SIZE);
        if (!recv_buf[i]) {
            fprintf(stderr, "[RDMA-REPL] slave: OOM for recv buf\n");
            goto fail;
        }
        RDMA_CHK(rdma_mr_reg(domain, recv_buf[i], CHUNK_WIRE_SIZE,
                              FI_RECV, &mr_recv[i]), "mr_reg recv");
        memset(&recv_ctx[i], 0, sizeof(recv_ctx[i]));

        ssize_t r = fi_recv(ep, recv_buf[i], CHUNK_WIRE_SIZE,
                             fi_mr_desc(mr_recv[i]), 0, &recv_ctx[i]);
        if (r) {
            fprintf(stderr, "[RDMA-REPL] fi_recv[%d]: %s\n",
                    i, fi_strerror((int)-r));
            goto fail;
        }
        pending++;
    }

    /* Connect + wait CONNECTED */
    RDMA_CHK(fi_connect(ep, info->dest_addr, NULL, 0), "fi_connect");
    {
        uint32_t event;
        struct fi_eq_cm_entry entry;
        if (rdma_wait_eq(eq, &event, &entry, sizeof(entry)) < 0) goto fail;
        if (event != FI_CONNECTED) {
            fprintf(stderr, "[RDMA-REPL] slave: expected FI_CONNECTED got %u\n",
                    event);
            goto fail;
        }
    }
    printf("[RDMA-REPL] slave: connected, awaiting streaming full sync\n");
    fflush(stdout);

    /* Timing: measure pure transfer + load time, not process startup */
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    /* Streaming recv loop */
    while (!eof_received && pending > 0) {
        struct fi_cq_data_entry ce;
        ssize_t r = fi_cq_sread(cq, &ce, 1, NULL, -1);
        if (r < 0) {
            if (r == -FI_EAVAIL) {
                struct fi_cq_err_entry ee; memset(&ee, 0, sizeof(ee));
                fi_cq_readerr(cq, &ee, 0);
                fprintf(stderr, "[RDMA-REPL] slave: CQ error: %s\n",
                        fi_cq_strerror(cq, ee.prov_errno, ee.err_data, NULL, 0));
            }
            break;
        }
        if (r != 1) continue;
        pending--;

        /* Identify which buffer received data via op_context */
        int buf_idx = -1;
        for (int i = 0; i < SHM_RING_SIZE; i++) {
            if (&recv_ctx[i] == (struct fi_context *)ce.op_context) {
                buf_idx = i;
                break;
            }
        }
        if (buf_idx < 0) {
            fprintf(stderr, "[RDMA-REPL] slave: unknown recv buffer "
                    "(buf=%p, ctx=%p)\n",
                    ce.buf, ce.op_context);
            break;
        }

        char   *buf = recv_buf[buf_idx];
        size_t  msg_len = ce.len;

        if (msg_len < CHUNK_HDR_SIZE) {
            fprintf(stderr, "[RDMA-REPL] slave: short chunk %zu bytes\n",
                    msg_len);
            break;
        }

        uint32_t chunk_len;
        uint8_t  chunk_type;
        memcpy(&chunk_len,  buf, 4);
        chunk_type = (uint8_t)buf[4];

        const char *payload = buf + CHUNK_HDR_SIZE;

        switch (chunk_type) {
        case CHUNK_TYPE_HEADER:
            /* Legacy header chunk (kept for compatibility).  The new
             * streaming protocol embeds the header in the first DATA
             * chunk, but old masters may still send a standalone HEADER. */
        case CHUNK_TYPE_DATA: {
            if (!header_parsed) {
                /* First data-bearing chunk: parse header from first
                 * 24 bytes, then feed the remainder as entries. */
                if (chunk_len < 24) {
                    fprintf(stderr, "[RDMA-REPL] slave: first chunk too "
                            "short for header (%u bytes)\n", chunk_len);
                    goto done;
                }
                int key_count = rdb_load_stream_begin(payload, 16);
                if (key_count < 0) {
                    fprintf(stderr, "[RDMA-REPL] slave: bad header magic\n");
                    goto done;
                }
                memcpy(&base_seq, payload + 16, 8);
                g_slave_base_seq = base_seq;
                printf("[RDMA-REPL] slave: header — base_seq=%" PRIu64 ", "
                       "expected=%d keys\n", base_seq, key_count);
                header_parsed = 1;

                /* Remaining bytes after the 24-byte header are entries */
                if (chunk_len > 24) {
                    int n = rdb_load_stream_append(payload + 24,
                                                   (size_t)(chunk_len - 24));
                    if (n < 0) {
                        fprintf(stderr, "[RDMA-REPL] slave: bad data in "
                                "first chunk\n");
                        goto done;
                    }
                    total_keys += n;
                }
            } else {
                /* Subsequent chunks are pure entry data */
                if (chunk_len > 0) {
                    int n = rdb_load_stream_append(payload,
                                                   (size_t)chunk_len);
                    if (n < 0) {
                        fprintf(stderr, "[RDMA-REPL] slave: bad data chunk\n");
                        goto done;
                    }
                    total_keys += n;
                }
            }
            break;
        }
        case CHUNK_TYPE_EOF:
            eof_received = 1;
            printf("[RDMA-REPL] slave: EOF received, rebuilding encodings\n");
            rdb_rebuild_all_encodings();
            goto done;
        default:
            fprintf(stderr, "[RDMA-REPL] slave: unknown chunk type %u\n",
                    chunk_type);
            goto done;
        }

        /* Re-post the buffer */
        {
            ssize_t rr = fi_recv(ep, recv_buf[buf_idx], CHUNK_WIRE_SIZE,
                                 fi_mr_desc(mr_recv[buf_idx]), 0,
                                 &recv_ctx[buf_idx]);
            if (rr) {
                fprintf(stderr, "[RDMA-REPL] slave: fi_recv repost: %s\n",
                        fi_strerror((int)-rr));
                break;
            }
            pending++;
        }
    }

done:
    clock_gettime(CLOCK_MONOTONIC, &t1);
    {
        double elapsed_ms = (t1.tv_sec - t0.tv_sec) * 1000.0 +
                            (t1.tv_nsec - t0.tv_nsec) / 1000000.0;
        printf("[RDMA-REPL] slave: fullsync_time_ms=%.1f\n", elapsed_ms);
    }
    printf("[RDMA-REPL] slave: loaded %d keys from fullsync (rdb streaming)\n",
           total_keys);
    fflush(stdout);

    rc_ret = 0;

fail:
    /* Release RDMA resources */
    for (int i = 0; i < SHM_RING_SIZE; i++) {
        if (mr_recv[i]) CLOSE_FID(mr_recv[i]);
        if (recv_buf[i]) kvs_free(recv_buf[i]);
    }
    if (eof_received && ep) fi_shutdown(ep, 0);
    CLOSE_FID(ep);
    CLOSE_FID(cq);
    CLOSE_FID(domain);
    CLOSE_FID(eq);
    CLOSE_FID(fabric);
    if (info)  fi_freeinfo(info);
    if (hints) fi_freeinfo(hints);
    info = NULL; hints = NULL;

    if (rc_ret != 0) return rc_ret;

    /* Start AF_XDP incremental stream from RDMA fullsync cutover seq. */
    repl_stream_start_slave(base_seq);
    printf("[RDMA-REPL] slave: AF_XDP incremental stream staged from seq=%" PRIu64 "\n",
           base_seq);

    return 0;
}
