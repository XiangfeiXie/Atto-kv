/* ===================================================================
 * rdma_repl.c  -  Full-sync over libfabric (Soft-RoCE / verbs / tcp)
 *                 with fork() for COW-consistent engine snapshot.
 *
 * Master:
 *   Listens for RDMA connections. On accept:
 *     1. Reads eBPF global_seq → base_seq (fullsync cut point)
 *     2. Signals incr thread: status = INPROGRESS (pause direct send)
 *     3. fork()
 *     4. Child: serializes engine (COW snapshot) → pipe → _exit
 *     5. Parent: reads pipe, sends [base_seq|payload_len|payload] via RDMA
 *     6. Signals incr thread: status = CATCHUP
 *     7. Slave later connects TCP incr, sends INCR_SYNC, catches up
 *
 * Slave:
 *   Connects to master RDMA, receives fullsync_header + payload.
 *   Reconstructs state, then connects TCP incr with INCR_SYNC <base_seq>.
 * =================================================================== */

#include "rdma_repl.h"
#include "kvstore.h"
#include "config.h"
#include "server.h"
#include "ebpf_incr_repl.h"
#include "persistence.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>

#include <sys/socket.h>
#include <sys/wait.h>
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

/* ==========================================================
 * 2. MASTER: fork-based fullsync session
 *
 * Child process (after fork):
 *   - Has COW snapshot of the entire engine
 *   - Serializes engine → pipe
 *   - _exit(0)
 *
 * Parent process (RDMA listener thread):
 *   - Reads serialized data from pipe
 *   - Sends [base_seq|payload_len|payload] via RDMA
 *   - Coordinates with incremental thread state machine
 * ========================================================== */

static int rdma_master_serve_one(struct fid_fabric *fabric,
                                 struct fid_eq     *eq,
                                 struct fi_info    *client_info)
{
    struct fid_domain   *domain = NULL;
    struct fid_ep       *ep     = NULL;
    struct fid_cq       *cq     = NULL;
    struct fid_mr       *mr_hdr = NULL, *mr_buf = NULL;
    char   *payload_buf = NULL;
    pid_t  child_pid    = -1;
    int    pipefd[2]    = {-1, -1};
    int    rc           = -1;

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
    printf("[RDMA-REPL] master: slave connected, starting full sync\n");

    /* 3) Begin fullsync: read eBPF global_seq, set status=INPROGRESS */
    uint64_t base_seq = ebpf_incr_fullsync_begin();

    /* 4) Create pipe for child→parent data transfer.
     *    The child serializes the engine (COW snapshot) and writes to pipe.
     *    The parent reads and sends via RDMA. */
    if (pipe(pipefd) != 0) {
        fprintf(stderr, "[RDMA-REPL] pipe() failed: %s\n", strerror(errno));
        ebpf_incr_fullsync_finish();  /* revert to IDLE */
        goto fail;
    }

    child_pid = fork();
    if (child_pid < 0) {
        fprintf(stderr, "[RDMA-REPL] fork() failed: %s\n", strerror(errno));
        close(pipefd[0]);
        close(pipefd[1]);
        ebpf_incr_fullsync_finish();
        goto fail;
    }

    if (child_pid == 0) {
        /* ============================================================
         * CHILD PROCESS: COW snapshot of engine at fork() moment.
         * Only async-signal-safe operations here (no malloc in theory,
         * but jemalloc is fork-safe and we _exit immediately after).
         * ============================================================ */
        close(pipefd[0]);  /* close read end */

        DynBuf payload;
        dynbuf_init(&payload, 64 * 1024);
        rdb_serialize_to_buf(&payload);

        uint64_t p_len = (uint64_t)payload.len;
        printf("[RDMA-REPL] child: serialized %" PRIu64 " bytes, "
               "base_seq=%" PRIu64 "\n", p_len, base_seq);

        /* Write payload_len then payload to pipe */
        ssize_t wn;
        wn = write(pipefd[1], &p_len, sizeof(p_len));
        (void)wn;
        if (p_len > 0) {
            wn = write(pipefd[1], payload.buf, (size_t)p_len);
            (void)wn;
        }

        dynbuf_free(&payload);
        close(pipefd[1]);
        _exit(0);
    }

    /* ============================================================
     * PARENT PROCESS: read serialized data from child, send via RDMA
     * ============================================================ */
    close(pipefd[1]);  /* close write end */

    /* Read payload_len + payload from the pipe */
    uint64_t payload_len = 0;
    ssize_t rn = read(pipefd[0], &payload_len, sizeof(payload_len));
    if (rn != (ssize_t)sizeof(payload_len)) {
        fprintf(stderr, "[RDMA-REPL] failed to read payload_len from pipe\n");
        close(pipefd[0]);
        goto wait_child;
    }

    if (payload_len > 0) {
        payload_buf = (char *)kvs_malloc((size_t)payload_len);
        if (!payload_buf) {
            fprintf(stderr, "[RDMA-REPL] OOM for payload (%" PRIu64 " bytes)\n",
                    payload_len);
            close(pipefd[0]);
            goto wait_child;
        }

        size_t remain = (size_t)payload_len;
        char  *p      = payload_buf;
        while (remain > 0) {
            rn = read(pipefd[0], p, remain);
            if (rn <= 0) {
                fprintf(stderr, "[RDMA-REPL] pipe read failed\n");
                kvs_free(payload_buf);
                payload_buf = NULL;
                close(pipefd[0]);
                goto wait_child;
            }
            p      += rn;
            remain -= (size_t)rn;
        }
    }
    close(pipefd[0]);
    pipefd[0] = -1;

    printf("[RDMA-REPL] master: received %" PRIu64 " bytes from child\n",
           payload_len);

    /* 5) Send [base_seq][payload_len][payload] via RDMA */
    struct fullsync_header hdr;
    hdr.base_seq    = base_seq;
    hdr.payload_len = payload_len;

    RDMA_CHK(rdma_mr_reg(domain, &hdr, sizeof(hdr),
                         FI_SEND, &mr_hdr), "mr_reg hdr");
    if (payload_len > 0 && payload_buf) {
        RDMA_CHK(rdma_mr_reg(domain, payload_buf, (size_t)payload_len,
                             FI_SEND, &mr_buf), "mr_reg buf");
    }

    /* Send header */
    {
        struct fi_context ctx;
        ssize_t sr = fi_send(ep, &hdr, sizeof(hdr),
                             fi_mr_desc(mr_hdr), 0, &ctx);
        if (sr) {
            fprintf(stderr, "[RDMA-REPL] fi_send hdr: %s\n",
                    fi_strerror((int)-sr));
            goto fail;
        }
        if (rdma_wait_cq(cq) < 0) goto fail;
    }

    /* Send payload */
    if (payload_len > 0 && payload_buf) {
        struct fi_context ctx;
        ssize_t sr = fi_send(ep, payload_buf, (size_t)payload_len,
                             fi_mr_desc(mr_buf), 0, &ctx);
        if (sr) {
            fprintf(stderr, "[RDMA-REPL] fi_send buf: %s\n",
                    fi_strerror((int)-sr));
            goto fail;
        }
        if (rdma_wait_cq(cq) < 0) goto fail;
    }

    printf("[RDMA-REPL] master: full sync shipped (base_seq=%" PRIu64 ", "
           "payload=%" PRIu64 " bytes)\n", base_seq, payload_len);
    rc = 0;

wait_child:
    /* Reap child process */
    if (child_pid > 0) {
        int wstatus;
        waitpid(child_pid, &wstatus, 0);
    }

    /* Transition incremental thread to CATCHUP.
     * Consumer has been buffering events to backlog during INPROGRESS.
     * Now the slave will connect TCP incr, send INCR_SYNC <base_seq>,
     * and receive backlog catchup. */
    if (rc == 0) {
        ebpf_incr_fullsync_send_done();
    } else {
        /* Fullsync failed — revert to IDLE so normal streaming resumes */
        ebpf_incr_fullsync_finish();
    }

fail:
    /* Release fullsync resources */
    CLOSE_FID(mr_buf);
    CLOSE_FID(mr_hdr);
    if (payload_buf) { kvs_free(payload_buf); payload_buf = NULL; }

    /* Close the RDMA connection; incr sync happens over TCP */
    if (rc == 0 && ep) fi_shutdown(ep, 0);

    CLOSE_FID(ep);
    CLOSE_FID(cq);
    CLOSE_FID(domain);
    if (pipefd[0] >= 0) close(pipefd[0]);
    if (pipefd[1] >= 0) close(pipefd[1]);
    return rc;
}

/* ========================================================== */
/* 3. MASTER: listener thread                                 */
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

    /* Start eBPF incremental sync module + TCP incr listener.
     * This must be running BEFORE we accept any slave connections,
     * because the master thread calls ebpf_incr_fullsync_begin()
     * which reads the eBPF global_seq map. */
    if (ebpf_incr_master_start() != 0) {
        fprintf(stderr, "[RDMA-REPL] eBPF incr sync unavailable "
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
 * 4. SLAVE: TCP incremental recv thread
 *    Reads wire frames: [uint64_t cmd_len][cmd_len bytes]
 *    cmd_len = 0 → disconnect sentinel
 * ========================================================== */

static void *incr_tcp_slave_recv_thread(void *arg)
{
    int fd = (int)(intptr_t)arg;
    int applied = 0;
    char *buf = (char *)malloc((size_t)INCR_MAX_CMD);
    if (!buf) {
        fprintf(stderr, "[RDMA-REPL] slave incr: OOM for recv buf\n");
        close(fd);
        return NULL;
    }

    printf("[RDMA-REPL] slave: TCP incremental recv thread started\n");

    for (;;) {
        uint64_t cmd_len = 0;
        ssize_t n = recv(fd, &cmd_len, sizeof(cmd_len), MSG_WAITALL);
        if (n <= 0) {
            fprintf(stderr, "[RDMA-REPL] slave incr: connection closed "
                    "(recv len)\n");
            break;
        }

        if (cmd_len == 0) {
            printf("[RDMA-REPL] slave incr: received disconnect sentinel\n");
            break;
        }
        if (cmd_len > INCR_MAX_CMD) {
            fprintf(stderr, "[RDMA-REPL] slave incr: oversized cmd %" PRIu64 "\n",
                    cmd_len);
            break;
        }

        size_t remain = (size_t)cmd_len;
        char *p = buf;
        while (remain > 0) {
            n = recv(fd, p, remain, MSG_WAITALL);
            if (n <= 0) {
                fprintf(stderr, "[RDMA-REPL] slave incr: recv payload failed\n");
                goto done;
            }
            p += n;
            remain -= (size_t)n;
        }

        DynBuf wb;
        dynbuf_init(&wb, 256);
        kvs_protocol(buf, (int)cmd_len, &wb);
        dynbuf_free(&wb);
        applied++;
    }

done:
    printf("[RDMA-REPL] slave incr: thread exiting, applied %d commands\n",
           applied);
    free(buf);
    close(fd);
    return NULL;
}

/* ==========================================================
 * 5. SLAVE: full-sync receiver + spawn incremental thread
 *
 * Wire protocol:
 *   1. master → slave: struct fullsync_header (16 bytes)
 *      [ uint64_t base_seq | uint64_t payload_len ]
 *   2. master → slave: payload_len bytes of RESP SET stream
 *
 * After full sync, slave connects TCP incr and sends:
 *   INCR_SYNC <base_seq>\r\n
 * ========================================================== */

int rdma_repl_slave_run(void) {
    struct fi_info    *hints  = NULL, *info = NULL;
    struct fid_fabric *fabric = NULL;
    struct fid_domain *domain = NULL;
    struct fid_eq     *eq     = NULL;
    struct fid_cq     *cq     = NULL;
    struct fid_ep     *ep     = NULL;
    struct fid_mr     *mr_hdr = NULL, *mr_buf = NULL;
    char   *payload  = NULL;
    struct fullsync_header fshdr;
    uint64_t base_seq    = 0;
    int    rc_ret        = -1;

    memset(&fshdr, 0, sizeof(fshdr));

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

    /* Pre-post header recv BEFORE connecting */
    RDMA_CHK(rdma_mr_reg(domain, &fshdr, sizeof(fshdr),
                         FI_RECV, &mr_hdr), "mr_reg hdr");
    {
        struct fi_context ctx;
        ssize_t r = fi_recv(ep, &fshdr, sizeof(fshdr),
                            fi_mr_desc(mr_hdr), 0, &ctx);
        if (r) {
            fprintf(stderr, "[RDMA-REPL] fi_recv hdr: %s\n",
                    fi_strerror((int)-r));
            goto fail;
        }
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
    printf("[RDMA-REPL] slave: connected, awaiting full sync\n");

    /* Wait for header */
    if (rdma_wait_cq(cq) < 0) goto fail;
    base_seq             = fshdr.base_seq;
    uint64_t payload_len = fshdr.payload_len;

    printf("[RDMA-REPL] slave: fullsync header — base_seq=%" PRIu64 ", "
           "payload=%" PRIu64 " bytes\n", base_seq, payload_len);

    if (payload_len > 0) {
        payload = (char *)kvs_malloc((size_t)payload_len);
        if (!payload) { fprintf(stderr, "[RDMA-REPL] oom\n"); goto fail; }

        RDMA_CHK(rdma_mr_reg(domain, payload, (size_t)payload_len,
                             FI_RECV, &mr_buf), "mr_reg buf");

        struct fi_context ctx;
        ssize_t r = fi_recv(ep, payload, (size_t)payload_len,
                            fi_mr_desc(mr_buf), 0, &ctx);
        if (r) {
            fprintf(stderr, "[RDMA-REPL] fi_recv buf: %s\n",
                    fi_strerror((int)-r));
            goto fail;
        }
        if (rdma_wait_cq(cq) < 0) goto fail;

        /* Clean local state and reconstruct from RDB binary stream.
         * rdb_load_from_buf() clears the engine internally. */
        int n = rdb_load_from_buf(payload, (size_t)payload_len);
        printf("[RDMA-REPL] slave: loaded %d keys from fullsync (rdb binary)\n",
               n);
    } else {
        printf("[RDMA-REPL] slave: master is empty, nothing to apply\n");
        kvs_flush_all();
    }

    /* Full sync done — close RDMA resources */
    CLOSE_FID(mr_buf);
    CLOSE_FID(mr_hdr);
    if (payload) { kvs_free(payload); payload = NULL; }

    fi_shutdown(ep, 0);
    CLOSE_FID(ep);
    CLOSE_FID(cq);
    CLOSE_FID(domain);
    CLOSE_FID(eq);
    CLOSE_FID(fabric);
    if (info)  fi_freeinfo(info);
    if (hints) fi_freeinfo(hints);
    info = NULL; hints = NULL;

    /* ---- Connect to master's TCP incr port with INCR_SYNC handshake ---- */
    int incr_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (incr_fd < 0) {
        fprintf(stderr, "[RDMA-REPL] slave: incr socket() failed: %s\n",
                strerror(errno));
        return 0;
    }

    struct sockaddr_in incr_addr;
    memset(&incr_addr, 0, sizeof(incr_addr));
    incr_addr.sin_family = AF_INET;
    incr_addr.sin_port   = htons((uint16_t)kvs_config.incr_port);
    inet_pton(AF_INET, kvs_config.rdma_master_ip, &incr_addr.sin_addr);

    if (connect(incr_fd, (struct sockaddr *)&incr_addr, sizeof(incr_addr)) < 0) {
        fprintf(stderr, "[RDMA-REPL] slave: incr connect(%s:%d) failed: %s\n",
                kvs_config.rdma_master_ip, kvs_config.incr_port,
                strerror(errno));
        close(incr_fd);
        return 0;
    }

    /* Send INCR_SYNC <base_seq> handshake so the master knows
     * where to start the backlog catchup. */
    char sync_line[256];
    int sync_len = snprintf(sync_line, sizeof(sync_line),
                            INCR_SYNC_CMD " %" PRIu64 "\r\n", base_seq);
    if (send(incr_fd, sync_line, (size_t)sync_len, MSG_NOSIGNAL) != sync_len) {
        fprintf(stderr, "[RDMA-REPL] slave: INCR_SYNC send failed: %s\n",
                strerror(errno));
        close(incr_fd);
        return 0;
    }

    printf("[RDMA-REPL] slave: connected to master incr TCP %s:%d, "
           "sent INCR_SYNC %" PRIu64 "\n",
           kvs_config.rdma_master_ip, kvs_config.incr_port, base_seq);

    /* Spawn TCP incremental recv thread */
    {
        pthread_t tid;
        if (pthread_create(&tid, NULL, incr_tcp_slave_recv_thread,
                           (void *)(intptr_t)incr_fd) != 0) {
            fprintf(stderr, "[RDMA-REPL] slave: pthread_create incr: %s\n",
                    strerror(errno));
            close(incr_fd);
        } else {
            pthread_detach(tid);
            printf("[RDMA-REPL] slave: TCP incremental recv thread spawned\n");
        }
    }

    return 0;

fail:
    CLOSE_FID(mr_buf);
    CLOSE_FID(mr_hdr);
    CLOSE_FID(ep);
    CLOSE_FID(cq);
    CLOSE_FID(domain);
    CLOSE_FID(eq);
    CLOSE_FID(fabric);
    if (payload) kvs_free(payload);
    if (info)  fi_freeinfo(info);
    if (hints) fi_freeinfo(hints);
    return rc_ret;
}
