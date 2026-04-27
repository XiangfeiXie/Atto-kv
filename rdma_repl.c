/* ===================================================================
 * rdma_repl.c  -  Full-sync over libfabric (Soft-RoCE / verbs / tcp)
 *
 * Implements requirement:
 *   1. Keep legacy replication intact but OFF by default; this module
 *      is fully isolated from repl.c and is toggled via kvs_config.
 *   2. Only the *full* sync phase is implemented here. Incremental
 *      propagation is NOT handled.
 *   3. Master serializes the whole dataset into an in-memory buffer
 *      using the Redis RESP protocol (one SET per key), then ships
 *      the buffer via libfabric. The slave deserializes in memory
 *      by replaying the commands through the existing kvs_protocol().
 *
 * Wire protocol on the MSG endpoint:
 *     master -> slave :  [uint64_t payload_len]
 *     master -> slave :  [payload_len bytes of RESP stream]
 * =================================================================== */

#include "rdma_repl.h"
#include "kvstore.h"
#include "config.h"
#include "server.h"
#include "ebpf_incr_repl.h"

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

/* -------- engine access (defined in the engine *.c files) -------- */
extern kvs_array_t     global_array;
extern kvs_rbtree_t    global_rbtree;
extern kvs_hash_t      global_hash;
extern kvs_skiptable_t global_skiptable;

/* -------- helpers from kvstore.c -------- */
extern int   kvs_protocol(char *msg, int len, DynBuf *write_buf);

/* 1 if eBPF incremental module started successfully, 0 otherwise */
static int g_ebpf_ok = 0;

/* ---- Slave-side incremental recv context ---- */
#define INCR_RECV_BUF_SZ  ((size_t)(sizeof(uint64_t) + INCR_MAX_CMD))

struct slave_incr_ctx {
    struct fid_ep     *ep;
    struct fid_cq     *cq;
    struct fid_domain *domain;
    struct fid_mr     *recv_mr;
    uint8_t           *recv_buf;  /* INCR_RECV_BUF_SZ bytes */
};

/* ========================================================== */
/* 1. RESP SERIALIZATION (master side)                        */
/* ========================================================== */

/* Append a RESP bulk string header "$<len>\r\n" + raw bytes + "\r\n" */
static void resp_append_bulk(DynBuf *out, const char *data, int len) {
    char hdr[32];
    int n = snprintf(hdr, sizeof(hdr), "$%d\r\n", len);
    dynbuf_append(out, hdr, n);
    dynbuf_append(out, data, len);
    dynbuf_append(out, "\r\n", 2);
}

/* Per-key callback: emit a full "SET key value" RESP command */
static void rdma_serialize_cb(const char *key, KVItem *item, void *priv) {
    if (!key || !item) return;
    DynBuf *out = (DynBuf *)priv;

    /* *3\r\n$3\r\nSET\r\n$<klen>\r\n<key>\r\n$<vlen>\r\n<val>\r\n */
    dynbuf_append(out, "*3\r\n", 4);
    resp_append_bulk(out, "SET", 3);
    resp_append_bulk(out, key, (int)strlen(key));
    resp_append_bulk(out, item->value, (int)item->len);
}

/* Walk the active engine and produce a RESP command stream. */
static void rdma_serialize_dataset(DynBuf *out) {
    if      (kvs_config.engine == ENGINE_ARRAY)
        kvs_array_scan(&global_array, rdma_serialize_cb, out);
    else if (kvs_config.engine == ENGINE_RBTREE)
        kvs_rbtree_scan(&global_rbtree, rdma_serialize_cb, out);
    else if (kvs_config.engine == ENGINE_HASH)
        kvs_hash_scan(&global_hash, rdma_serialize_cb, out);
    else if (kvs_config.engine == ENGINE_SKIPTABLE)
        kvs_skiptable_scan(&global_skiptable, rdma_serialize_cb, out);
}

/* ========================================================== */
/* 2. RESP DESERIALIZATION (slave side)                       */
/* ========================================================== */

static int rdma_apply_resp_stream(const char *buf, size_t len) {
    const char *p = buf;
    size_t remaining = len;
    int applied = 0;

    while (remaining > 0) {
        int cmd_len = resp_try_parse_full_cmd(p, (int)remaining);
        if (cmd_len <= 0) {
            if (remaining != 0)
                printf("[RDMA-REPL] trailing %zu bytes could not be parsed\n",
                       remaining);
            break;
        }

        DynBuf wb;
        dynbuf_init(&wb, BUF_INIT_CAP);
        kvs_protocol((char *)p, cmd_len, &wb);
        dynbuf_free(&wb);

        p += cmd_len;
        remaining -= (size_t)cmd_len;
        applied++;
    }
    return applied;
}

/* ========================================================== */
/* 3. libfabric PLUMBING                                      */
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
    /* hints->mode = 0: do NOT restrict mode bits in hints.
     * libfabric 1.17 verbs provider (vrb_check_hints) unconditionally checks
     * that the application declares FI_MR_LOCAL support via EITHER:
     *   a) hints->mode & FI_LOCAL_MR  (deprecated pre-1.5 API), or
     *   b) hints->domain_attr->mr_mode & FI_MR_LOCAL  (current API).
     * Setting mode=0 disables the legacy mode filter; setting
     * domain_attr->mr_mode = FI_MR_LOCAL satisfies (b) and also passes
     * the standard fi_getinfo domain-attr match (verbs has FI_MR_LOCAL
     * in its mr_mode, so the subset check passes).
     * Our code already satisfies FI_MR_LOCAL at runtime: fi_mr_reg() is
     * called on every buffer and fi_mr_desc() is passed to fi_send/recv. */
    hints->mode                         = 0;
    /* libfabric 1.17 ofi_check_mr_mode strips FI_MR_VIRT_ADDR/FI_MR_PROV_KEY
     * via OFI_MR_BASIC_MAP and then requires the remaining bits to be
     * declared in hints.  For the verbs/Soft-RoCE provider those are
     * FI_MR_LOCAL | FI_MR_ALLOCATED. */
    hints->domain_attr->mr_mode         = FI_MR_LOCAL | FI_MR_ALLOCATED;
    if (kvs_config.rdma_provider[0] &&
        strcasecmp(kvs_config.rdma_provider, "auto") != 0) {
        hints->fabric_attr->prov_name = strdup(kvs_config.rdma_provider);
    }
    return hints;
}

/* Blocking CQ wait for exactly one completion. */
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

/* Blocking EQ wait for a single CM event; returns the event id. */
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
/* 4. MASTER: per-slave session                               */
/* ========================================================== */

static int rdma_master_serve_one(struct fid_fabric *fabric,
                                 struct fid_eq     *eq,
                                 struct fi_info    *client_info) {
    struct fid_domain   *domain = NULL;
    struct fid_ep       *ep     = NULL;
    struct fid_cq       *cq     = NULL;
    struct fid_mr       *mr_hdr = NULL, *mr_buf = NULL;
    DynBuf payload = {0};
    int rc = -1;
    uint64_t incr_start_seq = 0;

    /* 1) Build domain, CQ, endpoint for this client */
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

    if (g_ebpf_ok) {
        if (!ebpf_incr_master_healthy() ||
            ebpf_incr_get_seq(&incr_start_seq) != 0) {
            fprintf(stderr,
                    "[RDMA-REPL] incremental stream unhealthy; "
                    "fallback to full-sync session\n");
            g_ebpf_ok = 0;
            incr_start_seq = 0;
        }
    }

    /* 3) Serialize whole dataset to an in-memory RESP stream */
    dynbuf_init(&payload, 64 * 1024);
    rdma_serialize_dataset(&payload);
    uint64_t payload_len = (uint64_t)payload.len;
    printf("[RDMA-REPL] master: serialized %" PRIu64 " bytes\n", payload_len);

    /* 4) Register MRs and ship header + body */
    RDMA_CHK(rdma_mr_reg(domain, &payload_len, sizeof(payload_len),
                         FI_SEND, &mr_hdr), "mr_reg hdr");
    if (payload_len > 0) {
        RDMA_CHK(rdma_mr_reg(domain, payload.buf, payload_len,
                             FI_SEND, &mr_buf), "mr_reg buf");
    }

    {
        struct fi_context ctx;
        ssize_t sr = fi_send(ep, &payload_len, sizeof(payload_len),
                             fi_mr_desc(mr_hdr), 0, &ctx);
        if (sr) { fprintf(stderr, "[RDMA-REPL] fi_send hdr: %s\n",
                          fi_strerror((int)-sr)); goto fail; }
        if (rdma_wait_cq(cq) < 0) goto fail;
    }

    if (payload_len > 0) {
        /* Single message: relies on ep_attr->max_msg_size which is very
         * large for tcp / verbs. For a first milestone this is fine; a
         * future improvement is to chunk at max_msg_size boundaries. */
        struct fi_context ctx;
        ssize_t sr = fi_send(ep, payload.buf, payload_len,
                             fi_mr_desc(mr_buf), 0, &ctx);
        if (sr) { fprintf(stderr, "[RDMA-REPL] fi_send buf: %s\n",
                          fi_strerror((int)-sr)); goto fail; }
        if (rdma_wait_cq(cq) < 0) goto fail;
    }

    printf("[RDMA-REPL] master: full sync shipped\n");
    rc = 0;

fail:
    /* Always release full-sync-only resources */
    CLOSE_FID(mr_buf);
    CLOSE_FID(mr_hdr);
    dynbuf_free(&payload);

    if (rc == 0 && g_ebpf_ok && ebpf_incr_master_healthy()) {
        /* Transfer connection ownership to the incremental sync module.
         * The module will fi_shutdown/close these when slave disconnects. */
        uint64_t catchup_seq = incr_start_seq + 1;
        if (ebpf_incr_add_slave(ep, cq, domain, catchup_seq) == 0) {
            ep = NULL;
            cq = NULL;
            domain = NULL;
            printf("[RDMA-REPL] master: connection handed to incr module "
                   "(catch-up from seq=%" PRIu64 ")\n", catchup_seq);
        } else {
            fprintf(stderr,
                    "[RDMA-REPL] incr add_slave failed; "
                    "closing session for clean re-sync\n");
            fi_shutdown(ep, 0);
            rc = -1;
        }
    } else {
        /* Error path, or eBPF unavailable: close normally */
        if (rc == 0) fi_shutdown(ep, 0);
    }

    CLOSE_FID(ep);
    CLOSE_FID(cq);
    CLOSE_FID(domain);
    return rc;
}

/* ========================================================== */
/* 5. MASTER: listener thread                                 */
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
    snprintf(service, sizeof(service), "%d", kvs_config.rdma_bind_port);

    int rc = fi_getinfo(FI_VERSION(1, 17),
                        kvs_config.rdma_bind_ip, service,
                        FI_SOURCE, hints, &info);
    if (rc) {
        fprintf(stderr, "[RDMA-REPL] fi_getinfo (master) failed: %s\n",
                fi_strerror(-rc));
        goto cleanup;
    }
    printf("[RDMA-REPL] master: using provider '%s' on %s:%s\n",
           info->fabric_attr->prov_name,
           kvs_config.rdma_bind_ip, service);

    struct fi_eq_attr eq_attr = {0};
    eq_attr.wait_obj = FI_WAIT_UNSPEC;

    RDMA_CHK(fi_fabric(info->fabric_attr, &fabric, NULL), "fi_fabric");
    RDMA_CHK(fi_eq_open(fabric, &eq_attr, &eq, NULL),     "fi_eq_open");
    RDMA_CHK(fi_passive_ep(fabric, info, &pep, NULL),     "fi_passive_ep");
    RDMA_CHK(fi_pep_bind(pep, &eq->fid, 0),               "fi_pep_bind");
    RDMA_CHK(fi_listen(pep),                              "fi_listen");

    /* Start eBPF incremental sync module (non-fatal if unavailable) */
    if (ebpf_incr_master_start() == 0) {
        g_ebpf_ok = 1;
    } else {
        fprintf(stderr, "[RDMA-REPL] eBPF incr sync unavailable "
                        "(full sync only)\n");
    }

    printf("[RDMA-REPL] master: listening for slaves\n");

    /* Main accept loop */
    for (;;) {
        /* Buffer big enough to carry the connreq + tail data */
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
        if (event != FI_CONNREQ) {
            /* Could be a FI_SHUTDOWN from a previous session; ignore. */
            continue;
        }

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

/* ========================================================== */
/* 6. SLAVE: incremental recv thread (runs after full sync)   */
/* ========================================================== */

static void *incr_slave_recv_thread(void *arg)
{
    struct slave_incr_ctx *ctx = (struct slave_incr_ctx *)arg;
    int applied = 0;

    printf("[RDMA-REPL] slave: incremental recv thread started\n");

    for (;;) {
        /* Pre-post recv buffer before waiting */
        struct fi_context recv_ctx;
        ssize_t r = fi_recv(ctx->ep, ctx->recv_buf, INCR_RECV_BUF_SZ,
                            fi_mr_desc(ctx->recv_mr), 0, &recv_ctx);
        if (r != 0) {
            fprintf(stderr, "[RDMA-REPL] slave incr: fi_recv: %s\n",
                    fi_strerror((int)-r));
            break;
        }

        /* Wait for recv completion */
        if (rdma_wait_cq(ctx->cq) < 0) break;

        /* Parse wire frame: [uint64_t cmd_len | cmd bytes] */
        uint64_t cmd_len = 0;
        memcpy(&cmd_len, ctx->recv_buf, sizeof(uint64_t));

        if (cmd_len == 0) {
            printf("[RDMA-REPL] slave incr: received disconnect sentinel\n");
            break;
        }
        if (cmd_len > INCR_MAX_CMD) {
            fprintf(stderr, "[RDMA-REPL] slave incr: oversized cmd %" PRIu64
                    ", aborting\n", cmd_len);
            break;
        }

        /* Apply the RESP command through the normal command path */
        DynBuf wb;
        dynbuf_init(&wb, 256);
        kvs_protocol((char *)(ctx->recv_buf + sizeof(uint64_t)),
                     (int)cmd_len, &wb);
        dynbuf_free(&wb);
        applied++;

        if (applied % 100 == 0 || applied <= 10)
            printf("[RDMA-REPL] slave incr: applied cmd #%d\n", applied);
    }

    printf("[RDMA-REPL] slave incr: thread exiting, applied %d commands\n",
           applied);

    /* Cleanup: all resources owned by this thread */
    fi_shutdown(ctx->ep, 0);
    CLOSE_FID(ctx->recv_mr);
    CLOSE_FID(ctx->ep);
    CLOSE_FID(ctx->cq);
    CLOSE_FID(ctx->domain);
    free(ctx->recv_buf);
    free(ctx);
    return NULL;
}

/* ========================================================== */
/* 7. SLAVE: full-sync receiver + spawn incremental thread    */
/* ========================================================== */

int rdma_repl_slave_run(void) {
    struct fi_info    *hints  = NULL, *info = NULL;
    struct fid_fabric *fabric = NULL;
    struct fid_domain *domain = NULL;
    struct fid_eq     *eq     = NULL;
    struct fid_cq     *cq     = NULL;
    struct fid_ep     *ep     = NULL;
    struct fid_mr     *mr_hdr = NULL, *mr_buf = NULL;
    char   *payload  = NULL;
    uint64_t payload_len = 0;
    int rc_ret = -1;

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

    /* Pre-post header recv BEFORE connecting so no race is possible */
    RDMA_CHK(rdma_mr_reg(domain, &payload_len, sizeof(payload_len),
                         FI_RECV, &mr_hdr), "mr_reg hdr");
    {
        struct fi_context ctx;
        ssize_t r = fi_recv(ep, &payload_len, sizeof(payload_len),
                            fi_mr_desc(mr_hdr), 0, &ctx);
        if (r) { fprintf(stderr, "[RDMA-REPL] fi_recv hdr: %s\n",
                         fi_strerror((int)-r)); goto fail; }
    }

    /* Connect + wait CONNECTED */
    RDMA_CHK(fi_connect(ep, info->dest_addr, NULL, 0), "fi_connect");
    {
        uint32_t event;
        struct fi_eq_cm_entry entry;
        if (rdma_wait_eq(eq, &event, &entry, sizeof(entry)) < 0) goto fail;
        if (event != FI_CONNECTED) {
            fprintf(stderr, "[RDMA-REPL] slave: expected FI_CONNECTED got %u\n", event);
            goto fail;
        }
    }
    printf("[RDMA-REPL] slave: connected, awaiting full sync\n");

    /* Wait for header completion */
    if (rdma_wait_cq(cq) < 0) goto fail;
    printf("[RDMA-REPL] slave: incoming payload = %" PRIu64 " bytes\n",
           payload_len);

    if (payload_len > 0) {
        payload = (char *)kvs_malloc((size_t)payload_len);
        if (!payload) { fprintf(stderr, "[RDMA-REPL] oom\n"); goto fail; }

        RDMA_CHK(rdma_mr_reg(domain, payload, payload_len,
                             FI_RECV, &mr_buf), "mr_reg buf");

        struct fi_context ctx;
        ssize_t r = fi_recv(ep, payload, payload_len,
                            fi_mr_desc(mr_buf), 0, &ctx);
        if (r) { fprintf(stderr, "[RDMA-REPL] fi_recv buf: %s\n",
                         fi_strerror((int)-r)); goto fail; }
        if (rdma_wait_cq(cq) < 0) goto fail;

        /* Clean local state before reconstruction so we mirror the master */
        kvs_flush_all();
        int n = rdma_apply_resp_stream(payload, (size_t)payload_len);
        printf("[RDMA-REPL] slave: applied %d commands from full sync\n", n);
    } else {
        printf("[RDMA-REPL] slave: master is empty, nothing to apply\n");
        kvs_flush_all();
    }

    /* Full sync done — release the payload MRs and buffer.
     * Keep ep/cq/domain alive for the incremental recv thread. */
    CLOSE_FID(mr_buf);
    CLOSE_FID(mr_hdr);
    if (payload) { kvs_free(payload); payload = NULL; }

    /* Allocate a new recv buffer for the incremental message loop */
    struct slave_incr_ctx *ictx =
        (struct slave_incr_ctx *)calloc(1, sizeof(*ictx));
    if (!ictx) {
        fprintf(stderr, "[RDMA-REPL] slave: OOM for incr ctx\n");
        fi_shutdown(ep, 0);
        rc_ret = 0;   /* full sync succeeded; incr is best-effort */
        goto fail;
    }

    ictx->recv_buf = (uint8_t *)malloc(INCR_RECV_BUF_SZ);
    if (!ictx->recv_buf) {
        fprintf(stderr, "[RDMA-REPL] slave: OOM for incr recv_buf\n");
        free(ictx);
        fi_shutdown(ep, 0);
        rc_ret = 0;
        goto fail;
    }
    memset(ictx->recv_buf, 0, INCR_RECV_BUF_SZ);

    {
        int rc = rdma_mr_reg(domain, ictx->recv_buf, INCR_RECV_BUF_SZ,
                             FI_RECV, &ictx->recv_mr);
        if (rc) {
            fprintf(stderr, "[RDMA-REPL] slave: fi_mr_reg incr: %s\n",
                    fi_strerror(-rc));
            free(ictx->recv_buf);
            free(ictx);
            fi_shutdown(ep, 0);
            rc_ret = 0;
            goto fail;
        }
    }

    ictx->ep     = ep;
    ictx->cq     = cq;
    ictx->domain = domain;

    /* Thread owns ep/cq/domain/recv_mr/recv_buf — don't close in fail: */
    ep = NULL; cq = NULL; domain = NULL;

    {
        pthread_t tid;
        if (pthread_create(&tid, NULL, incr_slave_recv_thread, ictx)) {
            fprintf(stderr, "[RDMA-REPL] slave: pthread_create incr: %s\n",
                    strerror(errno));
            /* Undo: reclaim resources the thread would have owned */
            CLOSE_FID(ictx->recv_mr);
            free(ictx->recv_buf);
            free(ictx);
        } else {
            pthread_detach(tid);
            printf("[RDMA-REPL] slave: incremental recv thread spawned\n");
        }
    }
    rc_ret = 0;

fail:
    CLOSE_FID(mr_buf);    /* no-op if already closed above */
    CLOSE_FID(mr_hdr);
    CLOSE_FID(ep);        /* no-op if NULL (owned by incr thread) */
    CLOSE_FID(cq);
    CLOSE_FID(domain);
    CLOSE_FID(eq);
    CLOSE_FID(fabric);
    if (payload) kvs_free(payload);
    if (info)  fi_freeinfo(info);
    if (hints) fi_freeinfo(hints);
    return rc_ret;
}
