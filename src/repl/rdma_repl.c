/* ===================================================================
 * rdma_repl.c — FI_EP_MSG batch fullsync (master + slave)
 *
 * Master: fi_passive_ep → accept → iterate KV → batch send → EOF
 * Slave:  fi_connect → HANDSHAKE → batch recv → parse → load KV
 * =================================================================== */

#include "rdma_repl.h"
#include "kvstore.h"
#include "config.h"
#include "server.h"
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

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>

#include <netinet/in.h>
#include <arpa/inet.h>

/* ── libfabric helpers ─────────────────────────────────────── */

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

static int rdma_wait_eq(struct fid_eq *eq, uint32_t *event,
                        void *buf, size_t buf_size) {
    extern volatile sig_atomic_t g_kvs_stop;
    for (;;) {
        if (g_kvs_stop) {
            fprintf(stderr, "[RDMA-REPL] EQ wait interrupted by signal\n");
            return -1;
        }
        ssize_t r = fi_eq_sread(eq, event, buf, buf_size, 1000, 0);
        if (r >= 0) return 0;
        if (r == -FI_EAGAIN) continue;
        fprintf(stderr, "[RDMA-REPL] EQ error: %s\n",
                fi_eq_strerror(eq, (int)-r, *event, NULL, 0));
        return -1;
    }
}

/* ── Master: listener thread ───────────────────────────────── */

static void *rdma_fullsync_master_thread(void *arg) {
    (void)arg;
    printf("[RDMA-REPL] Master fullsync thread (FI_EP_MSG)\n");

    struct fi_info *hints = rdma_build_hints();
    if (!hints) return NULL;
    struct fi_info *info = NULL;

    char service[16];
    snprintf(service, sizeof(service), "%d", kvs_config.rdma_listen_port);

    int rc = fi_getinfo(FI_VERSION(1, 17), kvs_config.rdma_listen_ip, service,
                        FI_SOURCE, hints, &info);
    fi_freeinfo(hints);
    if (rc) {
        fprintf(stderr, "[RDMA-REPL] fi_getinfo: %s\n", fi_strerror(-rc));
        return NULL;
    }
    printf("[RDMA-REPL] Master: %s on %s:%s\n",
           info->fabric_attr->prov_name, kvs_config.rdma_listen_ip, service);

    struct fid_fabric *fabric = NULL;
    struct fid_eq     *eq     = NULL;
    struct fid_pep    *pep    = NULL;
    if (fi_fabric(info->fabric_attr, &fabric, NULL) ||
        fi_eq_open(fabric, &(struct fi_eq_attr){.wait_obj = FI_WAIT_UNSPEC}, &eq, NULL) ||
        fi_passive_ep(fabric, info, &pep, NULL) ||
        fi_pep_bind(pep, &eq->fid, 0) ||
        fi_listen(pep)) {
        fprintf(stderr, "[RDMA-REPL] Master listen failed\n");
        goto out_fab;
    }
    printf("[RDMA-REPL] Master: listening\n");

    /* Accept one connection, run fullsync, then exit */
    struct fi_eq_cm_entry entry;
    uint32_t event;
    if (rdma_wait_eq(eq, &event, &entry, sizeof(entry)) < 0) goto out_pep;
    if (event != FI_CONNREQ) goto out_pep;

    struct fid_domain *domain = NULL;
    struct fid_ep     *ep     = NULL;
    struct fid_cq     *cq     = NULL;
    if (fi_domain(fabric, entry.info, &domain, NULL) ||
        fi_cq_open(domain, &(struct fi_cq_attr){
            .format = FI_CQ_FORMAT_DATA, .wait_obj = FI_WAIT_UNSPEC
        }, &cq, NULL) ||
        fi_endpoint(domain, entry.info, &ep, NULL) ||
        fi_ep_bind(ep, &eq->fid, 0) ||
        fi_ep_bind(ep, &cq->fid, FI_SEND | FI_RECV) ||
        fi_accept(ep, NULL, 0) ||
        fi_enable(ep)) {
        fprintf(stderr, "[RDMA-REPL] Master accept failed\n");
        goto out_dom;
    }

    if (rdma_wait_eq(eq, &event, &entry, sizeof(entry)) < 0) goto out_ep;
    if (event != FI_CONNECTED) goto out_ep;
    printf("[RDMA-REPL] Master: slave connected\n");

    master_fullsync_planB(domain, ep, cq);

out_ep:
    fi_close(&ep->fid);
    fi_close(&cq->fid);
out_dom:
    fi_close(&domain->fid);
out_pep:
    fi_close(&pep->fid);
    fi_close(&eq->fid);
out_fab:
    fi_close(&fabric->fid);
    fi_freeinfo(info);
    return NULL;
}

int rdma_repl_master_start(void) {
    printf("[RDMA-REPL] Starting master fullsync thread\n");
    pthread_t tid;
    int rc = pthread_create(&tid, NULL, rdma_fullsync_master_thread, NULL);
    if (rc) { fprintf(stderr, "[RDMA-REPL] pthread_create: %s\n", strerror(rc)); return -1; }
    pthread_detach(tid);
    return 0;
}

/* ── Slave: incremental handoff ────────────────────────────── */

static uint64_t g_slave_base_seq = 0;
static volatile int g_slave_need_fullresync = 0;

int rdma_repl_slave_need_fullresync(void) {
    return g_slave_need_fullresync;
}

static void xdp_slave_incremental_ready(uint64_t session_id, uint64_t epoch,
                                        uint64_t base_seq) {
    g_slave_base_seq = base_seq;
    repl_stream_start_slave(base_seq);
    printf("[RDMA-REPL] Slave: XDP incremental ready (session=%" PRIu64
           ", epoch=%" PRIu64 ", base_seq=%" PRIu64 ")\n",
           session_id, epoch, base_seq);
}

/* ── Slave: fullsync receiver ──────────────────────────────── */

int rdma_repl_slave_run(void) {
    printf("[RDMA-REPL] Slave fullsync (FI_EP_MSG)\n");
    struct fi_info *info = NULL, *hints = rdma_build_hints();
    struct fid_fabric *fab = NULL;
    struct fid_domain *dom = NULL;
    struct fid_eq *eq = NULL;
    struct fid_cq *cq = NULL;
    struct fid_ep *ep = NULL;
    if (!hints) return -1;
    char svc[16];
    snprintf(svc, sizeof(svc), "%d", kvs_config.rdma_master_port);
    int rc = fi_getinfo(FI_VERSION(1, 17), kvs_config.rdma_master_ip, svc,
                        0, hints, &info);
    fi_freeinfo(hints);
    if (rc) { fprintf(stderr, "[RDMA-REPL] fi_getinfo: %s\n", fi_strerror(-rc)); return -1; }
    printf("[RDMA-REPL] Slave: %s → %s:%s\n",
           info->fabric_attr->prov_name, kvs_config.rdma_master_ip, svc);

    if (fi_fabric(info->fabric_attr, &fab, NULL) ||
        fi_eq_open(fab, &(struct fi_eq_attr){.wait_obj = FI_WAIT_UNSPEC}, &eq, NULL) ||
        fi_domain(fab, info, &dom, NULL) ||
        fi_cq_open(dom, &(struct fi_cq_attr){.format = FI_CQ_FORMAT_DATA, .wait_obj = FI_WAIT_UNSPEC}, &cq, NULL) ||
        fi_endpoint(dom, info, &ep, NULL) ||
        fi_ep_bind(ep, &eq->fid, 0) ||
        fi_ep_bind(ep, &cq->fid, FI_SEND | FI_RECV) ||
        fi_enable(ep) ||
        fi_connect(ep, info->dest_addr, NULL, 0)) {
        fprintf(stderr, "[RDMA-REPL] Slave setup failed\n");
        rc = -1; goto fail;
    }

    /* Wait FI_CONNECTED */
    struct fi_eq_cm_entry entry; uint32_t ev;
    if (rdma_wait_eq(eq, &ev, &entry, sizeof(entry)) < 0 || ev != FI_CONNECTED)
        { rc = -1; goto fail; }
    printf("[RDMA-REPL] Slave: connected\n");

    rc = slave_receive_fullsync_batched(dom, ep, cq);

fail:
    if (ep)  fi_close(&ep->fid);
    if (cq)  fi_close(&cq->fid);
    if (dom) fi_close(&dom->fid);
    if (eq)  fi_close(&eq->fid);
    if (fab) fi_close(&fab->fid);
    fi_freeinfo(info);
    return rc;
}

/* ── Rewrite capture (no-op, called during fullsync) ────────── */

int rdma_repl_capture_write(const kvs_journal_record_t *rec) {
    (void)rec;
    return 0;
}
