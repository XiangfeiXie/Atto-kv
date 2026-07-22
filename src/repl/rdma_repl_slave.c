/* rdma_repl_slave.c — Slave fullsync receiver (simplified single connection)
 *
 * New Architecture:
 *   - Direct connect to child RDMA port (2101)
 *   - Pure data streaming: no control messages
 *   - Connection shutdown = EOF
 *   - Parse header + entries
 */

#include "rdma_repl.h"
#include "kvstore.h"
#include "kv_dict.h"
#include "repl_incr.h"
#include "persistence.h"
#include "config.h"

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_errno.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <arpa/inet.h>

/* Number of receive buffers for streaming */
#define NUM_RECV_BUFS  2

/* Size of each receive buffer — MUST be >= master's STREAM_BATCH_SIZE (512MB).
 * verbs RC (Reliable Connection) does not fragment messages: the entire send
 * must fit into a single posted recv buffer.  Undersized → buffer overflow
 * → memory corruption → SIGSEGV.  2 × 512MB = 1GB MR total (half the original
 * 2GB that triggered OOM). */
#define RECV_BUF_SIZE  (512 * 1024 * 1024)  /* 512MB, matches STREAM_BATCH_SIZE */

/* ── Helper: Wait for CQ completion ────────────────────────────────── */

static int wait_cq_with_shutdown_detect(struct fid_cq *cq, struct fi_cq_data_entry *entry, int *shutdown) {
    *shutdown = 0;

    for (;;) {
        ssize_t r = fi_cq_read(cq, entry, 1);
        if (r > 0) return (int)entry->len;
        if (r == -FI_EAGAIN) {
            usleep(100);
            continue;
        }
        /* r < 0: error completion */
        struct fi_cq_err_entry err;
        fi_cq_readerr(cq, &err, 0);

        /* FI_ECANCELED: recv canceled by remote shutdown */
        /* FI_ESHUTDOWN / FI_ENOTCONN: connection closed */
        if (err.err == FI_ECANCELED || err.err == FI_ESHUTDOWN || err.err == FI_ENOTCONN) {
            *shutdown = 1;
            return 0;
        }

        fprintf(stderr, "[SLAVE] CQ error: %s (err=%d, prov_errno=%d)\n",
                fi_strerror(err.err), err.err, err.prov_errno);
        return -1;
    }
}

/* ── Inline entry parsing ─────────────────────────────────────────────
 * Entries are parsed directly in the receive loop below.
 * Format: [klen:4][key][dlen:4][data][expire:8] */

/* ── Main entry point ──────────────────────────────────────────────── */

int rdma_repl_slave_fullsync(const char *master_ip, uint16_t rdma_port) {
    printf("[SLAVE] Starting fullsync to %s:%u\n", master_ip, rdma_port);

    struct timespec t_e2e_start;
    clock_gettime(CLOCK_MONOTONIC, &t_e2e_start);

    /* Step 1: Create libfabric resources */
    struct fi_info *hints = fi_allocinfo();
    if (!hints) return -1;
    hints->caps = FI_MSG;
    hints->ep_attr->type = FI_EP_MSG;
    hints->mode = 0;
    hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED;
    hints->fabric_attr->prov_name = strdup(kvs_config.rdma_provider);

    struct fi_info *info = NULL;
    char service[16];
    snprintf(service, sizeof(service), "%d", rdma_port);

    int rc = fi_getinfo(FI_VERSION(1, 17), master_ip, service, 0, hints, &info);
    fi_freeinfo(hints);
    if (rc) {
        fprintf(stderr, "[SLAVE] fi_getinfo: %s\n", fi_strerror(-rc));
        return -1;
    }
    printf("[SLAVE] fi_getinfo: node=%s svc=%s → domain=%s fabric=%s\n",
           master_ip, service, info->domain_attr->name,
           info->fabric_attr->name);

    struct fid_fabric *fabric = NULL;
    struct fid_eq *eq = NULL;
    struct fid_domain *domain = NULL;
    struct fid_cq *cq = NULL;
    struct fid_ep *ep = NULL;

    if (fi_fabric(info->fabric_attr, &fabric, NULL) ||
        fi_eq_open(fabric, &(struct fi_eq_attr){.wait_obj = FI_WAIT_UNSPEC}, &eq, NULL) ||
        fi_domain(fabric, info, &domain, NULL) ||
        fi_cq_open(domain, &(struct fi_cq_attr){
            .format = FI_CQ_FORMAT_DATA, .wait_obj = FI_WAIT_UNSPEC
        }, &cq, NULL) ||
        fi_endpoint(domain, info, &ep, NULL) ||
        fi_ep_bind(ep, &eq->fid, 0) ||
        fi_ep_bind(ep, &cq->fid, FI_SEND | FI_RECV) ||
        fi_enable(ep)) {
        fprintf(stderr, "[SLAVE] Setup failed\n");
        rc = -1;
        goto cleanup;
    }

    /* ── Allocate + register + pre-post recv buffers BEFORE fi_connect.
     * verbs has no kernel socket buffer: master starts sending immediately
     * after accept, so recvs must be posted before the connection is up. ── */
    char *recv_bufs[NUM_RECV_BUFS];
    struct fid_mr *recv_mr[NUM_RECV_BUFS] = {NULL};
    void          *recv_desc[NUM_RECV_BUFS] = {NULL};

    for (int i = 0; i < NUM_RECV_BUFS; i++) {
        recv_bufs[i] = (char *)malloc(RECV_BUF_SIZE);
        if (!recv_bufs[i]) {
            fprintf(stderr, "[SLAVE] malloc failed\n");
            rc = -1;
            goto cleanup_meta;
        }
        int r = fi_mr_reg(domain, recv_bufs[i], RECV_BUF_SIZE,
                          FI_RECV, 0, 0, 0, &recv_mr[i], NULL);
        if (r == 0 && recv_mr[i])
            recv_desc[i] = fi_mr_desc(recv_mr[i]);

        if (fi_recv(ep, recv_bufs[i], RECV_BUF_SIZE,
                    recv_desc[i], 0, recv_bufs[i]) < 0) {
            fprintf(stderr, "[SLAVE] fi_recv failed (buf %d)\n", i);
            rc = -1;
            goto cleanup_meta;
        }
    }
    printf("[SLAVE] Pre-posted %d x %dMB recv buffers\n",
           NUM_RECV_BUFS, (int)(RECV_BUF_SIZE / (1024 * 1024)));

    /* Now connect — recvs are already posted, ready for incoming data */
    if (fi_connect(ep, info->dest_addr, NULL, 0)) {
        fprintf(stderr, "[SLAVE] fi_connect failed\n");
        rc = -1;
        goto cleanup_meta;
    }

    /* Wait for connection */
    struct fi_eq_cm_entry entry;
    uint32_t event;
    ssize_t r = fi_eq_sread(eq, &event, &entry, sizeof(entry), 60000, 0);
    if (r < 0 || event != FI_CONNECTED) {
        fprintf(stderr, "[SLAVE] fi_eq_sread(FI_CONNECTED): r=%zd event=%u errno=%d\n",
                r, event, (int)(r < 0 ? (int)(-r) : 0));
        rc = -1;
        goto cleanup_meta;
    }

    printf("[SLAVE] Connected to master child process\n");

    /* Step 3: Receive loop with two-phase processing.
     * Phase 1: parse wire buffer → compact metadata array (sequential scan).
     * Phase 2: insert from metadata → hash table (random-access dict).
     * Separation eliminates cache ping-pong between wire buffer and dict. */

    /* ── Per-entry metadata (40 bytes, cache-line friendly) ────────── */
    struct entry_meta {
        uint64_t hash;
        uint64_t expire;
        const char *key;
        const char *val;
        uint32_t klen;
        uint32_t vlen;
    };

    struct entry_meta *meta = NULL;   /* allocated after header parsed */
    int meta_cap     = 0;    /* capacity (== expected_entries) */
    int meta_count   = 0;    /* total entries parsed so far */
    int arena_ready  = 0;

    int batches = 0;
    int total_entries = 0;
    uint64_t total_bytes = 0;
    uint32_t expected_entries = 0;
    uint64_t base_seq = 0;
    int header_parsed = 0;
    struct timespec t_start, t_end;

    while (1) {
        /* Wait for data completion */
        int shutdown = 0;
        struct fi_cq_data_entry ce;
        int dlen = wait_cq_with_shutdown_detect(cq, &ce, &shutdown);

        if (shutdown) {
            printf("[SLAVE] Connection closed, fullsync done\n");
            break;
        }

        if (dlen < 0) {
            fprintf(stderr, "[SLAVE] CQ error\n");
            rc = -1;
            goto cleanup_meta;
        }

        if (dlen == 0) {
            continue;  /* Spurious wakeup */
        }

        /* Get buffer from completion entry */
        char *recv_buf = (char *)ce.op_context;
        size_t received = ce.len;
        const char *p   = recv_buf;
        const char *end = recv_buf + received;
        int batch_start = meta_count;  /* first slot for this batch */

        /* ── Header (first batch only) ─────────────────────────────── */
        if (!header_parsed) {
            if (received < 24) {
                fprintf(stderr, "[SLAVE] Header too short\n");
                rc = -1;
                goto cleanup_meta;
            }

            uint32_t magic   = ntohl(*(uint32_t *)(p + 0));
            uint32_t version = ntohl(*(uint32_t *)(p + 4));
            expected_entries = ntohl(*(uint32_t *)(p + 12));
            base_seq = be64toh(*(uint64_t *)(p + 16));

            if (magic != 0x52444232) {
                fprintf(stderr, "[SLAVE] Invalid magic: 0x%x\n", magic);
                rc = -1;
                goto cleanup_meta;
            }

            printf("[SLAVE] Header: version=%u, key_count=%u, base_seq=%lu\n",
                   version, expected_entries, (unsigned long)base_seq);

            /* Clear existing data and pre-size hashtable */
            kvs_flush_all();
            kv_store_reserve(expected_entries);

            /* Allocate metadata array for the entire fullsync */
            meta_cap = (int)expected_entries;
            meta = (struct entry_meta *)malloc(
                (size_t)meta_cap * sizeof(*meta));
            if (!meta) {
                fprintf(stderr, "[SLAVE] meta malloc(%u) failed\n",
                        expected_entries);
                rc = -1;
                goto cleanup_meta;
            }

            p += 24;
            header_parsed = 1;
            clock_gettime(CLOCK_MONOTONIC, &t_start);
        }

        /* ── Phase 1: Parse entries into metadata array ──────────────
         * Pure sequential scan — no hash-table access, cache-friendly. */
        while (p + 24 <= end && meta_count < meta_cap) {
            struct entry_meta *m = &meta[meta_count];
            m->hash   = be64toh(*(const uint64_t *)p); p += 8;
            m->klen   = ntohl(*(const uint32_t *)p);   p += 4;
            if (p + m->klen > end) break;
            m->key    = p;                              p += m->klen;
            if (p + 4 > end) break;
            m->vlen   = ntohl(*(const uint32_t *)p);   p += 4;
            if (p + m->vlen + 8 > end) break;
            m->val    = p;                              p += m->vlen;
            m->expire = be64toh(*(const uint64_t *)p);  p += 8;
            meta_count++;
        }

        int n = meta_count - batch_start;
        total_bytes += received;

        /* ── Arena: allocate after first batch (exact heap sizing) ─── */
        if (!arena_ready && n > 0 && meta_count > 0) {
            kv_dict_t *dict = kv_dict_global();
            if (!dict->arena) {
                /* Sum exact heap bytes from the parsed metadata */
                size_t heap_sample = 0;
                for (int i = 0; i < meta_count; i++)
                    heap_sample += sizeof(kv_entry_t)
                                 + meta[i].klen + meta[i].vlen;
                size_t est_total = (heap_sample * expected_entries)
                                 / (size_t)meta_count;
                size_t arena_sz  = est_total + est_total / 10; /* +10% */
                if (arena_sz > 64 * 1024 * 1024) {
                    dict_arena_init(dict, arena_sz);
                    if (dict->arena)
                        printf("[SLAVE] Arena: %.1f MB (exact est., %u samples)\n",
                               (double)arena_sz / (1024 * 1024),
                               meta_count);
                }
                arena_ready = 1;
            }
        }

        /* ── Phase 2: Insert from metadata into hash table ───────────
         * All keys/values still live in recv_buf (valid until re-post). */
        for (int i = batch_start; i < meta_count; i++) {
            struct entry_meta *m = &meta[i];
            kv_entry_t *e = dict_add(kv_dict_for(m->hash),
                                      m->key, m->klen,
                                      m->val, m->vlen, m->hash);
            if (e) e->expire = (long long)m->expire;
        }

        total_entries = meta_count;
        batches++;

        /* Check if all entries received */
        if (expected_entries > 0 && (uint32_t)total_entries >= expected_entries) {
            printf("[SLAVE] All entries received, fullsync done\n");
            break;
        }

        /* Re-post receive buffer with matching MR desc */
        {
            void *desc = NULL;
            for (int i = 0; i < NUM_RECV_BUFS; i++)
                if (recv_bufs[i] == recv_buf) { desc = recv_desc[i]; break; }
            if (fi_recv(ep, recv_buf, RECV_BUF_SIZE, desc, 0, recv_buf) < 0) {
                fprintf(stderr, "[SLAVE] fi_recv re-post failed\n");
                rc = -1;
                goto cleanup_meta;
            }
        }
    }

    printf("[SLAVE] Total: %d batches, %d entries\n", batches, total_entries);

    /* Step 4: Rebuild encodings */
    extern void rdb_rebuild_all_encodings(void);
    rdb_rebuild_all_encodings();

    clock_gettime(CLOCK_MONOTONIC, &t_end);

    /* Step 5: Save base_seq for incremental sync */
    repl_incr_save_offset(base_seq);

    /* Print receive-side throughput (pure data path) */
    {
        double ms = (t_end.tv_sec - t_start.tv_sec) * 1000.0
                  + (t_end.tv_nsec - t_start.tv_nsec) / 1e6;
        double mbps = (ms > 0) ? (total_bytes / (ms / 1000.0) / (1024.0 * 1024.0)) : 0;
        double eps  = (ms > 0) ? (total_entries / (ms / 1000.0)) : 0;
        printf("[SLAVE] Recv throughput: %lu bytes, %d entries, %.1f ms, %.2f MB/s, %.0f entries/s\n",
               (unsigned long)total_bytes, total_entries, ms, mbps, eps);
    }

    /* Print end-to-end throughput (connect + transfer + parse + rebuild) */
    {
        struct timespec t_e2e_end;
        clock_gettime(CLOCK_MONOTONIC, &t_e2e_end);
        double ms = (t_e2e_end.tv_sec - t_e2e_start.tv_sec) * 1000.0
                  + (t_e2e_end.tv_nsec - t_e2e_start.tv_nsec) / 1e6;
        double mbps = (ms > 0) ? (total_bytes / (ms / 1000.0) / (1024.0 * 1024.0)) : 0;
        printf("[SLAVE] End-to-end: %lu bytes, %d entries, %.1f ms, %.2f MB/s (incl connect+rebuild)\n",
               (unsigned long)total_bytes, total_entries, ms, mbps);
    }

    rc = 0;

cleanup_meta:
    free(meta);
    for (int i = 0; i < NUM_RECV_BUFS; i++) {
        if (recv_mr[i]) fi_close(&recv_mr[i]->fid);
        free(recv_bufs[i]);
    }

cleanup:
    if (ep) fi_close(&ep->fid);
    if (cq) fi_close(&cq->fid);
    if (domain) fi_close(&domain->fid);
    if (eq) fi_close(&eq->fid);
    if (fabric) fi_close(&fabric->fid);
    fi_freeinfo(info);

    return rc;
}
