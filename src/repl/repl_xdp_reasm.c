/* repl_xdp_reasm.c — XDP packet-level mirror user-space reassembly
 *
 *   - Connection tracking by (saddr, sport)
 *   - SYN → ISN extraction
 *   - seq-based dedup + in-order linear buffer append
 *   - Out-of-order segment linked-list buffering
 *   - Ordered data → RESP command extraction (existing functions)
 *   - Write-command forwarding via callback
 *   - XDP BPF loader (libbpf, no libxdp required)
 *
 * Data pipeline:
 *   XDP ringbuf → xdp_reasm_on_event() → per-conn seq dedup/order
 *       → linear buffer → find_complete_resp_command()
 *       → is_write_command() → on_write_cb()
 */

#define _GNU_SOURCE
#include "repl_xdp_reasm.h"
#include "repl_xdp_common.h"
#include "repl_tcp_internal.h"   /* for is_write_command, find_complete_resp_command */

#include <arpa/inet.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <linux/if_link.h>   /* XDP_FLAGS_SKB_MODE, XDP_FLAGS_DRV_MODE */
#include <net/if.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* ── Constants ──────────────────────────────────────────────────────── */

#define XDP_CONN_HASH_SIZE   64
#define XDP_CONN_MAX         64
#define XDP_CONN_GC_TIMEOUT  30       /* seconds idle before GC */

/* ── Types ──────────────────────────────────────────────────────────── */

typedef struct xdp_ooo_seg {
    uint32_t            seq;
    uint32_t            len;
    uint8_t            *data;         /* malloc'd copy */
    struct xdp_ooo_seg *next;
} xdp_ooo_seg_t;

typedef struct xdp_conn {
    uint32_t        saddr;            /* network byte order */
    uint16_t        sport;            /* network byte order */
    uint32_t        rcv_nxt;          /* next expected seq (host order) */
    int             isn_known;
    uint32_t        isn;              /* ISN (host order) */

    xdp_ooo_seg_t  *ooo_head;        /* sorted by seq, non-overlapping */

    /* Linear buffer for RESP extraction (same pattern as sock_stream) */
    uint8_t        *buf;
    uint32_t        buf_len;
    uint32_t        buf_cap;

    int             closing;          /* FIN/RST seen */
    time_t          last_active;

    struct xdp_conn *hash_next;       /* hash chain */
} xdp_conn_t;

/* ── Global state ───────────────────────────────────────────────────── */

static struct {
    xdp_conn_t       *hash[XDP_CONN_HASH_SIZE];
    int               count;

    xdp_reasm_write_cb on_write;

    /* XDP attach state */
    struct bpf_object *bpf_obj;
    struct ring_buffer *ringbuf;
    struct bpf_link   *xdp_link;

    pthread_mutex_t    lock;

    struct xdp_reasm_stats stats;
} g_xdp;

/* ── Helpers ────────────────────────────────────────────────────────── */

static inline uint32_t conn_hash(uint32_t saddr, uint16_t sport) {
    /* Multiplicative hash: sports are ephemeral → good distribution */
    uint32_t h = saddr ^ ((uint32_t)sport * 0x9e3779b9u);
    return h & (XDP_CONN_HASH_SIZE - 1);
}

static uint32_t seq_to_host(uint32_t net_seq) {
    return ntohl(net_seq);
}

static xdp_conn_t *conn_lookup(uint32_t saddr, uint16_t sport) {
    uint32_t h = conn_hash(saddr, sport);
    for (xdp_conn_t *c = g_xdp.hash[h]; c; c = c->hash_next)
        if (c->saddr == saddr && c->sport == sport)
            return c;
    return NULL;
}

static xdp_conn_t *conn_create(uint32_t saddr, uint16_t sport) {
    if (g_xdp.count >= XDP_CONN_MAX)
        return NULL;

    uint32_t h = conn_hash(saddr, sport);
    xdp_conn_t *c = calloc(1, sizeof(*c));
    if (!c) return NULL;

    c->saddr       = saddr;
    c->sport       = sport;
    c->last_active = time(NULL);
    c->hash_next   = g_xdp.hash[h];
    g_xdp.hash[h]  = c;
    g_xdp.count++;
    return c;
}

static int conn_ensure_buf(xdp_conn_t *c, uint32_t needed) {
    if (needed <= c->buf_cap) return 0;

    uint32_t new_cap = c->buf_cap ? c->buf_cap : 4096;
    while (new_cap < needed) {
        if (new_cap > (4u * 1024 * 1024)) return -1;  /* 4MB sanity cap */
        new_cap *= 2;
    }
    uint8_t *p = realloc(c->buf, new_cap);
    if (!p) return -1;
    c->buf = p;
    c->buf_cap = new_cap;
    return 0;
}

/* ── OOO segment management ─────────────────────────────────────────── */

/* Insert a segment into the OOO list, merging overlapping/adjacent segments.
 * OOO list is sorted by seq, and segments are guaranteed non-overlapping. */
static void ooo_insert(xdp_conn_t *c, uint32_t seq,
                       const uint8_t *data, uint32_t len)
{
    xdp_ooo_seg_t *seg = malloc(sizeof(*seg));
    if (!seg) return;
    seg->seq = seq;
    seg->len = len;
    seg->data = malloc(len);
    if (!seg->data) { free(seg); return; }
    memcpy(seg->data, data, len);
    seg->next = NULL;

    g_xdp.stats.ooo_segments++;

    /* Insert into sorted (by seq) linked list with merge */
    xdp_ooo_seg_t *prev = NULL, *cur = c->ooo_head;

    /* Find insertion point */
    while (cur && cur->seq < seg->seq) {
        prev = cur;
        cur = cur->next;
    }

    /* Merge with predecessor if overlapping or adjacent */
    if (prev) {
        uint32_t prev_end = prev->seq + prev->len;
        if (prev_end >= seg->seq) {
            /* Overlap/adjacent: extend prev if seg goes beyond */
            if (seg->seq + seg->len > prev_end) {
                uint32_t new_len = seg->seq + seg->len - prev->seq;
                uint8_t *new_data = realloc(prev->data, new_len);
                if (new_data) {
                    prev->data = new_data;
                    memcpy(prev->data + prev->len, seg->data + (prev_end - seg->seq),
                           seg->seq + seg->len - prev_end);
                    prev->len = new_len;
                }
            }
            free(seg->data);
            free(seg);
            seg = NULL;
        }
    }

    if (!seg) return; /* consumed by merge */

    /* Insert seg between prev and cur, then merge forward */
    seg->next = cur;
    if (prev)
        prev->next = seg;
    else
        c->ooo_head = seg;
    prev = seg;

    /* Merge with successors */
    while (cur) {
        uint32_t seg_end = prev->seq + prev->len;
        if (seg_end >= cur->seq) {
            /* Merge cur into prev */
            if (cur->seq + cur->len > seg_end) {
                uint32_t new_len = cur->seq + cur->len - prev->seq;
                uint8_t *new_data = realloc(prev->data, new_len);
                if (new_data) {
                    prev->data = new_data;
                    memcpy(prev->data + prev->len, cur->data + (seg_end - cur->seq),
                           cur->seq + cur->len - seg_end);
                    prev->len = new_len;
                }
            }
            prev->next = cur->next;
            free(cur->data);
            free(cur);
            g_xdp.stats.ooo_segments--;
            cur = prev->next;
        } else {
            break;
        }
    }
}

static void consume_ordered(xdp_conn_t *c, const uint8_t *data, uint32_t len);

/* Drain OOO segments that are now contiguous with rcv_nxt.
 * Uses consume_ordered() so drained data triggers RESP extraction. */
static void ooo_drain(xdp_conn_t *c) {
    while (c->ooo_head) {
        xdp_ooo_seg_t *seg = c->ooo_head;

        if (seg->seq > c->rcv_nxt)
            break;  /* gap — wait for the missing segment */

        uint32_t seg_end = seg->seq + seg->len;
        if (seg_end > c->rcv_nxt) {
            uint32_t skip    = c->rcv_nxt - seg->seq;
            uint32_t new_len = seg_end - c->rcv_nxt;
            consume_ordered(c, seg->data + skip, new_len);
            c->rcv_nxt = seg_end;
        }

        /* Remove the segment */
        c->ooo_head = seg->next;
        free(seg->data);
        free(seg);
        g_xdp.stats.ooo_segments--;
    }
}

/* ── Ordered data consumer ──────────────────────────────────────────── */

/* Append ordered bytes to linear buffer, then extract and forward
 * complete RESP commands.  This is the same pattern as the current
 * master_on_data() in repl_tcp_master.c. */
static void consume_ordered(xdp_conn_t *c, const uint8_t *data, uint32_t len) {
    if (len == 0) return;

    g_xdp.stats.bytes_ordered += len;

    if (conn_ensure_buf(c, c->buf_len + len) != 0) {
        /* Buffer overflow — reset (likely garbage) */
        c->buf_len = 0;
        return;
    }

    memcpy(c->buf + c->buf_len, data, len);
    c->buf_len += len;

    /* Extract complete RESP commands */
    while (1) {
        int cmd_len = find_complete_resp_command(c->buf, c->buf_len);
        if (cmd_len <= 0) {
            /* 0 = incomplete, -1 = parse error */
            if (cmd_len < 0 && c->buf_len > (4u * 1024 * 1024)) {
                /* Stale data, reset */
                c->buf_len = 0;
            }
            break;
        }

        g_xdp.stats.cmds_extracted++;

        if (is_write_command(c->buf, (size_t)cmd_len)) {
            g_xdp.stats.cmds_written++;
            if (g_xdp.on_write)
                g_xdp.on_write(c->buf, (uint32_t)cmd_len);
        }

        /* Consume from buffer */
        c->buf_len -= (uint32_t)cmd_len;
        if (c->buf_len > 0)
            memmove(c->buf, c->buf + cmd_len, c->buf_len);
    }

    /* Free large idle buffers */
    if (c->buf_len == 0 && c->buf_cap > 65536) {
        free(c->buf);
        c->buf = NULL;
        c->buf_cap = 0;
    }
}

/* ── Packet processing (core seq logic) ─────────────────────────────── */

static void process_packet(xdp_conn_t *c,
                           uint32_t seq_h, const uint8_t *data, uint32_t len) {
    if (len == 0) return;

    uint32_t seg_end = seq_h + len;

    /* ── 1. Completely duplicate (fully before rcv_nxt) ─────────────── */
    if (seg_end <= c->rcv_nxt) {
        g_xdp.stats.dup_discarded++;
        return;
    }

    /* ── 2. Partial overlap (starts before rcv_nxt, ends after) ────── */
    if (seq_h < c->rcv_nxt) {
        uint32_t skip = c->rcv_nxt - seq_h;
        data += skip;
        len  -= skip;
        seq_h = c->rcv_nxt;
    }

    /* ── 3. In-order (seq == rcv_nxt) ───────────────────────────────── */
    if (seq_h == c->rcv_nxt) {
        consume_ordered(c, data, len);
        c->rcv_nxt += len;
        ooo_drain(c);
        return;
    }

    /* ── 4. Out-of-order (seq > rcv_nxt) ────────────────────────────── */
    ooo_insert(c, seq_h, data, len);
}

/* ── Ringbuf callback ────────────────────────────────────────────────── */

int xdp_reasm_on_event(void *ctx, void *data, size_t data_sz) {
    (void)ctx;
    (void)data_sz;

    struct xdp_repl_event *e = (struct xdp_repl_event *)data;

    pthread_mutex_lock(&g_xdp.lock);
    g_xdp.stats.events_total++;

    /* ── Lookup or create connection ──────────────────────────────── */
    xdp_conn_t *c = conn_lookup(e->saddr, e->sport);

    /* Handle SYN */
    if (e->flags & XDP_FLAG_SYN) {
        g_xdp.stats.events_syn++;
        if (!c) {
            c = conn_create(e->saddr, e->sport);
            if (!c) {
                pthread_mutex_unlock(&g_xdp.lock);
                return 0;  /* connection table full */
            }
        }
        /* SYN sets ISN. The SYN's own seq is the ISN.
         * rcv_nxt = ISN + 1 (next byte after SYN).
         * If we already had this connection (somehow), don't re-init. */
        if (!c->isn_known) {
            c->isn  = seq_to_host(e->seq);
            c->rcv_nxt = c->isn + 1;
            c->isn_known = 1;
            c->closing = 0;
        }
        c->last_active = time(NULL);
        /* SYN may or may not have payload (TCP Fast Open) —
         * process_packet handles len==0 correctly. */
        if (e->data_len > 0) {
            process_packet(c, seq_to_host(e->seq) + 1, e->data, e->data_len);
        }
        pthread_mutex_unlock(&g_xdp.lock);
        return 0;
    }

    /* Connection must exist for non-SYN packets */
    if (!c) {
        /* Mid-stream packet for unknown connection — could be:
         *   1. We missed the SYN (started observing after connection was established)
         *   2. XDP ringbuf overflow dropped the SYN event
         * Create a "lame" connection: guess ISN=seq-1 (first data byte after SYN). */
        c = conn_create(e->saddr, e->sport);
        if (!c) {
            pthread_mutex_unlock(&g_xdp.lock);
            return 0;
        }
        /* Assume the first byte of data we see is at ISN+1.
         * This is correct if the first captured packet is the first data packet. */
        c->isn  = seq_to_host(e->seq) - 1;
        c->rcv_nxt = c->isn + 1;
        c->isn_known = 0;  /* mark as estimated, not authoritative */
    }

    /* Handle FIN/RST */
    if (e->flags & (XDP_FLAG_FIN | XDP_FLAG_RST)) {
        g_xdp.stats.events_fin_rst++;
        c->closing = 1;
        /* Data comes before FIN in sequence space.
         * FIN byte = 1 sequence number after the last data byte. */
        uint32_t data_seq = seq_to_host(e->seq);
        if (e->data_len > 0) {
            process_packet(c, data_seq, e->data, e->data_len);
        }
        if (e->flags & XDP_FLAG_FIN) {
            uint8_t fin_byte[1] = {0};
            process_packet(c, data_seq + e->data_len, fin_byte, 1);
        }
        c->last_active = time(NULL);
        pthread_mutex_unlock(&g_xdp.lock);
        return 0;
    }

    /* Handle data packet */
    if (e->data_len > 0) {
        g_xdp.stats.events_data++;
        c->last_active = time(NULL);
        process_packet(c, seq_to_host(e->seq), e->data, e->data_len);
    }

    pthread_mutex_unlock(&g_xdp.lock);
    return 0;
}

/* ── Garbage collection ──────────────────────────────────────────────── */

void xdp_reasm_gc(void) {
    time_t now = time(NULL);

    pthread_mutex_lock(&g_xdp.lock);

    for (int i = 0; i < XDP_CONN_HASH_SIZE; i++) {
        xdp_conn_t *prev = NULL, *c = g_xdp.hash[i];
        while (c) {
            int should_free = 0;

            if (now - c->last_active > XDP_CONN_GC_TIMEOUT)
                should_free = 1;
            if (c->closing && c->ooo_head == NULL && c->buf_len == 0)
                should_free = 1;

            if (should_free) {
                xdp_conn_t *victim = c;
                if (prev)
                    prev->hash_next = c->hash_next;
                else
                    g_xdp.hash[i] = c->hash_next;
                c = c->hash_next;

                /* Free OOO segments */
                xdp_ooo_seg_t *seg = victim->ooo_head;
                while (seg) {
                    xdp_ooo_seg_t *next = seg->next;
                    free(seg->data);
                    free(seg);
                    g_xdp.stats.ooo_segments--;
                    seg = next;
                }
                free(victim->buf);
                free(victim);
                g_xdp.count--;
            } else {
                prev = c;
                c = c->hash_next;
            }
        }
    }

    g_xdp.stats.conns_active = g_xdp.count;
    pthread_mutex_unlock(&g_xdp.lock);
}

/* ── Public API ──────────────────────────────────────────────────────── */

int xdp_reasm_init(xdp_reasm_write_cb on_write) {
    memset(&g_xdp, 0, sizeof(g_xdp));
    g_xdp.on_write = on_write;

    if (pthread_mutex_init(&g_xdp.lock, NULL) != 0) {
        fprintf(stderr, "[XDP-REASM] mutex_init failed\n");
        return -1;
    }

    printf("[XDP-REASM] Initialized (max %d connections)\n", XDP_CONN_MAX);
    return 0;
}

void xdp_reasm_shutdown(void) {
    pthread_mutex_lock(&g_xdp.lock);

    for (int i = 0; i < XDP_CONN_HASH_SIZE; i++) {
        xdp_conn_t *c = g_xdp.hash[i];
        while (c) {
            xdp_conn_t *next = c->hash_next;
            xdp_ooo_seg_t *seg = c->ooo_head;
            while (seg) {
                xdp_ooo_seg_t *snext = seg->next;
                free(seg->data);
                free(seg);
                seg = snext;
            }
            free(c->buf);
            free(c);
            c = next;
        }
        g_xdp.hash[i] = NULL;
    }
    g_xdp.count = 0;

    pthread_mutex_unlock(&g_xdp.lock);
    pthread_mutex_destroy(&g_xdp.lock);

    printf("[XDP-REASM] Shutdown complete\n");
}

void xdp_reasm_get_stats(struct xdp_reasm_stats *st) {
    pthread_mutex_lock(&g_xdp.lock);
    memcpy(st, &g_xdp.stats, sizeof(*st));
    st->conns_active = g_xdp.count;
    pthread_mutex_unlock(&g_xdp.lock);
}

/* ── XDP BPF loader ────────────────────────────────────────────────────
 *
 * Loads XDP BPF object, sets config, attaches to interface, creates ringbuf.
 *
 * Interface: $XDP_IFACE  (default "lo")
 * XDP mode:   $XDP_MODE  ("auto"=link, "generic"=SKB, "native"=DRV)
 *
 * Uses BPF link API (kernel 5.7+): auto-detects best mode.
 * Falls back to netlink API for forced generic/native mode.
 *
 * Tracks whether we used a BPF link or netlink for proper cleanup. */

void xdp_detach(xdp_attach_t *a) {
    if (!a) return;
    if (a->use_netlink) {
        if (a->prog_fd >= 0 && a->ifindex > 0)
            bpf_xdp_detach(a->ifindex, 0, NULL);
    } else {
        if (a->link)
            bpf_link__destroy(a->link);
    }
    a->link   = NULL;
    a->prog_fd = -1;
}

int xdp_load_bpf(uint16_t kv_port, const char *iface_name,
                 ring_buffer_sample_fn ringbuf_cb,
                 struct bpf_object **out_obj,
                 struct ring_buffer **out_rb,
                 xdp_attach_t *out_att)
{
    memset(out_att, 0, sizeof(*out_att));
    out_att->prog_fd = -1;
    *out_obj  = NULL;
    *out_rb   = NULL;

    if (!iface_name)
        iface_name = getenv("XDP_IFACE");
    if (!iface_name || !*iface_name)
        iface_name = "lo";

    /* ── Resolve interface ─────────────────────────────────────────── */
    out_att->ifindex = if_nametoindex(iface_name);
    if (out_att->ifindex == 0) {
        fprintf(stderr, "[XDP-LOAD] Interface not found: '%s'\n", iface_name);
        fprintf(stderr, "[XDP-LOAD] Set XDP_IFACE env var. Available:\n");
        for (int i = 1; i <= 10; i++) {
            char name[IF_NAMESIZE];
            if (if_indextoname(i, name))
                fprintf(stderr, "[XDP-LOAD]   %s\n", name);
        }
        return -1;
    }

    /* ── Find BPF object file ──────────────────────────────────────── */
    char obj_path[512];
    const char *build_dir = getenv("BUILD_DIR");
    if (build_dir) {
        snprintf(obj_path, sizeof(obj_path), "%s/repl_filter_xdp.bpf.o", build_dir);
    } else {
        char self[256];
        ssize_t n = readlink("/proc/self/exe", self, sizeof(self) - 1);
        if (n > 0) {
            self[n] = '\0';
            char *slash = strrchr(self, '/');
            if (slash) *slash = '\0';
            snprintf(obj_path, sizeof(obj_path), "%s/repl_filter_xdp.bpf.o", self);
            if (access(obj_path, R_OK) != 0)
                snprintf(obj_path, sizeof(obj_path), "build/repl_filter_xdp.bpf.o");
        } else {
            snprintf(obj_path, sizeof(obj_path), "build/repl_filter_xdp.bpf.o");
        }
    }

    if (access(obj_path, R_OK) != 0) {
        fprintf(stderr, "[XDP-LOAD] BPF object not found: %s\n", obj_path);
        return -1;
    }

    /* ── Open and load ─────────────────────────────────────────────── */
    struct bpf_object *obj = bpf_object__open_file(obj_path, NULL);
    if (!obj) {
        fprintf(stderr, "[XDP-LOAD] bpf_object__open_file: %s\n", strerror(errno));
        return -1;
    }
    if (bpf_object__load(obj) != 0) {
        fprintf(stderr, "[XDP-LOAD] bpf_object__load failed: %s\n", strerror(errno));
        bpf_object__close(obj);
        return -1;
    }

    /* ── Set kvstore port ──────────────────────────────────────────── */
    struct bpf_map *cfg_map = bpf_object__find_map_by_name(obj, "repl_config");
    if (cfg_map) {
        uint32_t key = 0, val = (uint32_t)kv_port;
        bpf_map__update_elem(cfg_map, &key, sizeof(key), &val, sizeof(val), BPF_ANY);
    }

    /* ── Find XDP program ──────────────────────────────────────────── */
    struct bpf_program *prog =
        bpf_object__find_program_by_name(obj, "xdp_repl_sniff");
    if (!prog) {
        fprintf(stderr, "[XDP-LOAD] Program 'xdp_repl_sniff' not found\n");
        bpf_object__close(obj);
        return -1;
    }

    /* ── Attach XDP ────────────────────────────────────────────────── */
    const char *mode_str = getenv("XDP_MODE");
    const char *mode_label = "auto";

    if (!mode_str || strcmp(mode_str, "auto") == 0) {
        /* BPF link API — kernel auto-detects best mode */
        out_att->link = bpf_program__attach_xdp(prog, out_att->ifindex);
        if (!out_att->link) {
            fprintf(stderr, "[XDP-LOAD] XDP attach to '%s' failed: %s\n",
                    iface_name, strerror(errno));
            bpf_object__close(obj);
            return -1;
        }
        out_att->use_netlink = 0;
    } else {
        /* Forced mode via netlink API */
        uint32_t flags = 0;
        if (strcmp(mode_str, "generic") == 0) {
            flags = XDP_FLAGS_SKB_MODE;
            mode_label = "generic(SKB)";
        } else if (strcmp(mode_str, "native") == 0) {
            flags = XDP_FLAGS_DRV_MODE;
            mode_label = "native(DRV)";
        } else {
            fprintf(stderr, "[XDP-LOAD] Unknown XDP_MODE '%s' (valid: auto, generic, native)\n",
                    mode_str);
            bpf_object__close(obj);
            return -1;
        }
        out_att->prog_fd = bpf_program__fd(prog);
        if (bpf_xdp_attach(out_att->ifindex, out_att->prog_fd, flags, NULL) != 0) {
            fprintf(stderr, "[XDP-LOAD] XDP %s attach to '%s' failed: %s\n",
                    mode_label, iface_name, strerror(errno));
            bpf_object__close(obj);
            return -1;
        }
        out_att->use_netlink = 1;
    }

    /* ── Create ringbuf consumer ──────────────────────────────────── */
    struct bpf_map *rb_map = bpf_object__find_map_by_name(obj, "repl_ringbuf");
    if (!rb_map) {
        fprintf(stderr, "[XDP-LOAD] ringbuf map not found\n");
        xdp_detach(out_att);
        bpf_object__close(obj);
        return -1;
    }

    struct ring_buffer *rb = ring_buffer__new(bpf_map__fd(rb_map), ringbuf_cb, NULL, NULL);
    if (!rb) {
        fprintf(stderr, "[XDP-LOAD] ring_buffer__new failed\n");
        xdp_detach(out_att);
        bpf_object__close(obj);
        return -1;
    }

    *out_obj = obj;
    *out_rb  = rb;

    printf("[XDP-LOAD] Attached to '%s' (ifindex=%d, %s mode), port %u\n",
           iface_name, out_att->ifindex, mode_label, kv_port);
    return 0;
}
