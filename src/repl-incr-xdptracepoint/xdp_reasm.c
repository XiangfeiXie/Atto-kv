/* xdp_reasm.c — TCP reassembly + RESP extraction + XDP loader
 *
 * Data pipeline:
 *   XDP ringbuf → xdp_reasm_on_event() → per-bucket lock
 *     → conn lookup/create → seq dedup/order
 *     → circular buffer → RESP scan → write-command filter
 *     → on_write callback
 */

#define _GNU_SOURCE
#include "xdp_reasm.h"
#include "xdp_common.h"
#include "config.h"

#include <arpa/inet.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <linux/if_link.h>
#include <net/if.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* ── Circular buffer ──────────────────────────────────────────────────── */

typedef struct {
    uint8_t *data;
    uint32_t cap;      /* power of 2 */
    uint32_t mask;     /* cap - 1 */
    uint32_t rd;       /* parser read cursor (monotonically increasing) */
    uint32_t wr;       /* append write cursor (monotonically increasing) */
} circ_buf_t;

/* Available bytes to read */
static inline uint32_t circ_used(const circ_buf_t *cb) {
    return (cb->wr - cb->rd) & cb->mask;
}

/* Free space (reserve 1 byte to distinguish full from empty) */
static inline uint32_t circ_free(const circ_buf_t *cb) {
    return cb->cap - circ_used(cb) - 1;
}

/* Peek at byte at given offset from rd */
static inline uint8_t circ_peek(const circ_buf_t *cb, uint32_t off) {
    return cb->data[(cb->rd + off) & cb->mask];
}

/* Advance rd by n bytes */
static void circ_consume(circ_buf_t *cb, uint32_t n) {
    cb->rd += n;
}

/* Copy len bytes starting at rd into dst (linearized) */
static void circ_copyout(const circ_buf_t *cb, uint8_t *dst, uint32_t len) {
    uint32_t pos = cb->rd & cb->mask;
    uint32_t first = len;
    if (pos + first > cb->cap)
        first = cb->cap - pos;
    memcpy(dst, cb->data + pos, first);
    if (len > first)
        memcpy(dst + first, cb->data, len - first);
}

/* Expand circular buffer to new_cap (must be power of 2, > cap).
 * Linearizes data so rd=0, wr=used. */
static int circ_expand(circ_buf_t *cb, uint32_t new_cap) {
    uint8_t *nd = malloc(new_cap);
    if (!nd) return -1;
    uint32_t used = circ_used(cb);
    circ_copyout(cb, nd, used);
    free(cb->data);
    cb->data = nd;
    cb->cap  = new_cap;
    cb->mask = new_cap - 1;
    cb->rd   = 0;
    cb->wr   = used;
    return 0;
}

/* Ensure at least need bytes free. Expands if necessary.
 * Returns 0 on success, -1 if expansion fails or cap exceeded. */
static int circ_reserve(circ_buf_t *cb, uint32_t need) {
    /* Initial allocation */
    if (!cb->data) {
        uint32_t cap = CMD_BUF_CAP_MIN;
        while (cap - 1 < need) {
            if (cap >= CMD_BUF_CAP_MAX) return -1;
            cap *= 2;
        }
        cb->data = malloc(cap);
        if (!cb->data) return -1;
        cb->cap  = cap;
        cb->mask = cap - 1;
        cb->rd   = 0;
        cb->wr   = 0;
        return 0;
    }
    /* Already allocated — check free space */
    if (circ_free(cb) >= need)
        return 0;
    uint32_t new_cap = cb->cap;
    while (new_cap - circ_used(cb) - 1 < need) {
        if (new_cap >= CMD_BUF_CAP_MAX)
            return -1;
        new_cap *= 2;
    }
    return circ_expand(cb, new_cap);
}

/* Append len bytes to the circular buffer.
 * Returns 0 on success, -1 if buffer full and cannot expand. */
static int circ_write(circ_buf_t *cb, const uint8_t *src, uint32_t len) {
    if (len == 0) return 0;
    if (circ_reserve(cb, len) != 0)
        return -1;
    uint32_t pos = cb->wr & cb->mask;
    uint32_t first = len;
    if (pos + first > cb->cap)
        first = cb->cap - pos;
    memcpy(cb->data + pos, src, first);
    if (len > first)
        memcpy(cb->data, src + first, len - first);
    cb->wr += len;
    return 0;
}

/* Free large idle buffer */
static void circ_shrink(circ_buf_t *cb) {
    if (circ_used(cb) == 0 && cb->cap > 65536) {
        free(cb->data);
        cb->data = NULL;
        cb->cap  = 0;
        cb->mask = 0;
        cb->rd   = 0;
        cb->wr   = 0;
    }
}

/* ── OOO segment list ─────────────────────────────────────────────────── */

typedef struct ooo_seg {
    uint32_t        seq;
    uint32_t        len;
    uint8_t        *data;
    struct ooo_seg *next;
} ooo_seg_t;

/* ── RESP parser (works on circular buffer) ───────────────────────────── */

/* Returns: >0 = complete command length, 0 = incomplete, -1 = protocol error */
static int resp_scan(const circ_buf_t *cb) {
    uint32_t avail = circ_used(cb);
    if (avail < 4) return 0;
    if (circ_peek(cb, 0) != '*') return -1;

    uint32_t i = 1;
    int argc = 0;
    while (i < avail) {
        uint8_t c = circ_peek(cb, i);
        if (c >= '0' && c <= '9') {
            argc = argc * 10 + (c - '0');
            if (argc > 64) return -1;
            i++;
        } else {
            break;
        }
    }
    if (i == 1) return -1;
    if (i + 1 >= avail) return 0;
    if (circ_peek(cb, i) != '\r' || circ_peek(cb, i + 1) != '\n') return -1;
    i += 2;

    for (int arg = 0; arg < argc; arg++) {
        if (i >= avail) return 0;
        if (circ_peek(cb, i) != '$') return -1;
        i++;

        int arg_len = 0;
        while (i < avail) {
            uint8_t c = circ_peek(cb, i);
            if (c >= '0' && c <= '9') {
                arg_len = arg_len * 10 + (c - '0');
                if (arg_len > RESP_CMD_MAX_LEN) return -1;
                i++;
            } else {
                break;
            }
        }
        if (i + 1 >= avail) return 0;
        if (circ_peek(cb, i) != '\r' || circ_peek(cb, i + 1) != '\n') return -1;
        i += 2;

        if (i + (uint32_t)arg_len + 2 > avail) return 0;
        i += (uint32_t)arg_len + 2;
    }

    return (int)i;
}

/* Check if a complete RESP command (at cb->rd, length cmd_len) is a write.
 * Uses same 3-char prefix matching as the original. */
static int is_write_command_buf(const circ_buf_t *cb, uint32_t cmd_len) {
    if (cmd_len < 12) return 0;

    /* Skip *N\r\n */
    uint32_t i = 1;
    while (i < cmd_len) {
        uint8_t c = circ_peek(cb, i);
        if (c >= '0' && c <= '9') i++;
        else break;
    }
    if (i + 1 >= cmd_len || circ_peek(cb, i) != '\r' || circ_peek(cb, i + 1) != '\n') return 0;
    i += 2;

    /* Skip $L\r\n to reach command name */
    if (i >= cmd_len || circ_peek(cb, i) != '$') return 0;
    i++;
    while (i < cmd_len) {
        uint8_t c = circ_peek(cb, i);
        if (c >= '0' && c <= '9') i++;
        else break;
    }
    if (i + 1 >= cmd_len || circ_peek(cb, i) != '\r' || circ_peek(cb, i + 1) != '\n') return 0;
    i += 2;

    if (i + 2 >= cmd_len) return 0;

    /* Uppercase first 3 chars of command name */
    char c0 = (char)circ_peek(cb, i);
    char c1 = (char)circ_peek(cb, i + 1);
    char c2 = (char)circ_peek(cb, i + 2);
    if (c0 >= 'a' && c0 <= 'z') c0 -= 32;
    if (c1 >= 'a' && c1 <= 'z') c1 -= 32;
    if (c2 >= 'a' && c2 <= 'z') c2 -= 32;

    /* SET, DEL, DECR, INCR, LPUSH/RPUSH, SADD/SREM, ZADD/ZREM,
     * HSET/HDEL, APPEND, MSET, FLUSH(DB/ALL) */
    if (c0 == 'S' && c1 == 'E' && c2 == 'T') return 1;
    if (c0 == 'D' && c1 == 'E' && c2 == 'L') return 1;
    if (c0 == 'D' && c1 == 'E' && c2 == 'C') return 1;
    if (c0 == 'L' && c1 == 'P')               return 1;
    if (c0 == 'R' && c1 == 'P')               return 1;
    if (c0 == 'H' && (c1 == 'S' || c1 == 'D')) return 1;
    if (c0 == 'S' && (c1 == 'A' || c1 == 'R')) return 1;
    if (c0 == 'Z' && (c1 == 'A' || c1 == 'R')) return 1;
    if (c0 == 'I' && c1 == 'N' && c2 == 'C') return 1;
    if (c0 == 'A' && c1 == 'P' && c2 == 'P') return 1;
    if (c0 == 'M' && c1 == 'S' && c2 == 'E') return 1;
    if (c0 == 'F' && c1 == 'L' && c2 == 'U') return 1;

    return 0;
}

/* ── Connection ────────────────────────────────────────────────────────── */

typedef struct xdp_conn {
    uint32_t         saddr;        /* network byte order */
    uint16_t         sport;        /* network byte order */
    uint32_t         rcv_nxt;      /* next expected seq (host byte order) */
    int              isn_known;
    uint32_t         isn;          /* host byte order */

    ooo_seg_t       *ooo_head;
    int              ooo_count;

    circ_buf_t       buf;

    int              closing;      /* FIN/RST seen */
    time_t           last_active;

    struct xdp_conn *next;         /* hash chain */
} xdp_conn_t;

/* ── Bucket ────────────────────────────────────────────────────────────── */

typedef struct {
    pthread_mutex_t mutex;
    xdp_conn_t    *head;
} conn_bucket_t;

/* ── Global state ──────────────────────────────────────────────────────── */

static struct {
    conn_bucket_t       buckets[CONN_HASH_SIZE];
    int                 count;           /* current connections (unlocked, approximate) */
    xdp_reasm_write_cb  on_write;
    struct xdp_reasm_stats stats;
} g_xdp;

/* ── Helpers ───────────────────────────────────────────────────────────── */

static inline uint32_t conn_hash_idx(uint32_t saddr, uint16_t sport) {
    uint32_t h = saddr ^ ((uint32_t)sport * 0x9e3779b9u);
    return h & (CONN_HASH_SIZE - 1);
}

static uint32_t seq_to_host(uint32_t net_seq) {
    return ntohl(net_seq);
}

/* Must be called with bucket lock held */
static xdp_conn_t *conn_lookup_locked(conn_bucket_t *bkt,
                                      uint32_t saddr, uint16_t sport) {
    for (xdp_conn_t *c = bkt->head; c; c = c->next)
        if (c->saddr == saddr && c->sport == sport)
            return c;
    return NULL;
}

/* Must be called with bucket lock held.
 * On failure (count exceeded), returns NULL. */
static xdp_conn_t *conn_create_locked(conn_bucket_t *bkt,
                                      uint32_t saddr, uint16_t sport) {
    if (g_xdp.count >= CONN_MAX) {
        /* Try to evict the stalest connection in this bucket */
        xdp_conn_t *stale = NULL, *prev = NULL, *sprev = NULL;
        for (xdp_conn_t *c = bkt->head, *p = NULL; c; p = c, c = c->next) {
            if (c->closing && c->buf.rd == c->buf.wr && !c->ooo_head) {
                stale = c; sprev = p; break;
            }
            if (!stale || c->last_active < stale->last_active) {
                stale = c; sprev = p;
            }
        }
        if (stale) {
            /* Remove stale from list */
            if (sprev) sprev->next = stale->next;
            else bkt->head = stale->next;
            /* Free stale resources */
            ooo_seg_t *seg = stale->ooo_head;
            while (seg) { ooo_seg_t *nx = seg->next; free(seg->data); free(seg); seg = nx; }
            free(stale->buf.data);
            free(stale);
            g_xdp.count--;
        } else {
            return NULL;
        }
    }

    xdp_conn_t *c = calloc(1, sizeof(*c));
    if (!c) return NULL;

    c->saddr       = saddr;
    c->sport       = sport;
    c->last_active = time(NULL);
    c->next        = bkt->head;
    bkt->head      = c;
    g_xdp.count++;
    return c;
}

/* ── OOO segment management ────────────────────────────────────────────── */

/* Insert segment into OOO list sorted by seq, merging adjacent/overlapping.
 * Must be called with bucket lock held. */
static void ooo_insert(xdp_conn_t *c, uint32_t seq,
                       const uint8_t *data, uint32_t len) {
    if (c->ooo_count >= OOO_MAX_SEGMENTS) {
        /* Too many OOO segments — connection likely corrupt, reset */
        ooo_seg_t *seg = c->ooo_head;
        while (seg) { ooo_seg_t *nx = seg->next; free(seg->data); free(seg); seg = nx; }
        c->ooo_head = NULL;
        c->ooo_count = 0;
        return;
    }

    ooo_seg_t *ns = malloc(sizeof(*ns));
    if (!ns) return;
    ns->seq  = seq;
    ns->len  = len;
    ns->data = malloc(len);
    if (!ns->data) { free(ns); return; }
    memcpy(ns->data, data, len);
    ns->next = NULL;

    c->ooo_count++;
    g_xdp.stats.ooo_segments++;

    /* Find insertion point */
    ooo_seg_t *prev = NULL, *cur = c->ooo_head;
    while (cur && cur->seq < ns->seq) {
        prev = cur;
        cur  = cur->next;
    }

    /* Merge with predecessor */
    if (prev) {
        uint32_t prev_end = prev->seq + prev->len;
        if (prev_end >= ns->seq) {
            if (ns->seq + ns->len > prev_end) {
                uint32_t extend = ns->seq + ns->len - prev_end;
                uint32_t new_len = prev->len + extend;
                uint8_t *nd = realloc(prev->data, new_len);
                if (nd) {
                    prev->data = nd;
                    memcpy(prev->data + prev->len,
                           ns->data + (prev_end - ns->seq), extend);
                    prev->len = new_len;
                }
            }
            free(ns->data);
            free(ns);
            g_xdp.stats.ooo_segments--;
            c->ooo_count--;
            ns = NULL;
        }
    }

    if (!ns) return;

    /* Insert */
    ns->next = cur;
    if (prev) prev->next = ns;
    else      c->ooo_head = ns;
    prev = ns;

    /* Merge forward */
    while (cur) {
        uint32_t prev_end = prev->seq + prev->len;
        if (prev_end >= cur->seq) {
            if (cur->seq + cur->len > prev_end) {
                uint32_t extend = cur->seq + cur->len - prev_end;
                uint32_t new_len = prev->len + extend;
                uint8_t *nd = realloc(prev->data, new_len);
                if (nd) {
                    prev->data = nd;
                    memcpy(prev->data + prev->len,
                           cur->data + (prev_end - cur->seq), extend);
                    prev->len = new_len;
                }
            }
            prev->next = cur->next;
            free(cur->data);
            free(cur);
            g_xdp.stats.ooo_segments--;
            c->ooo_count--;
            cur = prev->next;
        } else {
            break;
        }
    }
}

/* Drain OOO segments that are now contiguous with rcv_nxt */
static void consume_ordered(xdp_conn_t *c, const uint8_t *data, uint32_t len);

static void ooo_drain(xdp_conn_t *c) {
    while (c->ooo_head) {
        ooo_seg_t *seg = c->ooo_head;
        if (seg->seq > c->rcv_nxt)
            break;

        uint32_t seg_end = seg->seq + seg->len;
        if (seg_end > c->rcv_nxt) {
            uint32_t skip    = c->rcv_nxt - seg->seq;
            uint32_t new_len = seg_end - c->rcv_nxt;
            consume_ordered(c, seg->data + skip, new_len);
            c->rcv_nxt = seg_end;
        }

        c->ooo_head = seg->next;
        free(seg->data);
        free(seg);
        g_xdp.stats.ooo_segments--;
        c->ooo_count--;
    }
}

/* ── Ordered data consumer ─────────────────────────────────────────────── */

static void consume_ordered(xdp_conn_t *c, const uint8_t *data, uint32_t len) {
    if (len == 0) return;

    g_xdp.stats.bytes_ordered += len;

    if (circ_write(&c->buf, data, len) != 0) {
        /* Buffer overflow — reset (likely garbage) */
        c->buf.rd = c->buf.wr = 0;
        circ_shrink(&c->buf);
        return;
    }

    /* Extract complete RESP commands */
    uint8_t scratch[4096];   /* stack buffer for linearization */

    for (;;) {
        int cmd_len = resp_scan(&c->buf);
        if (cmd_len <= 0) {
            if (cmd_len < 0 && circ_used(&c->buf) > CMD_BUF_CAP_MAX) {
                /* Stale/unparseable data, reset */
                c->buf.rd = c->buf.wr = 0;
                circ_shrink(&c->buf);
            }
            break;
        }

        g_xdp.stats.cmds_extracted++;

        if (is_write_command_buf(&c->buf, (uint32_t)cmd_len)) {
            g_xdp.stats.cmds_written++;
            if (g_xdp.on_write) {
                if ((uint32_t)cmd_len <= sizeof(scratch)) {
                    circ_copyout(&c->buf, scratch, (uint32_t)cmd_len);
                    g_xdp.on_write(scratch, (uint32_t)cmd_len);
                } else {
                    uint8_t *tmp = malloc((uint32_t)cmd_len);
                    if (tmp) {
                        circ_copyout(&c->buf, tmp, (uint32_t)cmd_len);
                        g_xdp.on_write(tmp, (uint32_t)cmd_len);
                        free(tmp);
                    }
                }
            }
        }

        circ_consume(&c->buf, (uint32_t)cmd_len);
    }

    circ_shrink(&c->buf);
}

/* ── Packet processing (seq logic) ─────────────────────────────────────── */

static void process_packet(xdp_conn_t *c,
                           uint32_t seq_h, const uint8_t *data, uint32_t len) {
    if (len == 0) return;

    uint32_t seg_end = seq_h + len;

    /* Completely duplicate (fully before rcv_nxt) */
    if (seg_end <= c->rcv_nxt) {
        g_xdp.stats.dup_discarded++;
        return;
    }

    /* Partial overlap (starts before rcv_nxt) */
    if (seq_h < c->rcv_nxt) {
        uint32_t skip = c->rcv_nxt - seq_h;
        data  += skip;
        len   -= skip;
        seq_h  = c->rcv_nxt;
    }

    /* In-order */
    if (seq_h == c->rcv_nxt) {
        consume_ordered(c, data, len);
        c->rcv_nxt += len;
        ooo_drain(c);
        return;
    }

    /* Out-of-order */
    ooo_insert(c, seq_h, data, len);
}

/* ── Ringbuf callback ──────────────────────────────────────────────────── */

int xdp_reasm_on_event(void *ctx, void *data, size_t data_sz) {
    (void)ctx;
    (void)data_sz;

    struct xdp_repl_event *e = (struct xdp_repl_event *)data;

    /* Count in global stats (atomic increment via relaxed store) */
    __atomic_add_fetch(&g_xdp.stats.events_total, 1, __ATOMIC_RELAXED);

    uint32_t idx = conn_hash_idx(e->saddr, e->sport);
    conn_bucket_t *bkt = &g_xdp.buckets[idx];

    pthread_mutex_lock(&bkt->mutex);

    xdp_conn_t *c = conn_lookup_locked(bkt, e->saddr, e->sport);

    /* ── Handle SYN ────────────────────────────────────────────────── */
    if (e->flags & XDP_FLAG_SYN) {
        __atomic_add_fetch(&g_xdp.stats.events_syn, 1, __ATOMIC_RELAXED);
        if (!c) {
            c = conn_create_locked(bkt, e->saddr, e->sport);
            if (!c) { pthread_mutex_unlock(&bkt->mutex); return 0; }
        }
        if (!c->isn_known) {
            c->isn      = seq_to_host(e->seq);
            c->rcv_nxt  = c->isn + 1;
            c->isn_known = 1;
            c->closing   = 0;
        }
        c->last_active = time(NULL);
        if (e->data_len > 0) {
            process_packet(c, seq_to_host(e->seq) + 1,
                           e->data, e->data_len);
        }
        pthread_mutex_unlock(&bkt->mutex);
        return 0;
    }

    /* Connection must exist for non-SYN */
    if (!c) {
        c = conn_create_locked(bkt, e->saddr, e->sport);
        if (!c) { pthread_mutex_unlock(&bkt->mutex); return 0; }
        /* Lame connection: estimate ISN = seq - 1 */
        c->isn       = seq_to_host(e->seq) - 1;
        c->rcv_nxt   = c->isn + 1;
        c->isn_known = 0;
    }

    /* ── Handle FIN/RST ────────────────────────────────────────────── */
    if (e->flags & (XDP_FLAG_FIN | XDP_FLAG_RST)) {
        __atomic_add_fetch(&g_xdp.stats.events_fin_rst, 1, __ATOMIC_RELAXED);
        c->closing = 1;
        uint32_t data_seq = seq_to_host(e->seq);
        if (e->data_len > 0)
            process_packet(c, data_seq, e->data, e->data_len);
        if (e->flags & XDP_FLAG_FIN) {
            uint8_t fin_byte = 0;
            process_packet(c, data_seq + e->data_len, &fin_byte, 1);
        }
        c->last_active = time(NULL);
        pthread_mutex_unlock(&bkt->mutex);
        return 0;
    }

    /* ── Handle data ───────────────────────────────────────────────── */
    if (e->data_len > 0) {
        __atomic_add_fetch(&g_xdp.stats.events_data, 1, __ATOMIC_RELAXED);
        c->last_active = time(NULL);
        process_packet(c, seq_to_host(e->seq), e->data, e->data_len);
    }

    /* Count fragment events */
    if (e->flags & XDP_FLAG_FRAG)
        __atomic_add_fetch(&g_xdp.stats.fragment_events, 1, __ATOMIC_RELAXED);

    pthread_mutex_unlock(&bkt->mutex);
    return 0;
}

/* ── Garbage collection ────────────────────────────────────────────────── */

void xdp_reasm_gc(void) {
    time_t now = time(NULL);

    for (int i = 0; i < CONN_HASH_SIZE; i++) {
        conn_bucket_t *bkt = &g_xdp.buckets[i];
        pthread_mutex_lock(&bkt->mutex);

        xdp_conn_t *prev = NULL, *c = bkt->head;
        while (c) {
            int gc = 0;
            if (now - c->last_active > CONN_GC_TIMEOUT)
                gc = 1;
            if (c->closing && !c->ooo_head && circ_used(&c->buf) == 0)
                gc = 1;

            if (gc) {
                xdp_conn_t *victim = c;
                if (prev) prev->next = c->next;
                else      bkt->head  = c->next;
                c = c->next;

                ooo_seg_t *seg = victim->ooo_head;
                while (seg) {
                    ooo_seg_t *nx = seg->next;
                    free(seg->data); free(seg);
                    g_xdp.stats.ooo_segments--;
                    seg = nx;
                }
                free(victim->buf.data);
                free(victim);
                g_xdp.count--;
            } else {
                prev = c;
                c = c->next;
            }
        }

        pthread_mutex_unlock(&bkt->mutex);
    }

    __atomic_store_n(&g_xdp.stats.conns_active, g_xdp.count, __ATOMIC_RELAXED);
}

/* ── Public API ────────────────────────────────────────────────────────── */

int xdp_reasm_init(xdp_reasm_write_cb on_write) {
    memset(&g_xdp, 0, sizeof(g_xdp));
    g_xdp.on_write = on_write;

    for (int i = 0; i < CONN_HASH_SIZE; i++) {
        if (pthread_mutex_init(&g_xdp.buckets[i].mutex, NULL) != 0) {
            fprintf(stderr, "[XDP-REASM] mutex_init[%d] failed\n", i);
            /* Destroy already-initialized mutexes */
            for (int j = 0; j < i; j++)
                pthread_mutex_destroy(&g_xdp.buckets[j].mutex);
            return -1;
        }
    }

    printf("[XDP-REASM] init OK: %d buckets, max %d conns\n",
           CONN_HASH_SIZE, CONN_MAX);
    return 0;
}

void xdp_reasm_shutdown(void) {
    for (int i = 0; i < CONN_HASH_SIZE; i++) {
        conn_bucket_t *bkt = &g_xdp.buckets[i];
        pthread_mutex_lock(&bkt->mutex);

        xdp_conn_t *c = bkt->head;
        while (c) {
            xdp_conn_t *next = c->next;
            ooo_seg_t *seg = c->ooo_head;
            while (seg) {
                ooo_seg_t *nx = seg->next;
                free(seg->data); free(seg);
                seg = nx;
            }
            free(c->buf.data);
            free(c);
            c = next;
        }
        bkt->head = NULL;

        pthread_mutex_unlock(&bkt->mutex);
        pthread_mutex_destroy(&bkt->mutex);
    }

    g_xdp.count = 0;
    printf("[XDP-REASM] shutdown complete\n");
}

void xdp_reasm_get_stats(struct xdp_reasm_stats *st) {
    memcpy(st, &g_xdp.stats, sizeof(*st));
    st->conns_active = g_xdp.count;
}

/* ── XDP BPF loader ────────────────────────────────────────────────────── */

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
    *out_obj = NULL;
    *out_rb  = NULL;

    if (!iface_name || !*iface_name)
        iface_name = XDP_IFACE;

    /* Resolve interface */
    out_att->ifindex = if_nametoindex(iface_name);
    if (out_att->ifindex == 0) {
        fprintf(stderr, "[XDP-LOAD] interface not found: '%s'\n", iface_name);
        fprintf(stderr, "[XDP-LOAD] set XDP_IFACE. Available:\n");
        for (int i = 1; i <= 10; i++) {
            char name[IF_NAMESIZE];
            if (if_indextoname(i, name))
                fprintf(stderr, "[XDP-LOAD]   %s\n", name);
        }
        return -1;
    }

    /* Find BPF object file */
    char obj_path[512];
    const char *build_dir = getenv("BUILD_DIR");
    if (build_dir) {
        snprintf(obj_path, sizeof(obj_path), "%s/xdp_filter.bpf.o", build_dir);
    } else {
        char self[256];
        ssize_t n = readlink("/proc/self/exe", self, sizeof(self) - 1);
        if (n > 0) {
            self[n] = '\0';
            char *slash = strrchr(self, '/');
            if (slash) *slash = '\0';
            snprintf(obj_path, sizeof(obj_path), "%s/xdp_filter.bpf.o", self);
            if (access(obj_path, R_OK) != 0)
                snprintf(obj_path, sizeof(obj_path), "build/xdp_filter.bpf.o");
        } else {
            snprintf(obj_path, sizeof(obj_path), "build/xdp_filter.bpf.o");
        }
    }

    if (access(obj_path, R_OK) != 0) {
        fprintf(stderr, "[XDP-LOAD] BPF object not found: %s\n", obj_path);
        return -1;
    }

    /* Open and load */
    struct bpf_object *obj = bpf_object__open_file(obj_path, NULL);
    if (!obj) {
        fprintf(stderr, "[XDP-LOAD] bpf_object__open_file: %s\n", strerror(errno));
        return -1;
    }
    if (bpf_object__load(obj) != 0) {
        fprintf(stderr, "[XDP-LOAD] bpf_object__load: %s\n", strerror(errno));
        bpf_object__close(obj);
        return -1;
    }

    /* Set kvstore port */
    struct bpf_map *cfg_map = bpf_object__find_map_by_name(obj, "repl_config");
    if (cfg_map) {
        uint32_t key = 0, val = (uint32_t)kv_port;
        bpf_map__update_elem(cfg_map, &key, sizeof(key), &val, sizeof(val), BPF_ANY);
    }

    /* Find XDP program */
    struct bpf_program *prog =
        bpf_object__find_program_by_name(obj, "xdp_repl_sniff");
    if (!prog) {
        fprintf(stderr, "[XDP-LOAD] program 'xdp_repl_sniff' not found\n");
        bpf_object__close(obj);
        return -1;
    }

    /* Attach XDP */
    const char *mode_str = getenv("XDP_MODE");
    const char *mode_label = "auto";

    if (!mode_str || strcmp(mode_str, "auto") == 0) {
        out_att->link = bpf_program__attach_xdp(prog, out_att->ifindex);
        if (!out_att->link) {
            fprintf(stderr, "[XDP-LOAD] attach to '%s' failed: %s\n",
                    iface_name, strerror(errno));
            bpf_object__close(obj);
            return -1;
        }
        out_att->use_netlink = 0;
    } else {
        uint32_t flags = 0;
        if (strcmp(mode_str, "generic") == 0) {
            flags = XDP_FLAGS_SKB_MODE;
            mode_label = "generic(SKB)";
        } else if (strcmp(mode_str, "native") == 0) {
            flags = XDP_FLAGS_DRV_MODE;
            mode_label = "native(DRV)";
        } else {
            fprintf(stderr, "[XDP-LOAD] unknown XDP_MODE '%s' "
                    "(valid: auto, generic, native)\n", mode_str);
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

    /* Create ringbuf consumer */
    struct bpf_map *rb_map = bpf_object__find_map_by_name(obj, "repl_ringbuf");
    if (!rb_map) {
        fprintf(stderr, "[XDP-LOAD] ringbuf map not found\n");
        xdp_detach(out_att);
        bpf_object__close(obj);
        return -1;
    }

    struct ring_buffer *rb = ring_buffer__new(bpf_map__fd(rb_map),
                                              ringbuf_cb, NULL, NULL);
    if (!rb) {
        fprintf(stderr, "[XDP-LOAD] ring_buffer__new failed\n");
        xdp_detach(out_att);
        bpf_object__close(obj);
        return -1;
    }

    *out_obj = obj;
    *out_rb  = rb;

    printf("[XDP-LOAD] attached '%s' (ifindex=%d, %s), port %u\n",
           iface_name, out_att->ifindex, mode_label, kv_port);
    return 0;
}
