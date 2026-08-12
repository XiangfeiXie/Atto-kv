/* xdp_reasm.c — TCP reassembly + RESP extraction
 *
 * Single-threaded, lock-free architecture.
 *
 * Data pipeline:
 *   XDP ringbuf → xdp_reasm_on_event()
 *     → conn lookup/create → seq dedup/ordering
 *     → circular buffer → RESP scan → write-command filter
 *     → on_write callback (returns 0=ok, -1=drop/backpressure)
 */

#define _GNU_SOURCE
#include "xdp_reasm.h"
#include "xdp_common.h"
#include "config.h"

#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ══════════════════════════════════════════════════════════════════════════
 * OOO data-buffer pool (avoids malloc in the hot path)
 * ══════════════════════════════════════════════════════════════════════════
 *
 * Each pre-allocated buffer is XDP_MAX_PAYLOAD bytes.  The pool
 * is a simple stack of pointers.  On ooo_insert we pop a buffer; on free
 * we push it back.  When the pool is exhausted we fall back to malloc.
 *
 * Merging: when two pool-backed segments are merged, we can't realloc()
 * the pool buffer (it's part of a larger block).  Instead we malloc a
 * fresh heap buffer for the merged result and return the displaced pool
 * buffers to the pool.                                       */

static struct {
    uint8_t *bufs[OOO_DBUF_POOL_SIZE];
    int      count;          /* free buffers available */
} g_dbuf_pool;

/* Try to get a buffer from the pool; returns NULL if pool is empty */
static inline uint8_t *dbuf_pool_get(void) {
    if (likely(g_dbuf_pool.count > 0))
        return g_dbuf_pool.bufs[--g_dbuf_pool.count];
    return NULL;
}

/* Return a buffer to the pool */
static inline void dbuf_pool_put(uint8_t *buf) {
    if (likely(g_dbuf_pool.count < OOO_DBUF_POOL_SIZE))
        g_dbuf_pool.bufs[g_dbuf_pool.count++] = buf;
    else
        free(buf);  /* pool full (shouldn't happen) — fallback free */
}

/* ══════════════════════════════════════════════════════════════════════════
 * Circular buffer
 * ══════════════════════════════════════════════════════════════════════════ */

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
    uint32_t pos   = cb->rd & cb->mask;
    uint32_t first = len;
    if (unlikely(pos + first > cb->cap))
        first = cb->cap - pos;
    memcpy(dst, cb->data + pos, first);
    if (unlikely(len > first))
        memcpy(dst + first, cb->data, len - first);
}

/* Expand circular buffer to new_cap (must be power of 2, > cap).
 * Linearizes data so rd=0, wr=used. */
static int circ_expand(circ_buf_t *cb, uint32_t new_cap) {
    uint8_t *nd = malloc(new_cap);
    if (unlikely(!nd)) return -1;
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

/* Ensure at least `need` bytes free. Expands if necessary.
 * Returns 0 on success, -1 if expansion fails or cap exceeded. */
static int circ_reserve(circ_buf_t *cb, uint32_t need) {
    /* Initial allocation */
    if (unlikely(!cb->data)) {
        uint32_t cap = CMD_BUF_CAP_MIN;
        while (cap - 1 < need) {
            if (unlikely(cap >= CMD_BUF_CAP_MAX)) return -1;
            cap *= 2;
        }
        cb->data = malloc(cap);
        if (unlikely(!cb->data)) return -1;
        cb->cap  = cap;
        cb->mask = cap - 1;
        cb->rd   = 0;
        cb->wr   = 0;
        return 0;
    }
    /* Already allocated — check free space */
    if (likely(circ_free(cb) >= need))
        return 0;
    uint32_t new_cap = cb->cap;
    while (new_cap - circ_used(cb) - 1 < need) {
        if (unlikely(new_cap >= CMD_BUF_CAP_MAX))
            return -1;
        new_cap *= 2;
    }
    return circ_expand(cb, new_cap);
}

/* Append len bytes to the circular buffer.
 * Returns 0 on success, -1 if buffer full and cannot expand. */
static int circ_write(circ_buf_t *cb, const uint8_t *src, uint32_t len) {
    if (len == 0) return 0;
    if (unlikely(circ_reserve(cb, len) != 0))
        return -1;
    uint32_t pos   = cb->wr & cb->mask;
    uint32_t first = len;
    if (unlikely(pos + first > cb->cap))
        first = cb->cap - pos;
    memcpy(cb->data + pos, src, first);
    if (unlikely(len > first))
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

/* ══════════════════════════════════════════════════════════════════════════
 * OOO segment list
 * ══════════════════════════════════════════════════════════════════════════ */

typedef struct ooo_seg {
    uint32_t        seq;
    uint32_t        len;
    uint8_t        *data;       /* pool buffer or heap buffer */
    struct ooo_seg *next;
    int             from_pool;  /* 1 → data came from dbuf pool */
} ooo_seg_t;

/* ══════════════════════════════════════════════════════════════════════════
 * RESP parser (operates on circular buffer)
 * ══════════════════════════════════════════════════════════════════════════ */

/* Returns: >0 = complete command length, 0 = incomplete, -1 = protocol error */
static int resp_scan(const circ_buf_t *cb) {
    uint32_t avail = circ_used(cb);
    if (unlikely(avail < 4)) return 0;
    if (unlikely(circ_peek(cb, 0) != '*')) return -1;

    uint32_t i = 1;
    int argc = 0;
    while (likely(i < avail)) {
        uint8_t c = circ_peek(cb, i);
        if (likely(c >= '0' && c <= '9')) {
            argc = argc * 10 + (c - '0');
            if (unlikely(argc > 64)) return -1;
            i++;
        } else {
            break;
        }
    }
    if (unlikely(i == 1)) return -1;
    if (unlikely(i + 1 >= avail)) return 0;
    if (unlikely(circ_peek(cb, i) != '\r' || circ_peek(cb, i + 1) != '\n'))
        return -1;
    i += 2;

    for (int arg = 0; arg < argc; arg++) {
        if (unlikely(i >= avail)) return 0;
        if (unlikely(circ_peek(cb, i) != '$')) return -1;
        i++;

        int arg_len = 0;
        while (likely(i < avail)) {
            uint8_t c = circ_peek(cb, i);
            if (likely(c >= '0' && c <= '9')) {
                arg_len = arg_len * 10 + (c - '0');
                if (unlikely(arg_len > RESP_CMD_MAX_LEN)) return -1;
                i++;
            } else {
                break;
            }
        }
        if (unlikely(i + 1 >= avail)) return 0;
        if (unlikely(circ_peek(cb, i) != '\r' ||
                     circ_peek(cb, i + 1) != '\n')) return -1;
        i += 2;

        if (unlikely(i + (uint32_t)arg_len + 2 > avail)) return 0;
        i += (uint32_t)arg_len + 2;
    }

    return (int)i;
}

/* ── Write-command filter ────────────────────────────────────────────────
 *
 * Matches 13 Redis write-command families.  Uses a switch on the first
 * two uppercase characters; the compiler emits a dense jump table.
 *
 * Commands recognized:
 *   SET DEL DECR INCR LPUSH RPUSH SADD SREM ZADD ZREM
 *   HSET HDEL APPEND MSET FLUSH(DB|ALL)                         */

static int is_write_command_buf(const circ_buf_t *cb, uint32_t cmd_len) {
    /* Need at least: *1\r\n$3\r\nXXX — i.e. a 3-char command name */
    if (unlikely(cmd_len < 12)) return 0;

    /* Skip *N\r\n */
    uint32_t i = 1;
    while (likely(i < cmd_len)) {
        uint8_t c = circ_peek(cb, i);
        if (likely(c >= '0' && c <= '9')) i++;
        else break;
    }
    if (unlikely(i + 1 >= cmd_len ||
                 circ_peek(cb, i) != '\r' ||
                 circ_peek(cb, i + 1) != '\n')) return 0;
    i += 2;

    /* Skip $L\r\n to reach command name */
    if (unlikely(i >= cmd_len || circ_peek(cb, i) != '$')) return 0;
    i++;
    while (likely(i < cmd_len)) {
        uint8_t c = circ_peek(cb, i);
        if (likely(c >= '0' && c <= '9')) i++;
        else break;
    }
    if (unlikely(i + 1 >= cmd_len ||
                 circ_peek(cb, i) != '\r' ||
                 circ_peek(cb, i + 1) != '\n')) return 0;
    i += 2;

    if (unlikely(i + 2 >= cmd_len)) return 0;

    /* Uppercase first 3 chars of command name */
    uint8_t c0 = circ_peek(cb, i);
    uint8_t c1 = circ_peek(cb, i + 1);
    uint8_t c2 = circ_peek(cb, i + 2);
    if (c0 >= 'a') c0 -= 32;
    if (c1 >= 'a') c1 -= 32;
    if (c2 >= 'a') c2 -= 32;

    /* Two-char key for the jump table */
    switch (((uint16_t)c0 << 8) | c1) {
    case ('S'<<8)|'E': return c2 == 'T';              /* SET */
    case ('D'<<8)|'E': return c2 == 'L' || c2 == 'C'; /* DEL, DECR */
    case ('L'<<8)|'P': return 1;                       /* LPUSH */
    case ('R'<<8)|'P': return 1;                       /* RPUSH */
    case ('H'<<8)|'S': return 1;                       /* HSET */
    case ('H'<<8)|'D': return 1;                       /* HDEL */
    case ('S'<<8)|'A': return 1;                       /* SADD */
    case ('S'<<8)|'R': return 1;                       /* SREM */
    case ('Z'<<8)|'A': return 1;                       /* ZADD */
    case ('Z'<<8)|'R': return 1;                       /* ZREM */
    case ('I'<<8)|'N': return c2 == 'C';              /* INCR */
    case ('A'<<8)|'P': return c2 == 'P';              /* APPEND */
    case ('M'<<8)|'S': return c2 == 'E';              /* MSET */
    case ('F'<<8)|'L': return c2 == 'U';              /* FLUSH */
    default:          return 0;
    }
}

/* ══════════════════════════════════════════════════════════════════════════
 * Connection
 * ══════════════════════════════════════════════════════════════════════════ */

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

/* ── Global state ────────────────────────────────────────────────────────
 *
 * Connections are stored in a flat array of chained hash buckets.
 * No bucket wrapper struct — the mutex was removed in Plan A.
 *
 * A one-entry lookup cache (g_lk_conn / g_lk_key) exploits the fact that
 * consecutive ringbuf events almost always belong to the same TCP stream
 * (the kernel delivers batches).                             */

static struct {
    xdp_conn_t          *buckets[CONN_HASH_SIZE];
    int                  count;
    xdp_reasm_write_cb   on_write;
    struct xdp_reasm_stats stats;

    /* One-entry lookup cache */
    xdp_conn_t          *lk_conn;
    uint64_t             lk_key;   /* (uint64_t)sport << 32 | saddr */
} g_xdp;

/* ── Helpers ───────────────────────────────────────────────────────────── */

static inline uint32_t conn_hash_idx(uint32_t saddr, uint16_t sport) {
    uint32_t h = saddr ^ ((uint32_t)sport * 0x9e3779b9u);
    return h & (CONN_HASH_SIZE - 1);
}

static inline uint64_t conn_key(uint32_t saddr, uint16_t sport) {
    return ((uint64_t)sport << 32) | saddr;
}

static inline uint32_t seq_to_host(uint32_t net_seq) {
    return ntohl(net_seq);
}

/* Free a connection's resources (OOO segments, circular buffer, conn struct).
 * Caller must have already unlinked the conn from its hash chain. */
static void conn_free_resources(xdp_conn_t *c) {
    ooo_seg_t *seg = c->ooo_head;
    while (seg) {
        ooo_seg_t *nx = seg->next;
        if (seg->from_pool)
            dbuf_pool_put(seg->data);
        else
            free(seg->data);
        free(seg);
        seg = nx;
    }
    free(c->buf.data);
    free(c);
}

/* Invalidate the one-entry lookup cache if it points to a freed connection. */
static void conn_cache_inval(xdp_conn_t *victim) {
    if (g_xdp.lk_conn == victim)
        g_xdp.lk_conn = NULL;
}

/* Create a new connection. On failure (count exceeded), evicts an idle or
 * stale connection from the target bucket. Returns NULL if no slot. */
static xdp_conn_t *conn_create(uint32_t idx,
                               uint32_t saddr, uint16_t sport) {
    xdp_conn_t **head = &g_xdp.buckets[idx];

    if (unlikely(g_xdp.count >= CONN_MAX)) {
        /* Try to evict an idle-closed connection, or the stalest */
        xdp_conn_t *stale = NULL, *sprev = NULL;
        for (xdp_conn_t *c = *head, *p = NULL; likely(c);
             p = c, c = c->next) {
            if (c->closing && c->buf.rd == c->buf.wr && !c->ooo_head) {
                stale = c; sprev = p; break;
            }
            if (!stale || c->last_active < stale->last_active) {
                stale = c; sprev = p;
            }
        }
        if (stale) {
            if (sprev) sprev->next = stale->next;
            else       *head       = stale->next;
            conn_cache_inval(stale);
            conn_free_resources(stale);
            g_xdp.count--;
        } else {
            return NULL;
        }
    }

    xdp_conn_t *c = calloc(1, sizeof(*c));
    if (unlikely(!c)) return NULL;

    c->saddr       = saddr;
    c->sport       = sport;
    c->last_active = time(NULL);
    c->next        = *head;
    *head          = c;
    g_xdp.count++;
    return c;
}

/* ══════════════════════════════════════════════════════════════════════════
 * OOO segment management
 * ══════════════════════════════════════════════════════════════════════════ */

/* Allocate a new OOO segment with a copy of [data, data+len).
 * Uses the pre-allocated dbuf pool in the common case, falls back
 * to malloc when the pool is exhausted. */
static ooo_seg_t *ooo_seg_alloc(const uint8_t *data, uint32_t len) {
    ooo_seg_t *seg = malloc(sizeof(*seg));
    if (unlikely(!seg)) return NULL;

    seg->len       = len;
    seg->next      = NULL;
    seg->from_pool = 0;

    uint8_t *buf = dbuf_pool_get();
    if (buf) {
        seg->data      = buf;
        seg->from_pool = 1;
    } else {
        seg->data = malloc(len);
        if (unlikely(!seg->data)) {
            free(seg);
            return NULL;
        }
    }
    memcpy(seg->data, data, len);
    return seg;
}

/* Free an OOO segment, returning its data buffer to the pool if applicable. */
static void ooo_seg_free(ooo_seg_t *seg) {
    if (seg->from_pool)
        dbuf_pool_put(seg->data);
    else
        free(seg->data);
    free(seg);
}

/* Insert segment into OOO list sorted by seq, merging adjacent/overlapping. */
static void ooo_insert(xdp_conn_t *c, uint32_t seq,
                       const uint8_t *data, uint32_t len) {
    if (unlikely(c->ooo_count >= OOO_MAX_SEGMENTS)) {
        /* Too many OOO segments — connection likely corrupt, reset */
        ooo_seg_t *seg = c->ooo_head;
        while (seg) {
            ooo_seg_t *nx = seg->next;
            ooo_seg_free(seg);
            seg = nx;
        }
        c->ooo_head  = NULL;
        c->ooo_count = 0;
        return;
    }

    ooo_seg_t *ns = ooo_seg_alloc(data, len);
    if (unlikely(!ns)) return;

    ns->seq = seq;
    c->ooo_count++;
    g_xdp.stats.ooo_segments++;

    /* Find insertion point */
    ooo_seg_t *prev = NULL, *cur = c->ooo_head;
    while (cur && cur->seq < ns->seq) {
        prev = cur;
        cur  = cur->next;
    }

    /* ── Merge with predecessor ─────────────────────────────────────── */
    if (prev) {
        uint32_t prev_end = prev->seq + prev->len;
        if (prev_end >= ns->seq) {
            if (ns->seq + ns->len > prev_end) {
                uint32_t extend  = ns->seq + ns->len - prev_end;
                uint32_t new_len = prev->len + extend;

                if (prev->from_pool) {
                    /* Can't realloc pool buffer — allocate heap copy */
                    uint8_t *nd = malloc(new_len);
                    if (likely(nd)) {
                        memcpy(nd, prev->data, prev->len);
                        memcpy(nd + prev->len,
                               ns->data + (prev_end - ns->seq), extend);
                        dbuf_pool_put(prev->data);
                        prev->data      = nd;
                        prev->from_pool = 0;
                        prev->len       = new_len;
                    }
                } else {
                    uint8_t *nd = realloc(prev->data, new_len);
                    if (likely(nd)) {
                        prev->data = nd;
                        memcpy(prev->data + prev->len,
                               ns->data + (prev_end - ns->seq), extend);
                        prev->len = new_len;
                    }
                }
            }
            ooo_seg_free(ns);
            g_xdp.stats.ooo_segments--;
            c->ooo_count--;
            ns = NULL;
        }
    }

    if (!ns) return;

    /* ── Insert into list ───────────────────────────────────────────── */
    ns->next = cur;
    if (prev) prev->next = ns;
    else      c->ooo_head = ns;
    prev = ns;

    /* ── Merge forward ──────────────────────────────────────────────── */
    while (cur) {
        uint32_t prev_end = prev->seq + prev->len;
        if (unlikely(prev_end < cur->seq))
            break;

        if (cur->seq + cur->len > prev_end) {
            uint32_t extend  = cur->seq + cur->len - prev_end;
            uint32_t new_len = prev->len + extend;

            if (prev->from_pool) {
                uint8_t *nd = malloc(new_len);
                if (likely(nd)) {
                    memcpy(nd, prev->data, prev->len);
                    memcpy(nd + prev->len,
                           cur->data + (prev_end - cur->seq), extend);
                    dbuf_pool_put(prev->data);
                    prev->data      = nd;
                    prev->from_pool = 0;
                    prev->len       = new_len;
                }
            } else {
                uint8_t *nd = realloc(prev->data, new_len);
                if (likely(nd)) {
                    prev->data = nd;
                    memcpy(prev->data + prev->len,
                           cur->data + (prev_end - cur->seq), extend);
                    prev->len = new_len;
                }
            }
        }

        prev->next = cur->next;
        ooo_seg_free(cur);
        g_xdp.stats.ooo_segments--;
        c->ooo_count--;
        cur = prev->next;
    }
}

/* Drain OOO segments that are now contiguous with rcv_nxt */
static void consume_ordered(xdp_conn_t *c,
                            const uint8_t *data, uint32_t len);

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
        ooo_seg_free(seg);
        g_xdp.stats.ooo_segments--;
        c->ooo_count--;
    }
}

/* ══════════════════════════════════════════════════════════════════════════
 * Ordered data consumer
 * ══════════════════════════════════════════════════════════════════════════ */

static void consume_ordered(xdp_conn_t *c,
                            const uint8_t *data, uint32_t len) {
    if (len == 0) return;

    g_xdp.stats.bytes_ordered += len;

    if (unlikely(circ_write(&c->buf, data, len) != 0)) {
        /* Buffer overflow — reset (likely garbage data) */
        g_xdp.stats.buf_resets++;
        c->buf.rd = c->buf.wr = 0;
        circ_shrink(&c->buf);
        return;
    }

    /* Extract complete RESP commands.
     * Stack buffer handles typical commands; oversize
     * commands (rare) fall back to a heap allocation. */
    uint8_t scratch[4096];

    for (;;) {
        int cmd_len = resp_scan(&c->buf);
        if (cmd_len <= 0) {
            if (unlikely(cmd_len < 0)) {
                g_xdp.stats.parse_errors++;
                if (circ_used(&c->buf) > CMD_BUF_CAP_MAX) {
                    /* Stale/unparseable data, reset */
                    c->buf.rd = c->buf.wr = 0;
                    circ_shrink(&c->buf);
                }
            }
            break;
        }

        g_xdp.stats.cmds_extracted++;

        if (is_write_command_buf(&c->buf, (uint32_t)cmd_len)) {
            g_xdp.stats.cmds_written++;
            if (g_xdp.on_write) {
                int rc;
                if (likely((uint32_t)cmd_len <= sizeof(scratch))) {
                    circ_copyout(&c->buf, scratch, (uint32_t)cmd_len);
                    rc = g_xdp.on_write(scratch, (uint32_t)cmd_len);
                } else {
                    uint8_t *tmp = malloc((uint32_t)cmd_len);
                    if (likely(tmp)) {
                        circ_copyout(&c->buf, tmp, (uint32_t)cmd_len);
                        rc = g_xdp.on_write(tmp, (uint32_t)cmd_len);
                        free(tmp);
                    } else {
                        rc = -1;
                    }
                }
                if (unlikely(rc < 0))
                    g_xdp.stats.send_dropped++;
            }
        }

        circ_consume(&c->buf, (uint32_t)cmd_len);
    }

    circ_shrink(&c->buf);
}

/* ══════════════════════════════════════════════════════════════════════════
 * Packet processing (seq logic)
 * ══════════════════════════════════════════════════════════════════════════ */

static void process_packet(xdp_conn_t *c,
                           uint32_t seq_h, const uint8_t *data, uint32_t len) {
    if (len == 0) return;

    uint32_t seg_end = seq_h + len;

    /* Completely duplicate (fully before rcv_nxt) */
    if (unlikely(seg_end <= c->rcv_nxt)) {
        g_xdp.stats.dup_discarded++;
        return;
    }

    /* Partial overlap (starts before rcv_nxt but extends past it) */
    if (unlikely(seq_h < c->rcv_nxt)) {
        uint32_t skip = c->rcv_nxt - seq_h;
        data  += skip;
        len   -= skip;
        seq_h  = c->rcv_nxt;
    }

    /* In-order: most common case */
    if (likely(seq_h == c->rcv_nxt)) {
        consume_ordered(c, data, len);
        c->rcv_nxt += len;
        ooo_drain(c);
        return;
    }

    /* Out-of-order */
    ooo_insert(c, seq_h, data, len);
}

/* ══════════════════════════════════════════════════════════════════════════
 * Ringbuf callback
 * ══════════════════════════════════════════════════════════════════════════ */

int xdp_reasm_on_event(void *ctx, void *data, size_t data_sz) {
    (void)ctx;
    (void)data_sz;

    struct xdp_repl_event *e = (struct xdp_repl_event *)data;
    g_xdp.stats.events_total++;

    /* ── Connection lookup (with one-entry cache) ──────────────────────
     * Consecutive ringbuf events almost always belong to the same TCP
     * stream — the kernel delivers batches.  A single comparison saves
     * a hash computation and chain walk on every cache hit.          */
    uint64_t    key = conn_key(e->saddr, e->sport);
    xdp_conn_t *c;

    if (likely(g_xdp.lk_conn && g_xdp.lk_key == key)) {
        c = g_xdp.lk_conn;                    /* cache hit */
    } else {
        uint32_t idx = conn_hash_idx(e->saddr, e->sport);
        for (c = g_xdp.buckets[idx]; c; c = c->next)
            if (c->saddr == e->saddr && c->sport == e->sport)
                break;
        g_xdp.lk_conn = c;
        g_xdp.lk_key  = key;
    }

    /* ── Handle SYN ────────────────────────────────────────────────── */
    if (unlikely(e->flags & XDP_FLAG_SYN)) {
        g_xdp.stats.events_syn++;
        if (!c) {
            uint32_t idx = conn_hash_idx(e->saddr, e->sport);
            c = conn_create(idx, e->saddr, e->sport);
            if (unlikely(!c)) return 0;
            g_xdp.lk_conn = c;
            g_xdp.lk_key  = key;
        }
        if (!c->isn_known) {
            c->isn       = seq_to_host(e->seq);
            c->rcv_nxt   = c->isn + 1;
            c->isn_known = 1;
            c->closing   = 0;
        }
        c->last_active = time(NULL);
        if (e->data_len > 0) {
            process_packet(c, seq_to_host(e->seq) + 1,
                           e->data, e->data_len);
        }
        return 0;
    }

    /* Connection must exist for non-SYN */
    if (unlikely(!c)) {
        uint32_t idx = conn_hash_idx(e->saddr, e->sport);
        c = conn_create(idx, e->saddr, e->sport);
        if (unlikely(!c)) return 0;
        g_xdp.lk_conn = c;
        g_xdp.lk_key  = key;
        /* Lame connection: estimate ISN = seq - 1 */
        c->isn       = seq_to_host(e->seq) - 1;
        c->rcv_nxt   = c->isn + 1;
        c->isn_known = 0;
    }

    /* ── Handle FIN/RST ────────────────────────────────────────────── */
    if (unlikely(e->flags & (XDP_FLAG_FIN | XDP_FLAG_RST))) {
        g_xdp.stats.events_fin_rst++;
        c->closing = 1;
        uint32_t data_seq = seq_to_host(e->seq);
        if (e->data_len > 0)
            process_packet(c, data_seq, e->data, e->data_len);
        if (e->flags & XDP_FLAG_FIN) {
            uint8_t fin_byte = 0;
            process_packet(c, data_seq + e->data_len, &fin_byte, 1);
        }
        c->last_active = time(NULL);
        return 0;
    }

    /* ── Handle data ───────────────────────────────────────────────── */
    if (likely(e->data_len > 0)) {
        g_xdp.stats.events_data++;
        c->last_active = time(NULL);
        process_packet(c, seq_to_host(e->seq), e->data, e->data_len);
    }

    /* Count fragment events */
    if (unlikely(e->flags & XDP_FLAG_FRAG))
        g_xdp.stats.fragment_events++;

    return 0;
}

/* ══════════════════════════════════════════════════════════════════════════
 * Garbage collection
 * ══════════════════════════════════════════════════════════════════════════ */

void xdp_reasm_gc(void) {
    time_t now = time(NULL);

    for (uint32_t i = 0; i < CONN_HASH_SIZE; i++) {
        xdp_conn_t *prev = NULL, *c = g_xdp.buckets[i];
        while (c) {
            int gc = (now - c->last_active > CONN_GC_TIMEOUT) ||
                     (c->closing && !c->ooo_head &&
                      circ_used(&c->buf) == 0);

            if (unlikely(gc)) {
                xdp_conn_t *victim = c;
                if (prev) prev->next = c->next;
                else      g_xdp.buckets[i] = c->next;
                c = c->next;

                conn_cache_inval(victim);

                /* Return OOO data buffers to pool */
                ooo_seg_t *seg = victim->ooo_head;
                while (seg) {
                    ooo_seg_t *nx = seg->next;
                    ooo_seg_free(seg);
                    g_xdp.stats.ooo_segments--;
                    seg = nx;
                }
                free(victim->buf.data);
                free(victim);
                g_xdp.count--;
            } else {
                prev = c;
                c    = c->next;
            }
        }
    }

    g_xdp.stats.conns_active = g_xdp.count;
}

/* ══════════════════════════════════════════════════════════════════════════
 * Public API
 * ══════════════════════════════════════════════════════════════════════════ */

int xdp_reasm_init(xdp_reasm_write_cb on_write) {
    memset(&g_xdp, 0, sizeof(g_xdp));
    g_xdp.on_write = on_write;

    /* Pre-allocate OOO data buffer pool */
    g_dbuf_pool.count = 0;
    for (int i = 0; i < OOO_DBUF_POOL_SIZE; i++) {
        g_dbuf_pool.bufs[i] = malloc(XDP_MAX_PAYLOAD);
        if (g_dbuf_pool.bufs[i])
            g_dbuf_pool.count++;
    }

    printf("[XDP-REASM] init OK: %d buckets, max %d conns, "
           "ooo dbuf pool %d/%d (single-threaded)\n",
           CONN_HASH_SIZE, CONN_MAX,
           g_dbuf_pool.count, OOO_DBUF_POOL_SIZE);
    return 0;
}

void xdp_reasm_shutdown(void) {
    for (uint32_t i = 0; i < CONN_HASH_SIZE; i++) {
        xdp_conn_t *c = g_xdp.buckets[i];
        while (c) {
            xdp_conn_t *next = c->next;
            ooo_seg_t  *seg  = c->ooo_head;
            while (seg) {
                ooo_seg_t *nx = seg->next;
                ooo_seg_free(seg);
                seg = nx;
            }
            free(c->buf.data);
            free(c);
            c = next;
        }
        g_xdp.buckets[i] = NULL;
    }

    g_xdp.count = 0;

    /* Free OOO data buffer pool */
    for (int i = 0; i < g_dbuf_pool.count; i++)
        free(g_dbuf_pool.bufs[i]);
    g_dbuf_pool.count = 0;

    printf("[XDP-REASM] shutdown complete\n");
}

void xdp_reasm_get_stats(struct xdp_reasm_stats *st) {
    memcpy(st, &g_xdp.stats, sizeof(*st));
    st->conns_active = g_xdp.count;
}
