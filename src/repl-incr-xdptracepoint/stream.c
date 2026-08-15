/* stream.c — TCP stream reconstruction (hole detection, no OOO buffering)
 *
 * Single-threaded, lock-free.  Data pipeline:
 *   ringbuf event → stream_on_event()
 *     → conn lookup/create → seq dedup/ordering (strict in-order)
 *     → on_data() ordered byte segment → replay layer
 *
 * There is deliberately no out-of-order buffer.  A gap is recorded with a
 * deadline; if it is not filled in time the connection is corrupt (the
 * segment reached master, so the client will never retransmit it).  This is
 * the one case the old OOO machinery could not recover from anyway.
 */

#define _GNU_SOURCE
#include "stream.h"
#include "config.h"

#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
    stream_conn_t  *buckets[CONN_HASH_SIZE];
    int             count;
    stream_ops_t    ops;
    struct stream_stats st;
} stream_globals_t;

static stream_globals_t g;

static inline uint64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}

static inline uint32_t conn_hash_idx(uint32_t saddr, uint16_t sport) {
    uint32_t h = saddr ^ ((uint32_t)sport * 0x9e3779b9u);
    return h & (CONN_HASH_SIZE - 1);
}

static stream_conn_t *conn_lookup(uint32_t saddr, uint16_t sport) {
    uint32_t idx = conn_hash_idx(saddr, sport);
    for (stream_conn_t *c = g.buckets[idx]; c; c = c->next)
        if (c->saddr == saddr && c->sport == sport)
            return c;
    return NULL;
}

static stream_conn_t *conn_create(uint32_t saddr, uint16_t sport, uint64_t now) {
    if (g.count >= CONN_MAX)
        return NULL;
    stream_conn_t *c = calloc(1, sizeof(*c));
    if (!c)
        return NULL;
    c->saddr = saddr;
    c->sport = sport;
    c->last_active_ms = now;
    uint32_t idx = conn_hash_idx(saddr, sport);
    c->next = g.buckets[idx];
    g.buckets[idx] = c;
    g.count++;
    return c;
}

/* Unlink + notify consumer + free. */
static void conn_free(stream_conn_t *c) {
    uint32_t idx = conn_hash_idx(c->saddr, c->sport);
    stream_conn_t **pp = &g.buckets[idx];
    while (*pp && *pp != c)
        pp = &(*pp)->next;
    if (*pp)
        *pp = c->next;
    if (c->corrupt)
        g.st.corrupt++;
    if (g.ops.on_close)
        g.ops.on_close(c);
    free(c);
    g.count--;
}

static void stream_emit(stream_conn_t *c, const uint8_t *data, uint32_t len) {
    g.st.bytes_ordered += len;
    if (g.ops.on_data)
        g.ops.on_data(c, data, len);
}

static void stream_process_data(stream_conn_t *c, uint64_t now,
                                uint32_t seq_h, const uint8_t *data, uint32_t len) {
    if (len == 0 || c->corrupt)
        return;

    uint32_t seg_end = seq_h + len;

    /* Fully duplicate (entirely before rcv_nxt). */
    if (seg_end <= c->rcv_nxt) {
        g.st.dup_discarded += len;
        return;
    }

    /* Partial overlap: trim the front. */
    if (seq_h < c->rcv_nxt) {
        uint32_t skip = c->rcv_nxt - seq_h;
        data  += skip;
        len   -= skip;
        seq_h  = c->rcv_nxt;
        g.st.dup_discarded += skip;
    }

    /* In-order: the common case. */
    if (seq_h == c->rcv_nxt) {
        stream_emit(c, data, len);
        c->rcv_nxt += len;
        c->hole = 0;
        return;
    }

    /* Gap (seq_h > rcv_nxt): record a hole, drop the segment. */
    g.st.holes++;
    if (!c->hole) {
        c->hole = 1;
        c->hole_deadline_ms = now + HOLE_TIMEOUT_MS;
    }
}

void stream_on_event(const struct xdp_repl_event *e) {
    g.st.events_total++;
    uint64_t now = now_ms();

    stream_conn_t *c = conn_lookup(e->saddr, e->sport);

    /* ── SYN: (re)initialize connection ─────────────────────────────── */
    if (e->flags & XDP_FLAG_SYN) {
        g.st.events_syn++;
        if (c)
            conn_free(c);   /* new incarnation of a reused 4-tuple */
        c = conn_create(e->saddr, e->sport, now);
        if (!c)
            return;
        uint32_t seq_h = ntohl(e->seq);
        c->isn = seq_h;
        c->rcv_nxt = seq_h + 1;   /* SYN consumes one sequence number */
        c->isn_known = 1;
        if (e->data_len > 0)      /* TFO data starts at seq + 1 */
            stream_process_data(c, now, seq_h + 1, e->data, e->data_len);
        return;
    }

    /* ── Non-SYN without state: mid-stream observation ──────────────── */
    if (!c) {
        c = conn_create(e->saddr, e->sport, now);
        if (!c)
            return;
        uint32_t seq_h = ntohl(e->seq);
        c->isn = seq_h - 1;
        c->rcv_nxt = seq_h;       /* first observed segment is "in order" */
        c->isn_known = 0;
    }

    if (c->corrupt)
        return;

    c->last_active_ms = now;

    /* ── FIN/RST ────────────────────────────────────────────────────── */
    if (e->flags & (XDP_FLAG_FIN | XDP_FLAG_RST)) {
        g.st.events_fin_rst++;
        uint32_t seq_h = ntohl(e->seq);
        if (e->data_len > 0)
            stream_process_data(c, now, seq_h, e->data, e->data_len);
        if (e->flags & XDP_FLAG_FIN) {
            /* FIN consumes a sequence number but carries no data byte —
             * advance rcv_nxt without emitting anything. */
            uint32_t fin_seq = seq_h + e->data_len;
            if (fin_seq == c->rcv_nxt)
                c->rcv_nxt = fin_seq + 1;
        }
        conn_free(c);
        return;
    }

    /* ── Data ───────────────────────────────────────────────────────── */
    if (e->data_len > 0) {
        g.st.events_data++;
        stream_process_data(c, now, ntohl(e->seq), e->data, e->data_len);
    }

    if (e->flags & XDP_FLAG_FRAG)
        g.st.fragment_events++;
}

void stream_tick(void) {
    uint64_t now = now_ms();
    for (uint32_t i = 0; i < CONN_HASH_SIZE; i++) {
        stream_conn_t *c = g.buckets[i];
        while (c) {
            stream_conn_t *next = c->next;
            int destroy = 0;
            if (c->hole && now >= c->hole_deadline_ms) {
                c->corrupt = 1;   /* hole not filled → observer-side loss */
                destroy = 1;
            } else if (c->corrupt) {
                destroy = 1;
            } else if (now - c->last_active_ms > CONN_GC_TIMEOUT_MS) {
                destroy = 1;      /* idle GC */
            }
            if (destroy)
                conn_free(c);
            c = next;
        }
    }
}

int stream_init(const stream_ops_t *ops) {
    memset(&g, 0, sizeof(g));
    if (ops)
        g.ops = *ops;
    printf("[STREAM] init OK: %d buckets, max %d conns (hole-based, no OOO)\n",
           CONN_HASH_SIZE, CONN_MAX);
    return 0;
}

void stream_shutdown(void) {
    for (uint32_t i = 0; i < CONN_HASH_SIZE; i++) {
        stream_conn_t *c = g.buckets[i];
        while (c) {
            stream_conn_t *next = c->next;
            if (g.ops.on_close)
                g.ops.on_close(c);
            free(c);
            c = next;
        }
        g.buckets[i] = NULL;
    }
    g.count = 0;
    printf("[STREAM] shutdown complete\n");
}

void stream_get_stats(struct stream_stats *st) {
    memcpy(st, &g.st, sizeof(*st));
    st->conns_active = (uint32_t)g.count;
}
