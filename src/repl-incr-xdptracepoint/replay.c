/* replay.c — verbatim replay of reconstructed streams to the slave
 *
 * Single-threaded, lock-free, epoll-driven.  One replay_conn per source
 * connection.  A source connection's FIN does not immediately tear down the
 * slave connection: the replay_conn is marked source_done and allowed to
 * finish connecting + flush its buffered bytes before it closes, so the
 * final command of a short-lived redis-cli connection is never lost.
 */

#define _GNU_SOURCE
#include "replay.h"
#include "config.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

struct replay_conn {
    stream_conn_t *sc;          /* backpointer (NULL once source is done) */
    int      fd;                /* slave socket, -1 = not connected yet */
    int      connecting;        /* async connect (EINPROGRESS) in flight */
    int      source_done;       /* source stream saw FIN/RST/corrupt/GC */
    int      registered;        /* fd is in the epoll set */
    uint64_t reconnect_after_ms;
    uint64_t drain_deadline_ms; /* source_done conns must flush by this time */

    uint8_t *wbuf;              /* unsent bytes queued to the slave */
    uint32_t wlen;
    uint32_t wcap;

    uint64_t sent_total;        /* bytes written to the slave */
    uint64_t dropped_total;     /* bytes dropped (overflow / no slave) */

    replay_conn_t *next;        /* active list */
};

static int            g_epfd = -1;
static replay_conn_t *g_list = NULL;

static inline uint64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}

/* ── list helpers ──────────────────────────────────────────────────────── */

static void replay_conn_unlink(replay_conn_t *rc) {
    replay_conn_t **pp = &g_list;
    while (*pp && *pp != rc)
        pp = &(*pp)->next;
    if (*pp)
        *pp = rc->next;
}

/* Detach any backpointer, close fd, unlink, free.  Safe to call on a conn
 * whose sc backpointer was already cleared. */
static void replay_close(replay_conn_t *rc) {
    if (rc->sc) {
        rc->sc->user = NULL;
        rc->sc = NULL;
    }
    if (rc->fd >= 0) {
        if (rc->registered)
            epoll_ctl(g_epfd, EPOLL_CTL_DEL, rc->fd, NULL);
        close(rc->fd);
        rc->fd = -1;
        rc->registered = 0;
    }
    replay_conn_unlink(rc);
    free(rc->wbuf);
    free(rc);
}

/* Close after marking the source corrupt (mid-stream slave failure). */
static void replay_abort(replay_conn_t *rc) {
    if (rc->sc) {
        rc->sc->corrupt = 1;
        rc->sc->user = NULL;
        rc->sc = NULL;
    }
    replay_close(rc);
}

/* ── buffer ────────────────────────────────────────────────────────────── */

static int replay_buf_append(replay_conn_t *rc, const uint8_t *data, uint32_t len) {
    uint32_t need = rc->wlen + len;
    if (need > rc->wcap) {
        uint32_t ncap = rc->wcap ? rc->wcap : REPLAY_BUF_INIT;
        while (ncap < need)
            ncap *= 2;
        if (ncap > REPLAY_BUF_MAX)
            ncap = REPLAY_BUF_MAX;
        if (ncap < need)
            return -1;              /* exceeds hard cap → overflow */
        uint8_t *nb = realloc(rc->wbuf, ncap);
        if (!nb)
            return -1;
        rc->wbuf = nb;
        rc->wcap = ncap;
    }
    memcpy(rc->wbuf + rc->wlen, data, len);
    rc->wlen = need;
    return 0;
}

/* ── epoll interest ────────────────────────────────────────────────────── */

static void replay_update_interest(replay_conn_t *rc) {
    if (rc->fd < 0 || g_epfd < 0)
        return;

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.data.ptr = rc;

    if (rc->connecting) {
        ev.events = EPOLLOUT | EPOLLERR | EPOLLHUP;
    } else {
        ev.events = EPOLLIN | EPOLLERR | EPOLLHUP;
        if (rc->wlen > 0)
            ev.events |= EPOLLOUT;
    }

    int op = rc->registered ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
    if (epoll_ctl(g_epfd, op, rc->fd, &ev) == 0)
        rc->registered = 1;
}

/* ── send / recv ───────────────────────────────────────────────────────── */

/* Drain wbuf non-blocking.  Returns -1 on fatal error (peer closed / real
 * error).  Never frees the conn. */
static int replay_flush(replay_conn_t *rc) {
    if (rc->fd < 0 || rc->connecting)
        return 0;

    while (rc->wlen > 0) {
        ssize_t n = send(rc->fd, rc->wbuf, rc->wlen,
                         MSG_NOSIGNAL | MSG_DONTWAIT);
        if (n > 0) {
            memmove(rc->wbuf, rc->wbuf + n, rc->wlen - n);
            rc->wlen -= n;
            rc->sent_total += (uint64_t)n;
            continue;
        }
        if (n == 0)
            return -1;              /* peer closed */
        if (errno == EINTR)
            continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            break;                  /* socket buffer full — retry next EPOLLOUT */
        return -1;
    }

    /* Fully drained — release an oversized buffer so we don't pin memory. */
    if (rc->wlen == 0 && rc->wcap > 65536) {
        free(rc->wbuf);
        rc->wbuf = NULL;
        rc->wcap = 0;
    }
    return 0;
}

/* Read + discard slave replies.  Returns -1 on EOF/error, 0 otherwise. */
static int replay_drain(replay_conn_t *rc) {
    char tmp[4096];
    for (;;) {
        ssize_t n = recv(rc->fd, tmp, sizeof(tmp), MSG_DONTWAIT);
        if (n > 0)
            continue;
        if (n == 0)
            return -1;              /* slave closed */
        if (errno == EINTR)
            continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return 0;
        return -1;
    }
}

/* ── connect ───────────────────────────────────────────────────────────── */

static void replay_connect(replay_conn_t *rc) {
    uint64_t now = now_ms();
    if (rc->fd >= 0 || rc->source_done)
        return;
    if (now < rc->reconnect_after_ms)
        return;

    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        rc->reconnect_after_ms = now + REPLAY_RECONNECT_MS;
        return;
    }

    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port   = htons((uint16_t)SLAVE_PORT),
    };
    if (inet_pton(AF_INET, SLAVE_IP, &addr.sin_addr) != 1) {
        close(fd);
        rc->reconnect_after_ms = now + REPLAY_RECONNECT_MS;
        return;
    }

    int r = connect(fd, (struct sockaddr *)&addr, sizeof(addr));
    if (r == 0) {
        rc->fd = fd;
        rc->connecting = 0;
        printf("[REPLAY] connected to %s:%u\n", SLAVE_IP, (unsigned)SLAVE_PORT);
        replay_update_interest(rc);
        return;
    }
    if (errno == EINPROGRESS) {
        rc->fd = fd;
        rc->connecting = 1;
        replay_update_interest(rc);
        return;
    }

    close(fd);
    rc->fd = -1;
    rc->reconnect_after_ms = now + REPLAY_RECONNECT_MS;
}

/* ── conn create ───────────────────────────────────────────────────────── */

static replay_conn_t *replay_conn_create(stream_conn_t *sc) {
    replay_conn_t *rc = calloc(1, sizeof(*rc));
    if (!rc)
        return NULL;
    rc->sc = sc;
    rc->fd = -1;
    rc->wcap = REPLAY_BUF_INIT;
    rc->wbuf = malloc(REPLAY_BUF_INIT);
    if (!rc->wbuf) {
        free(rc);
        return NULL;
    }
    rc->next = g_list;
    g_list = rc;
    sc->user = rc;
    return rc;
}

/* ── public: stream_ops callbacks ──────────────────────────────────────── */

void replay_on_data(stream_conn_t *sc, const uint8_t *data, uint32_t len) {
    replay_conn_t *rc = sc->user;
    if (!rc) {
        rc = replay_conn_create(sc);
        if (!rc) {
            sc->corrupt = 1;        /* cannot allocate replay state */
            return;
        }
    }
    if (rc->source_done)
        return;

    if (replay_buf_append(rc, data, len) != 0) {
        /* Buffer overflow: slave too slow or unreachable.  Drop the source
         * connection — continuing would replay a truncated stream. */
        rc->dropped_total += len;
        rc->sc = NULL;
        sc->user = NULL;
        sc->corrupt = 1;
        replay_close(rc);
        return;
    }

    if (rc->fd < 0)
        replay_connect(rc);
    else
        replay_update_interest(rc);   /* arm EPOLLOUT for the new bytes */
}

void replay_on_close(stream_conn_t *sc) {
    replay_conn_t *rc = sc->user;
    if (!rc)
        return;
    sc->user = NULL;
    rc->sc = NULL;
    rc->source_done = 1;
    rc->drain_deadline_ms = now_ms() + REPLAY_DRAIN_MS;

    /* Nothing left to deliver — close now. */
    if (rc->wlen == 0) {
        replay_close(rc);
        return;
    }

    /* Buffered bytes remain: deliver them (connect then flush) via the
     * event loop, which closes the conn once wlen drains to zero. */
    if (rc->fd < 0)
        replay_connect(rc);
    else if (!rc->connecting)
        replay_flush(rc);
    /* else: connecting — replay_handle_event finishes the job */
}

/* ── public: epoll event handling ──────────────────────────────────────── */

void replay_handle_event(replay_conn_t *rc, uint32_t events) {
    if (events & (EPOLLERR | EPOLLHUP)) {
        replay_abort(rc);
        return;
    }

    if (rc->connecting) {
        if (!(events & EPOLLOUT))
            return;
        int err = 0;
        socklen_t el = sizeof(err);
        if (getsockopt(rc->fd, SOL_SOCKET, SO_ERROR, &err, &el) != 0 || err != 0) {
            close(rc->fd);
            rc->fd = -1;
            rc->connecting = 0;
            rc->registered = 0;
            if (rc->source_done) {
                replay_close(rc);   /* source gone and we can't connect */
                return;
            }
            rc->reconnect_after_ms = now_ms() + REPLAY_RECONNECT_MS;
            return;
        }
        rc->connecting = 0;
        printf("[REPLAY] connected to %s:%u\n", SLAVE_IP, (unsigned)SLAVE_PORT);
        /* fall through to flush buffered bytes */
    }

    if (events & EPOLLOUT) {
        if (replay_flush(rc) < 0) {
            replay_abort(rc);
            return;
        }
    }
    if (events & EPOLLIN) {
        if (replay_drain(rc) < 0) {
            replay_abort(rc);
            return;
        }
    }

    if (rc->source_done && rc->wlen == 0) {
        replay_close(rc);
        return;
    }
    replay_update_interest(rc);
}

void replay_sync_interest(void) {
    for (replay_conn_t *rc = g_list; rc; rc = rc->next)
        replay_update_interest(rc);
}

void replay_tick(void) {
    uint64_t now = now_ms();
    replay_conn_t *rc = g_list;
    while (rc) {
        replay_conn_t *next = rc->next;
        if (rc->source_done && now >= rc->drain_deadline_ms) {
            replay_close(rc);       /* could not deliver in time */
        } else if (rc->fd < 0 && !rc->source_done &&
                   now >= rc->reconnect_after_ms) {
            replay_connect(rc);     /* retry a failed connect */
        }
        rc = next;
    }
}

/* ── public: lifecycle ─────────────────────────────────────────────────── */

int replay_init(int epfd) {
    g_epfd = epfd;
    printf("[REPLAY] init OK (verbatim replay, 1:1 conns, buf %u..%u)\n",
           REPLAY_BUF_INIT, REPLAY_BUF_MAX);
    return 0;
}

void replay_shutdown(void) {
    replay_conn_t *rc = g_list;
    while (rc) {
        replay_conn_t *next = rc->next;
        if (rc->fd >= 0) {
            replay_flush(rc);       /* best-effort final delivery */
            close(rc->fd);
        }
        free(rc->wbuf);
        free(rc);
        rc = next;
    }
    g_list = NULL;
    printf("[REPLAY] shutdown complete\n");
}

void replay_get_stats(uint64_t *active, uint64_t *sent, uint64_t *dropped) {
    uint64_t a = 0, s = 0, d = 0;
    for (replay_conn_t *rc = g_list; rc; rc = rc->next) {
        a++;
        s += rc->sent_total;
        d += rc->dropped_total;
    }
    if (active)  *active  = a;
    if (sent)    *sent    = s;
    if (dropped) *dropped = d;
}
