/* kvstore-incr-sync — standalone incremental sync daemon (verbatim replay)
 *
 * Single-threaded epoll pipeline:
 *   XDP BPF (pure observer) → ringbuf → stream.c (in-order reconstruction)
 *          → replay.c (1:1 verbatim replay to slave) → non-blocking send
 *
 * No RESP parsing, no write-command filtering, no OOO buffer.  The client
 * → master byte stream is reconstructed per connection and replayed verbatim
 * to a dedicated slave connection.  Read commands are replayed too (the
 * slave executes them and its replies are drained and discarded).
 *
 * Configuration via config.h macros (edit and recompile):
 *   SLAVE_IP, SLAVE_PORT, KVS_FILTER_PORT, XDP_IFACE
 *   REPLAY_BUF_INIT, REPLAY_BUF_MAX, HOLE_TIMEOUT_MS, CONN_GC_TIMEOUT_MS
 */

#define _GNU_SOURCE
#include "config.h"
#include "xdp_loader.h"   /* xdp_load_bpf, xdp_cleanup, xdp_attach_t */
#include "stream.h"
#include "replay.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <time.h>
#include <unistd.h>

/* epoll data.ptr tags (distinguished from replay_conn_t* heap pointers). */
#define TAG_RINGBUF  ((void *)1)
#define TAG_TIMER    ((void *)2)

static volatile int g_running = 1;

static void sig_handler(int sig) {
    (void)sig;
    g_running = 0;
}

/* ringbuf sample callback — dispatches each variable-size event to the
 * stream layer. */
static int ringbuf_cb(void *ctx, void *data, size_t size) {
    (void)ctx;
    if (size < sizeof(struct xdp_repl_event))
        return 0;
    stream_on_event((const struct xdp_repl_event *)data);
    return 0;
}

/* Startup slave reachability probe.  On loopback a connect to a down slave
 * fails immediately with ECONNREFUSED, so a blocking connect is safe.  The
 * "[REPLAY] connected to" line is the smoke test's liveness signal. */
static void slave_probe(void) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return;

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port   = htons((uint16_t)SLAVE_PORT),
    };
    if (inet_pton(AF_INET, SLAVE_IP, &addr.sin_addr) != 1) {
        close(fd);
        return;
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
        printf("[REPLAY] connected to %s:%u\n",
               SLAVE_IP, (unsigned)SLAVE_PORT);
    } else {
        printf("[REPLAY] slave %s:%u not reachable yet (%s)\n",
               SLAVE_IP, (unsigned)SLAVE_PORT, strerror(errno));
    }
    close(fd);
}

static void print_stats(void) {
    struct stream_stats st;
    stream_get_stats(&st);
    uint64_t active = 0, sent = 0, dropped = 0;
    replay_get_stats(&active, &sent, &dropped);

    printf("[STATS] events=%lu written=%lu ooo=%lu send_drop=%lu "
           "conns=%u holes=%lu dup=%lu corrupt=%lu\n",
           (unsigned long)st.events_total,
           (unsigned long)sent,
           (unsigned long)st.holes,
           (unsigned long)dropped,
           (unsigned)active,
           (unsigned long)st.holes,
           (unsigned long)st.dup_discarded,
           (unsigned long)st.corrupt);
}

int main(void) {
    setlinebuf(stdout);
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    printf("[MAIN] kvstore-incr-sync starting (single-threaded epoll)\n");
    printf("[MAIN] slave=%s:%u filter_port=%u iface=%s\n",
           SLAVE_IP, (unsigned)SLAVE_PORT,
           (unsigned)KVS_FILTER_PORT, XDP_IFACE);

    /* ── epoll + modules ─────────────────────────────────────────────── */

    int epfd = epoll_create1(EPOLL_CLOEXEC);
    if (unlikely(epfd < 0)) {
        perror("[MAIN] epoll_create1");
        return 1;
    }

    stream_ops_t ops = { .on_data = replay_on_data,
                         .on_close = replay_on_close };
    if (unlikely(stream_init(&ops) != 0)) {
        close(epfd);
        return 1;
    }
    if (unlikely(replay_init(epfd) != 0)) {
        stream_shutdown();
        close(epfd);
        return 1;
    }

    /* ── load + attach XDP BPF ───────────────────────────────────────── */

    struct bpf_object  *bpf_obj = NULL;
    struct ring_buffer *ringbuf = NULL;
    xdp_attach_t        xdp_att;

    if (unlikely(xdp_load_bpf(KVS_FILTER_PORT, XDP_IFACE, ringbuf_cb,
                              &bpf_obj, &ringbuf, &xdp_att) != 0)) {
        fprintf(stderr, "[MAIN] xdp_load_bpf failed\n");
        replay_shutdown();
        stream_shutdown();
        close(epfd);
        return 1;
    }

    /* ── timerfd (periodic tick) ─────────────────────────────────────── */

    int tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (unlikely(tfd < 0)) {
        perror("[MAIN] timerfd_create");
        xdp_cleanup(&xdp_att, bpf_obj, ringbuf);
        replay_shutdown();
        stream_shutdown();
        close(epfd);
        return 1;
    }
    struct itimerspec its = {0};
    its.it_interval.tv_sec  = TIMER_TICK_MS / 1000;
    its.it_interval.tv_nsec = (TIMER_TICK_MS % 1000) * 1000000L;
    its.it_value = its.it_interval;
    timerfd_settime(tfd, 0, &its, NULL);

    /* ── register ringbuf + timer in the epoll set ───────────────────── */

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN;
    ev.data.ptr = TAG_RINGBUF;
    epoll_ctl(epfd, EPOLL_CTL_ADD, ring_buffer__epoll_fd(ringbuf), &ev);

    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN;
    ev.data.ptr = TAG_TIMER;
    epoll_ctl(epfd, EPOLL_CTL_ADD, tfd, &ev);

    /* ── startup probe + go ──────────────────────────────────────────── */

    slave_probe();
    printf("[MAIN] incremental sync active\n");

    struct epoll_event events[REPLAY_EPOLL_MAXEVENTS];
    time_t last_stats = time(NULL);

    while (likely(g_running)) {
        int n = epoll_wait(epfd, events, REPLAY_EPOLL_MAXEVENTS, TIMER_TICK_MS);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("[MAIN] epoll_wait");
            break;
        }

        for (int i = 0; i < n; i++) {
            void *ptr = events[i].data.ptr;

            if (ptr == TAG_RINGBUF) {
                ring_buffer__consume(ringbuf);
            } else if (ptr == TAG_TIMER) {
                uint64_t expirations;
                ssize_t r = read(tfd, &expirations, sizeof(expirations));
                (void)r;
                stream_tick();
                replay_tick();
            } else {
                replay_handle_event((replay_conn_t *)ptr, events[i].events);
            }
        }
        replay_sync_interest();

        /* Periodic stats (~STATS_INTERVAL_S). */
        time_t now = time(NULL);
        if (now - last_stats >= STATS_INTERVAL_S) {
            print_stats();
            last_stats = now;
        }
    }

    /* ── shutdown ────────────────────────────────────────────────────── */

    printf("[MAIN] shutting down...\n");
    print_stats();

    xdp_print_bpf_stats(bpf_obj);

    /* Stop producing events first, then tear down conns + buffers. */
    xdp_cleanup(&xdp_att, bpf_obj, ringbuf);
    stream_shutdown();   /* calls replay_on_close for every source conn */
    replay_shutdown();   /* force-close any conns still draining */

    close(tfd);
    close(epfd);

    printf("[MAIN] done\n");
    return 0;
}
