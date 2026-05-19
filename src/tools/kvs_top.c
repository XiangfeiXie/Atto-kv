/* kvs-top — three-layer BPF observability dashboard for kvstore.
 *
 * Opens the BPF maps that kvstore pins under /sys/fs/bpf/kvstore/ and
 * renders a refreshing terminal dashboard combining:
 *   - L2/L3 layer: XDP hot-cache hits/misses/bypass (xdp_stats)
 *   - L4/L7 layer: sockmap per-cmd counts, per-conn profiles
 *
 * Usage:
 *   kvs-top                     # 1 Hz refresh, top-10 commands/conns
 *   kvs-top -d 0.5 -n 20        # 2 Hz refresh, top-20
 *   kvs-top --once              # single snapshot then exit (for scripts)
 *
 * Requires CAP_BPF (or root) to open the pinned maps.
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include <getopt.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <limits.h>

#include <bpf/bpf.h>

#include "../bpf/bpf_pin.h"
#include "../bpf/xdp_kvs.h"
#include "../bpf/sockmap_kvs.h"
#include "../bpf/xdp_manager.h"
#include "../bpf/sockmap_manager.h"

/* ── Configuration ─────────────────────────────────────────────────────── */

static volatile sig_atomic_t g_stop = 0;
static int      g_topn        = 10;
static double   g_interval_s  = 1.0;
static int      g_once        = 0;
static int      g_port        = 2000;     /* kvstore RESP port for INFO query */
static int      g_attach_xdp  = 0;
static int      g_attach_sockmap = 0;
static const char *g_xdp_iface = NULL;
static const char *g_sockmap_cgroup = NULL;
static pid_t    g_sockmap_pid = -1;
static int      g_detach_after_render = 0;

static void on_sigint(int sig) { (void)sig; g_stop = 1; }

/* ── Snapshot containers ───────────────────────────────────────────────── */

struct xdp_snapshot {
    uint64_t hits, misses, tx_drop, bypass;
    int      available;
};

/* Phase 10 — user-space lock-free fast-path cache stats.  Pulled via INFO
 * Stats over TCP because fp_cache lives in process memory (not a pinned
 * BPF map).  Connect-once-per-tick is fine at 1 Hz refresh. */
struct fp_snapshot {
    uint64_t hits, misses, invalidations, writer_collisions;
    int      enabled;       /* fastpath_cache_enabled flag */
    int      available;     /* INFO query succeeded */
};

struct sm_stats_snapshot {
    uint64_t added, add_fail, verdict, verdict_pass, parsed, parse_skip;
    int      available;
};

struct cmd_row {
    char     name[SOCKMAP_CMD_NAME_MAX + 1];
    uint64_t count;
    uint64_t bytes;
    uint64_t prev_count;   /* for delta */
};

struct conn_row {
    uint64_t cookie;
    uint64_t first_seen_ns;
    uint64_t last_seen_ns;
    uint64_t cmd_count;
    uint64_t bytes_in;
};

#define MAX_CMD_ROWS  SOCKMAP_CMD_MAX_ENTRIES
#define MAX_CONN_ROWS 1024

/* ── INFO Stats reader (Phase 10 fp_cache) ────────────────────────────────
 *
 * Issue a single RESP "INFO Stats" command to the loopback kvstore and
 * scrape the four fastpath_cache_* counters.  Failure is non-fatal —
 * we just mark the layer as unavailable and the render() draws a hint.
 *
 * Why a hand-rolled mini RESP client (instead of popen redis-cli):
 *   - Avoids fork+exec per refresh tick (popen is ~1 ms; this is ~50 µs)
 *   - No external dependency
 *   - Easy to reason about timeouts (we use a 200 ms budget total)
 */
static int connect_loopback_with_timeout(int port, int timeout_ms)
{
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) return -1;
    int flags = fcntl(s, F_GETFL, 0);
    fcntl(s, F_SETFL, flags | O_NONBLOCK);
    int one = 1; setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);

    if (connect(s, (struct sockaddr *)&sa, sizeof(sa)) < 0 && errno != EINPROGRESS) {
        close(s); return -1;
    }
    struct pollfd pf = { .fd = s, .events = POLLOUT };
    if (poll(&pf, 1, timeout_ms) <= 0) { close(s); return -1; }
    int err = 0; socklen_t el = sizeof(err);
    if (getsockopt(s, SOL_SOCKET, SO_ERROR, &err, &el) < 0 || err != 0) {
        close(s); return -1;
    }
    fcntl(s, F_SETFL, flags);  /* back to blocking for simple read/write */
    return s;
}

static int recv_with_timeout(int s, char *buf, size_t cap, int timeout_ms)
{
    size_t off = 0;
    struct timespec start, now;
    clock_gettime(CLOCK_MONOTONIC, &start);
    while (off < cap - 1) {
        clock_gettime(CLOCK_MONOTONIC, &now);
        long elapsed_ms = (now.tv_sec - start.tv_sec) * 1000 +
                          (now.tv_nsec - start.tv_nsec) / 1000000;
        long remaining = timeout_ms - elapsed_ms;
        if (remaining <= 0) break;
        struct pollfd pf = { .fd = s, .events = POLLIN };
        int pr = poll(&pf, 1, (int)remaining);
        if (pr <= 0) break;
        ssize_t n = recv(s, buf + off, cap - 1 - off, 0);
        if (n <= 0) break;
        off += (size_t)n;
        /* Heuristic: kvstore terminates the bulk reply with "\r\n" after
         * the body length is exhausted; INFO Stats is < 4 KB so a single
         * recv usually drains it.  Stop if we've seen a likely tail. */
        if (off >= 4 && memcmp(buf + off - 2, "\r\n", 2) == 0 &&
            memchr(buf, '$', off) != NULL) break;
    }
    buf[off] = '\0';
    return (int)off;
}

static uint64_t scan_u64(const char *body, const char *key)
{
    const char *p = strstr(body, key);
    if (!p) return 0;
    p += strlen(key);
    return (uint64_t)strtoull(p, NULL, 10);
}

static void read_fp_stats(int port, struct fp_snapshot *out)
{
    memset(out, 0, sizeof(*out));
    int s = connect_loopback_with_timeout(port, 100);
    if (s < 0) return;

    /* RESP request: INFO Stats */
    static const char req[] = "*2\r\n$4\r\nINFO\r\n$5\r\nstats\r\n";
    if (send(s, req, sizeof(req) - 1, MSG_NOSIGNAL) < 0) { close(s); return; }

    char body[8192];
    int n = recv_with_timeout(s, body, sizeof(body), 100);
    close(s);
    if (n <= 0) return;

    out->hits              = scan_u64(body, "fastpath_cache_hits:");
    out->misses            = scan_u64(body, "fastpath_cache_misses:");
    out->invalidations     = scan_u64(body, "fastpath_cache_invalidations:");
    out->writer_collisions = scan_u64(body, "fastpath_cache_writer_collisions:");
    out->enabled           = (int)scan_u64(body, "fastpath_cache_enabled:");
    out->available         = 1;
}

/* ── Map readers ───────────────────────────────────────────────────────── */

static void read_xdp_stats(int fd, struct xdp_snapshot *out)
{
    memset(out, 0, sizeof(*out));
    if (fd < 0) return;
    __u32 k; __u64 v;
    k = XDP_STAT_HITS;    v = 0; bpf_map_lookup_elem(fd, &k, &v); out->hits    = v;
    k = XDP_STAT_MISSES;  v = 0; bpf_map_lookup_elem(fd, &k, &v); out->misses  = v;
    k = XDP_STAT_TX_DROP; v = 0; bpf_map_lookup_elem(fd, &k, &v); out->tx_drop = v;
    k = XDP_STAT_BYPASS;  v = 0; bpf_map_lookup_elem(fd, &k, &v); out->bypass  = v;
    out->available = 1;
}

static void read_sm_stats(int fd, struct sm_stats_snapshot *out)
{
    memset(out, 0, sizeof(*out));
    if (fd < 0) return;
    __u32 k; __u64 v;
    #define R(idx, field) do { k=(idx); v=0; bpf_map_lookup_elem(fd,&k,&v); out->field=v; } while(0)
    R(SOCKMAP_STAT_ADDED,        added);
    R(SOCKMAP_STAT_ADD_FAIL,     add_fail);
    R(SOCKMAP_STAT_VERDICT,      verdict);
    R(SOCKMAP_STAT_VERDICT_PASS, verdict_pass);
    R(SOCKMAP_STAT_PARSED,       parsed);
    R(SOCKMAP_STAT_PARSE_SKIP,   parse_skip);
    #undef R
    out->available = 1;
}

static int read_cmd_counts(int fd, struct cmd_row *rows, int max,
                            const struct cmd_row *prev, int prev_n)
{
    if (fd < 0) return 0;
    struct sockmap_cmd_key prev_key = {}, next = {};
    int has_prev = 0, n = 0;

    while (n < max &&
           bpf_map_get_next_key(fd, has_prev ? &prev_key : NULL, &next) == 0) {
        struct sockmap_cmd_stats st = {};
        if (bpf_map_lookup_elem(fd, &next, &st) == 0) {
            struct cmd_row *r = &rows[n];
            int copy = next.len > SOCKMAP_CMD_NAME_MAX
                       ? SOCKMAP_CMD_NAME_MAX : next.len;
            memcpy(r->name, next.name, (size_t)copy);
            r->name[copy] = '\0';
            r->count = st.count;
            r->bytes = st.bytes;
            r->prev_count = 0;
            /* Look up previous count by name for delta. */
            for (int i = 0; i < prev_n; i++) {
                if (strcmp(prev[i].name, r->name) == 0) {
                    r->prev_count = prev[i].count;
                    break;
                }
            }
            n++;
        }
        prev_key = next;
        has_prev = 1;
    }
    return n;
}

static int read_conn_profiles(int fd, struct conn_row *rows, int max)
{
    if (fd < 0) return 0;
    __u64 prev = 0, next = 0;
    int has_prev = 0, n = 0;
    while (n < max &&
           bpf_map_get_next_key(fd, has_prev ? &prev : NULL, &next) == 0) {
        struct sockmap_conn_profile prof = {};
        if (bpf_map_lookup_elem(fd, &next, &prof) == 0) {
            rows[n].cookie        = next;
            rows[n].first_seen_ns = prof.first_seen_ns;
            rows[n].last_seen_ns  = prof.last_seen_ns;
            rows[n].cmd_count     = prof.cmd_count;
            rows[n].bytes_in      = prof.bytes_in;
            n++;
        }
        prev = next;
        has_prev = 1;
    }
    return n;
}

/* ── Sorting ───────────────────────────────────────────────────────────── */

static int cmp_cmd_rate(const void *a, const void *b) {
    const struct cmd_row *x = a, *y = b;
    uint64_t dx = x->count - x->prev_count;
    uint64_t dy = y->count - y->prev_count;
    if (dy != dx) return dy > dx ? 1 : -1;
    return (y->count > x->count) - (y->count < x->count);
}

static int cmp_conn_recent(const void *a, const void *b) {
    const struct conn_row *x = a, *y = b;
    if (y->last_seen_ns != x->last_seen_ns)
        return y->last_seen_ns > x->last_seen_ns ? 1 : -1;
    return 0;
}

/* ── Rendering ─────────────────────────────────────────────────────────── */

static void clear_screen(void) {
    /* CSI 2J: clear screen; CSI H: cursor home */
    fputs("\x1b[2J\x1b[H", stdout);
}

static void format_bytes(uint64_t b, char *buf, size_t buflen)
{
    if (b >= (1ULL<<30))
        snprintf(buf, buflen, "%.2fGB", (double)b/(1ULL<<30));
    else if (b >= (1ULL<<20))
        snprintf(buf, buflen, "%.2fMB", (double)b/(1ULL<<20));
    else if (b >= (1ULL<<10))
        snprintf(buf, buflen, "%.2fKB", (double)b/(1ULL<<10));
    else
        snprintf(buf, buflen, "%lluB", (unsigned long long)b);
}

static void render(const struct xdp_snapshot      *xdp,
                   const struct fp_snapshot       *prev_fp,
                   const struct fp_snapshot       *fp,
                   const struct sm_stats_snapshot *prev_sm,
                   const struct sm_stats_snapshot *sm,
                   struct cmd_row *cmds, int n_cmds,
                   struct conn_row *conns, int n_conns,
                   double dt_s)
{
    if (!g_once) clear_screen();

    time_t now = time(NULL);
    char ts[64];
    struct tm tmv;
    localtime_r(&now, &tmv);
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tmv);

    printf("\x1b[1;36mkvs-top\x1b[0m  %s   refresh=%.2fs   topN=%d\n",
           ts, g_interval_s, g_topn);
    puts("================================================================================");

    /* ── XDP layer ─────────────────────────────────────────────── */
    puts("\x1b[1;33m[L2/L3] XDP hot-cache\x1b[0m");
    if (!xdp->available) {
        puts("  (not loaded — start kvstore with xdp_enabled=yes)");
    } else {
        uint64_t total = xdp->hits + xdp->misses;
        double hr = total ? 100.0 * xdp->hits / total : 0.0;
        printf("  hits=%-12llu misses=%-12llu hit_rate=%.2f%%   bypass=%llu  tx_drop=%llu\n",
               (unsigned long long)xdp->hits,
               (unsigned long long)xdp->misses, hr,
               (unsigned long long)xdp->bypass,
               (unsigned long long)xdp->tx_drop);
    }
    puts("");

    /* ── Fast-path (user-space, Phase 10) ──────────────────────── */
    puts("\x1b[1;33m[FAST] lock-free seqlock fast-path (user-space)\x1b[0m");
    if (!fp->available) {
        printf("  (INFO query failed on port %d — is kvstore running?)\n", g_port);
    } else if (!fp->enabled) {
        puts("  (disabled — set 'fastpath_cache yes' in kvstore.conf to enable)");
    } else {
        uint64_t total = fp->hits + fp->misses;
        double hr = total ? 100.0 * fp->hits / total : 0.0;
        double dh = (double)(fp->hits   - prev_fp->hits)   / (dt_s > 0 ? dt_s : 1.0);
        double dm = (double)(fp->misses - prev_fp->misses) / (dt_s > 0 ? dt_s : 1.0);
        printf("  hits=%-12llu misses=%-12llu hit_rate=%.2f%%   hits/s=%-8.0f miss/s=%.0f\n",
               (unsigned long long)fp->hits,
               (unsigned long long)fp->misses, hr, dh, dm);
        printf("  invalidations=%llu  writer_collisions=%llu\n",
               (unsigned long long)fp->invalidations,
               (unsigned long long)fp->writer_collisions);
    }
    puts("");

    /* ── sockmap layer ─────────────────────────────────────────── */
    puts("\x1b[1;33m[L4/L7] sockmap stream-verdict\x1b[0m");
    if (!sm->available) {
        puts("  (not loaded — start kvstore with sockmap=yes)");
    } else {
        double dv = (double)(sm->verdict - prev_sm->verdict) / (dt_s > 0 ? dt_s : 1.0);
        double dp = (double)(sm->parsed  - prev_sm->parsed)  / (dt_s > 0 ? dt_s : 1.0);
        double pr = sm->verdict ? 100.0 * sm->parsed / sm->verdict : 0.0;
        printf("  socks_added=%llu  verdict=%llu (%.0f/s)  parsed=%llu (%.0f/s, %.2f%%)\n",
               (unsigned long long)sm->added,
               (unsigned long long)sm->verdict, dv,
               (unsigned long long)sm->parsed,  dp, pr);
        printf("  add_fail=%llu  parse_skip=%llu\n",
               (unsigned long long)sm->add_fail,
               (unsigned long long)sm->parse_skip);
    }
    puts("");

    /* ── Top commands ──────────────────────────────────────────── */
    printf("\x1b[1;33m[L7] Top %d commands\x1b[0m (sorted by rate)\n",
           g_topn);
    printf("  %-16s %12s %12s %12s\n", "CMD", "count", "rate(/s)", "bytes");
    printf("  ----------------------------------------------------\n");
    int show = n_cmds < g_topn ? n_cmds : g_topn;
    for (int i = 0; i < show; i++) {
        uint64_t d = cmds[i].count - cmds[i].prev_count;
        double rate = (double)d / (dt_s > 0 ? dt_s : 1.0);
        char b[16];
        format_bytes(cmds[i].bytes, b, sizeof(b));
        printf("  %-16s %12llu %12.1f %12s\n",
               cmds[i].name,
               (unsigned long long)cmds[i].count,
               rate, b);
    }
    if (n_cmds == 0) puts("  (no commands parsed yet)");
    puts("");

    /* ── Top connections ───────────────────────────────────────── */
    printf("\x1b[1;33m[L4] Top %d connections\x1b[0m (sorted by recent activity)\n",
           g_topn);
    printf("  %-18s %12s %12s %12s %12s\n",
           "cookie", "cmds", "bytes_in", "age(s)", "idle(ms)");
    printf("  ----------------------------------------------------------------------\n");
    show = n_conns < g_topn ? n_conns : g_topn;
    /* current monotonic time (ns) for age/idle */
    struct timespec tspec;
    clock_gettime(CLOCK_MONOTONIC, &tspec);
    uint64_t now_ns = (uint64_t)tspec.tv_sec * 1000000000ULL
                     + (uint64_t)tspec.tv_nsec;
    for (int i = 0; i < show; i++) {
        uint64_t age_ns  = now_ns > conns[i].first_seen_ns
                           ? now_ns - conns[i].first_seen_ns : 0;
        uint64_t idle_ns = now_ns > conns[i].last_seen_ns
                           ? now_ns - conns[i].last_seen_ns : 0;
        char b[16];
        format_bytes(conns[i].bytes_in, b, sizeof(b));
        printf("  %-18llu %12llu %12s %12.1f %12.1f\n",
               (unsigned long long)conns[i].cookie,
               (unsigned long long)conns[i].cmd_count,
               b,
               age_ns  / 1e9,
               idle_ns / 1e6);
    }
    if (n_conns == 0) puts("  (no live connections tracked)");

    if (!g_once) puts("\n[Ctrl-C to quit]");
    fflush(stdout);
}

/* ── Main loop ─────────────────────────────────────────────────────────── */

static void usage(const char *argv0) {
    fprintf(stderr,
        "Usage: %s [options]\n"
        "  -d, --interval SEC   refresh interval (default 1.0)\n"
        "  -n, --topn N         number of top rows per section (default 10)\n"
        "  -p, --port PORT      kvstore RESP port for [FAST] INFO query (default 2000)\n"
        "  -1, --once           render one frame and exit\n"
        "      --attach-xdp     load/attach XDP program before rendering (detach on exit)\n"
        "      --xdp-iface IF   interface for --attach-xdp (default: keep config default)\n"
        "      --attach-sockmap attach sockmap observability (detach on exit)\n"
        "      --sockmap-cgroup PATH attach sockmap to explicit cgroup path\n"
        "      --sockmap-pid PID attach sockmap to the cgroup of PID\n"
        "      --detach          detach requested layers then exit (no dashboard)\n"
        "  -h, --help           this help\n",
        argv0);
}

int main(int argc, char **argv)
{
    enum {
        OPT_ATTACH_XDP = 1000,
        OPT_XDP_IFACE,
        OPT_ATTACH_SOCKMAP,
        OPT_SOCKMAP_CGROUP,
        OPT_SOCKMAP_PID,
        OPT_DETACH_ONLY
    };

    static const struct option longopts[] = {
        {"interval", required_argument, 0, 'd'},
        {"topn",     required_argument, 0, 'n'},
        {"port",     required_argument, 0, 'p'},
        {"once",     no_argument,       0, '1'},
        {"attach-xdp", no_argument,     0, OPT_ATTACH_XDP},
        {"xdp-iface", required_argument,0, OPT_XDP_IFACE},
        {"attach-sockmap", no_argument,0, OPT_ATTACH_SOCKMAP},
        {"sockmap-cgroup", required_argument, 0, OPT_SOCKMAP_CGROUP},
        {"sockmap-pid", required_argument, 0, OPT_SOCKMAP_PID},
        {"detach",    no_argument,       0, OPT_DETACH_ONLY},
        {"help",     no_argument,       0, 'h'},
        {0,0,0,0}
    };
    int c;
    while ((c = getopt_long(argc, argv, "d:n:p:1h", longopts, NULL)) != -1) {
        switch (c) {
        case 'd': g_interval_s = strtod(optarg, NULL); break;
        case 'n': g_topn       = atoi(optarg);         break;
        case 'p': g_port       = atoi(optarg);         break;
        case '1': g_once       = 1;                    break;
        case OPT_ATTACH_XDP:      g_attach_xdp = 1; break;
        case OPT_XDP_IFACE:       g_xdp_iface = optarg; break;
        case OPT_ATTACH_SOCKMAP:  g_attach_sockmap = 1; break;
        case OPT_SOCKMAP_CGROUP:  g_sockmap_cgroup = optarg; break;
        case OPT_SOCKMAP_PID: {
            long pid = strtol(optarg, NULL, 10);
            if (pid <= 0 || pid > INT_MAX) {
                fprintf(stderr, "kvs-top: invalid --sockmap-pid '%s'\n", optarg);
                return 1;
            }
            g_sockmap_pid = (pid_t)pid;
            break;
        }
        case OPT_DETACH_ONLY:     g_detach_after_render = 1; break;
        case 'h': usage(argv[0]); return 0;
        default:  usage(argv[0]); return 1;
        }
    }
    if (g_interval_s < 0.05) g_interval_s = 0.05;
    if (g_topn < 1)          g_topn = 1;
    if (g_topn > MAX_CMD_ROWS) g_topn = MAX_CMD_ROWS;

    signal(SIGINT,  on_sigint);
    signal(SIGTERM, on_sigint);

    int attached_xdp = 0;
    int attached_sockmap = 0;

    if (g_attach_xdp) {
        const char *iface = g_xdp_iface && *g_xdp_iface ? g_xdp_iface : NULL;
        if (!iface) {
            /* Fallback to "lo" to keep behaviour predictable if not specified */
            iface = "lo";
        }
        if (xdp_manager_init(iface, g_port) != 0) {
            fprintf(stderr, "kvs-top: failed to attach XDP on iface %s\n", iface);
            return 1;
        }
        attached_xdp = 1;
    }

    if (g_attach_sockmap) {
        int rc = -1;
        if (g_sockmap_cgroup && *g_sockmap_cgroup) {
            rc = sockmap_manager_init(g_port, g_sockmap_cgroup);
        } else if (g_sockmap_pid > 0) {
            rc = sockmap_manager_init_for_pid(g_port, g_sockmap_pid);
        } else {
            fprintf(stderr,
                    "kvs-top: --attach-sockmap requires --sockmap-pid or --sockmap-cgroup\n");
            if (attached_xdp) xdp_manager_stop();
            return 1;
        }
        if (rc != 0) {
            if (attached_xdp) xdp_manager_stop();
            return 1;
        }
        attached_sockmap = 1;
    }

    /* Open all pinned maps; missing maps are tolerated (layer marked unavailable). */
    int xdp_stats_fd = kvs_bpf_open_pinned("xdp_stats");
    int sm_stats_fd  = kvs_bpf_open_pinned("sockmap_stats");
    int cmd_fd       = kvs_bpf_open_pinned("cmd_counts");
    int conn_fd      = kvs_bpf_open_pinned("conn_profiles");

    /* Note: BPF maps unavailable is no longer fatal — the [FAST] layer
     * works on plain user-space INFO Stats over TCP, so kvs-top is still
     * useful even when XDP/sockmap are off. */
    if (xdp_stats_fd < 0 && sm_stats_fd < 0) {
        fprintf(stderr,
            "kvs-top: no pinned BPF maps under %s — only [FAST] layer\n"
            "  will be shown.  Enable xdp_enabled/sockmap_enabled for L2-L7 stats.\n",
            KVS_PIN_DIR);
    }

    /* Two snapshots for delta calculation. */
    struct sm_stats_snapshot sm_prev = {}, sm_cur = {};
    struct fp_snapshot       fp_prev = {}, fp_cur = {};
    struct cmd_row cmds_prev[MAX_CMD_ROWS] = {};
    struct cmd_row cmds_cur[MAX_CMD_ROWS]  = {};
    struct conn_row conns[MAX_CONN_ROWS]   = {};
    int n_cmds_prev = 0;

    struct timespec ts_prev, ts_cur;
    clock_gettime(CLOCK_MONOTONIC, &ts_prev);

    /* First read primes the "prev" snapshot for sensible rates on first render. */
    read_sm_stats(sm_stats_fd, &sm_prev);
    read_fp_stats(g_port, &fp_prev);
    n_cmds_prev = read_cmd_counts(cmd_fd, cmds_prev, MAX_CMD_ROWS, NULL, 0);

    do {
        if (g_detach_after_render && !attached_xdp && !attached_sockmap) {
            /* Nothing to show, exit early */
            break;
        }
        if (!g_once) {
            struct timespec sl = {
                .tv_sec  = (time_t)g_interval_s,
                .tv_nsec = (long)((g_interval_s - (time_t)g_interval_s) * 1e9),
            };
            nanosleep(&sl, NULL);
        }

        clock_gettime(CLOCK_MONOTONIC, &ts_cur);
        double dt = (ts_cur.tv_sec - ts_prev.tv_sec)
                  + (ts_cur.tv_nsec - ts_prev.tv_nsec) / 1e9;

        struct xdp_snapshot xdp = {};
        read_xdp_stats(xdp_stats_fd, &xdp);
        read_sm_stats(sm_stats_fd, &sm_cur);
        read_fp_stats(g_port, &fp_cur);

        int n_cmds = read_cmd_counts(cmd_fd, cmds_cur, MAX_CMD_ROWS,
                                       cmds_prev, n_cmds_prev);
        qsort(cmds_cur, (size_t)n_cmds, sizeof(struct cmd_row), cmp_cmd_rate);

        int n_conns = read_conn_profiles(conn_fd, conns, MAX_CONN_ROWS);
        qsort(conns, (size_t)n_conns, sizeof(struct conn_row), cmp_conn_recent);

        render(&xdp, &fp_prev, &fp_cur, &sm_prev, &sm_cur,
               cmds_cur, n_cmds, conns, n_conns, dt);

        /* Rotate snapshots. */
        sm_prev = sm_cur;
        fp_prev = fp_cur;
        memcpy(cmds_prev, cmds_cur, sizeof(cmds_cur));
        n_cmds_prev = n_cmds;
        ts_prev = ts_cur;
        if (g_detach_after_render)
            break;
    } while (!g_once && !g_stop);

    if (xdp_stats_fd >= 0) close(xdp_stats_fd);
    if (sm_stats_fd  >= 0) close(sm_stats_fd);
    if (cmd_fd       >= 0) close(cmd_fd);
    if (conn_fd      >= 0) close(conn_fd);

    if (attached_sockmap)
        sockmap_manager_stop();
    if (attached_xdp)
        xdp_manager_stop();

    if (g_detach_after_render)
        return 0;
    return 0;
}
