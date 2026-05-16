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

#include <bpf/bpf.h>

#include "../bpf/bpf_pin.h"
#include "../bpf/xdp_kvs.h"
#include "../bpf/sockmap_kvs.h"

/* ── Configuration ─────────────────────────────────────────────────────── */

static volatile sig_atomic_t g_stop = 0;
static int      g_topn        = 10;
static double   g_interval_s  = 1.0;
static int      g_once        = 0;

static void on_sigint(int sig) { (void)sig; g_stop = 1; }

/* ── Snapshot containers ───────────────────────────────────────────────── */

struct xdp_snapshot {
    uint64_t hits, misses, tx_drop, bypass;
    int      available;
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
        "  -1, --once           render one frame and exit\n"
        "  -h, --help           this help\n",
        argv0);
}

int main(int argc, char **argv)
{
    static const struct option longopts[] = {
        {"interval", required_argument, 0, 'd'},
        {"topn",     required_argument, 0, 'n'},
        {"once",     no_argument,       0, '1'},
        {"help",     no_argument,       0, 'h'},
        {0,0,0,0}
    };
    int c;
    while ((c = getopt_long(argc, argv, "d:n:1h", longopts, NULL)) != -1) {
        switch (c) {
        case 'd': g_interval_s = strtod(optarg, NULL); break;
        case 'n': g_topn       = atoi(optarg);         break;
        case '1': g_once       = 1;                    break;
        case 'h': usage(argv[0]); return 0;
        default:  usage(argv[0]); return 1;
        }
    }
    if (g_interval_s < 0.05) g_interval_s = 0.05;
    if (g_topn < 1)          g_topn = 1;
    if (g_topn > MAX_CMD_ROWS) g_topn = MAX_CMD_ROWS;

    signal(SIGINT,  on_sigint);
    signal(SIGTERM, on_sigint);

    /* Open all pinned maps; missing maps are tolerated (layer marked unavailable). */
    int xdp_stats_fd = kvs_bpf_open_pinned("xdp_stats");
    int sm_stats_fd  = kvs_bpf_open_pinned("sockmap_stats");
    int cmd_fd       = kvs_bpf_open_pinned("cmd_counts");
    int conn_fd      = kvs_bpf_open_pinned("conn_profiles");

    if (xdp_stats_fd < 0 && sm_stats_fd < 0) {
        fprintf(stderr,
            "kvs-top: no pinned maps found under %s\n"
            "  Is kvstore running with XDP or sockmap enabled?\n"
            "  Also confirm /sys/fs/bpf is mounted and you have CAP_BPF (try sudo).\n",
            KVS_PIN_DIR);
        return 2;
    }

    /* Two snapshots for delta calculation. */
    struct sm_stats_snapshot sm_prev = {}, sm_cur = {};
    struct cmd_row cmds_prev[MAX_CMD_ROWS] = {};
    struct cmd_row cmds_cur[MAX_CMD_ROWS]  = {};
    struct conn_row conns[MAX_CONN_ROWS]   = {};
    int n_cmds_prev = 0;

    struct timespec ts_prev, ts_cur;
    clock_gettime(CLOCK_MONOTONIC, &ts_prev);

    /* First read primes the "prev" snapshot for sensible rates on first render. */
    read_sm_stats(sm_stats_fd, &sm_prev);
    n_cmds_prev = read_cmd_counts(cmd_fd, cmds_prev, MAX_CMD_ROWS, NULL, 0);

    do {
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

        int n_cmds = read_cmd_counts(cmd_fd, cmds_cur, MAX_CMD_ROWS,
                                       cmds_prev, n_cmds_prev);
        qsort(cmds_cur, (size_t)n_cmds, sizeof(struct cmd_row), cmp_cmd_rate);

        int n_conns = read_conn_profiles(conn_fd, conns, MAX_CONN_ROWS);
        qsort(conns, (size_t)n_conns, sizeof(struct conn_row), cmp_conn_recent);

        render(&xdp, &sm_prev, &sm_cur, cmds_cur, n_cmds, conns, n_conns, dt);

        /* Rotate snapshots. */
        sm_prev = sm_cur;
        memcpy(cmds_prev, cmds_cur, sizeof(cmds_cur));
        n_cmds_prev = n_cmds;
        ts_prev = ts_cur;
    } while (!g_once && !g_stop);

    if (xdp_stats_fd >= 0) close(xdp_stats_fd);
    if (sm_stats_fd  >= 0) close(sm_stats_fd);
    if (cmd_fd       >= 0) close(cmd_fd);
    if (conn_fd      >= 0) close(conn_fd);
    return 0;
}
