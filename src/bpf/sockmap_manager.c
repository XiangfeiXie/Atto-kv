/* sockmap_manager.c — userspace loader for the sockmap observability hooks.
 *
 * Loads sockmap_kvs.kern.o, then:
 *   1. Attaches the `sockops` program to a cgroup v2 directory containing
 *      this process so newly-accepted sockets on `port` get added to the
 *      sockhash automatically.
 *   2. Attaches the `sk_skb/stream_verdict` program to the sockhash so it
 *      runs against every skb arriving on managed sockets.
 *
 * Failure modes are non-fatal: kvstore runs normally without sockmap if
 * init returns -1 (e.g. cgroup v1 only system, missing CAP_BPF, etc.).
 */

#define _GNU_SOURCE
#include "sockmap_manager.h"
#include "sockmap_kvs.h"
#include "bpf_pin.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <bpf/libbpf.h>
#include <bpf/bpf.h>

/* ── Module state ──────────────────────────────────────────────────────── */

static struct bpf_object *g_obj         = NULL;
static int                g_cgroup_fd   = -1;
static int                g_sockmap_fd  = -1;
static int                g_config_fd   = -1;
static int                g_stats_fd    = -1;
static int                g_cmd_fd      = -1;
static int                g_conn_fd     = -1;
static int                g_sockops_fd  = -1;   /* program fd, kept for detach */
static int                g_verdict_fd  = -1;
static int                g_ready       = 0;

static char g_obj_path[1024] = "";
static char g_cgroup_path[512] = "";

/* ── Helpers ───────────────────────────────────────────────────────────── */

static void build_obj_path(void) {
    char self[512] = {};
    ssize_t n = readlink("/proc/self/exe", self, sizeof(self) - 1);
    if (n <= 0) {
        snprintf(g_obj_path, sizeof(g_obj_path), "sockmap_kvs.kern.o");
        return;
    }
    char *slash = strrchr(self, '/');
    if (slash) *slash = '\0';
    snprintf(g_obj_path, sizeof(g_obj_path), "%s/sockmap_kvs.kern.o", self);
}

static int set_cgroup_path(const char *path)
{
    if (!path || !*path) return -1;
    if (strlen(path) >= sizeof(g_cgroup_path))
        return -1;
    struct stat st;
    if (stat(path, &st) != 0 || !S_ISDIR(st.st_mode))
        return -1;
    strcpy(g_cgroup_path, path);
    return 0;
}

/* Find the cgroup v2 path containing the current process (pid < 0) or the
 * provided pid.  On success writes the absolute path into out (len bytes). */
static int resolve_cgroup_path_generic(pid_t pid, char *out, size_t len)
{
    char proc_path[64];
    if (pid < 0)
        snprintf(proc_path, sizeof(proc_path), "/proc/self/cgroup");
    else
        snprintf(proc_path, sizeof(proc_path), "/proc/%d/cgroup", pid);

    FILE *f = fopen(proc_path, "r");
    if (!f) return -1;

    char line[512];
    char rel[256] = "";
    while (fgets(line, sizeof(line), f)) {
        /* Format: <hierarchy-id>:<controller-list>:<path>\n
         * cgroup v2 unified hierarchy is hierarchy-id 0 with empty controllers. */
        if (line[0] == '0' && line[1] == ':' && line[2] == ':') {
            strncpy(rel, line + 3, sizeof(rel) - 1);
            char *nl = strchr(rel, '\n');
            if (nl) *nl = '\0';
            break;
        }
    }
    fclose(f);

    if (rel[0] == '\0') return -1;

    snprintf(out, len, "/sys/fs/cgroup%s",
             rel[0] == '/' && rel[1] == '\0' ? "" : rel);

    /* Verify the path exists and is a directory. */
    struct stat st;
    if (stat(out, &st) != 0 || !S_ISDIR(st.st_mode)) {
        /* Fall back to the unified root */
        strcpy(out, "/sys/fs/cgroup");
        if (stat(out, &st) != 0 || !S_ISDIR(st.st_mode))
            return -1;
    }
    return 0;
}

static int libbpf_print_fn(enum libbpf_print_level level,
                            const char *format, va_list args) {
    if (level == LIBBPF_DEBUG) return 0;
    return vfprintf(stderr, format, args);
}

/* ── Public API ────────────────────────────────────────────────────────── */

int sockmap_manager_init(int port, const char *cgroup_path) {
    if (port <= 0 || port > 65535) {
        fprintf(stderr, "[SOCKMAP] invalid port %d, sockmap disabled\n", port);
        return -1;
    }

    build_obj_path();
    if (access(g_obj_path, R_OK) != 0) {
        fprintf(stderr, "[SOCKMAP] BPF object not found at %s — "
                "run cmake --build to compile it\n", g_obj_path);
        return -1;
    }

    if (cgroup_path) {
        if (set_cgroup_path(cgroup_path) != 0) {
            fprintf(stderr, "[SOCKMAP] target cgroup '%s' invalid; sockmap disabled\n",
                    cgroup_path);
            return -1;
        }
    } else {
        if (resolve_cgroup_path_generic(-1, g_cgroup_path, sizeof(g_cgroup_path)) != 0) {
            fprintf(stderr, "[SOCKMAP] could not resolve cgroup v2 path "
                    "(system may be cgroup v1 only); sockmap disabled\n");
            return -1;
        }
    }

    libbpf_set_print(libbpf_print_fn);

    g_obj = bpf_object__open_file(g_obj_path, NULL);
    if (!g_obj) {
        fprintf(stderr, "[SOCKMAP] bpf_object__open_file(%s): %s\n",
                g_obj_path, strerror(errno));
        return -1;
    }

    int err = bpf_object__load(g_obj);
    if (err) {
        fprintf(stderr, "[SOCKMAP] bpf_object__load: %s\n", strerror(-err));
        bpf_object__close(g_obj);
        g_obj = NULL;
        return -1;
    }

    /* Resolve maps */
    struct bpf_map *m;
    m = bpf_object__find_map_by_name(g_obj, "sockmap");
    g_sockmap_fd = m ? bpf_map__fd(m) : -1;
    m = bpf_object__find_map_by_name(g_obj, "sockmap_config");
    g_config_fd  = m ? bpf_map__fd(m) : -1;
    m = bpf_object__find_map_by_name(g_obj, "sockmap_stats");
    g_stats_fd   = m ? bpf_map__fd(m) : -1;
    m = bpf_object__find_map_by_name(g_obj, "cmd_counts");
    g_cmd_fd     = m ? bpf_map__fd(m) : -1;
    m = bpf_object__find_map_by_name(g_obj, "conn_profiles");
    g_conn_fd    = m ? bpf_map__fd(m) : -1;

    if (g_sockmap_fd < 0 || g_config_fd < 0 || g_stats_fd < 0 ||
        g_cmd_fd < 0 || g_conn_fd < 0) {
        fprintf(stderr, "[SOCKMAP] failed to resolve required maps\n");
        sockmap_manager_stop();
        return -1;
    }

    /* Resolve programs */
    struct bpf_program *sockops_prog =
        bpf_object__find_program_by_name(g_obj, "sockmap_kvs_sockops");
    struct bpf_program *verdict_prog =
        bpf_object__find_program_by_name(g_obj, "sockmap_kvs_verdict");
    if (!sockops_prog || !verdict_prog) {
        fprintf(stderr, "[SOCKMAP] expected programs not found in ELF\n");
        sockmap_manager_stop();
        return -1;
    }

    g_sockops_fd = bpf_program__fd(sockops_prog);
    g_verdict_fd = bpf_program__fd(verdict_prog);

    /* Write target port */
    __u32 cfg_key = 0, cfg_val = (__u32)port;
    bpf_map_update_elem(g_config_fd, &cfg_key, &cfg_val, BPF_ANY);

    /* Zero stats */
    __u64 zero = 0;
    for (__u32 i = 0; i < SOCKMAP_STAT_MAX; i++)
        bpf_map_update_elem(g_stats_fd, &i, &zero, BPF_ANY);

    /* Open cgroup directory */
    g_cgroup_fd = open(g_cgroup_path, O_RDONLY | O_DIRECTORY);
    if (g_cgroup_fd < 0) {
        fprintf(stderr, "[SOCKMAP] open(%s): %s\n",
                g_cgroup_path, strerror(errno));
        sockmap_manager_stop();
        return -1;
    }

    /* Attach sockops to cgroup */
    err = bpf_prog_attach(g_sockops_fd, g_cgroup_fd,
                          BPF_CGROUP_SOCK_OPS, 0);
    if (err) {
        fprintf(stderr, "[SOCKMAP] attach sockops to cgroup %s: %s\n",
                g_cgroup_path, strerror(-err));
        sockmap_manager_stop();
        return -1;
    }

    /* Attach stream_verdict to sockmap */
    err = bpf_prog_attach(g_verdict_fd, g_sockmap_fd,
                          BPF_SK_SKB_STREAM_VERDICT, 0);
    if (err) {
        fprintf(stderr, "[SOCKMAP] attach stream_verdict: %s\n",
                strerror(-err));
        /* Roll back the cgroup attach */
        bpf_prog_detach2(g_sockops_fd, g_cgroup_fd, BPF_CGROUP_SOCK_OPS);
        sockmap_manager_stop();
        return -1;
    }

    /* Pin maps so kvs-top (or other observers) can read them out-of-process.
     * Pinning is best-effort: failures don't abort sockmap init. */
    kvs_bpf_pin_map(g_stats_fd, "sockmap_stats");
    kvs_bpf_pin_map(g_cmd_fd,   "cmd_counts");
    kvs_bpf_pin_map(g_conn_fd,  "conn_profiles");

    g_ready = 1;
    printf("[SOCKMAP] attached sockops→cgroup=%s, verdict→sockmap "
           "(port=%d, capacity=%d)\n",
           g_cgroup_path, port, SOCKMAP_MAX_ENTRIES);
    return 0;
}

int sockmap_manager_init_for_pid(int port, pid_t pid)
{
    char path[sizeof(g_cgroup_path)] = "";
    if (pid <= 0) {
        fprintf(stderr, "[SOCKMAP] invalid pid %d\n", (int)pid);
        return -1;
    }
    if (resolve_cgroup_path_generic(pid, path, sizeof(path)) != 0) {
        fprintf(stderr, "[SOCKMAP] could not resolve cgroup for pid %d\n", (int)pid);
        return -1;
    }
    return sockmap_manager_init(port, path);
}

void sockmap_manager_stop(void) {
    if (g_ready) {
        if (g_sockops_fd >= 0 && g_cgroup_fd >= 0)
            bpf_prog_detach2(g_sockops_fd, g_cgroup_fd, BPF_CGROUP_SOCK_OPS);
        if (g_verdict_fd >= 0 && g_sockmap_fd >= 0)
            bpf_prog_detach2(g_verdict_fd, g_sockmap_fd,
                             BPF_SK_SKB_STREAM_VERDICT);

        /* Remove map pins so a subsequent run starts clean. */
        kvs_bpf_unpin_map("sockmap_stats");
        kvs_bpf_unpin_map("cmd_counts");
        kvs_bpf_unpin_map("conn_profiles");
    }

    if (g_cgroup_fd >= 0) {
        close(g_cgroup_fd);
        g_cgroup_fd = -1;
    }

    if (g_obj) {
        bpf_object__close(g_obj);
        g_obj = NULL;
    }

    g_sockmap_fd = -1;
    g_config_fd  = -1;
    g_stats_fd   = -1;
    g_cmd_fd     = -1;
    g_conn_fd    = -1;
    g_sockops_fd = -1;
    g_verdict_fd = -1;

    if (g_ready) {
        g_ready = 0;
        printf("[SOCKMAP] detached and unloaded\n");
    }
}

void sockmap_manager_stats(uint64_t *added,
                            uint64_t *add_fail,
                            uint64_t *verdict,
                            uint64_t *verdict_pass,
                            uint64_t *parsed,
                            uint64_t *parse_skip) {
    if (!g_ready || g_stats_fd < 0) {
        if (added)        *added        = 0;
        if (add_fail)     *add_fail     = 0;
        if (verdict)      *verdict      = 0;
        if (verdict_pass) *verdict_pass = 0;
        if (parsed)       *parsed       = 0;
        if (parse_skip)   *parse_skip   = 0;
        return;
    }
    __u32 k; __u64 v;
    #define READ(idx, out) do { if (out) { k = (idx); v = 0; \
        bpf_map_lookup_elem(g_stats_fd, &k, &v); *(out) = v; } } while (0)
    READ(SOCKMAP_STAT_ADDED,        added);
    READ(SOCKMAP_STAT_ADD_FAIL,     add_fail);
    READ(SOCKMAP_STAT_VERDICT,      verdict);
    READ(SOCKMAP_STAT_VERDICT_PASS, verdict_pass);
    READ(SOCKMAP_STAT_PARSED,       parsed);
    READ(SOCKMAP_STAT_PARSE_SKIP,   parse_skip);
    #undef READ
}

int sockmap_manager_iter_cmds(sockmap_cmd_cb cb, void *uctx)
{
    if (!g_ready || g_cmd_fd < 0 || !cb) return -1;
    struct sockmap_cmd_key prev = {}, next = {};
    int has_prev = 0, n = 0;
    while (bpf_map_get_next_key(g_cmd_fd, has_prev ? &prev : NULL, &next) == 0) {
        struct sockmap_cmd_stats stats = {};
        if (bpf_map_lookup_elem(g_cmd_fd, &next, &stats) == 0) {
            struct sockmap_cmd_report rep = {};
            int copy = next.len > SOCKMAP_CMD_NAME_MAX ? SOCKMAP_CMD_NAME_MAX : next.len;
            memcpy(rep.name, next.name, (size_t)copy);
            rep.name[copy] = '\0';
            rep.count = stats.count;
            rep.bytes = stats.bytes;
            if (cb(&rep, uctx) != 0) break;
            n++;
        }
        prev = next;
        has_prev = 1;
        if (n >= SOCKMAP_CMD_MAX_ENTRIES) break;
    }
    return n;
}

int sockmap_manager_iter_conns(sockmap_conn_cb cb, void *uctx)
{
    if (!g_ready || g_conn_fd < 0 || !cb) return -1;
    __u64 prev = 0, next = 0;
    int has_prev = 0, n = 0;
    while (bpf_map_get_next_key(g_conn_fd, has_prev ? &prev : NULL, &next) == 0) {
        struct sockmap_conn_profile prof = {};
        if (bpf_map_lookup_elem(g_conn_fd, &next, &prof) == 0) {
            struct sockmap_conn_report rep = {
                .cookie        = next,
                .first_seen_ns = prof.first_seen_ns,
                .last_seen_ns  = prof.last_seen_ns,
                .cmd_count     = prof.cmd_count,
                .bytes_in      = prof.bytes_in,
            };
            if (cb(&rep, uctx) != 0) break;
            n++;
        }
        prev = next;
        has_prev = 1;
        if (n >= SOCKMAP_MAX_ENTRIES) break;
    }
    return n;
}

uint64_t sockmap_manager_live_socks(void) {
    if (!g_ready || g_sockmap_fd < 0) return 0;
    /* Iterate the sockhash to count live entries. */
    __u64 prev = 0, next = 0;
    uint64_t n = 0;
    int has_prev = 0;
    while (bpf_map_get_next_key(g_sockmap_fd,
                                  has_prev ? &prev : NULL, &next) == 0) {
        n++;
        prev = next;
        has_prev = 1;
        if (n >= SOCKMAP_MAX_ENTRIES) break;  /* defensive */
    }
    return n;
}
