/* xdp_manager.c — userspace side of the XDP packet monitor.
 *
 * Responsibilities:
 *   - Load xdp_kvs.kern.o (BPF ELF) via libbpf.
 *   - Attach the XDP program to the target network interface.
 *   - Expose stats query.
 *   - Clean shutdown (detach + unload).
 */

#define _GNU_SOURCE
#include "xdp_manager.h"
#include "xdp_kvs.h"
#include "bpf_pin.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <net/if.h>

#include <bpf/libbpf.h>
#include <bpf/bpf.h>
#include <linux/if_link.h>

/* ── Module state ──────────────────────────────────────────────────────── */

static struct bpf_object *g_obj        = NULL;
static struct bpf_link   *g_link       = NULL;
static int                g_config_fd  = -1;   /* config_map fd     */
static int                g_stats_fd   = -1;   /* stats_map fd      */
static int                g_ifindex    = 0;
static int                g_xdp_ready  = 0;

/* Path to compiled BPF object — resolved relative to executable dir at runtime */
static char g_obj_path[1024] = "";

/* ── Helpers ───────────────────────────────────────────────────────────── */

static void build_obj_path(void) {
    /* Resolve the BPF object next to the running executable */
    char self[512] = {};
    ssize_t n = readlink("/proc/self/exe", self, sizeof(self) - 1);
    if (n <= 0) {
        snprintf(g_obj_path, sizeof(g_obj_path), "xdp_kvs.kern.o");
        return;
    }
    /* Strip the executable name, append the BPF object name */
    char *slash = strrchr(self, '/');
    if (slash) *slash = '\0';
    snprintf(g_obj_path, sizeof(g_obj_path), "%s/xdp_kvs.kern.o", self);
}

static int libbpf_print_fn(enum libbpf_print_level level,
                            const char *format, va_list args) {
    if (level == LIBBPF_DEBUG) return 0;   /* suppress debug noise */
    return vfprintf(stderr, format, args);
}

/* ── Public API ─────────────────────────────────────────────────────────── */

int xdp_manager_init(const char *iface, int port) {
    if (!iface || iface[0] == '\0') {
        fprintf(stderr, "[XDP] no interface specified, XDP disabled\n");
        return -1;
    }

    g_ifindex = (int)if_nametoindex(iface);
    if (g_ifindex == 0) {
        fprintf(stderr, "[XDP] interface '%s' not found: %s\n",
                iface, strerror(errno));
        return -1;
    }

    build_obj_path();
    if (access(g_obj_path, R_OK) != 0) {
        fprintf(stderr, "[XDP] BPF object not found at %s — "
                "run cmake --build to compile it\n", g_obj_path);
        return -1;
    }

    libbpf_set_print(libbpf_print_fn);

    /* Open BPF object */
    struct bpf_object_open_opts opts = {
        .sz = sizeof(opts),
    };
    g_obj = bpf_object__open_file(g_obj_path, &opts);
    if (!g_obj) {
        fprintf(stderr, "[XDP] bpf_object__open_file(%s): %s\n",
                g_obj_path, strerror(errno));
        return -1;
    }

    /* Load (verify + JIT) */
    int err = bpf_object__load(g_obj);
    if (err) {
        fprintf(stderr, "[XDP] bpf_object__load: %s\n", strerror(-err));
        bpf_object__close(g_obj);
        g_obj = NULL;
        return -1;
    }

    /* Get program handle */
    struct bpf_program *prog = bpf_object__find_program_by_name(g_obj,
                                                                  "xdp_kvs_handler");
    if (!prog) {
        fprintf(stderr, "[XDP] program 'xdp_kvs_handler' not found in ELF\n");
        bpf_object__close(g_obj);
        g_obj = NULL;
        return -1;
    }

    /* Attach to interface (XDP native mode, fallback to SKB mode) */
    g_link = bpf_program__attach_xdp(prog, g_ifindex);
    if (!g_link) {
        fprintf(stderr, "[XDP] attach XDP native failed, trying SKB mode...\n");
        /* SKB mode works on virtual/loopback interfaces */
        err = bpf_xdp_attach(g_ifindex, bpf_program__fd(prog),
                             XDP_FLAGS_SKB_MODE, NULL);
        if (err) {
            fprintf(stderr, "[XDP] attach XDP SKB mode failed: %s\n",
                    strerror(-err));
            bpf_object__close(g_obj);
            g_obj = NULL;
            return -1;
        }
        g_link = NULL;  /* attached without bpf_link */
    }

    /* Resolve map file descriptors */
    struct bpf_map *m;

    m = bpf_object__find_map_by_name(g_obj, "config_map");
    g_config_fd = m ? bpf_map__fd(m) : -1;

    m = bpf_object__find_map_by_name(g_obj, "stats_map");
    g_stats_fd  = m ? bpf_map__fd(m) : -1;

    if (g_config_fd < 0) {
        fprintf(stderr, "[XDP] failed to find required BPF maps\n");
        xdp_manager_stop();
        return -1;
    }

    /* Write service port into config map (index 0) */
    __u32 cfg_key  = 0;
    __u32 cfg_val  = (__u32)port;
    bpf_map_update_elem(g_config_fd, &cfg_key, &cfg_val, BPF_ANY);

    /* Zero out stats */
    if (g_stats_fd >= 0) {
        __u64 zero = 0;
        for (__u32 i = 0; i < 4; i++)
            bpf_map_update_elem(g_stats_fd, &i, &zero, BPF_ANY);
    }

    /* Pin maps so external observers (kvs-top) can read them. Best-effort. */
    if (g_stats_fd >= 0) kvs_bpf_pin_map(g_stats_fd, "xdp_stats");

    g_xdp_ready = 1;
    printf("[XDP] attached to %s (ifindex=%d) port=%d\n",
           iface, g_ifindex, port);
    return 0;
}

void xdp_manager_stats(uint64_t *hits, uint64_t *misses,
                        uint64_t *tx_drop, uint64_t *bypass) {
    if (!g_xdp_ready || g_stats_fd < 0) {
        if (hits)    *hits    = 0;
        if (misses)  *misses  = 0;
        if (tx_drop) *tx_drop = 0;
        if (bypass)  *bypass  = 0;
        return;
    }

    __u32 k;
    __u64 v = 0;

    if (hits)    { k = XDP_STAT_GET;        v = 0; bpf_map_lookup_elem(g_stats_fd, &k, &v); *hits    = v; }
    if (misses)  { k = XDP_STAT_PARSE_SKIP; v = 0; bpf_map_lookup_elem(g_stats_fd, &k, &v); *misses  = v; }
    if (tx_drop) { k = XDP_STAT_RESERVED;   v = 0; bpf_map_lookup_elem(g_stats_fd, &k, &v); *tx_drop = v; }
    if (bypass)  { k = XDP_STAT_BYPASS;  v = 0; bpf_map_lookup_elem(g_stats_fd, &k, &v); *bypass  = v; }
}

void xdp_manager_stop(void) {
    if (g_xdp_ready) {
        kvs_bpf_unpin_map("xdp_stats");
    }

    if (g_link) {
        bpf_link__destroy(g_link);
        g_link = NULL;
    } else if (g_ifindex > 0) {
        /* Detach when attached without bpf_link (SKB mode path) */
        bpf_xdp_detach(g_ifindex, XDP_FLAGS_SKB_MODE, NULL);
    }

    if (g_obj) {
        bpf_object__close(g_obj);
        g_obj = NULL;
    }

    g_config_fd = -1;
    g_stats_fd  = -1;
    g_ifindex   = 0;
    g_xdp_ready = 0;

    printf("[XDP] detached and unloaded\n");
}
