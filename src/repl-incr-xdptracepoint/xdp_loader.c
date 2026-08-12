/* xdp_loader.c — XDP BPF program loader and attach/detach implementation
 *
 * Extracted from xdp_reasm.c.  Handles finding the BPF .o binary, loading
 * BPF bytecode, attaching to the target interface (bpf_link or netlink),
 * and creating the ringbuf consumer.
 */

#define _GNU_SOURCE
#include "xdp_loader.h"
#include "xdp_common.h"
#include "config.h"

#include <bpf/bpf.h>
#include <errno.h>
#include <linux/if_link.h>
#include <net/if.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* ── XDP detach ────────────────────────────────────────────────────────── */

void xdp_detach(xdp_attach_t *a)
{
    if (!a) return;

    if (a->use_netlink) {
        if (a->prog_fd >= 0 && a->ifindex > 0)
            bpf_xdp_detach(a->ifindex, 0, NULL);
    } else {
        if (a->link)
            bpf_link__destroy(a->link);
    }
    a->link    = NULL;
    a->prog_fd = -1;
}

/* ── XDP load + attach + ringbuf create ────────────────────────────────── */

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

    /* ── Resolve interface ──────────────────────────────────────────── */

    out_att->ifindex = if_nametoindex(iface_name);
    if (unlikely(out_att->ifindex == 0)) {
        fprintf(stderr, "[XDP-LOAD] interface not found: '%s'\n", iface_name);
        fprintf(stderr, "[XDP-LOAD] set XDP_IFACE. Available:\n");
        for (int i = 1; i <= 10; i++) {
            char name[IF_NAMESIZE];
            if (if_indextoname(i, name))
                fprintf(stderr, "[XDP-LOAD]   %s\n", name);
        }
        return -1;
    }

    /* ── Find BPF object file ───────────────────────────────────────── */

    char obj_path[512];
    const char *build_dir = getenv("BUILD_DIR");
    if (build_dir) {
        snprintf(obj_path, sizeof(obj_path),
                 "%s/xdp_filter.bpf.o", build_dir);
    } else {
        char self[256];
        ssize_t n = readlink("/proc/self/exe", self, sizeof(self) - 1);
        if (likely(n > 0)) {
            self[n] = '\0';
            char *slash = strrchr(self, '/');
            if (slash) *slash = '\0';
            snprintf(obj_path, sizeof(obj_path),
                     "%s/xdp_filter.bpf.o", self);
            if (access(obj_path, R_OK) != 0)
                snprintf(obj_path, sizeof(obj_path),
                         "build/xdp_filter.bpf.o");
        } else {
            snprintf(obj_path, sizeof(obj_path),
                     "build/xdp_filter.bpf.o");
        }
    }

    if (unlikely(access(obj_path, R_OK) != 0)) {
        fprintf(stderr, "[XDP-LOAD] BPF object not found: %s\n", obj_path);
        return -1;
    }

    /* ── Open and load BPF ──────────────────────────────────────────── */

    struct bpf_object *obj = bpf_object__open_file(obj_path, NULL);
    if (unlikely(!obj)) {
        fprintf(stderr, "[XDP-LOAD] bpf_object__open_file: %s\n",
                strerror(errno));
        return -1;
    }
    if (unlikely(bpf_object__load(obj) != 0)) {
        fprintf(stderr, "[XDP-LOAD] bpf_object__load: %s\n",
                strerror(errno));
        bpf_object__close(obj);
        return -1;
    }

    /* ── Set kvstore port in BPF config map ─────────────────────────── */

    struct bpf_map *cfg_map =
        bpf_object__find_map_by_name(obj, "repl_config");
    if (cfg_map) {
        uint32_t key = 0, val = (uint32_t)kv_port;
        bpf_map__update_elem(cfg_map, &key, sizeof(key),
                             &val, sizeof(val), BPF_ANY);
    }

    /* ── Find XDP program ───────────────────────────────────────────── */

    struct bpf_program *prog =
        bpf_object__find_program_by_name(obj, "xdp_repl_sniff");
    if (unlikely(!prog)) {
        fprintf(stderr, "[XDP-LOAD] program 'xdp_repl_sniff' not found\n");
        bpf_object__close(obj);
        return -1;
    }

    /* ── Attach XDP ─────────────────────────────────────────────────── */

    const char *mode_str = getenv("XDP_MODE");
    const char *mode_label = "auto";

    if (!mode_str || strcmp(mode_str, "auto") == 0) {
        out_att->link = bpf_program__attach_xdp(prog, out_att->ifindex);
        if (unlikely(!out_att->link)) {
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
        if (unlikely(bpf_xdp_attach(out_att->ifindex,
                                     out_att->prog_fd,
                                     flags, NULL) != 0)) {
            fprintf(stderr, "[XDP-LOAD] XDP %s attach to '%s' failed: %s\n",
                    mode_label, iface_name, strerror(errno));
            bpf_object__close(obj);
            return -1;
        }
        out_att->use_netlink = 1;
    }

    /* ── Create ringbuf consumer ────────────────────────────────────── */

    struct bpf_map *rb_map =
        bpf_object__find_map_by_name(obj, "repl_ringbuf");
    if (unlikely(!rb_map)) {
        fprintf(stderr, "[XDP-LOAD] ringbuf map not found\n");
        xdp_detach(out_att);
        bpf_object__close(obj);
        return -1;
    }

    struct ring_buffer *rb = ring_buffer__new(bpf_map__fd(rb_map),
                                              ringbuf_cb, NULL, NULL);
    if (unlikely(!rb)) {
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

/* Read BPF-side statistics counters from the "repl_stats" percpu array.
 * Must be called before bpf_object__close(). */
void xdp_print_bpf_stats(struct bpf_object *obj) {
    if (!obj) return;

    struct bpf_map *map = bpf_object__find_map_by_name(obj, "repl_stats");
    if (!map) return;

    /* Read percpu array — aggregate across all CPUs */
    uint64_t total = 0, submitted = 0, full = 0, probe_fail = 0;
    int fd = bpf_map__fd(map);
    int ncpus = libbpf_num_possible_cpus();
    if (ncpus <= 0) ncpus = 1;

    for (int i = 0; i < XDP_CNT_NUM; i++) {
        uint64_t sum = 0;
        uint32_t key = (uint32_t)i;
        /* Read per-CPU values and sum them */
        uint64_t vals[ncpus];
        memset(vals, 0, sizeof(vals));
        if (bpf_map_lookup_elem(fd, &key, vals) == 0) {
            for (int cpu = 0; cpu < ncpus; cpu++)
                sum += vals[cpu];
        }
        switch (i) {
        case XDP_CNT_SUBMITTED:    submitted   = sum; break;
        case XDP_CNT_RINGBUF_FULL: full        = sum; break;
        case XDP_CNT_PROBE_FAIL:   probe_fail  = sum; break;
        case XDP_CNT_TOTAL:        total       = sum; break;
        }
    }
    printf("[XDP-BPF] total=%lu submitted=%lu ringbuf_full=%lu probe_fail=%lu\n",
           (unsigned long)total,
           (unsigned long)submitted,
           (unsigned long)full,
           (unsigned long)probe_fail);
}
