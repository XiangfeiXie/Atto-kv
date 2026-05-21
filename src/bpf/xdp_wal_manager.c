/* xdp_wal_manager.c — XDP program loader and slave device manager
 *
 * Loads wal_xdp.kern.o, attaches to NIC, manages slave device maps,
 * and monitors redirect events via ringbuf.
 */

#define _GNU_SOURCE
#include "xdp_wal_manager.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <linux/if_link.h>
#include <linux/if_ether.h>

#include <bpf/libbpf.h>
#include <bpf/bpf.h>
#include <xdp/libxdp.h>

/* ── Ringbuf event handler ── */

struct redirect_event {
    uint32_t slave_idx;
    int32_t  error_code;
    uint64_t timestamp;
};

static int handle_redirect_event(void *ctx, void *data, size_t len)
{
    (void)ctx;
    
    if (len < sizeof(struct redirect_event)) {
        return 0;
    }
    
    struct redirect_event *event = (struct redirect_event *)data;
    
    fprintf(stderr, "[XDP-WAL] redirect failed: slave %u, error %d, ts %lu\n",
            event->slave_idx, event->error_code, event->timestamp);
    
    return 0;
}

/* ── Lifecycle ── */

int xdp_wal_init(xdp_wal_ctx_t *ctx, const char *ifname, uint32_t xdp_flags)
{
    if (!ctx || !ifname) {
        fprintf(stderr, "[XDP-WAL] invalid arguments\n");
        return -1;
    }
    
    memset(ctx, 0, sizeof(*ctx));
    strncpy(ctx->ifname, ifname, sizeof(ctx->ifname) - 1);
    ctx->xdp_flags = xdp_flags;
    
    ctx->ifindex = if_nametoindex(ifname);
    if (ctx->ifindex == 0) {
        fprintf(stderr, "[XDP-WAL] interface %s not found: %s\n",
                ifname, strerror(errno));
        return -1;
    }
    
    char obj_path[256];
    snprintf(obj_path, sizeof(obj_path), "build/wal_xdp.kern.o");
    
    ctx->obj = bpf_object__open_file(obj_path, NULL);
    if (!ctx->obj) {
        fprintf(stderr, "[XDP-WAL] failed to open %s: %s\n",
                obj_path, strerror(errno));
        return -1;
    }
    
    if (bpf_object__load(ctx->obj) != 0) {
        fprintf(stderr, "[XDP-WAL] failed to load BPF object: %s\n",
                strerror(errno));
        goto err_close_obj;
    }
    
    ctx->prog = bpf_object__find_program_by_name(ctx->obj, "wal_xdp_redirect");
    if (!ctx->prog) {
        fprintf(stderr, "[XDP-WAL] program 'wal_xdp_redirect' not found\n");
        goto err_close_obj;
    }
    
    ctx->prog_fd = bpf_program__fd(ctx->prog);
    if (ctx->prog_fd < 0) {
        fprintf(stderr, "[XDP-WAL] failed to get program fd\n");
        goto err_close_obj;
    }
    
    ctx->slave_devmap_fd = bpf_object__find_map_fd_by_name(ctx->obj, "slave_devmap");
    ctx->slave_mac_map_fd = bpf_object__find_map_fd_by_name(ctx->obj, "slave_mac_map");
    ctx->slave_ip_map_fd = bpf_object__find_map_fd_by_name(ctx->obj, "slave_ip_map");
    ctx->stats_map_fd = bpf_object__find_map_fd_by_name(ctx->obj, "stats_map");
    ctx->global_stats_map_fd = bpf_object__find_map_fd_by_name(ctx->obj, "global_stats_map");
    ctx->event_ringbuf_fd = bpf_object__find_map_fd_by_name(ctx->obj, "event_ringbuf");
    
    if (ctx->slave_devmap_fd < 0 || ctx->slave_mac_map_fd < 0 ||
        ctx->slave_ip_map_fd < 0 || ctx->stats_map_fd < 0 ||
        ctx->global_stats_map_fd < 0 || ctx->event_ringbuf_fd < 0) {
        fprintf(stderr, "[XDP-WAL] failed to find BPF maps\n");
        goto err_close_obj;
    }
    
    if (bpf_xdp_attach(ctx->ifindex, ctx->prog_fd, xdp_flags, NULL) != 0) {
        fprintf(stderr, "[XDP-WAL] failed to attach XDP program to %s: %s\n",
                ifname, strerror(errno));
        goto err_close_obj;
    }
    
    ctx->attached = 1;
    
    ctx->rb = ring_buffer__new(ctx->event_ringbuf_fd, handle_redirect_event, NULL, NULL);
    if (!ctx->rb) {
        fprintf(stderr, "[XDP-WAL] failed to create ringbuf: %s\n",
                strerror(errno));
        goto err_detach;
    }
    
    ctx->initialized = 1;
    
    printf("[XDP-WAL] initialized on %s (ifindex %d, flags 0x%x)\n",
           ifname, ctx->ifindex, xdp_flags);
    
    return 0;

err_detach:
    bpf_xdp_detach(ctx->ifindex, xdp_flags, NULL);
    ctx->attached = 0;
err_close_obj:
    bpf_object__close(ctx->obj);
    ctx->obj = NULL;
    return -1;
}

void xdp_wal_cleanup(xdp_wal_ctx_t *ctx)
{
    if (!ctx || !ctx->initialized) return;
    
    if (ctx->rb) {
        ring_buffer__free(ctx->rb);
        ctx->rb = NULL;
    }
    
    if (ctx->attached) {
        bpf_xdp_detach(ctx->ifindex, ctx->xdp_flags, NULL);
        ctx->attached = 0;
    }
    
    if (ctx->obj) {
        bpf_object__close(ctx->obj);
        ctx->obj = NULL;
    }
    
    ctx->initialized = 0;
    printf("[XDP-WAL] cleaned up %s\n", ctx->ifname);
}

/* ── Slave management ── */

int xdp_wal_add_slave(xdp_wal_ctx_t *ctx, uint32_t slave_idx,
                      const xdp_slave_info_t *slave_info)
{
    if (!ctx || !ctx->initialized || !slave_info) {
        return -1;
    }
    
    if (slave_idx >= 8) {
        fprintf(stderr, "[XDP-WAL] invalid slave index %u\n", slave_idx);
        return -1;
    }
    
    if (bpf_map_update_elem(ctx->slave_devmap_fd, &slave_idx,
                            &slave_info->ifindex, BPF_ANY) != 0) {
        fprintf(stderr, "[XDP-WAL] failed to update devmap[%u]: %s\n",
                slave_idx, strerror(errno));
        return -1;
    }
    
    if (bpf_map_update_elem(ctx->slave_mac_map_fd, &slave_idx,
                            slave_info->mac, BPF_ANY) != 0) {
        fprintf(stderr, "[XDP-WAL] failed to update mac_map[%u]: %s\n",
                slave_idx, strerror(errno));
        return -1;
    }
    
    uint32_t ip_net = slave_info->ip.s_addr;
    if (bpf_map_update_elem(ctx->slave_ip_map_fd, &slave_idx,
                            &ip_net, BPF_ANY) != 0) {
        fprintf(stderr, "[XDP-WAL] failed to update ip_map[%u]: %s\n",
                slave_idx, strerror(errno));
        return -1;
    }
    
    char ip_str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &slave_info->ip, ip_str, sizeof(ip_str));
    
    printf("[XDP-WAL] added slave %u: %s (ifindex %u, MAC %02x:%02x:%02x:%02x:%02x:%02x)\n",
           slave_idx, ip_str, slave_info->ifindex,
           slave_info->mac[0], slave_info->mac[1], slave_info->mac[2],
           slave_info->mac[3], slave_info->mac[4], slave_info->mac[5]);
    
    return 0;
}

int xdp_wal_remove_slave(xdp_wal_ctx_t *ctx, uint32_t slave_idx)
{
    if (!ctx || !ctx->initialized) {
        return -1;
    }
    
    if (slave_idx >= 8) {
        return -1;
    }
    
    uint32_t zero_ifindex = 0;
    if (bpf_map_update_elem(ctx->slave_devmap_fd, &slave_idx,
                            &zero_ifindex, BPF_ANY) != 0) {
        fprintf(stderr, "[XDP-WAL] failed to remove slave %u: %s\n",
                slave_idx, strerror(errno));
        return -1;
    }
    
    printf("[XDP-WAL] removed slave %u\n", slave_idx);
    return 0;
}

/* ── Statistics ── */

int xdp_wal_get_slave_stats(xdp_wal_ctx_t *ctx, uint32_t slave_idx,
                             xdp_slave_stats_t *stats)
{
    if (!ctx || !ctx->initialized || !stats || slave_idx >= 8) {
        return -1;
    }
    
    memset(stats, 0, sizeof(*stats));
    
    struct {
        uint64_t packets_redirected;
        uint64_t bytes_redirected;
        uint64_t redirect_errors;
    } percpu_stats;
    
    int num_cpus = libbpf_num_possible_cpus();
    if (num_cpus <= 0) num_cpus = 1;
    
    for (int cpu = 0; cpu < num_cpus; cpu++) {
        if (bpf_map_lookup_elem(ctx->stats_map_fd, &slave_idx, &percpu_stats) == 0) {
            stats->packets_redirected += percpu_stats.packets_redirected;
            stats->bytes_redirected += percpu_stats.bytes_redirected;
            stats->redirect_errors += percpu_stats.redirect_errors;
        }
    }
    
    return 0;
}

int xdp_wal_get_global_stats(xdp_wal_ctx_t *ctx, xdp_global_stats_t *stats)
{
    if (!ctx || !ctx->initialized || !stats) {
        return -1;
    }
    
    memset(stats, 0, sizeof(*stats));
    
    uint32_t zero = 0;
    struct {
        uint64_t total_packets;
        uint64_t wal_packets;
        uint64_t non_wal_packets;
        uint64_t parse_errors;
    } percpu_stats;
    
    int num_cpus = libbpf_num_possible_cpus();
    if (num_cpus <= 0) num_cpus = 1;
    
    for (int cpu = 0; cpu < num_cpus; cpu++) {
        if (bpf_map_lookup_elem(ctx->global_stats_map_fd, &zero, &percpu_stats) == 0) {
            stats->total_packets += percpu_stats.total_packets;
            stats->wal_packets += percpu_stats.wal_packets;
            stats->non_wal_packets += percpu_stats.non_wal_packets;
            stats->parse_errors += percpu_stats.parse_errors;
        }
    }
    
    return 0;
}

int xdp_wal_reset_stats(xdp_wal_ctx_t *ctx)
{
    if (!ctx || !ctx->initialized) {
        return -1;
    }
    
    struct {
        uint64_t packets_redirected;
        uint64_t bytes_redirected;
        uint64_t redirect_errors;
    } zero_slave_stats = {0};
    
    for (uint32_t i = 0; i < 8; i++) {
        bpf_map_update_elem(ctx->stats_map_fd, &i, &zero_slave_stats, BPF_ANY);
    }
    
    struct {
        uint64_t total_packets;
        uint64_t wal_packets;
        uint64_t non_wal_packets;
        uint64_t parse_errors;
    } zero_global_stats = {0};
    
    uint32_t zero = 0;
    bpf_map_update_elem(ctx->global_stats_map_fd, &zero, &zero_global_stats, BPF_ANY);
    
    return 0;
}

/* ── Utility ── */

int xdp_wal_resolve_slave(const char *ip_str, xdp_slave_info_t *slave_info)
{
    if (!ip_str || !slave_info) {
        return -1;
    }
    
    memset(slave_info, 0, sizeof(*slave_info));
    
    if (inet_pton(AF_INET, ip_str, &slave_info->ip) != 1) {
        fprintf(stderr, "[XDP-WAL] invalid IP address: %s\n", ip_str);
        return -1;
    }
    
    fprintf(stderr, "[XDP-WAL] WARNING: ARP/routing resolution not implemented\n");
    fprintf(stderr, "[XDP-WAL] Please manually configure slave MAC and ifindex\n");
    
    return -1;
}

int xdp_wal_check_drv_support(const char *ifname)
{
    if (!ifname) return 0;
    
    int ifindex = if_nametoindex(ifname);
    if (ifindex == 0) return 0;
    
    char obj_path[256];
    snprintf(obj_path, sizeof(obj_path), "build/wal_xdp.kern.o");
    
    struct bpf_object *obj = bpf_object__open_file(obj_path, NULL);
    if (!obj) return 0;
    
    if (bpf_object__load(obj) != 0) {
        bpf_object__close(obj);
        return 0;
    }
    
    struct bpf_program *prog = bpf_object__find_program_by_name(obj, "wal_xdp_redirect");
    if (!prog) {
        bpf_object__close(obj);
        return 0;
    }
    
    int prog_fd = bpf_program__fd(prog);
    if (prog_fd < 0) {
        bpf_object__close(obj);
        return 0;
    }
    
    int ret = bpf_xdp_attach(ifindex, prog_fd, XDP_FLAGS_DRV_MODE, NULL);
    int supported = (ret == 0);
    
    if (supported) {
        bpf_xdp_detach(ifindex, XDP_FLAGS_DRV_MODE, NULL);
    }
    
    bpf_object__close(obj);
    
    return supported;
}

void xdp_wal_print_stats(xdp_wal_ctx_t *ctx)
{
    if (!ctx || !ctx->initialized) {
        return;
    }
    
    xdp_global_stats_t gstats;
    if (xdp_wal_get_global_stats(ctx, &gstats) == 0) {
        printf("[XDP-WAL] Global stats:\n");
        printf("  total_packets:     %lu\n", gstats.total_packets);
        printf("  wal_packets:       %lu\n", gstats.wal_packets);
        printf("  non_wal_packets:   %lu\n", gstats.non_wal_packets);
        printf("  parse_errors:      %lu\n", gstats.parse_errors);
    }
    
    for (uint32_t i = 0; i < 8; i++) {
        xdp_slave_stats_t stats;
        if (xdp_wal_get_slave_stats(ctx, i, &stats) == 0) {
            if (stats.packets_redirected > 0 || stats.redirect_errors > 0) {
                printf("[XDP-WAL] Slave %u stats:\n", i);
                printf("  packets_redirected: %lu\n", stats.packets_redirected);
                printf("  bytes_redirected:   %lu\n", stats.bytes_redirected);
                printf("  redirect_errors:    %lu\n", stats.redirect_errors);
            }
        }
    }
    
    if (ctx->rb) {
        ring_buffer__poll(ctx->rb, 0);
    }
}
