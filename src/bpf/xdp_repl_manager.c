#include "xdp_repl_manager.h"

#include <stdio.h>
#include <string.h>

static int g_xdp_repl_started = 0;
static char g_xdp_repl_ifname[64];
static int g_xdp_repl_queue = 0;
static int g_xdp_repl_port = 0;
static int g_xdp_repl_zero_copy_required = 0;

int xdp_repl_manager_init(const char *ifname, int queue_id, int udp_port,
                          int zero_copy_required)
{
    if (!ifname || udp_port <= 0) return -1;
    snprintf(g_xdp_repl_ifname, sizeof(g_xdp_repl_ifname), "%s", ifname);
    g_xdp_repl_queue = queue_id;
    g_xdp_repl_port = udp_port;
    g_xdp_repl_zero_copy_required = zero_copy_required;
    g_xdp_repl_started = 1;
    printf("[XDP-REPL] AF_XDP manager staged: iface=%s queue=%d udp_port=%d zero_copy=%s\n",
           g_xdp_repl_ifname,
           g_xdp_repl_queue,
           g_xdp_repl_port,
           g_xdp_repl_zero_copy_required ? "require" : "auto");
    return 0;
}

int xdp_repl_send_fragment(const kvs_repl_frame_hdr_t *hdr,
                           const char *payload, uint32_t len)
{
    if (!g_xdp_repl_started || !hdr || (!payload && len > 0)) return -1;
    return 0;
}

int xdp_repl_poll(void)
{
    if (!g_xdp_repl_started) return -1;
    return 0;
}

void xdp_repl_manager_stop(void)
{
    if (!g_xdp_repl_started) return;
    printf("[XDP-REPL] manager stopped\n");
    g_xdp_repl_started = 0;
}
