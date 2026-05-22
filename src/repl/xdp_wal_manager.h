/* xdp_wal_manager.h — XDP program loader and slave device manager
 *
 * Responsibilities:
 *   1. Load wal_xdp.kern.o and attach to network interface
 *   2. Manage slave device map (add/remove slave NICs)
 *   3. Manage slave MAC/IP maps for packet rewriting
 *   4. Monitor XDP statistics and redirect events
 *   5. Graceful cleanup on shutdown
 *
 * Integration with WAL replication:
 *   - Called by ebpf_incr_master_start() to initialize XDP
 *   - Called by ebpf_incr_add_slave() to register slave NICs
 *   - Called by ebpf_incr_stop() to detach XDP and cleanup
 */

#ifndef XDP_WAL_MANAGER_H
#define XDP_WAL_MANAGER_H

#include <stdint.h>
#include <netinet/in.h>

/* Forward declarations */
struct bpf_object;
struct bpf_program;
struct ring_buffer;

/* ── XDP manager context ── */

typedef struct {
    /* BPF object and program */
    struct bpf_object  *obj;
    struct bpf_program *prog;
    int                 prog_fd;
    
    /* BPF maps */
    int                 slave_devmap_fd;
    int                 slave_mac_map_fd;
    int                 slave_ip_map_fd;
    int                 stats_map_fd;
    int                 global_stats_map_fd;
    int                 event_ringbuf_fd;
    
    /* Event ringbuf consumer */
    struct ring_buffer *rb;
    
    /* Network interface */
    char                ifname[16];
    int                 ifindex;
    uint32_t            xdp_flags;
    
    /* State */
    int                 attached;
    int                 initialized;
} xdp_wal_ctx_t;

/* ── Slave device info ── */

typedef struct {
    uint32_t            ifindex;
    uint8_t             mac[6];
    struct in_addr      ip;
    char                ifname[16];
} xdp_slave_info_t;

/* ── XDP statistics ── */

typedef struct {
    uint64_t packets_redirected;
    uint64_t bytes_redirected;
    uint64_t redirect_errors;
} xdp_slave_stats_t;

typedef struct {
    uint64_t total_packets;
    uint64_t wal_packets;
    uint64_t non_wal_packets;
    uint64_t parse_errors;
} xdp_global_stats_t;

/* ── Lifecycle ── */

/* Initialize XDP manager and load BPF program.
 * 
 * Parameters:
 *   ctx       - XDP manager context to initialize
 *   ifname    - Network interface to attach XDP program to
 *   xdp_flags - XDP attachment flags (XDP_FLAGS_DRV_MODE, XDP_FLAGS_SKB_MODE, etc.)
 * 
 * Returns:
 *   0 on success, -1 on failure
 * 
 * Notes:
 *   - Loads wal_xdp.kern.o from build/ directory
 *   - Attaches XDP program to the specified interface
 *   - Opens all BPF maps for slave management
 *   - Starts ringbuf consumer thread for redirect events
 *   - XDP_FLAGS_DRV_MODE requires NIC driver support (fastest)
 *   - XDP_FLAGS_SKB_MODE works on any NIC (slower, but compatible)
 *   - Soft-RoCE (rxe) only supports SKB_MODE
 */
int xdp_wal_init(xdp_wal_ctx_t *ctx, const char *ifname, uint32_t xdp_flags);

/* Detach XDP program and cleanup resources.
 * 
 * Parameters:
 *   ctx - XDP manager context to cleanup
 * 
 * Notes:
 *   - Detaches XDP program from network interface
 *   - Closes all BPF maps
 *   - Stops ringbuf consumer thread
 *   - Frees BPF object
 */
void xdp_wal_cleanup(xdp_wal_ctx_t *ctx);

/* ── Slave management ── */

/* Register a slave NIC for WAL multicast.
 * 
 * Parameters:
 *   ctx        - XDP manager context
 *   slave_idx  - Slave index (0-7)
 *   slave_info - Slave device information (ifindex, MAC, IP)
 * 
 * Returns:
 *   0 on success, -1 on failure
 * 
 * Notes:
 *   - Updates slave_devmap with slave's ifindex
 *   - Updates slave_mac_map with slave's MAC address
 *   - Updates slave_ip_map with slave's IP address
 *   - XDP program will start redirecting packets to this slave
 */
int xdp_wal_add_slave(xdp_wal_ctx_t *ctx, uint32_t slave_idx, 
                      const xdp_slave_info_t *slave_info);

/* Unregister a slave NIC.
 * 
 * Parameters:
 *   ctx       - XDP manager context
 *   slave_idx - Slave index (0-7)
 * 
 * Returns:
 *   0 on success, -1 on failure
 * 
 * Notes:
 *   - Removes slave from slave_devmap (sets ifindex to 0)
 *   - XDP program will stop redirecting packets to this slave
 */
int xdp_wal_remove_slave(xdp_wal_ctx_t *ctx, uint32_t slave_idx);

/* ── Statistics ── */

/* Get per-slave XDP statistics.
 * 
 * Parameters:
 *   ctx       - XDP manager context
 *   slave_idx - Slave index (0-7)
 *   stats     - Output: slave statistics
 * 
 * Returns:
 *   0 on success, -1 on failure
 * 
 * Notes:
 *   - Reads from per-CPU stats map and aggregates
 *   - Statistics are cumulative since XDP program load
 */
int xdp_wal_get_slave_stats(xdp_wal_ctx_t *ctx, uint32_t slave_idx,
                             xdp_slave_stats_t *stats);

/* Get global XDP statistics.
 * 
 * Parameters:
 *   ctx   - XDP manager context
 *   stats - Output: global statistics
 * 
 * Returns:
 *   0 on success, -1 on failure
 */
int xdp_wal_get_global_stats(xdp_wal_ctx_t *ctx, xdp_global_stats_t *stats);

/* Reset all XDP statistics to zero.
 * 
 * Parameters:
 *   ctx - XDP manager context
 * 
 * Returns:
 *   0 on success, -1 on failure
 */
int xdp_wal_reset_stats(xdp_wal_ctx_t *ctx);

/* ── Utility ── */

/* Resolve slave device information from IP address.
 * 
 * Parameters:
 *   ip_str     - Slave IP address (string, e.g., "192.168.1.100")
 *   slave_info - Output: resolved device information
 * 
 * Returns:
 *   0 on success, -1 on failure
 * 
 * Notes:
 *   - Queries ARP table to find MAC address
 *   - Queries routing table to find outgoing interface
 *   - Fills in ifindex, MAC, IP, and ifname
 */
int xdp_wal_resolve_slave(const char *ip_str, xdp_slave_info_t *slave_info);

/* Check if XDP driver mode is supported on the interface.
 * 
 * Parameters:
 *   ifname - Network interface name
 * 
 * Returns:
 *   1 if driver mode is supported, 0 otherwise
 */
int xdp_wal_check_drv_support(const char *ifname);

/* Print XDP statistics to stdout (for debugging/monitoring).
 * 
 * Parameters:
 *   ctx - XDP manager context
 */
void xdp_wal_print_stats(xdp_wal_ctx_t *ctx);

#endif /* XDP_WAL_MANAGER_H */
