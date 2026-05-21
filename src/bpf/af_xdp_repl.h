/* af_xdp_repl.h — AF_XDP socket manager for zero-copy WAL replication
 *
 * Architecture:
 *   AF_XDP (Address Family XDP) provides a fast path for user-space
 *   packet processing with zero-copy semantics. Packets are placed
 *   directly into a shared memory region (UMEM) that both kernel and
 *   user space can access without copying.
 *
 * Components:
 *   - UMEM: shared memory region for packet buffers
 *   - Fill Queue (FQ): user space provides empty buffers to kernel
 *   - Completion Queue (CQ): kernel returns sent buffers to user space
 *   - RX Queue: kernel delivers received packets to user space
 *   - TX Queue: user space submits packets for transmission
 *
 * Integration with WAL replication:
 *   consumer_thread() → af_xdp_wal_send() → TX queue → XDP program
 *   → multicast to slave NICs → slave receives via normal TCP stack
 *
 * Performance characteristics:
 *   - Zero-copy: packets written directly to NIC DMA region
 *   - Batching: submit multiple packets per syscall
 *   - Polling: avoid interrupt overhead
 *   - CPU pinning: dedicated core for packet processing
 */

#ifndef AF_XDP_REPL_H
#define AF_XDP_REPL_H

#include <stdint.h>
#include <stddef.h>

/* Forward declarations for libxdp types */
struct xsk_socket;
struct xsk_umem;
struct xsk_ring_prod;
struct xsk_ring_cons;

/* ── Configuration ── */

#define AF_XDP_UMEM_SIZE        (64 * 1024 * 1024)  /* 64 MB */
#define AF_XDP_FRAME_SIZE       4096                /* 4 KB per frame */
#define AF_XDP_NUM_FRAMES       (AF_XDP_UMEM_SIZE / AF_XDP_FRAME_SIZE)
#define AF_XDP_BATCH_SIZE       64                  /* TX batch size */
#define AF_XDP_FILL_RING_SIZE   2048
#define AF_XDP_COMP_RING_SIZE   2048
#define AF_XDP_RX_RING_SIZE     2048
#define AF_XDP_TX_RING_SIZE     2048

/* ── AF_XDP context (per network interface) ── */

typedef struct {
    /* libxdp handles */
    struct xsk_socket      *xsk;
    struct xsk_umem        *umem;
    
    /* UMEM region */
    void                   *umem_area;
    size_t                  umem_size;
    
    /* Ring buffers */
    struct xsk_ring_prod   *fq;   /* Fill Queue (user → kernel) */
    struct xsk_ring_cons   *cq;   /* Completion Queue (kernel → user) */
    struct xsk_ring_prod   *tx;   /* TX Queue (user → kernel) */
    struct xsk_ring_cons   *rx;   /* RX Queue (kernel → user) */
    
    /* Frame management */
    uint64_t               *free_frames;  /* Stack of free frame indices */
    uint32_t                free_count;   /* Number of free frames */
    uint32_t                frame_size;
    
    /* Network interface */
    char                    ifname[16];
    int                     ifindex;
    int                     queue_id;
    
    /* Statistics */
    uint64_t                tx_packets;
    uint64_t                tx_bytes;
    uint64_t                tx_errors;
    uint64_t                tx_dropped;   /* No free frames */
    
    /* State */
    int                     initialized;
} af_xdp_ctx_t;

/* ── Lifecycle ── */

/* Initialize AF_XDP socket on the specified network interface.
 * 
 * Parameters:
 *   ctx      - AF_XDP context to initialize
 *   ifname   - Network interface name (e.g., "eth0", "rxe0")
 *   queue_id - RX/TX queue ID (use 0 for single-queue NICs)
 *   flags    - XDP flags (XDP_FLAGS_DRV_MODE, XDP_FLAGS_SKB_MODE, etc.)
 * 
 * Returns:
 *   0 on success, -1 on failure
 * 
 * Notes:
 *   - Requires CAP_NET_RAW capability
 *   - DRV_MODE (driver mode) requires NIC driver support
 *   - SKB_MODE (generic mode) works on any NIC but is slower
 *   - Soft-RoCE (rxe) only supports SKB_MODE
 */
int af_xdp_init(af_xdp_ctx_t *ctx, const char *ifname, 
                int queue_id, uint32_t xdp_flags);

/* Clean up AF_XDP socket and release resources.
 * 
 * Parameters:
 *   ctx - AF_XDP context to clean up
 */
void af_xdp_cleanup(af_xdp_ctx_t *ctx);

/* ── Packet transmission ── */

/* Send a WAL packet via AF_XDP socket (zero-copy).
 * 
 * Parameters:
 *   ctx  - AF_XDP context
 *   data - Packet data (wire format: [uint64_t len][payload])
 *   len  - Total packet length (including 8-byte length header)
 * 
 * Returns:
 *   0 on success, -1 on failure (no free frames or TX queue full)
 * 
 * Notes:
 *   - This function does NOT block; caller should retry on -EAGAIN
 *   - Packet is copied into UMEM frame (one copy, but shared with NIC)
 *   - Actual transmission happens when af_xdp_tx_flush() is called
 *   - For best performance, batch multiple sends before flushing
 */
int af_xdp_send(af_xdp_ctx_t *ctx, const void *data, size_t len);

/* Flush pending TX packets to the kernel.
 * 
 * Parameters:
 *   ctx - AF_XDP context
 * 
 * Returns:
 *   Number of packets submitted, or -1 on error
 * 
 * Notes:
 *   - This triggers the actual sendmsg() syscall
 *   - Should be called after a batch of af_xdp_send() calls
 *   - Kernel will invoke XDP program for each packet
 */
int af_xdp_tx_flush(af_xdp_ctx_t *ctx);

/* ── Frame management ── */

/* Reclaim completed TX frames from the Completion Queue.
 * 
 * Parameters:
 *   ctx - AF_XDP context
 * 
 * Returns:
 *   Number of frames reclaimed
 * 
 * Notes:
 *   - Should be called periodically to avoid running out of free frames
 *   - Typically called after af_xdp_tx_flush() or when send fails
 */
int af_xdp_reclaim_frames(af_xdp_ctx_t *ctx);

/* Refill the Fill Queue with empty buffers for RX.
 * 
 * Parameters:
 *   ctx - AF_XDP context
 * 
 * Returns:
 *   Number of buffers added to Fill Queue
 * 
 * Notes:
 *   - Only needed if using RX path (we don't for WAL replication)
 *   - Included for completeness
 */
int af_xdp_refill_fq(af_xdp_ctx_t *ctx);

/* ── Statistics ── */

/* Get AF_XDP statistics.
 * 
 * Parameters:
 *   ctx        - AF_XDP context
 *   tx_packets - Output: total packets transmitted
 *   tx_bytes   - Output: total bytes transmitted
 *   tx_errors  - Output: transmission errors
 *   tx_dropped - Output: packets dropped (no free frames)
 */
void af_xdp_get_stats(const af_xdp_ctx_t *ctx,
                      uint64_t *tx_packets,
                      uint64_t *tx_bytes,
                      uint64_t *tx_errors,
                      uint64_t *tx_dropped);

/* Reset statistics counters to zero. */
void af_xdp_reset_stats(af_xdp_ctx_t *ctx);

/* ── Utility ── */

/* Get network interface index by name.
 * 
 * Parameters:
 *   ifname - Interface name (e.g., "eth0")
 * 
 * Returns:
 *   Interface index (> 0) on success, -1 on failure
 */
int af_xdp_get_ifindex(const char *ifname);

/* Check if the network interface supports XDP driver mode.
 * 
 * Parameters:
 *   ifname - Interface name
 * 
 * Returns:
 *   1 if driver mode is supported, 0 otherwise
 * 
 * Notes:
 *   - Driver mode (XDP_FLAGS_DRV_MODE) is fastest but requires NIC support
 *   - Generic mode (XDP_FLAGS_SKB_MODE) works on any NIC
 *   - Soft-RoCE (rxe) only supports generic mode
 */
int af_xdp_check_drv_support(const char *ifname);

#endif /* AF_XDP_REPL_H */
