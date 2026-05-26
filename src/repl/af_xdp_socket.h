/* AF_XDP Socket Interface for Zero-Copy Networking
 *
 * This module provides AF_XDP socket abstraction for high-performance
 * packet processing with kernel bypass and zero-copy semantics.
 *
 * Architecture:
 *   - UMEM: User-space memory shared between kernel and userspace
 *   - FILL ring: Userspace provides empty frames to kernel for RX
 *   - RX ring: Kernel delivers received packets to userspace
 *   - TX ring: Userspace submits packets to kernel for TX
 *   - COMP ring: Kernel notifies userspace of TX completion
 */

#ifndef AF_XDP_SOCKET_H
#define AF_XDP_SOCKET_H

#include <stdint.h>
#include <stddef.h>
#include <linux/if_xdp.h>
#include <xdp/xsk.h>

/* Configuration constants */
#define XDP_UMEM_SIZE           (8 * 1024 * 1024)  // 8MB total
#define XDP_FRAME_SIZE          2048                // 2KB per frame
#define XDP_FRAME_COUNT         (XDP_UMEM_SIZE / XDP_FRAME_SIZE)  // 4096 frames
#define XDP_RING_SIZE           2048                // Ring buffer size

/* UMEM (User Memory) structure
 * Manages the shared memory region between kernel and userspace
 */
struct xdp_umem {
    void *buffer;               // mmap-allocated memory region
    size_t size;                // Total size (8MB)
    uint32_t frame_size;        // Size of each frame (2048 bytes)
    uint32_t frame_count;       // Number of frames (4096)

    // Frame allocator
    uint32_t *free_frames;      // Stack of free frame indices
    uint32_t free_count;        // Number of free frames

    // libxdp UMEM handle
    struct xsk_umem *umem;
};

/* AF_XDP Socket structure
 * Represents a single AF_XDP socket with its ring buffers
 */
struct af_xdp_socket {
    int fd;                     // Socket file descriptor

    // Ring buffers
    struct xsk_ring_prod fill_ring;   // User → Kernel: provide empty frames
    struct xsk_ring_cons rx_ring;     // Kernel → User: received packets
    struct xsk_ring_prod tx_ring;     // User → Kernel: packets to send
    struct xsk_ring_cons comp_ring;   // Kernel → User: TX completion

    // UMEM
    struct xdp_umem *umem;

    // libxdp socket handle
    struct xsk_socket *xsk;

    // Network interface info
    char ifname[16];            // Interface name (e.g., "eth0", "lo")
    uint32_t queue_id;          // RX queue ID
    uint32_t ifindex;           // Interface index
};

/* Packet structure for batch operations */
struct xdp_packet {
    void *data;                 // Pointer to packet data
    size_t len;                 // Packet length
    uint64_t addr;              // UMEM frame address
};

/* ========== UMEM Management ========== */

/* Create and initialize UMEM
 * Returns: 0 on success, -1 on error
 */
int umem_create(struct xdp_umem **umem, size_t size);

/* Destroy UMEM and free resources */
void umem_destroy(struct xdp_umem *umem);

/* Allocate a frame from UMEM
 * Returns: frame address (offset in UMEM), or UINT64_MAX if no frames available
 */
uint64_t umem_alloc_frame(struct xdp_umem *umem);

/* Free a frame back to UMEM */
void umem_free_frame(struct xdp_umem *umem, uint64_t addr);

/* Get pointer to frame data */
static inline void *umem_get_data(struct xdp_umem *umem, uint64_t addr)
{
    return (char *)umem->buffer + addr;
}

/* ========== AF_XDP Socket Operations ========== */

/* Create AF_XDP socket
 *
 * Parameters:
 *   xsk: Output pointer to created socket
 *   ifname: Network interface name (e.g., "eth0", "lo")
 *   queue_id: RX queue ID (usually 0 for single-queue NICs)
 *   umem: UMEM to use (if NULL, a new one will be created)
 *
 * Returns: 0 on success, -1 on error
 */
int af_xdp_socket_create(struct af_xdp_socket **xsk,
                         const char *ifname,
                         uint32_t queue_id,
                         struct xdp_umem *umem);

/* Destroy AF_XDP socket */
void af_xdp_socket_destroy(struct af_xdp_socket *xsk);

/* ========== Packet I/O ========== */

/* Send a single packet
 *
 * Parameters:
 *   xsk: AF_XDP socket
 *   data: Packet data (will be copied to UMEM frame)
 *   len: Packet length
 *
 * Returns: number of bytes sent, or -1 on error
 */
int af_xdp_send(struct af_xdp_socket *xsk, const void *data, size_t len);

/* Receive a single packet
 *
 * Parameters:
 *   xsk: AF_XDP socket
 *   buf: Buffer to copy packet data into
 *   len: Input: buffer size, Output: packet length
 *
 * Returns: number of bytes received, 0 if no packets, -1 on error
 */
int af_xdp_recv(struct af_xdp_socket *xsk, void *buf, size_t *len);

/* ========== Batch Operations (for future optimization) ========== */

/* Receive multiple packets in batch
 * Returns: number of packets received
 */
int af_xdp_recv_batch(struct af_xdp_socket *xsk,
                      struct xdp_packet *pkts,
                      int max_pkts);

/* Send multiple packets in batch
 * Returns: number of packets sent
 */
int af_xdp_send_batch(struct af_xdp_socket *xsk,
                      struct xdp_packet *pkts,
                      int num_pkts);

/* ========== Utility Functions ========== */

/* Get interface index by name */
int get_ifindex(const char *ifname);

/* Kick TX ring (trigger packet transmission) */
void af_xdp_kick_tx(struct af_xdp_socket *xsk);

/* Kick RX ring (trigger packet reception) */
void af_xdp_kick_rx(struct af_xdp_socket *xsk);

#endif /* AF_XDP_SOCKET_H */
