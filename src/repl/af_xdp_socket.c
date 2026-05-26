/* AF_XDP Socket Implementation
 *
 * Provides zero-copy packet I/O using AF_XDP sockets and shared UMEM.
 */

#include "af_xdp_socket.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <linux/if_link.h>
#include <net/if.h>
#include <bpf/libbpf.h>
#include <bpf/bpf.h>

/* ========== UMEM Management ========== */

int umem_create(struct xdp_umem **umem_out, size_t size)
{
    struct xdp_umem *umem;
    int ret;

    umem = calloc(1, sizeof(*umem));
    if (!umem) {
        fprintf(stderr, "[AF_XDP] Failed to allocate UMEM structure\n");
        return -1;
    }

    // Allocate UMEM buffer using mmap
    umem->buffer = mmap(NULL, size,
                        PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
                        -1, 0);
    if (umem->buffer == MAP_FAILED) {
        // Fallback to regular pages if huge pages not available
        umem->buffer = mmap(NULL, size,
                           PROT_READ | PROT_WRITE,
                           MAP_PRIVATE | MAP_ANONYMOUS,
                           -1, 0);
        if (umem->buffer == MAP_FAILED) {
            fprintf(stderr, "[AF_XDP] Failed to mmap UMEM: %s\n", strerror(errno));
            free(umem);
            return -1;
        }
    }

    umem->size = size;
    umem->frame_size = XDP_FRAME_SIZE;
    umem->frame_count = size / XDP_FRAME_SIZE;

    // Initialize frame allocator
    umem->free_frames = calloc(umem->frame_count, sizeof(uint32_t));
    if (!umem->free_frames) {
        fprintf(stderr, "[AF_XDP] Failed to allocate free_frames array\n");
        munmap(umem->buffer, umem->size);
        free(umem);
        return -1;
    }

    // Populate free frames stack (all frames initially free)
    for (uint32_t i = 0; i < umem->frame_count; i++) {
        umem->free_frames[i] = i * XDP_FRAME_SIZE;
    }
    umem->free_count = umem->frame_count;

    // Create libxdp UMEM
    struct xsk_umem_config umem_cfg = {
        .fill_size = XDP_RING_SIZE,
        .comp_size = XDP_RING_SIZE,
        .frame_size = XDP_FRAME_SIZE,
        .frame_headroom = 0,
        .flags = 0
    };

    struct xsk_ring_prod fill_tmp;
    struct xsk_ring_cons comp_tmp;

    ret = xsk_umem__create(&umem->umem, umem->buffer, size,
                          &fill_tmp, &comp_tmp, &umem_cfg);
    if (ret) {
        fprintf(stderr, "[AF_XDP] Failed to create UMEM: %s\n", strerror(-ret));
        free(umem->free_frames);
        munmap(umem->buffer, umem->size);
        free(umem);
        return -1;
    }

    *umem_out = umem;
    printf("[AF_XDP] UMEM created: %zu bytes, %u frames\n", size, umem->frame_count);
    return 0;
}

void umem_destroy(struct xdp_umem *umem)
{
    if (!umem)
        return;

    if (umem->umem)
        xsk_umem__delete(umem->umem);

    if (umem->buffer)
        munmap(umem->buffer, umem->size);

    free(umem->free_frames);
    free(umem);
}

uint64_t umem_alloc_frame(struct xdp_umem *umem)
{
    if (umem->free_count == 0)
        return UINT64_MAX;

    return umem->free_frames[--umem->free_count];
}

void umem_free_frame(struct xdp_umem *umem, uint64_t addr)
{
    if (umem->free_count >= umem->frame_count) {
        fprintf(stderr, "[AF_XDP] Warning: free_frames overflow\n");
        return;
    }

    umem->free_frames[umem->free_count++] = addr;
}

/* ========== Utility Functions ========== */

int get_ifindex(const char *ifname)
{
    unsigned int ifindex = if_nametoindex(ifname);
    if (ifindex == 0) {
        fprintf(stderr, "[AF_XDP] Interface %s not found: %s\n",
                ifname, strerror(errno));
        return -1;
    }
    return ifindex;
}

/* ========== AF_XDP Socket Operations ========== */

int af_xdp_socket_create(struct af_xdp_socket **xsk_out,
                         const char *ifname,
                         uint32_t queue_id,
                         struct xdp_umem *umem)
{
    struct af_xdp_socket *xsk;
    int ret;
    int own_umem = 0;

    xsk = calloc(1, sizeof(*xsk));
    if (!xsk) {
        fprintf(stderr, "[AF_XDP] Failed to allocate socket structure\n");
        return -1;
    }

    // Get interface index
    xsk->ifindex = get_ifindex(ifname);
    if (xsk->ifindex < 0) {
        free(xsk);
        return -1;
    }

    strncpy(xsk->ifname, ifname, sizeof(xsk->ifname) - 1);
    xsk->queue_id = queue_id;

    // Create UMEM if not provided
    if (!umem) {
        ret = umem_create(&umem, XDP_UMEM_SIZE);
        if (ret) {
            free(xsk);
            return -1;
        }
        own_umem = 1;
    }
    xsk->umem = umem;

    // Configure AF_XDP socket
    struct xsk_socket_config xsk_cfg = {
        .rx_size = XDP_RING_SIZE,
        .tx_size = XDP_RING_SIZE,
        .libbpf_flags = 0,
        .xdp_flags = XDP_FLAGS_UPDATE_IF_NOEXIST,
        .bind_flags = XDP_USE_NEED_WAKEUP
    };

    // Get FILL and COMP rings from UMEM (they were created with the UMEM)
    // We need to use xsk_socket__create_shared to share these rings

    // Create AF_XDP socket with shared UMEM rings
    ret = xsk_socket__create_shared(&xsk->xsk, ifname, queue_id,
                                   umem->umem,
                                   &xsk->rx_ring, &xsk->tx_ring,
                                   &xsk->fill_ring, &xsk->comp_ring,
                                   &xsk_cfg);
    if (ret) {
        fprintf(stderr, "[AF_XDP] Failed to create socket on %s queue %u: %s\n",
                ifname, queue_id, strerror(-ret));
        if (own_umem)
            umem_destroy(umem);
        free(xsk);
        return -1;
    }

    // Get socket FD
    xsk->fd = xsk_socket__fd(xsk->xsk);

    // Populate FILL ring with empty frames for RX
    uint32_t idx;
    ret = xsk_ring_prod__reserve(&xsk->fill_ring, XDP_RING_SIZE / 2, &idx);
    if (ret != XDP_RING_SIZE / 2) {
        fprintf(stderr, "[AF_XDP] Failed to reserve FILL ring\n");
        xsk_socket__delete(xsk->xsk);
        if (own_umem)
            umem_destroy(umem);
        free(xsk);
        return -1;
    }

    for (int i = 0; i < XDP_RING_SIZE / 2; i++) {
        uint64_t addr = umem_alloc_frame(umem);
        if (addr == UINT64_MAX) {
            fprintf(stderr, "[AF_XDP] Failed to allocate frame for FILL ring\n");
            xsk_socket__delete(xsk->xsk);
            if (own_umem)
                umem_destroy(umem);
            free(xsk);
            return -1;
        }
        *xsk_ring_prod__fill_addr(&xsk->fill_ring, idx++) = addr;
    }

    xsk_ring_prod__submit(&xsk->fill_ring, XDP_RING_SIZE / 2);

    *xsk_out = xsk;
    printf("[AF_XDP] Socket created on %s queue %u (fd=%d)\n",
           ifname, queue_id, xsk->fd);
    return 0;
}

void af_xdp_socket_destroy(struct af_xdp_socket *xsk)
{
    if (!xsk)
        return;

    if (xsk->xsk)
        xsk_socket__delete(xsk->xsk);

    // Note: UMEM is not destroyed here as it might be shared
    // Caller should destroy UMEM separately if needed

    free(xsk);
}

/* ========== Packet I/O ========== */

int af_xdp_send(struct af_xdp_socket *xsk, const void *data, size_t len)
{
    uint32_t idx;
    int ret;

    if (len > XDP_FRAME_SIZE) {
        fprintf(stderr, "[AF_XDP] Packet too large: %zu > %d\n", len, XDP_FRAME_SIZE);
        return -1;
    }

    // Reserve TX ring slot
    ret = xsk_ring_prod__reserve(&xsk->tx_ring, 1, &idx);
    if (ret != 1) {
        // TX ring full, try to reclaim completed frames
        uint32_t comp_idx;
        int reclaimed = xsk_ring_cons__peek(&xsk->comp_ring, 32, &comp_idx);
        if (reclaimed > 0) {
            for (int i = 0; i < reclaimed; i++) {
                uint64_t addr = *xsk_ring_cons__comp_addr(&xsk->comp_ring, comp_idx++);
                umem_free_frame(xsk->umem, addr);
            }
            xsk_ring_cons__release(&xsk->comp_ring, reclaimed);
        }

        // Retry reservation
        ret = xsk_ring_prod__reserve(&xsk->tx_ring, 1, &idx);
        if (ret != 1)
            return -1;
    }

    // Allocate frame
    uint64_t addr = umem_alloc_frame(xsk->umem);
    if (addr == UINT64_MAX) {
        fprintf(stderr, "[AF_XDP] No free frames for TX\n");
        return -1;
    }

    // Copy data to frame
    void *frame = umem_get_data(xsk->umem, addr);
    memcpy(frame, data, len);

    // Submit to TX ring
    struct xdp_desc *tx_desc = xsk_ring_prod__tx_desc(&xsk->tx_ring, idx);
    tx_desc->addr = addr;
    tx_desc->len = len;

    xsk_ring_prod__submit(&xsk->tx_ring, 1);

    // Kick TX (trigger transmission)
    af_xdp_kick_tx(xsk);

    return len;
}

int af_xdp_recv(struct af_xdp_socket *xsk, void *buf, size_t *len)
{
    uint32_t idx;
    int ret;

    // Check RX ring
    ret = xsk_ring_cons__peek(&xsk->rx_ring, 1, &idx);
    if (ret != 1)
        return 0;  // No packets available

    // Get packet descriptor
    const struct xdp_desc *rx_desc = xsk_ring_cons__rx_desc(&xsk->rx_ring, idx);
    uint64_t addr = rx_desc->addr;
    uint32_t pkt_len = rx_desc->len;

    if (pkt_len > *len) {
        fprintf(stderr, "[AF_XDP] Packet too large for buffer: %u > %zu\n",
                pkt_len, *len);
        xsk_ring_cons__release(&xsk->rx_ring, 1);
        return -1;
    }

    // Copy packet data
    void *pkt_data = umem_get_data(xsk->umem, addr);
    memcpy(buf, pkt_data, pkt_len);
    *len = pkt_len;

    // Release RX descriptor
    xsk_ring_cons__release(&xsk->rx_ring, 1);

    // Return frame to FILL ring
    uint32_t fill_idx;
    ret = xsk_ring_prod__reserve(&xsk->fill_ring, 1, &fill_idx);
    if (ret == 1) {
        *xsk_ring_prod__fill_addr(&xsk->fill_ring, fill_idx) = addr;
        xsk_ring_prod__submit(&xsk->fill_ring, 1);
    } else {
        // FILL ring full, just free the frame
        umem_free_frame(xsk->umem, addr);
    }

    return pkt_len;
}

/* ========== Ring Kick Functions ========== */

void af_xdp_kick_tx(struct af_xdp_socket *xsk)
{
    // Send a dummy message to trigger TX
    sendto(xsk->fd, NULL, 0, MSG_DONTWAIT, NULL, 0);
}

void af_xdp_kick_rx(struct af_xdp_socket *xsk)
{
    // Receive to trigger RX processing
    recvfrom(xsk->fd, NULL, 0, MSG_DONTWAIT, NULL, NULL);
}

/* ========== Batch Operations (Stubs for future implementation) ========== */

int af_xdp_recv_batch(struct af_xdp_socket *xsk,
                      struct xdp_packet *pkts,
                      int max_pkts)
{
    // TODO: Implement batch receive
    return 0;
}

int af_xdp_send_batch(struct af_xdp_socket *xsk,
                      struct xdp_packet *pkts,
                      int num_pkts)
{
    // TODO: Implement batch send
    return 0;
}
