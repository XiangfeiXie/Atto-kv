#ifndef __RDMA_CTRL_PROTOCOL_H__
#define __RDMA_CTRL_PROTOCOL_H__

#include <stdint.h>
#include <stddef.h>

/* =====================================================================
 * RDMA Fullsync Control Protocol (batch transfer)
 *
 * Architecture:
 *   - Port 2100: RDMA connection (FI_MSG for control, FI_RMA for data)
 *   - Port 6483: Incremental sync (unchanged)
 *
 * Memory:
 *   - Master: 512MB local batch buffer
 *   - Slave:  1GB remote buffer (reused via ACK)
 *
 * Flow:
 *   1. Slave connects to Master 2100 via RDMA
 *   2. Slave sends HANDSHAKE via FI_MSG (remote_addr, remote_key)
 *   3. Loop:
 *      - Master serializes batch → fi_write() to remote buffer
 *      - Master sends BATCH_ACK via FI_MSG
 *      - Slave parses batch → sends ACK via FI_MSG
 *   4. Master sends EOF via FI_MSG
 * ===================================================================== */

/* Control message types (simplified to 3 types) */
typedef enum {
    CTRL_MSG_HANDSHAKE      = 1,  /* Slave → Master: initial handshake */
    CTRL_MSG_BATCH_ACK      = 2,  /* Slave → Master: batch received */
    CTRL_MSG_EOF            = 3,  /* Master → Slave: fullsync done */
} ctrl_msg_type_t;

/* Control message structure */
typedef struct {
    ctrl_msg_type_t type;         /* Message type */
    uint64_t remote_addr;         /* Slave's remote buffer address (HANDSHAKE) */
    uint64_t remote_key;          /* Slave's MR key (HANDSHAKE) */
    size_t batch_size;            /* Current batch size in bytes */
    uint32_t batch_id;            /* Batch sequence number */
    uint32_t crc32;               /* CRC32 checksum for batch data */
} ctrl_msg_t;

/* Fixed buffer sizes (simplified, no adaptive resizing) */
#define LOCAL_BATCH_SIZE   (512ULL * 1024 * 1024)  /* 512MB */
#define REMOTE_BUF_SIZE    (1024ULL * 1024 * 1024) /* 1GB */

/* Timeout values (simplified to single timeout) */
#define FULLSYNC_TIMEOUT_MS  3600000  /* 1 hour total timeout */

/* Max retry count */
#define MAX_RETRIES  3

/* Helper functions */

/* Forward declarations for libfabric types */
struct fid_ep;
struct fid_cq;

/**
 * Send control message via RDMA FI_MSG
 * @param ep libfabric endpoint
 * @param msg Control message to send
 * @return 0 on success, -1 on error
 */
int ctrl_msg_send(struct fid_ep *ep, const ctrl_msg_t *msg);

/**
 * Receive control message via RDMA FI_MSG
 * @param ep libfabric endpoint
 * @param cq completion queue
 * @param msg Buffer to store received message
 * @return 0 on success, -1 on error
 */
int ctrl_msg_recv(struct fid_ep *ep, struct fid_cq *cq, ctrl_msg_t *msg);

/**
 * Calculate CRC32 checksum for data
 * @param data Data buffer
 * @param len Data length
 * @return CRC32 checksum
 */
uint32_t calc_crc32(const void *data, size_t len);

#endif /* __RDMA_CTRL_PROTOCOL_H__ */
