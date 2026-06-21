#include "rdma_ctrl_protocol.h"

#include <rdma/fabric.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_errno.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <errno.h>

/* CRC32 lookup table (IEEE 802.3 polynomial) */
static uint32_t crc32_table[256];
static int crc32_table_initialized = 0;

/* Initialize CRC32 lookup table */
static void init_crc32_table(void) {
    uint32_t poly = 0xEDB88320;
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++) {
            if (crc & 1) {
                crc = (crc >> 1) ^ poly;
            } else {
                crc >>= 1;
            }
        }
        crc32_table[i] = crc;
    }
    crc32_table_initialized = 1;
}

/* Calculate CRC32 checksum */
uint32_t calc_crc32(const void *data, size_t len) {
    if (!crc32_table_initialized) {
        init_crc32_table();
    }

    const uint8_t *buf = (const uint8_t *)data;
    uint32_t crc = 0xFFFFFFFF;

    for (size_t i = 0; i < len; i++) {
        uint8_t index = (crc ^ buf[i]) & 0xFF;
        crc = (crc >> 8) ^ crc32_table[index];
    }

    return crc ^ 0xFFFFFFFF;
}

/* Send control message via RDMA FI_MSG */
int ctrl_msg_send(struct fid_ep *ep, const ctrl_msg_t *msg) {
    if (!ep || !msg) {
        return -1;
    }

    /* Serialize message (convert to network byte order) */
    ctrl_msg_t wire_msg;
    wire_msg.type = htonl(msg->type);
    wire_msg.remote_addr = htobe64(msg->remote_addr);
    wire_msg.remote_key = htobe64(msg->remote_key);
    wire_msg.batch_size = htobe64(msg->batch_size);
    wire_msg.batch_id = htonl(msg->batch_id);
    wire_msg.crc32 = htonl(msg->crc32);

    printf("[CTRL] Sending message type=%u, batch_id=%u, size=%zu\n",
           msg->type, msg->batch_id, msg->batch_size);

    /* Send via FI_MSG */
    ssize_t ret = fi_send(ep, &wire_msg, sizeof(wire_msg), NULL, 0, NULL);
    if (ret < 0) {
        fprintf(stderr, "[CTRL] fi_send failed: %s\n", fi_strerror((int)-ret));
        return -1;
    }

    return 0;
}

/* Receive control message via RDMA FI_MSG */
int ctrl_msg_recv(struct fid_ep *ep, struct fid_cq *cq, ctrl_msg_t *msg) {
    if (!ep || !cq || !msg) {
        return -1;
    }

    /* Allocate buffer on heap to keep it alive during async operation */
    ctrl_msg_t *wire_msg = malloc(sizeof(ctrl_msg_t));
    if (!wire_msg) return -1;

    /* Post receive buffer */
    ssize_t ret = fi_recv(ep, wire_msg, sizeof(ctrl_msg_t), NULL, 0, wire_msg);
    if (ret < 0) {
        free(wire_msg);
        return -1;
    }

    /* Wait for completion */
    struct fi_cq_data_entry entry;
    while (1) {
        ret = fi_cq_read(cq, &entry, 1);
        if (ret > 0) break;
        if (ret < 0 && ret != -FI_EAGAIN) {
            free(wire_msg);
            return -1;
        }
    }

    /* Deserialize message (convert from network byte order) */
    msg->type = ntohl(wire_msg->type);
    msg->remote_addr = be64toh(wire_msg->remote_addr);
    msg->remote_key = be64toh(wire_msg->remote_key);
    msg->batch_size = be64toh(wire_msg->batch_size);
    msg->batch_id = ntohl(wire_msg->batch_id);
    msg->crc32 = ntohl(wire_msg->crc32);

    printf("[CTRL] Received message type=%u, batch_id=%u, size=%zu\n",
           msg->type, msg->batch_id, msg->batch_size);

    free(wire_msg);
    return 0;
}
