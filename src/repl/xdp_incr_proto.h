#ifndef KVS_XDP_INCR_PROTO_H
#define KVS_XDP_INCR_PROTO_H

#include <stdint.h>

#define XDP_INCR_MAGIC   0x58494E43u  /* "XINC" */
#define XDP_INCR_VERSION 1u

enum {
    XDP_INCR_PKT_DATA      = 1,
    XDP_INCR_PKT_HEARTBEAT = 2,
    XDP_INCR_PKT_ERROR     = 3,
    XDP_INCR_PKT_FULLSYNC  = 4,
};

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint16_t version;
    uint16_t type;
    uint64_t session_id;
    uint64_t epoch;
    uint64_t seq;
    uint32_t payload_len;
    uint32_t checksum;
} xdp_incr_hdr_t;

#endif /* KVS_XDP_INCR_PROTO_H */
