#ifndef KVS_REPL_FRAME_H
#define KVS_REPL_FRAME_H

#include <stdint.h>

#define KVS_REPL_FRAME_MAGIC 0x5852504cU  /* "XRPL" */
#define KVS_REPL_FRAME_VERSION 1u

typedef enum {
    KVS_REPL_FRAME_DATA      = 1,
    KVS_REPL_FRAME_ACK       = 2,
    KVS_REPL_FRAME_NACK      = 3,
    KVS_REPL_FRAME_HEARTBEAT = 4,
    KVS_REPL_FRAME_FULLREQ   = 5,
} kvs_repl_frame_type_t;

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint16_t version;
    uint16_t type;
    uint64_t repl_epoch;
    uint64_t seq;
    uint32_t payload_len;
    uint32_t crc32;
    uint16_t frag_idx;
    uint16_t frag_cnt;
    uint32_t frag_off;
    uint32_t total_len;
    uint16_t kind;
    uint16_t flags;
} kvs_repl_frame_hdr_t;

#endif
