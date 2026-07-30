#ifndef KVS_WAL_PERSIST_H
#define KVS_WAL_PERSIST_H

#include <nmmintrin.h>
#include <stdint.h>
#include <string.h>

/* Hardware-accelerated CRC32C using SSE4.2 (Castagnoli polynomial). */
static inline uint32_t kvs_crc32c(uint32_t crc, const void *data, size_t len)
{
    const unsigned char *p = (const unsigned char *)data;
    while (len >= 8) {
        uint64_t word;
        memcpy(&word, p, 8);
        crc = (uint32_t)_mm_crc32_u64((uint64_t)crc, word);
        p += 8; len -= 8;
    }
    if (len >= 4) {
        uint32_t word;
        memcpy(&word, p, 4);
        crc = _mm_crc32_u32(crc, word);
        p += 4; len -= 4;
    }
    while (len > 0) {
        crc = _mm_crc32_u8(crc, *p);
        p++; len--;
    }
    return crc;
}

#define KVS_WAL2_MAGIC  0x4a57414cU  /* "JWAL" */
#define KVS_WAL2_VERSION 1u
#define KVS_JOURNAL_MAX_PAYLOAD (1024u * 1024u)
#define KVS_MUT_KIND_RESP_STRING  1u

/* Sequence number — exposed for fast-path in kvstore.c */
extern _Atomic uint64_t g_next_seq;

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint16_t version;
    uint16_t kind;
    uint16_t flags;
    uint16_t reserved;
    uint64_t seq;
    uint32_t len;
    uint32_t crc32;
} kvs_wal2_record_hdr_t;

int  kvs_wal_persist_start(void);
/* Fast path: append raw payload directly */
int  kvs_wal_persist_append_raw(uint64_t seq, uint16_t kind, uint16_t flags,
                                const char *payload, uint32_t len, uint32_t crc);
void kvs_wal_persist_stop(void);

/* Set fsync policy: "always", "everysec", or "no" */
void kvs_wal_set_fsync_policy(const char *policy);

#endif
