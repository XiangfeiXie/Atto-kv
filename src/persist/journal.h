#ifndef KVS_JOURNAL_H
#define KVS_JOURNAL_H

#include <nmmintrin.h>
#include <stdint.h>
#include <string.h>

/* Hardware-accelerated CRC32C using SSE4.2 (Castagnoli polynomial).
 * Replaces zlib crc32() — ~10x faster on x86_64 with SSE4.2. */
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

#define KVS_JOURNAL_BACKLOG_CAP 16384u
#define KVS_JOURNAL_BACKLOG_MASK (KVS_JOURNAL_BACKLOG_CAP - 1u)
#define KVS_JOURNAL_MAX_PAYLOAD (1024u * 1024u)

#define KVS_MUT_KIND_RESP_STRING 1u

typedef struct {
    uint64_t seq;
    uint16_t kind;
    uint16_t flags;
    uint32_t len;
    uint32_t crc32;
    char *payload;
} kvs_journal_record_t;

int      kvs_journal_init(void);
void     kvs_journal_shutdown(void);
int      kvs_journal_append(uint16_t kind, uint16_t flags,
                            const char *payload, uint32_t len,
                            uint64_t *out_seq, uint32_t *out_crc);
uint64_t kvs_journal_next_seq(void);
int      kvs_journal_copy(uint64_t seq, kvs_journal_record_t *out);
void     kvs_journal_record_release(kvs_journal_record_t *rec);

#endif
