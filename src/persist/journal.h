#ifndef KVS_JOURNAL_H
#define KVS_JOURNAL_H

#include <stdint.h>

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
                            uint64_t *out_seq);
uint64_t kvs_journal_next_seq(void);
int      kvs_journal_copy(uint64_t seq, kvs_journal_record_t *out);
void     kvs_journal_record_release(kvs_journal_record_t *rec);

#endif
