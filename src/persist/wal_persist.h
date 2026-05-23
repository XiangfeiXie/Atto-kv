#ifndef KVS_WAL_PERSIST_H
#define KVS_WAL_PERSIST_H

#include "journal.h"

#include <stdint.h>

#define KVS_WAL2_MAGIC 0x4a57414cU  /* "JWAL" */
#define KVS_WAL2_VERSION 1u

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
int  kvs_wal_persist_append(const kvs_journal_record_t *rec);
void kvs_wal_persist_stop(void);

#endif
