#ifndef KVS_VEC_WAL_H
#define KVS_VEC_WAL_H

#include "vec_types.h"

#define VEC_WAL_MAGIC 0x5657414cu
#define VEC_WAL_VERSION 1u
#define VEC_WAL_DEFAULT_PATH "build/vector/vector.wal"

typedef enum {
    VEC_WAL_CREATE_COLLECTION = 1,
    VEC_WAL_DROP_COLLECTION   = 2,
    VEC_WAL_INSERT            = 3,
    VEC_WAL_DELETE            = 4,
} vec_wal_op_t;

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint16_t version;
    uint16_t op;
    uint64_t lsn;
    uint32_t payload_len;
    uint32_t crc32;
} vec_wal_record_hdr_t;

typedef struct {
    int fd;
    char path[256];
} vec_wal_t;

typedef int (*vec_wal_replay_cb)(uint16_t op, uint64_t lsn,
                                 const char *payload, uint32_t len,
                                 void *arg);

int vec_wal_open(vec_wal_t *wal, const char *path);
void vec_wal_close(vec_wal_t *wal);
int vec_wal_append(vec_wal_t *wal, uint16_t op, uint64_t lsn,
                   const void *payload, uint32_t payload_len);
int vec_wal_replay(const char *path, vec_wal_replay_cb cb, void *arg,
                   uint64_t *max_lsn_out);

int vec_wal_append_create(vec_wal_t *wal, uint64_t lsn,
                          const char *name, uint32_t name_len,
                          uint32_t dim, vec_metric_t metric,
                          vec_index_type_t index_type);
int vec_wal_append_drop(vec_wal_t *wal, uint64_t lsn,
                        const char *name, uint32_t name_len);
int vec_wal_append_insert(vec_wal_t *wal, uint64_t lsn,
                          const char *collection, uint32_t collection_len,
                          const char *id, uint32_t id_len,
                          const float *vec, uint32_t dim,
                          vec_metric_t metric);
int vec_wal_append_delete(vec_wal_t *wal, uint64_t lsn,
                          const char *collection, uint32_t collection_len,
                          const char *id, uint32_t id_len);

#endif
