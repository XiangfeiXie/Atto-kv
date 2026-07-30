#ifndef __PERSIST_INTERNAL_H__
#define __PERSIST_INTERNAL_H__

#include <sys/types.h>
#include "kv_dict.h"

#define RDB_TMP  "build/dump.rdb.tmp"

/* Dynamic buffer for RDB serialization */
typedef struct {
    char *buf;
    size_t len;
    size_t cap;
    /* P0: per-page reference count — pre-scan populates, serialize
       scan decrements; madvise(MADV_DONTNEED) when count → 0 */
    struct { uintptr_t page; int count; } *page_map;
    size_t page_map_size;   /* power of 2 */
    size_t page_map_mask;
} rdb_buf_t;

void rdb_buf_reserve(rdb_buf_t *b, size_t need);
void rdb_buf_put(rdb_buf_t *b, const void *p, size_t n);

/* Dict scan */
void store_scan(void (*cb)(kv_entry_t *e, void *priv), void *priv);

/* io_uring chunked write + fdatasync */
void rdb_write_uring(int fd, const char *data, size_t total);

/* Binary-safe read */
const char *mread(const char *p, const char *end, void *out, size_t n);

/* RDB BGSAVE state */
extern volatile pid_t g_rdb_child_pid;

/* SIGCHLD handler for BGSAVE children */
void rdb_sigchld(int signo);

/* RDB entry payload accessor */
int rdb_entry_payload(kv_entry_t *e, const char **data, int *dlen);

#endif
