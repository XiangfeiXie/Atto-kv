#ifndef __PERSIST_INTERNAL_H__
#define __PERSIST_INTERNAL_H__

#include <pthread.h>
#include <sys/types.h>
#include "kv_dict.h"

#define RDB_TMP  "build/dump.rdb.tmp"

/* Dynamic buffer for RDB/AOF serialization */
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

/* AOF rewrite buffer: captures writes during AOF rewrite */
typedef struct {
    char *data;
    size_t len;
    size_t cap;
} aof_rewrite_buf_t;

/* Dict scan */
void store_scan(void (*cb)(kv_entry_t *e, void *priv), void *priv);

/* io_uring chunked write + fdatasync */
void rdb_write_uring(int fd, const char *data, size_t total);

/* Binary-safe read */
const char *mread(const char *p, const char *end, void *out, size_t n);

/* RDB BGSAVE state */
extern volatile pid_t g_rdb_child_pid;
extern int g_rdb_in_fork_child;

/* AOF rewrite state */
extern volatile pid_t g_aof_rewrite_child_pid;
extern int g_aof_rewrite_in_progress;
extern aof_rewrite_buf_t g_aof_rewrite_buf;
extern pthread_mutex_t g_aof_rewrite_mu;

/* SIGCHLD handler for BGSAVE / BGREWRITEAOF children */
void rdb_sigchld(int signo);

/* AOF rewrite helpers */
void aof_rewrite_buf_append(const char *data, size_t len);
void aof_rewrite_entry(kv_entry_t *e, void *priv);

/* RDB entry payload accessor: returns data pointer and length.
 * Returns 0 on success, -1 if the entry has no inline payload (e.g. DICT encoding). */
int rdb_entry_payload(kv_entry_t *e, const char **data, int *dlen);

#endif
