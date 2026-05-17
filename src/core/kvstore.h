#ifndef __KV_STORE_H__
#define __KV_STORE_H__

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <sys/time.h>
#include <unistd.h>

#include "server.h"
#include "config.h"
#include "protocol.h"
#include "kv_dict.h"

#include <jemalloc/jemalloc.h>

#define NETWORK_REACTOR   0
#define NETWORK_PROACTOR  1
#define NETWORK_NTYCO     2

#define ENABLE_JEMALLOC 1

extern int reactor_start(unsigned short port, msg_handler handler);
extern int proactor_start(unsigned short port, msg_handler handler);
extern int ntyco_start(unsigned short port, msg_handler handler);

long long get_time_now(void);          /* cached, ~loop-iteration granularity */
long long get_time_now_real(void);     /* gettimeofday(); use when accuracy matters
                                          (e.g. cross-worker BLPOP deadlines) */
void update_cached_time(void);  /* call once per event-loop iteration */

/* ── Command dispatch ── */
dispatch_result_t kvs_dispatch(kvs_token_t *tokens, int argc,
                               const char *msg_raw, int msg_len,
                               DynBuf *wb, struct conn *c);

void kvs_flush_all(void);
void kvs_cleanup_entry(kv_entry_t *e);  /* free type-specific internal structures */

/* ── Value item (legacy, used by persistence layer) ── */
typedef struct {
    char      *value;
    size_t     len;
    long long  expire;
} KVItem;

typedef void (*kv_scan_cb)(const char *key, KVItem *item, void *priv);

void *kvs_malloc(size_t size);
void  kvs_free(void *ptr);

KVItem* create_kvitem_len(const char *value, int vlen);
void    free_kvitem(KVItem *item);

/* ── Global dict ── */
/* g_dict replaced by sharded g_shards (kv_dict.h) */
extern int g_in_reload;   /* 1 during AOF/RDB reload — suppress AOF append + eBPF */

#endif
