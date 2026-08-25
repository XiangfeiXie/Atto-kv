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

#ifdef USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

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
                               DynBuf *wb);

void kvs_flush_all(void);

/* ── Global dict ── */
extern int g_in_reload;   /* 1 during AOF/RDB reload — suppress AOF append + eBPF */

/* ── INFO command counters ──
 * Incremented by hot paths (dispatch / accept / release).  Read on demand
 * by the INFO command handler.  All atomics use relaxed ordering: we don't
 * require happens-before with the workload, just torn-read freedom and
 * TSAN-clean semantics.  On x86_64 these compile to plain loads/stores. */
#include <stdatomic.h>
#include <stdint.h>
extern _Atomic uint64_t g_kvs_total_commands;       /* every kvs_dispatch invocation */
extern _Atomic uint64_t g_kvs_total_connections;    /* every successful EVENT_ACCEPT */
extern _Atomic int      g_kvs_connected_clients;    /* live conns (++accept, --release) */
extern long long        g_kvs_start_time_ms;        /* set in main(); never mutated */

#endif
