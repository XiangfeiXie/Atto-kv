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
                               DynBuf *wb, struct conn *c);

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
#include <time.h>
extern _Atomic uint64_t g_kvs_total_commands;       /* every kvs_dispatch invocation */
extern _Atomic uint64_t g_kvs_total_connections;    /* every successful EVENT_ACCEPT */
extern _Atomic int      g_kvs_connected_clients;    /* live conns (++accept, --release) */
extern long long        g_kvs_start_time_ms;        /* set in main(); never mutated */

/* ── Phase 10.6 — dispatch latency histogram ──
 *
 * 16 log2-spaced buckets, ns granularity:
 *   B0:  <1 µs       B4:  8–16 µs     B8:  128–256 µs   B12: 2–4 ms
 *   B1:  1–2 µs      B5:  16–32 µs    B9:  256–512 µs   B13: 4–8 ms
 *   B2:  2–4 µs      B6:  32–64 µs    B10: 512–1024 µs  B14: 8–16 ms
 *   B3:  4–8 µs      B7:  64–128 µs   B11: 1–2 ms       B15: ≥16 ms
 *
 * Bumped from the network path around every kvs_dispatch() invocation
 * (see core/protocol.c kvs_drain_resp_commands_off).  Replay paths
 * (AOF/WAL reload) deliberately bypass the histogram to avoid skew.
 * INFO Stats exposes per-bucket counts plus derived p50/p99/p999.
 *
 * Latency counters are distributed across KVS_LAT_SLOTS cacheline-aligned
 * slots to reduce contention. Each slot is independently updated and the
 * INFO command aggregates across all slots.
 *
 * Cost: 2× clock_gettime(CLOCK_MONOTONIC) on the wire path = ~30 ns/op
 * (vDSO) + 3 uncontended atomic_fetch_add ≈ 10 ns.  At 491 k QPS that's
 * ≤2 % CPU.  Reader (INFO Stats) sums all slots — slow but rare. */
#define KVS_LAT_BUCKETS 16
#define KVS_LAT_SLOTS   8

struct kvs_lat_slot {
    _Atomic uint64_t hist[KVS_LAT_BUCKETS];     /* 128 B */
    _Atomic uint64_t count;                     /*   8 B */
    _Atomic uint64_t sum_ns;                    /*   8 B */
    char             _pad[64 - 16];             /* push next slot to a fresh line */
} __attribute__((aligned(64)));
_Static_assert(sizeof(struct kvs_lat_slot) % 64 == 0,
               "kvs_lat_slot must be a whole number of cachelines");

extern struct kvs_lat_slot g_kvs_lat_slots[KVS_LAT_SLOTS];

void kvs_lat_record_ns(uint64_t ns);

/* ── Phase 10.7 — TSC-based fast monotonic clock ──
 *
 * clock_gettime(CLOCK_MONOTONIC) is implemented via vDSO ONLY when the
 * kernel's clocksource is "tsc".  On VMs / containers where the kernel
 * picked hpet or acpi_pm (TSC unstable, or hypervisor exposed only paravirt
 * time) the call falls back to a real syscall — measured at 73 µs/call on
 * this dev VM.  Two calls bracketing every kvs_dispatch() then add 147 µs
 * of pure measurement overhead per command, collapsing 491 k QPS baseline
 * to 3.6 k QPS (130× regression).  The histogram was lying about its cost.
 *
 * Fix: use rdtsc directly.  __builtin_ia32_rdtsc is ~5 ns, never traps,
 * available on every x86_64 CPU since Pentium.  We calibrate ticks→ns
 * once at startup against CLOCK_MONOTONIC over a 10 ms sleep window.
 *
 * Trade-offs we accept:
 *   • rdtsc is not serializing — the CPU may reorder it across nearby
 *     loads/stores.  For ~µs-scale measurements this drift is invisible.
 *   • TSC is invariant on every modern x86 since Nehalem (constant_tsc +
 *     nonstop_tsc CPU flags), and migrated TSC across cores under same
 *     socket since Sandy Bridge.  Cross-socket / live-migration drift
 *     would be a problem, but we don't support those configurations.
 *   • If TSC turns out unusable (calibration negative / zero) we fall
 *     back to clock_gettime and live with the histogram cost.        */
extern double  g_kvs_tsc_ns_per_tick;   /* 0.0 ⇒ TSC unusable, use syscall */
extern int     g_kvs_tsc_usable;

void kvs_tsc_calibrate(void);

static inline uint64_t kvs_now_ns_fast(void) {
#if defined(__x86_64__) || defined(__i386__)
    if (__builtin_expect(g_kvs_tsc_usable, 1)) {
        return (uint64_t)(__builtin_ia32_rdtsc() * g_kvs_tsc_ns_per_tick);
    }
#endif
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}

#endif
