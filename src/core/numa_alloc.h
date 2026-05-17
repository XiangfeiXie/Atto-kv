#ifndef __KVS_NUMA_ALLOC_H__
#define __KVS_NUMA_ALLOC_H__

/* Phase 5.4 — NUMA-aware allocation skeleton.
 *
 * On single-socket machines this is plain malloc/free; on multi-socket
 * machines (when built with -DKVS_USE_NUMA and linked against libnuma)
 * memory is bound to the local node of the calling thread, so per-
 * worker structures stay on the same memory controller as the worker.
 *
 * The skeleton lives in code now so that Phase 5+ alloc sites can be
 * upgraded with a single `git grep kvs_numa_*` and no API churn.
 *
 * Compile-time switch:
 *   -DKVS_USE_NUMA=1   →  use libnuma (links -lnuma)
 *   default            →  plain libc malloc / mmap (current behaviour)
 */

#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

#ifndef KVS_USE_NUMA
#define KVS_USE_NUMA 0
#endif

#if KVS_USE_NUMA
#include <numa.h>
#include <numaif.h>
#endif

/* Allocate `size` bytes on the NUMA node local to the calling thread.
 * Falls back to malloc when KVS_USE_NUMA is 0 or libnuma reports the
 * system has no NUMA. */
static inline void *kvs_numa_alloc_local(size_t size) {
#if KVS_USE_NUMA
    if (numa_available() < 0) return calloc(1, size);
    void *p = numa_alloc_local(size);
    if (p) memset(p, 0, size);
    return p;
#else
    return calloc(1, size);
#endif
}

static inline void kvs_numa_free(void *p, size_t size) {
#if KVS_USE_NUMA
    if (numa_available() >= 0) { numa_free(p, size); return; }
    free(p);
#else
    (void)size;
    free(p);
#endif
}

/* Hint the kernel to bind a previously-mmap'd region to the calling
 * thread's local NUMA node.  Best-effort: a no-op when KVS_USE_NUMA is
 * 0 or the mbind syscall fails (e.g. inside a container without the
 * capability). */
static inline void kvs_numa_bind_local(void *addr, size_t size) {
#if KVS_USE_NUMA
    if (!addr || numa_available() < 0) return;
    struct bitmask *bm = numa_get_mems_allowed();
    if (!bm) return;
    int node = numa_node_of_cpu(sched_getcpu());
    if (node < 0) { numa_bitmask_free(bm); return; }
    numa_bitmask_clearall(bm);
    numa_bitmask_setbit(bm, (unsigned)node);
    mbind(addr, size, MPOL_PREFERRED,
          bm->maskp, bm->size + 1, MPOL_MF_MOVE);
    numa_bitmask_free(bm);
#else
    (void)addr; (void)size;
#endif
}

#endif /* __KVS_NUMA_ALLOC_H__ */
