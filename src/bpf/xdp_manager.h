#ifndef XDP_MANAGER_H
#define XDP_MANAGER_H

#include <stdint.h>
#include "xdp_kvs.h"

/* Load the XDP BPF program from xdp_kvs.kern.o, attach it to `iface`,
 * and configure it to intercept packets on `port`.
 * Returns 0 on success, -1 on failure (kvstore runs normally without XDP). */
int  xdp_manager_init(const char *iface, int port);

/* Update or insert a key-value pair in the hot cache.
 * Call after every SET / GETSET command completes.
 * Values longer than XDP_VAL_MAX are silently ignored. */
void xdp_cache_put(const char *key, int klen, const char *val, int vlen);

/* Remove a key from the hot cache.
 * Call after DEL, RENAME (old key), or EXPIRE expiry. */
void xdp_cache_del(const char *key, int klen);

/* Detach XDP program and release all BPF resources.
 * Safe to call even if xdp_manager_init() failed. */
void xdp_manager_stop(void);

/* Fill *hits, *misses, *bypass with cumulative counters from the BPF stats map.
 * Any pointer may be NULL to skip that counter. */
void xdp_manager_stats(uint64_t *hits, uint64_t *misses, uint64_t *bypass);

#endif /* XDP_MANAGER_H */
