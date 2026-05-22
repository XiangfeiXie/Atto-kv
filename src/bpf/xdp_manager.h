#ifndef XDP_MANAGER_H
#define XDP_MANAGER_H

#include <stdint.h>
#include "xdp_kvs.h"

/* Load the XDP BPF program from xdp_kvs.kern.o, attach it to `iface`,
 * and configure it to intercept packets on `port`.
 * Returns 0 on success, -1 on failure (kvstore runs normally without XDP). */
int  xdp_manager_init(const char *iface, int port);

/* Detach XDP program and release all BPF resources.
 * Safe to call even if xdp_manager_init() failed. */
void xdp_manager_stop(void);

/* Fill *hits, *misses, *bypass, *tx_drop with cumulative counters from the
 * BPF stats map.  Any pointer may be NULL to skip that counter. */
void xdp_manager_stats(uint64_t *hits, uint64_t *misses,
                        uint64_t *tx_drop, uint64_t *bypass);

#endif /* XDP_MANAGER_H */
