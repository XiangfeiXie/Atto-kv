/* shm_ipc.h — lock-free SPSC ring in POSIX shared memory (/dev/shm)
 *
 * This is the engine ↔ gateway transport.  A single SPSC queue lives in a
 * named /dev/shm region shared by the two processes; which side is producer
 * vs consumer is a matter of role, not a property of the queue:
 *
 *   master machine:  engine (kvstore) produces write commands → gateway consumes
 *   slave  machine:  gateway produces ordered bytes        → engine (kvstore) consumes
 *
 * Both binaries compile this same file, so the struct layout is identical.
 *
 * Lock-freedom relies on the classic SPSC discipline:
 *   - exactly one producer bumps `head` (release), exactly one consumer bumps
 *     `tail` (release); the other side loads the opposite index (acquire).
 *   - `head`/`tail` are monotonic 64-bit counters (never reset), masked by
 *     `SHM_RING_SIZE - 1` to index the ring.  This avoids ABA on the 32-bit
 *     wrap that a plain ring index would suffer at high rates.
 */

#ifndef SHM_IPC_H
#define SHM_IPC_H

#include <stdint.h>
#include <stddef.h>

/* Slot payload ceiling.  Benchmark write commands are typically < 100 bytes;
 * 8 KiB leaves headroom for large MSET / bulk values.  Anything larger is
 * dropped by the producer (with a warning) rather than silently truncated. */
#ifndef SHM_MAX_PAYLOAD
#define SHM_MAX_PAYLOAD  8192
#endif

/* Ring depth (power of two).  1024 slots × ~8.2 KiB ≈ 8.4 MB per region. */
#ifndef SHM_RING_SIZE
#define SHM_RING_SIZE    1024
#endif

#define SHM_RING_MASK    (SHM_RING_SIZE - 1)

typedef struct shm_pkt {
    uint32_t len;                       /* bytes used in data[] */
    char     data[SHM_MAX_PAYLOAD];
} __attribute__((aligned(64))) shm_pkt_t;

typedef struct spsc_queue {
    _Atomic uint64_t head __attribute__((aligned(64)));  /* producer counter */
    _Atomic uint64_t tail __attribute__((aligned(64)));  /* consumer counter */
    shm_pkt_t        ring[SHM_RING_SIZE];
} spsc_queue_t;

typedef struct shm_region {
    spsc_queue_t q;                     /* one queue; direction set by role */
} shm_region_t;

/* Create the /dev/shm region (O_CREAT) and zero the queue.  Returns the
 * mapped region, or NULL on error.  Call once from the first-started process
 * (the engine). */
shm_region_t *shm_region_create(void);

/* Attach to an existing region (no O_CREAT, no reset).  Returns NULL if the
 * region does not exist yet — start the engine first. */
shm_region_t *shm_region_attach(void);

/* Unmap and (for the creator) unlink.  Safe to call with NULL. */
void shm_region_destroy(shm_region_t *r, int creator);

/* Producer side.  Returns 0 on success, -1 if the queue is full (caller
 * should drop or back off). */
int spsc_enqueue(spsc_queue_t *q, const void *data, uint32_t len);

/* Consumer side.  Returns 0 on success and writes the payload into `data`
 * (capacity must be >= SHM_MAX_PAYLOAD), setting *len.  Returns -1 if empty. */
int spsc_dequeue(spsc_queue_t *q, void *data, uint32_t *len);

/* Non-destructive: 1 if empty (consumer sees no unread items). */
int spsc_empty(const spsc_queue_t *q);

#endif /* SHM_IPC_H */
