/* shm_ipc.c — see shm_ipc.h for the design notes. */

#include "shm_ipc.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

/* Fixed region name.  Override with XDPKCP_SHM_NAME to run multiple
 * master/slave pairs on one machine (each needs its own name). */
#ifndef SHM_NAME
#define SHM_NAME "/kvstore_xdpkcp_shm"
#endif

static const char *shm_name(void) {
    const char *n = getenv("XDPKCP_SHM_NAME");
    return n ? n : SHM_NAME;
}

shm_region_t *shm_region_create(void) {
    const char *name = shm_name();
    int fd = shm_open(name, O_CREAT | O_RDWR, 0600);
    if (fd < 0) {
        fprintf(stderr, "[SHM] shm_open(%s) create failed: %s\n",
                name, strerror(errno));
        return NULL;
    }

    if (ftruncate(fd, (off_t)sizeof(shm_region_t)) != 0) {
        fprintf(stderr, "[SHM] ftruncate failed: %s\n", strerror(errno));
        close(fd);
        return NULL;
    }

    shm_region_t *r = mmap(NULL, sizeof(shm_region_t),
                           PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (r == MAP_FAILED) {
        fprintf(stderr, "[SHM] mmap failed: %s\n", strerror(errno));
        return NULL;
    }

    /* Creator owns the initial state: empty queue. */
    atomic_init(&r->q.head, 0);
    atomic_init(&r->q.tail, 0);
    return r;
}

shm_region_t *shm_region_attach(void) {
    const char *name = shm_name();
    int fd = shm_open(name, O_RDWR, 0600);
    if (fd < 0) {
        fprintf(stderr, "[SHM] shm_open(%s) attach failed: %s\n",
                name, strerror(errno));
        return NULL;
    }

    shm_region_t *r = mmap(NULL, sizeof(shm_region_t),
                           PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (r == MAP_FAILED) {
        fprintf(stderr, "[SHM] mmap failed: %s\n", strerror(errno));
        return NULL;
    }
    return r;
}

void shm_region_destroy(shm_region_t *r, int creator) {
    if (!r) return;
    munmap(r, sizeof(shm_region_t));
    if (creator) shm_unlink(shm_name());
}

int spsc_enqueue(spsc_queue_t *q, const void *data, uint32_t len) {
    if (!data || len == 0 || len > SHM_MAX_PAYLOAD)
        return -1;

    uint64_t head = atomic_load_explicit(&q->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&q->tail, memory_order_acquire);

    if (head - tail >= SHM_RING_SIZE)
        return -1;                     /* full */

    shm_pkt_t *slot = &q->ring[head & SHM_RING_MASK];
    slot->len = len;
    memcpy(slot->data, data, len);

    /* Publish the write before the producer index becomes visible. */
    atomic_store_explicit(&q->head, head + 1, memory_order_release);
    return 0;
}

int spsc_dequeue(spsc_queue_t *q, void *data, uint32_t *len) {
    uint64_t tail = atomic_load_explicit(&q->tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&q->head, memory_order_acquire);

    if (tail == head)
        return -1;                     /* empty */

    shm_pkt_t *slot = &q->ring[tail & SHM_RING_MASK];
    uint32_t n = slot->len;
    if (n > SHM_MAX_PAYLOAD)
        n = SHM_MAX_PAYLOAD;           /* defensive; should never happen */
    memcpy(data, slot->data, n);
    if (len) *len = n;

    /* Release the slot only after the read is complete. */
    atomic_store_explicit(&q->tail, tail + 1, memory_order_release);
    return 0;
}

int spsc_empty(const spsc_queue_t *q) {
    uint64_t tail = atomic_load_explicit(&q->tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&q->head, memory_order_acquire);
    return head == tail;
}
