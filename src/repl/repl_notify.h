/* repl_notify.h — Long command replication notification
 *
 * For commands >4KB, eBPF cannot efficiently capture across multiple skbs.
 * Main process handles these commands normally (recv + execute), then
 * notifies the replication module via a lock-free queue.
 *
 * Architecture:
 *   Main process → lock-free queue (~10ns) → Master replication thread
 */

#ifndef REPL_NOTIFY_H
#define REPL_NOTIFY_H

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>

/* Maximum number of pending notifications */
#define REPL_NOTIFY_QUEUE_SIZE  256

/* Maximum command size we buffer (4MB, matches REPL_OBSERVE_MAX_CMD) */
#define REPL_NOTIFY_MAX_CMD_SIZE  (4 * 1024 * 1024)

/* Notification item */
struct repl_notify_item {
    char     data[REPL_NOTIFY_MAX_CMD_SIZE];
    uint32_t len;
};

/* Lock-free ring buffer queue */
struct repl_notify_queue {
    struct repl_notify_item items[REPL_NOTIFY_QUEUE_SIZE];
    atomic_uint_fast32_t    head;  /* Consumer index (master thread) */
    atomic_uint_fast32_t    tail;  /* Producer index (main process) */
};

/* Global queue instance */
extern struct repl_notify_queue g_repl_notify_queue;

/* Initialize notification queue */
void repl_notify_init(void);

/* Push notification (called by main process after command execution)
 * Returns 0 on success, -1 if queue is full
 * Thread-safe, lock-free
 */
int repl_notify_push(const char *cmd, size_t len);

/* Process all pending notifications (called by master replication thread)
 * Returns number of items processed
 */
int repl_notify_flush(void);

/* Get statistics */
void repl_notify_get_stats(uint64_t *pushed, uint64_t *dropped, uint64_t *processed);

#endif /* REPL_NOTIFY_H */
