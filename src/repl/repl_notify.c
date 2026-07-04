/* repl_notify.c — Long command replication notification implementation */

#include "repl_notify.h"
#include "repl_tcp_internal.h"
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>

/* Global queue */
struct repl_notify_queue g_repl_notify_queue = {0};

/* Statistics */
static atomic_uint_fast64_t stat_pushed = 0;
static atomic_uint_fast64_t stat_dropped = 0;
static atomic_uint_fast64_t stat_processed = 0;

/* Initialize notification queue */
void repl_notify_init(void)
{
    atomic_store(&g_repl_notify_queue.head, 0);
    atomic_store(&g_repl_notify_queue.tail, 0);
    atomic_store(&stat_pushed, 0);
    atomic_store(&stat_dropped, 0);
    atomic_store(&stat_processed, 0);
}

/* Push notification (called by main process)
 * Lock-free, single-producer single-consumer safe
 */
int repl_notify_push(const char *cmd, size_t len)
{
    /* Validate input */
    if (!cmd || len == 0 || len > REPL_NOTIFY_MAX_CMD_SIZE) {
        return -1;
    }

    /* Only handle commands >4KB (≤4KB are handled by eBPF) */
    if (len <= 4096) {
        return 0;
    }

    /* Load indices with relaxed ordering (producer only touches tail) */
    uint32_t tail = atomic_load_explicit(&g_repl_notify_queue.tail, memory_order_relaxed);
    uint32_t next_tail = (tail + 1) % REPL_NOTIFY_QUEUE_SIZE;

    /* Check if queue is full (acquire to sync with consumer's head update) */
    uint32_t head = atomic_load_explicit(&g_repl_notify_queue.head, memory_order_acquire);
    if (next_tail == head) {
        /* Queue full - drop notification
         * Slave will recover via backlog on reconnect
         */
        atomic_fetch_add(&stat_dropped, 1);
        return -1;
    }

    /* Copy command data */
    memcpy(g_repl_notify_queue.items[tail].data, cmd, len);
    g_repl_notify_queue.items[tail].len = (uint32_t)len;

    /* Update tail (release to make data visible to consumer) */
    atomic_store_explicit(&g_repl_notify_queue.tail, next_tail, memory_order_release);

    atomic_fetch_add(&stat_pushed, 1);
    return 0;
}

/* Process all pending notifications (called by master replication thread) */
int repl_notify_flush(void)
{
    int processed = 0;

    /* Load indices (acquire to sync with producer's tail update) */
    uint32_t head = atomic_load_explicit(&g_repl_notify_queue.head, memory_order_relaxed);
    uint32_t tail = atomic_load_explicit(&g_repl_notify_queue.tail, memory_order_acquire);

    /* Process all pending items */
    while (head != tail) {
        struct repl_notify_item *item = &g_repl_notify_queue.items[head];

        /* Append to backlog (for slave reconnect) */
        repl_incr_append((const char *)item->data, item->len);

        /* Forward to slave if connected */
        if (g_repl.peer_fd >= 0) {
            /* For large commands, send in frames to avoid TCP buffer overflow */
            size_t pos = 0;
            while (pos < item->len) {
                uint32_t frame_size = (item->len - pos > REPL_MAX_FRAME_SIZE)
                                      ? REPL_MAX_FRAME_SIZE
                                      : (item->len - pos);

                if (tcp_send_data_frame(g_repl.peer_fd, item->data + pos, frame_size) != 0) {
                    /* Slave disconnected */
                    close(g_repl.peer_fd);
                    g_repl.peer_fd = -1;
                    pthread_mutex_lock(&g_repl.stats_lock);
                    g_repl.stats.connected = 0;
                    pthread_mutex_unlock(&g_repl.stats_lock);
                    fprintf(stderr, "[REPL-NOTIFY] Slave disconnected during send\n");
                    break;
                }

                pos += frame_size;
            }

            /* Update statistics if fully sent */
            if (pos == item->len) {
                pthread_mutex_lock(&g_repl.stats_lock);
                g_repl.stats.bytes_sent += item->len;
                g_repl.stats.large_commands++;
                pthread_mutex_unlock(&g_repl.stats_lock);
            }
        }

        /* Move to next item */
        head = (head + 1) % REPL_NOTIFY_QUEUE_SIZE;
        processed++;
    }

    /* Update head (release to make space visible to producer) */
    if (processed > 0) {
        atomic_store_explicit(&g_repl_notify_queue.head, head, memory_order_release);
        atomic_fetch_add(&stat_processed, processed);
    }

    return processed;
}

/* Get statistics */
void repl_notify_get_stats(uint64_t *pushed, uint64_t *dropped, uint64_t *processed)
{
    if (pushed) *pushed = atomic_load(&stat_pushed);
    if (dropped) *dropped = atomic_load(&stat_dropped);
    if (processed) *processed = atomic_load(&stat_processed);
}
