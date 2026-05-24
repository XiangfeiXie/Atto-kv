#ifndef KVS_REPL_BACKLOG_H
#define KVS_REPL_BACKLOG_H

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>

/* ===================================================================
 * Replication Backlog - Ring buffer for incremental replication
 *
 * Inspired by Redis replication backlog design:
 * - Ring buffer stores recent write operations
 * - Supports partial resync on reconnection
 * - Master tracks write offset, slave tracks ack offset
 * =================================================================== */

#define REPL_BACKLOG_SIZE (16 * 1024 * 1024)  /* 16MB ring buffer */

typedef struct {
    char *buffer;
    size_t size;
    uint64_t master_offset;   /* Master current write position */
    uint64_t slave_offset;    /* Slave acknowledged position */
    pthread_mutex_t mutex;
    int enabled;
} repl_backlog_t;

/* Initialize backlog */
int repl_backlog_init(void);

/* Cleanup backlog */
void repl_backlog_free(void);

/* Append data to backlog (Master side) */
int repl_backlog_append(const char *data, size_t len);

/* Get data range from backlog (for resync) */
int repl_backlog_get_range(uint64_t start_offset, char *out, size_t max_len, size_t *out_len);

/* Update slave acknowledged offset */
void repl_backlog_ack(uint64_t offset);

/* Get current master offset */
uint64_t repl_backlog_get_master_offset(void);

/* Get current slave offset */
uint64_t repl_backlog_get_slave_offset(void);

/* Check if offset is still available in backlog */
int repl_backlog_has_offset(uint64_t offset);

#endif /* KVS_REPL_BACKLOG_H */
