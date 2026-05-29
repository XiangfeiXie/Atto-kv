/* repl_incr.h — Incremental recovery for replication
 *
 * Features:
 *   - Ring buffer (backlog) for storing recent write operations
 *   - Partial resync on slave reconnection
 *   - Offset tracking and persistence
 *   - Automatic fallback to full sync when offset too old
 */
#ifndef REPL_INCR_H
#define REPL_INCR_H

#include <stdint.h>
#include <stddef.h>

/* Initialize incremental recovery subsystem */
int repl_incr_init(void);

/* Cleanup incremental recovery */
void repl_incr_shutdown(void);

/* Append data to backlog (Master side) */
int repl_incr_append(const char *data, size_t len);

/* Get data range from backlog for partial resync */
int repl_incr_get_range(uint64_t start_offset, char *out, size_t max_len, size_t *out_len);

/* Check if offset is still available in backlog */
int repl_incr_has_offset(uint64_t offset);

/* Get current master offset */
uint64_t repl_incr_get_master_offset(void);

/* Update slave acknowledged offset */
void repl_incr_ack_offset(uint64_t offset);

/* Save slave offset to disk (Slave side) */
int repl_incr_save_offset(uint64_t offset);

/* Load slave offset from disk (Slave side) */
uint64_t repl_incr_load_offset(void);

/* Get statistics */
typedef struct {
    uint64_t master_offset;
    uint64_t slave_offset;
    uint64_t backlog_size;
    uint64_t backlog_used;
    int      enabled;
} repl_incr_stats_t;

void repl_incr_get_stats(repl_incr_stats_t *stats);

#endif /* REPL_INCR_H */
