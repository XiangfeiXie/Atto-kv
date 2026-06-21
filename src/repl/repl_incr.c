/* repl_incr.c — Incremental recovery implementation
 *
 * Architecture:
 *   - Master maintains 16MB ring buffer (backlog)
 *   - Stores recent write operations with offset tracking
 *   - Slave saves last offset to /tmp/repl_offset
 *   - On reconnect, slave sends offset to master
 *   - Master checks if offset still in backlog
 *   - If yes: partial resync (send missing data)
 *   - If no: trigger full sync
 */
#include "repl_incr.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>

#define REPL_BACKLOG_SIZE (64 * 1024 * 1024)  /* 64MB ring buffer */
#define REPL_OFFSET_FILE "/tmp/repl_offset"

typedef struct {
    char *buffer;
    size_t size;
    uint64_t master_offset;
    uint64_t slave_offset;
    pthread_mutex_t mutex;
    int enabled;
} repl_backlog_t;

static repl_backlog_t g_backlog = {0};

/* ── Initialization ──────────────────────────────────────────────── */

int repl_incr_init(void) {
    memset(&g_backlog, 0, sizeof(g_backlog));

    g_backlog.buffer = (char *)malloc(REPL_BACKLOG_SIZE);
    if (!g_backlog.buffer) {
        fprintf(stderr, "[REPL-INCR] malloc failed\n");
        return -1;
    }

    g_backlog.size = REPL_BACKLOG_SIZE;
    g_backlog.master_offset = 0;
    g_backlog.slave_offset = 0;
    g_backlog.enabled = 1;

    if (pthread_mutex_init(&g_backlog.mutex, NULL) != 0) {
        free(g_backlog.buffer);
        fprintf(stderr, "[REPL-INCR] mutex init failed\n");
        return -1;
    }

    printf("[REPL-INCR] Initialized, backlog size=%d MB\n",
           (int)(REPL_BACKLOG_SIZE / (1024 * 1024)));
    return 0;
}

void repl_incr_shutdown(void) {
    pthread_mutex_lock(&g_backlog.mutex);
    g_backlog.enabled = 0;
    if (g_backlog.buffer) {
        free(g_backlog.buffer);
        g_backlog.buffer = NULL;
    }
    pthread_mutex_unlock(&g_backlog.mutex);
    pthread_mutex_destroy(&g_backlog.mutex);
    printf("[REPL-INCR] Shutdown complete\n");
}

/* ── Backlog operations ──────────────────────────────────────────── */

int repl_incr_append(const char *data, size_t len) {
    if (!data || len == 0) return 0;

    pthread_mutex_lock(&g_backlog.mutex);

    if (!g_backlog.enabled || !g_backlog.buffer) {
        pthread_mutex_unlock(&g_backlog.mutex);
        return -1;
    }

    /* Check if buffer has enough space */
    uint64_t used = g_backlog.master_offset - g_backlog.slave_offset;
    if (used + len > g_backlog.size) {
        /* Rate-limit error logging to once per second */
        static time_t last_log = 0;
        time_t now = time(NULL);
        if (now > last_log) {
            fprintf(stderr, "[REPL-INCR] Backlog full, used=%llu len=%zu\n",
                    (unsigned long long)used, len);
            last_log = now;
        }
        pthread_mutex_unlock(&g_backlog.mutex);
        return -1;
    }

    /* Ring buffer write */
    size_t pos = g_backlog.master_offset % g_backlog.size;
    size_t remain = g_backlog.size - pos;

    if (len <= remain) {
        memcpy(g_backlog.buffer + pos, data, len);
    } else {
        /* Wrap around */
        memcpy(g_backlog.buffer + pos, data, remain);
        memcpy(g_backlog.buffer, data + remain, len - remain);
    }

    g_backlog.master_offset += len;
    pthread_mutex_unlock(&g_backlog.mutex);
    return 0;
}

int repl_incr_get_range(uint64_t start_offset, char *out, size_t max_len, size_t *out_len) {
    if (!out || !out_len) return -1;

    pthread_mutex_lock(&g_backlog.mutex);

    if (!g_backlog.enabled || !g_backlog.buffer) {
        pthread_mutex_unlock(&g_backlog.mutex);
        return -1;
    }

    /* Check if offset is too old (outside ring buffer) */
    uint64_t oldest = 0;
    if (g_backlog.master_offset > g_backlog.size) {
        oldest = g_backlog.master_offset - g_backlog.size;
    }
    if (start_offset < oldest) {
        pthread_mutex_unlock(&g_backlog.mutex);
        return -1;  /* Need full resync */
    }

    /* Check if offset is beyond current position */
    if (start_offset >= g_backlog.master_offset) {
        *out_len = 0;
        pthread_mutex_unlock(&g_backlog.mutex);
        return 0;  /* No new data */
    }

    /* Calculate available data */
    uint64_t available = g_backlog.master_offset - start_offset;
    size_t copy_len = (available < max_len) ? (size_t)available : max_len;

    /* Ring buffer read */
    size_t pos = start_offset % g_backlog.size;
    size_t remain = g_backlog.size - pos;

    if (copy_len <= remain) {
        memcpy(out, g_backlog.buffer + pos, copy_len);
    } else {
        /* Wrap around */
        memcpy(out, g_backlog.buffer + pos, remain);
        memcpy(out + remain, g_backlog.buffer, copy_len - remain);
    }

    *out_len = copy_len;
    pthread_mutex_unlock(&g_backlog.mutex);
    return 0;
}

int repl_incr_has_offset(uint64_t offset) {
    pthread_mutex_lock(&g_backlog.mutex);

    if (!g_backlog.enabled) {
        pthread_mutex_unlock(&g_backlog.mutex);
        return 0;
    }

    uint64_t oldest = 0;
    if (g_backlog.master_offset > g_backlog.size) {
        oldest = g_backlog.master_offset - g_backlog.size;
    }

    int has = (offset >= oldest) && (offset <= g_backlog.master_offset);
    pthread_mutex_unlock(&g_backlog.mutex);
    return has;
}

uint64_t repl_incr_get_master_offset(void) {
    pthread_mutex_lock(&g_backlog.mutex);
    uint64_t offset = g_backlog.master_offset;
    pthread_mutex_unlock(&g_backlog.mutex);
    return offset;
}

uint64_t repl_incr_get_backlog_start(void) {
    pthread_mutex_lock(&g_backlog.mutex);
    uint64_t oldest = 0;
    if (g_backlog.master_offset > g_backlog.size) {
        oldest = g_backlog.master_offset - g_backlog.size;
    }
    pthread_mutex_unlock(&g_backlog.mutex);
    return oldest;
}

void repl_incr_ack_offset(uint64_t offset) {
    pthread_mutex_lock(&g_backlog.mutex);
    if (offset > g_backlog.slave_offset) {
        g_backlog.slave_offset = offset;
    }
    pthread_mutex_unlock(&g_backlog.mutex);
}

/* ── Offset persistence (Slave side) ─────────────────────────────── */

int repl_incr_save_offset(uint64_t offset) {
    FILE *fp = fopen(REPL_OFFSET_FILE, "w");
    if (!fp) {
        perror("[REPL-INCR] fopen");
        return -1;
    }

    fprintf(fp, "%llu\n", (unsigned long long)offset);
    fclose(fp);
    return 0;
}

uint64_t repl_incr_load_offset(void) {
    FILE *fp = fopen(REPL_OFFSET_FILE, "r");
    if (!fp) {
        return 0;  /* No saved offset, start from 0 */
    }

    uint64_t offset = 0;
    if (fscanf(fp, "%llu", (unsigned long long *)&offset) != 1) {
        offset = 0;
    }
    fclose(fp);

    printf("[REPL-INCR] Loaded offset: %llu\n", (unsigned long long)offset);
    return offset;
}

/* ── Statistics ──────────────────────────────────────────────────── */

void repl_incr_get_stats(repl_incr_stats_t *stats) {
    if (!stats) return;

    pthread_mutex_lock(&g_backlog.mutex);
    stats->master_offset = g_backlog.master_offset;
    stats->slave_offset = g_backlog.slave_offset;
    stats->backlog_size = g_backlog.size;
    stats->backlog_used = g_backlog.master_offset - g_backlog.slave_offset;
    stats->enabled = g_backlog.enabled;
    pthread_mutex_unlock(&g_backlog.mutex);
}
