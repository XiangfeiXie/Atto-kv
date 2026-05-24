#include "repl_backlog.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

static repl_backlog_t g_backlog;

int repl_backlog_init(void) {
    memset(&g_backlog, 0, sizeof(g_backlog));

    g_backlog.buffer = (char *)malloc(REPL_BACKLOG_SIZE);
    if (!g_backlog.buffer) {
        fprintf(stderr, "[REPL-BACKLOG] malloc failed\n");
        return -1;
    }

    g_backlog.size = REPL_BACKLOG_SIZE;
    g_backlog.master_offset = 0;
    g_backlog.slave_offset = 0;
    g_backlog.enabled = 1;

    if (pthread_mutex_init(&g_backlog.mutex, NULL) != 0) {
        free(g_backlog.buffer);
        fprintf(stderr, "[REPL-BACKLOG] mutex init failed\n");
        return -1;
    }

    printf("[REPL-BACKLOG] initialized, size=%d MB\n", (int)(REPL_BACKLOG_SIZE / (1024 * 1024)));
    return 0;
}

void repl_backlog_free(void) {
    pthread_mutex_lock(&g_backlog.mutex);
    g_backlog.enabled = 0;
    if (g_backlog.buffer) {
        free(g_backlog.buffer);
        g_backlog.buffer = NULL;
    }
    pthread_mutex_unlock(&g_backlog.mutex);
    pthread_mutex_destroy(&g_backlog.mutex);
}

int repl_backlog_append(const char *data, size_t len) {
    if (!data || len == 0) return 0;

    pthread_mutex_lock(&g_backlog.mutex);

    if (!g_backlog.enabled || !g_backlog.buffer) {
        pthread_mutex_unlock(&g_backlog.mutex);
        fprintf(stderr, "[REPL-BACKLOG] append failed: not enabled or no buffer\n");
        return -1;
    }

    /* Check if buffer has enough space */
    uint64_t used = g_backlog.master_offset - g_backlog.slave_offset;
    if (used + len > g_backlog.size) {
        pthread_mutex_unlock(&g_backlog.mutex);
        fprintf(stderr, "[REPL-BACKLOG] buffer full, used=%llu len=%zu\n",
                (unsigned long long)used, len);
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
    printf("[REPL-BACKLOG] appended %zu bytes, master_offset=%llu\n",
           len, (unsigned long long)g_backlog.master_offset);
    pthread_mutex_unlock(&g_backlog.mutex);
    return 0;
}

int repl_backlog_get_range(uint64_t start_offset, char *out, size_t max_len, size_t *out_len) {
    if (!out || !out_len) return -1;

    pthread_mutex_lock(&g_backlog.mutex);

    if (!g_backlog.enabled || !g_backlog.buffer) {
        pthread_mutex_unlock(&g_backlog.mutex);
        return -1;
    }

    /* Check if offset is too old (outside ring buffer) */
    if (start_offset < g_backlog.master_offset - g_backlog.size) {
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

void repl_backlog_ack(uint64_t offset) {
    pthread_mutex_lock(&g_backlog.mutex);
    if (offset > g_backlog.slave_offset) {
        g_backlog.slave_offset = offset;
    }
    pthread_mutex_unlock(&g_backlog.mutex);
}

uint64_t repl_backlog_get_master_offset(void) {
    pthread_mutex_lock(&g_backlog.mutex);
    uint64_t offset = g_backlog.master_offset;
    pthread_mutex_unlock(&g_backlog.mutex);
    return offset;
}

uint64_t repl_backlog_get_slave_offset(void) {
    pthread_mutex_lock(&g_backlog.mutex);
    uint64_t offset = g_backlog.slave_offset;
    pthread_mutex_unlock(&g_backlog.mutex);
    return offset;
}

int repl_backlog_has_offset(uint64_t offset) {
    pthread_mutex_lock(&g_backlog.mutex);
    int has = (offset >= g_backlog.master_offset - g_backlog.size) &&
              (offset <= g_backlog.master_offset);
    pthread_mutex_unlock(&g_backlog.mutex);
    return has;
}
