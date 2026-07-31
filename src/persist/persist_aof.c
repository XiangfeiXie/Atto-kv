/* =========================================================
 * persist_aof.c — WAL/AOF reload (mmap + raw RESP replay).
 * ========================================================= */

#include "persistence.h"
#include "kvstore.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/stat.h>

/* =============================================================================
 * WAL reload — raw RESP format (one complete RESP command after another)
 * ============================================================================= */

void aof_reload_mmap(void) {
    int fd = open(persist_path(WAL_REL), O_RDONLY);
    if (fd < 0)
        fd = open(persist_path(AOF_REL), O_RDONLY);
    if (fd < 0) return;

    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size <= 0) { close(fd); return; }
    size_t fsz = (size_t)st.st_size;

    char *base = (char *)mmap(NULL, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
    int use_mmap = (base != MAP_FAILED);

    if (!use_mmap) {
        printf("[AOF] mmap failed: %s, falling back to read()\n", strerror(errno));

        base = malloc(fsz);
        if (!base) { close(fd); return; }

        lseek(fd, 0, SEEK_SET);
        size_t total_read = 0;
        while (total_read < fsz) {
            ssize_t n = read(fd, base + total_read, fsz - total_read);
            if (n <= 0) break;
            total_read += n;
        }
        close(fd);

        if (total_read != fsz) {
            printf("[AOF] read failed: only read %zu/%zu bytes\n", total_read, fsz);
            free(base);
            return;
        }
    } else {
        close(fd);
        madvise(base, fsz, MADV_SEQUENTIAL);
    }

    printf("[AOF] load start, size=%zu\n", fsz);

    g_in_reload = 1;
    DynBuf wb;
    dynbuf_init(&wb, BUF_INIT_CAP);
    kvs_token_t tokens[KVS_MAX_TOKENS];
    int loaded = 0;

    const char *p = base;
    long remaining = (long)fsz;
    while (remaining > 0) {
        int argc = 0;
        int cmd_len = resp_parse_slices(p, (int)remaining,
                                        tokens, KVS_MAX_TOKENS, &argc);
        if (cmd_len <= 0) break;
        dynbuf_clear(&wb);
        kvs_dispatch(tokens, argc, p, cmd_len, &wb, NULL);
        p += cmd_len;
        remaining -= cmd_len;
        loaded++;
    }

    dynbuf_free(&wb);

    if (use_mmap) {
        munmap(base, fsz);
    } else {
        free(base);
    }

    g_in_reload = 0;
    printf("[AOF] load finished, %d commands replayed\n", loaded);
}
