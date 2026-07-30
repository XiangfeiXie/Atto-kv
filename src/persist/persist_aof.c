/* =========================================================
 * persist_aof.c — WAL reload (mmap).
 * ========================================================= */

#include "persistence.h"
#include "kvstore.h"
#include "wal_persist.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <inttypes.h>
#include <sys/mman.h>
#include <sys/stat.h>

/* =============================================================================
 * WAL reload (mmap)
 * ============================================================================= */

void aof_reload_mmap(void) {
    int fd = open(persist_path(WAL_REL), O_RDONLY);
    int wal_format = (fd >= 0);
    if (!wal_format)
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

    printf("[AOF] %s load start, size=%zu\n",
           wal_format ? "WAL" : "legacy AOF", fsz);

    g_in_reload = 1;
    DynBuf wb;
    dynbuf_init(&wb, BUF_INIT_CAP);
    kvs_token_t tokens[KVS_MAX_TOKENS];
    int loaded = 0;

    if (wal_format) {
        const char *p   = base;
        const char *end = base + fsz;
        while (p + sizeof(kvs_wal2_record_hdr_t) <= end) {
            kvs_wal2_record_hdr_t hdr;
            memcpy(&hdr, p, sizeof(hdr));
            if (hdr.magic != KVS_WAL2_MAGIC) break;
            if (p + sizeof(kvs_wal2_record_hdr_t) + hdr.len > end) break;
            const char *payload = p + sizeof(kvs_wal2_record_hdr_t);
            uint32_t computed = kvs_crc32c(0, &hdr.seq, sizeof(hdr.seq));
            computed = kvs_crc32c(computed, &hdr.kind, sizeof(hdr.kind));
            computed = kvs_crc32c(computed, &hdr.flags, sizeof(hdr.flags));
            computed = kvs_crc32c(computed, &hdr.len, sizeof(hdr.len));
            computed = kvs_crc32c(computed, payload, (size_t)hdr.len);
            if (hdr.crc32 != computed) {
                fprintf(stderr, "[AOF] WAL CRC32 mismatch seq=%" PRIu64
                        ", stopping replay\n", hdr.seq);
                break;
            }
            int argc = 0;
            int cmd_len = resp_parse_slices(payload, (int)hdr.len,
                                            tokens, KVS_MAX_TOKENS, &argc);
            if (cmd_len > 0) {
                dynbuf_clear(&wb);
                kvs_dispatch(tokens, argc, payload, cmd_len, &wb, NULL);
                loaded++;
            }
            p += sizeof(kvs_wal2_record_hdr_t) + hdr.len;
        }
    } else {
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
