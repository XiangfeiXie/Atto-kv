/* =========================================================
 * persistence.c — project root resolution & path utilities.
 *
 * All persistence I/O logic lives in the sub-modules:
 *   persist_aof.c  — AOF write + rewrite + reload
 *   persist_rdb.c  — RDB save (fork + io_uring) and load (mmap)
 * ========================================================= */

#include "persistence.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <libgen.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/stat.h>

/* ── Project root directory — set once by persist_set_basedir ── */
static char  g_basedir[1024] = ".";
static int   g_basedir_set = 0;

/* Resolve a relative path against the project root.
 * Returns a pointer to a static buffer (not thread-safe). */
const char *persist_path(const char *rel) {
    static char buf[1024];
    int n = snprintf(buf, sizeof(buf), "%s/%s", g_basedir, rel);
    if (n < 0 || (size_t)n >= sizeof(buf)) return rel;
    return buf;
}

/* Determine the project root from the binary path (argv[0]).
 * If the binary lives in <root>/build/, strips /build to get <root>.
 * Otherwise uses the binary's directory as root. */
void persist_set_basedir(const char *argv0) {
    if (g_basedir_set) return;

    /* 1) Resolve the real path of the binary */
    char binpath[1024] = {0};
    if (argv0 && argv0[0] == '/') {
        strncpy(binpath, argv0, sizeof(binpath) - 1);
    } else if (argv0) {
        ssize_t n = readlink("/proc/self/exe", binpath, sizeof(binpath) - 1);
        if (n <= 0 && realpath(argv0, binpath) == NULL) {
            if (getcwd(binpath, sizeof(binpath) - 1)) {
                size_t blen = strlen(binpath);
                snprintf(binpath + blen, sizeof(binpath) - blen - 1, "/%s", argv0);
            }
        }
    } else {
        ssize_t n = readlink("/proc/self/exe", binpath, sizeof(binpath) - 1);
        if (n > 0) binpath[n] = '\0';
    }

    /* 2) Binary's directory (dirname() may truncate binpath in place) */
    char *dir = dirname(binpath);

    /* 3) If the binary lives in <root>/build/, strip /build to get <root> */
    char *slash = strrchr(dir, '/');
    char *base  = slash ? slash + 1 : dir;
    if (strcmp(base, "build") == 0 && slash)
        *slash = '\0';

    strncpy(g_basedir, dir, sizeof(g_basedir) - 1);
    g_basedir[sizeof(g_basedir) - 1] = '\0';
    g_basedir_set = 1;
}

/* Read a whole file into memory, preferring mmap (with a read() fallback).
 *
 * On success returns FILE_READ_OK with *buf and *size set; *is_mmap==1 means
 * the caller must munmap(*buf, *size), otherwise it must free(*buf).
 * `tag` is used only for log-prefix consistency ("AOF" / "RDB"). */
int file_read_full(const char *path, char **buf, size_t *size,
                   int *is_mmap, const char *tag) {
    *buf = NULL; *size = 0; *is_mmap = 0;

    int fd = open(path, O_RDONLY);
    if (fd < 0) return FILE_READ_OPEN_FAIL;

    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size <= 0) {
        close(fd);
        return FILE_READ_EMPTY;
    }
    size_t fsz = (size_t)st.st_size;

    char *base = (char *)mmap(NULL, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
    if (base != MAP_FAILED) {
        close(fd);
        madvise(base, fsz, MADV_SEQUENTIAL);
        *buf = base; *size = fsz; *is_mmap = 1;
        return FILE_READ_OK;
    }

    printf("[%s] mmap failed: %s, falling back to read()\n", tag, strerror(errno));

    base = malloc(fsz);
    if (!base) { close(fd); return FILE_READ_EMPTY; }

    lseek(fd, 0, SEEK_SET);
    size_t total_read = 0;
    while (total_read < fsz) {
        ssize_t n = read(fd, base + total_read, fsz - total_read);
        if (n <= 0) break;
        total_read += n;
    }
    close(fd);

    if (total_read != fsz) {
        printf("[%s] read failed: only read %zu/%zu bytes\n",
               tag, total_read, fsz);
        free(base);
        return FILE_READ_EMPTY;
    }

    *buf = base; *size = fsz; *is_mmap = 0;
    return FILE_READ_OK;
}
