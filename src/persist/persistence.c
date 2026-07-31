/* =========================================================
 * persistence.c — project root resolution & path utilities.
 *
 * All persistence I/O logic lives in the sub-modules:
 *   wal_persist.c  — WAL incremental write (background consumer)
 *   persist_aof.c  — WAL/AOF reload (mmap replay)
 *   persist_rdb.c  — RDB save (fork + io_uring) and load (mmap)
 * ========================================================= */

#include "persistence.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <libgen.h>
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

    /* 2) Get the binary's parent directory */
    char *d = dirname(binpath);

    char dircopy[1024];
    strncpy(dircopy, binpath, sizeof(dircopy) - 1);
    dircopy[sizeof(dircopy) - 1] = '\0';
    char *dd = dirname(dircopy);
    char *dn = strrchr(dd, '/');
    if (dn) dn++; else dn = dd;

    /* If binary is in .../build/, set root to the parent of build */
    if (strcmp(dn, "build") == 0) {
        char *last = strrchr(dd, '/');
        if (last) *last = '\0';
        strncpy(g_basedir, dd, sizeof(g_basedir) - 1);
    } else {
        strncpy(g_basedir, dd, sizeof(g_basedir) - 1);
    }

    g_basedir[sizeof(g_basedir) - 1] = '\0';
    g_basedir_set = 1;
}
