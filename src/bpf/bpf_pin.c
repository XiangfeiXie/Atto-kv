/* bpf_pin.c — implementation of the kvstore BPF pin-dir helpers */
#define _GNU_SOURCE
#include "bpf_pin.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mount.h>

#include <bpf/bpf.h>

/* Build "/sys/fs/bpf/kvstore/<name>" into buf, returns 0/-1. */
static int join_pin_path(char *buf, size_t buflen, const char *name)
{
    if (!name || !*name) return -1;
    int n = snprintf(buf, buflen, "%s/%s", KVS_PIN_DIR, name);
    return (n > 0 && (size_t)n < buflen) ? 0 : -1;
}

int kvs_bpf_pin_dir_ensure(void)
{
    struct stat st;

    /* /sys/fs/bpf must be a bpf filesystem.  Most distros already mount it
     * via systemd; if not we let the user mount it themselves. */
    if (stat("/sys/fs/bpf", &st) != 0) {
        fprintf(stderr, "[BPF-PIN] /sys/fs/bpf missing — "
                "is the bpf filesystem mounted?\n");
        return -1;
    }

    if (stat(KVS_PIN_DIR, &st) == 0) {
        if (S_ISDIR(st.st_mode)) return 0;
        fprintf(stderr, "[BPF-PIN] %s exists but is not a directory\n",
                KVS_PIN_DIR);
        return -1;
    }

    if (mkdir(KVS_PIN_DIR, 0755) != 0) {
        fprintf(stderr, "[BPF-PIN] mkdir(%s): %s\n",
                KVS_PIN_DIR, strerror(errno));
        return -1;
    }
    return 0;
}

int kvs_bpf_pin_map(int map_fd, const char *name)
{
    if (map_fd < 0) return -1;
    if (kvs_bpf_pin_dir_ensure() != 0) return -1;

    char path[512];
    if (join_pin_path(path, sizeof(path), name) != 0) return -1;

    /* Drop any stale pin from a previous run (or this run's earlier crash). */
    (void)unlink(path);

    if (bpf_obj_pin(map_fd, path) != 0) {
        fprintf(stderr, "[BPF-PIN] bpf_obj_pin(%s): %s\n",
                path, strerror(errno));
        return -1;
    }
    return 0;
}

void kvs_bpf_unpin_map(const char *name)
{
    char path[512];
    if (join_pin_path(path, sizeof(path), name) != 0) return;
    (void)unlink(path);
}

int kvs_bpf_open_pinned(const char *name)
{
    char path[512];
    if (join_pin_path(path, sizeof(path), name) != 0) return -1;
    int fd = bpf_obj_get(path);
    if (fd < 0) {
        /* Caller may treat as soft failure (program not loaded). */
        return -1;
    }
    return fd;
}
