#include "dynbuf.h"
#include "server.h"
#include <stdlib.h>
#include <string.h>

void dynbuf_init(DynBuf *db, int init_cap) {
    if (!db || init_cap <= 0) return;
    db->buf = (char *)malloc((size_t)init_cap);
    db->len = 0;
    db->cap = init_cap;
}

int dynbuf_reserve(DynBuf *db, int total_cap) {
    if (!db || total_cap <= 0) return -1;
    if (total_cap <= db->cap) return 0;

    /* Cap growth at KVS_NET_BUF_MAX — reject rather than grow unbounded */
    int new_cap = db->cap > 0 ? db->cap : 16;
    while (new_cap < total_cap) new_cap *= 2;
    if (new_cap > KVS_NET_BUF_MAX) return -2;

    char *nb = (char *)realloc(db->buf, (size_t)new_cap);
    if (!nb) return -1;

    /* Pre-fault newly added pages to avoid minor faults on write */
    size_t old_bytes = db->cap > 0 ? (size_t)db->cap : 0;
    for (size_t off = (old_bytes + 4095) & ~(size_t)4095;
         off < (size_t)new_cap; off += 4096)
        nb[off] = 0;

    db->buf = nb;
    db->cap = new_cap;
    return 0;
}

int dynbuf_append(DynBuf *db, const char *data, int len) {
    if (!db || !data || len <= 0) return -1;
    int rc = dynbuf_reserve(db, db->len + len);
    if (rc < 0) return rc;
    memcpy(db->buf + db->len, data, (size_t)len);
    db->len += len;
    return 0;
}

int dynbuf_cat(DynBuf *db, const char *data, int len) {
    return dynbuf_append(db, data, len);
}

void dynbuf_clear(DynBuf *db) {
    if (!db) return;
    db->len = 0;
}

void dynbuf_free(DynBuf *db) {
    if (!db) return;
    free(db->buf);
    db->buf = NULL;
    db->len = 0;
    db->cap = 0;
}
