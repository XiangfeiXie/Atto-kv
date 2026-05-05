#include "dynbuf.h"
#include <stdlib.h>
#include <string.h>

void dynbuf_init(DynBuf *db, int init_cap) {
    if (!db || init_cap <= 0) return;
    db->buf = (char *)malloc((size_t)init_cap);
    db->len = 0;
    db->cap = init_cap;
}

int dynbuf_append(DynBuf *db, const char *data, int len) {
    if (!db || !data || len <= 0) return -1;
    if (db->len + len > db->cap) {
        int new_cap = db->cap > 0 ? db->cap : 16;
        while (new_cap < db->len + len) new_cap *= 2;
        char *nb = (char *)realloc(db->buf, (size_t)new_cap);
        if (!nb) return -1;
        db->buf = nb;
        db->cap = new_cap;
    }
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
