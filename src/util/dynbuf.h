#ifndef __DYN_BUF_H__
#define __DYN_BUF_H__

/* Lightweight dynamic buffer — single contiguous allocation with len/cap. */

typedef struct {
    char *buf;
    int   len;
    int   cap;
} DynBuf;

void dynbuf_init(DynBuf *db, int init_cap);
int  dynbuf_reserve(DynBuf *db, int total_cap);
int  dynbuf_cat(DynBuf *db, const char *data, int len);
void dynbuf_clear(DynBuf *db);
void dynbuf_free(DynBuf *db);


#define dynbuf_lit(wb, s) dynbuf_cat((wb), "" s, (int)(sizeof(s) - 1))

#endif
