#ifndef __KV_STORE_H__
#define __KV_STORE_H__

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <sys/time.h>
#include <unistd.h>

#include "server.h"
#include "config.h"
#include "kv_dict.h"

#include <jemalloc/jemalloc.h>

#define NETWORK_REACTOR   0
#define NETWORK_PROACTOR  1
#define NETWORK_NTYCO     2

#define ENABLE_JEMALLOC 1
#define BUF_INIT_CAP    4096
#define KVS_MAX_TOKENS  256

/* --- msg_handler callback (compat) --- */
typedef int (*msg_handler)(char *msg, int len, DynBuf *write_buf);

extern int reactor_start(unsigned short port, msg_handler handler);
extern int proactor_start(unsigned short port, msg_handler handler);
extern int ntyco_start(unsigned short port, msg_handler handler);

long long get_time_now(void);

/* ==================== Zero-copy RESP parsing ==================== */

typedef struct {
    const char *ptr;
    int         len;
} kvs_token_t;

#define TOKEN_EQ(t, literal) \
    ((t).len == (int)(sizeof(literal)-1) && memcmp((t).ptr, literal, sizeof(literal)-1) == 0)

int  resp_parse_slices(const char *buf, int buflen,
                       kvs_token_t *tokens, int max_tokens,
                       int *out_argc);

int  kvs_protocol(char *msg, int length, DynBuf *write_buf);
int  kvs_dispatch(kvs_token_t *tokens, int argc,
                  const char *msg_raw, int msg_len, DynBuf *wb);

void kvs_flush_all(void);

/* ==================== Fast inline dynbuf append ==================== */

static inline int dynbuf_cat(DynBuf *db, const char *data, int len)
{
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

/* ==================== Value item ==================== */

typedef struct {
    char      *value;
    size_t     len;
    long long  expire;
} KVItem;

typedef void (*kv_scan_cb)(const char *key, KVItem *item, void *priv);

/* --- Allocator --- */
void *kvs_malloc(size_t size);
void kvs_free(void *ptr);

/* --- Item lifecycle --- */
KVItem* create_kvitem_len(const char *value, int vlen);
void    free_kvitem(KVItem *item);

/* --- Global dict --- */
extern kv_dict_t g_dict;

#endif
