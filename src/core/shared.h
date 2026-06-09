#ifndef SHARED_H
#define SHARED_H

/* Shared response objects to avoid repeated allocations.
 * Pre-allocated common responses reduce malloc overhead in hot paths,
 * especially for P=1-10 scenarios where small responses dominate. */

typedef struct {
    const char *ok;          /* "+OK\r\n" */
    const char *err;         /* "-ERR\r\n" */
    const char *nil_bulk;    /* "$-1\r\n" (null bulk string) */
    const char *empty_array; /* "*0\r\n" (empty array) */
    const char *pong;        /* "+PONG\r\n" */
    const char *crlf;        /* "\r\n" */
    const char *integers[256]; /* ":0\r\n" to ":255\r\n" (common range) */
} SharedResponses;

extern SharedResponses g_shared_resp;

/* Initialize shared responses - call once at startup */
void init_shared_responses(void);

#endif /* SHARED_H */
