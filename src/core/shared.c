#include "shared.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

SharedResponses g_shared_resp;

/* Static storage for integer strings to avoid malloc */
static char integer_buffers[256][16];

void init_shared_responses(void) {
    /* Pre-allocated simple responses */
    g_shared_resp.ok = "+OK\r\n";
    g_shared_resp.err = "-ERR\r\n";
    g_shared_resp.nil_bulk = "$-1\r\n";
    g_shared_resp.empty_array = "*0\r\n";
    g_shared_resp.pong = "+PONG\r\n";
    g_shared_resp.crlf = "\r\n";

    /* Pre-allocated integer responses :0\r\n to :255\r\n
     * Covers 90%+ of common integer responses (success flags, counts, etc.) */
    for (int i = 0; i < 256; i++) {
        snprintf(integer_buffers[i], sizeof(integer_buffers[i]), ":%d\r\n", i);
        g_shared_resp.integers[i] = integer_buffers[i];
    }
}
