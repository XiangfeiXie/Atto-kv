#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "proactor_types.h"
#include "proactor_inline.h"
#include "kvstore.h"
#include "config.h"

/* Initialize connection pool (lazy allocation - no pre-heating) */
int conn_pool_init(int max_clients) {
    conn_pool_size = max_clients;
    conn_pool = calloc(max_clients, sizeof(struct conn));
    if (!conn_pool) {
        fprintf(stderr, "[conn] failed to allocate connection pool\n");
        return -1;
    }

    printf("[conn] pool initialized: %d slots (lazy allocation)\n", max_clients);
    return 0;
}

/* Allocate connection (on-demand) */
struct conn *conn_alloc(int fd) {
    if (fd < 0 || fd >= conn_pool_size) return NULL;

    struct conn *c = &conn_pool[fd];
    c->fd = fd;
    c->active = 1;
    c->gen = next_conn_gen();
    c->r_off = 0;
    c->w_off = 0;
    c->recv_armed = 0;
    c->send_armed = 0;
    c->status = 0;

    /* Allocate buffers on-demand */
    dynbuf_init(&c->read_buf, KVS_NET_BUF_INIT);
    dynbuf_init(&c->write_buf, KVS_NET_BUF_INIT);

    /* Allocate client on-demand */
    c->client = calloc(1, sizeof(client_t));
    if (c->client) {
        c->client->conn = c;
    }

    return c;
}

/* Reset connection state */
void conn_reset(struct conn *c) {
    if (!c) return;

    c->r_off = 0;
    c->w_off = 0;
    c->recv_armed = 0;
    c->send_armed = 0;
    c->status = 0;

    dynbuf_clear(&c->read_buf);
    dynbuf_clear(&c->write_buf);

    if (c->client) {
        memset(c->client, 0, sizeof(client_t));
        c->client->conn = c;
    }
}

/* Release connection */
void conn_release(int fd) {
    if (fd < 0 || fd >= conn_pool_size) return;

    struct conn *c = &conn_pool[fd];

    if (c->client) {
        free(c->client);
        c->client = NULL;
    }

    if (c->active) {
        atomic_fetch_sub_explicit(&g_kvs_connected_clients, 1,
                                   memory_order_relaxed);
    }

    if (proactor_debug_enabled()) {
        fprintf(stderr, "[proactor] release fd=%d gen=%u recv_armed=%d send_armed=%d\n",
                fd, c->gen, c->recv_armed, c->send_armed);
    }

    dynbuf_free(&c->read_buf);
    dynbuf_free(&c->write_buf);
    memset(c, 0, sizeof(*c));
    close(fd);
}

/* Destroy connection pool */
void conn_pool_destroy(void) {
    if (!conn_pool) return;

    for (int i = 0; i < conn_pool_size; i++) {
        if (conn_pool[i].active) {
            conn_release(i);
        }
    }

    free(conn_pool);
    conn_pool = NULL;
    conn_pool_size = 0;

    printf("[conn] pool destroyed\n");
}
