/* tcp_sender.h — Slave TCP sender with command queue + writer thread
 *
 * Architecture:
 *   ringbuf poll threads → cmdq_push() → cmd_queue_t
 *       → writer thread → cmdq_pop_timeout() → tcp_send_all()
 *       → periodic tcp_drain() → slave
 *
 * The command queue decouples capture from transmission:
 *   - queue full → drop (backpressure propagates to BPF)
 *   - slave disconnect → writer auto-reconnects
 *   - pipeline batching: send N commands, drain once
 */

#ifndef TCP_SENDER_H
#define TCP_SENDER_H

#include <pthread.h>
#include <stdint.h>
#include <stddef.h>

/* ── Command queue ────────────────────────────────────────────────────── */

typedef struct cmd_node {
    struct cmd_node *next;
    uint32_t         len;
    uint8_t          data[];      /* flexible array */
} cmd_node_t;

typedef struct {
    cmd_node_t     *head;
    cmd_node_t     *tail;
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    int             count;
    int             max_count;
    int             shutdown;
} cmd_queue_t;

/* Init queue. max_count is the hard cap; push fails when reached. */
void cmdq_init(cmd_queue_t *q, int max_count);

/* Non-blocking push. Returns 0 on success, -1 if full. */
int  cmdq_push(cmd_queue_t *q, const uint8_t *data, uint32_t len);

/* Blocking pop with timeout (ms). Returns NULL on timeout or shutdown. */
cmd_node_t *cmdq_pop_timeout(cmd_queue_t *q, int timeout_ms);

/* Signal shutdown and wake blocked pop. */
void cmdq_shutdown(cmd_queue_t *q);

/* Free all remaining nodes. */
void cmdq_drain(cmd_queue_t *q);

/* ── Writer thread ────────────────────────────────────────────────────── */

typedef struct {
    cmd_queue_t *cmdq;
    int          fd;                /* current slave fd, -1 if disconnected */
    char         ip[64];
    uint16_t     port;
    int          batch_size;
    int          shutdown;
} tcp_writer_t;

/* Start writer thread. Returns 0 on success. */
int  tcp_writer_start(tcp_writer_t *w, cmd_queue_t *q,
                      const char *ip, uint16_t port);

/* Signal writer to stop. */
void tcp_writer_stop(tcp_writer_t *w);

/* ── Low-level TCP helpers (used by writer, also available standalone) ── */

int tcp_connect_slave(const char *ip, uint16_t port);
int tcp_send_all(int fd, const void *data, size_t len);
int tcp_drain(int fd);

#endif /* TCP_SENDER_H */
