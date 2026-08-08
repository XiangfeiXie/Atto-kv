/* tcp_sender.c — Slave TCP sender with command queue + writer thread */

#define _GNU_SOURCE
#include "tcp_sender.h"
#include "config.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* ── Command queue ────────────────────────────────────────────────────── */

void cmdq_init(cmd_queue_t *q, int max_count) {
    memset(q, 0, sizeof(*q));
    q->max_count = max_count > 0 ? max_count : CMD_QUEUE_SIZE;
    pthread_mutex_init(&q->lock, NULL);
    pthread_cond_init(&q->cond, NULL);
}

int cmdq_push(cmd_queue_t *q, const uint8_t *data, uint32_t len) {
    pthread_mutex_lock(&q->lock);

    if (q->count >= q->max_count) {
        pthread_mutex_unlock(&q->lock);
        return -1;
    }

    cmd_node_t *node = malloc(sizeof(*node) + len);
    if (!node) {
        pthread_mutex_unlock(&q->lock);
        return -1;
    }

    node->next = NULL;
    node->len  = len;
    memcpy(node->data, data, len);

    if (q->tail) q->tail->next = node;
    else         q->head       = node;
    q->tail = node;
    q->count++;

    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->lock);
    return 0;
}

cmd_node_t *cmdq_pop_timeout(cmd_queue_t *q, int timeout_ms) {
    pthread_mutex_lock(&q->lock);

    while (!q->head && !q->shutdown) {
        if (timeout_ms <= 0) {
            pthread_mutex_unlock(&q->lock);
            return NULL;
        }
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec  += timeout_ms / 1000;
        ts.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }
        if (pthread_cond_timedwait(&q->cond, &q->lock, &ts) == ETIMEDOUT)
            break;
    }

    cmd_node_t *node = q->head;
    if (node) {
        q->head = node->next;
        if (!q->head) q->tail = NULL;
        q->count--;
    }

    pthread_mutex_unlock(&q->lock);
    return node;
}

void cmdq_shutdown(cmd_queue_t *q) {
    pthread_mutex_lock(&q->lock);
    q->shutdown = 1;
    pthread_cond_broadcast(&q->cond);
    pthread_mutex_unlock(&q->lock);
}

void cmdq_drain(cmd_queue_t *q) {
    pthread_mutex_lock(&q->lock);
    cmd_node_t *node = q->head;
    while (node) {
        cmd_node_t *next = node->next;
        free(node);
        node = next;
    }
    q->head  = NULL;
    q->tail  = NULL;
    q->count = 0;
    pthread_mutex_unlock(&q->lock);
}

/* ── Low-level TCP ────────────────────────────────────────────────────── */

int tcp_connect_slave(const char *ip, uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("[SENDER] socket");
        return -1;
    }

    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    struct sockaddr_in addr = {.sin_family = AF_INET, .sin_port = htons(port)};
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "[SENDER] invalid IP: %s\n", ip);
        close(fd);
        return -1;
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("[SENDER] connect");
        close(fd);
        return -1;
    }

    printf("[SENDER] connected to %s:%u\n", ip, (unsigned)port);
    return fd;
}

int tcp_send_all(int fd, const void *data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, (const char *)data + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0)
            return -1;
        sent += (size_t)n;
    }
    return 0;
}

int tcp_drain(int fd) {
    char buf[4096];
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    ssize_t n;
    while ((n = recv(fd, buf, sizeof(buf), 0)) > 0) {}

    fcntl(fd, F_SETFL, flags);
    return (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) ? -1 : 0;
}

/* ── Writer thread ────────────────────────────────────────────────────── */

static void *writer_thread_fn(void *arg) {
    tcp_writer_t *w = (tcp_writer_t *)arg;
    int batch = 0;

    while (!w->shutdown) {
        /* Ensure connected */
        if (w->fd < 0) {
            w->fd = tcp_connect_slave(w->ip, w->port);
            if (w->fd < 0) {
                fprintf(stderr, "[WRITER] connect failed, retry in %ds...\n",
                        WRITER_RECONNECT_S);
                /* Wait but check for shutdown */
                struct timespec ts = {.tv_sec = WRITER_RECONNECT_S};
                for (int i = 0; i < WRITER_RECONNECT_S * 10 && !w->shutdown; i++) {
                    struct timespec rem = {.tv_sec = 0, .tv_nsec = 100000000L};
                    nanosleep(&rem, NULL);
                }
                continue;
            }
            batch = 0;
        }

        /* Pop command (blocks until data or timeout) */
        cmd_node_t *node = cmdq_pop_timeout(w->cmdq, WRITER_POP_TIMEOUT_MS);
        if (!node) {
            /* Timeout — check if we're still connected */
            if (w->shutdown) break;
            continue;
        }

        /* Send */
        if (tcp_send_all(w->fd, node->data, node->len) < 0) {
            fprintf(stderr, "[WRITER] send failed, reconnecting...\n");
            close(w->fd);
            w->fd = -1;
            free(node);
            continue;
        }

        batch++;
        free(node);

        /* Periodic drain */
        if (batch >= w->batch_size) {
            tcp_drain(w->fd);
            batch = 0;
        }
    }

    if (w->fd >= 0) {
        tcp_drain(w->fd);
        close(w->fd);
        w->fd = -1;
    }

    printf("[WRITER] thread exit\n");
    return NULL;
}

int tcp_writer_start(tcp_writer_t *w, cmd_queue_t *q,
                     const char *ip, uint16_t port) {
    memset(w, 0, sizeof(*w));
    w->cmdq       = q;
    w->fd         = -1;
    w->batch_size = WRITER_BATCH_SIZE;
    strncpy(w->ip, ip, sizeof(w->ip) - 1);

    /* Resolve hostname if given as hostname rather than IP.
     * For simplicity, we store as-is; tcp_connect_slave does inet_pton. */
    w->port = port;

    pthread_t tid;
    if (pthread_create(&tid, NULL, writer_thread_fn, w) != 0) {
        fprintf(stderr, "[WRITER] pthread_create failed\n");
        return -1;
    }
    pthread_detach(tid);

    printf("[WRITER] thread started (batch=%d)\n", w->batch_size);
    return 0;
}

void tcp_writer_stop(tcp_writer_t *w) {
    w->shutdown = 1;
}
