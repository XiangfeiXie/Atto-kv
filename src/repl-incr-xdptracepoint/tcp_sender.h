#ifndef TCP_SENDER_H
#define TCP_SENDER_H

#include <stdint.h>
#include <stddef.h>

/* Connect to slave. Returns fd or -1 on error. */
int tcp_connect_slave(const char *ip, uint16_t port);

/* Send raw bytes. Returns 0 on success, -1 on error. */
int tcp_send_all(int fd, const void *data, size_t len);

/* Non-blocking drain (discard responses). Returns 0 on success, -1 on error. */
int tcp_drain(int fd);

#endif
