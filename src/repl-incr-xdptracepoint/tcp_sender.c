#include "tcp_sender.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

int tcp_connect_slave(const char *ip, uint16_t port)
{
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

    printf("[SENDER] connected to %s:%u\n", ip, port);
    return fd;
}

int tcp_send_all(int fd, const void *data, size_t len)
{
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, (const char *)data + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0)
            return -1;
        sent += (size_t)n;
    }
    return 0;
}

int tcp_drain(int fd)
{
    char buf[4096];
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    ssize_t n;
    int drained = 0;
    while ((n = recv(fd, buf, sizeof(buf), 0)) > 0)
        drained += (int)n;

    fcntl(fd, F_SETFL, flags);  /* restore blocking */
    return (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) ? -1 : 0;
}
