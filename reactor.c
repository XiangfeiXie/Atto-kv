#include <errno.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <stdlib.h>


#include "server.h"
#include"kvstore.h"

#define CONNECTION_SIZE			1024 // 1024 * 1024
#define MAX_PORTS				20


#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)



int accept_cb(int fd);
int recv_cb(int fd);
int send_cb(int fd);



int epfd = 0;
struct timeval begin;

#if ENABLE_KVSTORE
static void conn_set_events(int fd, struct conn *c) {
	uint32_t ev = EPOLLIN;
	if (c->write_buf.len > 0)
		ev |= EPOLLOUT;
	struct epoll_event e;
	e.events = ev;
	e.data.fd = fd;
	epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &e);
}
#endif

struct conn conn_list[CONNECTION_SIZE] = {0};
// fd


int set_event(int fd, int event, int flag) {

	if (flag) {  // non-zero add

		struct epoll_event ev;
		ev.events = event;
		ev.data.fd = fd;
		epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);

	} else {  // zero mod

		struct epoll_event ev;
		ev.events = event;
		ev.data.fd = fd;
		epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
		
	}
	

}


int event_register(int fd, int event) {

	if (fd < 0) return -1;

	conn_list[fd].fd = fd;
	conn_list[fd].r_action.recv_callback = recv_cb;
	conn_list[fd].send_callback = send_cb;

	dynbuf_init(&conn_list[fd].read_buf, KVS_NET_BUF_INIT);
	dynbuf_init(&conn_list[fd].write_buf, KVS_NET_BUF_INIT);

	if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1)
		perror("fcntl O_NONBLOCK");

	set_event(fd, event, 1);
}


// listenfd(sockfd) --> EPOLLIN --> accept_cb
int accept_cb(int fd) {

	struct sockaddr_in  clientaddr;
	socklen_t len = sizeof(clientaddr);

	int clientfd = accept(fd, (struct sockaddr*)&clientaddr, &len);
	if (clientfd < 0) {
		printf("accept errno: %d --> %s\n", errno, strerror(errno));
		return -1;
	}
	
	event_register(clientfd, EPOLLIN);  // | EPOLLET

	return 0;
}


int recv_cb(int fd) {

	struct conn*c = &conn_list[fd];

	/* 非阻塞下尽量排空内核接收队列：一次 epoll 可读可对应多次 recv，减少后续唤醒 */
	for (;;) {
		int remain = c->read_buf.cap - c->read_buf.len;
		if (remain <= 0) {
			char *new_buf = realloc(c->read_buf.buf, c->read_buf.cap * 2);
			if (!new_buf)
				return -1;
			c->read_buf.buf = new_buf;
			c->read_buf.cap *= 2;
			remain = c->read_buf.cap - c->read_buf.len;
		}

		int count = recv(fd, c->read_buf.buf + c->read_buf.len, remain, 0);
		if (count < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				break;
			dynbuf_free(&c->read_buf);
			dynbuf_free(&c->write_buf);
			close(fd);
			epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
			return -1;
		}
		if (count == 0) {
			dynbuf_free(&c->read_buf);
			dynbuf_free(&c->write_buf);
			close(fd);
			epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
			return 0;
		}
		c->read_buf.len += count;
		if (count < remain)
			break;
	}

#if ENABLE_KVSTORE
	kvs_drain_resp_commands(&c->read_buf, &c->write_buf);
	conn_set_events(fd, c);
#endif
	return 0;
}


int send_cb(int fd) {
    struct conn *c = &conn_list[fd];

    while (c->write_buf.len > 0) {
        ssize_t n = send(fd, c->write_buf.buf, c->write_buf.len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            dynbuf_free(&c->read_buf);
            dynbuf_free(&c->write_buf);
            close(fd);
            epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
            return -1;
        }
        if (n == 0)
            break;
        memmove(c->write_buf.buf, c->write_buf.buf + n, c->write_buf.len - (int)n);
        c->write_buf.len -= (int)n;
    }

#if ENABLE_KVSTORE
    conn_set_events(fd, c);
#endif
    return 0;
}



int r_init_server(unsigned short port) {

	int sockfd = socket(AF_INET, SOCK_STREAM, 0);

	struct sockaddr_in servaddr;
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = htonl(INADDR_ANY); // 0.0.0.0
	servaddr.sin_port = htons(port); // 0-1023, 

	if (-1 == bind(sockfd, (struct sockaddr*)&servaddr, sizeof(struct sockaddr))) {
		printf("bind failed: %s\n", strerror(errno));
		return -1;
	}

	listen(sockfd, 10);
	//printf("listen finshed: %d\n", sockfd); // 3 

	return sockfd;

}

int reactor_start(unsigned short port, msg_handler handler) {

	(void)handler;

	epfd = epoll_create(1);

	int i = 0;

	for (i = 0;i < MAX_PORTS;i ++) {
		
		int sockfd = r_init_server(port + i);
		
		conn_list[sockfd].fd = sockfd;
		conn_list[sockfd].r_action.recv_callback = accept_cb;
		
		set_event(sockfd, EPOLLIN, 1);
	}

	gettimeofday(&begin, NULL);

	while (1) { // mainloop

		struct epoll_event events[1024] = {0};
		int nready = epoll_wait(epfd, events, 1024, -1);

		int i = 0;
		for (i = 0;i < nready;i ++) {

			int connfd = events[i].data.fd;

#if 0
			if (events[i].events & EPOLLIN) {
				conn_list[connfd].r_action.recv_callback(connfd);
			} else if (events[i].events & EPOLLOUT) {
				conn_list[connfd].send_callback(connfd);
			}

#else 
			if (events[i].events & EPOLLIN) {
				conn_list[connfd].r_action.recv_callback(connfd);
			} 

			if (events[i].events & EPOLLOUT) {
				conn_list[connfd].send_callback(connfd);
			}
#endif
		}

	}
	

}




