#include <errno.h>
#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <stdlib.h>
#include <fcntl.h>

#include "server.h"
#include "kvstore.h"

#define CONNECTION_SIZE			1024
#define MAX_PORTS				20

#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

#if ENABLE_KVSTORE
extern int epfd;
static void conn_set_events(int fd, struct conn *c) {
	uint32_t ev = EPOLLIN;
	if (c->write_buf.len > 0){
		ev |= EPOLLOUT;
		//printf("=== conn_set_events: 设置 EPOLLOUT，write_buf.len=%d ===\n", c->write_buf.len);
	}
	struct epoll_event e;
	e.events = ev;
	e.data.fd = fd;
	epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &e);
}
#endif

int accept_cb(int fd);
int recv_cb(int fd);
int send_cb(int fd);

int epfd = 0;
struct timeval begin;
struct conn conn_list[CONNECTION_SIZE] = {0};

int set_event(int fd, int event, int flag) {
	if (flag) {
		struct epoll_event ev;
		ev.events = event;
		ev.data.fd = fd;
		epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
	} else {
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

	// 🔥 修复1：彻底释放旧缓冲区，重新初始化
	dynbuf_free(&conn_list[fd].read_buf);
	dynbuf_free(&conn_list[fd].write_buf);
	dynbuf_init(&conn_list[fd].read_buf, KVS_NET_BUF_INIT);
	dynbuf_init(&conn_list[fd].write_buf, KVS_NET_BUF_INIT);
	conn_list[fd].r_off = 0;  // 🔥 强制重置 r_off
	conn_list[fd].w_off = 0;  // 🔥 强制重置 w_off

	if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1)
		perror("fcntl O_NONBLOCK");

	set_event(fd, event, 1);
}

int accept_cb(int fd) {
	struct sockaddr_in clientaddr;
	socklen_t len = sizeof(clientaddr);

	int clientfd = accept(fd, (struct sockaddr*)&clientaddr, &len);
	if (clientfd < 0) {
		printf("accept errno: %d --> %s\n", errno, strerror(errno));
		return -1;
	}
	
	event_register(clientfd, EPOLLIN);
	return 0;
}

int recv_cb(int fd) {
	struct conn* c = &conn_list[fd];

	/* 尽量多读：一次 EPOLLIN 对应多次 recv，写入到 (r_off + len) 之后 */
	for (;;) {
		int tail = c->r_off + c->read_buf.len;
		int remain = c->read_buf.cap - tail;
		if (remain <= 0) {
			char *new_buf = realloc(c->read_buf.buf, c->read_buf.cap * 2);
			if (!new_buf) return -1;
			c->read_buf.buf = new_buf;
			c->read_buf.cap *= 2;
			remain = c->read_buf.cap - tail;
		}

		int n = recv(fd, c->read_buf.buf + tail, remain, 0);
		if (n < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				break;
			dynbuf_free(&c->read_buf);
			dynbuf_free(&c->write_buf);
			close(fd);
			epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
			return -1;
		}
		if (n == 0) {
			dynbuf_free(&c->read_buf);
			dynbuf_free(&c->write_buf);
			close(fd);
			epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
			return 0;
		}
		c->read_buf.len += n;
	}

#if ENABLE_KVSTORE
	// 🔥 确认：这里没有 dynbuf_clear(&c->write_buf)！
	c->w_off = 0;
	kvs_drain_resp_commands_off(&c->read_buf, &c->r_off, &c->write_buf);
	conn_set_events(fd, c);
#endif
	return 0;
}

int send_cb(int fd) {
    struct conn *c = &conn_list[fd];
	DynBuf *wb = &c->write_buf;

	while (wb->len > 0) {
		ssize_t n = send(fd, wb->buf + c->w_off, wb->len, MSG_NOSIGNAL);
		if (n < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				break;
			dynbuf_free(&c->read_buf);
			dynbuf_free(&c->write_buf);
			close(fd);
			epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
			return -1;
		}
		if (n == 0) break;
		c->w_off += (int)n;
		wb->len -= (int)n;
	}

	if (wb->len == 0) {
		c->w_off = 0;
		// 🔥 确认：发完所有数据后，清空 write_buf
		dynbuf_clear(wb);
	}

#if ENABLE_KVSTORE
	conn_set_events(fd, c);
#endif
	return 0;
}

int r_init_server(unsigned short port) {
	int sockfd = socket(AF_INET, SOCK_STREAM, 0);

	// 🔥 修复2：设置端口复用
	int opt = 1;
	setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
	// setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

	struct sockaddr_in servaddr;
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
	servaddr.sin_port = htons(port);

	if (-1 == bind(sockfd, (struct sockaddr*)&servaddr, sizeof(struct sockaddr))) {
		printf("bind failed: %s\n", strerror(errno));
		return -1;
	}

	listen(sockfd, 1024); // 🔥 修复3：放大 listen backlog
	return sockfd;
}

int reactor_start(unsigned short port, msg_handler handler) {
	(void)handler;

	epfd = epoll_create(1024); // 🔥 修复4：放大 epoll 初始大小

	int i = 0;
	for (i = 0; i < MAX_PORTS; i++) {
		int sockfd = r_init_server(port + i);
		conn_list[sockfd].fd = sockfd;
		conn_list[sockfd].r_action.recv_callback = accept_cb;
		set_event(sockfd, EPOLLIN, 1);
	}

	gettimeofday(&begin, NULL);

	while (1) {
		struct epoll_event events[1024] = {0};
		int nready = epoll_wait(epfd, events, 1024, -1);

		int i = 0;
		for (i = 0; i < nready; i++) {
			int connfd = events[i].data.fd;

			// 🔥 修复5：处理错误事件，避免连接卡死
			if (events[i].events & (EPOLLERR | EPOLLHUP)) {
				// printf("=== 连接 %d 异常关闭 ===\n", connfd);
				dynbuf_free(&conn_list[connfd].read_buf);
				dynbuf_free(&conn_list[connfd].write_buf);
				close(connfd);
				epoll_ctl(epfd, EPOLL_CTL_DEL, connfd, NULL);
				continue;
			}

			if (events[i].events & EPOLLIN) {
				conn_list[connfd].r_action.recv_callback(connfd);
			} 

			if (events[i].events & EPOLLOUT) {
				conn_list[connfd].send_callback(connfd);
			}
		}
	}
}