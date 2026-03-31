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


#include "server.h"
#include"kvstore.h"

#define CONNECTION_SIZE			1024 // 1024 * 1024
#define MAX_PORTS				20


#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)



#if ENABLE_KVSTORE

int kvs_request(struct conn*c);
int kvs_response(struct conn *c);

int kvs_request(struct conn *c) {

	//流式解析：循环解析
	while(1){
		//解析完整RESP命令
		int cmd_len = resp_try_parse_full_cmd(c->read_buf.buf,c ->read_buf.len);
		if(cmd_len <= 0) break;

		//调用动态缓冲区版本kvs_protocol
		kvs_protocol(c->read_buf.buf,cmd_len,&c->write_buf);

		//移除已解析数据（动态缓冲区）
		memmove(c->read_buf.buf,c->read_buf.buf + cmd_len,c->read_buf.len - cmd_len);
		c->read_buf.len -= cmd_len;
	}
	return 0;

}

int kvs_response(struct conn *c) {

	
	return 0;
}


#endif



int accept_cb(int fd);
int recv_cb(int fd);
int send_cb(int fd);



int epfd = 0;
struct timeval begin;



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

	dynbuf_init(&conn_list[fd].read_buf,BUF_INIT_CAP);
	dynbuf_init(&conn_list[fd].write_buf,BUF_INIT_CAP);

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
	//计算动态缓冲区剩余空间
	int remain = c->read_buf.cap - c->read_buf.len;
	if(remain <= 0){
		//自动扩容
		char *new_buf = realloc(c->read_buf.buf,c->read_buf.cap*2);
		if(!new_buf) return -1;
		c->read_buf.buf = new_buf;
		c->read_buf.cap *= 2;
		remain = c->read_buf.cap - c->read_buf.len; 
	}

	//读取数据到动态缓冲区
	int count = recv(fd,c->read_buf.buf + c->read_buf.len,remain,0);
	if(count <= 0){
		//关闭时释放动态缓冲区内存
		dynbuf_free(&c->read_buf);
		dynbuf_free(&c->write_buf);
		close(fd);
		epoll_ctl(epfd,EPOLL_CTL_DEL,fd,NULL);
		return 0;
	}
	c->read_buf.len += count;

#if ENABLE_KVSTORE
	kvs_request(c);
#endif
	set_event(fd,EPOLLOUT,0);
	return count;
}


int send_cb(int fd) {
    struct conn *c = &conn_list[fd];
    int count = 0;

    if (c->write_buf.len > 0) {
        count = send(fd, c->write_buf.buf, c->write_buf.len, 0);
		//printf("[SEND] fd=%d, len=%d, buf: %.*s\n", fd, c->write_buf.len, c->write_buf.len, c->write_buf.buf);
        if (count > 0) {
            // 只移除已发送的数据，不全部清空
            memmove(c->write_buf.buf, c->write_buf.buf + count, c->write_buf.len - count);
            c->write_buf.len -= count;
        }
    }

    // 数据发完再切回读事件
    if (c->write_buf.len == 0)
        set_event(fd, EPOLLIN, 0);

    return count;
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




