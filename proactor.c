


#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>


#define EVENT_ACCEPT   	0
#define EVENT_READ		1
#define EVENT_WRITE		2

#define ENTRIES_LENGTH		1024
#define BUFFER_LENGTH		1024

typedef struct {
	char buf[BUFFER_LENGTH];
	int len;
}UringClient;

extern int kvs_protocol(char *msg, int length, char *response);



struct conn_info {
	int fd;
	int event;
};

UringClient uring_clients[1024] = {0};





int p_init_server(unsigned short port) {	

	int sockfd = socket(AF_INET, SOCK_STREAM, 0);	
	struct sockaddr_in serveraddr;	
	memset(&serveraddr, 0, sizeof(struct sockaddr_in));	
	serveraddr.sin_family = AF_INET;	
	serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);	
	serveraddr.sin_port = htons(port);	

	if (-1 == bind(sockfd, (struct sockaddr*)&serveraddr, sizeof(struct sockaddr))) {		
		perror("bind");		
		return -1;	
	}	

	listen(sockfd, 10);
	
	return sockfd;
}





int set_event_recv(struct io_uring *ring, int sockfd,
				      void *buf, size_t len, int flags) {

	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);

	struct conn_info accept_info = {
		.fd = sockfd,
		.event = EVENT_READ,
	};
	
	io_uring_prep_recv(sqe, sockfd, buf, len, flags);
	memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));

}


int set_event_send(struct io_uring *ring, int sockfd,
				      void *buf, size_t len, int flags) {

	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);

	struct conn_info accept_info = {
		.fd = sockfd,
		.event = EVENT_WRITE,
	};
	
	io_uring_prep_send(sqe, sockfd, buf, len, flags);
	memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));

}



int set_event_accept(struct io_uring *ring, int sockfd, struct sockaddr *addr,
					socklen_t *addrlen, int flags) {

	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);

	struct conn_info accept_info = {
		.fd = sockfd,
		.event = EVENT_ACCEPT,
	};
	
	io_uring_prep_accept(sqe, sockfd, (struct sockaddr*)addr, addrlen, flags);
	memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));

}


typedef int (*msg_handler)(char *msg, int length, char *response);
static msg_handler kvs_handler;




int proactor_start(unsigned short port, msg_handler handler) {

	int sockfd = p_init_server(port);
	kvs_handler = handler;

	struct io_uring_params params;
	memset(&params, 0, sizeof(params));

	struct io_uring ring;
	io_uring_queue_init_params(ENTRIES_LENGTH, &ring, &params);

	
#if 0
	struct sockaddr_in clientaddr;	
	socklen_t len = sizeof(clientaddr);
	accept(sockfd, (struct sockaddr*)&clientaddr, &len);
#else

	struct sockaddr_in clientaddr;	
	socklen_t len = sizeof(clientaddr);
	set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0);
	
#endif

	char buffer[BUFFER_LENGTH] = {0};
	char response[BUFFER_LENGTH] = {0};


	while (1) {
		io_uring_submit(&ring);
		struct io_uring_cqe *cqe;
		io_uring_wait_cqe(&ring, &cqe);
		struct io_uring_cqe *cqes[128];
		int nready = io_uring_peek_batch_cqe(&ring, cqes, 128);

		int i = 0;
		for (i = 0; i < nready; i++) {
			struct io_uring_cqe *entries = cqes[i];
			struct conn_info result;
			memcpy(&result, &entries->user_data, sizeof(struct conn_info));

			if (result.event == EVENT_ACCEPT) {
				set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0);
				int connfd = entries->res;
                // === 初始化缓冲区 ===
                uring_clients[connfd].len = 0;
				set_event_recv(&ring, connfd, uring_clients[connfd].buf + uring_clients[connfd].len,
                              BUFFER_LENGTH - uring_clients[connfd].len, 0);
			} else if (result.event == EVENT_READ) {
				int ret = entries->res;
				if (ret <= 0) {
					close(result.fd);
					continue;
				}
                UringClient *cli = &uring_clients[result.fd];
                cli->len += ret;

                // === 流式循环解析 ===
                while (1) {
                    int cmd_len = resp_try_parse_full_cmd(cli->buf, cli->len);
                    if (cmd_len <= 0) break;

                    int slen = kvs_handler(cli->buf, cmd_len, response);
                    set_event_send(&ring, result.fd, response, slen, 0);

                    memmove(cli->buf, cli->buf + cmd_len, cli->len - cmd_len);
                    cli->len -= cmd_len;
                }
                // 继续读
                set_event_recv(&ring, result.fd, cli->buf + cli->len,
                              BUFFER_LENGTH - cli->len, 0);
			} else if (result.event == EVENT_WRITE) {
				set_event_recv(&ring, result.fd, uring_clients[result.fd].buf + uring_clients[result.fd].len,
                              BUFFER_LENGTH - uring_clients[result.fd].len, 0);
			}
		}
		io_uring_cq_advance(&ring, nready);
	}
}


