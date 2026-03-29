


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
#define MAX_RESPONSE 		1024

// typedef struct {
// 	char buf[BUFFER_LENGTH];
// 	int len;
// }UringClient;

// 给每个连接增加：读缓冲区 + 写缓冲区（解决异步覆盖问题）
typedef struct {
    char rbuf[BUFFER_LENGTH];  // 读缓冲区（流式解析）
    int  rlen;                 // 读长度

    char wbuf[MAX_RESPONSE];   // 写缓冲区（每条命令独立）
    int  wlen;                // 写长度
    int  sending;             // 是否正在发送
} ClientData;


ClientData client_data[1024] = {0};  // 固定大小，对应 fd


extern int kvs_protocol(char *msg, int length, char *response);
int resp_try_parse_full_cmd(const char *buf, int buflen);



struct conn_info {
	int fd;
	int event;
};

// UringClient uring_clients[1024] = {0};





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

    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);
    set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0);

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
            int fd = result.fd;

            if (result.event == EVENT_ACCEPT) {
                // 重新监听 accept
                set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0);
                int connfd = entries->res;

                // 初始化连接数据
                memset(&client_data[connfd], 0, sizeof(ClientData));

                // 开始接收
                set_event_recv(&ring, connfd,
                               client_data[connfd].rbuf,
                               BUFFER_LENGTH, 0);
            }
            else if (result.event == EVENT_READ) {
                int ret = entries->res;
                ClientData *c = &client_data[fd];

                if (ret <= 0) {
                    close(fd);
                    continue;
                }

                // 把新收到的数据加入读缓冲区
                if (c->rlen + ret > BUFFER_LENGTH) {
                    ret = BUFFER_LENGTH - c->rlen; // 防止越界
                }
                c->rlen += ret;

                // ====================== 流式解析核心 ======================
                while (1) {
                    int cmd_len = resp_try_parse_full_cmd(c->rbuf, c->rlen);
                    if (cmd_len <= 0) break; // 数据不完整，退出

                    // 处理命令 → 写入独立写缓冲区
                    c->wlen = kvs_handler(c->rbuf, cmd_len, c->wbuf);

                    // 发送回复
                    set_event_send(&ring, fd, c->wbuf, c->wlen, 0);
                    c->sending = 1;

                    // 移除已处理命令（关键！）
                    memmove(c->rbuf, c->rbuf + cmd_len, c->rlen - cmd_len);
                    c->rlen -= cmd_len;
                }

                // 继续接收
                set_event_recv(&ring, fd,
                               c->rbuf + c->rlen,
                               BUFFER_LENGTH - c->rlen, 0);
            }
            else if (result.event == EVENT_WRITE) {
                ClientData *c = &client_data[fd];
                c->sending = 0;

                // 发送完成 → 继续接收
                set_event_recv(&ring, fd,
                               c->rbuf + c->rlen,
                               BUFFER_LENGTH - c->rlen, 0);
            }
        }

        io_uring_cq_advance(&ring, nready);
    }
}


