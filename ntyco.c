#include "nty_coroutine.h"

#include <arpa/inet.h>
#include <errno.h>
#include <sys/socket.h>
#include "server.h"
#include "kvstore.h"


msg_handler kvs_handler = NULL;



// 协程客户端结构体（和 Reactor 完全一致）
typedef struct {
    int fd;
    DynBuf read_buf;   // 动态读缓冲区
    DynBuf write_buf;  // 动态写缓冲区
} NtyClient;


//协程读包 + 协议解析 + 处理
void server_reader(void*arg){
	int fd = *(int *)arg;
	NtyClient client = {0};
	client.fd = fd;

	//初始化动态缓冲区
	dynbuf_init(&client.read_buf, KVS_NET_BUF_INIT);
	dynbuf_init(&client.write_buf, KVS_NET_BUF_INIT);

	while (1) {
		int remain = client.read_buf.cap - client.read_buf.len;
		if (remain <= 0) {
			char *nb = realloc(client.read_buf.buf, client.read_buf.cap * 2);
			if (!nb) {
				dynbuf_free(&client.read_buf);
				dynbuf_free(&client.write_buf);
				close(fd);
				return;
			}
			client.read_buf.buf = nb;
			client.read_buf.cap *= 2;
			remain = client.read_buf.cap - client.read_buf.len;
		}

		/* 阻塞读：单次系统调用最多拉取 remain 字节，pipeline 下可一次带回多条命令 */
		int ret = recv(fd, client.read_buf.buf + client.read_buf.len, remain, 0);
		if (ret <= 0) {
			dynbuf_free(&client.read_buf);
			dynbuf_free(&client.write_buf);
			close(fd);
			return;
		}
		client.read_buf.len += ret;

		kvs_drain_resp_commands(&client.read_buf, &client.write_buf);

		while (client.write_buf.len > 0) {
			ssize_t count = send(fd, client.write_buf.buf, client.write_buf.len, MSG_NOSIGNAL);
			if (count < 0) {
				dynbuf_free(&client.read_buf);
				dynbuf_free(&client.write_buf);
				close(fd);
				return;
			}
			if (count == 0)
				break;
			memmove(client.write_buf.buf, client.write_buf.buf + count,
				client.write_buf.len - count);
			client.write_buf.len -= (int)count;
		}
	}
}



void server(void *arg) {

	unsigned short port = *(unsigned short *)arg;

	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) return ;

	struct sockaddr_in local, remote;
	local.sin_family = AF_INET;
	local.sin_port = htons(port);
	local.sin_addr.s_addr = INADDR_ANY;
	bind(fd, (struct sockaddr*)&local, sizeof(struct sockaddr_in));

	listen(fd, 20);
	printf("listen port : %d\n", port);


	while (1) {
		socklen_t len = sizeof(struct sockaddr_in);
		int cli_fd = accept(fd, (struct sockaddr*)&remote, &len);
		

		nty_coroutine *read_co;
		nty_coroutine_create(&read_co, server_reader, &cli_fd);

	}
	
}





int ntyco_start(unsigned short port, msg_handler handler) {

	//int port = atoi(argv[1]);
	kvs_handler = handler;

	
	nty_coroutine *co = NULL;
	nty_coroutine_create(&co, server, &port);

	nty_schedule_run();
	return 0;
}




