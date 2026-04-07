



#include "nty_coroutine.h"

#include <arpa/inet.h>
#include "server.h"
#include "kvstore.h"


msg_handler kvs_handler = NULL;



// 协程客户端结构体（和 Reactor 完全一致）
typedef struct {
    int fd;
    DynBuf read_buf;   // 动态读缓冲区
    DynBuf write_buf;  // 动态写缓冲区
	int r_off;         // 读偏移（避免 memmove）
	int w_off;         // 写偏移（避免 memmove）
} NtyClient;


//协程读包 + 协议解析 + 处理
void server_reader(void*arg){
	int fd = *(int *)arg;
	NtyClient client = {0};
	client.fd = fd;

	//初始化动态缓冲区
	dynbuf_init(&client.read_buf, KVS_NET_BUF_INIT);
	dynbuf_init(&client.write_buf, KVS_NET_BUF_INIT);
	client.r_off = 0;
	client.w_off = 0;

	while(1){
		/* 直接读入 read_buf 尾部：buf + r_off + len */
		int tail = client.r_off + client.read_buf.len;
		int remain = client.read_buf.cap - tail;
		if (remain <= 0) {
			char *nb = realloc(client.read_buf.buf, client.read_buf.cap * 2);
			if (!nb) {
				dynbuf_free(&client.read_buf);
				dynbuf_free(&client.write_buf);
				close(fd);
				break;
			}
			client.read_buf.buf = nb;
			client.read_buf.cap *= 2;
			tail = client.r_off + client.read_buf.len;
			remain = client.read_buf.cap - tail;
		}
		int ret = recv(fd, client.read_buf.buf + tail, remain, 0);

		if(ret <= 0){
			//客户端断开，释放内存
			dynbuf_free(&client.read_buf);
			dynbuf_free(&client.write_buf);
			close(fd);
			break;
		}
		client.read_buf.len += ret;

		dynbuf_clear(&client.write_buf);
		client.w_off = 0;
		kvs_drain_resp_commands_off(&client.read_buf, &client.r_off, &client.write_buf);

		//发送响应数据
		while (client.write_buf.len > 0) {
			ssize_t count = send(fd, client.write_buf.buf + client.w_off, client.write_buf.len, MSG_NOSIGNAL);
			if (count <= 0) break;
			client.w_off += (int)count;
			client.write_buf.len -= (int)count;
		}
		if (client.write_buf.len == 0) client.w_off = 0;
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




