



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
} NtyClient;


//协程读包 + 协议解析 + 处理
void server_reader(void*arg){
	int fd = *(int *)arg;
	NtyClient client = {0};
	client.fd = fd;

	//初始化动态缓冲区
	dynbuf_init(&client.read_buf,BUF_INIT_CAP);
	dynbuf_init(&client.write_buf,BUF_INIT_CAP);

	while(1){
		//临时接收缓冲区
		char tmp[2096];
		int ret = recv(fd,tmp,sizeof(tmp),0);

		if(ret <= 0){
			//客户端断开，释放内存
			dynbuf_free(&client.read_buf);
			dynbuf_free(&client.write_buf);
			close(fd);
			break;
		}

		//动态扩容追加数据（支持无限长文本）
		dynbuf_append(&client.read_buf,tmp,ret);

		//流式解析RESP协议
		while(1){
			int cmd_len = resp_try_parse_full_cmd(client.read_buf.buf,client.read_buf.len);
			if(cmd_len <= 0) break;

			//调用统一KV协议处理
			kvs_protocol(client.read_buf.buf,cmd_len,&client.write_buf);

			//移除已解析命令
			memmove(client.read_buf.buf,client.read_buf.buf + cmd_len,client.read_buf.len - cmd_len);
			client.read_buf.len -= cmd_len;
		}

		//发送响应数据
		if(client.write_buf.len > 0){
			ssize_t count = send(fd,client.write_buf.buf,client.write_buf.len,0);
			if (count > 0) {
                memmove(client.write_buf.buf,
                        client.write_buf.buf + count,
                        client.write_buf.len - count);
                client.write_buf.len -= count;
            }
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




