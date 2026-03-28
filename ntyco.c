



#include "nty_coroutine.h"

#include <arpa/inet.h>


#define BUFFER_SIZE 1024

typedef int (*msg_handler)(char *msg, int length, char *response);
static msg_handler kvs_handler;


typedef struct {
	char buf[BUFFER_SIZE];
	int len ;
}StreamClient;


void server_reader(void *arg) {
	int fd = *(int *)arg;
	StreamClient client = {0};

 
	while (1) {
		//流式修改，追加读取，不清空
		int remain = BUFFER_SIZE - client.len;
		if(remain <= 0) break;
		int ret = recv(fd,client.buf + client.len ,remain,0);

		if(ret <= 0){
			close(fd);
			break;
		}
		client.len += ret;

		while(1){
			int cmd_len = resp_try_parse_full_cmd(client.buf,client.len);
			if(cmd_len <= 0) break;

			char response[1024] = {0};
			int slen = kvs_handler(client.buf,cmd_len,response);
			send(fd,response,slen,0);

			//移除已解析数据
			memmove(client.buf,client.buf + cmd_len,client.len - cmd_len);
			client.len -= cmd_len;
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

}




