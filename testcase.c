#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include<unistd.h>


#define MAX_MSG_LENGTH		1024
#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)
#define BUF_LEN 1024
#define MAX_TOKENS   256     // 最多命令参数

char* resp_pack_request(char *msg);

int send_msg(int connfd, char *msg, int length) {

	int res = send(connfd, msg, length, 0);
	if (res < 0) {
		perror("send");
		exit(1);
	}
	return res;
}

int recv_msg(int connfd, char *msg, int length) {

	int res = recv(connfd, msg, length, 0);
	if (res < 0) {
		perror("recv");
		exit(1);
	}
	return res;

}

void testcase(int connfd, char *msg, char *pattern, char *casename) {

	if (!msg || !pattern || !casename) return ;

	send_msg(connfd, msg, strlen(msg));

	char result[MAX_MSG_LENGTH] = {0};
	recv_msg(connfd, result, MAX_MSG_LENGTH);


 
	if (strcmp(result, pattern) == 0) {
		printf("==> PASS ->  %s\n", casename);
	} else {
		printf("==> FAILED -> %s, '%s' != '%s' \n", casename, result, pattern);
		exit(1);
	}

}

void txt_testcase_01(int connfd,const char road){

	FILE *fp = fopen("test.txt","r");
	fseek(fp,0,SEEK_END);
	int len = ftell(fp);
	fseek(fp,0,SEEK_SET);

	char *content = malloc(len+ 1);
	fread(content,1,len,fp);
	content[len] = 0;
	fclose(fp);

	//组装RESP客户端包
	char send_buf[10240];
	sprintf(send_buf,"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$%d\r\n%s\r\n", len, content);
	
	//发送
	send_msg(connfd, content, strlen(content));

	char result[MAX_MSG_LENGTH] = {0};
	recv_msg(connfd, result, MAX_MSG_LENGTH);

}



int connect_tcpserver(const char *ip, unsigned short port) {

	int connfd = socket(AF_INET, SOCK_STREAM, 0);

	struct sockaddr_in server_addr;
	memset(&server_addr, 0, sizeof(struct sockaddr_in));

	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = inet_addr(ip);
	server_addr.sin_port = htons(port);

	if (0 !=  connect(connfd, (struct sockaddr*)&server_addr, sizeof(struct sockaddr_in))) {
		perror("connect");
		return -1;
	}
	
	return connfd;
	
}

// 封包函数
char *resp_pack_request(char *msg){
	char *out_buf = (char*)malloc(BUF_LEN);
	if(!out_buf) return NULL;
	memset(out_buf,0,BUF_LEN);

	char*tokens[MAX_TOKENS] = {0};
	int idx = 0;
	char *p = msg;

	while(*p && idx < MAX_TOKENS){
		while(*p == ' ') p++;
		if(*p == '\0') break;

		//长文本处理
		if(*p == '"'){
			p++;
			char *start = p;
			while(*p && *p !='"') p++;
			int len = p- start;
			tokens[idx] = malloc(len + 1);
			strncpy(tokens[idx],start,len);
			tokens[idx][len]='\0';
			idx++;
			p++; //跳过闭合逗号
		}else{
			//普通token
			char * start = p;
			while(*p && *p!=' ') p++;
			int len = p - start;
			tokens[idx] = malloc(len + 1);
			strncpy(tokens[idx],start,len);
			tokens[idx][len] = '\0';
			idx++;
		}
	}

    // 生成 RESP
    int total = 0;
    total += snprintf(out_buf + total, BUF_LEN - total, "*%d\r\n", idx);
    for (int i = 0; i < idx; i++) {
        int len = strlen(tokens[i]);
        total += snprintf(out_buf + total, BUF_LEN - total, "$%d\r\n%s\r\n", len, tokens[i]);
        free(tokens[i]);
    }

	return out_buf;

}

void common_testcase_03(int connfd){
		




		testcase(connfd, resp_pack_request("SET Teacher King 2"), "+OK\r\n", "SET-Teacher");
		sleep(1);
		testcase(connfd, resp_pack_request("GET Teacher"), "$4\r\nKing\r\n", "GET-King-Teacher");
		sleep(1);
		testcase(connfd, resp_pack_request("GET Teacher"), "$4\r\nKing\r\n", "GET-King-Teacher");
		// testcase(connfd, resp_pack_request("MOD Teacher Darren"), "+OK\r\n", "MOD-D-Teacher");
		// testcase(connfd, resp_pack_request("GET Teacher"),"$6\r\nDarren\r\n", "GET-Darren-Teacher");
		// testcase(connfd, resp_pack_request("EXIST Teacher"), ":1\r\n", "EXIST-Teacher");
		// testcase(connfd, resp_pack_request("DEL Teacher"), "+OK\r\n", "DEL-Teacher");
		// testcase(connfd, resp_pack_request("GET Teacher"), ":0\r\n", "GET-K-Teacher");
		// testcase(connfd, resp_pack_request("MOD Teacher KING"), ":0\r\n", "MOD-K-Teacher");
		// testcase(connfd, resp_pack_request("EXIST Teacher"), ":0\r\n", "EXIST-Teacher");





}


void common_testcase_02(int connfd){
		
	int count = 10000;
	int i = 0;

	struct timeval tv_begin;
	gettimeofday(&tv_begin, NULL);

	for (i = 0;i < count;i ++) {

		testcase(connfd, resp_pack_request("SET Teacher King 10"), "+OK\r\n", "SET-Teacher");
		testcase(connfd, resp_pack_request("GET Teacher"), "$4\r\nKing\r\n", "GET-King-Teacher");
		testcase(connfd, resp_pack_request("MOD Teacher Darren"), "+OK\r\n", "MOD-D-Teacher");
		testcase(connfd, resp_pack_request("GET Teacher"),"$6\r\nDarren\r\n", "GET-Darren-Teacher");
		testcase(connfd, resp_pack_request("EXIST Teacher"), ":1\r\n", "EXIST-Teacher");
		testcase(connfd, resp_pack_request("DEL Teacher"), "+OK\r\n", "DEL-Teacher");
		testcase(connfd, resp_pack_request("GET Teacher"), ":0\r\n", "GET-K-Teacher");
		testcase(connfd, resp_pack_request("MOD Teacher KING"), ":0\r\n", "MOD-K-Teacher");
		testcase(connfd, resp_pack_request("EXIST Teacher"), ":0\r\n", "EXIST-Teacher");

	}

	struct timeval tv_end;
	gettimeofday(&tv_end, NULL);

	int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

	printf("skiptable testcase --> time_used: %d, qps: %d\n", time_used, 90000 * 1000 / time_used);
}


void common_testcase_01(int connfd){
		




		testcase(connfd, resp_pack_request("SET Teacher King"), "+OK\r\n", "SET-Teacher");
		testcase(connfd, resp_pack_request("GET Teacher"), "$4\r\nKing\r\n", "GET-King-Teacher");
		testcase(connfd, resp_pack_request("MOD Teacher Darren"), "+OK\r\n", "MOD-D-Teacher");
		testcase(connfd, resp_pack_request("GET Teacher"),"$6\r\nDarren\r\n", "GET-Darren-Teacher");
		testcase(connfd, resp_pack_request("EXIST Teacher"), ":1\r\n", "EXIST-Teacher");
		testcase(connfd, resp_pack_request("DEL Teacher"), "+OK\r\n", "DEL-Teacher");
		testcase(connfd, resp_pack_request("GET Teacher"), ":0\r\n", "GET-K-Teacher");
		testcase(connfd, resp_pack_request("MOD Teacher KING"), ":0\r\n", "MOD-K-Teacher");
		testcase(connfd, resp_pack_request("EXIST Teacher"), ":0\r\n", "EXIST-Teacher");





}


void spe_test_free(int connfd){
	char input[BUF_LEN];
	char response[BUF_LEN];

	printf("===================================\n");
    printf("    KV Store CLI (redis-cli mode)\n");
    printf("===================================\n");
    printf("输入命令即可执行 (SET/GET/DEL/MOD/EXIST/SAVE)\n");
    printf("输入 quit 退出\n\n");


	while(1){
		//提示符
		printf("kv-cli->");
		fflush(stdout);

		//读终端输入
		if(!fgets(input,sizeof(input),stdin)){
			break;
		}

		int len = strlen(input);
		if(len > 0 && input[len - 1]=='\n')
			input[len-1] = '\n';

		//空指令跳过
		if(strlen(input) == 0) continue;

		//退出
		if(strcmp(input,"quit") == 0 || strcmp(input,"exit") == 0){
			printf("bye~\n");
			break;
		}

		//封装成RESP
		char*resp_buf = resp_pack_request(input);
		if(!resp_buf) continue;

		//发送 & 接收
		send_msg(connfd,resp_buf,strlen(resp_buf));
		memset(response,0,sizeof(response));
		recv_msg(connfd,response,sizeof(response)-1);

		//输出结果
		printf("%s",response);

		free(resp_buf);
	}
}





int main(int argc,char *argv[]){

    	if (argc != 4) {
printf("=================== KV Store Test Menu ===================\n"
       "  		./kvstore 192.168.30.128	2000 mode			  \n"
       "  MODE 0        spe_test_free(connfd);    		     	  \n"
       "  MODE 1        common_testcase_01(connfd);		     	  \n"
       "  MODE 2        common_testcase_02(connfd);		     	  \n"
       "  MODE 3        common_testcase_03(connfd);               \n"
	   "  MODE 4        skiptable_testcase(connfd);		     	  \n"
       "==========================================================\n");
		return -1;
	}


    char *ip = argv[1];
	int port = atoi(argv[2]);
	int mode = atoi(argv[3]);

	int connfd = connect_tcpserver(ip, port);

    if(mode == 0){
		spe_test_free(connfd);
	}else if(mode == 1){
		common_testcase_01(connfd);
	}else if(mode == 2){
		common_testcase_02(connfd);
	}else if(mode==3){
		common_testcase_03(connfd);
	}
    return 0;
}