#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include<unistd.h>


#define MAX_MSG_LENGTH		1024
#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)
#define BUF_LEN (1024*1024)
#define MAX_TOKENS   256     // 最多命令参数
#define PIPELINE_BUF_SIZE (1024*1024)	//批量处理缓冲区


char g_resp_buf[BUF_LEN];

typedef enum{
	MODE_INTERACTIVE,
	MODE_PIPELINE,
}ClientMode;

ClientMode current_mode = MODE_INTERACTIVE;
char*pipeline_buf = NULL;	//批量命令缓冲区
int pipeline_len = 0;

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
//初始化批量缓冲区
void pipeline_init(void){
	if(!pipeline_buf){
		pipeline_buf = malloc(PIPELINE_BUF_SIZE);
		pipeline_len = 0;
	}
}
//向缓冲区添加命令
void pipeline_append(int connfd,char *cmd){
	pipeline_init();
	char*resp = resp_pack_request(cmd);
	int resp_len = strlen(resp);

	//缓冲区批量扩容
	if(pipeline_len + resp_len >= PIPELINE_BUF_SIZE){
		PIPELINE_BUF_SIZE*2;
		pipeline_buf = realloc(pipeline_buf,PIPELINE_BUF_SIZE);
	}

	memcpy(pipeline_buf+pipeline_len,resp,resp_len);
	pipeline_len +=resp_len;
	
}
//批量发送所有命令
void pipeline_exec(int connfd){
	if(pipeline_len == 0){
		printf("(empty pipeline)\n");
		return ;
	}
	//批量发送
	send_msg(connfd,pipeline_buf,pipeline_len);

	//批量接受所有响应（流式读取）
	char recv_buf[BUF_LEN] = {0};
	int total_recv =0;
	while(total_recv<pipeline_len){
		int n= recv_msg(connfd,recv_buf + total_recv,BUF_LEN-total_recv);
		if(n <= 0) break;
		total_recv += n;
		printf("%s",recv_buf + total_recv - n);
	}

	//重置批量缓冲区
	pipeline_len = 0;
	current_mode = MODE_INTERACTIVE;
	printf("\n(pipeline executed)\n");
}

//新增：-x长文本（交互模式）
void exec_x_command(int connfd,char*key,const char*filename){

	FILE*fp = fopen(filename,"rb");
	if(!fp){perror("fopen");return ;}

	fseek(fp,0,SEEK_END);
	int data_len = ftell(fp);
	fseek(fp,0,SEEK_SET);

	char*data = malloc(data_len);
	fread(data,1,data_len,fp);
	fclose(fp);

	//组装RESP长文本SET
	char send_buf[BUF_LEN] = {0};
	int len = snprintf(send_buf,BUF_LEN,
		"*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n",
		(int)strlen(key),key,data_len);
	memcpy(send_buf+len,data,data_len);
	memcpy(send_buf+len+data_len,"\r\n",2);

	send_msg(connfd,send_buf,len + data_len +2);
	
	char res[BUF_LEN] = {0};
	recv_msg(connfd,res,BUF_LEN);
	printf("%s",res);

	free(data);
}

void testcase(int connfd, char *msg, char *pattern, char *casename) {

	if (!msg || !pattern || !casename) return ;

	send_msg(connfd, msg, strlen(msg));

	char result[MAX_MSG_LENGTH] = {0};
	recv_msg(connfd, result, MAX_MSG_LENGTH);


 
	if (strcmp(result, pattern) == 0) {
		//printf("==> PASS ->  %s\n", casename);
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
	
	memset(g_resp_buf,0,BUF_LEN);

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
    total += snprintf(g_resp_buf + total, BUF_LEN - total, "*%d\r\n", idx);
    for (int i = 0; i < idx; i++) {
        int len = strlen(tokens[i]);
        total += snprintf(g_resp_buf + total, BUF_LEN - total, "$%d\r\n%s\r\n", len, tokens[i]);
        free(tokens[i]);
    }

	return g_resp_buf;

}
void common_testcase_04(int connfd){
	int i;
	for (i = 0; i < 10000; i++) {
		// 100% 不重复的KEY和VALUE，绝对无重复
		char key[32];
		char val[32];
		char cmd[128];
		
		// 安全格式化，无溢出，生成唯一标识
		snprintf(key, sizeof(key), "key_%d", i);
		snprintf(val, sizeof(val), "val_%d", i);
		snprintf(cmd, sizeof(cmd), "SET %s %s", key, val);
		
		// 执行唯一SET，一万次全不重复
		testcase(connfd, resp_pack_request(cmd), "+OK\r\n", cmd);
	}
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
		testcase(connfd, resp_pack_request("GET Teacher"), "$-1\r\n", "GET-K-Teacher");
		testcase(connfd, resp_pack_request("MOD Teacher KING"), "$-1\r\n", "MOD-K-Teacher");
		testcase(connfd, resp_pack_request("EXIST Teacher"), "$-1\r\n", "EXIST-Teacher");

	}

	struct timeval tv_end;
	gettimeofday(&tv_end, NULL);

	int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

	printf("skiptable testcase --> time_used: %d, qps: %d\n", time_used, 90000 * 1000 / time_used);
}


void common_testcase_01(int connfd){
		

	int i;
	for (i = 0; i < 10000; i++) {


		testcase(connfd, resp_pack_request("SET Teacher King"), "+OK\r\n", "SET-Teacher");
		testcase(connfd, resp_pack_request("GET Teacher"), "$4\r\nKing\r\n", "GET-King-Teacher");
		testcase(connfd, resp_pack_request("MOD Teacher Darren"), "+OK\r\n", "MOD-D-Teacher");
		testcase(connfd, resp_pack_request("GET Teacher"),"$6\r\nDarren\r\n", "GET-Darren-Teacher");
		testcase(connfd, resp_pack_request("EXIST Teacher"), ":1\r\n", "EXIST-Teacher");
		testcase(connfd, resp_pack_request("DEL Teacher"), "+OK\r\n", "DEL-Teacher");
		testcase(connfd, resp_pack_request("GET Teacher"), "$-1\r\n", "GET-K-Teacher");
		testcase(connfd, resp_pack_request("MOD Teacher KING"), "$-1\r\n", "MOD-K-Teacher");
		testcase(connfd, resp_pack_request("EXIST Teacher"), "$-1\r\n", "EXIST-Teacher");




	}
}


void spe_test_free(int connfd){
	char input[BUF_LEN];
	char response[BUF_LEN];

    printf("===================================\n");
    printf("    KV Store CLI (Enhanced)\n");
    printf("  支持: PIPELINE / EXEC / -x <file>\n");
    printf("===================================\n");



	while(1){
		//提示符显示当前模式
		if(current_mode == MODE_PIPELINE){
			printf("kv-cli(pipeline)->");
		}else{
			printf("kv-cli->");
		}
		fflush(stdout);

		//支持超长文本输入（getline）动态分配
		char *line = NULL;
		size_t line_len = 0;
		ssize_t n = getline(&line,&line_len,stdin);
		if(n<=0) {free(line);break;}

		//去掉换行符
		if(line[n-1]=='\n') line[n-1] = '\0';
		if(strlen(line) == 0){free(line);continue;}

		//退出命令
		if(strcmp(line,"quit") == 0 || strcmp(line,"exit")==0){
			free(line);
			printf("bye~\n");
			break;
		}

		//模式切换命令
		if(strcmp(line,"PIPELINE")==0){
			current_mode = MODE_PIPELINE;
			pipeline_init();
			printf("Entering pipeline mode. Use EXEC to send.\n");
			free(line);
			continue;
		}
		if(strcmp(line,"EXEC") == 0){
			pipeline_exec(connfd);
			free(line);
			continue;
		}

		//支持-x长文本
		//格式：SET key < file.txt
		if(strstr(line,"<")!= NULL){
			char cmd[64], key[256], filename[256];
			if(sscanf(line,"%s %s < %s",cmd,key,filename)== 3&&
			strcasecmp(cmd,"SET")==0){
				exec_x_command(connfd,key,filename);
				free(line);
				continue;
			}
		}
		//普通命令处理
		if(current_mode == MODE_PIPELINE){
			//批量处理：缓存模式
			pipeline_append(connfd,line);
		}else {
			//普通模式：逐条发送
			char*resp_buf = resp_pack_request(line);
			send_msg(connfd,resp_buf,strlen(resp_buf));

			memset(response,0,sizeof(response));
			recv_msg(connfd,response,sizeof(response)-1);
			printf("%s",response);

		}
		free(line);
	}
	if(pipeline_buf) free(pipeline_buf);
}


// MODE 5：Pipeline批处理压力测试（无逐行打印，仅失败+QPS）
void common_testcase_05(int connfd) {
    struct timeval tv_begin, tv_end;
    int fail_count = 0;
    const int TEST_COUNT = 1;

    // 进入流水线模式
    current_mode = MODE_PIPELINE;
    pipeline_init();

    // 生成10000条不重复SET命令
    for (int i = 0; i < TEST_COUNT; i++) {
        char key[32], val[32], cmd[128];
        snprintf(key, sizeof(key), "batch_key_%05d", i);
        snprintf(val, sizeof(val), "batch_val_%05d", i);
        snprintf(cmd, sizeof(cmd), "SET %s %s", key, val);
        pipeline_append(connfd, cmd);
    }

    // 批量发送+接收（无打印）
    gettimeofday(&tv_begin, NULL);
    send_msg(connfd, pipeline_buf, pipeline_len);

    // 静默接收所有响应（不打印任何内容）
    char recv_buf[BUF_LEN] = {0};
    int total_recv = 0;
    while (total_recv < pipeline_len) {
        int n = recv_msg(connfd, recv_buf + total_recv, BUF_LEN - total_recv);
        if (n <= 0) {
            fail_count++;
            break;
        }
        total_recv += n;
    }
    pipeline_len = 0;
    current_mode = MODE_INTERACTIVE;

    // 仅打印失败数 + QPS
    gettimeofday(&tv_end, NULL);
    int time_used = TIME_SUB_MS(tv_end, tv_begin);
    if (fail_count > 0) {
        printf("[BATCH TEST FAILED] Fail: %d | Total: %d\n", fail_count, TEST_COUNT);
    }
    printf("[BATCH TEST QPS] Time: %d ms | QPS: %d\n", time_used, TEST_COUNT * 1000 / time_used);
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
	}else if(mode==4){
		common_testcase_04(connfd);
	}else if(mode==5){
	common_testcase_05(connfd);
	}
    return 0;
}