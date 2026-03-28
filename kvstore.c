#include "kvstore.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <assert.h>
#include <malloc.h>

//成功
#define RESP_OK         "+OK\r\n"
//不存在
#define RESP_NIL        "$-1\r\n"
//查询是否存在
#define RESP_EXIST      ":1\r\n"
#define RESP_NOEXIST    ":0\r\n"
//错误
#define RESP_ERROR      "-ERR error\r\n"

#define BUF_LEN 1024
#define MAX_TOKENS   256     // 最多命令参数
//启动AOF
#define ENABLE_AOF 1


#if ENABLE_ARRAY
extern kvs_array_t global_array;
#endif

#if ENABLE_RBTREE
extern kvs_rbtree_t global_rbtree;
#endif

#if ENABLE_HASH
extern kvs_hash_t global_hash;
#endif

#if ENABLE_SKIPTABLE
extern kvs_skiptable_t global_skiptable;
#endif

void *kvs_malloc(size_t size) {
    return malloc(size);
}

void kvs_free(void *ptr) {
    return free(ptr);
}

int kvs_filter_protocol(char **tokens, int count, char *response);
void kvs_flush_all();
void rdb_save();
void aof_append(const char* resp_buf,int buf_len);
//修复token释放
void free_tokens(char** tokens,int count);


const char *command[] = {
    "SET", "GET", "DEL", "MOD", "EXIST",
    "SAVE","FLUSHALL"
};

enum {
    KVS_CMD_START = 0,
    KVS_CMD_SET = KVS_CMD_START,
    KVS_CMD_GET,
    KVS_CMD_DEL,
    KVS_CMD_MOD,
    KVS_CMD_EXIST,

    KVS_CMD_SAVE,
    KVS_CMD_FLUSHALL,

    KVS_CMD_COUNT,
};


int resp_try_parse_full_cmd(const char*buf ,int buflen){
	int idx = 0;
	if(buflen<5 || buf[idx]!= '*') return -1;

	idx++;
	int argc = 0;
	while(idx < buflen && buf[idx]!='\r'){
		argc = argc * 10 + (buf[idx] - '0');
		idx++;
	}
	if(idx + 2 > buflen ) return 0;
	idx += 2;

	for(int i=0;i < argc ;i++){
		if(idx + 1>=buflen || buf[idx] != '$') return -1;
		idx++;
		int slen = 0;
		while(idx <buflen && buf[idx] != '\r'){
			slen = slen * 10 + (buf[idx] - '0');
			idx++;
		}
		if(idx + 2 >buflen) return 0;
		idx+=2;
		if(idx + slen +2 > buflen) return 0;
		idx+=slen + 2;
	}
	return idx;
}
// 修复：resp_unpack 函数（核心段错误来源）
bool resp_unpack(const char *data, int len, char** out_commands) {
    int idx = 0;

    if (idx >= len || data[idx] != '*') {
        return false;
    }
    idx++;

    // 读取参数个数
    int argc = 0;
    while (idx < len && data[idx] != '\r') {
        argc = argc * 10 + (data[idx] - '0');
        idx++;
    }
    idx += 2;

    for (int i = 0; i < argc; i++) {
        if (idx >= len || data[idx] != '$')
            return false;
        idx++;

        int str_len = 0;
        while (idx < len && data[idx] != '\r') {
            str_len = str_len * 10 + (data[idx] - '0');
            idx++;
        }
        idx += 2;

        if (idx + str_len > len) {
            return false;
        }

        // ===================== 修复：分配内存 =====================
        out_commands[i] = (char*)kvs_malloc(str_len + 1);
        if (!out_commands[i])
            return false;

        strncpy(out_commands[i], data + idx, str_len);
        out_commands[i][str_len] = '\0';
        // ==========================================================

        idx += str_len;
        idx += 2;
    }
    return true;
}


//修复:释放解包的token
void free_tokens(char **tokens,int count ){
    for(int i=0;i< count ;i++){
        if(tokens[i]){
            free(tokens[i]);
            tokens[i] = NULL;
        }
    }
}

// 修复：kvs_protocol 调用逻辑
int kvs_protocol(char *msg, int length, char *response) {
    if (msg == NULL || length <= 0 || response == NULL)
        return -1;

    char *tokens[KVS_MAX_TOKENS] = {0};

    // 修复：bool 类型接收
    bool ok = resp_unpack(msg, length, tokens);
    if (!ok)
        return -1;

    int ret = kvs_filter_protocol(tokens, KVS_MAX_TOKENS, response);

    //aof正确添加位置
    if(ENABLE_AOF && ret > 0){
        if(strcmp(tokens[0],"SET") == 0 ||
            strcmp(tokens[0],"DEL") == 0 ||
            strcmp(tokens[0],"MOD") == 0 ){

                aof_append(msg,length);
            }
    }
    free_tokens(tokens,KVS_MAX_TOKENS);
    return ret;
}



//获取当前毫秒时间戳
long long get_time_now(void){

	struct timeval tv;
	gettimeofday(&tv,NULL);
	//秒转毫秒 +微秒转毫秒
	return tv.tv_sec*1000LL + tv.tv_usec/1000;

}

//item创建函数
KVItem*create_kvitem(char*value_str,char*expire_str){
    if(!value_str) return NULL;

    KVItem*item = (KVItem*)kvs_malloc(sizeof(KVItem));
    memset(item,0,sizeof(KVItem));

    //拷贝value
    item->len = strlen(value_str);
    item->value = (char*)kvs_malloc(item->len + 1);
    memcpy(item->value,value_str,item->len);
    item->value[item->len] = 0;

    //过期时间
    item->expire = 0;
    if(expire_str){
        int  sec = atoi(expire_str);
        item->expire = get_time_now() + (long long)sec*1000;
    }
    return item;
}

//item释放函数
void free_kvitem(KVItem*item){
    if(!item) return ;
    if(item -> value) kvs_free(item->value);
    kvs_free(item);
}

// 命令处理
int kvs_filter_protocol(char **tokens, int count, char *response) {
    if (tokens[0] == NULL || count == 0 || response == NULL)
        return -1;

    int cmd = KVS_CMD_START;
    for (cmd = KVS_CMD_START; cmd < KVS_CMD_COUNT; cmd++) {
        if (strcmp(tokens[0], command[cmd]) == 0)
            break;
    }

    int length = 0;
    int ret = 0;
    char *key = tokens[1];


    KVItem *item = NULL;
    if(cmd == KVS_CMD_SET || cmd == KVS_CMD_MOD){
        item = create_kvitem(tokens[2],tokens[3]);
    }


    switch(cmd) {
        case KVS_CMD_SET:
#if ENABLE_ARRAY
            ret = kvs_array_set(&global_array, key,item);
#endif
#if ENABLE_RBTREE
            ret = kvs_rbtree_set(&global_rbtree, key, value);
#endif
#if ENABLE_HASH
            ret = kvs_hash_set(&global_hash, key, value);
#endif
#if ENABLE_SKIPTABLE
            ret = kvs_skiptable_set(&global_skiptable, key, value);
#endif
            if (ret < 0) {
                length = sprintf(response, RESP_ERROR);
            } else if (ret == 0) {
                length = sprintf(response, RESP_OK);
            } else {
                length = sprintf(response, RESP_EXIST);
            }  
            break;

        case KVS_CMD_GET: {
            KVItem *result = NULL;
#if ENABLE_ARRAY
            result = kvs_array_get(&global_array, key);
#endif
#if ENABLE_RBTREE
            result = kvs_rbtree_get(&global_rbtree, key);
#endif
#if ENABLE_HASH
            result = kvs_hash_get(&global_hash, key);
#endif
#if ENABLE_SKIPTABLE
            result = kvs_skiptable_get(&global_skiptable, key);
#endif
            if (result == NULL) {
                length = sprintf(response, RESP_NOEXIST);
            } else {
                length = sprintf(response, "$%zu\r\n%s\r\n", result->len, result->value);
                // length = sprintf(response, "%s\r\n", result->value);
            }
            break;
        }

        case KVS_CMD_DEL:
#if ENABLE_ARRAY
            ret = kvs_array_del(&global_array, key);
#endif
#if ENABLE_RBTREE
            ret = kvs_rbtree_del(&global_rbtree, key);
#endif
#if ENABLE_HASH
            ret = kvs_hash_del(&global_hash, key);
#endif
#if ENABLE_SKIPTABLE
            ret = kvs_skiptable_del(&global_skiptable, key);
#endif
            if (ret < 0) {
                length = sprintf(response, RESP_ERROR);
            } else if (ret == 0) {
                length = sprintf(response, RESP_OK);
            } else {
                length = sprintf(response, RESP_NOEXIST);
            }

            break;

        case KVS_CMD_MOD:
#if ENABLE_ARRAY
            ret = kvs_array_mod(&global_array,key,item);
#endif
#if ENABLE_RBTREE
            ret = kvs_rbtree_mod(&global_rbtree, key, value);
#endif
#if ENABLE_HASH
            ret = kvs_hash_mod(&global_hash, key, value);
#endif
#if ENABLE_SKIPTABLE
            ret = kvs_skiptable_mod(&global_skiptable, key, value);
#endif
            if (ret < 0) {
                length = sprintf(response, RESP_ERROR);
            } else if (ret == 0) {
                length = sprintf(response, RESP_OK);
            } else {
                length = sprintf(response, RESP_NOEXIST);
            }

            break;

        case KVS_CMD_EXIST:
#if ENABLE_ARRAY
            ret = kvs_array_exist(&global_array, key);
#endif
#if ENABLE_RBTREE
            ret = kvs_rbtree_exist(&global_rbtree, key);
#endif
#if ENABLE_HASH
            ret = kvs_hash_exist(&global_hash, key);
#endif
#if ENABLE_SKIPTABLE
            ret = kvs_skiptable_exist(&global_skiptable, key);
#endif
            if (ret == 0) {
                length = sprintf(response, RESP_EXIST);
            } else {
                length = sprintf(response, RESP_NOEXIST);
            }
            break;

        case KVS_CMD_SAVE:
            //rdb全量持久化
            rdb_save();
            break;

        case KVS_CMD_FLUSHALL:
            kvs_flush_all();
            break;

        default:
            // assert(0);
            length = sprintf(response, "-ERR unknown command\r\n");
            break;
    }

    //释放Item
    if(item){
        free_kvitem(item);
    }

    return length;
}

int init_kvengine(void) {
#if ENABLE_ARRAY
    memset(&global_array, 0, sizeof(kvs_array_t));
    kvs_array_create(&global_array);
#endif
#if ENABLE_RBTREE
    memset(&global_rbtree, 0, sizeof(kvs_rbtree_t));
    kvs_rbtree_create(&global_rbtree);
#endif
#if ENABLE_HASH
    memset(&global_hash, 0, sizeof(kvs_hash_t));
    kvs_hash_create(&global_hash);
#endif
#if ENABLE_SKIPTABLE
    memset(&global_skiptable, 0, sizeof(kvs_skiptable_t));
    kvs_skiptable_create(&global_skiptable);
#endif
    return 0;
}

void dest_kvengine(void) {
#if ENABLE_ARRAY
    kvs_array_destory(&global_array);
#endif
#if ENABLE_RBTREE
    kvs_rbtree_destory(&global_rbtree);
#endif
#if ENABLE_HASH
    kvs_hash_destory(&global_hash);
#endif
#if ENABLE_SKIPTABLE
    kvs_skiptable_destroy(&global_skiptable);
#endif
}

// 修复：宏条件全部正确
void kvs_flush_all() {
#if ENABLE_ARRAY
    kvs_array_destory(&global_array);
#endif
#if ENABLE_HASH
    kvs_hash_destory(&global_hash);
#endif
#if ENABLE_RBTREE
    kvs_rbtree_destory(&global_rbtree);
#endif
#if ENABLE_SKIPTABLE
    kvs_skiptable_destroy(&global_skiptable);
#endif

    mallctl("arenas.purge", NULL, NULL, NULL, 0);
}

//RDB全量持久化实现
void rdb_save(){
    
    

}
//rdb加载
void rdb_load(){


}

//AOF增量持久化实现
/*
SET → 无论之前是什么，都覆盖 expire
MOD → 不带时间 → 保留旧 expire
MOD → 带时间 → 覆盖 expire
DEL → 直接删除
*/
void aof_append(const char* resp_buf,int buf_len){
    FILE *fp = fopen("appendonly.aof","ab");
    if (!fp) {
        printf("AOF: 打开文件失败\n"); 
        return;
    }

    //写入原始数据
    fwrite(resp_buf,1,buf_len,fp);

    //刷入磁盘
    fflush(fp);
    fsync(fileno(fp));

    fclose(fp);
}


void aof_reload(){

    FILE*fp = fopen("appendonly.aof","rb");
    if(!fp) return ;

    //获取文件大小
    fseek(fp,0,SEEK_END);
    long file_size = ftell(fp);
    fseek(fp,0,SEEK_SET);

    char* buf = kvs_malloc(file_size + 1);
    fread(buf,1,file_size,fp);
    buf[file_size] = 0;
    fclose(fp);

   
    //逐行解析RESP命令
    char*p = buf ;
    while(p < buf + file_size){
        //找到当前命令的结尾(\r\n分隔多条命令)
        char *end = strstr(p,"\r\n*");
        if(!end) end = buf + file_size;

        int cmd_len = end -p;
        if(cmd_len <= 0) break;

        //单挑命令交给kvs_protocol执行
        char response[1024];
        kvs_protocol(p,cmd_len,response);

        p = end;
    }
}


void engine_reload(void) {
    // 1. AOF 优先
    if (ENABLE_AOF && access("appendonly.aof", F_OK) == 0) {
        printf("Loading AOF...\n");
        aof_reload();
    } 
    // 2. 没有 AOF 才加载 RDB
    else if (access("dump.rdb", F_OK) == 0) {
        printf("Loading RDB...\n");
        rdb_load();
    }
}



int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("====================== KV Store Start ======================\n");
        printf("			./kvstore 端口号		        		\n");
        printf("==========================================================\n");
        return -1;
    }
    int port = atoi(argv[1]);
    init_kvengine();
    engine_reload();
#if (NETWORK_SELECT == NETWORK_REACTOR)
    //reactor_start(port, kvs_protocol);
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
    proactor_start(port, kvs_protocol);
#elif (NETWORK_SELECT == NETWORK_NTYCO)
    ntyco_start(port, kvs_protocol);
#endif

    dest_kvengine();
    return 0;
}