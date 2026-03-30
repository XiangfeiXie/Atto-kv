#include "kvstore.h"
#include "server.h"  // ✅ 修改点1：添加server.h（DynBuf结构体）
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <assert.h>
#include <malloc.h>
#include <sys/time.h>   // ✅ 修改点2：添加gettimeofday头文件
#include <unistd.h>     // ✅ 修改点3：添加fsync/access头文件
#include <fcntl.h>      // ✅ 修改点4：添加文件操作头文件

// ==================== 动态缓冲区工具函数实现（放在这里，彻底解决链接问题） ====================
void dynbuf_init(DynBuf *db, int init_cap) {
    if (!db || init_cap <= 0) return;
    db->buf = (char*)malloc(init_cap);
    db->len = 0;
    db->cap = init_cap;
}

int dynbuf_append(DynBuf *db, const char *data, int len) {
    if (!db || !data || len <= 0) return -1;
    
    // 空间不足时自动翻倍扩容
    if (db->len + len > db->cap) {
        while (db->len + len > db->cap) {
            db->cap *= 2;
        }
        char *new_buf = (char*)realloc(db->buf, db->cap);
        if (!new_buf) return -1;
        db->buf = new_buf;
    }
    
    memcpy(db->buf + db->len, data, len);
    db->len += len;
    return 0;
}

void dynbuf_clear(DynBuf *db) {
    if (!db) return;
    db->len = 0;
}

void dynbuf_free(DynBuf *db) {
    if (!db) return;
    if (db->buf) {
        free(db->buf);
        db->buf = NULL;
    }
    db->len = 0;
    db->cap = 0;
}
// ==========================================================================================




//成功
#define RESP_OK         "+OK\r\n"
//不存在
#define RESP_NIL        "$-1\r\n"
//查询是否存在
#define RESP_EXIST      ":1\r\n"
//错误
#define RESP_ERROR      "-ERR error\r\n"

#define KVS_MAX_TOKENS   256     // ✅ 修改点5：统一宏名称（修复不匹配）
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

// ✅ 修改点6：函数签名适配DynBuf（核心！）
int kvs_filter_protocol(char **tokens, int count, DynBuf *wb);
void kvs_flush_all();
void rdb_save();
void aof_append(const char* resp_buf,int buf_len);
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

bool resp_unpack(const char *data, int len, char** out_commands) {
    int idx = 0;

    if (idx >= len || data[idx] != '*') {
        return false;
    }
    idx++;

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

        out_commands[i] = (char*)kvs_malloc(str_len + 1);
        if (!out_commands[i])
            return false;

        strncpy(out_commands[i], data + idx, str_len);
        out_commands[i][str_len] = '\0';

        idx += str_len;
        idx += 2;
    }
    return true;
}

void free_tokens(char **tokens,int count ){
    for(int i=0;i< count ;i++){
        if(tokens[i]){
            free(tokens[i]);
            tokens[i] = NULL;
        }
    }
}

// ======================================================================
// ✅ 修改点7：核心函数签名适配DynBuf（彻底解决编译/段错误）
// 原：int kvs_protocol(char *msg, int length, char *response)
// 新：int kvs_protocol(char *msg, int length, DynBuf *write_buf)
// ======================================================================
int kvs_protocol(char *msg, int length, DynBuf *write_buf) {
    if (msg == NULL || length <= 0 || write_buf == NULL)
        return -1;

    // 清空动态缓冲区
    write_buf->len = 0;

    char *tokens[KVS_MAX_TOKENS] = {0};
    bool ok = resp_unpack(msg, length, tokens);
    if (!ok)
        return -1;

    // ✅ 修改点8：传入DynBuf，不再用固定数组
    kvs_filter_protocol(tokens, KVS_MAX_TOKENS, write_buf);

    if(ENABLE_AOF){
        if(strcmp(tokens[0],"SET") == 0 ||
            strcmp(tokens[0],"DEL") == 0 ||
            strcmp(tokens[0],"MOD") == 0 ){
                aof_append(msg,length);
            }
    }

    free_tokens(tokens,KVS_MAX_TOKENS);
    return write_buf->len;
}

long long get_time_now(void){
	struct timeval tv;
	gettimeofday(&tv,NULL);
	return tv.tv_sec*1000LL + tv.tv_usec/1000;
}

KVItem* create_kvitem(char*value_str,char*expire_str){
    if(!value_str) return NULL;

    KVItem*item = (KVItem*)kvs_malloc(sizeof(KVItem));
    memset(item,0,sizeof(KVItem));

    item->len = strlen(value_str);
    item->value = (char*)kvs_malloc(item->len + 1);
    memcpy(item->value,value_str,item->len);
    item->value[item->len] = 0;

    item->expire = 0;
    if(expire_str){
        int  sec = atoi(expire_str);
        item->expire = get_time_now() + (long long)sec*1000;
    }
    return item;
}

void free_kvitem(KVItem*item){
    if(!item) return ;
    if(item -> value) kvs_free(item->value);
    kvs_free(item);
}

// ======================================================================
// ✅ 修改点9：适配DynBuf，删除固定response，支持超长文本
// 原：int kvs_filter_protocol(char **tokens, int count, char *response)
// 新：int kvs_filter_protocol(char **tokens, int count, DynBuf *wb)
// ======================================================================
int kvs_filter_protocol(char **tokens, int count, DynBuf *wb) {
    if (tokens[0] == NULL || count == 0 || wb == NULL)
        return -1;

    int cmd = KVS_CMD_START;
    for (cmd = KVS_CMD_START; cmd < KVS_CMD_COUNT; cmd++) {
        if (strcmp(tokens[0], command[cmd]) == 0)
            break;
    }

    char *key = tokens[1];
    KVItem *item = NULL;
    if(cmd == KVS_CMD_SET || cmd == KVS_CMD_MOD){
        item = create_kvitem(tokens[2],tokens[3]);
    }

    int ret = 0;
    char temp_buf[1024] = {0}; // 临时小缓冲区，拼接后写入动态buf

    switch(cmd) {
        case KVS_CMD_SET:
#if ENABLE_ARRAY
            ret = kvs_array_set(&global_array, key,item);
#endif
#if ENABLE_RBTREE
            ret = kvs_rbtree_set(&global_rbtree, key,item);
#endif
#if ENABLE_HASH
            ret = kvs_hash_set(&global_hash, key,item);
#endif
#if ENABLE_SKIPTABLE
            ret = kvs_skiptable_set(&global_skiptable, key,item);
#endif
            if (ret < 0) {
                strcpy(temp_buf, RESP_ERROR);
            } else if (ret == 0) {
                strcpy(temp_buf, RESP_OK);
            } else {
                strcpy(temp_buf, RESP_EXIST);
            }  
            dynbuf_append(wb, temp_buf, strlen(temp_buf)); // ✅ 写入动态buf
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
                dynbuf_append(wb, RESP_NIL, strlen(RESP_NIL));
            } else {
                // ✅ 修改点10：支持超长value，动态扩容
                sprintf(temp_buf, "$%zu\r\n", result->len);
                dynbuf_append(wb, temp_buf, strlen(temp_buf));
                dynbuf_append(wb, result->value, result->len);
                dynbuf_append(wb, "\r\n", 2);
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
                dynbuf_append(wb, RESP_ERROR, strlen(RESP_ERROR));
            } else if (ret == 0) {
                dynbuf_append(wb, RESP_OK, strlen(RESP_OK));
            } else {
                dynbuf_append(wb, RESP_NIL, strlen(RESP_NIL));
            }
            break;

        case KVS_CMD_MOD:
#if ENABLE_ARRAY
            ret = kvs_array_mod(&global_array,key,item);
#endif
#if ENABLE_RBTREE
            ret = kvs_rbtree_mod(&global_rbtree, key,item);
#endif
#if ENABLE_HASH
            ret = kvs_hash_mod(&global_hash, key,item);
#endif
#if ENABLE_SKIPTABLE
            ret = kvs_skiptable_mod(&global_skiptable, key,item);
#endif
            if (ret < 0) {
                dynbuf_append(wb, RESP_ERROR, strlen(RESP_ERROR));
            } else if (ret == 0) {
                dynbuf_append(wb, RESP_OK, strlen(RESP_OK));
            } else {
                dynbuf_append(wb, RESP_NIL, strlen(RESP_NIL));
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
                dynbuf_append(wb, RESP_EXIST, strlen(RESP_EXIST));
            } else {
                dynbuf_append(wb, RESP_NIL, strlen(RESP_NIL));
            }
            break;

        case KVS_CMD_SAVE:
            rdb_save();
            dynbuf_append(wb, RESP_OK, strlen(RESP_OK));
            break;

        case KVS_CMD_FLUSHALL:
            kvs_flush_all();
            dynbuf_append(wb, RESP_OK, strlen(RESP_OK));
            break;

        default:
            strcpy(temp_buf, "-ERR unknown command\r\n");
            dynbuf_append(wb, temp_buf, strlen(temp_buf));
            break;
    }

    if(item){
        free_kvitem(item);
    }

    return wb->len;
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

void rdb_save(){}
void rdb_load(){}

void aof_append(const char* resp_buf,int buf_len){
    FILE *fp = fopen("appendonly.aof","ab");
    if (!fp) {
        printf("AOF: 打开文件失败\n"); 
        return;
    }
    fwrite(resp_buf,1,buf_len,fp);
    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);
}

void aof_reload(){
    FILE*fp = fopen("appendonly.aof","rb");
    if(!fp) return ;

    fseek(fp,0,SEEK_END);
    long file_size = ftell(fp);
    fseek(fp,0,SEEK_SET);

    char* buf = kvs_malloc(file_size + 1);
    fread(buf,1,file_size,fp);
    buf[file_size] = 0;
    fclose(fp);

    char*p = buf ;
    while(p < buf + file_size){
        char *end = strstr(p,"\r\n*");
        if(!end) end = buf + file_size;

        int cmd_len = end -p;
        if(cmd_len <= 0) break;

        DynBuf wb; // ✅ 修改点11：AOF重载适配DynBuf
        dynbuf_init(&wb, 4096);
        kvs_protocol(p, cmd_len, &wb);
        dynbuf_free(&wb);

        p = end;
    }
    kvs_free(buf);
}

void engine_reload(void) {
    if (ENABLE_AOF && access("appendonly.aof", F_OK) == 0) {
        printf("Loading AOF...\n");
        aof_reload();
    } 
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
    reactor_start(port, kvs_protocol);
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
    proactor_start(port, kvs_protocol);
#elif (NETWORK_SELECT == NETWORK_NTYCO)
#endif
    dest_kvengine();
    return 0;
}