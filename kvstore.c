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
    free(ptr);
}

int kvs_filter_protocol(char **tokens, int count, char *response);
void kvs_flush_all();
void rdb_save();
void aof_append(char **tokens,int argc);


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
        out_commands[i] = (char*)malloc(str_len + 1);
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

// 修复：kvs_protocol 调用逻辑
int kvs_protocol(char *msg, int length, char *response) {
    if (msg == NULL || length <= 0 || response == NULL)
        return -1;

    char *tokens[KVS_MAX_TOKENS] = {0};

    // 修复：bool 类型接收
    bool ok = resp_unpack(msg, length, tokens);
    if (!ok)
        return -1;

    return kvs_filter_protocol(tokens, KVS_MAX_TOKENS, response);
}

// 封包函数
char* resp_pack_request(char *msg) {
    // 修复：改用 malloc，避免静态数组覆盖/线程冲突
    char *out_buf = (char*)malloc(BUF_LEN);
    if (!out_buf) return NULL;
    memset(out_buf, 0, BUF_LEN);

    char tmp_msg[BUF_LEN];
    char *tokens[MAX_TOKENS] = {0};
    int idx = 0;

    if (msg == NULL) {
        free(out_buf);
        return NULL;
    }

    strncpy(tmp_msg, msg, BUF_LEN - 1);

    char *token = strtok(tmp_msg, " ");
    while (token != NULL && idx < MAX_TOKENS) {
        tokens[idx++] = token;
        token = strtok(NULL, " ");
    }

    int total = 0;
    total += snprintf(out_buf + total, BUF_LEN - total, "*%d\r\n", idx);

    for (int i = 0; i < idx; i++) {
        int len = strlen(tokens[i]);
        total += snprintf(out_buf + total, BUF_LEN - total,
                          "$%d\r\n%s\r\n", len, tokens[i]);
    }

    return out_buf; // 调用者记得 free
}


//获取当前毫秒时间戳
static long long get_ms_now(void){

	struct timeval tv;
	gettimeofday(&tv,NULL);
	//秒转毫秒 +微秒转毫秒
	return tv.tv_sec*1000LL + tv.tv_usec/1000;

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
    // KVItem* item = (KVItem*)kvs_malloc(sizeof(KVItem));
    // item -> value = tokens[2];
    // item -> len = sizeof(tokens[2]);
    // item -> expire = tokens[3];

    // 正确、安全、支持大文件、无野指针
    KVItem *item = (KVItem*)kvs_malloc(sizeof(KVItem));
    memset(item, 0, sizeof(KVItem));

    // 1. value 必须深拷贝（不能直接赋值指针）
    char *val_str = tokens[2];
    item->len = strlen(val_str);               // 真实长度
    item->value = (char*)kvs_malloc(item->len + 1);
    memcpy(item->value, val_str, item->len);
    item->value[item->len] = '\0';

    // 2. 过期时间：有就用，没有默认 0
    item->expire = 0; // 默认永不过期
    if (tokens[3] != NULL) {
        int sec = atoi(tokens[3]);
        item->expire = get_ms_now() + (long long)sec * 1000;
    }



    switch(cmd) {
        case KVS_CMD_SET:
#if ENABLE_ARRAY
             kvs_array_set(&global_array, key,item);
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
            // AOF 持久化
            aof_append(tokens, 4);
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
                length = sprintf(response, "%s\r\n", result->value);
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
            // AOF 持久化
            aof_append(tokens, 2);
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

            // AOF 持久化
            aof_append(tokens, 4);
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
            assert(0);
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
    // FILE*fp

}

//AOF增量持久化实现
/*
SET → 无论之前是什么，都覆盖 expire
MOD → 不带时间 → 保留旧 expire
MOD → 带时间 → 覆盖 expire
DEL → 直接删除
*/
void aof_append(char **tokens,int argc){
    FILE *fp = fopen("appendonly.aof","a");

    for(int i=0;i<argc; i++){
        fprintf(fp,"%s",tokens[i]);
    }
    fprintf(fp,"\n");
    fclose(fp);
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

#if (NETWORK_SELECT == NETWORK_REACTOR)
    reactor_start(port, kvs_protocol);
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
    proactor_start(port, kvs_protocol);
#elif (NETWORK_SELECT == NETWORK_NTYCO)
    ntyco_start(port, kvs_protocol);
#endif

    dest_kvengine();
    return 0;
}