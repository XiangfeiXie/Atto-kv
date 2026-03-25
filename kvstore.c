#include "kvstore.h"

//成功
#define RESP_OK         "+OK\r\n"
//不存在
#define RESP_NIL        "$-1\r\n"
//查询是否存在
#define RESP_EXIST     	":1\r\n"
#define RESP_NOEXIST    ":0\r\n"
//错误
// #define RESP_ERROR(err) ("-ERR %s\r\n", err)
#define RESP_ERROR 		"-ERR error\r\n"
//查询值



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

const char *command[] = {
	"SET", "GET", "DEL", "MOD", "EXIST",
	"RSET", "RGET", "RDEL", "RMOD", "REXIST",
	"HSET", "HGET", "HDEL", "HMOD", "HEXIST",
	"SSET", "SGET", "SDEL", "SMOD", "SEXIST",
	"SAVE","FLUSHALL"
};

enum {
	KVS_CMD_START = 0,
	// array
	KVS_CMD_SET = KVS_CMD_START,
	KVS_CMD_GET,
	KVS_CMD_DEL,
	KVS_CMD_MOD,
	KVS_CMD_EXIST,
	// rbtree
	KVS_CMD_RSET,
	KVS_CMD_RGET,
	KVS_CMD_RDEL,
	KVS_CMD_RMOD,
	KVS_CMD_REXIST,
	// hash
	KVS_CMD_HSET,
	KVS_CMD_HGET,
	KVS_CMD_HDEL,
	KVS_CMD_HMOD,
	KVS_CMD_HEXIST,
	// skiptable
	KVS_CMD_SSET,
	KVS_CMD_SGET,
	KVS_CMD_SDEL,
	KVS_CMD_SMOD,
	KVS_CMD_SEXIST,

	//控制命令
	KVS_CMD_SAVE,
	KVS_CMD_FLUSHALL,

	KVS_CMD_COUNT,
};

const char *response[] = {};


/**
 * @brief RESP协议解包函数
 * @param msg      
 * @param token   
 * @return 
 */
// int kvs_split_token(char *msg, char *tokens[]) {
// 	if (msg == NULL || tokens == NULL) return -1;

// 	int idx = 0;
// 	char *token = strtok(msg, " ");

// 	while (token != NULL) {
// 		tokens[idx ++] = token;
// 		token = strtok(NULL, " ");
// 	}
// 	return idx;
// }

bool resp_unpack(const char * data,int len ,char** out_commands){
	//*3\r\n$3\r\nset\r\n$4\r\nname\r\n$3\r\n123\r\n
	out_commands=NULL;
	int idx=0;

	if(idx >= len ||data[idx] != '*'){
		return false;
	}
	idx++;
	
	//取协议包的第一个数字
	int argc = 0;
	while(idx < len && data[idx] != '\r'){
		argc = argc * 10 + (data[idx] - '0');
		idx++;
	}

	idx+=2;

	for(int i=0;i < argc ;i++){
		if(idx >=len || data[idx] != '$')
			return false;

		idx++;

		int str_len = 0;
		while(idx < len && data[idx] != '\r'){
			str_len = str_len*10 + (data[idx] - '0');
			idx ++; 
		}
		idx+=2;

		if(idx +str_len > len){
			return false;
		}

		out_commands[i]=substr(data + idx ,strlen);

		idx += str_len;
		idx += 2;
	}
	return true;

}
int kvs_protocol(char *msg, int length, char *response) {
	if (msg == NULL || length <= 0 || response == NULL) return -1;

	char *tokens[KVS_MAX_TOKENS] = {0};
	
	//  kvs_split_token(msg, tokens);
	int count=resp_unpack(msg,length,tokens);
	if (count == -1) return -1;

	return kvs_filter_protocol(tokens, count, response);
}

//封包函数




//命令查找
int kvs_filter_protocol(char **tokens, int count, char *response) {
	if (tokens[0] == NULL || count == 0 || response == NULL) return -1;

	int cmd = KVS_CMD_START;
	for (cmd = KVS_CMD_START; cmd < KVS_CMD_COUNT; cmd ++) {
		if (strcmp(tokens[0], command[cmd]) == 0) break;
	}

	int length = 0;
	int ret = 0;
	char *key = tokens[1];
	char *value = tokens[2];

	switch(cmd) {
#if ENABLE_ARRAY
	case KVS_CMD_SET:
		ret = kvs_array_set(&global_array, key, value);
		if (ret < 0) {
			length = sprintf(response, RESP_ERROR);
		} else if (ret == 0) {
			length = sprintf(response, RESP_OK);
		} else {
			length = sprintf(response, RESP_EXIST);
		}
		break;
	case KVS_CMD_GET: {
		char *result = kvs_array_get(&global_array, key);
		if (result == NULL) {
			length = sprintf(response, RESP_NOEXIST);
		} else {
			length = sprintf(response, "%s\r\n", result);
		}
		break;
	}
	case KVS_CMD_DEL:
		ret = kvs_array_del(&global_array, key);
		if (ret < 0) {
			length = sprintf(response, RESP_ERROR);
		} else if (ret == 0) {
			length = sprintf(response, RESP_OK);
		} else {
			length = sprintf(response, RESP_NOEXIST);
		}
		break;
	case KVS_CMD_MOD:
		ret = kvs_array_mod(&global_array, key, value);
		if (ret < 0) {
			length = sprintf(response, RESP_ERROR);
		} else if (ret == 0) {
			length = sprintf(response, RESP_OK);
		} else {
			length = sprintf(response, RESP_NOEXIST);
		}
		break;
	case KVS_CMD_EXIST:
		ret = kvs_array_exist(&global_array, key);
		if (ret == 0) {
			length = sprintf(response, RESP_EXIST);
		} else {
			length = sprintf(response, RESP_NOEXIST);
		}
		break;
#endif

#if ENABLE_RBTREE
	case KVS_CMD_RSET:
		ret = kvs_rbtree_set(&global_rbtree, key, value);
		if (ret < 0) {
			length = sprintf(response, RESP_ERROR);
		} else if (ret == 0) {
			length = sprintf(response, RESP_OK);
		} else {
			length = sprintf(response, RESP_EXIST);
		}
		break;
	case KVS_CMD_RGET: {
		char *result = kvs_rbtree_get(&global_rbtree, key);
		if (result == NULL) {
			length = sprintf(response, RESP_NOEXIST);
		} else {
			length = sprintf(response, "%s\r\n", result);
		}
		break;
	}
	case KVS_CMD_RDEL:
		ret = kvs_rbtree_del(&global_rbtree, key);
		if (ret < 0) {
			length = sprintf(response, RESP_ERROR);
		} else if (ret == 0) {
			length = sprintf(response, RESP_OK);
		} else {
			length = sprintf(response, RESP_NOEXIST);
		}
		break;
	case KVS_CMD_RMOD:
		ret = kvs_rbtree_mod(&global_rbtree, key, value);
		if (ret < 0) {
			length = sprintf(response, RESP_ERROR);
		} else if (ret == 0) {
			length = sprintf(response, RESP_OK);
		} else {
			length = sprintf(response, RESP_NOEXIST);
		}
		break;
	case KVS_CMD_REXIST:
		ret = kvs_rbtree_exist(&global_rbtree, key);
		if (ret == 0) {
			length = sprintf(response, RESP_EXIST);
		} else {
			length = sprintf(response, RESP_NOEXIST);
		}
		break;
#endif

#if ENABLE_HASH
	case KVS_CMD_HSET:
		ret = kvs_hash_set(&global_hash, key, value);
		if (ret < 0) {
			length = sprintf(response, RESP_ERROR);
		} else if (ret == 0) {
			length = sprintf(response, RESP_OK);
		} else {
			length = sprintf(response, RESP_EXIST);
		}
		break;
	case KVS_CMD_HGET: {
		char *result = kvs_hash_get(&global_hash, key);
		if (result == NULL) {
			length = sprintf(response, RESP_NOEXIST);
		} else {
			length = sprintf(response, "%s\r\n", result);
		}
		break;
	}
	case KVS_CMD_HDEL:
		ret = kvs_hash_del(&global_hash, key);
		if (ret < 0) {
			length = sprintf(response, RESP_ERROR);
		} else if (ret == 0) {
			length = sprintf(response, RESP_OK);
		} else {
			length = sprintf(response, RESP_NOEXIST);
		}
		break;
	case KVS_CMD_HMOD:
		ret = kvs_hash_mod(&global_hash, key, value);
		if (ret < 0) {
			length = sprintf(response, RESP_ERROR);
		} else if (ret == 0) {
			length = sprintf(response, RESP_OK);
		} else {
			length = sprintf(response, RESP_NOEXIST);
		}
		break;
	case KVS_CMD_HEXIST:
		ret = kvs_hash_exist(&global_hash, key);
		if (ret == 0) {
			length = sprintf(response, RESP_EXIST);
		} else {
			length = sprintf(response, RESP_NOEXIST);
		}
		break;
#endif

#if ENABLE_SKIPTABLE
	case KVS_CMD_SSET:
		ret = kvs_skiptable_set(&global_skiptable, key, value);
		if (ret < 0) {
			length = sprintf(response, RESP_ERROR);
		} else if (ret == 0) {
			length = sprintf(response, RESP_OK);
		} else {
			length = sprintf(response, RESP_EXIST);
		}
		break;
	case KVS_CMD_SGET: {
		char *result = kvs_skiptable_get(&global_skiptable, key);
		if (result == NULL) {
			length = sprintf(response, RESP_NOEXIST);
		} else {
			length = sprintf(response, "%s\r\n", result);
		}
		break;
	}
	case KVS_CMD_SDEL:
		ret = kvs_skiptable_del(&global_skiptable, key);
		if (ret < 0) {
			length = sprintf(response, RESP_ERROR);
		} else if (ret == 0) {
			length = sprintf(response, RESP_OK);
		} else {
			length = sprintf(response, RESP_NOEXIST);
		}
		break;
	case KVS_CMD_SMOD:
		ret = kvs_skiptable_mod(&global_skiptable, key, value);
		if (ret < 0) {
			length = sprintf(response, RESP_ERROR);
		} else if (ret == 0) {
			length = sprintf(response, RESP_OK);
		} else {
			length = sprintf(response, RESP_NOEXIST);
		}
		break;
	case KVS_CMD_SEXIST:
		ret = kvs_skiptable_exist(&global_skiptable, key);
		if (ret == 0) {
			length = sprintf(response, RESP_EXIST);
		} else {
			length = sprintf(response, RESP_NOEXIST);
		}
		break;
#endif


	case KVS_CMD_SAVE:

		break;
	case KVS_CMD_FLUSHALL:

		//清空函数
		kvs_flushall_all();

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


void kvs_flush_all() {
    // 依次清空所有数据结构
    kvs_array_destory(&global_array);       // 清空数组 KV
    kvs_hash_destory(&global_hash);      	// 清空哈希表
    kvs_rbtree_destory(&global_rbtree);     // 清空红黑树 
    kvs_skiptable_destroy(&global_skiptable);    // 清空跳表

    // 清空后 → 强制归还内存 
    mallctl("arenas.purge", NULL, NULL, NULL, 0);
}

int main(int argc, char *argv[]) {
	if (argc != 2) {
printf("====================== KV Store Start ======================\n"
       "			./kvstore 192.168.30.128	2000				"
       "==========================================================\n");
		return -1;
	}
	int port = atoi(argv[1]);
	init_kvengine();

#if (NETWORK_SELECT == NETWORK_REACTOR)
	reactor_start(port, kvs_protocol);
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
	ntyco_start(port, kvs_protocol);
#elif (NETWORK_SELECT == NETWORK_NTYCO)
	proactor_start(port, kvs_protocol);
#endif

	dest_kvengine();
	return 0;
}