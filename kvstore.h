#ifndef __KV_STORE_H__
#define __KV_STORE_H__


#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stddef.h>
#include<sys/time.h>
#include<unistd.h>
#include "server.h"


#include<jemalloc/jemalloc.h>
#include "repl.h"


// extern msg_handler kvs_protocol;

#define ENABLE_RDB 1       
#define ENABLE_AOF 0

#define NETWORK_REACTOR 	0
#define NETWORK_PROACTOR	1
#define NETWORK_NTYCO		2

#define NETWORK_SELECT		NETWORK_PROACTOR
#define ENABLE_JEMALLOC 1
//初始缓冲区大小
#define BUF_INIT_CAP			4096


#define KVS_MAX_TOKENS		256

#define ENABLE_ARRAY		0
#define ENABLE_RBTREE		0
#define ENABLE_HASH			0
#define ENABLE_SKIPTABLE	1




// 协议处理回调（统一所有网络模型：io_uring / reactor / 协程）
typedef int (*msg_handler)(char *msg, int len, DynBuf *write_buf);
extern msg_handler kvs_handler_batch;

extern int reactor_start(unsigned short port, msg_handler handler);
extern int proactor_start(unsigned short port, msg_handler handler);
extern int ntyco_start(unsigned short port, msg_handler handler);
long long get_time_now(void);
int resp_try_parse_full_cmd(const char*buf ,int buflen);
void rdb_save_async(void);
void kvs_flush_all();
void rdb_load(void);
// RESP协议解析函数声明
bool resp_unpack(const char *data, int len, char** out_commands);
void free_tokens(char **tokens, int count);

//value结构体负责存储值
typedef struct {
    char*  value;	//数据本体
    size_t len;		//数据长度（大文件，二进制文件）
    long long expire;//过期时间戳
} KVItem;


typedef struct {
	FILE *fp;
}RdbSaveCtx;

//引擎遍历函数回调
typedef void (*kv_scan_cb)(const char *key, KVItem *item, void *priv);


#if ENABLE_ARRAY


typedef struct kvs_array_item_s {
	char*key;
	KVItem*item;
} kvs_array_item_t;




#define KVS_ARRAY_SIZE		1024

typedef struct kvs_array_s {
	kvs_array_item_t *table;
	int idx;
	int total;
} kvs_array_t;

int kvs_array_create(kvs_array_t *inst);
void kvs_array_destory(kvs_array_t *inst);

int kvs_array_set(kvs_array_t *inst, const char *key,const KVItem*item);
KVItem* kvs_array_get(kvs_array_t *inst, const char *key);
int kvs_array_del(kvs_array_t *inst, const char *key);
int kvs_array_mod(kvs_array_t *inst, const char *key, const KVItem*item);
int kvs_array_exist(kvs_array_t *inst, const char *key);


//rdb处理函数
void kvs_array_scan(kvs_array_t *db, kv_scan_cb cb, void *priv);
//清空数据（RDB 加载前用）
void kvs_array_clear(kvs_array_t *db);

#endif


#if ENABLE_RBTREE

#define RED				1
#define BLACK 			2

#define ENABLE_KEY_CHAR		1

#if ENABLE_KEY_CHAR
typedef char* KEY_TYPE;
#else
typedef int KEY_TYPE;
#endif

typedef struct _rbtree_node {
	unsigned char color;
	struct _rbtree_node *right;
	struct _rbtree_node *left;
	struct _rbtree_node *parent;
	KEY_TYPE key;
	void *value;     // 这里存 KVItem*
} rbtree_node;

typedef struct _rbtree {
	rbtree_node *root;
	rbtree_node *nil;
	int total;      // ✅ 键值对数量
} rbtree;

typedef struct _rbtree kvs_rbtree_t;

// 全部统一成 KVItem 接口
int kvs_rbtree_create(kvs_rbtree_t *inst);
void kvs_rbtree_destory(kvs_rbtree_t *inst);

int kvs_rbtree_set(kvs_rbtree_t *inst, const char *key, const KVItem *item);
KVItem* kvs_rbtree_get(kvs_rbtree_t *inst, const char *key);
int kvs_rbtree_del(kvs_rbtree_t *inst, const char *key);
int kvs_rbtree_mod(kvs_rbtree_t *inst, const char *key, const KVItem *item);
int kvs_rbtree_exist(kvs_rbtree_t *inst, const char *key);

void kvs_rbtree_scan(kvs_rbtree_t *db, kv_scan_cb cb, void *priv);
void kvs_rbtree_clear(kvs_rbtree_t *db);

#endif


#if ENABLE_HASH

#define MAX_KEY_LEN		128
#define MAX_VALUE_LEN	512
#define MAX_TABLE_SIZE	1024

#define ENABLE_KEY_POINTER	1

typedef struct hashnode_s {
#if ENABLE_KEY_POINTER
	char *key;
	KVItem *value;  // 🔥 统一改成 KVItem
#else
	char key[MAX_KEY_LEN];
	char value[MAX_VALUE_LEN];
#endif
	struct hashnode_s *next;
} hashnode_t;

typedef struct hashtable_s {
	hashnode_t **nodes;
	int max_slots;
	int count;
} hashtable_t;

typedef struct hashtable_s kvs_hash_t;

// 🔥 全部对齐 KVItem 接口
int kvs_hash_create(kvs_hash_t *hash);
void kvs_hash_destory(kvs_hash_t *hash);

int kvs_hash_set(kvs_hash_t *hash, const char *key, const KVItem *item);
KVItem* kvs_hash_get(kvs_hash_t *hash, const char *key);
int kvs_hash_mod(kvs_hash_t *hash, const char *key, const KVItem *item);
int kvs_hash_del(kvs_hash_t *hash, const char *key);
int kvs_hash_exist(kvs_hash_t *hash, const char *key);

//  RDB 遍历 + 清空
void kvs_hash_scan(kvs_hash_t *db, kv_scan_cb cb, void *priv);
void kvs_hash_clear(kvs_hash_t *db);

#endif



#if ENABLE_SKIPTABLE

#define MAX_LEVEL 6

// 彻底改成字符串key，和其他引擎统一
typedef struct Node {
    char *key;          // 字符串key，支持中文/任意字符
    KVItem *value;      // 统一KVItem
    struct Node **forward;
} Node;

typedef struct SkipList {
    int level;
    Node *header;
    int total;          // RDB计数
} SkipList;

struct kvs_skiptable {
    SkipList *skl;
};

typedef struct kvs_skiptable kvs_skiptable_t;

// 统一接口，和array/hash/rbtree完全一致
int kvs_skiptable_create(kvs_skiptable_t *inst);
void kvs_skiptable_destroy(kvs_skiptable_t *inst);

int kvs_skiptable_set(kvs_skiptable_t *inst, const char *key, const KVItem *item);
KVItem* kvs_skiptable_get(kvs_skiptable_t *inst, const char *key);
int kvs_skiptable_del(kvs_skiptable_t *inst, const char *key);
int kvs_skiptable_mod(kvs_skiptable_t *inst, const char *key, const KVItem *item);
int kvs_skiptable_exist(kvs_skiptable_t *inst, const char *key);

// RDB遍历+清空
void kvs_skiptable_scan(kvs_skiptable_t *db, kv_scan_cb cb, void *priv);
void kvs_skiptable_clear(kvs_skiptable_t *db);

#endif









void *kvs_malloc(size_t size);
void kvs_free(void *ptr);

#endif