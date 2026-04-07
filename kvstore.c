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
#include "repl.h"
#include "repl_net.h"

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
void rdb_save(void);
void aof_append(const char* resp_buf,int buf_len);
void free_tokens(char** tokens,int count);

const char *command[] = {
    "SET", "GET", "DEL", "MOD", "EXIST",
    "SAVE","FLUSHALL",
    "SLAVEOF", "PSYNC", "REPLCONF", "PING"  // 新增
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

    KVS_CMD_SLAVEOF,    // 新增：主从切换
    KVS_CMD_PSYNC,      // 新增：同步
    KVS_CMD_REPLCONF,   // 新增：配置
    KVS_CMD_PING,       // 新增：心跳

    KVS_CMD_COUNT,
};

// int resp_try_parse_full_cmd(const char*buf ,int buflen){
// 	int idx = 0;
// 	if(buflen<5 || buf[idx]!= '*') return -1;

// 	idx++;
// 	int argc = 0;
// 	while(idx < buflen && buf[idx]!='\r'){
// 		argc = argc * 10 + (buf[idx] - '0');
// 		idx++;
// 	}
// 	if(idx + 2 > buflen ) return 0;
// 	idx += 2;

// 	for(int i=0;i < argc ;i++){
// 		if(idx + 1>=buflen || buf[idx] != '$') return -1;
// 		idx++;
// 		int slen = 0;
// 		while(idx <buflen && buf[idx] != '\r'){
// 			slen = slen * 10 + (buf[idx] - '0');
// 			idx++;
// 		}
// 		if(idx + 2 >buflen) return 0;
// 		idx+=2;
// 		if(idx + slen +2 > buflen) return 0;
// 		idx+=slen + 2;
// 	}
// 	return idx;
// }
int resp_try_parse_full_cmd(const char *buf, int buflen) {
    int idx = 0;
    if (buflen < 5 || buf[idx] != '*')
        return -1;

    idx++;
    int argc = 0;
    while (idx < buflen && buf[idx] != '\r') {
        argc = argc * 10 + (buf[idx] - '0');
        idx++;
    }
    if (idx + 2 > buflen) return 0;
    idx += 2;

    for (int i = 0; i < argc; i++) {
        if (idx + 1 >= buflen || buf[idx] != '$')
            return -1;
        idx++;

        int slen = 0;
        while (idx < buflen && buf[idx] != '\r') {
            slen = slen * 10 + (buf[idx] - '0');
            idx++;
        }
        if (idx + 2 > buflen) return 0;
        idx += 2;

        if (idx + slen + 2 > buflen) return 0;
        idx += slen + 2;
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

    /* 不在此清空 write_buf：网络层可能一次读取到多条命令（pipeline），需要连续追加响应。
     * 调用方在开启新一批输出前自行 dynbuf_clear(write_buf)。 */

    char *tokens[KVS_MAX_TOKENS] = {0};
    bool ok = resp_unpack(msg, length, tokens);
    if (!ok)
        return -1;

    // ✅ 修改点8：传入DynBuf，不再用固定数组
    kvs_filter_protocol(tokens, KVS_MAX_TOKENS, write_buf);

    if (strcmp(tokens[0],"SET") == 0 ||
        strcmp(tokens[0],"DEL") == 0 ||
        strcmp(tokens[0],"MOD") == 0 )
    {
        if(ENABLE_AOF) aof_append(msg,length);
        
        // ✅ 调用复制层接口：同步给从机
        if (repl_role == REPL_MASTER) {
            repl_feed_backlog(msg, length);
            repl_propagate(msg, length);
        }
    }

    free_tokens(tokens,KVS_MAX_TOKENS);
    return write_buf->len;
}

/* ===================== 批量解析（偏移指针版，避免 memmove） ===================== */
static void dynbuf_compact_by_copy(DynBuf *rb, int *r_off)
{
    if (!rb || !rb->buf || !r_off)
        return;
    if (*r_off == 0 || rb->len <= 0)
        return;

    char *nb = (char *)malloc((size_t)rb->cap);
    if (!nb)
        return;
    memcpy(nb, rb->buf + *r_off, (size_t)rb->len);
    free(rb->buf);
    rb->buf = nb;
    *r_off = 0;
}

int kvs_drain_resp_commands_ex_off(DynBuf *rb, int *r_off, DynBuf *wb, msg_handler h)
{
    if (!rb || !rb->buf || !r_off || !wb || !h)
        return -1;

    int ncmd = 0;
    int off = *r_off;
    int remaining = rb->len;

    while (remaining > 0) {
        int cmd_len = resp_try_parse_full_cmd(rb->buf + off, remaining);
        if (cmd_len <= 0)
            break;
        h(rb->buf + off, cmd_len, wb);
        off += cmd_len;
        remaining -= cmd_len;
        ncmd++;
    }

    *r_off = off;
    rb->len = remaining;

    if (rb->len == 0) {
        *r_off = 0;
    } else if (*r_off > (rb->cap / 2)) {
        /* 及时回收已读数据：用 malloc+memcpy 进行一次性整理（避免 memmove）。 */
        dynbuf_compact_by_copy(rb, r_off);
    }

    return ncmd;
}

int kvs_drain_resp_commands_off(DynBuf *rb, int *r_off, DynBuf *wb)
{
    return kvs_drain_resp_commands_ex_off(rb, r_off, wb, kvs_protocol);
}
/* ============================================================================ */

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



        case KVS_CMD_SLAVEOF: {
            // 命令：SLAVEOF NO ONE → 切换主机
            if (strcmp(tokens[1], "NO") == 0 && strcmp(tokens[2], "ONE") == 0) {
                repl_switch_to_master();
                dynbuf_append(wb, "+OK\r\n", 5);
            } 
            // 命令：SLAVEOF IP PORT → 切换从机
            else {
                char *ip = tokens[1];
                int port = atoi(tokens[2]);
                repl_switch_to_slave(ip, port);
                dynbuf_append(wb, "+OK\r\n", 5);
            }
            break;
        }

        case KVS_CMD_PSYNC:
            // 这里不处理，复制层会处理
            break;
        case KVS_CMD_REPLCONF:
            // 这里不处理，复制层会处理
            break;
        case KVS_CMD_PING:
            dynbuf_append(wb, "+PONG\r\n", 7);
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

// 正确：清空所有数据，保留引擎可用
void kvs_flush_all() {
#if ENABLE_ARRAY
    kvs_array_clear(&global_array);    // 清空数组数据
#endif
#if ENABLE_HASH
    kvs_hash_clear(&global_hash);      // 清空哈希表数据
#endif
#if ENABLE_RBTREE
    kvs_rbtree_clear(&global_rbtree);   // 清空红黑树数据
#endif
#if ENABLE_SKIPTABLE
    kvs_skiptable_clear(&global_skiptable);  // 清空跳表数据
#endif
    // 只清理内存碎片，不销毁引擎
    mallctl("arenas.purge", NULL, NULL, NULL, 0);
}

static void rdb_save_callback(const char *key,KVItem*item,void*priv){
    if(!key || !item) return ;

    RdbSaveCtx *ctx = (RdbSaveCtx*) priv;
    FILE *fp = ctx -> fp;
    
    //写入key
    int klen = strlen(key);
    fwrite(&klen,sizeof(int),1,fp);
    fwrite(key,1,klen,fp);

    //写入value
    int vlen = item -> len;
    fwrite(&vlen,sizeof(int),1,fp);
    fwrite(item->value,1,vlen,fp);

    //写入过期时间
    fwrite(&item->expire,sizeof(long long),1,fp);
}

void rdb_save(void){

    FILE*fp = fopen("dump.rdb","wb");
    if(!fp) return ;

    //头部
    int magic = 0x524442;
    int version = 1;
    int db_num = 0;
#if ENABLE_ARRAY
    int key_count = global_array.total;           //统一宏处理
#endif
#if ENABLE_RBTREE
    int key_count = global_rbtree.total;           //统一宏处理
#endif
#if ENABLE_HASH
    int key_count = global_hash.count;           //统一宏处理
#endif
#if ENABLE_SKIPTABLE
    int key_count = global_skiptable.skl->total;
#endif

    fwrite(&magic, sizeof(int), 1, fp);
    fwrite(&version, sizeof(int), 1, fp);
    fwrite(&db_num, sizeof(int), 1, fp);
    fwrite(&key_count, sizeof(int), 1, fp);


    // 遍历所有引擎数据并写入
    RdbSaveCtx ctx = {.fp = fp};

#if (ENABLE_ARRAY)                         
        kvs_array_scan(&global_array, rdb_save_callback, &ctx);  
#endif
#if (ENABLE_RBTREE)                  
        kvs_rbtree_scan(&global_rbtree, rdb_save_callback, &ctx);
#endif
#if (ENABLE_HASH)                    
        kvs_hash_scan(&global_hash, rdb_save_callback, &ctx);
#endif   
#if (ENABLE_SKIPTABLE)                
        kvs_skiptable_scan(&global_skiptable, rdb_save_callback, &ctx); 
#endif

    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);
    printf("RDB saved success: dump.rdb\n");
}


// 主机：异步RDB保存（Redis BGSAVE 实现）
void rdb_save_async(void) {
    pid_t pid = fork();
    if (pid == -1) {
        printf("fork失败，使用同步RDB\n");
        rdb_save(); // 降级
        return;
    }
    if (pid == 0) {
        // 子进程：执行阻塞式RDB保存
        rdb_save();
        exit(0); // 完成后退出
    } else {
        // 父进程：直接返回，继续处理请求 ✅ 无阻塞
        printf("主机异步RDB生成中，pid=%d\n", pid);
    }
}
void rdb_load(void) {
    FILE *fp = fopen("dump.rdb", "rb");
    if (!fp) {
        printf("RDB: 打开文件失败\n");
        return;
    }
    printf("RDB load start...\n"); // 加这个

    int magic, version, db_num, key_count;
    fread(&magic, sizeof(int), 1, fp);
    fread(&version, sizeof(int), 1, fp);
    fread(&db_num, sizeof(int), 1, fp);
    fread(&key_count, sizeof(int), 1, fp);

    if (magic != 0x524442) {
        fclose(fp);
        return;
    }


#if (ENABLE_ARRAY) 
    kvs_array_clear(&global_array); 
#endif 
#if (ENABLE_RBTREE) 
    kvs_rbtree_clear(&global_rbtree); 
#endif 
#if (ENABLE_HASH) 
    kvs_hash_clear(&global_hash);
#endif  
#if (ENABLE_SKIPTABLE) 
    kvs_skiptable_clear(&global_skiptable); 
#endif 
   

    for (int i = 0; i < key_count; i++) {
        int klen, vlen;
        long long expire;

        fread(&klen, sizeof(int), 1, fp);
        char *key = (char*)kvs_malloc(klen + 1);
        fread(key, 1, klen, fp);
        key[klen] = 0;

        fread(&vlen, sizeof(int), 1, fp);
        char *val = (char*)kvs_malloc(vlen + 1);
        fread(val, 1, vlen, fp);
        val[vlen] = 0;

        fread(&expire, sizeof(long long), 1, fp);

        //  直接构建 KVItem 并插入引擎（不经过协议）
        KVItem *item = create_kvitem(val, NULL);
        item->expire = expire;

        // 统一插入
#if (ENABLE_ARRAY)
            kvs_array_set(&global_array, key, item);
#endif
#if (ENABLE_RBTREE) 
            kvs_rbtree_set(&global_rbtree, key, item);
#endif
#if (ENABLE_HASH) 
            kvs_hash_set(&global_hash, key, item);
#endif
#if (ENABLE_SKIPTABLE) 
            kvs_skiptable_set(&global_skiptable, key, item);
#endif


        kvs_free(key);
        kvs_free(val);
    }

    fclose(fp);
    printf("RDB load success!\n");
}

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
    if(!buf) return ;
    fread(buf,1,file_size,fp);
    buf[file_size] = 0;
    fclose(fp);

    char*p = buf ;
    long remaining = file_size;
    printf("AOF load start,size:%ld\n",file_size);

    while(remaining > 0){
        int cmd_len = resp_try_parse_full_cmd(p,remaining);

        if(cmd_len <= 0){
            printf("AOF load exit,cmd_len:%d,remaining:%ld\n",cmd_len,remaining);
            break;
        }

        DynBuf wb; //  修改点11：AOF重载适配DynBuf
        dynbuf_init(&wb, BUF_INIT_CAP);
        kvs_protocol(p, cmd_len, &wb);
        dynbuf_free(&wb);

        //移动指针和剩余长度
        p += cmd_len;
        remaining -= cmd_len;
    }
    kvs_free(buf);
    printf("AOF load finished!\n");
}

void engine_reload(void) {
    if (ENABLE_AOF && access("appendonly.aof", F_OK) == 0) {
        printf("Loading AOF...\n");
        aof_reload();
    } 
    else if (access("dump.rdb", F_OK) == 0) {
        printf("Loading RDB...\n");
        rdb_load();
    }else{
        printf("Engine reload fail\n");
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

    repl_init();  // ✅ 初始化复制模块

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