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
#include <signal.h>     // 用于 SIGPIPE 忽略（防止从机断连导致主机崩溃）
#include "repl.h"
#include "repl_net.h"
#include "rdma_repl.h"
#include "persistence.h"

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


extern kvs_array_t    global_array;
extern kvs_rbtree_t   global_rbtree;
extern kvs_hash_t     global_hash;
extern kvs_skiptable_t global_skiptable;

void *kvs_malloc(size_t size) {
    return malloc(size);
}

void kvs_free(void *ptr) {
    return free(ptr);
}

// ✅ 修改点6：函数签名适配DynBuf（核心！）
int kvs_filter_protocol(char **tokens, int count, DynBuf *wb);
void kvs_flush_all();
void free_tokens(char** tokens,int count);

const char *command[] = {
    "SET", "GET", "DEL", "MOD", "EXIST",
    "SAVE","FLUSHALL",
    "SLAVEOF", "PSYNC", "REPLCONF", "PING",
    "ECHO"  // 新增
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

    KVS_CMD_ECHO,

    KVS_CMD_COUNT,
};


int resp_try_parse_full_cmd(const char *buf, int buflen) {
    int idx = 0;

    // 修复：分开判断协议错误和数据不足
    if (buflen < 1) return 0; // 空数据，不完整
    if (buf[idx] != '*') return -1; // 协议错误，不是RESP数组
    if (buflen < 5) return 0; // 长度不足，不完整

    idx++;
    int argc = 0;
    while (idx < buflen && buf[idx] != '\r') {
        // 新增：非数字字符，协议错误
        if (buf[idx] < '0' || buf[idx] > '9') return -1;
        argc = argc * 10 + (buf[idx] - '0');
        idx++;
    }
    if (idx + 2 > buflen) return 0; // 没收到\r\n，不完整
    idx += 2;

    // 新增：参数数量非法，协议错误
    if (argc <= 0 || argc > KVS_MAX_TOKENS) return -1;

    for (int i = 0; i < argc; i++) {
        // 修复：分开判断数据不足和协议错误
        if (idx + 1 >= buflen) return 0; // 长度不足，不完整
        if (buf[idx] != '$') return -1; // 协议错误，不是字符串
        idx++;

        int slen = 0;
        while (idx < buflen && buf[idx] != '\r') {
            // 新增：非数字字符，协议错误
            if (buf[idx] < '0' || buf[idx] > '9') return -1;
            slen = slen * 10 + (buf[idx] - '0');
            idx++;
        }
        if (idx + 2 > buflen) return 0; // 没收到\r\n，不完整
        idx += 2;

        // 新增：字符串长度非法，协议错误
        if (slen < 0) return -1;
        if (idx + slen + 2 > buflen) return 0; // 内容没收全，不完整
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
/* USDT-style probe point for eBPF incremental replication.
 * __attribute__((noinline)) prevents the compiler from folding this
 * into the caller; the uprobe in ebpf_incr.bpf.c attaches here.
 * Called ONLY after kvs_protocol() confirms the write succeeded,
 * so the BPF program captures (msg, length) from argument registers
 * without needing any success/failure logic of its own.            */
__attribute__((noinline))
void kvs_rdma_incr_probe(const char *msg, int length)
{
    (void)msg; (void)length;
}

int kvs_protocol(char *msg, int length, DynBuf *write_buf) {
    if (msg == NULL || length <= 0 || write_buf == NULL)
        return -1;

    char *tokens[KVS_MAX_TOKENS] = {0};
    bool ok = resp_unpack(msg, length, tokens);
    if (!ok) {
        // 修复：解析失败时，生成标准错误回复
        dynbuf_append(write_buf, "-ERR invalid RESP protocol\r\n", 28);
        return -1;
    }

    int pre_len = write_buf->len;
    kvs_filter_protocol(tokens, KVS_MAX_TOKENS, write_buf);

    if (strcmp(tokens[0],"SET") == 0 ||
        strcmp(tokens[0],"DEL") == 0 ||
        strcmp(tokens[0],"MOD") == 0 ||
        strcmp(tokens[0],"FLUSHALL") == 0 )
    {
        /* Only persist/propagate when the operation actually succeeded.
         * Responses starting with '-' are errors; "$-1\r\n" means key
         * not found (DEL/MOD on non-existent key).  Neither should be
         * replayed from the AOF or forwarded to replicas. */
        const char *resp = write_buf->buf + pre_len;
        int rlen = write_buf->len - pre_len;
        int cmd_ok = (rlen > 0 && resp[0] != '-' &&
                      !(rlen >= 5 && strncmp(resp, "$-1\r\n", 5) == 0));

        if (cmd_ok && ENABLE_AOF) aof_async_append(msg, length);

        if (cmd_ok && kvs_config.repl_enabled && repl_role == REPL_MASTER) {
            repl_feed_backlog(msg, length);
            repl_propagate(msg, length);
        }

        /* eBPF USDT probe: captured by uprobe in ebpf_incr.bpf.c.
         * Fires only on successful writes; carries raw RESP bytes. */
        if (cmd_ok) kvs_rdma_incr_probe(msg, length);
    }

    free_tokens(tokens,KVS_MAX_TOKENS);
    return write_buf->len;
}
// 管道批处理总入口（网络层调用这个，才能执行你的批处理逻辑）
// 🔥 修正：批量协议处理入口（100% 匹配 msg_handler 函数指针）
int kvs_protocol_batch(char *msg, int len, DynBuf *write_buf) {
    // 静态变量：维护连接的读偏移（批量解析核心）
    static int read_offset = 0;
    DynBuf read_buf;

    // 初始化临时读缓冲区
    dynbuf_init(&read_buf, BUF_INIT_CAP);
    dynbuf_append(&read_buf, msg, len);

    // 清空写缓冲区，批量写入响应
    dynbuf_clear(write_buf);

    // 调用核心批量解析函数（循环解析所有命令）
    kvs_drain_resp_commands_off(&read_buf, &read_offset, write_buf);

    // 释放临时缓冲区
    dynbuf_free(&read_buf);

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

    // 🔥 修复2：检查 off 是否越界，越界则强制重置
    if (off < 0 || off >= rb->cap) {
        //printf("=== DRAIN: 警告！off=%d 越界，强制重置 off=0, rb->len=0 ===\n", off);
        off = 0;
        remaining = 0;
        rb->len = 0;
        *r_off = 0;
        return 0;
    }

    //printf("=== DRAIN: 进入循环，初始 off=%d, remaining=%d, rb->cap=%d ===\n", off, remaining, rb->cap);

    while (remaining > 0) {
        // ===================== 新增：跳过开头空白字符 =====================
        while (remaining > 0 && (rb->buf[off] == '\r' || rb->buf[off] == '\n' || rb->buf[off] == ' ')) {
            off++;
            remaining--;
        }
        if (remaining <= 0) break;
        // ==================================================================
        
        // 🔥 再次检查 off 是否越界
        if (off < 0 || off >= rb->cap) {
            //printf("=== DRAIN: 警告！off=%d 越界，强制退出 ===\n", off);
            break;
        }

        int cmd_len = resp_try_parse_full_cmd(rb->buf + off, remaining);
        
        //printf("=== DEBUG: off=%d, remaining=%d, cmd_len=%d ===\n", off, remaining, cmd_len);

        if (cmd_len > 0) {
            h(rb->buf + off, cmd_len, wb);
            off += cmd_len;
            remaining -= cmd_len;
            ncmd++;
        } else if (cmd_len == 0) {
            break;
        } else {
            // 🔥 修复3：改进错误处理，找到下一个 '*' 作为新的命令开头
            //printf("=== DRAIN: 解析错误，跳过垃圾数据，寻找下一个 '*' ===\n");
            
            int skip = 1;
            while (skip < remaining && (off + skip) < rb->cap && rb->buf[off + skip] != '*') {
                skip++;
            }
            
            if (skip >= remaining || (off + skip) >= rb->cap) {
                skip = remaining;
            }
            
            off += skip;
            remaining -= skip;
        }
    }

    //printf("=== DRAIN: 退出循环，最终 off=%d, remaining=%d, ncmd=%d, wb->len=%d ===\n", 
       //    off, remaining, ncmd, wb->len);

    // 更新偏移量和缓冲区长度
    *r_off = off;
    rb->len = remaining;

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
    //printf("=== FILTER: 处理命令 %s，进入时 wb->len=%d ===\n", tokens[0], wb->len);
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
            if      (kvs_config.engine == ENGINE_ARRAY)     ret = kvs_array_set(&global_array, key, item);
            else if (kvs_config.engine == ENGINE_RBTREE)    ret = kvs_rbtree_set(&global_rbtree, key, item);
            else if (kvs_config.engine == ENGINE_HASH)      ret = kvs_hash_set(&global_hash, key, item);
            else if (kvs_config.engine == ENGINE_SKIPTABLE) ret = kvs_skiptable_set(&global_skiptable, key, item);
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
            if      (kvs_config.engine == ENGINE_ARRAY)     result = kvs_array_get(&global_array, key);
            else if (kvs_config.engine == ENGINE_RBTREE)    result = kvs_rbtree_get(&global_rbtree, key);
            else if (kvs_config.engine == ENGINE_HASH)      result = kvs_hash_get(&global_hash, key);
            else if (kvs_config.engine == ENGINE_SKIPTABLE) result = kvs_skiptable_get(&global_skiptable, key);
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
            if      (kvs_config.engine == ENGINE_ARRAY)     ret = kvs_array_del(&global_array, key);
            else if (kvs_config.engine == ENGINE_RBTREE)    ret = kvs_rbtree_del(&global_rbtree, key);
            else if (kvs_config.engine == ENGINE_HASH)      ret = kvs_hash_del(&global_hash, key);
            else if (kvs_config.engine == ENGINE_SKIPTABLE) ret = kvs_skiptable_del(&global_skiptable, key);
            if (ret < 0) {
                dynbuf_append(wb, RESP_ERROR, strlen(RESP_ERROR));
            } else if (ret == 0) {
                dynbuf_append(wb, RESP_OK, strlen(RESP_OK));
            } else {
                dynbuf_append(wb, RESP_NIL, strlen(RESP_NIL));
            }
            break;

        case KVS_CMD_MOD:
            if      (kvs_config.engine == ENGINE_ARRAY)     ret = kvs_array_mod(&global_array, key, item);
            else if (kvs_config.engine == ENGINE_RBTREE)    ret = kvs_rbtree_mod(&global_rbtree, key, item);
            else if (kvs_config.engine == ENGINE_HASH)      ret = kvs_hash_mod(&global_hash, key, item);
            else if (kvs_config.engine == ENGINE_SKIPTABLE) ret = kvs_skiptable_mod(&global_skiptable, key, item);
            if (ret < 0) {
                dynbuf_append(wb, RESP_ERROR, strlen(RESP_ERROR));
            } else if (ret == 0) {
                dynbuf_append(wb, RESP_OK, strlen(RESP_OK));
            } else {
                dynbuf_append(wb, RESP_NIL, strlen(RESP_NIL));
            }
            break;

        case KVS_CMD_EXIST:
            if      (kvs_config.engine == ENGINE_ARRAY)     ret = kvs_array_exist(&global_array, key);
            else if (kvs_config.engine == ENGINE_RBTREE)    ret = kvs_rbtree_exist(&global_rbtree, key);
            else if (kvs_config.engine == ENGINE_HASH)      ret = kvs_hash_exist(&global_hash, key);
            else if (kvs_config.engine == ENGINE_SKIPTABLE) ret = kvs_skiptable_exist(&global_skiptable, key);
            if (ret == 0) {
                dynbuf_append(wb, RESP_EXIST, strlen(RESP_EXIST));
            } else {
                dynbuf_append(wb, RESP_NIL, strlen(RESP_NIL));
            }
            break;

        case KVS_CMD_SAVE:
            rdb_save_async_uring();   /* fork + io_uring，主线程零阻塞 */
            dynbuf_append(wb, RESP_OK, strlen(RESP_OK));
            break;

        case KVS_CMD_FLUSHALL:
            kvs_flush_all();
            dynbuf_append(wb, RESP_OK, strlen(RESP_OK));
            break;



        case KVS_CMD_SLAVEOF: {
            if (!kvs_config.repl_enabled) {
                dynbuf_append(wb, "-ERR replication disabled in config\r\n", 38);
                break;
            }
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
            
        case KVS_CMD_ECHO:
            if (count >= 2) {
                sprintf(temp_buf, "$%zu\r\n", strlen(tokens[1]));
                dynbuf_append(wb, temp_buf, strlen(temp_buf));
                dynbuf_append(wb, tokens[1], strlen(tokens[1]));
                dynbuf_append(wb, "\r\n", 2);
            } else {
                dynbuf_append(wb, "-ERR wrong number of arguments for 'echo' command\r\n", 50);
            }
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
    if (kvs_config.engine == ENGINE_ARRAY) {
        printf("Using array engine\n");
        memset(&global_array, 0, sizeof(kvs_array_t));
        kvs_array_create(&global_array);
    } else if (kvs_config.engine == ENGINE_RBTREE) {
        printf("Using rbtree engine\n");
        memset(&global_rbtree, 0, sizeof(kvs_rbtree_t));
        kvs_rbtree_create(&global_rbtree);
    } else if (kvs_config.engine == ENGINE_HASH) {
        printf("Using hash engine\n");
        memset(&global_hash, 0, sizeof(kvs_hash_t));
        kvs_hash_create(&global_hash);
    } else if (kvs_config.engine == ENGINE_SKIPTABLE) {
        printf("Using skiptable engine\n");
        memset(&global_skiptable, 0, sizeof(kvs_skiptable_t));
        kvs_skiptable_create(&global_skiptable);
    }
    return 0;
}

void dest_kvengine(void) {
    if      (kvs_config.engine == ENGINE_ARRAY)     kvs_array_destory(&global_array);
    else if (kvs_config.engine == ENGINE_RBTREE)    kvs_rbtree_destory(&global_rbtree);
    else if (kvs_config.engine == ENGINE_HASH)      kvs_hash_destory(&global_hash);
    else if (kvs_config.engine == ENGINE_SKIPTABLE) kvs_skiptable_destroy(&global_skiptable);
}

// 正确：清空所有数据，保留引擎可用
void kvs_flush_all() {
    if      (kvs_config.engine == ENGINE_ARRAY)     kvs_array_clear(&global_array);
    else if (kvs_config.engine == ENGINE_HASH)      kvs_hash_clear(&global_hash);
    else if (kvs_config.engine == ENGINE_RBTREE)    kvs_rbtree_clear(&global_rbtree);
    else if (kvs_config.engine == ENGINE_SKIPTABLE) kvs_skiptable_clear(&global_skiptable);
    // 只清理内存碎片，不销毁引擎
    mallctl("arenas.purge", NULL, NULL, NULL, 0);
}

void engine_reload(void) {
    if (ENABLE_AOF && access("appendonly.aof", F_OK) == 0) {
        printf("Loading AOF (mmap)...\n");
        aof_reload_mmap();
    } else if (ENABLE_RDB && access("dump.rdb", F_OK) == 0) {
        printf("Loading RDB (mmap)...\n");
        rdb_load_mmap();
    } else {
        printf("Engine reload: no persistence file\n");
    }
}

int main(int argc, char *argv[]) {
    signal(SIGPIPE, SIG_IGN);   /* 防止从机断连时 send() 触发 SIGPIPE 杀死进程 */
    const char *conf_path = (argc >= 2) ? argv[1] : "kvstore.conf";
    config_load(conf_path);
    config_print();

    init_kvengine();
    engine_reload();
    if (ENABLE_AOF) aof_async_init("appendonly.aof");

    if (kvs_config.repl_enabled) {
        if (kvs_config.role == ROLE_SLAVE && kvs_config.masterip[0] != '\0') {
            repl_switch_to_slave(kvs_config.masterip, kvs_config.masterport);
        } else {
            repl_init();
        }
    } else {
        printf("[REPL] legacy replication disabled by config\n");
    }

    /* --- RDMA + eBPF 主从同步（新模式，与旧 TCP 复制相互独立）.
     * rdma_sync 是总开关，rdma_repl 为兼容保留。
     * 全量同步走 RDMA (libfabric)，增量同步由 eBPF 捕获经 TCP 传输。 */
    if (kvs_config.rdma_sync_enabled || kvs_config.rdma_repl_enabled) {
        if (kvs_config.rdma_role == ROLE_MASTER) {
            rdma_repl_master_start();
        } else {
            if (rdma_repl_slave_run() != 0)
                fprintf(stderr, "[RDMA-REPL] slave full-sync failed; "
                                "continuing with local state\n");
        }
    } else {
        printf("[RDMA-REPL] rdma replication disabled by config\n");
    }

    if (kvs_config.network_model == NET_REACTOR)
        reactor_start(kvs_config.port, kvs_protocol_batch);
    else if (kvs_config.network_model == NET_PROACTOR)
        proactor_start(kvs_config.port, kvs_protocol_batch);
    else if (kvs_config.network_model == NET_NTYCO)
        ntyco_start(kvs_config.port, kvs_protocol_batch);

    if (ENABLE_AOF) aof_async_shutdown();
    dest_kvengine();
    return 0;
}
