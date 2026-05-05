#include "kvstore.h"
#include "server.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <assert.h>
#include <malloc.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include "rdma_repl.h"
#include "persistence.h"

// ==================== 静态 RESP 响应（编译期 sizeof 消除 strlen） ====================
#define RESP_OK           "+OK\r\n"
#define RESP_OK_LEN       ((int)(sizeof(RESP_OK)-1))
#define RESP_NIL          "$-1\r\n"
#define RESP_NIL_LEN      ((int)(sizeof(RESP_NIL)-1))
#define RESP_EXIST        ":1\r\n"
#define RESP_EXIST_LEN    ((int)(sizeof(RESP_EXIST)-1))
#define RESP_ERROR        "-ERR error\r\n"
#define RESP_ERROR_LEN    ((int)(sizeof(RESP_ERROR)-1))
#define RESP_PONG         "+PONG\r\n"
#define RESP_PONG_LEN     ((int)(sizeof(RESP_PONG)-1))
#define RESP_ERR_UNK      "-ERR unknown command\r\n"
#define RESP_ERR_UNK_LEN  ((int)(sizeof(RESP_ERR_UNK)-1))
#define RESP_ERR_INV      "-ERR invalid RESP protocol\r\n"
#define RESP_ERR_INV_LEN  ((int)(sizeof(RESP_ERR_INV)-1))

#define KVS_MAX_TOKENS    256

// ==================== 动态缓冲区工具函数 ====================
void dynbuf_init(DynBuf *db, int init_cap) {
    if (!db || init_cap <= 0) return;
    db->buf = (char*)malloc((size_t)init_cap);
    db->len = 0;
    db->cap = init_cap;
}

int dynbuf_append(DynBuf *db, const char *data, int len) {
    if (!db || !data || len <= 0) return -1;
    if (db->len + len > db->cap) {
        int new_cap = db->cap > 0 ? db->cap : 16;
        while (new_cap < db->len + len) new_cap *= 2;
        char *new_buf = (char*)realloc(db->buf, (size_t)new_cap);
        if (!new_buf) return -1;
        db->buf = new_buf;
        db->cap = new_cap;
    }
    memcpy(db->buf + db->len, data, (size_t)len);
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

// ==================== 工具函数 ====================

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

/* 将 token slice 复制到栈缓冲区并 null-terminate。
   引擎接口需要 C 字符串 key，但 key 通常很短（<128 字节），栈拷贝可忽略。 */
static inline char *token_to_key(kvs_token_t t, char *buf, int buf_size) {
    int n = t.len < buf_size - 1 ? t.len : buf_size - 1;
    memcpy(buf, t.ptr, (size_t)n);
    buf[n] = '\0';
    return buf;
}

static inline bool token_is_write_cmd(kvs_token_t t) {
    return TOKEN_EQ(t, "SET") || TOKEN_EQ(t, "DEL") ||
           TOKEN_EQ(t, "MOD") || TOKEN_EQ(t, "FLUSHALL");
}

// ==================== 零拷贝 RESP 解析器（单遍扫描） ====================
// 替代 resp_try_parse_full_cmd() + resp_unpack() 两次扫描 + malloc
// 一次遍历完成格式验证 + 提取 token slice (ptr, len)
int resp_parse_slices(const char *buf, int buflen,
                      kvs_token_t *tokens, int max_tokens,
                      int *out_argc) {
    int idx = 0;

    if (buflen < 1) return 0;
    if (buf[idx] != '*') return -1;
    if (buflen < 5) return 0;

    idx++;
    int argc = 0;
    while (idx < buflen && buf[idx] != '\r') {
        if (buf[idx] < '0' || buf[idx] > '9') return -1;
        argc = argc * 10 + (buf[idx] - '0');
        idx++;
    }
    if (idx + 2 > buflen) return 0;
    idx += 2;

    if (argc <= 0 || argc > max_tokens) return -1;

    for (int i = 0; i < argc; i++) {
        if (idx + 1 >= buflen) return 0;
        if (buf[idx] != '$') return -1;
        idx++;

        int slen = 0;
        while (idx < buflen && buf[idx] != '\r') {
            if (buf[idx] < '0' || buf[idx] > '9') return -1;
            slen = slen * 10 + (buf[idx] - '0');
            idx++;
        }
        if (idx + 2 > buflen) return 0;
        idx += 2;

        if (slen < 0) return -1;
        if (idx + slen + 2 > buflen) return 0;

        /* 零拷贝：直接指向 buf 内部，不分配不复制 */
        tokens[i].ptr = buf + idx;
        tokens[i].len = slen;

        idx += slen + 2;
    }

    *out_argc = argc;
    return idx;
}

// ==================== 命令路由（Token Slice 版本） ====================

enum {
    KVS_CMD_SET = 0,
    KVS_CMD_GET,
    KVS_CMD_DEL,
    KVS_CMD_MOD,
    KVS_CMD_EXIST,
    KVS_CMD_SAVE,
    KVS_CMD_FLUSHALL,
    KVS_CMD_PING,
    KVS_CMD_ECHO,
    KVS_CMD_COUNT,
};

long long get_time_now(void){
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000LL + tv.tv_usec / 1000;
}

KVItem* create_kvitem(char *value_str, char *expire_str){
    if (!value_str) return NULL;
    int vlen = (int)strlen(value_str);
    KVItem *item = create_kvitem_len(value_str, vlen);
    if (item && expire_str) {
        int sec = atoi(expire_str);
        item->expire = get_time_now() + (long long)sec * 1000;
    }
    return item;
}

KVItem* create_kvitem_len(const char *value, int vlen) {
    if (!value || vlen < 0) return NULL;
    KVItem *item = (KVItem*)kvs_malloc(sizeof(KVItem));
    memset(item, 0, sizeof(KVItem));
    item->len = (size_t)vlen;
    item->value = (char*)kvs_malloc((size_t)vlen + 1);
    memcpy(item->value, value, (size_t)vlen);
    item->value[vlen] = '\0';
    item->expire = 0;
    return item;
}

void free_kvitem(KVItem *item){
    if (!item) return;
    if (item->value) kvs_free(item->value);
    kvs_free(item);
}

__attribute__((noinline))
void kvs_rdma_incr_probe(const char *msg, int length)
{
    (void)msg; (void)length;
}

/* ==================== 命令过滤执行器（Token Slice 版本） ==================== */
int kvs_filter_protocol(char **tokens, int count, DynBuf *wb);

static int kvs_filter_protocol_v2(kvs_token_t *tokens, int argc, DynBuf *wb)
{
    if (tokens[0].ptr == NULL || argc == 0 || wb == NULL)
        return -1;

    /* 首字节预筛 + TOKEN_EQ：O(1) 命令判别，无 strlen/strcmp */
    int cmd = KVS_CMD_COUNT;
    switch (tokens[0].ptr[0]) {
    case 'S':
        if (TOKEN_EQ(tokens[0], "SET"))  cmd = KVS_CMD_SET;
        if (TOKEN_EQ(tokens[0], "SAVE")) cmd = KVS_CMD_SAVE;
        break;
    case 'G':
        if (TOKEN_EQ(tokens[0], "GET")) cmd = KVS_CMD_GET;
        break;
    case 'D':
        if (TOKEN_EQ(tokens[0], "DEL")) cmd = KVS_CMD_DEL;
        break;
    case 'M':
        if (TOKEN_EQ(tokens[0], "MOD")) cmd = KVS_CMD_MOD;
        break;
    case 'E':
        if (TOKEN_EQ(tokens[0], "EXIST")) cmd = KVS_CMD_EXIST;
        if (TOKEN_EQ(tokens[0], "ECHO"))  cmd = KVS_CMD_ECHO;
        break;
    case 'F':
        if (TOKEN_EQ(tokens[0], "FLUSHALL")) cmd = KVS_CMD_FLUSHALL;
        break;
    case 'P':
        if (TOKEN_EQ(tokens[0], "PING")) cmd = KVS_CMD_PING;
        break;
    default:
        break;
    }

    /* 从 token slice 提取 key（引擎接口仍需要 null-terminated） */
    char key_buf[256] = {0};
    const char *key = NULL;
    if (argc >= 2) {
        key = token_to_key(tokens[1], key_buf, (int)sizeof(key_buf));
    }

    /* SET/MOD 需要创建 KVItem：value 用 create_kvitem_len（支持长度），
       expire 从 token 提取 */
    KVItem *item = NULL;
    if ((cmd == KVS_CMD_SET || cmd == KVS_CMD_MOD) && argc >= 3) {
        item = create_kvitem_len(tokens[2].ptr, tokens[2].len);
        if (item && argc >= 4) {
            char expire_buf[32];
            token_to_key(tokens[3], expire_buf, (int)sizeof(expire_buf));
            item->expire = get_time_now() + (long long)atoi(expire_buf) * 1000;
        }
    }

    int ret = 0;
    char temp_buf[64]; /* 仅用于 sprintf RESP 头 */

    switch (cmd) {

    case KVS_CMD_SET:
        if      (kvs_config.engine == ENGINE_ARRAY)     ret = kvs_array_set(&global_array, key, item);
        else if (kvs_config.engine == ENGINE_RBTREE)    ret = kvs_rbtree_set(&global_rbtree, key, item);
        else if (kvs_config.engine == ENGINE_HASH)      ret = kvs_hash_set(&global_hash, key, item);
        else if (kvs_config.engine == ENGINE_SKIPTABLE) ret = kvs_skiptable_set(&global_skiptable, key, item);
        if (ret < 0) {
            dynbuf_cat(wb, RESP_ERROR, RESP_ERROR_LEN);
        } else if (ret == 0) {
            dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        } else {
            dynbuf_cat(wb, RESP_EXIST, RESP_EXIST_LEN);
        }
        break;

    case KVS_CMD_GET: {
        KVItem *result = NULL;
        if      (kvs_config.engine == ENGINE_ARRAY)     result = kvs_array_get(&global_array, key);
        else if (kvs_config.engine == ENGINE_RBTREE)    result = kvs_rbtree_get(&global_rbtree, key);
        else if (kvs_config.engine == ENGINE_HASH)      result = kvs_hash_get(&global_hash, key);
        else if (kvs_config.engine == ENGINE_SKIPTABLE) result = kvs_skiptable_get(&global_skiptable, key);
        if (result == NULL) {
            dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);
        } else {
            int hdr_len = snprintf(temp_buf, sizeof(temp_buf), "$%zu\r\n", result->len);
            dynbuf_cat(wb, temp_buf, hdr_len);
            dynbuf_cat(wb, result->value, (int)result->len);
            dynbuf_cat(wb, "\r\n", 2);
        }
        break;
    }

    case KVS_CMD_DEL:
        if      (kvs_config.engine == ENGINE_ARRAY)     ret = kvs_array_del(&global_array, key);
        else if (kvs_config.engine == ENGINE_RBTREE)    ret = kvs_rbtree_del(&global_rbtree, key);
        else if (kvs_config.engine == ENGINE_HASH)      ret = kvs_hash_del(&global_hash, key);
        else if (kvs_config.engine == ENGINE_SKIPTABLE) ret = kvs_skiptable_del(&global_skiptable, key);
        if (ret < 0) {
            dynbuf_cat(wb, RESP_ERROR, RESP_ERROR_LEN);
        } else if (ret == 0) {
            dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        } else {
            dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);
        }
        break;

    case KVS_CMD_MOD:
        if      (kvs_config.engine == ENGINE_ARRAY)     ret = kvs_array_mod(&global_array, key, item);
        else if (kvs_config.engine == ENGINE_RBTREE)    ret = kvs_rbtree_mod(&global_rbtree, key, item);
        else if (kvs_config.engine == ENGINE_HASH)      ret = kvs_hash_mod(&global_hash, key, item);
        else if (kvs_config.engine == ENGINE_SKIPTABLE) ret = kvs_skiptable_mod(&global_skiptable, key, item);
        if (ret < 0) {
            dynbuf_cat(wb, RESP_ERROR, RESP_ERROR_LEN);
        } else if (ret == 0) {
            dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        } else {
            dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);
        }
        break;

    case KVS_CMD_EXIST:
        if      (kvs_config.engine == ENGINE_ARRAY)     ret = kvs_array_exist(&global_array, key);
        else if (kvs_config.engine == ENGINE_RBTREE)    ret = kvs_rbtree_exist(&global_rbtree, key);
        else if (kvs_config.engine == ENGINE_HASH)      ret = kvs_hash_exist(&global_hash, key);
        else if (kvs_config.engine == ENGINE_SKIPTABLE) ret = kvs_skiptable_exist(&global_skiptable, key);
        if (ret == 0) {
            dynbuf_cat(wb, RESP_EXIST, RESP_EXIST_LEN);
        } else {
            dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);
        }
        break;

    case KVS_CMD_SAVE:
        rdb_save_async_uring();
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        break;

    case KVS_CMD_FLUSHALL:
        kvs_flush_all();
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        break;

    case KVS_CMD_PING:
        dynbuf_cat(wb, RESP_PONG, RESP_PONG_LEN);
        break;

    case KVS_CMD_ECHO:
        if (argc >= 2) {
            int hdr_len = snprintf(temp_buf, sizeof(temp_buf), "$%d\r\n", tokens[1].len);
            dynbuf_cat(wb, temp_buf, hdr_len);
            dynbuf_cat(wb, tokens[1].ptr, tokens[1].len);
            dynbuf_cat(wb, "\r\n", 2);
        } else {
            dynbuf_cat(wb, "-ERR wrong number of arguments for 'echo' command\r\n", 50);
        }
        break;

    default:
        dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
        break;
    }

    if (item) {
        free_kvitem(item);
    }

    return wb->len;
}

// ==================== 命令分发入口 ====================

int kvs_dispatch(kvs_token_t *tokens, int argc,
                 const char *msg_raw, int msg_len,
                 DynBuf *wb)
{
    if (tokens == NULL || argc <= 0 || wb == NULL)
        return -1;

    int pre_len = wb->len;
    kvs_filter_protocol_v2(tokens, argc, wb);

    /* 成功的写命令 → AOF 持久化 + eBPF 增量复制探针 */
    if (token_is_write_cmd(tokens[0])) {
        const char *resp = wb->buf + pre_len;
        int rlen = wb->len - pre_len;
        int cmd_ok = (rlen > 0 && resp[0] != '-' &&
                      !(rlen >= 5 && memcmp(resp, "$-1\r\n", 5) == 0));

        if (cmd_ok && ENABLE_AOF) aof_async_append(msg_raw, msg_len);
        if (cmd_ok) kvs_rdma_incr_probe(msg_raw, msg_len);
    }

    return wb->len;
}

// ==================== 协议入口（兼容旧接口） ====================

int kvs_protocol(char *msg, int length, DynBuf *write_buf) {
    if (msg == NULL || length <= 0 || write_buf == NULL)
        return -1;

    kvs_token_t tokens[KVS_MAX_TOKENS];
    int argc = 0;
    int cmd_len = resp_parse_slices(msg, length, tokens, KVS_MAX_TOKENS, &argc);
    if (cmd_len <= 0) {
        dynbuf_cat(write_buf, RESP_ERR_INV, RESP_ERR_INV_LEN);
        return -1;
    }

    return kvs_dispatch(tokens, argc, msg, length, write_buf);
}

// ==================== 管道批处理入口（兼容旧网络层接口） ====================
int kvs_protocol_batch(char *msg, int len, DynBuf *write_buf) {
    static int read_offset = 0;
    DynBuf read_buf;
    dynbuf_init(&read_buf, BUF_INIT_CAP);
    dynbuf_append(&read_buf, msg, len);
    dynbuf_clear(write_buf);
    kvs_drain_resp_commands_off(&read_buf, &read_offset, write_buf);
    dynbuf_free(&read_buf);
    return write_buf->len;
}

// ==================== 批量解析（零拷贝+Token Slice） ====================

int kvs_drain_resp_commands_off(DynBuf *rb, int *r_off, DynBuf *wb)
{
    if (!rb || !rb->buf || !r_off || !wb)
        return -1;

    int ncmd = 0;
    int off = *r_off;
    int remaining = rb->len;

    if (off < 0 || off >= rb->cap) {
        off = 0;
        remaining = 0;
        rb->len = 0;
        *r_off = 0;
        return 0;
    }

    kvs_token_t tokens[KVS_MAX_TOKENS];

    while (remaining > 0) {
        /* 跳过开头空白字符（\r, \n, ' '） */
        while (remaining > 0 &&
               (rb->buf[off] == '\r' || rb->buf[off] == '\n' || rb->buf[off] == ' ')) {
            off++;
            remaining--;
        }
        if (remaining <= 0) break;
        if (off < 0 || off >= rb->cap) break;

        int argc = 0;
        int cmd_len = resp_parse_slices(rb->buf + off, remaining,
                                        tokens, KVS_MAX_TOKENS, &argc);

        if (cmd_len > 0) {
            kvs_dispatch(tokens, argc, rb->buf + off, cmd_len, wb);
            off += cmd_len;
            remaining -= cmd_len;
            ncmd++;
        } else if (cmd_len == 0) {
            /* 数据不完整，等待更多网络数据 */
            break;
        } else {
            /* 协议错误：跳过垃圾数据，寻找下一个 '*' */
            int skip = 1;
            while (skip < remaining && (off + skip) < rb->cap &&
                   rb->buf[off + skip] != '*') {
                skip++;
            }
            if (skip >= remaining || (off + skip) >= rb->cap) {
                skip = remaining;
            }
            off += skip;
            remaining -= skip;
        }
    }

    *r_off = off;
    rb->len = remaining;
    return ncmd;
}

// ==================== 引擎管理 ====================

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

void kvs_flush_all() {
    if      (kvs_config.engine == ENGINE_ARRAY)     kvs_array_clear(&global_array);
    else if (kvs_config.engine == ENGINE_HASH)      kvs_hash_clear(&global_hash);
    else if (kvs_config.engine == ENGINE_RBTREE)    kvs_rbtree_clear(&global_rbtree);
    else if (kvs_config.engine == ENGINE_SKIPTABLE) kvs_skiptable_clear(&global_skiptable);
    mallctl("arenas.purge", NULL, NULL, NULL, 0);
}

void engine_reload(void) {
    if (ENABLE_AOF && access(persist_path(AOF_REL), F_OK) == 0) {
        printf("Loading AOF (mmap)...\n");
        aof_reload_mmap();
    } else if (ENABLE_RDB && access(persist_path(RDB_REL), F_OK) == 0) {
        printf("Loading RDB (mmap)...\n");
        rdb_load_mmap();
    } else {
        printf("Engine reload: no persistence file\n");
    }
}

/* ==================== kvs_filter_protocol 旧签名兼容层 ====================
   仅用于 test/testcase.c 的测试用例。
   内部将 char** tokens 适配为 token slice 再调用 v2。 */
int kvs_filter_protocol(char **tokens, int count, DynBuf *wb) {
    if (tokens == NULL || tokens[0] == NULL || count <= 0 || wb == NULL)
        return -1;

    /* 将旧的 char** tokens 转为 kvs_token_t 数组（栈上转换） */
    kvs_token_t tarray[KVS_MAX_TOKENS];
    int argc = 0;
    for (int i = 0; i < count && tokens[i] != NULL; i++) {
        tarray[i].ptr = tokens[i];
        tarray[i].len = (int)strlen(tokens[i]);
        argc++;
    }

    return kvs_filter_protocol_v2(tarray, argc, wb);
}

int main(int argc, char *argv[]) {
    signal(SIGPIPE, SIG_IGN);
    persist_set_basedir(argv[0]);
    const char *conf_path = (argc >= 2) ? argv[1] : "conf/kvstore.conf";
    config_load(conf_path);
    config_print();

    init_kvengine();
    engine_reload();
    if (ENABLE_AOF) aof_async_init();

    if (kvs_config.rdma_sync_enabled) {
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
