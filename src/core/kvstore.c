/*=============================================================================
 *  kvstore.c — KV 存储核心模块
 *  
 *  功能：
 *  1. 全局状态管理（统计计数器、缓存时间）
 *  2. 工具函数（整数转换、RESP 响应构建）
 *  3. 引擎生命周期（初始化、重载、销毁）
 *  4. 信号处理和程序入口（main）
 *  5. 持久化和复制初始化协调
 *============================================================================*/

#include "kvstore_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <signal.h>
#include <fcntl.h>
#include <ctype.h>

#include "repl.h"
#include "persistence.h"

/*=============================================================================
 *  全局状态
 *============================================================================*/

int g_in_reload = 0;                              /* 是否在数据重载中（抑制日志/复制） */

long long        g_kvs_start_time_ms      = 0;    /* 启动时间戳（毫秒） */
volatile sig_atomic_t g_kvs_stop = 0;             /* 停止标志（信号触发） */

/*=============================================================================
 *  缓存时间
 *  每轮事件循环更新一次，减少 gettimeofday 系统调用
 *============================================================================*/

static _Atomic long long g_cached_ms;             /* 缓存的时间戳（毫秒） */

/**
 * 获取实时时间（调用系统调用）
 * @return 当前时间戳（毫秒）
 */
long long get_time_now_real(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000LL + tv.tv_usec / 1000;
}

/**
 * 获取缓存时间（不调用系统调用）
 * @return 缓存的时间戳（毫秒）
 */
long long get_time_now(void) {
    return atomic_load_explicit(&g_cached_ms, memory_order_relaxed);
}

/**
 * 更新缓存时间
 * 每轮事件循环调用一次
 * 同时触发：
 * 1. AOF 周期性操作（刷新缓冲区）
 * 2. 主动过期清理（如果启用）
 */
void update_cached_time(void) {
    atomic_store_explicit(&g_cached_ms, get_time_now_real(), memory_order_relaxed);
    if (ENABLE_AOF) {
        kvs_aof_periodic();       /* AOF 定期刷新 */
    }
    if (ENABLE_EXPIRE) active_expire_cycle();  /* 主动过期清理 */
}

/*=============================================================================
 *  工具函数
 *============================================================================*/

/**
 * 将 token 转换为 long long 整数
 * @param t 输入 token
 * @param out 输出整数值
 * @return 0 成功，-1 失败（非数字或溢出）
 */
int token_to_ll(kvs_token_t t, long long *out) {
    if (t.len <= 0 || t.len > 20) return -1;
    
    int i = 0, neg = 0;
    if (t.ptr[0] == '-') { 
        neg = 1; 
        i = 1; 
        if (t.len == 1) return -1;   /* 单独的 '-' 无效 */
    } else if (t.ptr[0] == '+') { 
        i = 1; 
        if (t.len == 1) return -1;   /* 单独的 '+' 无效 */
    }
    
    unsigned long long v = 0;
    for (; i < t.len; i++) {
        unsigned c = (unsigned char)t.ptr[i] - (unsigned)'0';
        if (c > 9) return -1;        /* 非数字字符 */
        
        /* 溢出检查 */
        if (v > (unsigned long long)9223372036854775807ULL / 10) return -1;
        v = v * 10 + c;
        if (!neg && v > (unsigned long long)9223372036854775807ULL) return -1;
        if (neg && v > (unsigned long long)9223372036854775808ULL) return -1;
    }
    *out = neg ? -(long long)v : (long long)v;
    return 0;
}

/*=============================================================================
 *  共享响应缓存
 *  预分配 0-255 的整数响应，避免重复构造
 *============================================================================*/

static const char *g_int_resp[256];   /* 响应字符串指针 */
static int         g_int_resp_len[256]; /* 响应字符串长度 */
static char        g_int_buf[256][16];  /* 响应字符串存储 */

/**
 * 初始化共享响应缓存
 * 预生成 ":0\r\n" 到 ":255\r\n"
 */
static void init_shared_responses(void) {
    for (int i = 0; i < 256; i++) {
        int n = snprintf(g_int_buf[i], sizeof(g_int_buf[i]), ":%d\r\n", i);
        g_int_resp[i]     = g_int_buf[i];
        g_int_resp_len[i] = n;
    }
}

/**
 * 格式化 RESP 头部（整数或 Bulk 长度）
 * 手写替代 snprintf，避免 varargs + 格式解析开销
 * @param dst 目标缓冲区
 * @param sig 类型标识（':' 整数，'$' Bulk）
 * @param v 值
 * @return 写入的字节数
 */
static inline int fmt_resp_header(char *dst, char sig, int v) {
    char *p = dst;
    *p++ = sig;
    
    if (v == 0) {
        *p++ = '0';
    } else {
        char tmp[10];
        int n = 0;
        while (v > 0) { tmp[n++] = (char)('0' + (v % 10)); v /= 10; }
        while (n > 0) *p++ = tmp[--n];
    }
    *p++ = '\r';
    *p++ = '\n';
    return (int)(p - dst);
}

/**
 * 回复整数（RESP 整数格式）
 * 0-255 走缓存，其他手写
 * @param wb 写缓冲
 * @param v 整数值
 */
void reply_integer(DynBuf *wb, long long v) {
    /* 小值走缓存 */
    if (v >= 0 && v <= 255) {
        dynbuf_cat(wb, g_int_resp[v], g_int_resp_len[v]);
        return;
    }
    
    /* 大值手写：单次 reserve 一次写满 */
    char buf[32], *p = buf;
    *p++ = ':';
    
    unsigned long long uv;
    if (v < 0) {
        *p++ = '-';
        uv = (unsigned long long)(-(v + 1)) + 1ULL;  /* 规避 INT64_MIN 溢出 */
    } else {
        uv = (unsigned long long)v;
    }
    
    if (uv == 0) {
        *p++ = '0';
    } else {
        char tmp[20];
        int n = 0;
        while (uv > 0) { tmp[n++] = (char)('0' + (uv % 10)); uv /= 10; }
        while (n > 0) *p++ = tmp[--n];
    }
    *p++ = '\r';
    *p++ = '\n';
    
    int n = (int)(p - buf);
    if (dynbuf_reserve(wb, wb->len + n) < 0) return;
    memcpy(wb->buf + wb->len, buf, (size_t)n);
    wb->len += n;
}

/**
 * 回复 Bulk String（RESP 批量字符串格式）
 * 单次 reserve 一次写满（头 + 数据 + CRLF）
 * 省去 3 次 dynbuf_cat 的函数调用与各自容量检查
 * @param wb 写缓冲
 * @param s 字符串指针
 * @param len 字符串长度
 */
void reply_bulk(DynBuf *wb, const char *s, int len) {
    char hdr[16];
    int  hl = fmt_resp_header(hdr, '$', len);

    /* 单次 reserve：头 + 数据 + CRLF */
    if (dynbuf_reserve(wb, wb->len + hl + len + 2) < 0) return;
    
    char *dst = wb->buf + wb->len;
    memcpy(dst, hdr, (size_t)hl); dst += hl;
    if (len > 0) { memcpy(dst, s, (size_t)len); dst += len; }
    memcpy(dst, "\r\n", 2);
    wb->len += hl + len + 2;
}

/*=============================================================================
 *  引擎生命周期
 *============================================================================*/

/**
 * 初始化 KV 引擎
 * @return 0 成功
 */
int init_kvengine(void) {
    dict_init(&g_dict);
    init_shared_responses();
    printf("Using string storage engine (single global dict)\n");
    return 0;
}

/**
 * 销毁 KV 引擎
 */
void dest_kvengine(void) {
    kvs_flush_all();
    dict_destroy(&g_dict);
}

/**
 * 清空所有数据
 * 使用 jemalloc 时额外 purge 释放内存
 */
void kvs_flush_all(void) {
    dict_clear(&g_dict);
#ifdef USE_JEMALLOC
    mallctl("arenas.purge", NULL, NULL, NULL, 0);  /* 强制释放内存给 OS */
#endif
}

/**
 * 重载数据（启动时调用）
 * 优先级：AOF > RDB
 * 使用 mmap 快速加载
 */
void engine_reload(void) {
    if (ENABLE_AOF) {
        if (access(persist_path(AOF_REL), F_OK) == 0) {
            printf("Loading AOF (mmap)...\n");
            aof_reload_mmap();
            return;
        }
    }
    if (ENABLE_RDB && access(persist_path(RDB_REL), F_OK) == 0) {
        printf("Loading RDB (mmap)...\n");
        rdb_load_mmap();
    } else {
        printf("Engine reload: no persistence file\n");
    }
}

/*=============================================================================
 *  信号处理
 *============================================================================*/

/**
 * 关闭信号处理器
 * 触发优雅关闭
 */
static void sig_shutdown(int signo) {
    (void)signo;
    g_kvs_stop = 1;
}

/*=============================================================================
 *  程序入口
 *============================================================================*/

int main(int argc, char *argv[]) {
    /*-------------------------------------------------------------------------
     *  1. 信号设置
     *-------------------------------------------------------------------------*/
    signal(SIGPIPE, SIG_IGN);  /* 忽略 SIGPIPE（写已关闭 socket 会返回 EPIPE） */
    
    struct sigaction sa;
    sa.sa_handler = sig_shutdown;
    sa.sa_flags   = 0;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);
    
    setlinebuf(stdout);
    setlinebuf(stderr);

    /*-------------------------------------------------------------------------
     *  2. 配置加载
     *-------------------------------------------------------------------------*/
    g_kvs_start_time_ms = get_time_now_real();
    persist_set_basedir(argv[0]);
    const char *conf_path = (argc >= 2) ? argv[1] : "conf/kvstore.conf";
    config_load(conf_path);
    config_print();

    /*-------------------------------------------------------------------------
     *  3. 引擎初始化
     *-------------------------------------------------------------------------*/
    init_kvengine();
    engine_reload();  /* 从持久化文件恢复数据 */
    
    if (ENABLE_AOF) {
        kvs_aof_set_fsync_policy(kvs_config.aof_fsync_policy);
        kvs_aof_persist_start();  /* 启动 AOF 持久化 */
    }

    /*-------------------------------------------------------------------------
     *  4. 复制初始化
     *  repl_role off → 不 fork、不建 backlog
     *  repl_on_close / repl_tick 为直接函数，内部按 role 判断，无需布线
     *
     *  角色接线（控制面入口即引擎三个回调：client_handler / tick / on_close）：
     *    MASTER：
     *      启动   repl_fullsync_master_start()   安装 SIGCHLD 回收（数据面子进程）
     *             repl_backlog_init()            1MB 积压缓冲（仅 incr on）
     *      运行时 client_handler 注入 repl_incr_master_client_handler（PSYNC 探测）：
     *             PSYNC → +CONTINUE 流式 / +FULLRESYNC fork 数据面子进程
     *             repl_tick 无动作；repl_on_close 清理 streaming 从库
     *    SLAVE：
     *      启动   repl_incr_slave_start()        记录 Master 地址，状态置 IDLE
     *      运行时 repl_tick → repl_incr_slave_tick 统一推进
     *             （连接 / 重连 / +FULLRESYNC 数据面全量交接）
     *             Master 链路的 client_handler 由 repl 内部挂载
     *             repl_on_close → 断线安排重连
     *-------------------------------------------------------------------------*/
    if (kvs_config.repl_role == REPL_ROLE_MASTER) {
        /* Master：启动全量同步服务（PSYNC 握手判定 +FULLRESYNC 时 fork 传输） */
        repl_fullsync_master_start();
        if (kvs_config.incr_sync)
            repl_backlog_init();  /* 增量关闭时无 backlog/无传播 */
    } else if (kvs_config.repl_role == REPL_ROLE_SLAVE) {
        /* Slave：非阻塞连接 Master
         * 首次全量（建立快照）+ 连接 + PSYNC
         * 由 repl_incr_slave_tick（引擎主循环每轮调用）统一推进
         * incr_sync off → tick 只做一次全量便结束 */
        repl_incr_slave_start(kvs_config.fullsync_master_ip, kvs_config.master_port);
    }

    /*-------------------------------------------------------------------------
     *  5. 网络引擎选择
     *  命令解析入口注入：
     *  - Master：用 repl 的 client handler 包一层（PSYNC 探测；
     *    incr off 时无 backlog → 一律回 +FULLRESYNC）
     *  - 其他：普通 RESP 解析器
     *-------------------------------------------------------------------------*/
    kvs_drain_fn handler = kvs_drain_resp_commands_off;
    if (kvs_config.repl_role == REPL_ROLE_MASTER)
        handler = repl_incr_master_client_handler;

    if (kvs_config.network_model == NET_REACTOR)
        reactor_start(kvs_config.port, handler);
    else if (kvs_config.network_model == NET_PROACTOR)
        proactor_start(kvs_config.port, handler);
    else if (kvs_config.network_model == NET_NTYCO)
        ntyco_start(kvs_config.port, kvs_protocol_batch);

    /*-------------------------------------------------------------------------
     *  6. 清理
     *-------------------------------------------------------------------------*/
    kvs_aof_persist_stop();
    dest_kvengine();
    return 0;
}