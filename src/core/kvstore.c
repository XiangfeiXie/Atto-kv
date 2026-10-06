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
#include <sys/wait.h>
#include <fcntl.h>
#include <errno.h>
#include <ctype.h>
#ifndef USE_JEMALLOC
#include <malloc.h>   /* mallinfo2: glibc 构建的 used_memory 探测 */
#endif

#include "repl.h"
#include "persistence.h"
#include "kv_pool.h"      /* kv_pool_init / kv_pool_shutdown：数据域记录内存池 */

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
    char buf[32];
    if (t.len <= 0 || t.len >= (int)sizeof(buf)) return -1;
    memcpy(buf, t.ptr, (size_t)t.len);
    buf[t.len] = '\0';

    /* 拒绝非数字（含前导空白）：strtoll 会跳过空白，原始语义不跳 */
    if (buf[0] != '+' && buf[0] != '-' &&
        !(buf[0] >= '0' && buf[0] <= '9'))
        return -1;

    errno = 0;
    char *end = NULL;
    long long v = strtoll(buf, &end, 10);
    if (errno == ERANGE || end != buf + t.len)
        return -1;               /* 溢出或尾部有垃圾字符 */
    *out = v;
    return 0;
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

/* 最常用整数值的预置响应（DEL/EXPIRE 返回 0/1，直接拼接避免格式化开销） */
static const char RESP_INT_0[] = ":0\r\n";
static const char RESP_INT_1[] = ":1\r\n";

/**
 * 回复整数（RESP 整数格式）
 * 0/1 走预置串，其他手写（单次 reserve 一次写满，含负数与 INT64_MIN）
 * @param wb 写缓冲
 * @param v 整数值
 */
void reply_integer(DynBuf *wb, long long v) {
    /* 最常见值直接拼接 */
    if (v == 0) { dynbuf_cat(wb, RESP_INT_0, sizeof(RESP_INT_0) - 1); return; }
    if (v == 1) { dynbuf_cat(wb, RESP_INT_1, sizeof(RESP_INT_1) - 1); return; }

    /* 其他值手写：单次 reserve 一次写满 */
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
    /* 先定引擎，再动任何一条记录：dict_init 之后到进程结束，所有 dict_*
     * 都路由到这一个引擎（conf 里 engine hash|array|rbtree|skiplist）。 */
    if (kv_engine_select(kvs_config.storage_engine) < 0) {
        fprintf(stderr, "[kvstore] 未知存储引擎 '%s'，回退 hash\n",
                kvs_config.storage_engine);
        kv_engine_select("hash");
    }
    dict_init();
    /* 数据域 kv 节点分配接入自研内存池：必须在 engine_reload 加载数据
     * 之前初始化（否则首轮节点已走底座）。池主线程专属；配置关或初始化
     * 失败都回退底座 malloc，不影响正确性。
     * 四个引擎各自管自己的节点分配，但都向同一个池取块。 */
    if (kvs_config.kv_pool_enabled && kv_pool_init() < 0)
        fprintf(stderr, "[kvstore] kv_pool 创建失败，entry 分配回退底座 malloc\n");
    printf("Using string storage engine: %s\n", kv_engine_name());
    return 0;
}

/**
 * 销毁 KV 引擎
 *
 * 退出前打印两次内存快照：进程退出后 /proc 条目消失，(used_memory, VmRSS)
 * 的"关闭时"读数只能由进程自报 —— 这是回收情况的唯一可读时点。
 * 两次分别对应：数据仍在（≈峰值）与 全清归还后（entry/桶/超块均已释放）。
 */
void dest_kvengine(void) {
    mem_report("shutdown/pre-release");
    kvs_flush_all();
    mem_report("shutdown/post-release");
    /* 池须在记录全部归还之后再销毁 —— 上面 kvs_flush_all 已把 dict 全清
     * （各引擎的 clear 会调 kv_pool_release_all）。未启用时为 no-op。 */
    kv_pool_shutdown();
}

/**
 * 优雅停机的收尾（三个网络引擎共用）
 *
 * reactor / proactor 在 main 的第 6 段调用：它们的主循环读到 g_kvs_stop 就
 * 返回，控制权自然落回 main。ntyco 没有这条路 —— 它进的是 NtyCo 的
 * nty_schedule_run()，那个循环要四个协程队列全空才返回，而监听协程与 ticker
 * 协程常驻 ⇒ 永不返回。所以 ntyco 由 ticker 协程直接调本函数再 exit(0)。
 *
 * 两条路径共用这一份，是为了让"停机做了什么"只有一个答案：目前只有 AOF 落盘
 * + 引擎回收，不含 SAVE（RDB 只由显式 SAVE/BGSAVE 产生，三引擎一致）。
 *
 * 本函数不返回前的调用者须自行决定退出方式（main 返回 / ticker 里 exit）。
 */
void kvs_shutdown(void) {
    kvs_aof_persist_stop();
    dest_kvengine();
}

/**
 * 清空所有数据
 * 两个 allocator 各补一句「立即归还」把 free 掉的内存还给 OS，语义对称：
 *   jemalloc  mallctl("arenas.purge")   —— 但要先 flush 线程 tcache，否则无物可清
 *   glibc     malloc_trim(0)            —— 走完全部 free list 把页还给内核
 * 不调的话 free 的内存只留在各自的空闲链/arena 里，RSS 不回落。
 */
void kvs_flush_all(void) {
    dict_clear();
#ifdef USE_JEMALLOC
    /* 先 flush 线程 tcache: free 的对象先进 tcache, 不清它 arenas.purge 无物可清;
     * 再 purge 脏页把内存还给 OS (实测踩坑) */
    mallctl("thread.tcache.flush", NULL, NULL, NULL, 0);
    mallctl("arenas.purge", NULL, NULL, NULL, 0);
#else
    /* glibc 没有 purge，malloc_trim(0) 是它对等的接口。注意环境变量
     * MALLOC_TRIM_THRESHOLD_ 在这条路径上是多余的：实测带不带它 trim(0) 结果
     * 逐字节相同，因为 trim(0) 自己就会扫完整个 free list */
    malloc_trim(0);
#endif
}

#ifdef USE_JEMALLOC
/* malloc_stats_print 输出捕获缓冲（INFO 低频调用，静态缓冲足够）
 * opts="mdga" 只打印全局汇总行（约 2KB），首行即 "Allocated:" 实时总量 */
static char   g_je_stats_buf[8192];
static size_t g_je_stats_len;

static void je_stats_cb(void *opaque, const char *s) {
    (void)opaque;
    size_t n = strlen(s);
    if (g_je_stats_len + n < sizeof(g_je_stats_buf))
        memcpy(g_je_stats_buf + g_je_stats_len, s, n);
    g_je_stats_len += n;
}
#endif

/**
 * 获取 allocator 自报的存活分配字节数（INFO used_memory 用）
 * jemalloc: malloc_stats_print 汇总行的 Allocated = 当前存活分配，Redis 同款语义
 * glibc:    mallinfo2 的 uordblks（堆内已用）+ hblkhd（mmap 已用）近似
 *
 * 实测踩坑：本系统 jemalloc(5.3.0) 的 mallctl("stats.allocated")（命名和 mib
 * 两种路径都试过）只统计大分配，海量小分配（dict 条目这类）不反映进去，值恒定；
 * 只有 malloc_stats_print 的 "Allocated:" 汇总行才含全部存活分配（含小分配）。
 */
uint64_t get_used_mem(void) {
#ifdef USE_JEMALLOC
    g_je_stats_len = 0;
    malloc_stats_print(je_stats_cb, NULL, "mdga");
    const char *p = strstr(g_je_stats_buf, "Allocated:");
    if (!p) return 0;
    return (uint64_t)atoll(p + 10);   /* "Allocated:" 后是数值 */
#else
    struct mallinfo2 mi = mallinfo2();
    return (uint64_t)mi.uordblks + (uint64_t)mi.hblkhd;
#endif
}

/**
 * 打印内存快照（used_memory + VmRSS）
 *
 * 启动 / 关闭等关键时点调用：退出后 /proc 消失，进程内自报是"关闭时内存"
 * 的唯一可读来源。used_memory 口径见 get_used_mem（jemalloc=Allocated 汇总行，
 * glibc=uordblks+hblkhd，两者都含池的超块 —— 超块是底座的存活分配）。
 *
 * @param tag 时点标签，直接进日志，便于脚本按标签抓取
 */
void mem_report(const char *tag) {
    uint64_t used = get_used_mem();
    long     rss  = 0;   /* VmRSS，读 /proc/self/status（非 Linux 则为 0） */
    FILE *fp = fopen("/proc/self/status", "r");
    if (fp) {
        char line[256];
        while (fgets(line, sizeof(line), fp)) {
            if (strncmp(line, "VmRSS:", 6) == 0) {
                sscanf(line + 6, "%ld", &rss);
                break;
            }
        }
        fclose(fp);
    }
    fprintf(stdout, "[MEM] %s used=%llu rss=%ld KB\n",
            tag, (unsigned long long)used, rss);
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

/**
 * 子进程回收信号处理器（SIGCHLD）
 * 用 waitpid(-1) 回收所有已退出子进程（BGSAVE / 全量传输），防止僵尸
 * 无派发框架：各模块在各自入口 waitpid 探活自己的 PID（ECHILD = 已被本处理器回收）
 */
static void sig_child_reap(int signo) {
    (void)signo;
    while (waitpid(-1, NULL, WNOHANG) > 0) { }
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

    /* 子进程回收：waitpid(-1) 回收所有已退出子进程，防僵尸
     * 无派发框架：各模块入口 waitpid 探活自己的 PID（ECHILD = 已回收） */
    struct sigaction sa_chld;
    sa_chld.sa_handler = sig_child_reap;
    sa_chld.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigemptyset(&sa_chld.sa_mask);
    sigaction(SIGCHLD, &sa_chld, NULL);
    
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

    /* （原 network=ntyco + repl 组合的 fail-fast 已删：三引擎现均赋值
     *   repl_engine_*，ntyco 侧见 src/net/ntyco.c 的 "repl 接线" 段） */

    /*-------------------------------------------------------------------------
     *  3. 引擎初始化
     *-------------------------------------------------------------------------*/
    init_kvengine();
    /* 启动内存快照：距空库基线最近的时点（此时尚未接受任何连接/数据，
     * 已含池实例与分配器 arena 的固有开销） */
    mem_report("startup");
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
     *  角色接线（控制面入口）：
     *    MASTER：
     *     启动   repl_backlog_init()            1MB 积压缓冲（仅 incr on）
     *      运行时 连接默认入口一律 kvs_drain_rbuf_commands；PSYNC 作为控制命令
     *             在命令分发层（kvs_dispatch 的 PSYNC 分支，kvstore_dispatch.c）
     *             接管 → +CONTINUE 流式 / +FULLRESYNC fork 数据面子进程
     *             repl_tick 无动作；repl_on_close 清理 streaming 从库
     *    SLAVE：
     *      启动   repl_incr_slave_start()        记录 Master 地址，状态置 IDLE
     *      运行时 repl_tick → repl_incr_slave_tick 统一推进
     *             （连接 / 重连 / +FULLRESYNC 数据面全量交接）
     *             Master 链路的 client_handler 由 repl 内部挂载
     *             repl_on_close → 断线安排重连
     *-------------------------------------------------------------------------*/
    if (kvs_config.repl_role == REPL_ROLE_MASTER) {
        /* Master：全量同步在 PSYNC 握手判定 +FULLRESYNC 时 fork 传输子进程
         * （repl_fullsync_master_trigger），无需启动时初始化 */
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
     *  三引擎全连接读缓冲统一 rbuf 节点队列；数据入口统一注入同一签名
     *  kvs_rbuf_drain_fn（PSYNC 由命令分发层拦截，无需按角色选 handler）
     *-------------------------------------------------------------------------*/
    /* 命令解析入口按引擎注入，reactor/proactor/ntyco 一律 kvs_drain_rbuf_commands。
     * master 的 PSYNC 握手作为控制命令经 dispatch 的 PSYNC 分支接管
     * （gate = repl_engine_append != NULL && role==MASTER，见 kvstore_dispatch.c）。
     * 三引擎都在自己的 start 里赋值 repl_engine_*（ntyco 见 src/net/ntyco.c），
     * 故 repl_role 非 off 时门控一律可用 */
    if (kvs_config.network_model == NET_REACTOR) {
        reactor_start(kvs_config.port, kvs_drain_rbuf_commands);
    } else if (kvs_config.network_model == NET_PROACTOR) {
        proactor_start(kvs_config.port, kvs_drain_rbuf_commands);
    } else if (kvs_config.network_model == NET_NTYCO) {
        ntyco_start(kvs_config.port, kvs_drain_rbuf_commands);
    }

    /*-------------------------------------------------------------------------
     *  6. 清理
     *
     *  reactor / proactor：主循环看到 g_kvs_stop 返回 → 落到这里收尾。
     *  ntyco：调度器不返回（见 kvs_shutdown 的说明），由 ticker 协程自己调
     *  kvs_shutdown —— 那条路径到不了本行，两者共用同一段清理逻辑。
     *-------------------------------------------------------------------------*/
    kvs_shutdown();
    return 0;
}