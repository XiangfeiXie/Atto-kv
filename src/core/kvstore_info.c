/* kvstore_info.c — INFO 命令处理器
 *
 * 职责：按客户端请求的 section（server/clients/memory/persistence/stats/
 * replication/cpu/keyspace）输出对应统计信息；未指定 section 时默认全出。
 * 统计值来自单线程事件循环维护的全局计数（见 kvstore.c）。 */

#include "kvstore_internal.h"

#include <stdatomic.h>   /* C11 原子操作：连接/请求计数用 memory_order_relaxed */
#include <stdio.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/utsname.h>
#ifndef USE_JEMALLOC
#include <malloc.h>   /* mallinfo2() — glibc live in-use chunk counter */
#endif

#include "persistence.h"

/* section 匹配：token 与名字长度一致且大小写不敏感相等（如 "STATS"/"stats"） */
static int section_match(kvs_token_t t, const char *name) {
    size_t n = strlen(name);
    if ((size_t)t.len != n) return 0;
    return strncasecmp(t.ptr, name, n) == 0;
}

/* INFO 命令入口：组装各 section 的文本块。
 * body[4096] 定长缓冲，APPEND 宏检测溢出并跳 truncated 收尾（截断不崩溃）。 */
dispatch_result_t handle_info(kvs_token_t *tokens, int argc, DynBuf *wb) {
    int want_server      = 1;
    int want_clients     = 1;
    int want_memory      = 1;
    int want_persistence = 1;
    int want_stats       = 1;
    int want_replication = 1;
    int want_cpu         = 1;
    int want_keyspace    = 1;

    if (argc >= 2) {
        kvs_token_t s = tokens[1];
        if (!section_match(s, "all") && !section_match(s, "everything") &&
            !section_match(s, "default")) {
            want_server      = section_match(s, "server");
            want_clients     = section_match(s, "clients");
            want_memory      = section_match(s, "memory");
            want_persistence = section_match(s, "persistence");
            want_stats       = section_match(s, "stats");
            want_replication = section_match(s, "replication");
            want_cpu         = section_match(s, "cpu");
            want_keyspace    = section_match(s, "keyspace");
        }
    }

    char body[4096];
    int  off = 0;
    /* 定长缓冲安全追加：snprintf 返回值判错/判满，超限跳 truncated 收尾 */
    #define APPEND(...) do {                                                 \
        int _n = snprintf(body + off, sizeof(body) - off, __VA_ARGS__);      \
        if (_n < 0 || _n >= (int)(sizeof(body) - off)) goto truncated;       \
        off += _n;                                                            \
    } while (0)

    if (want_server) {
        struct utsname un = {0};
        uname(&un);
        APPEND("# Server\r\n");
        APPEND("os:%s %s %s\r\n", un.sysname, un.release, un.machine);
        APPEND("process_id:%d\r\n", (int)getpid());
        APPEND("tcp_port:%d\r\n", kvs_config.port);
        APPEND("network_model:%s\r\n",
            (kvs_config.network_model == NET_PROACTOR) ? "proactor (io_uring)" :
            (kvs_config.network_model == NET_REACTOR)  ? "reactor (epoll)"     :
                                                         "ntyco (coroutine)");
        APPEND("uptime_in_seconds:%lld\r\n",
               (get_time_now_real() - g_kvs_start_time_ms) / 1000);
        APPEND("\r\n");
    }

    if (want_clients) {
        APPEND("# Clients\r\n");
        APPEND("connected_clients:%d\r\n",
               atomic_load_explicit(&g_kvs_connected_clients,
                                    memory_order_relaxed));
        APPEND("maxclients:%d\r\n",
               __atomic_load_n(&kvs_config.maxclients, __ATOMIC_RELAXED));
        APPEND("\r\n");
    }

    /* 已分配内存：USE_JEMALLOC 走 mallctl("stats.allocated")，否则 glibc mallinfo2 */
    if (want_memory) {
        size_t allocated = 0;
        const char *allocator = "libc";
#ifdef USE_JEMALLOC
        size_t sz = sizeof(allocated);
        uint64_t epoch = 1;
        size_t esz = sizeof(epoch);
        mallctl("epoch", &epoch, &esz, &epoch, esz);
        if (mallctl("stats.allocated", &allocated, &sz, NULL, 0) != 0)
            allocated = 0;
        allocator = "jemalloc";
#else
        struct mallinfo2 mi = mallinfo2();
        allocated = (size_t)(mi.uordblks + mi.hblkhd);
#endif
        APPEND("# Memory\r\n");
        APPEND("used_memory:%zu\r\n", allocated);
        APPEND("used_memory_human:%.2fM\r\n",
               (double)allocated / (1024.0 * 1024.0));
        APPEND("mem_allocator:%s\r\n", allocator);
        APPEND("\r\n");
    }

    if (want_persistence) {
        APPEND("# Persistence\r\n");
        APPEND("aof_enabled:%d\r\n", ENABLE_AOF ? 1 : 0);
        APPEND("rdb_enabled:%d\r\n", ENABLE_RDB ? 1 : 0);
        APPEND("loading:%d\r\n", g_in_reload ? 1 : 0);
        APPEND("\r\n");
    }

    /* 统计计数：单线程内普通变量直接读；跨文件共享的连接/过期计数是 _Atomic */
    if (want_stats) {
        APPEND("# Stats\r\n");
        APPEND("total_connections_received:%llu\r\n",
               (unsigned long long)atomic_load_explicit(
                   &g_kvs_total_connections, memory_order_relaxed));
        APPEND("total_commands_processed:%llu\r\n",
               (unsigned long long)g_kvs_total_commands);
        APPEND("expired_keys:%llu\r\n",
               (unsigned long long)atomic_load_explicit(
                   kvs_expired_keys_ptr(), memory_order_relaxed));
        APPEND("\r\n");
    }

    if (want_replication) {
        const char *role =
            (kvs_config.repl_role == REPL_ROLE_MASTER) ? "master" :
            (kvs_config.repl_role == REPL_ROLE_SLAVE)  ? "slave"  : "off";
        APPEND("# Replication\r\n");
        APPEND("role:%s\r\n", role);
        APPEND("fullsync_transport:%s\r\n", kvs_config.fullsync_transport);
        APPEND("incr_sync:%s\r\n", kvs_config.incr_sync ? "on" : "off");
        if (kvs_config.repl_role == REPL_ROLE_MASTER) {
            APPEND("master_port:%d\r\n", kvs_config.master_port);
            APPEND("fullsync:%s:%d\r\n",
                   kvs_config.fullsync_listen_ip, kvs_config.fullsync_port);
        } else if (kvs_config.repl_role == REPL_ROLE_SLAVE) {
            APPEND("master_host:%s\r\n", kvs_config.fullsync_master_ip);
            APPEND("master_port:%d\r\n", kvs_config.master_port);
            APPEND("fullsync_port:%d\r\n", kvs_config.fullsync_port);
            APPEND("slave_repl_offset:%zu\r\n",
                   repl_incr_slave_offset());
        }
        APPEND("\r\n");
    }

    if (want_cpu) {
        struct rusage ru;
        if (getrusage(RUSAGE_SELF, &ru) == 0) {
            double sys_s = ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1e6;
            double usr_s = ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1e6;
            APPEND("# CPU\r\n");
            APPEND("used_cpu_sys:%.6f\r\n", sys_s);
            APPEND("used_cpu_user:%.6f\r\n", usr_s);
            APPEND("\r\n");
        }
    }

    if (want_keyspace) {
        APPEND("# Keyspace\r\n");
        APPEND("db0:keys=%u\r\n", (unsigned)dict_count(&g_dict));
        APPEND("\r\n");
    }

truncated:
    {
        char hdr[32];
        int hl = snprintf(hdr, sizeof(hdr), "$%d\r\n", off);
        dynbuf_cat(wb, hdr, hl);
        dynbuf_cat(wb, body, off);
        dynbuf_lit(wb, "\r\n");
    }
    #undef APPEND
    return DISPATCH_OK;
}
