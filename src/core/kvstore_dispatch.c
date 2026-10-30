/*=============================================================================
 *  kvstore_dispatch.c — RESP 命令分发器
 *  功能：解析 RESP 协议命令，执行 K/V 操作，生成响应
 *  设计原则：热路径优先（GET/SET），命令名不区分大小写
 *============================================================================*/

#include "kvstore_internal.h"
#include "kvstore_vector.h"   /* 向量命令：VSET / VGET / VDEL / VSEARCH */
#include "repl.h"
#include "persistence.h"

#include <stdio.h>
#include <string.h>

/*=============================================================================
 *  写效果收口（journal = AOF 日志 + 增量复制）
 *
 *  由各读缓冲 drain 在 kvs_dispatch 返回 journaled=1（命令已产生需要落盘/
 *  传播的写效果）后调用：把整条原始命令喂给 AOF 与增量复制消费方。
 *  各消费方的配置门（ENABLE_AOF / master & incr）收拢在此处一次判定，调用方
 *  无需预判"这是不是写命令"——效果判定以 kvs_dispatch 的 journaled 回传为准，
 *  单一权威，消除了旧"命令名谓词须与 dispatch 分支保持同步"的漏判风险。
 *  g_in_reload（slave 回放 / 启动重载）在此统一短路：重载期不产生新日志/传播。
 *  拷贝粒度：整条原始命令（AOF 块 / backlog 环都只消费整命令字节，不看 token）。
 *  超限 / persist 未启动由 kvs_aof_persist_append_raw 内部拒绝，命令本身已照常
 *  执行（fail-soft，与消费侧拒绝同级）。
 *
 *  dispatch 不背数据指针/生命周期：整命令字节由 drain 在 consume 前持有并传入，
 *  本函数只负责同步 memcpy 到各目标，不滞留指针。
 *
 * @param cmd 整条 RESP 原始命令
 * @param len 命令长度
 */
void kvs_journal_effect(const char *cmd, int len) {
    if (!cmd || len <= 0 || g_in_reload) return;

    if (ENABLE_AOF) kvs_aof_persist_append_raw(cmd, (uint32_t)len);  /* AOF 块 */

    /* 只有 master 且增量复制开启时才需要维护 backlog 和传播；
     * repl_role 非 master、或 incr_sync 关闭时，维护 backlog 是纯浪费 */
    if (kvs_config.repl_role == REPL_ROLE_MASTER && kvs_config.incr_sync) {
        repl_backlog_append(cmd, len);      /* 追加到复制积压缓冲区 */
        repl_incr_propagate(cmd, len);      /* 推送给 streaming 从库 */
    }
}

/*=============================================================================
 *  命令分发主函数
 *  遍历命令表，匹配并执行对应命令
 *============================================================================*/

/**
 * 命令分发器：解析并执行 RESP 命令
 * @param tokens 解析出的 token 数组
 * @param argc 参数个数（第一个是命令名）
 * @param wb 写缓冲（输出响应）
 * @param journaled 输出（必填）：本条命令是否产生需要落 AOF/复制的写效果
 *        （SET 新增 / MOD 改写 / DEL 命中删除 / FLUSHALL）。分发是本
 *        判定的权威，只置 1，入口统一归零。调用方据此在字节仍有效时把整条
 *        命令交给 kvs_journal_effect，不再需要命令名谓词预判。
 * @return DISPATCH_OK 成功, DISPATCH_ERROR 错误, DISPATCH_CLOSE 关闭连接
 */
dispatch_result_t kvs_dispatch(kvs_token_t *tokens, int argc,
                               DynBuf *wb, int *journaled)
{
    if (tokens == NULL || argc <= 0 || wb == NULL || journaled == NULL)
        return DISPATCH_ERROR;
    *journaled = 0;   /* 无效果分支不逐个清，入口统一归零 */

    kvs_token_t cmd = tokens[0];

    /*-------------------------------------------------------------------------
     *  命令名大小写不敏感：无条件单遍转大写（栈缓冲区）
     *  命令名 ≤ 32B，逐条预判有无小写是多余的二次扫描，直接转
     *-------------------------------------------------------------------------*/
    char ucmd_buf[32];
    int  ulen = cmd.len;
    if (ulen > (int)sizeof(ucmd_buf)) ulen = (int)sizeof(ucmd_buf);
    for (int i = 0; i < ulen; i++) {
        unsigned char ch = (unsigned char)cmd.ptr[i];
        /* 小写转大写：利用 ASCII 大小写差 32（0x20） */
        ucmd_buf[i] = (ch >= 'a' && ch <= 'z') ? (char)(ch & 0xDF) : (char)ch;
    }
    cmd.ptr   = ucmd_buf;
    cmd.len   = ulen;
    tokens[0] = cmd;

    /*=========================================================================
     *  热路径 1：GET 命令（60-80% 的请求）
     *  优先检查，减少分支预测开销
     *=========================================================================*/
    if (TOKEN_EQ(cmd, "GET")) {
        if (argc != 2) {
            dynbuf_cat(wb, RESP_ERR_ARGS, RESP_ERR_ARGS_LEN);
            return DISPATCH_ERROR;
        }
        
        kvs_token_t key_tok = tokens[1];
        kv_node_t *entry = dict_find(key_tok.ptr, (uint32_t)key_tok.len);

        if (!entry) {
            dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);  /* 键不存在：返回空 */
        } else {
            /* 过期检查。这一步以前漏了：GET 能读到已经过期的值，而 TTL 却报 -2 ——
             * 同一个键，一个命令说"没了"、另一个说"还在"，自相矛盾。
             *
             * 这里**只回空、不删除**，是刻意的：读路径只负责读对，回收是过期模块
             * （active_expire_cycle）的职责。让 GET 产生写效果，会把 AOF 和复制的
             * 语义绕进来，而收益（早一点回收）在主动过期开着时不明显。
             *
             * entry_expire 返回 0 表示"永不过期"，不能当时间戳用 ——
             * 这个判断和 TTL 命令完全一致。 */
            int64_t exp = entry_expire(entry);
            if (exp && exp <= get_time_now()) {
                dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);  /* 已过期：和"不存在"一样 */
            } else {
                reply_bulk(wb, entry_payload(entry), (int)entry_payload_len(entry));
            }
        }
        return DISPATCH_OK;
    }

    /*=========================================================================
     *  热路径 2：SET / SETEX —— 新增或覆盖，两种情况都回 +OK
     *
     *  两个命令只差中间两个参数的位置：
     *      SET   <key> <值>  [秒]    argc 3 或 4，省略秒 = 永不过期
     *      SETEX <key> <秒>  <值>    argc 恒为 4，秒必给且必须为正
     *
     *  所以放同一个分支里，写入逻辑只写一份 —— 免得以后改一处忘一处。
     *  参数位置一律走 val_i / ttl_i 两个下标，底下十几行原样不动。
     *
     *  为什么不把 SETEX "翻译"成 `SET k v ttl` 了事：自研 KV 要借 Redis 生态，
     *  就得兼容标准命令。而"标准"在这里的定义是 —— Redis 能跑的，我们也得能跑。
     *
     *  键在不在由 dict_add_or_find 自己查（见 kv_engine.h 契约 2）：键不存在
     *  时它已经把 payload 写好了；键已存在时它只返回既有记录、记录一个字没动，
     *  覆盖由下面的 exists 分支补写 —— 等长原地写，变长才 dict_resize_entry
     *  重建（与 MOD 同一套写法）。于是数据路径仍然只碰一次引擎调用，哈希引擎
     *  不会算两次哈希。
     *  ttl 跟随本次写入：省略即清除过期。新增和覆盖一视同仁，不留上一条的残留。
     *=========================================================================*/
    if (TOKEN_EQ(cmd, "SET") || TOKEN_EQ(cmd, "SETEX")) {
        const int is_setex = TOKEN_EQ(cmd, "SETEX");

        if (is_setex ? (argc != 4) : (argc != 3 && argc != 4)) {
            dynbuf_cat(wb, RESP_ERR_ARGS, RESP_ERR_ARGS_LEN);
            return DISPATCH_ERROR;
        }

        /* 参数位置：SETEX 把秒放在值前面 */
        const int val_i = is_setex ? 3 : 2;
        const int ttl_i = is_setex ? 2 : 3;

        long long ttl_secs = 0;
        if (is_setex || argc == 4) {
            /* 显式的 ttl 必须是正数。把「0 秒过期」静默当成永不过期放行，
             * 是最容易埋雷的写法，直接拒绝比猜调用方意图安全 */
            if (token_to_ll(tokens[ttl_i], &ttl_secs) < 0 || ttl_secs <= 0) {
                /* dynbuf_lit 是宏，靠字符串字面量拼接算长度，吃不了三元表达式，
                 * 所以这里必须分开写。错误文案里带上实际命令名，方便对着 RESP 排查。 */
                if (is_setex) {
                    dynbuf_lit(wb, "-ERR invalid expire time in 'setex' command\r\n");
                } else {
                    dynbuf_lit(wb, "-ERR invalid expire time in 'set' command\r\n");
                }
                return DISPATCH_ERROR;
            }
        }

        kvs_token_t key_tok = tokens[1];
        uint32_t val_len = (uint32_t)tokens[val_i].len;
        int exists = 0;
        kv_node_t *e = dict_add_or_find(key_tok.ptr, (uint32_t)key_tok.len,
                                        tokens[val_i].ptr, val_len, &exists);
        if (!e) {
            dynbuf_cat(wb, RESP_ERROR, RESP_ERROR_LEN);
            return DISPATCH_OK;
        }

        if (exists) {   /* 覆盖：add_or_find 没动记录，这里补写 */
            if (entry_payload_len(e) != val_len) {   /* 等长原地写，变长才重建 */
                e = dict_resize_entry(e, val_len);
                if (!e) {
                    dynbuf_cat(wb, RESP_ERROR, RESP_ERROR_LEN);
                    return DISPATCH_OK;
                }
            }
            memcpy(entry_payload(e), tokens[val_i].ptr, val_len);
        }
        entry_set_expire(e, ttl_secs ? get_time_now() + ttl_secs * 1000 : 0);

        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        *journaled = 1;   /* 新增或覆盖都是写效果 → 回传，交由 drain 落 AOF/复制 */
        return DISPATCH_OK;
    }

    /*=========================================================================
     *  热路径 3：PING 命令
     *=========================================================================*/
    if (TOKEN_EQ(cmd, "PING")) {
        if (argc == 1)
            dynbuf_cat(wb, RESP_PONG, RESP_PONG_LEN);
        else if (argc == 2)
            reply_bulk(wb, tokens[1].ptr, tokens[1].len);  /* 回显参数 */
        else
            dynbuf_lit(wb, "-ERR wrong number of arguments for 'ping' command\r\n");
        return DISPATCH_OK;
    }

    /*=========================================================================
     *  热路径 4：MOD 命令 —— 只修改，键不存在 → nil（不新增）
     *  形参：<key> <value> [ttl秒]；省略 ttl = 清除过期（覆盖即重置，不留残留）。
     *  改的前提是先拿到那条记录，所以定位省不掉：dict_find 再改；等长原地写
     *  回原记录，变长才走 dict_resize_entry 重建（新指针顶替旧的）。
     *  MOD 与 SET 的分工：SET 是 upsert（建键 + 覆盖），MOD 只改已存在的键，
     *  未命中回 nil 不新增 —— 两者共用同一套「等长原地写 / 变长重建」写法。
     *=========================================================================*/
    if (TOKEN_EQ(cmd, "MOD")) {
        if (argc != 3 && argc != 4) {
            dynbuf_cat(wb, RESP_ERR_ARGS, RESP_ERR_ARGS_LEN);
            return DISPATCH_ERROR;
        }

        long long ttl_secs = 0;
        if (argc == 4) {
            /* 同 SET：显式的 ttl 必须是正数 */
            if (token_to_ll(tokens[3], &ttl_secs) < 0 || ttl_secs <= 0) {
                dynbuf_lit(wb, "-ERR invalid expire time in 'mod' command\r\n");
                return DISPATCH_ERROR;
            }
        }

        kvs_token_t key_tok = tokens[1];
        kv_node_t *e = dict_find(key_tok.ptr, (uint32_t)key_tok.len);
        if (!e) {
            dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);   /* 不存在：MOD 不新增 */
            return DISPATCH_OK;
        }

        uint32_t val_len = (uint32_t)tokens[2].len;
        if (entry_payload_len(e) != val_len) {   /* 等长原地写，变长才重建 */
            e = dict_resize_entry(e, val_len);
            if (!e) {
                dynbuf_cat(wb, RESP_ERROR, RESP_ERROR_LEN);
                return DISPATCH_OK;
            }
        }
        memcpy(entry_payload(e), tokens[2].ptr, val_len);
        entry_set_expire(e, ttl_secs ? get_time_now() + ttl_secs * 1000 : 0);

        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        *journaled = 1;   /* 确有改写 → 回传，交由 drain 落 AOF/复制 */
        return DISPATCH_OK;
    }

    /*=========================================================================
     *  热路径 5：DEL 命令
     *=========================================================================*/
    if (TOKEN_EQ(cmd, "DEL")) {
        /* 只允许一条命令删一个 key：多 key 中间失败无回滚，本项目不做 */
        if (argc != 2) {
            dynbuf_cat(wb, RESP_ERR_ARGS, RESP_ERR_ARGS_LEN);
            return DISPATCH_ERROR;
        }

        kvs_token_t kt = tokens[1];
        /* 只需一次引擎调用：dict_delete 的返回值本身就区分了成败，
         * 先 find 一次纯属白走一趟 */
        if (dict_delete(kt.ptr, (uint32_t)kt.len) == 0) {
            reply_integer(wb, 1);
            *journaled = 1;   /* 确有删除 → 回传效果，交由 drain 落 AOF/复制 */
        } else {
            reply_integer(wb, 0);
        }
        return DISPATCH_OK;
    }

    /*=========================================================================
     *  向量命令：VSET / VGET / VDEL / VSEARCH（实现在 kvstore_vector.c）
     *
     *  向量就是 payload 带 16B 头的普通 KV 记录，所以这里只做路由：
     *  VSET/VDEL 置 journaled=1，把整条原始命令交给 AOF/增量复制；
     *  VGET/VSEARCH 纯读，journaled 保持 0。
     *
     *  先用「长度 ≥ 4 且首字母 V」收口，普通 GET/SET 热路径只多一次
     *  cmd.len 比较和一次 cmd.ptr[0] 比较，不会走四次 memcmp。
     *=========================================================================*/
    if (cmd.len >= 4 && cmd.ptr[0] == 'V') {
        if (TOKEN_EQ(cmd, "VSET"))
            return vec_cmd_vset(tokens, argc, wb, journaled);
        if (TOKEN_EQ(cmd, "VGET"))
            return vec_cmd_vget(tokens, argc, wb, journaled);
        if (TOKEN_EQ(cmd, "VDEL"))
            return vec_cmd_vdel(tokens, argc, wb, journaled);
        if (TOKEN_EQ(cmd, "VSEARCH"))
            return vec_cmd_vsearch(tokens, argc, wb, journaled);
    }

    /* QUIT：关闭连接（响应后由上层关闭） */
    if (TOKEN_EQ(cmd, "QUIT")) {
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        return DISPATCH_CLOSE;
    }

    /* ECHO：回显参数 */
    if (TOKEN_EQ(cmd, "ECHO")) {
        if (argc >= 2)
            reply_bulk(wb, tokens[1].ptr, tokens[1].len);
        else
            dynbuf_lit(wb, "-ERR wrong number of arguments for 'echo' command\r\n");
        return DISPATCH_OK;
    }

    /*=========================================================================
     *  控制面命令：PSYNC（master 增量握手）—— 置于热路径之下
     *  并入命令分发层（与 Redis 控制命令走命令表对齐）。门控 repl_engine_append
     *  （三引擎在自己 start 里赋值，启动后恒非 NULL）&& 本机为 master；从库
     *  一律不进 → fall through 到 unknown，行为不变。journaled 保持 0：PSYNC
     *  不产生需落 AOF/复制的写效果。
     *=========================================================================*/
    if (repl_engine_append != NULL &&
        kvs_config.repl_role == REPL_ROLE_MASTER &&
        TOKEN_EQ(cmd, "PSYNC"))
        return repl_master_psync_handle(tokens, argc, wb);

    /*=========================================================================
     *  管理命令
     *=========================================================================*/
    
    /* DBSIZE：返回当前键数量 */
    if (TOKEN_EQ(cmd, "DBSIZE")) {
        long long count = (long long)dict_count();
        reply_integer(wb, count);
        return DISPATCH_OK;
    }

    /* SAVE / BGSAVE：两个拼写同一条路径 —— fork 子进程 io_uring 落盘，
     * 主循环不阻塞，+OK 一律表示「已 fork」而非「已落盘」。
     * 原先 SAVE 走的主线程同步落盘已删除：它和 BGSAVE 共用同一份
     * dump.rdb.tmp + rename，两者并存却对同一个 +OK 给出不同含义
     * （一个已落盘、一个已 fork），留着只会让调用方分不清。
     * 仍需等落盘真正完成才继续的路径（全量同步）走 rdb_save_sync 自身，
     * 不经过这里。 */
    if (TOKEN_EQ(cmd, "SAVE") || TOKEN_EQ(cmd, "BGSAVE")) {
        if (rdb_save_async_uring() == 0)
            dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        else
            dynbuf_lit(wb, "-ERR Background save already in progress\r\n");
        return DISPATCH_OK;
    }

    /* CONFIG：伪装包——只应答 redis-benchmark 启动探测的 GET save / GET appendonly */
    if (TOKEN_EQ(cmd, "CONFIG"))
        return handle_config(tokens, argc, wb);

    /* INFO：最小实现，只回 memory 段 used_memory（bench_memory.sh 读内存用）
     * 其余字段按需再加，见 kvstore_info.c */
    if (TOKEN_EQ(cmd, "INFO"))
        return handle_info(tokens, argc, wb);

    /* FLUSHALL：清空所有键（只有一个全局 dict，不需要 FLUSHDB）
     * 走 kvs_flush_all() 而非裸 dict_clear: jemalloc 版在清库后同步
     * arenas.purge 把内存还给 OS, 否则 FLUSHALL 后 RSS 不回落 */
    if (TOKEN_EQ(cmd, "FLUSHALL")) {
        kvs_flush_all();
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        *journaled = 1;   /* 全库清空生效 → 交由 drain 落 AOF/复制 */
        return DISPATCH_OK;
    }

    /*=========================================================================
     *  TTL：查询剩余生存时间（秒）—— 纯读，无副作用
     *  过期时间只在 SET / MOD 写入时一次给定，没有独立的 EXPIRE 命令
     *=========================================================================*/

    if (TOKEN_EQ(cmd, "TTL")) {
        if (argc != 2) {
            dynbuf_cat(wb, RESP_ERR_ARGS, RESP_ERR_ARGS_LEN);
            return DISPATCH_ERROR;
        }

        kvs_token_t key_tok = tokens[1];
        kv_node_t *entry = dict_find(key_tok.ptr, (uint32_t)key_tok.len);

        if (!entry) {
            reply_integer(wb, -2);  /* 键不存在 */
            return DISPATCH_OK;
        }
        int64_t exp = entry_expire(entry);   /* 一次分派，下面复用 */
        if (!exp) {
            reply_integer(wb, -1);  /* 永不过期 */
            return DISPATCH_OK;
        }
        long long rem_ms = exp - get_time_now();
        if (rem_ms < 0) { 
            reply_integer(wb, -2);  /* 已过期 */
            return DISPATCH_OK; 
        }
        reply_integer(wb, (rem_ms + 500) / 1000);  /* 向上取整 */
        return DISPATCH_OK;
    }

    /*=========================================================================
     *  未知命令处理
     *=========================================================================*/
    dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
    return DISPATCH_ERROR;
}