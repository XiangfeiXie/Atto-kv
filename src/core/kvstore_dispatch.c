/*=============================================================================
 *  kvstore_dispatch.c — RESP 命令分发器
 *  功能：解析 RESP 协议命令，执行 K/V 操作，生成响应
 *  设计原则：热路径优先（GET/SET），命令名不区分大小写
 *============================================================================*/

#include "kvstore_internal.h"
#include "repl.h"
#include "persistence.h"

#include <stdio.h>
#include <string.h>

/*=============================================================================
 *  SET 命令核心实现
 *  最简语义：SET key value —— 总是覆盖原值，无 NX/XX/EX/PX 选项
 *  过期时间通过独立 EXPIRE 命令设置（默认秒级）
 *============================================================================*/

/**
 * SET 命令底层实现
 * @param key 键
 * @param key_len 键长度
 * @param val 值
 * @param val_len 值长度
 * @param hash 键的哈希值（预先计算，避免重复计算）
 * @param existing 预先查找的条目（NULL 表示不存在）
 * @return 0 成功, -1 内存分配错误
 */
static int string_set(const char *key, uint32_t key_len,
                      const char *val, uint32_t val_len,
                      uint64_t hash, kv_entry_t *existing)
{
    if (existing) {
        /* 键已存在：原地覆盖值（长度相同直接覆盖，否则重分配条目） */
        kv_entry_t *e;
        if (existing->val_len == val_len) {
            memcpy(entry_payload(existing), val, val_len);
            e = existing;
        } else {
            e = dict_resize_entry(&g_dict, existing, val_len);
            if (!e) return -1;
            memcpy(entry_payload(e), val, val_len);
        }
        e->expire = 0;   /* 覆盖清除旧过期；TTL 由 EXPIRE 命令设置 */
        return 0;
    }

    /* 键不存在：添加新条目 */
    kv_entry_t *e = dict_add(&g_dict, key, key_len, val, val_len, hash);
    if (!e) return -1;
    return 0;
}

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
 *        （SET 存成 / DEL 命中删除 / FLUSHALL / EXPIRE 命中设置）。分发是本
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
        uint64_t hash = kv_hash((const uint8_t *)key_tok.ptr, (uint32_t)key_tok.len);
        kv_entry_t *entry = dict_find(&g_dict, key_tok.ptr, (uint32_t)key_tok.len, hash);
        
        if (!entry) {
            dynbuf_cat(wb, RESP_NIL, RESP_NIL_LEN);  /* 键不存在：返回空 */
        } else {
            reply_bulk(wb, entry_payload(entry), (int)entry_payload_len(entry));
        }
        return DISPATCH_OK;
    }

    /*=========================================================================
     *  热路径 2：SET 命令
     *  最简语义：SET key value —— 总是覆盖原值，无 NX/XX/EX/PX 选项
     *  过期时间通过 EXPIRE 命令设置（默认秒级）
     *=========================================================================*/
    if (TOKEN_EQ(cmd, "SET")) {
        if (argc != 3) {
            dynbuf_cat(wb, RESP_ERR_ARGS, RESP_ERR_ARGS_LEN);
            return DISPATCH_ERROR;
        }

        kvs_token_t key_tok = tokens[1];
        uint64_t hash = kv_hash((const uint8_t *)key_tok.ptr, (uint32_t)key_tok.len);
        kv_entry_t *entry = dict_find(&g_dict, key_tok.ptr, (uint32_t)key_tok.len, hash);

        int ret = string_set(key_tok.ptr, (uint32_t)key_tok.len,
                             tokens[2].ptr, (uint32_t)tokens[2].len,
                             hash, entry);
        if (ret < 0) {
            dynbuf_cat(wb, RESP_ERROR, RESP_ERROR_LEN);
        } else {
            dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
            *journaled = 1;   /* 存成 → 回传效果，交由 drain 落 AOF/复制 */
        }
        return DISPATCH_OK;
    }

    /*=========================================================================
     *  热路径 3：PING / DEL 命令
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

    if (TOKEN_EQ(cmd, "DEL")) {
        /* 只允许一条命令删一个 key：多 key 中间失败无回滚，本项目不做 */
        if (argc != 2) {
            dynbuf_cat(wb, RESP_ERR_ARGS, RESP_ERR_ARGS_LEN);
            return DISPATCH_ERROR;
        }

        kvs_token_t kt = tokens[1];
        uint64_t kh = kv_hash((const uint8_t *)kt.ptr, (uint32_t)kt.len);
        kv_entry_t *ke = dict_find(&g_dict, kt.ptr, (uint32_t)kt.len, kh);
        if (ke && dict_delete(&g_dict, kt.ptr, (uint32_t)kt.len, kh) == 0) {
            reply_integer(wb, 1);
            *journaled = 1;   /* 确有删除 → 回传效果，交由 drain 落 AOF/复制 */
        } else {
            reply_integer(wb, 0);
        }
        return DISPATCH_OK;
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
     *  （rbuf 引擎在位，ntyco 与 AOF 重载期恒 NULL）&& 本机为 master；从库/重载
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
        long long count = (long long)dict_count(&g_dict);
        reply_integer(wb, count);
        return DISPATCH_OK;
    }

    /* SAVE：阻塞落盘（同步序列化 + 写入，完成才回 +OK）
     * BGSAVE 在跑时拒绝：两路都写同一 dump.rdb.tmp + rename，避免竞态 */
    if (TOKEN_EQ(cmd, "SAVE")) {
        if (rdb_bgsave_running()) {
            dynbuf_lit(wb, "-ERR Background save already in progress\r\n");
            return DISPATCH_OK;
        }
        rdb_save_sync();
        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        return DISPATCH_OK;
    }

    /* BGSAVE：独立进程落盘（fork 子进程 io_uring 写，不阻塞主循环） */
    if (TOKEN_EQ(cmd, "BGSAVE")) {
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
     *  键过期管理命令
     *=========================================================================*/

    if (argc < 2) {
        dynbuf_cat(wb, RESP_ERR_UNK, RESP_ERR_UNK_LEN);
        return DISPATCH_ERROR;
    }

    /* 提取键和哈希（后续命令共用） */
    kvs_token_t key_tok = tokens[1];
    uint64_t hash = kv_hash((const uint8_t *)key_tok.ptr, (uint32_t)key_tok.len);
    kv_entry_t *entry = dict_find(&g_dict, key_tok.ptr, (uint32_t)key_tok.len, hash);

    /* EXPIRE：设置秒级过期时间 */
    if (TOKEN_EQ(cmd, "EXPIRE")) {
        if (argc < 3) {
            dynbuf_lit(wb, "-ERR wrong number of arguments for 'expire' command\r\n");
            return DISPATCH_ERROR;
        }
        long long secs;
        if (token_to_ll(tokens[2], &secs) < 0) {
            dynbuf_lit(wb, "-ERR value is not an integer or out of range\r\n");
            return DISPATCH_ERROR;
        }
        if (!entry) { 
            reply_integer(wb, 0);  /* 键不存在 */
            return DISPATCH_OK; 
        }
        entry->expire = get_time_now() + secs * 1000;  /* 秒转毫秒 */
        reply_integer(wb, 1);
        *journaled = 1;   /* 命中设置 → 交由 drain 落 AOF/复制 */
        return DISPATCH_OK;
    }

    /* TTL：查询剩余生存时间（秒） */
    if (TOKEN_EQ(cmd, "TTL")) {
        if (!entry) { 
            reply_integer(wb, -2);  /* 键不存在 */
            return DISPATCH_OK; 
        }
        if (!entry->expire) { 
            reply_integer(wb, -1);  /* 永不过期 */
            return DISPATCH_OK; 
        }
        long long rem_ms = entry->expire - get_time_now();
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