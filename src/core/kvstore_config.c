/*=============================================================================
 *  kvstore_config.c — CONFIG GET / SET 命令实现
 *  功能：运行时查看和修改服务器配置参数
 *  设计原则：支持热更新（部分参数），只读参数需重启生效
 *============================================================================*/

#include "kvstore_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <errno.h>

/*=============================================================================
 *  配置值格式化函数（用于 CONFIG GET 输出）
 *  每个函数将配置值格式化为字符串
 *============================================================================*/

/**
 * 格式化最大客户端数
 */
static int fmt_maxclients(char *out, int cap) {
    int v = __atomic_load_n(&kvs_config.maxclients, __ATOMIC_RELAXED);
    return snprintf(out, cap, "%d", v);
}

/**
 * 格式化持久化模式
 */
static int fmt_persistence(char *out, int cap) {
    const char *s = (kvs_config.persist_mode == PERSIST_NONE) ? "none" :
                    (kvs_config.persist_mode == PERSIST_RDB)  ? "rdb"  : "aof";
    int n = (int)strlen(s);
    if (n > cap) n = cap;
    memcpy(out, s, n);
    return n;
}

/**
 * 格式化监听端口
 */
static int fmt_port(char *out, int cap) {
    return snprintf(out, cap, "%d", kvs_config.port);
}

/**
 * 格式化 AOF 状态
 */
static int fmt_appendonly(char *out, int cap) {
    const char *s = ENABLE_AOF ? "yes" : "no";
    int n = (int)strlen(s);
    if (n > cap) n = cap;
    memcpy(out, s, n);
    return n;
}

/*=============================================================================
 *  配置值设置函数（用于 CONFIG SET）
 *  每个函数验证输入并更新配置
 *============================================================================*/

/**
 * 设置最大客户端数
 * @param val 数字字符串，范围 [64, 65536]
 * @param wb 写缓冲（输出错误信息）
 * @return 0 成功，-1 失败
 */
static int set_maxclients(const char *val, DynBuf *wb) {
    char *end; 
    errno = 0;
    long v = strtol(val, &end, 10);
    
    /* 验证：必须是整数且在有效范围内 */
    if (*end != '\0' || errno != 0 || v < 64 || v > 65536) {
        dynbuf_lit(wb, "-ERR maxclients must be an integer in [64, 65536]\r\n");
        return -1;
    }
    __atomic_store_n(&kvs_config.maxclients, (int)v, __ATOMIC_RELAXED);
    return 0;
}

/*=============================================================================
 *  配置参数表
 *  定义所有可配置参数及其读写函数
 *============================================================================*/

/* 配置格式化函数类型 */
typedef int (*cfg_fmt_fn)(char *out, int cap);
/* 配置设置函数类型 */
typedef int (*cfg_set_fn)(const char *val, DynBuf *wb);

/**
 * 配置条目结构
 * name: 配置名称（CONFIG GET/SET 使用的键名）
 * fmt:  格式化函数（用于 GET）
 * set:  设置函数（用于 SET，NULL 表示只读）
 */
struct cfg_entry {
    const char *name;
    cfg_fmt_fn  fmt;
    cfg_set_fn  set;       /* NULL → 只读参数，运行时不可修改 */
};

/* 配置表：所有可配置参数 */
static const struct cfg_entry g_cfg_table[] = {
    { "maxclients",   fmt_maxclients,  set_maxclients  },  /* 可热更新 */
    { "persistence",  fmt_persistence, NULL },              /* 只读：需重启 */
    { "port",         fmt_port,        NULL },              /* 只读：需重启 */
    { "appendonly",   fmt_appendonly,  NULL },              /* 只读：需重启 */
};

/* 配置表大小 */
static const int g_cfg_table_n =
    (int)(sizeof(g_cfg_table) / sizeof(g_cfg_table[0]));

/**
 * 在配置表中查找参数
 * @param t 参数名 token
 * @return 配置条目指针，未找到返回 NULL
 */
static const struct cfg_entry *cfg_lookup(const kvs_token_t *t) {
    for (int i = 0; i < g_cfg_table_n; i++) {
        const char *n = g_cfg_table[i].name;
        int nl = (int)strlen(n);
        /* 不区分大小写比较 */
        if (nl == t->len && strncasecmp(n, t->ptr, (size_t)nl) == 0)
            return &g_cfg_table[i];
    }
    return NULL;
}

/*=============================================================================
 *  CONFIG 命令分发
 *  支持子命令：GET 和 SET
 *============================================================================*/

/**
 * 处理 CONFIG 命令
 * @param tokens 解析后的 token 数组
 * @param argc 参数个数
 * @param wb 写缓冲（输出响应）
 * @return DISPATCH_OK 成功，DISPATCH_ERROR 失败
 */
dispatch_result_t handle_config(kvs_token_t *tokens, int argc, DynBuf *wb) {
    /* 检查参数数量 */
    if (argc < 2) {
        dynbuf_cat(wb,
            "-ERR wrong number of arguments for 'config' command\r\n",
            sizeof("-ERR wrong number of arguments for 'config' command\r\n") - 1);
        return DISPATCH_ERROR;
    }

    kvs_token_t sub = tokens[1];

    /*=========================================================================
     *  CONFIG GET <parameter>
     *  返回格式：RESP 数组 [参数名, 参数值]
     *=========================================================================*/
    if (sub.len == 3 && strncasecmp(sub.ptr, "GET", 3) == 0) {
        if (argc != 3) {
            dynbuf_cat(wb,
                "-ERR wrong number of arguments for 'config|get' command\r\n",
                sizeof("-ERR wrong number of arguments for 'config|get' command\r\n") - 1);
            return DISPATCH_ERROR;
        }

        const struct cfg_entry *e = cfg_lookup(&tokens[2]);
        if (!e) {
            /* 未知参数：返回空数组（Redis 兼容行为） */
            dynbuf_lit(wb, "*0\r\n");
            return DISPATCH_OK;
        }

        /* 格式化参数值 */
        char vbuf[256];
        int  vlen = e->fmt(vbuf, (int)sizeof(vbuf));
        if (vlen < 0) vlen = 0;
        if (vlen > (int)sizeof(vbuf)) vlen = (int)sizeof(vbuf);

        /* 返回 RESP 数组：[参数名, 参数值] */
        dynbuf_lit(wb, "*2\r\n");
        reply_bulk(wb, e->name, (int)strlen(e->name));
        reply_bulk(wb, vbuf, vlen);
        return DISPATCH_OK;
    }

    /*=========================================================================
     *  CONFIG SET <parameter> <value>
     *  支持运行时热更新部分参数
     *=========================================================================*/
    if (sub.len == 3 && strncasecmp(sub.ptr, "SET", 3) == 0) {
        if (argc != 4) {
            dynbuf_cat(wb,
                "-ERR wrong number of arguments for 'config|set' command\r\n",
                sizeof("-ERR wrong number of arguments for 'config|set' command\r\n") - 1);
            return DISPATCH_ERROR;
        }

        const struct cfg_entry *e = cfg_lookup(&tokens[2]);
        if (!e) {
            dynbuf_cat(wb,
                "-ERR Unknown CONFIG parameter\r\n",
                sizeof("-ERR Unknown CONFIG parameter\r\n") - 1);
            return DISPATCH_ERROR;
        }

        /* 检查是否只读参数 */
        if (!e->set) {
            dynbuf_cat(wb,
                "-ERR parameter is read-only at runtime "
                "(boot-time only — restart with updated .conf)\r\n",
                sizeof("-ERR parameter is read-only at runtime "
                       "(boot-time only — restart with updated .conf)\r\n") - 1);
            return DISPATCH_ERROR;
        }

        /* 复制值到栈缓冲区（安全拷贝） */
        char vbuf[192];
        int vlen = tokens[3].len;
        if (vlen >= (int)sizeof(vbuf)) vlen = (int)sizeof(vbuf) - 1;
        memcpy(vbuf, tokens[3].ptr, (size_t)vlen);
        vbuf[vlen] = '\0';

        /* 调用设置函数 */
        if (e->set(vbuf, wb) < 0)
            return DISPATCH_ERROR;

        dynbuf_cat(wb, RESP_OK, RESP_OK_LEN);
        return DISPATCH_OK;
    }

    /*=========================================================================
     *  未知子命令
     *=========================================================================*/
    dynbuf_cat(wb,
        "-ERR Unknown CONFIG subcommand (supported: GET, SET)\r\n",
        sizeof("-ERR Unknown CONFIG subcommand (supported: GET, SET)\r\n") - 1);
    return DISPATCH_ERROR;
}