/*=============================================================================
 *  config.c — 配置文件加载与解析
 *  
 *  功能：
 *  1. 设置默认配置
 *  2. 解析 .conf 配置文件
 *  3. 打印当前配置
 *  
 *  配置格式：
 *  - KEY VALUE
 *  - 支持 # 注释
 *  - 键不区分大小写
 *  - 支持布尔值（on/off, yes/no, true/false, 1/0）
 *============================================================================*/

#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <unistd.h>      /* sysconf */
#include <sys/stat.h>

/*=============================================================================
 *  全局配置实例
 *============================================================================*/

KvsConfig kvs_config;

/*=============================================================================
 *  默认配置
 *  安全值，可被 .conf 覆盖
 *============================================================================*/

/**
 * 设置默认配置
 * 
 * 所有配置项都有合理的默认值
 * 确保即使没有配置文件也能正常运行
 */
static void config_default(void) {
    /* 服务配置 */
    kvs_config.port          = 6379;           /* 默认端口（Redis 兼容） */
    kvs_config.persist_mode  = PERSIST_NONE;   /* 默认不持久化 */
    kvs_config.network_model = NET_REACTOR;    /* 默认 Reactor */
    kvs_config.maxclients    = 10000;          /* 最大 10000 客户端 */
    strncpy(kvs_config.aof_fsync_policy, "everysec", sizeof(kvs_config.aof_fsync_policy) - 1);

    /* 复制配置 */
    kvs_config.repl_role         = REPL_ROLE_OFF;      /* 复制默认关闭 */
    kvs_config.incr_sync         = 1;                  /* 增量同步默认开启 */
    kvs_config.master_port       = 6379;               /* 与 port 保持一致 */
    kvs_config.fullsync_port     = 2101;               /* 全量数据端口 */

    strncpy(kvs_config.fullsync_transport, "rdma",
            sizeof(kvs_config.fullsync_transport) - 1);
    strncpy(kvs_config.fullsync_listen_ip, "0.0.0.0",
            sizeof(kvs_config.fullsync_listen_ip) - 1);
    strncpy(kvs_config.fullsync_master_ip, "127.0.0.1",
            sizeof(kvs_config.fullsync_master_ip) - 1);

    kvs_config.fullsync_timeout = 3600000;     /* 1 小时 */
    kvs_config.fullsync_max_retries = 3;

    kvs_config.expire_enabled            = 0;  /* 过期默认关闭 */
}

/*=============================================================================
 *  配置解析辅助函数
 *============================================================================*/

/**
 * 去除字符串右侧空白字符
 * @param s 待处理的字符串
 */
static void rtrim(char *s) {
    int len = (int)strlen(s);
    while (len > 0 && (s[len-1] == '\n' || s[len-1] == '\r' ||
                       s[len-1] == ' '  || s[len-1] == '\t'))
        s[--len] = '\0';
}

/**
 * 修剪配置值
 * 移除注释（#）和首尾空白
 * @param val 配置值
 */
static void trim_val(char *val) {
    char *c = strchr(val, '#');   /* 查找注释标记 */
    if (c) *c = '\0';             /* 截断注释 */
    rtrim(val);
}

/**
 * 解析整数值
 * @param val 字符串值
 * @param lineno 行号（用于错误报告）
 * @param key 键名（用于错误报告）
 * @param lo 最小值
 * @param hi 最大值
 * @param out 输出整数值
 * @return 0 成功，-1 失败
 */
static int parse_int(const char *val, int lineno, const char *key,
                     long lo, long hi, int *out) {
    char *end;
    errno = 0;
    long v = strtol(val, &end, 10);
    if (*end != '\0' || errno != 0 || v < lo || v > hi) {
        printf("[CONFIG] line %d: invalid value '%s' for '%s' "
               "(expected integer %ld-%ld)\n", lineno, val, key, lo, hi);
        return -1;
    }
    *out = (int)v;
    return 0;
}

/**
 * 解析布尔值
 * 支持：on/off, yes/no, true/false, 1/0
 * @param val 字符串值
 * @param lineno 行号（用于错误报告）
 * @param key 键名（用于错误报告）
 * @return 1 真，0 假，-1 错误
 */
static int parse_bool(const char *val, int lineno, const char *key) {
    if (strcasecmp(val, "on")   == 0 || strcasecmp(val, "yes") == 0 ||
        strcasecmp(val, "1")    == 0 || strcasecmp(val, "true") == 0)
        return 1;
    if (strcasecmp(val, "off")  == 0 || strcasecmp(val, "no")  == 0 ||
        strcasecmp(val, "0")    == 0 || strcasecmp(val, "false") == 0)
        return 0;
    printf("[CONFIG] line %d: bad boolean '%s' for %s\n", lineno, val, key);
    return -1;
}

/*=============================================================================
 *  配置文件解析器
 *============================================================================*/

/**
 * 加载配置文件
 * @param path 配置文件路径
 * @return 0 成功，-1 文件不存在（使用默认配置）
 * 
 * 配置格式示例：
 *   port 6379
 *   persist_mode rdb
 *   repl_role master
 *   expire off
 */
int config_load(const char *path) {
    config_default();   /* 先加载默认值 */

    FILE *fp = fopen(path, "r");
    if (!fp) {
        printf("[CONFIG] '%s' not found, using defaults\n", path);
        return -1;
    }
    printf("[CONFIG] Loading %s\n", path);

    char line[256];
    int  lineno = 0;

    while (fgets(line, sizeof(line), fp)) {
        lineno++;
        
        /* 检查行是否过长（缓冲区满且末尾没有换行） */
        if (strlen(line) == sizeof(line) - 1 && line[sizeof(line) - 2] != '\n') {
            printf("[CONFIG] line %d: line too long (>%d chars), skipped\n",
                   lineno, (int)(sizeof(line) - 2));
            int c;
            while ((c = fgetc(fp)) != EOF && c != '\n') {}  /* 排空剩余 */
            continue;
        }

        /* 跳过首部空白 */
        char *p = line;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '#' || *p == '\0' || *p == '\n' || *p == '\r')
            continue;   /* 注释或空行 */

        rtrim(p);
        if (*p == '\0') continue;

        /* 解析 KEY VALUE */
        char key[64] = {0}, val[192] = {0};
        if (sscanf(p, "%63s %191[^\n]", key, val) < 2) continue;
        trim_val(val);
        if (val[0] == '\0') continue;

        /*-------------------------------------------------------------------
         *  服务配置
         *-------------------------------------------------------------------*/
        if (strcasecmp(key, "port") == 0) {
            parse_int(val, lineno, "port", 1, 65535, &kvs_config.port);

        } else if (strcasecmp(key, "maxclients") == 0) {
            parse_int(val, lineno, "maxclients", 64, 65536, &kvs_config.maxclients);

        } else if (strcasecmp(key, "network") == 0) {
            if      (strcasecmp(val, "reactor")  == 0) kvs_config.network_model = NET_REACTOR;
            else if (strcasecmp(val, "proactor") == 0) kvs_config.network_model = NET_PROACTOR;
            else if (strcasecmp(val, "ntyco")    == 0) kvs_config.network_model = NET_NTYCO;
            else printf("[CONFIG] line %d: unknown network '%s'\n", lineno, val);

        } else if (strcasecmp(key, "persistence") == 0) {
            if      (strcasecmp(val, "none") == 0) kvs_config.persist_mode = PERSIST_NONE;
            else if (strcasecmp(val, "rdb")  == 0) kvs_config.persist_mode = PERSIST_RDB;
            else if (strcasecmp(val, "aof")  == 0) kvs_config.persist_mode = PERSIST_AOF;
            else printf("[CONFIG] line %d: unknown persistence '%s'\n", lineno, val);

        } else if (strcasecmp(key, "aof_fsync_policy") == 0) {
            strncpy(kvs_config.aof_fsync_policy, val,
                    sizeof(kvs_config.aof_fsync_policy) - 1);

        /*-------------------------------------------------------------------
         *  复制配置
         *-------------------------------------------------------------------*/
        } else if (strcasecmp(key, "repl_role") == 0) {
            if      (strcasecmp(val, "off")    == 0) kvs_config.repl_role = REPL_ROLE_OFF;
            else if (strcasecmp(val, "master") == 0) kvs_config.repl_role = REPL_ROLE_MASTER;
            else if (strcasecmp(val, "slave")  == 0) kvs_config.repl_role = REPL_ROLE_SLAVE;
            else printf("[CONFIG] line %d: unknown repl_role '%s' "
                        "(expected off|master|slave)\n", lineno, val);

        } else if (strcasecmp(key, "fullsync_transport") == 0) {
            if (strcasecmp(val, "sendfile") == 0 || strcasecmp(val, "tcp") == 0) {
                /* "tcp" 是遗留别名，标准化为 "sendfile" */
                strncpy(kvs_config.fullsync_transport, "sendfile",
                        sizeof(kvs_config.fullsync_transport) - 1);
            } else if (strcasecmp(val, "rdma") == 0) {
                strncpy(kvs_config.fullsync_transport, "rdma",
                        sizeof(kvs_config.fullsync_transport) - 1);
            } else printf("[CONFIG] line %d: unknown transport '%s' "
                           "(expected 'sendfile' or 'rdma')\n", lineno, val);

        } else if (strcasecmp(key, "incr_sync") == 0) {
            int v = parse_bool(val, lineno, "incr_sync");
            if (v >= 0) kvs_config.incr_sync = v;

        } else if (strcasecmp(key, "fullsync_listen_ip") == 0) {
            strncpy(kvs_config.fullsync_listen_ip, val,
                    sizeof(kvs_config.fullsync_listen_ip) - 1);

        } else if (strcasecmp(key, "fullsync_master_ip") == 0) {
            strncpy(kvs_config.fullsync_master_ip, val,
                    sizeof(kvs_config.fullsync_master_ip) - 1);

        } else if (strcasecmp(key, "fullsync_port") == 0) {
            parse_int(val, lineno, "fullsync_port", 1, 65535, &kvs_config.fullsync_port);

        } else if (strcasecmp(key, "master_port") == 0) {
            parse_int(val, lineno, "master_port", 1, 65535, &kvs_config.master_port);

        } else if (strcasecmp(key, "fullsync_timeout") == 0) {
            parse_int(val, lineno, "fullsync_timeout", 1000, 7200000, &kvs_config.fullsync_timeout);

        } else if (strcasecmp(key, "fullsync_max_retries") == 0) {
            parse_int(val, lineno, "fullsync_max_retries", 0, 10, &kvs_config.fullsync_max_retries);

        } else if (strcasecmp(key, "expire") == 0) {
            int v = parse_bool(val, lineno, "expire");
            if (v >= 0) kvs_config.expire_enabled = v;

        } else {
            printf("[CONFIG] line %d: unknown key '%s'\n", lineno, key);
        }
    }

    fclose(fp);
    return 0;
}

/*=============================================================================
 *  打印配置
 *  启动时输出，便于确认当前配置
 *============================================================================*/

/**
 * 打印当前配置（美化输出）
 * 用于启动时确认配置
 */
void config_print(void) {
    static const char *persist[] = {"none", "rdb", "aof"};
    static const char *netmod[]  = {"reactor", "proactor", "ntyco"};

    const char *role =
        (kvs_config.repl_role == REPL_ROLE_MASTER) ? "MASTER" :
        (kvs_config.repl_role == REPL_ROLE_SLAVE)  ? "SLAVE"  : "OFF";

    printf("\n"
           "========================================\n"
           "  KVStore 10.0 — Configuration\n"
           "========================================\n"
           "  Service   : 0.0.0.0:%-5d  maxclients=%d\n"
           "  Engine    : dict        persistence=%s\n"
           "  Network   : %s\n",
           kvs_config.port,
           kvs_config.maxclients,
           persist[kvs_config.persist_mode],
           netmod[kvs_config.network_model]);

    if (kvs_config.repl_role == REPL_ROLE_OFF) {
        printf("\n"
               "  Replication [OFF]\n");
    } else {
        printf("\n"
               "  Replication [%s]  fullsync=%s  incr=%s\n",
               role, kvs_config.fullsync_transport,
               kvs_config.incr_sync ? "on" : "off");
        if (kvs_config.repl_role == REPL_ROLE_MASTER) {
            printf("    Service port : %d  (PSYNC 握手, +FULLRESYNC 驱动全量)\n"
                   "    Fullsync     : %s:%d  (数据端口, 两种传输共用)\n",
                   kvs_config.master_port,
                   kvs_config.fullsync_listen_ip,
                   kvs_config.fullsync_port);
        } else {
            printf("    Master       : %s:%d  (PSYNC 握手, +FULLRESYNC 驱动全量)\n"
                   "    Fullsync     : %s:%d  (数据端口, 两种传输共用)\n",
                   kvs_config.fullsync_master_ip,
                   kvs_config.port,
                   kvs_config.fullsync_master_ip,
                   kvs_config.fullsync_port);
        }
    }

    printf("  Expire        : %s\n",
           kvs_config.expire_enabled ? "on (Redis-style lazy)" : "off");
    printf("========================================\n\n");
}