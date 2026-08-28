/*=============================================================================
 *  kvstore_config.c — CONFIG 伪装包（masquerade）
 *
 *  本项目不支持动态修改配置（没有 CONFIG SET）。CONFIG 命令唯一的存在意义
 *  是让 redis-benchmark 启动时的配置探测通过：它启动时只发两个 CONFIG GET
 *  （"save" 与 "appendonly"）。本模块默认伪装成"配置了空 save 规则且
 *  appendonly 为 no"的 Redis 服务：
 *
 *      CONFIG GET save       → *2  save        + 空字符串（无 save 规则）
 *      CONFIG GET appendonly → *2  appendonly  + no
 *
 *  其余 CONFIG GET 参数 → 空数组；CONFIG SET / 未知子命令 → 错误。
 *============================================================================*/

#include "kvstore_internal.h"

#include <stdio.h>
#include <string.h>
#include <strings.h>

/* CONFIG GET 探测参数（redis-benchmark 启动探测就这两个） */
#define CFG_NAME_SAVE       "save"
#define CFG_NAME_APPENDONLY "appendonly"

/**
 * 处理 CONFIG 命令（伪装包）
 * 只应答 redis-benchmark 启动探测的 CONFIG GET save / appendonly，
 * 其余参数返回空数组；不支持任何动态修改。
 * @param tokens 解析后的 token 数组
 * @param argc 参数个数
 * @param wb 写缓冲（输出响应）
 * @return DISPATCH_OK 成功，DISPATCH_ERROR 错误
 */
dispatch_result_t handle_config(kvs_token_t *tokens, int argc, DynBuf *wb) {
    if (argc < 2) {
        dynbuf_lit(wb, "-ERR wrong number of arguments for 'config' command\r\n");
        return DISPATCH_ERROR;
    }

    kvs_token_t sub = tokens[1];

    /*-------------------------------------------------------------------------
     *  CONFIG GET [param ...]
     *  只应答已知探测参数，未知参数直接忽略（Redis 同款行为）
     *-------------------------------------------------------------------------*/
    if (sub.len == 3 && strncasecmp(sub.ptr, "GET", 3) == 0) {
        /* 先数应答条数，写 *N 头 */
        int entries = 0;
        for (int i = 2; i < argc; i++) {
            kvs_token_t p = tokens[i];
            if ((p.len == (int)strlen(CFG_NAME_SAVE) &&
                 strncasecmp(p.ptr, CFG_NAME_SAVE, p.len) == 0) ||
                (p.len == (int)strlen(CFG_NAME_APPENDONLY) &&
                 strncasecmp(p.ptr, CFG_NAME_APPENDONLY, p.len) == 0))
                entries++;
        }

        char hdr[32];
        int  hl = snprintf(hdr, sizeof(hdr), "*%d\r\n", entries * 2);
        dynbuf_cat(wb, hdr, hl);

        /* 每条 [参数名, 参数值]：save 为空规则，appendonly 恒为 no */
        for (int i = 2; i < argc; i++) {
            kvs_token_t p = tokens[i];
            if (p.len == (int)strlen(CFG_NAME_SAVE) &&
                strncasecmp(p.ptr, CFG_NAME_SAVE, p.len) == 0) {
                reply_bulk(wb, CFG_NAME_SAVE, (int)strlen(CFG_NAME_SAVE));
                reply_bulk(wb, "", 0);
            } else if (p.len == (int)strlen(CFG_NAME_APPENDONLY) &&
                       strncasecmp(p.ptr, CFG_NAME_APPENDONLY, p.len) == 0) {
                reply_bulk(wb, CFG_NAME_APPENDONLY, (int)strlen(CFG_NAME_APPENDONLY));
                reply_bulk(wb, "no", 2);
            }
        }
        return DISPATCH_OK;
    }

    /*-------------------------------------------------------------------------
     *  其余子命令（SET / REWRITE / …）：不支持动态修改配置
     *-------------------------------------------------------------------------*/
    dynbuf_lit(wb,
        "-ERR CONFIG is read-only (no dynamic modification); "
        "boot-time settings only from .conf\r\n");
    return DISPATCH_ERROR;
}
