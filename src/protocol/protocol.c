#include "protocol.h"
#include "config.h"
#include "kvstore.h"
#include <stdio.h>
#include <string.h>

/*=============================================================================
 *  RESP 协议固定响应字符串
 *  使用宏定义响应字符串及其长度，避免运行时 strlen 计算
 *============================================================================*/

#define S(id, str) \
    const char id[] = str; \
    const int  id##_LEN = (int)(sizeof(str) - 1)

/* 简单字符串响应（Simple Strings） */
S(RESP_OK,        "+OK\r\n");                    /* 操作成功 */
S(RESP_PONG,      "+PONG\r\n");                  /* PING 命令响应 */
S(RESP_NIL,       "$-1\r\n");                    /* Null Bulk String（空值） */
S(RESP_ONE,       ":1\r\n");                     /* 整数 1 */
S(RESP_ZERO,      ":0\r\n");                     /* 整数 0 */

/* 错误响应（Errors） */
S(RESP_ERROR,     "-ERR error\r\n");             /* 通用错误 */
S(RESP_ERR_INV,   "-ERR invalid RESP protocol\r\n");  /* 协议格式错误 */
S(RESP_ERR_UNK,   "-ERR unknown command\r\n");   /* 未知命令 */
S(RESP_ERR_ARGS,  "-ERR wrong number of arguments\r\n"); /* 参数数量错误 */
S(RESP_ERR_INT,   "-ERR value is not an integer or out of range\r\n"); /* 整数解析错误 */
S(RESP_WRONGTYPE, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"); /* 类型错误 */

#undef S

/*=============================================================================
 *  RESP 解析器
 *  仅支持数组格式："*N\r\n$L\r\n<data>\r\n..." 的批量命令格式
 *  
 *  解析策略：
 *  1. 先解析数组头（*N），获取命令数量
 *  2. 逐个解析每个参数（$L），提取数据
 *  3. 严格校验 CRLF 边界，防止协议注入
 *  4. 区分"数据不完整"和"协议错误"，支持流式解析
 *============================================================================*/

/**
 * 快速 CRLF 检测
 * 使用 16 位比较代替两个字节比较，性能优化
 * '\r'=0x0d, '\n'=0x0a，小端序下组合为 0x0a0d
 * memcpy（非强制转换）保持严格别名安全，-O2 优化为单条 unaligned movzwl + cmp
 * @param p 指向待检测位置的指针
 * @return 1 如果是 CRLF，否则 0
 */
static inline int kvs_is_crlf(const char *p) {
    uint16_t w;
    memcpy(&w, p, sizeof(w));
    return w == 0x0a0d;
}

/**
 * RESP 数组解析核心函数
 * @param buf 输入缓冲区
 * @param buflen 缓冲区有效数据长度
 * @param tokens 输出：解析出的 token 数组
 * @param max_tokens token 数组最大容量
 * @param out_argc 输出：解析出的参数个数
 * @return 成功：消耗的字节数（>0）；数据不完整：0；协议错误：-1
 */
int resp_parse_slices(const char *buf, int buflen,
                      kvs_token_t *tokens, int max_tokens, int *out_argc)
{
    /* 最小有效 RESP 数组：至少 4 字节（"*1\r\n"） */
    if (unlikely(buflen < 4 || buf[0] != '*')) return -1;

    const char *p = buf + 1, *end = buf + buflen;

    /*=========================================================================
     *  步骤 1：解析数组长度（argc）
     *  限制：最多 3 位数字（*N > 999 已超过合理的 max_tokens）
     *  更长的数字串视为协议错误
     *=========================================================================*/
    int argc = 0, nd = 0;
    while (nd < 3 && p < end && *p >= '0' && *p <= '9') {
        argc = argc * 10 + (*p - '0');
        p++; nd++;
    }

    /* 关键区分：数据不完整 vs 协议错误
     * 如果只剩 '\r' 而 '\n' 尚未到达，这是数据不完整，不是协议错误
     * 返回 -1 会导致 fallback 到内联命令解析，错误解析 "*3\r\n" 头
     * 从而破坏协议流 */
    if (unlikely(p + 1 >= end)) return 0;    /* 不完整：等待更多数据 */
    if (unlikely(!kvs_is_crlf(p))) return -1; /* 协议错误 */
    if (unlikely(argc <= 0 || argc > max_tokens)) return -1;
    p += 2;  /* 跳过 \r\n */

    /*=========================================================================
     *  步骤 2：解析每个 bulk 参数
     *=========================================================================*/
    for (int i = 0; i < argc; i++) {
        /* 检查是否还有足够数据 */
        if (p >= end) return 0;    /* 不完整 */
        if (unlikely(*p != '$')) return -1;  /* 必须是 bulk 类型 */
        p++;

        /*-------------------------------------------------------------------
         *  解析 bulk 长度
         *  限制：最多 9 位数字（KVS_MAX_BULK_LEN=512MB 有 9 位数字）
         *  更长的数字串只能是超长长度 → 直接报错
         *-------------------------------------------------------------------*/
        int slen = 0; nd = 0;
        while (nd < 9 && p < end && *p >= '0' && *p <= '9') {
            slen = slen * 10 + (*p - '0');
            p++; nd++;
        }

        /* 同样区分不完整 vs 协议错误 */
        if (unlikely(p + 1 >= end)) return 0;    /* 不完整 */
        if (unlikely(!kvs_is_crlf(p))) return -1;
        if (unlikely(slen > KVS_MAX_BULK_LEN)) return -1;
        p += 2;

        /* 检查 bulk 数据及其尾部 CRLF 是否完整到达 */
        if (unlikely(p + slen + 2 > end)) return 0;  /* 不完整 */

        /* 记录 token */
        tokens[i].ptr = p;
        tokens[i].len = slen;
        p += slen;

        /* 验证尾部 CRLF（p + slen + 2 <= end 已保证至少 2 字节） */
        if (unlikely(!kvs_is_crlf(p))) return -1;
        p += 2;
    }

    *out_argc = argc;
    return (int)(p - buf);  /* 返回实际消耗的字节数 */
}

/*=============================================================================
 *  命令分发循环
 *  从读缓冲中提取命令，执行并生成响应到写缓冲
 *  
 *  支持两种输入格式：
 *  1. RESP 数组格式（主要）："*N\r\n$L\r\n<data>\r\n..."
 *  2. 内联命令格式（兼容）："PING\r\n" 或 "SET key value\r\n"
 *============================================================================*/

/**
 * 命令处理主循环（带偏移管理）
 * @param fd 连接 fd（用于日志/统计）
 * @param rb 读缓冲（输入）
 * @param r_off 读缓冲已消费偏移（输入/输出）
 * @param wb 写缓冲（输出）
 * @param want_close 输出：是否需要关闭连接
 * @return 处理的命令数量，-1 表示错误
 */
int kvs_drain_resp_commands_off(int fd, DynBuf *rb, int *r_off, DynBuf *wb, int *want_close)
{
    (void)fd;  /* 未使用，保留接口兼容性 */

    int off = *r_off, remaining = rb->len, ncmd = 0;
    if (want_close) *want_close = 0;

    /*=========================================================================
     *  主循环：逐个解析并执行命令
     *=========================================================================*/
    while (remaining > 0) {
        kvs_token_t tokens[KVS_MAX_TOKENS];
        int argc = 0;
        
        /* 尝试按 RESP 数组格式解析 */
        int cmd_len = resp_parse_slices(rb->buf + off, remaining,
                                        tokens, KVS_MAX_TOKENS, &argc);

        /*-------------------------------------------------------------------
         *  内联命令 fallback（兼容 redis-benchmark 和 redis-cli）
         *  redis-benchmark 发送 "PING\r\n"
         *  redis-cli 发送内联命令如 "SET key value\r\n"
         *-------------------------------------------------------------------*/
        if (cmd_len == -1) {
            const char *p = rb->buf + off;
            const char *end = p + remaining;
            
            /* 查找换行符 */
            const char *ln = memchr(p, '\n', (size_t)(end - p));
            if (!ln || p == ln) { 
                cmd_len = 0; 
                break;  /* 行不完整，等待更多数据 */
            }

            /* 拷贝行到临时缓冲区（限制长度防止溢出） */
            char line[256];
            int linelen = (int)(ln - p);
            if (linelen > 255) linelen = 255;
            if (linelen > 0 && p[linelen - 1] == '\r') linelen--;
            memcpy(line, p, linelen);
            line[linelen] = '\0';

            /* 按空格分割 token */
            argc = 0;
            char *saveptr = NULL;
            char *tok = strtok_r(line, " ", &saveptr);
            while (tok && argc < KVS_MAX_TOKENS) {
                tokens[argc].ptr = tok;
                tokens[argc].len = (int)strlen(tok);
                argc++;
                tok = strtok_r(NULL, " ", &saveptr);
            }

            /* 空行处理（例如 redis-cli --pipe 同步标记的 \r\n）
             * 静默消费，不产生任何命令 */
            if (argc == 0) {
                int consumed = (int)(ln + 1 - p);
                off       += consumed;
                remaining -= consumed;
                continue;
            }

            cmd_len = (int)(ln + 1 - p); /* 消耗的字节数（包含 \n） */
        }

        /* 解析失败：发送错误响应，标记关闭 */
        if (cmd_len <= 0) {
            if (cmd_len == 0) break;  /* 数据不完整，等待更多 */
            dynbuf_cat(wb, RESP_ERR_INV, RESP_ERR_INV_LEN);
            if (want_close) *want_close = 1;
            ncmd = -1;
            break;
        }

        /*-------------------------------------------------------------------
         *  执行命令
         *  kvs_dispatch 解析 token 并执行具体命令
         *  返回 DISPATCH_CLOSE 表示需要关闭连接（如 QUIT 命令）
         *-------------------------------------------------------------------*/
        dispatch_result_t r = kvs_dispatch(tokens, argc,
                                            rb->buf + off, cmd_len, wb);
        off += cmd_len;
        remaining -= cmd_len;
        ncmd++;

        if (r == DISPATCH_CLOSE) { 
            if (want_close) *want_close = 1; 
            break; 
        }
    }

    /*-----------------------------------------------------------------------
     *  压缩读缓冲（性能优化）
     *  条件：已消费前缀超过容量的一半，且剩余数据量小于已消费量
     *  使用 memmove 回收空间，避免频繁扩容
     *-----------------------------------------------------------------------*/
    if (off > rb->cap / KVS_RB_COMPACT_RATIO && remaining > 0 && remaining < off) {
        memmove(rb->buf, rb->buf + off, (size_t)remaining);
        off = 0;
    }

    /* 更新偏移和剩余长度 */
    *r_off = off;
    rb->len = remaining;
    return ncmd;
}

/*=============================================================================
 *  遗留兼容接口
 *  仅供未启用的 ntyco 引擎使用（其 msg_handler 签名匹配）
 *  proactor/reactor 引擎使用 kvs_drain_resp_commands_off，不调用此函数
 *============================================================================*/

int kvs_protocol_batch(char *msg, int len, DynBuf *write_buf) {
    (void)msg; (void)len; (void)write_buf; 
    return 0;  /* 空实现 */
}