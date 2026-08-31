/*=============================================================================
 *  kvstore_info.c — INFO 命令（最小实现）
 *
 *  只应答 bench_memory.sh 读取 used_memory 需要的 `INFO memory`:
 *  返回 # Memory 段的 used_memory 一行（redis_exporter 同款键名）。
 *  其余段落（server/keyspace 等）与字段按需再加，不提前堆功能。
 *============================================================================*/

#include "kvstore_internal.h"

#include <stdio.h>
#include <string.h>

/**
 * 处理 INFO 命令（最小实现）
 * 裸 INFO 与 INFO memory / INFO all 都回 memory 段；其他段落名 → 空 bulk
 * @param tokens 解析后的 token 数组
 * @param argc 参数个数
 * @param wb 写缓冲（输出响应）
 * @return DISPATCH_OK 成功
 */
dispatch_result_t handle_info(kvs_token_t *tokens, int argc, DynBuf *wb) {
    /* 指定了非 memory 段落 → 空 bulk（Redis 对未知段同款行为） */
    if (argc >= 2) {
        kvs_token_t s = tokens[1];
        int is_mem = (s.len == 6 && strncasecmp(s.ptr, "memory", 6) == 0);
        int is_all = (s.len == 3 && strncasecmp(s.ptr, "all", 3) == 0);
        if (!is_mem && !is_all) {
            dynbuf_lit(wb, "$0\r\n\r\n");
            return DISPATCH_OK;
        }
    }

    /* 一行 used_memory 就够：bench 的 awk 按 $1=="used_memory" 精确匹配 */
    char body[128];
    int  n = snprintf(body, sizeof(body),
                      "# Memory\r\nused_memory:%llu\r\n",
                      (unsigned long long)get_used_mem());
    reply_bulk(wb, body, n);
    return DISPATCH_OK;
}
