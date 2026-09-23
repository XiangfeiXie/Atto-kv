#include "protocol.h"
#include "config.h"
#include "kvstore.h"
#include "rbuf.h"
#include "kv_key.h"   /* likely/unlikely：分支预测宏 */
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
    /* 非 '*' 开头必为协议错误；'*' 前缀不足 4 字节是未完成的命令开头
     * （如只收到 "*3"），交由下方 CRLF 完整性检查返回 0（等待更多数据），
     * 不能在此判为协议错误——proactor 分块读时 recv 边界常落在命令中间 */
    if (unlikely(buf[0] != '*')) return -1;

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
     * 返回 -1 即协议错误（非 RESP 数组格式），由上层拒绝 */
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
 *  rbuf 读缓冲命令消费端（段内零拷贝 + 跨段整命令拉平）
 *
 *  网络层读缓冲为节点队列（rbuf_t / struct rbuf）时的解析入口。
 *  模型：每条完整命令要么整条落在队头连续段（head 节点剩余内存）内，要么跨段。
 *  两种情形都用同一个 token 解析器——连续缓冲解析器 resp_parse_slices（与 AOF
 *  重载同一解析器）——区别只在解析源与是否多一步整命令拉平：
 *
 *    - 段内命令：resp_parse_slices 直接在队头连续段上解析，token 零拷贝直指
 *      节点内存；"整条原始命令"本就在这段连续内存里，日志/复制可直接以它为源。
 *    - 跨段命令：段内试解析返回 0 → 量长游标（rbuf_rdr，见下）只数整条命令
 *      长度 → 整命令一次 rbuf_copy 拉平进本 drain 复用的 flat → 再统一解析。
 *
 *  "是否跨段"由 resp_parse_slices 在队头连续段上的返回值免费判定，无专门扫描：
 *    >0 段内完整命令；0 且缓冲不止这一连续段 → 跨段（或跨段仍不完整，量长定）；
 *    0 且全缓冲都在段内 → 真不完整，break 等更多数据；-1 段内即协议错误。
 *  不完整的分片始终躺在 rbuf 节点零拷贝等待，只在命令完整且跨段那刻拷一次——
 *  不退回旧连续缓冲"每收一包 memmove 尾部"的病理。
 *
 *  AOF / 增量复制（backlog + streaming）的整命令拷贝收口在本 drain（消费方在
 *  解析层）。dispatch 只收 token、回传 journaled 标志（命令是否产生需落盘/传播
 *  的写效果，dispatch 权威判定）；drain 在 dispatch 返回后、consume 前按标志
 *  把本命令的"整条原始命令"喂 kvs_journal_effect（AOF/repl 配置门在钩子内判
 *  一次）。源统一记为 cmd_src：段内 = head 节点连续内存（kvs_journal_effect
 *  同步拷贝、不滞留指针，段内直读即合法），跨段 = flat（已拉平）——除日志进
 *  自身缓冲那一次必要内部拷贝外，drain 层零二次物化（旧实现 journaled 命令
 *  一律先 rbuf_copy 进 rawbuf 再喂，段内小命令也白多拷一次）。
 *
 *  PSYNC（master 增量握手）作为控制命令并入命令分发层（见 kvs_dispatch 的
 *  PSYNC 分支）——drain 不再感知命令身份，master/普通连接共用同一解析循环。
 *============================================================================*/

/* 逻辑流只读游标（量长用）：pos 是下一字节逻辑偏移；(base,start,lim) 描述
 * 当前所在节点的连续区。段内直接取，越段（pos==lim）经 rbuf_contig 换段——
 * 换段只发生在段界（帧头/尾 CRLF 可能跨段），bulk 载荷从不逐字节读。 */
typedef struct {
    const rbuf_t *rb;
    uint32_t      pos;       /* 下一字节逻辑偏移 */
    uint32_t      avail;     /* rbuf_len 快照 */
    uint32_t      start;     /* 当前连续段起点逻辑偏移 */
    uint32_t      lim;       /* 当前连续段末端（排他）；pos<lim 时 base 有效 */
    const char   *base;      /* 当前连续段起点指针（节点内存） */
} rbuf_rdr;

/** 读 1 字节并前进；流尾返回 -1 */
static int rdr_get(rbuf_rdr *r) {
    if (r->pos >= r->lim) {
        if (r->pos >= r->avail) return -1;   /* 流尾 */
        r->start = r->pos;
        r->base  = NULL;
        uint32_t n = rbuf_contig(r->rb, r->pos, &r->base);
        r->lim = r->pos + n;
    }
    return (unsigned char)r->base[r->pos++ - r->start];
}

/**
 * 量长：返回 rbuf 逻辑流第一条完整命令的长度（>0）；0 = 不完整；-1 = 协议错误。
 *
 * 只在命令跨段（未完整落在队头连续段内）时被 drain 调用，用于决定整命令拉平
 * 的拷贝字节数。语义与 resp_parse_slices 逐条对齐（帧头/每参数结构/长度上限/
 * 尾 CRLF/不完整 vs 错误判定一致）；只推进游标，不产 token、不物化字节。
 * 跨段只影响字节所在节点，不影响长度语义——载荷按 slen 整体跳越，段界由
 * rdr_get 自动换段接续。
 *
 * @param rb         读队列（只读，不消费）
 * @param max_tokens token 容量上限（KVS_MAX_TOKENS；argc 超限判协议错）
 * @return >0 = 整条命令长（帧头/载荷/CRLF 全含）；0 = 不完整（队列原样保留）；
 *         -1 = 协议错误
 */
static int resp_cmd_len_rbuf(const rbuf_t *rb, int max_tokens)
{
    rbuf_rdr rd;
    rd.rb = rb; rd.pos = 0; rd.avail = rbuf_len(rb);
    rd.start = 0; rd.lim = 0; rd.base = NULL;
    if (rd.avail == 0) return 0;

    int c = rdr_get(&rd);
    if (c != '*') return -1;                 /* 非数组格式：拒绝 */

    /* argc：至多 3 位数字（*N > 999 已超合理 token 上限，判协议错） */
    int argc = 0, nd = 0;
    for (;;) {
        c = rdr_get(&rd);
        if (c < 0) return 0;                 /* 流尾截断：不完整 */
        if (c == '\r') break;
        if (c == '\n' || c < '0' || c > '9' || nd >= 3) return -1;
        argc = argc * 10 + (c - '0'); nd++;
    }
    c = rdr_get(&rd);
    if (c < 0) return 0;
    if (c != '\n') return -1;
    if (argc <= 0 || argc > max_tokens) return -1;

    for (int i = 0; i < argc; i++) {
        c = rdr_get(&rd);                    /* 每个参数都必须是 bulk string */
        if (c < 0) return 0;
        if (c != '$') return -1;

        /* slen：至多 9 位数字（超过 KVS_MAX_BULK_LEN 判协议错） */
        int slen = 0; nd = 0;
        for (;;) {
            c = rdr_get(&rd);
            if (c < 0) return 0;
            if (c == '\r') break;
            if (c == '\n' || c < '0' || c > '9' || nd >= 9) return -1;
            slen = slen * 10 + (c - '0'); nd++;
        }
        c = rdr_get(&rd);
        if (c < 0) return 0;
        if (c != '\n') return -1;
        if (slen > KVS_MAX_BULK_LEN) return -1;

        /* 载荷 + 载荷尾 CRLF 必须全部在场（先判在场、后校 CRLF，同 resp） */
        if ((uint64_t)rd.pos + (uint32_t)slen + 2 > rd.avail) return 0;

        rd.pos += (uint32_t)slen;            /* 整段跳越载荷：段信息过期，下次 rdr_get 换段 */

        /* 载荷尾 CRLF 校验（可能跨段） */
        c = rdr_get(&rd); if (c < 0) return 0;
        if (c != '\r') return -1;
        c = rdr_get(&rd); if (c < 0) return 0;
        if (c != '\n') return -1;
    }

    return (int)rd.pos;                      /* 整条命令逻辑长（帧头/载荷/CRLF 全含） */
}

int g_kvs_drain_fd = -1;   /* 当前正被 drain 的连接的 fd；repl 侧读它设 streaming 从库 */

int kvs_drain_rbuf_commands(int fd, struct rbuf *rb, DynBuf *wb, int *want_close) {
    g_kvs_drain_fd = fd;   /* 协议层瞬态上下文：dispatch 内 PSYNC 分支据此设 streaming 从库 g_slave_fd */

    int ncmd = 0;
    if (want_close) *want_close = 0;

    DynBuf flat = {0};                   /* 跨段命令整命令拉平缓冲（复用容量，懒分配） */

    /* journal 消费方 = AOF + 增量复制（master & incr on），run 期配置不变；
     * g_in_reload（slave 回放复用本 drain 时置 1）期间不 journal。提前算一次，
     * 避免热路径逐命令重复读配置。flat 拷贝与 want_raw 无关（它是跨段命令的
     * 解析源），want_raw 只决定 journaled 命令要不要喂 kvs_journal_effect。 */
    int want_raw = !g_in_reload &&
        (ENABLE_AOF ||
         (kvs_config.repl_role == REPL_ROLE_MASTER && kvs_config.incr_sync));

    while (rbuf_len(rb) > 0) {
        /* 队头连续段（head 节点剩余内存）：段内命令可零拷贝直读 */
        const char *base;
        uint32_t contig = rbuf_contig(rb, 0, &base);

        kvs_token_t tokens[KVS_MAX_TOKENS];
        int argc = 0;
        const char *cmd_src = NULL;      /* 本命令整条原命令所在：head 节点内存 or flat */
        int cmd_len = 0;

        /* 段内试解析：返回 >0 即命令完整落在队头连续段内（零拷贝 token，
         * 无需任何分段扫描；"是否跨段"由此返回值免费判定） */
        int consumed = resp_parse_slices(base, (int)contig,
                                         tokens, KVS_MAX_TOKENS, &argc);
        if (consumed > 0) {
            cmd_len = consumed;
            cmd_src = base;              /* 整条命令已在段内连续：日志直读这段节点内存 */
        } else if (consumed == 0) {
            if (contig >= rbuf_len(rb))
                break;                   /* 全缓冲就在这一个连续段仍不完整：等更多数据 */

            /* 跨段：量长 → 整命令一次拉平进 flat → 再统一连续解析出 token */
            cmd_len = resp_cmd_len_rbuf(rb, KVS_MAX_TOKENS);
            if (cmd_len < 0) goto proto_error;
            if (cmd_len == 0) break;     /* 跨全部可用数据仍不完整：等更多数据 */
            if (!flat.buf)
                dynbuf_init(&flat, BUF_INIT_CAP);
            if (dynbuf_reserve(&flat, cmd_len) < 0) goto proto_error;  /* OOM 关连接 */
            rbuf_copy(rb, 0, flat.buf, (uint32_t)cmd_len);
            cmd_src = flat.buf;
            /* 重解析出 token（量长只数长度，不产 token）；防御：量长与解析
             * 语义失配（consumed != cmd_len）即协议错，fail-closed */
            if (resp_parse_slices(flat.buf, cmd_len,
                                  tokens, KVS_MAX_TOKENS, &argc) != cmd_len)
                goto proto_error;
        } else {
            goto proto_error;            /* 段内已可判协议错误 */
        }

        /* 执行命令：dispatch 只收 token、回传 journaled（是否产生需落盘/传播的
         * 写效果）。效果判定是 dispatch 权威，解析层不再预判命令名；master 的
         * PSYNC 握手由 dispatch 的控制命令分支接管（kvstore_dispatch.c）。 */
        int journaled = 0;
        dispatch_result_t r = kvs_dispatch(tokens, argc, wb, &journaled);

        /* journal：把本命令"整条原始命令"喂钩子。cmd_src 段内 = head 节点连续
         * 内存（kvs_journal_effect 同步拷贝、不滞留指针，直读即合法），跨段 =
         * flat（已拉平）；两者在 consume 前均稳定。除日志进自身缓冲那一次必要
         * 内部拷贝外无二次物化。OOM 降级在 kvs_journal_effect 内（fail-soft）。 */
        if (want_raw && journaled)
            kvs_journal_effect(cmd_src, cmd_len);

        rbuf_consume(rb, (uint32_t)cmd_len);
        ncmd++;

        if (r == DISPATCH_CLOSE) {
            if (want_close) *want_close = 1;
            break;
        }
    }

    dynbuf_free(&flat);                  /* flat.buf 为 NULL 时空操作（dynbuf_free 安全） */
    g_kvs_drain_fd = -1;
    return ncmd;

proto_error:
    dynbuf_free(&flat);
    dynbuf_cat(wb, RESP_ERR_INV, RESP_ERR_INV_LEN);
    if (want_close) *want_close = 1;
    g_kvs_drain_fd = -1;
    return -1;
}

