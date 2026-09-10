/*=============================================================================
 *  protocol.h — RESP 协议处理接口
 *  
 *  功能：
 *  1. RESP（Redis Serialization Protocol）解析
 *  2. 命令分发回调类型定义
 *  3. 响应常量声明
 *  
 *  设计原则：
 *  1. 零拷贝 Token 切片（只引用原始数据，不复制）
 *  2. 流式解析（支持不完整数据，区分协议错误）
 *  3. 只支持 RESP 数组格式（内联命令等非 RESP 一律拒绝）
 *============================================================================*/

#ifndef __KVS_PROTOCOL_H__
#define __KVS_PROTOCOL_H__

#include <stdint.h>
#include "dynbuf.h"

/* rbuf 节点队列读缓冲（存储层在 src/net/rbuf.h；协议头只依赖类型名，
 * 避免把网络存储头拉进纯协议头。协议层见到的永远是 rbuf 的逻辑字节流） */
struct rbuf;

/*=============================================================================
 *  协议常量
 *============================================================================*/

#define KVS_MAX_TOKENS         128                /* 单条命令最大参数个数 */
#define KVS_MAX_BULK_LEN       (512 * 1024 * 1024) /* Bulk String 最大长度 512MB */
#define KVS_RB_COMPACT_RATIO   2                  /* 读缓冲压缩阈值（已消费 > cap/2） */
#define BUF_INIT_CAP           4096               /* 初始缓冲容量 4KB */

/*=============================================================================
 *  零拷贝 Token 切片
 *
 *  只引用原始数据中的位置和长度，不复制数据
 *  所有 token 都指向解析源中的原始位置：段内命令指向 head 节点连续内存，
 *  跨段命令指向拉平后的 flat——都是"切片引用"，不做逐参数拷贝
 *
 *  优点：
 *  - 零拷贝，减少内存分配
 *  - CPU cache 友好
 *  - 适合高吞吐场景
 *============================================================================*/

typedef struct {
    const char *ptr;  /* 指向原始数据中的位置（非 \0 终止） */
    int         len;  /* token 长度 */
} kvs_token_t;

/**
 * Token 与字面量比较宏
 * 自动比较长度和内容，不区分大小写支持由调用方处理
 * 
 * @param t token 变量
 * @param literal 字符串字面量（如 "GET"）
 * @return 1 相等，0 不等
 * 
 * 示例：TOKEN_EQ(cmd, "GET")
 */
#define TOKEN_EQ(t, literal) \
    ((t).len == (int)(sizeof(literal)-1) && memcmp((t).ptr, literal, sizeof(literal)-1) == 0)

/*=============================================================================
 *  命令分发结果
 *============================================================================*/

typedef enum {
    DISPATCH_OK    = 0,   /* 成功处理，继续 */
    DISPATCH_CLOSE = 2,   /* 关闭连接（如 QUIT 命令） */
    DISPATCH_ERROR = -1,  /* 协议错误，响应已发送 */
} dispatch_result_t;

#define KVS_DRAIN_PROTOERR  (-1)  /* 协议解析错误（返回值） */

/*=============================================================================
 *  RESP 解析函数
 *============================================================================*/

/**
 * 解析 RESP 数组格式
 * 
 * 支持的格式：*N\r\n$L\r\n<data>\r\n...
 * 
 * 特性：
 * - 流式解析：区分"数据不完整"和"协议错误"
 * - 零拷贝：tokens 指向原始缓冲，不复制
 * - 严格校验：CRLF 边界检查，防止协议注入
 * 
 * @param buf 输入缓冲区
 * @param buflen 缓冲区有效数据长度
 * @param tokens 输出：解析出的 token 数组
 * @param max_tokens token 数组最大容量（KVS_MAX_TOKENS）
 * @param out_argc 输出：解析出的参数个数
 * @return 成功：消耗的字节数（>0）；数据不完整：0；协议错误：-1
 */
int resp_parse_slices(const char *buf, int buflen,
                      kvs_token_t *tokens, int max_tokens, int *out_argc);

/*=============================================================================
 *  命令解析处理函数类型
 *
 *  这是网络引擎调用的命令解析入口点
 *  通过 *start(port, handler) 参数注入到 reactor/proactor/ntyco（rbuf 形态，
 *  见下方 kvs_rbuf_drain_fn）。三个引擎统一注入 kvs_drain_rbuf_commands；
 *  slave 出站链路连接由 repl 侧按需 eng_repl_register_rbuf 覆盖同签名 handler。
 *
 *  设计说明：
 *  - fd 参数让 handler 能回调到引擎（repl_engine_* 访问器）
 *  - 复制模块的 per-connection handler 使用 fd
 *  - 普通客户端 drain 忽略 fd
 *============================================================================*/

/**
 * rbuf 读缓冲命令消费回调类型（reactor/proactor/ntyco 引擎的统一数据入口签名）
 *
 * 引擎把收到的逻辑字节流交给本回调，回调内部解析并消费完整命令：段内命令
 * 零拷贝解析，跨段命令整命令拉平一次（见 kvs_drain_rbuf_commands）。
 *
 * @param fd 连接 fd（用于 repl 回调）
 * @param rb 读缓冲（节点队列；只认逻辑字节流，消费由本回调负责）
 * @param wb 写缓冲（输出）
 * @param want_close 输出：是否冲刷后关闭
 * @return 处理的命令数量，-1 表示协议错误
 */
typedef int (*kvs_rbuf_drain_fn)(int fd, struct rbuf *rb, DynBuf *wb, int *want_close);

/**
 * rbuf 读缓冲命令消费（段内零拷贝 + 跨段整命令拉平）
 *
 * 网络层读缓冲为节点队列（rbuf_t / struct rbuf）时的解析入口。
 * 模型：每条完整命令要么整条落在队头连续段内，要么跨段；两种情形都只用连续
 * 缓冲解析器 resp_parse_slices 出 token（与 AOF 重载同一解析器）：
 *   - 段内命令：直接在队头连续段上解析，token 零拷贝直指节点内存；
 *   - 跨段命令：量长游标只数长度 → 整命令一次 rbuf_copy 拉平进本 drain 复用的
 *     flat → 统一解析。"是否跨段"由 resp_parse_slices 的返回值免费判定，无专门
 *     扫描；命令不完整即停（队列原样保留等更多数据，分片零拷贝等待）。
 *
 * AOF / 增量复制整命令拷贝收口在 drain：dispatch 回传 journaled（写效果），
 * drain 于 consume 前把本命令"整条原始命令"喂 kvs_journal_effect——段内命令
 * 直读 head 节点连续内存，跨段命令读已拉平的 flat；除日志进自身缓冲的必要
 * 内部拷贝外无二次物化。解析层无需命令名谓词预判。
 * ENABLE_AOF 下日志语义与旧连续缓冲 drain 对齐。
 *
 * @param fd 连接 fd
 * @param rb 节点队列读缓冲
 * @param wb 写缓冲（输出）
 * @param want_close 输出：是否冲刷后关闭
 * @return 处理命令数，-1 表示协议错误
 */
int kvs_drain_rbuf_commands(int fd, struct rbuf *rb, DynBuf *wb, int *want_close);

/* 当前正被 drain 的连接的 fd（协议层瞬态上下文：drain 入口设、出口清回 -1）。
 * master 的 PSYNC 作为控制命令并入命令分发层后，repl 侧据此设 streaming 从库
 * fd——不用把 fd 传进 kvs_dispatch 签名。单线程引擎下无并发，语义可靠 */
extern int g_kvs_drain_fd;

/*=============================================================================
 *  RESP 响应常量
 *  
 *  预定义常用响应字符串及其长度
 *  避免运行时动态构建，减少开销
 *============================================================================*/

/* 简单字符串响应（Simple Strings） */
extern const char RESP_OK[];          /* +OK\r\n */
extern const char RESP_PONG[];        /* +PONG\r\n */
extern const char RESP_NIL[];         /* $-1\r\n */
extern const char RESP_ONE[];         /* :1\r\n */
extern const char RESP_ZERO[];        /* :0\r\n */

/* 错误响应（Errors） */
extern const char RESP_ERROR[];       /* -ERR error\r\n */
extern const char RESP_ERR_INV[];     /* -ERR invalid RESP protocol\r\n */
extern const char RESP_ERR_UNK[];     /* -ERR unknown command\r\n */
extern const char RESP_ERR_ARGS[];    /* -ERR wrong number of arguments\r\n */
extern const char RESP_ERR_INT[];     /* -ERR value is not an integer or out of range\r\n */
extern const char RESP_WRONGTYPE[];   /* -WRONGTYPE Operation against a key holding the wrong kind of value\r\n */

/* 响应长度常量 */
extern const int  RESP_OK_LEN, RESP_PONG_LEN, RESP_NIL_LEN;
extern const int  RESP_ONE_LEN, RESP_ZERO_LEN, RESP_ERROR_LEN;
extern const int  RESP_ERR_INV_LEN, RESP_ERR_UNK_LEN, RESP_ERR_ARGS_LEN;
extern const int  RESP_ERR_INT_LEN, RESP_WRONGTYPE_LEN;

#endif /* __KVS_PROTOCOL_H__ */