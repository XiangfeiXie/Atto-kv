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
 *  3. 兼容 RESP 数组格式和内联命令格式
 *============================================================================*/

#ifndef __KVS_PROTOCOL_H__
#define __KVS_PROTOCOL_H__

#include <stdint.h>
#include "dynbuf.h"

/*=============================================================================
 *  协议常量
 *============================================================================*/

#define KVS_MAX_TOKENS         128                /* 单条命令最大参数个数 */
#define KVS_MAX_BULK_LEN       (512 * 1024 * 1024) /* Bulk String 最大长度 512MB */
#define KVS_MAX_INLINE_LEN     65536              /* 内联命令最大长度 64KB */
#define KVS_RB_COMPACT_RATIO   2                  /* 读缓冲压缩阈值（已消费 > cap/2） */
#define BUF_INIT_CAP           4096               /* 初始缓冲容量 4KB */

/*=============================================================================
 *  零拷贝 Token 切片
 *  
 *  只引用原始数据中的位置和长度，不复制数据
 *  所有 token 都指向 RESP 解析缓冲区中的原始位置
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
 *  通过 *start(port, handler) 参数注入到 proactor/reactor
 *  
 *  设计说明：
 *  - fd 参数让 handler 能回调到引擎（repl_engine_* 访问器）
 *  - 复制模块的 per-connection handler 使用 fd
 *  - 普通客户端 drain 忽略 fd
 *============================================================================*/

/**
 * 命令解析回调函数类型
 * 
 * @param fd 连接文件描述符（用于 repl 回调）
 * @param rb 读缓冲（输入）
 * @param r_off 已消费偏移（输入/输出）
 * @param wb 写缓冲（输出）
 * @param want_close 输出：是否需要在发送完成后关闭连接
 * @return 处理的命令数量，-1 表示协议错误
 */
typedef int (*kvs_drain_fn)(int fd, DynBuf *rb, int *r_off, DynBuf *wb, int *want_close);

/**
 * 默认命令解析函数（RESP + 内联命令 fallback）
 * 
 * 功能：
 * 1. 优先解析 RESP 数组格式（*N\r\n$L\r\n...）
 * 2. 失败时 fallback 到内联命令格式（PING\r\n）
 * 3. 支持流式解析（不完整数据返回 0）
 * 4. 支持协议错误检测（返回 -1）
 * 
 * @param fd 连接 fd（可用于 repl 回调）
 * @param rb 读缓冲
 * @param r_off 读偏移
 * @param wb 写缓冲
 * @param want_close 关闭标志
 * @return 处理的命令数，-1 表示协议错误
 */
int kvs_drain_resp_commands_off(int fd, DynBuf *rb, int *r_off, DynBuf *wb, int *want_close);

/*=============================================================================
 *  遗留兼容接口
 *  
 *  仅供未启用的 ntyco 引擎使用（其 msg_handler 签名匹配）
 *  proactor/reactor 引擎使用 kvs_drain_resp_commands_off
 *  
 *  注意：新代码不应使用此接口
 *============================================================================*/

int kvs_protocol_batch(char *msg, int len, DynBuf *write_buf);

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