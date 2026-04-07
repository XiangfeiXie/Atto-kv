#ifndef __SERVER_H__
#define __SERVER_H__

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#define ENABLE_HTTP			0
#define ENABLE_WEBSOCKET	0
#define ENABLE_KVSTORE		1

// 动态缓冲区结构体（Redis同款，支持超长文本）
typedef struct {
    char *buf;      // 动态内存指针
    int len;        // 已使用长度
    int cap;        // 总容量
} DynBuf;

// ==================== 动态缓冲区工具函数声明（仅声明，不实现） ====================
// 初始化动态缓冲区（init_cap：初始容量，建议4096）
void dynbuf_init(DynBuf *db, int init_cap);

// 向动态缓冲区追加数据（自动扩容，返回0成功，-1失败）
int dynbuf_append(DynBuf *db, const char *data, int len);

// 清空动态缓冲区（不释放内存，仅重置长度）
void dynbuf_clear(DynBuf *db);

// 释放动态缓冲区内存（彻底清空）
void dynbuf_free(DynBuf *db);
// ==================================================================================

// 回调函数定义
typedef int (*RCALLBACK)(int fd);

// 协议处理回调（统一接口，适配io_uring/Reactor/协程）
typedef int (*msg_handler)(char *msg, int len, DynBuf *write_buf);

// 连接结构体（无冗余，无错误）
struct conn {
	int fd;
	DynBuf read_buf;   // 动态读缓冲区（自带len，无需额外rlength）
	DynBuf write_buf;  // 动态写缓冲区（自带len，无需额外wlength）

	RCALLBACK send_callback;
	union {
		RCALLBACK recv_callback;
		RCALLBACK accept_callback;
	} r_action;

	int status;
};

#if ENABLE_KVSTORE
// KV核心协议接口（正确声明）
int kvs_protocol(char *msg, int len, DynBuf *write_buf);
int resp_try_parse_full_cmd(const char *buf, int buflen);
/* 网络层读缓冲初始大小（pipeline 批处理） */
#ifndef KVS_NET_BUF_INIT
#define KVS_NET_BUF_INIT 65536
#endif
int kvs_drain_resp_commands_ex(DynBuf *rb, DynBuf *wb, msg_handler h);
int kvs_drain_resp_commands(DynBuf *rb, DynBuf *wb);
#endif

#endif