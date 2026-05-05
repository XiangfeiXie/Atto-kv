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

// ==================== 动态缓冲区工具函数声明 ====================
void dynbuf_init(DynBuf *db, int init_cap);
int  dynbuf_append(DynBuf *db, const char *data, int len);
void dynbuf_clear(DynBuf *db);
void dynbuf_free(DynBuf *db);
// ==================================================================================

// 回调函数定义
typedef int (*RCALLBACK)(int fd);

// 协议处理回调（统一接口，适配io_uring/Reactor/协程）
typedef int (*msg_handler)(char *msg, int len, DynBuf *write_buf);

// 连接结构体
struct conn {
	int fd;
	DynBuf read_buf;   // 动态读缓冲区
	DynBuf write_buf;  // 动态写缓冲区
	int r_off;         // 读缓冲已消费偏移（避免 memmove）
	int w_off;         // 写缓冲已发送偏移（避免 memmove）

	RCALLBACK send_callback;
	union {
		RCALLBACK recv_callback;
		RCALLBACK accept_callback;
	} r_action;

	int status;
};

#if ENABLE_KVSTORE
// KV核心协议接口
int kvs_protocol(char *msg, int len, DynBuf *write_buf);

/* 网络层读缓冲初始大小（pipeline 批处理） */
#ifndef KVS_NET_BUF_INIT
#define KVS_NET_BUF_INIT 65536
#endif

/* 偏移指针版本：rb + r_off 表示有效数据起点，内部不做 memmove */
int kvs_drain_resp_commands_off(DynBuf *rb, int *r_off, DynBuf *wb);
#endif

#endif