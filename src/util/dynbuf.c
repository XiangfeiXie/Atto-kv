/*=============================================================================
 *  dynbuf.c — 轻量级动态缓冲区实现
 *  
 *  功能：提供动态字节缓冲区的核心操作
 *  
 *  设计原则：
 *  1. 简单高效：只有最基本的 buffer 操作
 *  2. 安全扩容：有硬上限防止内存耗尽
 *  3. 内存复用：clear 保留内存，减少分配
 *============================================================================*/

#include "dynbuf.h"   /* KVS_NET_BUF_MAX */
#include <stdlib.h>
#include <string.h>

/*=============================================================================
 *  初始化
 *============================================================================*/

/**
 * 初始化动态缓冲区
 * 
 * @param db 动态缓冲区指针
 * @param init_cap 初始容量
 * 
 * 行为：
 * - 分配 init_cap 字节的堆内存
 * - 设置 len = 0, cap = init_cap
 * 
 * 注意：
 * - 如果 init_cap <= 0，不执行任何操作（保护性检查）
 * - 调用者必须确保 db 指针有效
 * - 失败时 buf 为 NULL，len=0，cap=init_cap
 */
void dynbuf_init(DynBuf *db, int init_cap) {
    if (!db || init_cap <= 0) return;
    db->buf = (char *)malloc((size_t)init_cap);
    db->len = 0;
    db->cap = init_cap;
}

/*=============================================================================
 *  容量管理
 *============================================================================*/

/**
 * 保留/扩容缓冲区
 * 
 * @param db 动态缓冲区指针
 * @param total_cap 需要的总容量
 * @return 0 成功，-1 内存分配失败，-2 超过 KVS_NET_BUF_MAX
 * 
 * 扩容策略：
 * 1. 如果 total_cap <= 当前 cap，直接返回成功
 * 2. 否则从当前 cap 开始翻倍扩容，直到 >= total_cap
 * 3. 如果新 cap > KVS_NET_BUF_MAX，返回 -2（拒绝）
 * 4. 使用 realloc 扩容，保留原有数据
 * 
 * 安全设计：
 * - KVS_NET_BUF_MAX 硬上限防止恶意客户端耗尽内存
 * - 拒绝而非静默截断，保证数据完整性
 */
int dynbuf_reserve(DynBuf *db, int total_cap) {
    if (!db || total_cap <= 0) return -1;
    if (total_cap <= db->cap) return 0;

    /* 计算新容量（翻倍策略，从 16 开始） */
    int new_cap = db->cap > 0 ? db->cap : 16;
    while (new_cap < total_cap) new_cap *= 2;
    
    /* 检查是否超过硬上限 */
    if (new_cap > KVS_NET_BUF_MAX) return -2;

    /* 执行扩容 */
    char *nb = (char *)realloc(db->buf, (size_t)new_cap);
    if (!nb) return -1;

    db->buf = nb;
    db->cap = new_cap;
    return 0;
}

/*=============================================================================
 *  数据操作
 *============================================================================*/

/**
 * 追加数据到缓冲区
 * 
 * @param db 动态缓冲区指针
 * @param data 待追加的数据指针
 * @param len 待追加的数据长度
 * @return 0 成功，负数表示错误（详见 dynbuf_reserve）
 * 
 * 流程：
 * 1. 参数检查
 * 2. 确保有足够空间（自动扩容）
 * 3. memcpy 追加数据
 * 4. 更新 len
 * 
 * 性能优化：
 * - 如果预期多次追加，可先用 dynbuf_reserve 预分配
 * - 减少扩容次数
 */
int dynbuf_cat(DynBuf *db, const char *data, int len) {
    if (!db || !data || len <= 0) return -1;
    
    /* 确保有足够空间（自动扩容） */
    int rc = dynbuf_reserve(db, db->len + len);
    if (rc < 0) return rc;
    
    /* 追加数据 */
    memcpy(db->buf + db->len, data, (size_t)len);
    db->len += len;
    return 0;
}

/*=============================================================================
 *  清理与释放
 *============================================================================*/

/**
 * 清空缓冲区（保留内存）
 * 
 * @param db 动态缓冲区指针
 * 
 * 行为：
 * - 设置 len = 0
 * - 不释放内存（保留 cap）
 * - 下次写入直接覆盖
 * 
 * 优点：
 * - 避免频繁分配/释放
 * - 适合连接复用场景（Slab 风格）
 * - 减少内存碎片
 * 
 * 使用场景：
 * - 每轮命令处理后清空写缓冲
 * - 连接复用时的缓冲重置
 */
void dynbuf_clear(DynBuf *db) {
    if (!db) return;
    db->len = 0;
}

/**
 * 释放缓冲区内存
 * 
 * @param db 动态缓冲区指针
 * 
 * 行为：
 * - 释放 buf 指向的堆内存
 * - 设置 buf = NULL, len = 0, cap = 0
 * 
 * 调用时机：
 * - 连接关闭时释放缓冲
 * - 程序退出时清理资源
 * 
 * 安全设计：
 * - 允许重复调用（free(NULL) 是安全的）
 * - 释放后所有字段归零
 */
void dynbuf_free(DynBuf *db) {
    if (!db) return;
    free(db->buf);
    db->buf = NULL;
    db->len = 0;
    db->cap = 0;
}