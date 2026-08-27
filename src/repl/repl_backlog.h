/*=============================================================================
 *  repl_backlog.h — 复制积压缓冲区（Replication Backlog）
 *  
 *  功能：保存原始 RESP 写命令，供从库断线后按偏移量补发
 *  大小：1MB 环形缓冲区
 *  
 *  设计要点：
 *  1. 只属于增量同步（incr_sync），与全量同步正交
 *  2. 偏移量是绝对写偏移，单调递增不重置
 *  3. 环形缓冲区，新数据覆盖旧数据
 *  4. 单线程无锁（Master 事件循环）
 *============================================================================*/

#ifndef __REPL_BACKLOG_H__
#define __REPL_BACKLOG_H__

#include <stddef.h>

/*=============================================================================
 *  常量定义
 *============================================================================*/

/**
 * 积压缓冲区大小
 * 1MB 对于大多数场景足够：
 * - 存储最近几秒到几分钟的写命令
 * - 覆盖从库短暂断线重连的时间窗口
 * 
 * 可根据实际 workload 调整
 */
#define REPL_BACKLOG_SIZE      (1024 * 1024)   /* 1 MB */

/*=============================================================================
 *  积压缓冲区结构
 *  
 *  内存布局：
 *  +------------------------------------------+
 *  |           buf[REPL_BACKLOG_SIZE]          |
 *  +------------------------------------------+
 *  
 *  head: 绝对写偏移（单调递增）
 *  used: 当前有效数据量（<= REPL_BACKLOG_SIZE）
 *  
 *  环形位置计算：pos = head % REPL_BACKLOG_SIZE
 *  
 *  窗口范围：
 *  - 未满：[0, head]
 *  - 已满：[head - size, head]
 *============================================================================*/

typedef struct {
    char   buf[REPL_BACKLOG_SIZE];  /* 环形缓冲区 */
    size_t head;                    /* 绝对写偏移（单调递增，永不重置） */
    size_t used;                    /* 当前存量字节数（<= REPL_BACKLOG_SIZE） */
} repl_backlog_t;

/*=============================================================================
 *  公开 API
 *============================================================================*/

/**
 * 初始化积压缓冲区
 * 清空所有状态，在服务器启动时调用
 */
void repl_backlog_init(void);

/**
 * 追加写命令到积压缓冲区
 * 
 * @param resp_data RESP 格式的命令数据（如 "*3\r\n$3\r\nSET\r\n..."）
 * @param len 数据长度
 * 
 * 特性：
 * - 环形写入，满则覆盖最旧数据
 * - 单线程调用（Master 事件循环），无锁
 * - len 必须 <= REPL_BACKLOG_SIZE
 * 
 * 调用位置：每次成功的写命令后（SET/DEL/FLUSHALL/EXPIRE 等）
 */
void repl_backlog_append(const char *resp_data, int len);

/**
 * 从积压缓冲区获取数据
 * 
 * @param start_offset 起始偏移量（来自 Slave 的 PSYNC 请求）
 * @param dst 目标缓冲区（用于发送给 Slave）
 * @param max_len 最大可拷贝字节数
 * @param out_end_offset 输出：拷贝后的新偏移量
 * @return 实际拷贝字节数，-1 表示偏移量无效（需要 FULLRESYNC）
 * 
 * 设计要点：
 * - 允许在任意字节处截断（不对齐命令边界）
 * - Slave 端用 RESP 解析器（resp_parse_slices）重组完整命令
 * - 一个大于缓冲区的命令会分多批送达
 * 
 * 使用场景：PSYNC 增量同步
 */
int repl_backlog_fetch(size_t start_offset, char *dst, int max_len,
                       size_t *out_end_offset);

/**
 * 获取当前 head 偏移量
 * @return 积压缓冲区当前最大偏移量
 * 
 * 用途：
 * - 生成 Master 的复制偏移量（INFO replication）
 * - Slave 的 PSYNC 请求中使用
 */
size_t repl_backlog_offset(void);

/**
 * 检查偏移量是否在积压窗口内
 * @param offset 要检查的偏移量
 * @return 1 有效（在窗口内），0 无效（需要 FULLRESYNC）
 * 
 * 窗口范围：
 * - 缓冲区未满：[0, head]
 * - 缓冲区已满：[head - REPL_BACKLOG_SIZE, head]
 * 
 * 特殊设计：
 * offset == head 是"已追平"的合法位置
 * （全量快照刚做完、期间无新写入）
 * 应返回 +CONTINUE 进入 streaming，而非 +FULLRESYNC
 * 因此使用 <= 包含右边界
 * 
 * 使用场景：处理 PSYNC 请求时判断是否需要全量同步
 */
int repl_backlog_valid(size_t offset);

#endif /* __REPL_BACKLOG_H__ */