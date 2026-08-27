/*=============================================================================
 *  repl_backlog.c — 复制积压缓冲区（Replication Backlog）环形实现
 *  
 *  功能：存储最近执行的写命令，供从库断线重连时增量同步
 *  设计：固定大小环形缓冲区，新数据覆盖旧数据
 *  
 *  使用场景：
 *  1. Master 将写命令追加到 backlog
 *  2. Slave 断线重连时，发送 PSYNC <offset> 请求
 *  3. Master 检查 offset 是否在 backlog 窗口内
 *     - 在窗口内：发送 +CONTINUE + 增量数据
 *     - 不在窗口内：触发 +FULLRESYNC 全量同步
 *============================================================================*/

#include "repl_backlog.h"
#include <string.h>

/*=============================================================================
 *  全局积压缓冲区
 *  单实例，所有复制连接共享
 *============================================================================*/

static repl_backlog_t g_backlog;

/**
 * 初始化积压缓冲区
 * 清空所有状态
 */
void repl_backlog_init(void) {
    memset(&g_backlog, 0, sizeof(g_backlog));
}

/*=============================================================================
 *  追加写命令到积压缓冲区
 *  环形写入，满则覆盖最旧数据
 *============================================================================*/

/**
 * 追加 RESP 命令到积压缓冲区
 * 
 * @param resp_data RESP 格式的命令数据
 * @param len 数据长度
 * 
 * 设计要点：
 * 1. 环形写入：处理跨边界数据（wrap-around）
 * 2. 超过缓冲区大小时覆盖最旧数据
 * 3. 单线程调用（Master 事件循环），无需加锁
 * 
 * 注意：len 必须 <= REPL_BACKLOG_SIZE
 */
void repl_backlog_append(const char *resp_data, int len) {
    if (len <= 0 || len > REPL_BACKLOG_SIZE) return;

    /* 计算写入位置（处理环形边界） */
    size_t pos = g_backlog.head % REPL_BACKLOG_SIZE;
    size_t remain = REPL_BACKLOG_SIZE - pos;
    size_t wlen   = (size_t)len;

    /* 分两段写入（如果需要跨边界） */
    if (wlen <= remain) {
        memcpy(g_backlog.buf + pos, resp_data, wlen);
    } else {
        memcpy(g_backlog.buf + pos, resp_data, remain);
        memcpy(g_backlog.buf, resp_data + remain, wlen - remain);
    }
    g_backlog.head += wlen;  /* head 单调递增，永不回绕 */

    /* 更新已用大小（上限为缓冲区总大小） */
    if (g_backlog.used + wlen > REPL_BACKLOG_SIZE)
        g_backlog.used = REPL_BACKLOG_SIZE;
    else
        g_backlog.used += wlen;
}

/*=============================================================================
 *  积压缓冲区查询
 *============================================================================*/

/**
 * 获取当前 head 偏移量
 * @return 当前积压缓冲区的最大偏移量
 */
size_t repl_backlog_offset(void) {
    return g_backlog.head;
}

/**
 * 检查偏移量是否在积压窗口内
 * 
 * @param offset 要检查的偏移量
 * @return 1 有效（在窗口内），0 无效
 * 
 * 窗口范围：
 * - 缓冲区未满：[0, head]
 * - 缓冲区已满：[head - size, head]
 * 
 * 特殊设计：
 * offset == head 是"已追平"的合法位置
 * （全量快照刚做完、期间无新写入）
 * 应返回 +CONTINUE 进入 streaming，而非 +FULLRESYNC
 * 因此使用 <= 包含右边界
 */
int repl_backlog_valid(size_t offset) {
    size_t earliest;
    if (g_backlog.used < REPL_BACKLOG_SIZE)
        earliest = 0;                      /* 缓冲区未满：从 0 开始 */
    else
        earliest = g_backlog.head - REPL_BACKLOG_SIZE;  /* 缓冲区已满：最早数据位置 */

    /* offset == head 是"已追平"的合法位置 */
    return (offset >= earliest && offset <= g_backlog.head) ? 1 : 0;
}

/*=============================================================================
 *  从积压缓冲区获取数据
 *  
 *  用于 PSYNC 增量同步：从指定偏移量开始拷贝数据给从库
 *  允许在任意字节处截断，命令边界由 slave 端的 resp_parse_slices 重组
 *============================================================================*/

/**
 * 从绝对偏移量 off 拷贝 len 字节到 dst（处理环形包装）
 */
static void backlog_copy(char *dst, size_t off, size_t len) {
    size_t pos = off % REPL_BACKLOG_SIZE;
    size_t remain = REPL_BACKLOG_SIZE - pos;
    
    if (len <= remain) {
        memcpy(dst, g_backlog.buf + pos, len);
    } else {
        memcpy(dst, g_backlog.buf + pos, remain);
        memcpy(dst + remain, g_backlog.buf, len - remain);
    }
}

/**
 * 从积压缓冲区获取数据
 * 
 * @param start_offset 起始偏移量
 * @param dst 目标缓冲区
 * @param max_len 最大可拷贝字节数
 * @param out_end_offset 输出：拷贝后的新偏移量
 * @return 实际拷贝字节数，-1 表示偏移量无效
 * 
 * 设计要点：
 * 1. 不对命令边界做对齐，允许任意位置截断
 * 2. Slave 端用 RESP 解析器重组完整命令
 * 3. 一个大于缓冲区大小的命令会分多批送达
 */
int repl_backlog_fetch(size_t start_offset, char *dst, int max_len,
                        size_t *out_end_offset) {
    /* 校验偏移量是否在窗口内 */
    if (!repl_backlog_valid(start_offset))
        return -1;

    /* 计算可拷贝数据量（不超过 max_len） */
    size_t avail = g_backlog.head - start_offset;
    size_t n = (avail < (size_t)max_len) ? avail : (size_t)max_len;
    
    backlog_copy(dst, start_offset, n);
    *out_end_offset = start_offset + n;
    return (int)n;
}