/*=============================================================================
 *  repl_incr.c — 增量同步公共部件（积压缓冲 + 引擎接线）
 *
 *  由 repl_backlog.c（积压缓冲环形实现）与 repl_incr.c（引擎接线）合并，
 *  两者都属于增量同步的控制面：
 *
 *  职责：
 *  1. 复制积压缓冲区：存储最近执行的写命令，供从库断线重连时按偏移量补发
 *  2. 引擎接线：定义 repl_on_close / repl_tick（引擎→repl 直接函数，按角色
 *     分发），管理 repl_engine_* 访问点（复制调用引擎，指针做运行时引擎选择）
 *
 *  协作关系：
 *  - repl_engine_*：引擎在各自 *start() 里赋值为自己的实现，repl 调用
 *  - repl_on_close / repl_tick：直接函数，引擎无条件调用，内部按角色判断
 *  - 全量交接 token：repl_fullsync.c
 *
 *  使用场景（backlog）：
 *  1. Master 将写命令追加到 backlog
 *  2. Slave 断线重连时，发送 PSYNC <offset> 请求
 *  3. Master 检查 offset 是否在 backlog 窗口内
 *     - 在窗口内：发送 +CONTINUE + 增量数据
 *     - 不在窗口内：触发 +FULLRESYNC 全量同步
 *============================================================================*/

#include "repl.h"
#include "kvstore.h"

#include <string.h>

/*=============================================================================
 *  Part I — 复制积压缓冲区（原 repl_backlog.c）
 *
 *  功能：存储最近执行的写命令，供从库断线重连时增量同步
 *  设计：固定大小环形缓冲区，新数据覆盖旧数据
 *============================================================================*/

/*-------------------------------------------------------------------------
 *  全局积压缓冲区
 *  单实例，所有复制连接共享
 *-------------------------------------------------------------------------*/

static repl_backlog_t g_backlog;

/**
 * 初始化积压缓冲区
 * 清空所有状态
 */
void repl_backlog_init(void) {
    memset(&g_backlog, 0, sizeof(g_backlog));
}

/*-------------------------------------------------------------------------
 *  追加写命令到积压缓冲区
 *  环形写入，满则覆盖最旧数据
 *-------------------------------------------------------------------------*/

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

/*-------------------------------------------------------------------------
 *  积压缓冲区查询
 *-------------------------------------------------------------------------*/

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

/*-------------------------------------------------------------------------
 *  从积压缓冲区获取数据
 *
 *  用于 PSYNC 增量同步：从指定偏移量开始拷贝数据给从库
 *  允许在任意字节处截断，命令边界由 slave 端的 resp_parse_slices 重组
 *-------------------------------------------------------------------------*/

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

/*=============================================================================
 *  Part II — 引擎接线（原 repl_incr.c）
 *============================================================================*/

/*-------------------------------------------------------------------------
 *  引擎访问点指针（repl → engine）
 *
 *  由引擎在各自 *start() 里赋为自己的实现（运行时引擎选择）
 *  repl 模块通过这些指针调用引擎的功能，不感知 reactor/proactor 差异
 *
 *  见 kvstore.h 中的注释说明
 *-------------------------------------------------------------------------*/

int  (*repl_engine_register_rbuf)(int fd, kvs_rbuf_drain_fn handler) = NULL; /* 收编连接 + 挂 handler */
int  (*repl_engine_append)(int fd, const char *data, int len) = NULL;  /* 按 fd 向连接写数据 */

/*-------------------------------------------------------------------------
 *  引擎调用点（engine → repl）：直接函数
 *
 *  与 repl_engine_* 不同，这里无需指针——行为只由 repl_role 决定、
 *  启动时固定、调用时可查。引擎无条件调用，内部按角色分发
 *
 *  调用图（控制面跳转的根）：
 *
 *    引擎 conn_release ──> repl_on_close(fd) ──┬─ MASTER → repl_incr_master_on_close
 *                                              └─ SLAVE  → repl_incr_slave_on_close
 *    引擎主循环每轮 ──> repl_tick() ────────────── SLAVE  → repl_incr_slave_tick
 *                                                   （master 无 tick 动作）
 *
 *  调用者：引擎的 conn_release / 主循环
 *-------------------------------------------------------------------------*/

/**
 * 连接关闭通知：引擎释放任意连接时调用
 * 转发给 master/slave 各自的状态处理，普通客户端 fd 均为 no-op
 */
void repl_on_close(int fd) {
    if (kvs_config.repl_role == REPL_ROLE_MASTER)
        repl_incr_master_on_close(fd);   /* Master 端清理 streaming 从库 */
    else if (kvs_config.repl_role == REPL_ROLE_SLAVE)
        repl_incr_slave_on_close(fd);    /* Slave 端安排重连 */
}

/**
 * 事件循环 tick：引擎每轮调用一次
 * 仅 slave 需要（断线重连 / 全量交接推进），master 无需 tick
 */
void repl_tick(void) {
    if (kvs_config.repl_role == REPL_ROLE_SLAVE)
        repl_incr_slave_tick();
}
