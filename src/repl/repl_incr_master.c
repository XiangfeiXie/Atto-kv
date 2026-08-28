/*=============================================================================
 *  repl_incr_master.c — Master 端增量同步
 *  
 *  职责：
 *  1. 处理从库的 PSYNC 请求（握手协议）
 *  2. 管理唯一的从库连接状态
 *  3. 向 streaming 从库推送写命令
 *  4. 判断增量或全量同步
 *  
 *  设计概要：
 *  - PSYNC 握手走主服务端口，由引擎按 per-conn handler 调度
 *  - 只有一台从机（设计上），streaming 状态以 g_slave.fd >= 0 表示
 *  - 决策点（master_psync_handle）：offset 在窗口内（且 incr on）→ +CONTINUE
 *    进入 streaming；否则 → +FULLRESYNC 走数据面全量
 *  - offset 交接契约：+FULLRESYNC 回复文本中的 offset 仅信息性，从库实际以
 *    数据面流头 token（g_repl_snapshot_offset）为权威续传 offset
 *  - 推送用 repl_engine_append 追加到引擎连接缓冲
 *============================================================================*/

#include "repl.h"
#include "kvstore.h"
#include "protocol.h"
#include <stdio.h>
#include <string.h>

/*=============================================================================
 *  模块状态：唯一 streaming 从机
 *
 *  g_slave.fd 的语义：
 *  - >= 0：有一个从库处于 streaming 状态（PSYNC +CONTINUE 握手成功），
 *          传播路径 repl_incr_propagate 只推送给它
 *  - -1  ：当前无 streaming 从库
 *============================================================================*/

static struct {
    int fd;         /* 引擎连接表的索引键；-1 表示无 streaming 从库 */
} g_slave;

/*=============================================================================
 *  写命令传播（Master 热路径）
 *  向 streaming 从库推送写命令
 *============================================================================*/

/**
 * 向从库传播写命令
 * 
 * 调用位置：每次成功的写命令后（SET/DEL/FLUSHALL/EXPIRE 等）
 * 调用者：kvs_repl_incr_hook（kvstore_internal.h）
 * 
 * @param resp_data RESP 格式的命令数据
 * @param len 数据长度
 * 
 * 设计要点：
 * 1. 只推送给 streaming 状态的从库
 * 2. 通过 repl_engine_append 追加到引擎发送缓冲
 * 3. 热路径函数，应尽量高效
 */
void repl_incr_propagate(const char *resp_data, int len) {
    if (g_slave.fd < 0)
        return;
    if (repl_engine_append)
        repl_engine_append(g_slave.fd, resp_data, len);
}

/*=============================================================================
 *  连接管理
 *============================================================================*/

/**
 * Master 端连接关闭回调
 * 
 * 功能：引擎释放从库连接时清理状态
 * 
 * 调用者：引擎的 conn_release（通过 repl_on_close 函数指针）
 * 
 * @param fd 被释放的连接 fd
 */
void repl_incr_master_on_close(int fd) {
    if (g_slave.fd != fd) return;
    printf("[INCR-MASTER] slave fd=%d link closed\n", fd);
    g_slave.fd = -1;
}

/**
 * Streaming 从库的 handler
 * 
 * 功能：
 * 1. 从库在 streaming 期间不应再发数据
 * 2. 任何输入视为连接断开
 * 
 * 调用者：引擎（连接已切换为该 handler）
 */
int repl_incr_master_slave_handler(int fd, DynBuf *rb, int *r_off,
                                   DynBuf *wb, int *want_close) {
    (void)wb; (void)want_close;
    if (rb->len > 0) {   /* rb->len 即 r_off 起未消费尾部 */
        printf("[INCR-MASTER] slave fd=%d sent unexpected data, drop\n", fd);
        if (repl_engine_release)
            repl_engine_release(fd);
    }
    return 0;
}

/*=============================================================================
 *  PSYNC 握手处理
 *============================================================================*/

/**
 * 分支一：+CONTINUE，进入流式
 *
 * 功能：
 * 1. 记录 streaming 从库 fd
 * 2. 切换 handler（从库在 streaming 期间不应再发数据）
 * 3. 回复 +CONTINUE + backlog 补发（从 offset 追到当前 head）
 *
 * 调用：master_psync_handle 判定 offset 在 backlog 窗口内时
 */
static void master_enter_streaming(int fd, size_t off) {
    g_slave.fd = fd;

    if (repl_engine_register)
        repl_engine_register(fd, repl_incr_master_slave_handler);

    if (repl_engine_append) {
        repl_engine_append(fd, "+CONTINUE\r\n", 11);

        char buf[65536];
        size_t o = off, target = repl_backlog_offset();
        while (o < target) {
            size_t next;
            int n = repl_backlog_fetch(o, buf, (int)sizeof(buf), &next);
            if (n <= 0) break;
            if (repl_engine_append(fd, buf, n) != 0) break;
            o = next;
        }
        printf("[INCR-MASTER] fd=%d streaming (backfill %zu -> %zu)\n",
               fd, off, o);
    }
}

/**
 * 分支二：+FULLRESYNC，走数据面全量
 *
 * 功能：
 * 1. 回复 +FULLRESYNC <offset>
 * 2. 当场 fork 数据面传输子进程（数据面端口备好），从库随后走 transport 全量
 * 3. 关闭控制面连接
 *
 * offset 交接契约：
 * 回复文本中的 offset 仅信息性；从库实际以数据面流头 token
 * （g_repl_snapshot_offset）为权威续传 offset——两者在同一时刻取同一值。
 *
 * 调用：master_psync_handle 判定 offset 过期（或 incr off 无 backlog）时
 */
static void master_start_fullsync(int fd, int *want_close) {
    char resp[64];
    int rlen = snprintf(resp, sizeof(resp), "+FULLRESYNC %zu\r\n",
                        repl_backlog_offset());
    if (repl_engine_append)
        repl_engine_append(fd, resp, rlen);
    repl_fullsync_master_trigger();   /* fork 传输子进程（数据面端口） */
    if (want_close) *want_close = 1;   /* 引擎冲刷回复后关闭 */
}

/**
 * 处理 PSYNC 命令（决策点）
 *
 * @param fd 连接 fd
 * @param rb 读缓冲
 * @param r_off 读偏移
 * @param wb 写缓冲
 * @param want_close 关闭标志
 * @return 1 已接管（PSYNC 已处理），0 非 PSYNC 或不完整
 *
 * 流程：
 * 1. 解析 PSYNC 命令，提取 offset
 * 2. 判定 offset 是否在 backlog 窗口内，二选一：
 *    - 窗口内（且 incr on）→ master_enter_streaming（+CONTINUE）
 *    - 窗口外 / incr off 无 backlog → master_start_fullsync（+FULLRESYNC）
 */
static int master_psync_handle(int fd, DynBuf *rb, int *r_off,
                               DynBuf *wb, int *want_close) {
    const char *b = rb->buf + *r_off;
    int rem = rb->len;   /* rb->len 即 r_off 起未消费尾部 */

    /* 解析 PSYNC 命令 */
    kvs_token_t tokens[3];
    int argc = 0;
    int cmd_len = resp_parse_slices(b, rem, tokens, 3, &argc);
    if (cmd_len <= 0)
        return 0;   /* 数据不完整，等待更多 */

    /* 验证命令格式 */
    if (argc != 2 || !TOKEN_EQ(tokens[0], "PSYNC")) {
        fprintf(stderr, "[INCR-MASTER] fd=%d 前缀命中但非法 PSYNC\n", fd);
        *r_off += cmd_len;
        rb->len -= cmd_len;   /* 维护不变量：len 为 r_off 起未消费尾部 */
        dynbuf_cat(wb, RESP_ERR_INV, RESP_ERR_INV_LEN);
        if (want_close) *want_close = 1;
        return 1;
    }

    *r_off += cmd_len;   /* 消费 PSYNC，后续数据走 slave handler */
    rb->len -= cmd_len;  /* 维护不变量：len 为 r_off 起未消费尾部 */

    /* 解析 offset（必须是纯数字） */
    size_t off = 0;
    for (int i = 0; i < tokens[1].len; i++) {
        char c = tokens[1].ptr[i];
        if (c < '0' || c > '9') {
            fprintf(stderr, "[INCR-MASTER] fd=%d bad offset\n", fd);
            dynbuf_cat(wb, RESP_ERR_INT, RESP_ERR_INT_LEN);
            if (want_close) *want_close = 1;
            return 1;
        }
        off = off * 10 + (size_t)(c - '0');
    }

    printf("[INCR-MASTER] PSYNC %zu from fd=%d (current=%zu)\n",
           off, fd, repl_backlog_offset());

    /*-------------------------------------------------------------------------
     *  决策：offset 是否在 backlog 窗口内？
     *  - 窗口内（且 incr on）→ +CONTINUE：进入 streaming
     *  - 窗口外 / incr off（无 backlog）→ +FULLRESYNC：走数据面全量
     *-------------------------------------------------------------------------*/
    if (kvs_config.incr_sync && repl_backlog_valid(off))
        master_enter_streaming(fd, off);
    else
        master_start_fullsync(fd, want_close);

    return 1;
}

/*=============================================================================
 *  主端口默认 Handler（注入给引擎）
 *============================================================================*/

/**
 * Master 主端口的默认 client handler
 * 
 * 功能：
 * 1. 快速探测 PSYNC 命令（15 字节 memcmp 前缀匹配）
 * 2. 命中 → master_psync_handle 处理
 * 3. 未命中 → 委托 kvs_drain_resp_commands_off 处理普通命令
 * 
 * 注入方式：kvstore.c 注入给 proactor/reactor 作为默认 handler
 * 
 * 性能考虑：
 * - 每批收包一次 15 字节 memcmp，代价可忽略
 * - 命中才进完整解析，否则原样委托普通客户端 drain
 * - 不完整数组也会等待
 * 
 * @param fd 连接 fd
 * @param rb 读缓冲
 * @param r_off 读偏移
 * @param wb 写缓冲
 * @param want_close 关闭标志
 * @return 处理的命令数，-1 表示协议错误
 */
int repl_incr_master_client_handler(int fd, DynBuf *rb, int *r_off,
                                    DynBuf *wb, int *want_close) {
    /* 快速前缀探测：*2\r\n$5\r\nPSYNC\r\n（15 字节） */
    const char *b = rb->buf + *r_off;
    int rem = rb->len;   /* rb->len 即 r_off 起未消费尾部 */
    if (rem >= 15 && memcmp(b, "*2\r\n$5\r\nPSYNC\r\n", 15) == 0) {
        if (master_psync_handle(fd, rb, r_off, wb, want_close))
            return 0;  /* PSYNC 已处理，当前批次无普通命令 */
    }
    /* 普通命令：委托默认解析器 */
    return kvs_drain_resp_commands_off(fd, rb, r_off, wb, want_close);
}