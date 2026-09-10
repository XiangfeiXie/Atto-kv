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
 *  - PSYNC 握手走主服务端口。PSYNC 作为控制命令并入命令分发层（kvstore_dispatch.c
 *    的 PSYNC 分支，gate = repl_engine_append && role==master），本文件导出
 *    repl_master_psync_handle 供 dispatch 调用；当前连接 fd 经全局 g_kvs_drain_fd
 *    （协议层 drain 入口设置）获取，不扩 dispatch 签名
 *  - 只有一台从机（设计上），streaming 状态以 g_slave_fd >= 0 表示
 *  - 决策：offset 在窗口内（且 incr on）→ +CONTINUE 进入 streaming；否则 →
 *    +FULLRESYNC 走全量
 *  - +CONTINUE 进入 streaming 后**不切换 handler**（P4：不设 streaming guard——
 *    从库若发来可解析数据照常当普通命令执行，协议错则断链）。连接默认入口
 *    一律是 kvs_drain_rbuf_commands，PSYNC 仅在命令分发层被接管
 *    （repl_engine_register_rbuf 仅 slave 出站链路使用）
 *  - offset 交接契约：+FULLRESYNC 回复不带 offset（从库只做前缀匹配，从不解析
 *    数字），续传权威在数据面流头 token（g_repl_snapshot_offset）
 *  - 握手回复（+CONTINUE/+FULLRESYNC/错误）直接 dynbuf_cat 进引擎 wb（drain
 *    返回后引擎冲刷，与普通命令回复同路径）；streaming 期间的实时传播仍用
 *    repl_engine_append
 *============================================================================*/

#include "repl.h"
#include "kvstore.h"
#include "protocol.h"
#include <stdio.h>
#include <string.h>

/*=============================================================================
 *  模块状态：唯一 streaming 从机
 *
 *  g_slave_fd 的语义：
 *  - >= 0：有一个从库处于 streaming 状态（PSYNC +CONTINUE 握手成功），
 *          传播路径 repl_incr_propagate 只推送给它
 *  - -1  ：当前无 streaming 从库
 *============================================================================*/

static int g_slave_fd = -1;   /* streaming 从库的引擎连接 fd；-1=无 */

/*=============================================================================
 *  写命令传播（Master 热路径）
 *  向 streaming 从库推送写命令
 *============================================================================*/

/**
 * 向从库传播写命令
 *
 * 调用位置：每次成功的写命令后（SET/DEL/FLUSHALL/EXPIRE 等）
 * 调用者：kvs_journal_effect（kvstore_dispatch.c）
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
    if (g_slave_fd < 0)
        return;
    if (repl_engine_append)
        repl_engine_append(g_slave_fd, resp_data, len);
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
    if (g_slave_fd != fd) return;
    printf("[INCR-MASTER] slave fd=%d link closed\n", fd);
    g_slave_fd = -1;
}

/*=============================================================================
 *  PSYNC 握手处理
 *============================================================================*/

/**
 * 分支一：+CONTINUE，进入流式
 *
 * 功能：
 * 1. 记录 streaming 从库 fd（传播路径只推送给它）
 * 2. 回复 +CONTINUE + backlog 补发（从 offset 追到当前 head）
 *
 * 从库在 streaming 期间不应再发数据；若真发来，连接默认入口仍是
 * kvs_drain_rbuf_commands，可解析的命令照常执行、协议错则断链（P4，不切 guard）。
 *
 * 调用：repl_master_psync_handle 判定 offset 在 backlog 窗口内时
 */
static void master_enter_streaming(int fd, size_t off, DynBuf *wb) {
    g_slave_fd = fd;

    /* 回复 + 回补直接追加进引擎 wb（= 连接发送缓冲，drain 返回后引擎冲刷）。
     * 前置：引擎在 drain 入口保证 w_off==0（wb 物理尾部 == 逻辑尾部），
     * 否则 dynbuf_cat 会写到已发前缀区造成乱序——每条普通命令回复都依赖它 */
    dynbuf_cat(wb, "+CONTINUE\r\n", 11);

    char buf[65536];
    size_t o = off, target = repl_backlog_offset();
    while (o < target) {
        size_t next;
        int n = repl_backlog_fetch(o, buf, (int)sizeof(buf), &next);
        if (n <= 0) break;
        if (dynbuf_cat(wb, buf, n) < 0) break;   /* OOM 降级：截断回补 */
        o = next;
    }
    printf("[INCR-MASTER] fd=%d streaming (backfill %zu -> %zu)\n",
           fd, off, o);
}

/**
 * 分支二：+FULLRESYNC，走数据面全量
 *
 * 功能：
 * 1. 回复 +FULLRESYNC（从库判定信号，只做前缀匹配，不带 offset）
 * 2. 当场 fork 数据面传输子进程（数据面端口备好），从库随后走 transport 全量
 * 3. 关闭控制面连接
 *
 * offset 交接契约：续传权威在数据面——trigger 在 fork 时以 repl_backlog_offset()
 * 作传输流头 token，从库加载后按它复位并重新 PSYNC（g_repl_snapshot_offset）。
 * 本回复文本不带 offset，避免"回复文本里的数字"这一从库从不解析的第二份拷贝。
 *
 * 调用：repl_master_psync_handle 判定 offset 过期（或 incr off 无 backlog）时
 */
static void master_start_fullsync(DynBuf *wb) {
    dynbuf_cat(wb, "+FULLRESYNC\r\n", 13);
    repl_fullsync_master_trigger();   /* fork 传输子进程（数据面端口，流头带 offset） */
    /* 调用方返回 DISPATCH_CLOSE → drain 置 want_close → 引擎冲刷回复后关连接 */
}

/**
 * Master 端 PSYNC 命令处理（命令分发层入口，dispatch 的 PSYNC 分支调用）
 *
 * 已确认命令名为 PSYNC（dispatch 已大写）时：
 * 1. 解析 offset（纯数字校验；argc/offset 非法 → 写错误并返回 DISPATCH_CLOSE）
 * 2. 判定 offset 是否在 backlog 窗口内，二选一：
 *    - 窗口内（且 incr on）→ master_enter_streaming（+CONTINUE），返回 DISPATCH_OK
 *    - 窗口外 / incr off 无 backlog → master_start_fullsync（+FULLRESYNC），返回
 *      DISPATCH_CLOSE（drain 置 want_close → 引擎冲刷 +FULLRESYNC 后关连接）
 *
 * 当前连接 fd 取协议层全局 g_kvs_drain_fd（drain 入口设置），用于记 streaming
 * 从库；回复直接写 wb（= 引擎连接缓冲，drain 返回后冲刷）。不碰读缓冲、不置
 * journaled（PSYNC 无写效果）。
 */
dispatch_result_t repl_master_psync_handle(kvs_token_t *tokens, int argc,
                                           DynBuf *wb) {
    int fd = g_kvs_drain_fd;

    if (argc != 2) {
        fprintf(stderr, "[INCR-MASTER] fd=%d 非法 PSYNC 命令\n", fd);
        dynbuf_cat(wb, RESP_ERR_INV, RESP_ERR_INV_LEN);
        return DISPATCH_CLOSE;
    }

    /* 解析 offset（必须是纯数字） */
    size_t off = 0;
    for (int i = 0; i < tokens[1].len; i++) {
        char c = tokens[1].ptr[i];
        if (c < '0' || c > '9') {
            fprintf(stderr, "[INCR-MASTER] fd=%d bad offset\n", fd);
            dynbuf_cat(wb, RESP_ERR_INT, RESP_ERR_INT_LEN);
            return DISPATCH_CLOSE;
        }
        off = off * 10 + (size_t)(c - '0');
    }

    printf("[INCR-MASTER] PSYNC %zu from fd=%d (current=%zu)\n",
           off, fd, repl_backlog_offset());

    /*-------------------------------------------------------------------------
     *  决策：offset 是否在 backlog 窗口内？
     *  - 窗口内（且 incr on）→ +CONTINUE：进入 streaming（连接保留，继续管道）
     *  - 窗口外 / incr off（无 backlog）→ +FULLRESYNC：走数据面全量（关连接）
     *-------------------------------------------------------------------------*/
    if (kvs_config.incr_sync && repl_backlog_valid(off)) {
        master_enter_streaming(fd, off, wb);
        return DISPATCH_OK;
    }
    master_start_fullsync(wb);
    return DISPATCH_CLOSE;
}
