/*=============================================================================
 *  repl_incr.c — 增量同步公共状态 + 引擎接线
 *
 *  职责：
 *  1. 定义 repl_on_close / repl_tick（引擎→repl 直接函数，按角色分发）
 *  2. 管理 repl_engine_* 访问点（复制调用引擎，指针做运行时引擎选择）
 *
 *  协作关系：
 *  - repl_engine_*：引擎在各自 *start() 里赋值为自己的实现，repl 调用
 *  - repl_on_close / repl_tick：直接函数，引擎无条件调用，内部按角色判断
 *
 *  依赖：
 *  - backlog：repl_backlog.c
 *  - 全量交接 token：repl_fullsync.c
 *============================================================================*/

#include "repl_incr.h"
#include "kvstore.h"

/*=============================================================================
 *  引擎访问点指针（repl → engine）
 *
 *  由引擎在各自 *start() 里赋为自己的实现（运行时引擎选择）
 *  repl 模块通过这些指针调用引擎的功能，不感知 reactor/proactor 差异
 *
 *  见 kvstore.h 中的注释说明
 *============================================================================*/

int  (*repl_engine_register)(int fd, kvs_drain_fn handler) = NULL;  /* 注册连接 */
int  (*repl_engine_append)(int fd, const char *data, int len) = NULL; /* 追加数据 */
void (*repl_engine_release)(int fd) = NULL;                           /* 释放连接 */

/*=============================================================================
 *  引擎调用点（engine → repl）：直接函数
 *
 *  与 repl_engine_* 不同，这里无需指针——行为只由 repl_role 决定、
 *  启动时固定、调用时可查。引擎无条件调用，内部按角色分发
 *
 *  调用者：引擎的 conn_release / 主循环
 *============================================================================*/

/**
 * 连接关闭通知：引擎释放任意连接时调用
 * 转发给 master/slave 各自的状态处理，普通客户端 fd 均为 no-op
 */
void repl_on_close(int fd) {
    if (kvs_config.repl_role == REPL_ROLE_MASTER)
        repl_incr_master_on_close(fd);   /* Master 端清理从库状态 */
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