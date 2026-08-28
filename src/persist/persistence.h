/*=============================================================================
 *  persistence.h — 持久化模块公开接口
 *
 *  功能：提供数据持久化的完整解决方案
 *  包含：
 *  1. AOF（Append-Only File）：io_uring 增量落盘
 *  2. RDB（Redis Database）：fork + io_uring 批量保存
 *  3. mmap 快速加载
 *  4. 流式 RDB 序列化/反序列化（全量同步）
 *  5. 子进程回收管理
 *
 *  注：AOF 接口原在 persist_aof.h，已并入本文件（同一模块）
 *============================================================================*/

#ifndef KVS_PERSISTENCE_H
#define KVS_PERSISTENCE_H

#include <stddef.h>
#include <stdint.h>      /* uint32_t：AOF 接口 */
#include <sys/types.h>   /* pid_t */
#include "kv_dict.h"

/*=============================================================================
 *  模块概述
 *  
 *  - kvs_aof_persist_*：单线程无锁 + io_uring 增量写 AOF
 *  - rdb_save_async_uring：主线程 fork，子进程用 io_uring 批量写 RDB
 *  - rdb_load_mmap / aof_reload_mmap：mmap 加载
 *  - persist_set_basedir：设置项目根目录
 *============================================================================*/

/*=============================================================================
 *  路径管理
 *============================================================================*/

/**
 * 设置持久化文件的基础目录
 * 根据可执行文件路径推导项目根目录
 * @param argv0 可执行文件路径（main 函数的 argv[0]）
 */
void persist_set_basedir(const char *argv0);

/**
 * 持久化文件相对路径常量
 * AOF_REL：AOF 文件路径（相对于项目根目录）
 * RDB_REL：RDB 文件路径（相对于项目根目录）
 */
#define AOF_REL  "build/appendonly.aof"
#define RDB_REL  "build/dump.rdb"

/**
 * 获取持久化文件的绝对路径
 * @param rel 相对路径（如 AOF_REL 或 RDB_REL）
 * @return 绝对路径字符串（静态缓冲区，不可多线程共享）
 */
const char *persist_path(const char *rel);

/*=============================================================================
 *  文件读取工具
 *  支持 mmap 和 read() fallback
 *  被 aof_reload_mmap / rdb_load_mmap 共享
 *============================================================================*/

#define FILE_READ_OK          0   /* 成功 */
#define FILE_READ_OPEN_FAIL  -1   /* open() 失败，调用者可尝试备选路径 */
#define FILE_READ_EMPTY      -2   /* fstat 失败或文件为空 */

/**
 * 读取整个文件
 * @param path 文件路径
 * @param buf 输出：文件内容缓冲区（mmap 或 malloc 分配）
 * @param size 输出：文件大小
 * @param is_mmap 输出：1 表示 mmap，0 表示 malloc + read
 * @param tag 调试标签（用于日志）
 * @return FILE_READ_OK 成功，负数表示错误码
 */
int file_read_full(const char *path, char **buf, size_t *size,
                   int *is_mmap, const char *tag);

/*=============================================================================
 *  子进程回收管理（SIGCHLD 统一处理）
 *  
 *  问题：SIGCHLD 每进程只能有一个 handler
 *  解决方案：统一 handler + 回调注册机制
 *  
 *  使用场景：
 *  - BGSAVE 子进程（persist_rdb.c）
 *  - 全量同步传输子进程（repl_fullsync.c）
 *  
 *  两个模块各自注册 reap 回调，由统一 handler 用 waitpid(-1) 回收后逐个通知
 *  回调在异步上下文运行：只能读 pid、写 sig_atomic_t 或打日志
 *============================================================================*/

/**
 * 子进程回收回调函数类型
 * @param pid 已终止的子进程 PID
 * @param status waitpid 返回的状态码
 */
typedef void (*kvs_child_reap_fn)(pid_t pid, int status);

/**
 * 初始化子进程回收机制（幂等）
 * 安装全局 SIGCHLD handler
 */
void kvs_child_reap_init(void);

/**
 * 注册子进程回收回调（幂等，去重注册）
 * @param fn 回调函数
 */
void kvs_child_reap_register(kvs_child_reap_fn fn);

/*=============================================================================
 *  AOF 持久化（原 persist_aof.h，已并入）
 *
 *  功能：提供增量命令日志的持久化能力
 *  设计特点：
 *  1. 无锁环形缓冲区（单线程写入）
 *  2. io_uring 异步落盘
 *  3. 支持三种 fsync 策略
 *============================================================================*/

/**
 * 单条命令写入 AOF 的字节上限（512KB）
 *
 * 与 AOF 槽大小 AOF_SLOT_SIZE（persist_aof.c）一致
 * 保证 flush 后新记录总能装进空槽
 *
 * 超限命令执行成功但不落 AOF（kvs_aof_persist_append_raw 拒绝）
 *
 * 设计考虑：
 * - 防止超大命令占满环形缓冲区
 * - 限制单次 I/O 大小，避免阻塞
 * - 512KB 对大多数 Redis 命令已足够
 */
#define KVS_JOURNAL_MAX_PAYLOAD  (512u * 1024u)

/**
 * 启动 AOF 持久化
 *
 * 初始化：
 * - 环形缓冲区
 * - io_uring 实例
 * - AOF 文件（追加模式）
 *
 * @return 0 成功，-1 失败
 */
int kvs_aof_persist_start(void);

/**
 * 追加原始 RESP 字节到环形缓冲区
 *
 * 特点：
 * - 无锁（单线程调用）
 * - 非阻塞（写入环形缓冲区即返回）
 * - 异步落盘（由后台 io_uring 处理）
 *
 * @param payload RESP 命令数据
 * @param len 数据长度
 * @return 0 成功（入队），-1 失败（缓冲区满或超限）
 *
 * 注意：调用者需保证 payload 是完整的 RESP 命令
 */
int kvs_aof_persist_append_raw(const char *payload, uint32_t len);

/**
 * 停止 AOF 持久化
 *
 * 流程：
 * 1. 刷新所有待写入数据
 * 2. 关闭 io_uring
 * 3. 关闭 AOF 文件
 *
 * 在程序退出时调用，确保数据不丢失
 */
void kvs_aof_persist_stop(void);

/**
 * 设置 fsync 策略
 *
 * @param policy 策略字符串：
 *   - "always"：每条命令后 fsync（最安全，性能最低）
 *   - "everysec"：每秒 fsync（平衡，默认）
 *   - "no"：由操作系统决定（最快，可能丢失数据）
 *
 * 只在启动时调用一次（不支持运行时修改）
 */
void kvs_aof_set_fsync_policy(const char *policy);

/**
 * 周期性维护函数
 *
 * 由事件循环（update_cached_time）每轮调用
 * 功能：
 * - 收割已完成的 io_uring 写入
 * - 时间驱动的缓冲区刷新（wall-clock 判断，非 tick 计数）
 * - 检查 fsync 定时器（everysec 模式）
 *
 * 调用频率：每轮事件循环
 */
void kvs_aof_periodic(void);

/*=============================================================================
 *  RDB 持久化
 *============================================================================*/

/**
 * 同步 RDB 保存（io_uring 写）
 * 阻塞直到写入完成
 */
void rdb_save_sync(void);

/**
 * 异步 RDB 保存（Redis 风格）
 * 主线程 fork，子进程用 io_uring 批量写 RDB
 * 同时只跑一个 BGSAVE 进程
 * @return 0 成功（或已触发），-1 失败
 */
int rdb_save_async_uring(void);

/**
 * mmap 加载 RDB 文件
 */
void rdb_load_mmap(void);

/**
 * mmap 加载 AOF 文件
 */
void aof_reload_mmap(void);

/**
 * 从内存缓冲区加载 RDB（全量同步使用）
 * @param buf RDB 数据缓冲区
 * @param len 数据长度
 * @return 0 成功，-1 失败
 */
int rdb_load_from_buf(const char *buf, size_t len);

/**
 * 流式 RDB 加载（RDMA 全量同步 Slave 端使用）
 * 支持分块接收和增量解析
 */
int rdb_load_stream_begin(const char *buf, size_t len);   /* 开始流式加载 */
int rdb_load_stream_append(const char *buf, size_t len);  /* 追加数据块（到缓冲末尾） */
int rdb_load_stream_append_n(const char *buf, size_t len, int limit,
                             size_t *consumed);  /* 追加数据块（限条数，可选输出消费字节数） */

/*=============================================================================
 *  增量 RDB 序列化器（RDMA 全量同步 Master 端使用）
 *  
 *  设计目标：直接流式输出 RDB 批次到 RDMA MR 缓冲区
 *  避免中间 DynBuf 拷贝，减少内存开销
 *  
 *  使用示例：
 *  rdb_serializer_t rs;
 *  rdb_serialize_begin(&rs);
 *  int done = 0;
 *  while (!done) {
 *      size_t n = rdb_serialize_next(&rs, dst, max_bytes, &done);
 *      // dst 中有 n 字节的 RDB 数据，发送...
 *  }
 *  rdb_serialize_end(&rs);
 *============================================================================*/

typedef struct {
    kv_iter_t *iter;        /* 字典迭代器，结束为 NULL */
    int        header_sent; /* 是否已发送 4 字节头部（键数量） */
    int        exhausted;   /* 所有条目已消费 */
} rdb_serializer_t;

/**
 * 开始 RDB 序列化
 * @param rs 序列化器指针
 */
void rdb_serialize_begin(rdb_serializer_t *rs);

/**
 * 获取下一批 RDB 数据
 * @param rs 序列化器指针
 * @param dst 目标缓冲区
 * @param max_bytes 缓冲区最大容量
 * @param done 输出：是否已完成（所有数据已序列化）
 * @return 写入 dst 的字节数
 */
size_t rdb_serialize_next(rdb_serializer_t *rs, char *dst,
                          size_t max_bytes, int *done);

/**
 * 结束 RDB 序列化
 * @param rs 序列化器指针
 */
void rdb_serialize_end(rdb_serializer_t *rs);

#endif /* KVS_PERSISTENCE_H */