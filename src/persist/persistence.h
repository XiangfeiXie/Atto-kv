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
 *  - kvs_aof_persist_*：主线程 memcpy 进块池 + 专用落盘线程 io_uring 写 AOF
 *  - rdb_save_sync / rdb_save_async_uring：SAVE 主线程阻塞 / BGSAVE fork 子进程，
 *    共用逐条目 io_uring 流水线直写 RDB（见 persist_rdb.c）
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
 *  AOF 持久化（原 persist_aof.h，已并入）
 *
 *  功能：提供增量命令日志的持久化能力
 *  线程模型：主线程逐命令 memcpy 进共享块池的当前块；块满即移交，半块由主线程
 *  周期节拍按年龄封（首字节后攒 AOF_SEAL_MS，kvs_aof_periodic 每轮事件循环检查）。
 *  落盘线程 io_uring 异步写 + 按策略 fsync（fsync 节奏归落盘线程单调钟）。
 *  无请求-授权：发布节奏归主线程、落盘节奏归落盘线程，两钟互不跨线程握手。
 *  设计特点：
 *  1. 块池（块=任务=io_uring 写源），mutex+condvar 单生产者/单消费者
 *  2. 封块节奏 = 满块 + 主线程年龄定时；fsync 节奏归落盘线程（不绑网络模型）
 *  3. io_uring 异步落盘（单属主：只由落盘线程 submit/reap）
 *  4. 支持三种 fsync 策略
 *============================================================================*/

/**
 * 单条命令写入 AOF 的字节上限（512KB）
 *
 * 与 AOF 块大小 AOF_BLOCK_SIZE（persist_aof.c）一致
 * 保证整块清空后新命令总能装进空块
 *
 * 超限命令执行成功但不落 AOF（kvs_aof_persist_append_raw 拒绝）
 *
 * 设计考虑：
 * - 防止超大命令撑爆块池
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
 * 追加原始 RESP 字节到当前块（主线程调用）
 *
 * 特点：
 * - 热路径逐命令无锁 memcpy；块满才封块入队（加锁移交，每 ~1.2 万命令一次）
 * - 通常不阻塞；仅当整池块在飞（落盘线程落后）时在封块点阻塞 = 有界背压
 * - 异步落盘（由专用落盘线程 io_uring 处理）
 *
 * @param payload RESP 命令数据
 * @param len 数据长度
 * @return 0 成功（已拷贝入块），-1 未启动（重放期）/超限/已停机
 *         （命令仍执行成功，只是不落 AOF）
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
 *   - "always"：每批落盘后 fsync（批 = 一次移交的块，最安全）
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
 * 功能：主线程自己的封半块节拍——当前块非空且首字节后攒够 AOF_SEAL_MS 即封入队
 *       （满块在 append_raw 已封；这里是半块兜底定时）。
 *
 * 调用频率：每轮事件循环（引擎空闲 100ms 级必醒，节拍可靠）
 */
void kvs_aof_periodic(void);

/*=============================================================================
 *  RDB 持久化
 *============================================================================*/

/**
 * 同步 RDB 保存（SAVE 与 BGSAVE 共用）：逐条目 io_uring 流水线落盘
 * 每条 entry 以 5 段 writev gather dict 内存直写（无块、零拷贝），全量快照
 * 无丢弃（任意长度条目都完整保存）；一遍扫描 key_count=dict_count O(1) 预知，
 * 排空后末尾一次性补写 8B 文件头（key_count+crc）→ 一次 fsync → tmp→rename。
 * 内存与库大小无关；阻塞直到写入完成；失败不覆盖旧 dump.rdb
 */
void rdb_save_sync(void);

/**
 * BGSAVE 子进程是否仍在运行（waitpid 探活，已退出则顺手清状态）
 * 供 SAVE/BGSAVE 命令守卫：有 BGSAVE 在跑时两者都拒绝
 * @return 1 在运行，0 无
 */
int rdb_bgsave_running(void);

/**
 * 异步 RDB 保存（BGSAVE，Redis 风格）
 * 主线程 fork，子进程跑 rdb_save_sync（与 SAVE 同一逐条目流水线路径）
 * 同时只跑一个 BGSAVE 进程；有 BGSAVE 在跑时本函数拒绝
 * @return 0 成功（或已触发），-1 已在运行
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
 * 从内存缓冲区加载磁盘 RDB 文件（文件格式带 crc 槽）
 * 用途：启动加载 dump.rdb（rdb_load_mmap）/ sendfile 对端收到文件后加载
 * crc 校验失败（被篡改/损坏）即拒绝并清空字典
 * @param buf RDB 文件缓冲区
 * @param len 数据长度
 * @return 0 成功，-1 失败
 */
int rdb_load_from_buf(const char *buf, size_t len);

/**
 * 流式 RDB 加载（RDMA 全量同步 Slave 端使用，纯流无 crc）
 * 流格式 [key_count:4][entry...]：begin 读 key_count，
 * append_n 从 buf+4 解析条目（磁盘文件带 crc 槽、条目在 +8，由 rdb_load_from_buf 处理）
 */
int rdb_load_stream_begin(const char *buf, size_t len);   /* 开始流式加载 */
int rdb_load_stream_append_n(const char *buf, size_t len, int limit,
                             size_t *consumed);  /* 追加数据块（限条数，可选输出消费字节数） */

/*=============================================================================
 *  增量 RDB 序列化器（RDMA 全量同步 Master 端用；RDMA 共享代码，零逻辑改动）
 *
 *  设计目标：直接流式输出 RDB 批次到目标缓冲区，避免中间 DynBuf 拷贝
 *
 *  磁盘 dump.rdb 不再经本序列化器产生——改由 rdb_save_sync 逐条目 io_uring
 *  流水线直写（见 persist_rdb.c）；sendfile 传输的仍是该文件，格式带 crc 槽
 *  [key_count:4][crc32:4][entry...]，crc32 覆盖 key_count + 全部条目，由
 *  写入器排空后"末尾一次性补写 8B 头"产生。加载端（rdb_load_mmap 启动 /
 *  sendfile 对端）校验失败即拒绝，防 dump.rdb 被篡改/损坏后仍被成功加载。
 *
 *  本序列化器现仅被 RDMA 全量 Master 以 with_crc=0（无盘纯流）调用：
 *  流格式 [key_count:4][entry...]，与磁盘格式唯一差别是无 crc 槽。
 *  with_crc=1（磁盘文件格式，旧 rdb_save_sync 用，含 offset 4 回填 crc）为
 *  惰性残留分支，无当前调用方。
 *
 *  使用示例（RDMA 无盘流 with_crc=0）：
 *  rdb_serializer_t rs;
 *  rdb_serialize_begin(&rs, 0);
 *  int done = 0;
 *  while (!done) {
 *      size_t n = rdb_serialize_next(&rs, dst, max_bytes, &done);
 *      // dst 中有 n 字节的 RDB 数据，发送...
 *  }
 *  rdb_serialize_end(&rs);
 *============================================================================*/

#define RDB_FILE_HDR_LEN  8   /* RDB 文件头长度：key_count[4] + crc32 槽[4]（磁盘格式用） */
#define RDB_FILE_CRC_OFF  4   /* crc 槽相对文件头起点偏移（紧跟 key_count） */

typedef struct {
    kv_iter_t *iter;        /* 字典迭代器，结束为 NULL */
    int        header_sent; /* 是否已发送流头（with_crc: 8B / 无 crc: 4B） */
    int        exhausted;   /* 所有条目已消费 */
    int        with_crc;    /* 0=RDMA 无盘纯流（全量 Master 用）；1=磁盘格式惰性残留 */
    uint32_t   crc;         /* with_crc 时运行中的 crc32：key_count + 已序列化条目 */
} rdb_serializer_t;

/**
 * 开始 RDB 序列化（RDMA 无盘全量 Master 用；磁盘保存已不调用本序列化器）
 * @param rs 序列化器指针
 * @param with_crc 0=RDMA 无盘纯流（4B 头无 crc 槽，全量同步 Master 用）；
 *                 1=磁盘文件格式惰性残留分支（流头带 crc 槽并累计 crc，
 *                   旧 rdb_save_sync 用，现已无调用方）
 */
void rdb_serialize_begin(rdb_serializer_t *rs, int with_crc);

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

/**
 * 校验磁盘 RDB 文件头 crc（防篡改：dump.rdb 被篡改/损坏即拒绝加载）
 * 仅磁盘文件格式含 crc 槽；RDMA 无盘流不走本函数。
 * @param stream 文件头起点（key_count[4] + crc[4] + 条目...）
 * @param entry_bytes 条目区实际字节数（= 文件大小 - RDB_FILE_HDR_LEN）
 * @return 1 通过，0 不通过
 */
int rdb_stream_crc_verify(const char *stream, size_t entry_bytes);

#endif /* KVS_PERSISTENCE_H */