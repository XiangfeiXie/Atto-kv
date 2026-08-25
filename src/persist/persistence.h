#ifndef KVS_PERSISTENCE_H
#define KVS_PERSISTENCE_H

#include <stddef.h>
#include "server.h"
#include "kv_dict.h"

/* =========================================================
 * 持久化模块：AOF io_uring 落盘 + RDB fork 保存 + mmap 加载
 *
 * - kvs_aof_persist_*：单线程无锁 + io_uring 增量写 AOF
 * - rdb_save_async_uring：主线程 fork，子进程用 io_uring 批量写 RDB
 * - rdb_load_mmap / aof_reload_mmap：mmap 加载
 * - persist_set_basedir：设置项目根目录
 * ========================================================= */

void persist_set_basedir(const char *argv0);

/* 持久化文件相对路径常量 */
#define AOF_REL  "build/appendonly.aof"
#define RDB_REL  "build/dump.rdb"

const char *persist_path(const char *rel);

/* Internal: whole-file read helper (mmap with read() fallback), shared by
 * aof_reload_mmap / rdb_load_mmap. */
#define FILE_READ_OK          0
#define FILE_READ_OPEN_FAIL  -1   /* open() failed — caller may try an alt path */
#define FILE_READ_EMPTY      -2   /* fstat failed or file is empty */
int file_read_full(const char *path, char **buf, size_t *size,
                   int *is_mmap, const char *tag);

/* RDB：同步 io_uring 写 */
void rdb_save_sync(void);
/* RDB：fork + io_uring 批量写（Redis 式：同时只跑一个） */
int rdb_save_async_uring(void);

/* RDB 保存状态统计 */
extern uint64_t g_rdb_changes;
uint64_t rdb_save_time(void);

/* mmap 加载 */
void rdb_load_mmap(void);
void aof_reload_mmap(void);

/* RDB 二进制反序列化 ← 内存 buffer（全量同步用） */
int rdb_load_from_buf(const char *buf, size_t len);

/* Streaming RDB load (for RDMA fullsync slave). */
int rdb_load_stream_begin(const char *buf, size_t len);
int rdb_load_stream_append(const char *buf, size_t len);

/* ── Incremental RDB serialiser (for RDMA fullsync master) ──
 *
 * Streams RDB batches directly into an RDMA MR buffer, avoiding an
 * intermediate DynBuf copy.
 *
 * Usage:
 *   rdb_serializer_t rs;
 *   rdb_serialize_begin(&rs);
 *   int done = 0;
 *   while (!done) {
 *       size_t n = rdb_serialize_next(&rs, dst, max_bytes, &done);
 *       // n bytes of RDB data in dst, send it...
 *   }
 *   rdb_serialize_end(&rs);                                   */

typedef struct {
    kv_iter_t *iter;        /* dict iterator, NULL after end */
    int        header_sent; /* has the 16-byte RDB header been emitted? */
    int        exhausted;   /* all entries consumed */
} rdb_serializer_t;

void   rdb_serialize_begin(rdb_serializer_t *rs);
size_t rdb_serialize_next(rdb_serializer_t *rs, char *dst,
                          size_t max_bytes, int *done);
void   rdb_serialize_end(rdb_serializer_t *rs);

#endif
