#ifndef __PERSISTENCE_H__
#define __PERSISTENCE_H__

#include <stddef.h>
#include "server.h"
#include "kv_dict.h"

#define BGSAVE_COOLDOWN_MIN_SEC 1

/* =========================================================
 * 持久化模块：WAL io_uring 落盘 + RDB fork 保存 + mmap 加载
 *
 * - kvs_wal_persist_*：后台线程 + io_uring 增量写 WAL
 * - rdb_save_async_uring：主线程 fork，子进程用 io_uring 批量写 RDB
 * - rdb_load_mmap / aof_reload_mmap：mmap 加载
 * - persist_set_basedir：设置项目根目录
 * ========================================================= */

void persist_set_basedir(const char *argv0);

/* 持久化文件相对路径常量 */
#define AOF_REL  "build/appendonly.aof"
#define RDB_REL  "build/dump.rdb"
#define WAL_REL  "build/incr.wal"

const char *persist_path(const char *rel);

/* RDB：同步 io_uring 写 */
void rdb_save_sync(void);
/* RDB：fork + io_uring 批量写 */
int rdb_save_async_uring(void);
/* RDB：检查 BGSAVE 是否正在进行 */
int rdb_bgsave_in_progress(void);
/* RDB：事件循环周期性维护（pending BGSAVE 重试） */
void rdb_periodic(void);

/* RDB 保存状态统计 */
extern uint64_t g_rdb_changes;
uint64_t rdb_save_time(void);
uint64_t rdb_bgsave_total(void);
uint64_t rdb_bgsave_skipped(void);
int     rdb_bgsave_cooldown_sec(void);
int     rdb_bgsave_last_duration(void);

/* mmap 加载 */
void rdb_load_mmap(void);
void aof_reload_mmap(void);

/* RDB 二进制序列化 → DynBuf（全量同步用） */
void rdb_serialize_to_buf(DynBuf *out);

/* RDB 二进制反序列化 ← 内存 buffer（全量同步用） */
int rdb_load_from_buf(const char *buf, size_t len);

/* Streaming RDB load (for RDMA fullsync slave). */
int rdb_load_stream_begin(const char *buf, size_t len);
int rdb_load_stream_append(const char *buf, size_t len);

/* Rebuild encodings after streaming load */
void rdb_rebuild_all_encodings(void);

/* ── Incremental RDB serialiser (for RDMA fullsync master) ──
 *
 * Replaces rdb_serialize_to_buf() when the caller wants to write batches
 * directly into an RDMA MR buffer, avoiding an intermediate DynBuf copy.
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
