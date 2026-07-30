#ifndef __PERSISTENCE_H__
#define __PERSISTENCE_H__

#include <stddef.h>
#include "server.h"
#include "kv_dict.h"

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
void rdb_save_async_uring(void);
/* RDB：检查 BGSAVE 是否正在进行 */
int rdb_bgsave_in_progress(void);

/* mmap 加载 */
void rdb_load_mmap(void);
void aof_reload_mmap(void);

/* RDB 二进制序列化 → DynBuf（全量同步用） */
void rdb_serialize_to_buf(DynBuf *out);

/* RDB 二进制反序列化 ← 内存 buffer（全量同步用） */
int rdb_load_from_buf(const char *buf, size_t len);

/* Single-entry serialization (used by streaming fullsync).
 * [klen:4][key][dlen:4][data][expire:8] */
void serialize_entry_to_buf(DynBuf *out, kv_entry_t *e);

/* Streaming RDB load (for RDMA fullsync slave). */
int rdb_load_stream_begin(const char *buf, size_t len);
int rdb_load_stream_append(const char *buf, size_t len);

/* Rebuild encodings after streaming load */
void rdb_rebuild_all_encodings(void);

#endif
