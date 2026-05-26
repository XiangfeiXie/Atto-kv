#ifndef __PERSISTENCE_H__
#define __PERSISTENCE_H__

#include <stddef.h>
#include "server.h"
#include "kv_dict.h"

/* =========================================================
 * 持久化模块：AOF / RDB 异步化 + io_uring 写入 + mmap 加载
 *
 * - aof_async_init / aof_async_shutdown：启动/关闭 AOF 后台写线程
 * - aof_async_append：主线程调用，零阻塞入队，由后台线程 + io_uring 落盘
 * - rdb_save_async_uring：主线程 fork，子进程用 io_uring 批量写 RDB
 * - rdb_load_mmap / aof_reload_mmap：mmap 加载，避免逐次 read 拷贝
 * - persist_set_basedir：设置项目根目录，所有持久化路径从此解析
 * ========================================================= */

/* 设置项目根目录（由 main() 在启动时调用一次）。
 * 传入 argv[0]，内部解析为项目根目录。
 * 所有持久化文件（build/dump.rdb, build/appendonly.aof 等）的路径
 * 都从此目录解析，确保不受 CWD 变化影响。 */
void persist_set_basedir(const char *argv0);

/* 持久化文件相对路径常量 */
#define AOF_REL  "build/appendonly.aof"
#define RDB_REL  "build/dump.rdb"
#define WAL_REL  "build/incr.wal"   /* Phase 4: WAL unified with AOF */

/* 将相对路径拼接到项目根目录，返回静态 buffer（调用方不可 free） */
const char *persist_path(const char *rel);

/* AOF 异步写：初始化后台线程 & io_uring */
int  aof_async_init(void);
/* AOF 异步写：优雅关闭，等待队列落盘 */
void aof_async_shutdown(void);
/* AOF 异步写：主线程入队（拷贝一次，零阻塞 IO） */
void aof_async_append(const char *buf, int len);

/* RDB：同步 io_uring 写（repl 全量同步等需要等待完成的场景） */
void rdb_save_sync(void);
/* RDB：fork + io_uring 批量写，主线程不阻塞 */
void rdb_save_async_uring(void);
/* RDB：检查 BGSAVE 是否正在进行（返回 1 表示正在进行，0 表示空闲） */
int rdb_bgsave_in_progress(void);

/* Phase 7.1.0 — BGREWRITEAOF stub.
 *
 * Trigger an AOF rewrite (compact the AOF by emitting one canonical
 * SET/HSET/ZADD/RPUSH/SADD per live entry instead of replaying every
 * historical mutation).  Returns:
 *   0  rewrite scheduled (or already running)
 *  -1  not implemented in this build (current state — see persistence.c)
 *
 * Design notes for the future implementation are at the top of the
 * stub body in persistence.c.  Until then this returns -1 and the
 * BGREWRITEAOF command replies with -ERR. */
int aof_rewrite_bg(void);

/* Finalize AOF rewrite: append rewrite buffer and rename temp file.
 * Called after BGREWRITEAOF child completes. Returns 0 on success. */
int aof_rewrite_finalize(void);

/* mmap 加载：避免逐条 fread 的系统调用开销 */
void rdb_load_mmap(void);
void aof_reload_mmap(void);

/* RDB 二进制序列化 → DynBuf（全量同步用，纯内存，不写盘） */
void rdb_serialize_to_buf(DynBuf *out);

/* RDB 二进制反序列化 ← 内存 buffer（全量同步用）
 * 返回加载的 key 数量，失败返回 -1 */
int rdb_load_from_buf(const char *buf, size_t len);

/* Single-entry serialization (used by streaming fullsync child process).
 * Appends [klen:4][key][dlen:4][data][expire:8] to out.
 * Handles quicklist flattening internally. */
void serialize_entry_to_buf(DynBuf *out, kv_entry_t *e);

/* Inline serialization: writes [klen:4][key][dlen:4][data][expire:8]
 * directly to dst. Returns bytes written, or -1 if larger than max_space.
 * Caller advances its write pointer by the return value. */
int serialize_entry_inline(char *dst, size_t max_space, kv_entry_t *e);

/* Streaming RDB load (for RDMA fullsync slave).
 * rdb_load_stream_begin: parse header, clear engine, return key_count or -1.
 *   Buffer format: [magic:4][version:4][db_num:4][key_count:4]
 * rdb_load_stream_append: parse entries from chunk, return count loaded.
 *   Buffer format: [klen:4][key][dlen:4][data][expire:8]... */
int rdb_load_stream_begin(const char *buf, size_t len);
int rdb_load_stream_append(const char *buf, size_t len);

/* Rebuild encodings for all loaded entries that exceed PACKED thresholds.
 * Call after streaming load to upgrade SET→DICT, HASH→DICT, ZSET→SKIPLIST. */
void rdb_rebuild_all_encodings(void);

#endif
