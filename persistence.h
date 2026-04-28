#ifndef __PERSISTENCE_H__
#define __PERSISTENCE_H__

#include <stddef.h>

/* =========================================================
 * 持久化模块：AOF / RDB 异步化 + io_uring 写入 + mmap 加载
 *
 * - aof_async_init / aof_async_shutdown：启动/关闭 AOF 后台写线程
 * - aof_async_append：主线程调用，零阻塞入队，由后台线程 + io_uring 落盘
 * - rdb_save_async_uring：主线程 fork，子进程用 io_uring 批量写 RDB
 * - rdb_load_mmap / aof_reload_mmap：mmap 加载，避免逐次 read 拷贝
 * ========================================================= */

/* AOF 异步写：初始化后台线程 & io_uring */
int  aof_async_init(const char *path);
/* AOF 异步写：优雅关闭，等待队列落盘 */
void aof_async_shutdown(void);
/* AOF 异步写：主线程入队（拷贝一次，零阻塞 IO） */
void aof_async_append(const char *buf, int len);

/* RDB：fork + io_uring 批量写，主线程不阻塞 */
void rdb_save_async_uring(void);

/* mmap 加载：避免逐条 fread 的系统调用开销 */
void rdb_load_mmap(void);
void aof_reload_mmap(void);

#endif
