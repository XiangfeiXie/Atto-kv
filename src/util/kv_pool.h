/*=============================================================================
 *  kv_pool.h — kvstore 自研内存池
 *
 *  目标：接管数据域（kv entry）热路径分配，替代对底座 malloc 的直接调用，
 *  以支撑后续的「池 vs glibc vs jemalloc」基准对比（矩阵 A/B/C/D）。
 *
 *  版本：v0 最基本版（评审用）。只保证语义正确、够跑基准，刻意不做全，
 *  待评审后再逐轮迭代（见文件尾部「评审点」）。
 *
 *  设计要点：
 *  1. 档位（size class）制：超块从底座 malloc 一次性拿一整块，内部按档位
 *     切分；档内自由对象链 LIFO 复用。稳态下分配/释放零底座调用。
 *  2. free 携带尺寸（池免费带尺寸）：pool_free(p, ptr, size) 的 size 必须
 *     等于当初 pool_alloc(p, size) 请求的尺寸。对象因此无需块头元数据——
 *     kv_entry_t 布局零改动即可接入；尺寸语义也同时充当「底座兜底」路由：
 *     size > KV_POOL_CLASS_MAX 的对象在 alloc/free 两侧都透明走底座 malloc，
 *     大 value 的 entry 天然落底座。
 *  3. 对齐：档位尺寸为 16 的倍数 + 超块 16B 对齐 → 返回地址 ≥16B 对齐。
 *  4. 单线程、主线程专属（reactor 事件循环线程）：无锁无原子。多线程
 *     上下文（AOF worker / fork 子进程）请直接用底座，不要碰池。
 *  5. 底座 = 当前链接的 malloc（USE_JEMALLOC 开时为 jemalloc）：
 *     超块只走一次大块分配，其余都是档内内存搬运。
 *  6. 池实例化对象 kv_pool_t*，可创建多个互不干扰；p 传 NULL 时
 *     alloc/free 直接透传底座（容错冷启动路径，如 AOF 落盘线程）。
 *
 *  使用约定（不变量）：
 *   - 池内分配的指针只经 kv_pool_free(p, ptr, size) 归还，size 与 alloc 时
 *     一致（0 ≤ 底层语义：同一次 alloc/free 的 size 必须映射到同一档位）。
 *   - kv_pool_free 收到 NULL 为 no-op。
 *
 *  评审点（v0 刻意未做，供讨论）：
 *  1. 档内碎片：请求尺寸向上取整到 16B 倍数档位，小档位最多浪费 ≤15B；
 *     档位表（见 .c）步进在后段加密到 32/64/128，中大体量浪费有界。
 *  2. 超块归还：kv_pool_release_all 在「池内无存活对象」时（如 dict_clear
 *     全清后）把全部超块整批还给底座、各档切分状态重置为空。v0 只做
 *     "全清归还"这一档：dict_clear 一清到底，契约天然满足。部分删除后
 *     的"增量归还空超块"（需 per-slab 存活记账、碰 alloc/free 热路径）
 *     刻意不做。
 *  3. 超块大小 KV_POOL_SUPERBLOCK 暂定 64KB（< glibc mmap 阈值，走堆）。
 *  4. 档位上限 2048：kv entry 总尺寸超过即走底座，接入后按真实尺寸分布再调。
 *  5. 本版未接入 kv_dict（接入点 = dict entry_alloc/delete/resize/clear 四处）。
 *============================================================================*/

#ifndef __KV_POOL_H__
#define __KV_POOL_H__

#include <stddef.h>
#include <stdint.h>

/* 对象对齐（也是档位颗粒度） */
#define KV_POOL_ALIGN        16u

/* 档位上界：请求尺寸 > 此值走底座 malloc/free（kv entry 的小值域都在内） */
#define KV_POOL_CLASS_MAX    2048u

/* 单个超块字节数（从底座一次性拿一块；< glibc 128KB mmap 阈值，落堆） */
#define KV_POOL_SUPERBLOCK   (64u * 1024u)

typedef struct kv_pool kv_pool_t;   /* 池实现细节在 .c，本头只见不透明指针 */

/**
 * 创建内存池（空池：所有超块按需惰性分配）
 * @return 池指针，失败返回 NULL
 */
kv_pool_t *kv_pool_create(void);

/**
 * 销毁内存池（释放全部超块）
 * @param p 池指针（NULL 为 no-op）
 */
void kv_pool_destroy(kv_pool_t *p);

/**
 * 整批归还超块给底座（"全清归还"）
 *
 * @param p 池指针（NULL 为 no-op）
 *
 * 契约：调用方须保证池内已无存活对象——典型场景是 dict_clear 把 dict 全清
 * 之后（所有 entry 均已归还，各档 live_n == 0）。此时每个超块必空，直接
 * 整块 free 回底座并把各档切分状态（cur/freelist）重置为空：下次 kv_pool_alloc
 * 重新开块。累计统计（alloc_calls/free_calls/base_*）保留，slab_count/slab_bytes
 * 归零。部分删除（仍存活对象）后调用属未定义行为。
 */
void kv_pool_release_all(kv_pool_t *p);

/**
 * 分配
 *
 * @param p 池指针（NULL → 直接底座 malloc）
 * @param size 请求字节数（0 → 按最小档位处理）
 * @return 对齐到 ≥KV_POOL_ALIGN 的内存块；失败返回 NULL
 *
 * 语义：
 *  - size ≤ KV_POOL_CLASS_MAX：从对应档位取块（空闲链优先，空则切当前超块）
 *  - size >  KV_POOL_CLASS_MAX：底座 malloc 原样返回
 */
void *kv_pool_alloc(kv_pool_t *p, size_t size);

/**
 * 归还
 *
 * @param p 池指针（NULL → 直接底座 free）
 * @param ptr 由 kv_pool_alloc 返回的指针（NULL 为 no-op）
 * @param size 与当初 alloc 请求的尺寸一致（档位路由与底座判定靠它，不查块头）
 */
void kv_pool_free(kv_pool_t *p, void *ptr, size_t size);

/*-------------------------------------------------------------------------
 *  观测（评审 / 后续接 get_used_mem）
 *-------------------------------------------------------------------------*/

typedef struct {
    uint64_t slab_count;    /* 现存超块数 */
    uint64_t slab_bytes;    /* 底座为超块分配的总字节 */
    uint64_t live_objects;  /* 池内存活对象数（已 alloc 未 free） */
    uint64_t free_objects;  /* 档内空闲链上对象数（待复用） */
    uint64_t alloc_calls;   /* alloc 总调用次数 */
    uint64_t free_calls;    /* free 总调用次数 */
    uint64_t alloc_bytes;   /* alloc 请求字节累计（不含档位取整） */
    uint64_t base_alloc;    /* 底座 malloc 次数（> 档位上界的对象） */
    uint64_t base_free;     /* 底座 free 次数 */
} kv_pool_stats_t;

/**
 * 取池统计快照
 * @param p 池指针
 * @param out 输出结构（非 NULL）
 */
void kv_pool_stats(const kv_pool_t *p, kv_pool_stats_t *out);

#endif /* __KV_POOL_H__ */
