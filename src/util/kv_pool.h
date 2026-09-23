/*=============================================================================
 *  kv_pool.h — kvstore 自研内存池
 *
 *  目标：接管数据域（kv 节点）热路径分配，替代对底座 malloc 的直接调用。
 *
 *  设计要点：
 *  1. 档位（size class）制：超块从底座 malloc 一次性拿一整块，内部按档位
 *     切分；档内自由对象链 LIFO 复用。稳态下分配/释放零底座调用。
 *  2. free 携带尺寸（池免费带尺寸）：pool_free(p, ptr, size) 的 size 必须
 *     等于当初 pool_alloc(p, size) 请求的尺寸。对象因此无需块头元数据；
 *     尺寸语义也同时充当「底座兜底」路由：size > KV_POOL_CLASS_MAX 的对象
 *     在 alloc/free 两侧都透明走底座 malloc，大 value 的节点天然落底座。
 *     各引擎的节点尺寸恒为 sizeof(节点) + key_len + val_len，node_free 由
 *     头部字段反推该值 —— 变长一律重建，不得原地改长度。
 *  3. 对齐：档位尺寸为 16 的倍数 + 超块 16B 对齐 → 返回地址 ≥16B 对齐。
 *  4. 单线程、主线程专属（reactor 事件循环线程）：无锁无原子。多线程
 *     上下文（AOF worker / fork 子进程）请直接用底座，不要碰池。
 *  5. 底座 = 当前链接的 malloc（USE_JEMALLOC 开时为 jemalloc）：
 *     超块只走一次大块分配，其余都是档内内存搬运。
 *  6. 池是进程内单例（kv_pool_init / kv_pool_shutdown）：未 init 时
 *     alloc/free 直接透传底座（容错冷启动路径，如 AOF 落盘线程）。
 *
 *  使用约定（不变量）：
 *   - 池内分配的指针只经 kv_pool_free(ptr, size) 归还，size 与 alloc 时
 *     一致（底层语义：同一次 alloc/free 的 size 必须映射到同一档位）。
 *   - kv_pool_free 收到 NULL 为 no-op。
 *
 *  已知取舍（刻意未做）：
 *  1. 档内碎片：请求尺寸向上取整到 16B 倍数档位，小档位最多浪费 ≤15B；
 *     档位表（见 .c）步进在后段加密到 32/64/128，中大体量浪费有界。
 *  2. 超块归还只做"全清归还"：kv_pool_release_all 在「池内无存活对象」时
 *     （如 dict_clear 全清后）把全部超块整批还给底座。部分删除后的
 *     "增量归还空超块"（需 per-slab 存活记账、碰 alloc/free 热路径）不做。
 *  3. 超块大小 KV_POOL_SUPERBLOCK 暂定 64KB（< glibc mmap 阈值，走堆）。
 *  4. 档位上限 2048：节点总尺寸超过即走底座，接入后按真实尺寸分布再调。
 *  5. 接入点 = 四个引擎各自 .c 里的 node_new / node_free（池句柄由本文件
 *     自己持有，见 kv_pool_init）。各引擎的全清路径在全释放后调
 *     kv_pool_release_all 触发整批归还。
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

/*-------------------------------------------------------------------------
 *  生命周期（进程单例：调用方不持有句柄）
 *-------------------------------------------------------------------------*/

/**
 * 初始化内存池（空池：所有超块按需惰性分配）
 *
 * 池在进程内只有一份 —— 数据域只有一个，池也就只需要一个。重复调用为 no-op。
 * 未 init 或 init 失败时，后续 kv_pool_alloc 直接落底座 malloc，调用方无需
 * 判空、也无需在任何地方保存池句柄。
 *
 * @return 0 成功（含重复调用），-1 失败（档位表异常 / OOM，此后走底座）
 */
int kv_pool_init(void);

/**
 * 销毁内存池（释放全部超块）
 * 须在池内已无存活对象时调用（同 kv_pool_release_all 的契约）；未 init 为 no-op
 */
void kv_pool_shutdown(void);

/**
 * 整批归还超块给底座（"全清归还"）
 *
 * 契约：调用方须保证池内已无存活对象——典型场景是 dict_clear 把 dict 全清
 * 之后（所有 entry 均已归还，各档 live_n == 0）。此时每个超块必空，直接
 * 整块 free 回底座并把各档切分状态（cur/freelist）重置为空：下次 kv_pool_alloc
 * 重新开块。slab_count 随之归零。部分删除（仍存活对象）后调用属未定义行为。
 * 未 init 为 no-op。
 */
void kv_pool_release_all(void);

/**
 * 分配
 *
 * @param size 请求字节数（0 → 按最小档位处理）
 * @return 对齐到 ≥KV_POOL_ALIGN 的内存块；失败返回 NULL
 *
 * 语义：
 *  - 未 init（或 init 失败）：底座 malloc 原样返回
 *  - size ≤ KV_POOL_CLASS_MAX：从对应档位取块（空闲链优先，空则切当前超块）
 *  - size >  KV_POOL_CLASS_MAX：底座 malloc 原样返回
 */
void *kv_pool_alloc(size_t size);

/**
 * 归还
 *
 * @param ptr 由 kv_pool_alloc 返回的指针（NULL 为 no-op）
 * @param size 与当初 alloc 请求的尺寸一致（档位路由与底座判定靠它，不查块头）
 */
void kv_pool_free(void *ptr, size_t size);

/*-------------------------------------------------------------------------
 *  观测（只留尺寸契约的两个哨兵，读者 = test/engtest.c）
 *-------------------------------------------------------------------------*/

typedef struct {
    uint64_t slab_count;    /* 现存超块数；全清后须归 0 */
    uint64_t live_objects;  /* 池内存活对象数（已 alloc 未 free）；负数=契约被破坏 */
} kv_pool_stats_t;

/**
 * 取池统计快照
 * @param out 输出结构（非 NULL）；未 init 时全 0
 */
void kv_pool_stats(kv_pool_stats_t *out);

#endif /* __KV_POOL_H__ */
