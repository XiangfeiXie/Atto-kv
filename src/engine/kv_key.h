/*=============================================================================
 *  kv_key.h — 键原语（原 kv_node.h 的公共部分）
 *
 *  这里只有「怎么比两个键」，没有任何节点类型：
 *
 *  - kv_fast_cmp    ：字节相等判定（不等长直接拒，8/4 字节块比对）
 *  - kv_key_cmp     ：全序比较（红黑树 / 跳表要排序，光有相等不够）
 *  - kv_next_pow2   ：桶数 / 槽位数的 2 的幂对齐
 *  - kv_key_eq      ：长度 + 字节的等值判定（前两者的糖）
 *
 *  没有哈希函数：哈希只有哈希引擎用得上（桶定位），因此它是 kv_hash.c 里的
 *  static —— 外面既不需要算，也拿不到，键的哈希成了哈希引擎的内部事务。
 *
 *  「记录长什么样」不在这里 —— 四个引擎各在自己的 .c 里定义自己的节点
 *  （见 kv_engine.h 的文件头）。本文件对它们是纯工具，不含任何共享状态。
 *============================================================================*/

#ifndef __KV_KEY_H__
#define __KV_KEY_H__

#include <stdint.h>
#include <string.h>

/*=============================================================================
 *  分支预测宏
 *============================================================================*/

#ifndef likely
#define likely(x)   __builtin_expect(!!(x), 1)   /* 大概率 true */
#define unlikely(x) __builtin_expect(!!(x), 0)   /* 小概率 true */
#endif

/*=============================================================================
 *  极速字节比对（相等判定）
 *
 *  策略：8/4 字节块比对 + 尾部逐字节
 *  使用 memcpy 装入 uint64_t/uint32_t：
 *  - 编译器在 -O2 下退化为单条非对齐 load
 *  - 既快又规避了 `*(uint64_t*)p` 的 strict-aliasing UB
 *
 * @return 0 相等，非 0 不等
 *============================================================================*/

static inline int kv_fast_cmp(const char *p1, const char *p2, int len) {
    int i = 0;

    /* 8 字节块比对（一次比较 8 字节） */
    while (len - i >= 8) {
        uint64_t a, b;
        memcpy(&a, p1 + i, 8);
        memcpy(&b, p2 + i, 8);
        if (a != b) return 1;
        i += 8;
    }

    /* 4 字节块比对 */
    if (len - i >= 4) {
        uint32_t a, b;
        memcpy(&a, p1 + i, 4);
        memcpy(&b, p2 + i, 4);
        if (a != b) return 1;
        i += 4;
    }

    /* 尾部逐字节比对 */
    while (i < len) {
        if (p1[i] != p2[i]) return 1;
        i++;
    }
    return 0;
}

/*=============================================================================
 *  键序比较（大小判定）
 *
 *  红黑树 / 跳表要有序，必须有全序而不只是相等：逐字节 memcmp，前 min(len)
 *  字节相同则短的在前。哈希 / 数组引擎只用相等判定（kv_fast_cmp），不用这个。
 *============================================================================*/

/**
 * 比较两个键的大小
 * @return <0 表示 a < b，0 表示相等，>0 表示 a > b
 */
static inline int kv_key_cmp(const char *a, uint32_t alen,
                             const char *b, uint32_t blen)
{
    uint32_t n = alen < blen ? alen : blen;
    int c = memcmp(a, b, n);
    if (c) return c;
    if (alen == blen) return 0;
    return alen < blen ? -1 : 1;
}

/**
 * 两个键是否相等（先比长度再比字节）
 *
 * 只是 kv_fast_cmp 的糖，四个引擎共用。参数是两个裸键 —— 不接节点类型：
 * 调用方自己用 entry_key()/skip_key() 之类的访问器把键取出来传进来。
 */
static inline int kv_key_eq(const char *a, uint32_t alen,
                            const char *b, uint32_t blen) {
    return alen == blen && kv_fast_cmp(a, b, (int)alen) == 0;
}

/** 计算大于等于 v 的最小 2 的幂（哈希桶数 / 数组槽位数用） */
static inline uint32_t kv_next_pow2(uint32_t v, uint32_t floor) {
    if (v <= floor) return floor;
    v--;
    v |= v >> 1;  v |= v >> 2;
    v |= v >> 4;  v |= v >> 8;
    v |= v >> 16;
    return v + 1;
}

#endif /* __KV_KEY_H__ */
