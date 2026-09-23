/*=============================================================================
 *  engtest.c — 引擎内存账本自检（不连网络，直接驱动四个引擎）
 *
 *  用途：验证「每个引擎自己管自己节点的内存」是否守住了 kv_pool 的尺寸契约。
 *  契约一旦被破坏（例如变长时原地改 val_len 而不重建），free 时推导出的尺寸
 *  会小于实际块，池的档位 live 计数被减成负数 —— 用 live_objects 收尾必须
 *  回 0 来当哨兵。
 *
 *  构建：与四个引擎 .c / kv_pool.c 一起编译
 *  运行：./engtest <hash|array|rbtree|skiplist>
 *============================================================================*/

#include "kv_engine.h"
#include "kv_pool.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define N        20000
#define VLEN_MAX 512

static int g_fail = 0;

static void check(int cond, const char *what) {
    if (!cond) { printf("  [FAIL] %s\n", what); g_fail = 1; }
}

static void mk(int i, char *k, char *v) {
    snprintf(k, 32, "fill:%012d", i);
    snprintf(v, 32, "value:%012d", i);
}

int main(int argc, char **argv) {
    if (argc < 2) { fprintf(stderr, "用法: %s <engine>\n", argv[0]); return 2; }
    if (kv_engine_select(argv[1]) < 0) { fprintf(stderr, "未知引擎\n"); return 2; }

    kv_pool_init();
    dict_init();

    kv_pool_stats_t s0, s1, s2, s3;

    /*--- 1. 填充 N 条 ---*/
    char k[32], v[32];
    for (int i = 0; i < N; i++) {
        mk(i, k, v);
        void *e = dict_add(k, (uint32_t)strlen(k), v, (uint32_t)strlen(v));
        if (!e) { fprintf(stderr, "add 失败 @%d\n", i); return 1; }
    }
    kv_pool_stats(&s0);
    check(dict_count() == N, "填充后 count == N");

    /*--- 2. 逐条 find + 值校验 ---*/
    int bad = 0;
    for (int i = 0; i < N; i++) {
        mk(i, k, v);
        kv_node_t *e = dict_find(k, (uint32_t)strlen(k));
        if (!e || entry_payload_len(e) != (uint32_t)strlen(v) ||
            memcmp(entry_payload(e), v, strlen(v)) != 0) bad++;
    }
    check(bad == 0, "逐条 find 与值校验");

    /*--- 3. 变长重建：把每 10 条改成长短交替（放大 / 缩小都走一遍） ---*/
    int resized = 0;
    for (int i = 0; i < N; i += 10) {
        mk(i, k, v);
        kv_node_t *e = dict_find(k, (uint32_t)strlen(k));
        if (!e) continue;
        uint32_t nlen = (i % 20 == 0) ? VLEN_MAX : 2;   /* 一会放大、一会缩小 */
        kv_node_t *ne = dict_resize_entry(e, nlen);
        if (!ne) { bad++; continue; }
        memset(entry_payload(ne), (i % 20 == 0) ? 'A' : 'z', nlen);
        resized++;
    }
    check(bad == 0, "变长重建无失败");

    /* 重建后逐条复核：key 没丢、长度对、内容对 */
    bad = 0;
    for (int i = 0; i < N; i++) {
        mk(i, k, v);
        kv_node_t *e = dict_find(k, (uint32_t)strlen(k));
        if (!e) { bad++; continue; }
        uint32_t wl = entry_payload_len(e);
        char *vp = entry_payload(e);
        if (i % 10 == 0) {
            uint32_t want = (i % 20 == 0) ? VLEN_MAX : 2;
            char   wc   = (i % 20 == 0) ? 'A' : 'z';
            if (wl != want || vp[0] != wc) bad++;
        } else if (wl != (uint32_t)strlen(v) || memcmp(vp, v, wl) != 0) {
            bad++;
        }
    }
    check(bad == 0, "变长重建后全量复核");
    check(dict_count() == N, "重建后 count 仍 == N");

    /*--- 4. 迭代器走一遍，条数必须对 ---*/
    kv_iter_t *it = dict_iter_new();
    uint32_t seen = 0;
    while (dict_iter_next(it)) seen++;
    dict_iter_free(it);
    check(seen == N, "迭代器遍历条数 == N");

    /*--- 5. 随机采样取得到东西 ---*/
    check(dict_get_random_key() != NULL, "随机采样非空");

    /*--- 6. 覆盖写（删一半再加回，走 delete 的收尾路径） ---*/
    for (int i = 0; i < N; i += 2) {
        mk(i, k, v);
        if (dict_delete(k, (uint32_t)strlen(k)) != 0) bad++;
    }
    check(bad == 0, "删除一半全部命中");
    check(dict_count() == N / 2, "删除后 count == N/2");
    kv_pool_stats(&s1);

    /*--- 7. 删光 ---*/
    for (int i = 1; i < N; i += 2) {
        mk(i, k, v);
        dict_delete(k, (uint32_t)strlen(k));
    }
    check(dict_count() == 0, "删光后 count == 0");
    kv_pool_stats(&s2);

    /*--- 8. 全清：池内必须无存活对象，且超块整批归还 ---*/
    dict_clear();
    kv_pool_stats(&s3);

    printf("%-8s count=%u→%u→%u\n"
           "  池账本： 填充后 live=%llu\n"
           "           删半后 live=%llu\n"
           "           删光后 live=%llu\n"
           "           全清后 live=%llu slab=%llu\n",
           argv[1], N, N / 2, 0,
           (unsigned long long)s0.live_objects,
           (unsigned long long)s1.live_objects,
           (unsigned long long)s2.live_objects,
           (unsigned long long)s3.live_objects,
           (unsigned long long)s3.slab_count);

    check(s3.live_objects == 0,        "全清后 live_objects == 0（负数=尺寸契约被破坏）");
    check(s3.slab_count == 0,          "全清后超块整批归还（slab_count == 0）");
    check(s0.live_objects == N,        "填充后 live_objects == N");
    check(s1.live_objects == N / 2,    "删半后 live_objects == N/2");
    check(s2.live_objects == 0,        "删光后 live_objects == 0");

    kv_pool_shutdown();
    printf("  ==> %s\n", g_fail ? "FAIL" : "PASS");
    return g_fail;
}
