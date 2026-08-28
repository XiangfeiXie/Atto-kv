

/* kvstore_expire.c — 主动过期（active expire）
 *
 * 在单线程事件循环里按时间预算抽样删除已过期键；lazy 过期（访问时检查）
 * 在查询路径兜底。本文件只维护抽样删除循环，不涉及 fork 子进程。 */

#include "kvstore_internal.h"

/* 主动过期参数：
 * EXPIRE_BASE_SAMPLE     单轮抽样起点（键数）
 * EXPIRE_MAX_SAMPLE      抽样上限（超出则停止本轮）
 * EXPIRE_TIME_BUDGET_MS  单轮时间预算：超过即收尾，避免阻塞事件循环 */
#define EXPIRE_BASE_SAMPLE   20
#define EXPIRE_MAX_SAMPLE   200
#define EXPIRE_TIME_BUDGET_MS 1

/* 主动过期循环：每轮随机抽样 sample 个键删除已过期的；
 * 若过期比例高（>25%）则下一轮加倍抽样（自适应放大），否则结束。
 * 事件循环每次命令处理前调用，保证 TTL 键最终被清除。 */
void active_expire_cycle(void) {
    kv_dict_t *dict = &g_dict;
    long long   start = get_time_now_real();   /* 墙钟时间（毫秒），时间预算基准 */
    int         sample = EXPIRE_BASE_SAMPLE;

    do {
        int expired = 0;

        for (int i = 0; i < sample; i++) {
            /* 每 16 个键检查一次时间预算：超时立即收尾 */
            if ((i & 0xF) == 0) {
                if (get_time_now_real() - start > EXPIRE_TIME_BUDGET_MS)
                    return;                    /* 超预算，本轮收尾 */
            }

            kv_entry_t *e = dict_get_random_key(dict);
            if (!e) return;                    /* 字典已空 */

            if (e->expire && get_time_now() > e->expire) {
                /* 键内联在 entry 里——删除前先保存指针/长度/哈希 */
                const char *kp = entry_key(e);
                uint32_t    kl = e->key_len;
                uint64_t    h  = e->hash;

                dict_delete(dict, kp, kl, h);
                expired++;
            }
        }

        /* 自适应：本轮过期比例高则下一轮加倍抽样 */
        if (expired > sample / 4)
            sample *= 2;
        else
            break;

    } while (sample <= EXPIRE_MAX_SAMPLE);
}
