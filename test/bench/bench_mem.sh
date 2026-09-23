#!/bin/bash
# =============================================================================
# bench_mem.sh — kvstore 三个内存时点的虚拟内存 / 物理内存
#
# 在项目根目录执行：
#   ./test/bench/bench_mem.sh
#
# 跑之前自己准备好（脚本一概不管）：
#   · 二进制编到 build/kvstore（链 glibc 还是 jemalloc，由你编译时决定）
#   · conf/kvstore-none.conf 里 kv_pool 的开关，由你手动改
#
# 一次跑一轮，读三个时点的内存（都取自 /proc/<pid>/status）：
#   ① 启动后（空库）                      读 VmSize / VmRSS —— 当时水位
#   ② 填充 200000 个 key（value 16B）后    读 VmPeak / VmHWM —— 峰值水位线
#   ③ FLUSHALL 后                         读 VmSize / VmRSS —— 回落水位
# VmSize/VmRSS 是读的那一刻的数，会随着归还内存掉下来；VmPeak/VmHWM 只增不减，
# 填充撑到多高只有它记得住，所以峰值看 ② 的这两个。
#
# 注入两边 allocator 的「立即归还」，各版本只认自己那份、忽略另一份：
#   MALLOC_CONF=dirty_decay_ms:0,muzzy_decay_ms:0       jemalloc 认
#   MALLOC_TRIM_THRESHOLD_=0 MALLOC_MMAP_THRESHOLD_=0   glibc 认
# =============================================================================

PORT=2000
KEYS=2000000
VSIZE=16

fuser -k -n tcp "$PORT" 2>/dev/null
sleep 0.5

MALLOC_CONF="dirty_decay_ms:0,muzzy_decay_ms:0" \
MALLOC_TRIM_THRESHOLD_=0 MALLOC_MMAP_THRESHOLD_=0 \
taskset -c 1 build/kvstore conf/kvstore-none.conf &
PID=$!
sleep 1

# 读 /proc/<pid>/status → "VmSize VmRSS VmPeak VmHWM"（kB）
mem() { awk '/^VmSize:/{s=$2} /^VmRSS:/{r=$2} /^VmPeak:/{p=$2} /^VmHWM:/{h=$2}
            END{print s, r, p, h}' "/proc/$PID/status"; }

read -r v0 r0 _ _ <<<"$(mem)"

# 填充：awk 造 RESP 流 → redis-cli --pipe（不依赖 python）。
# 每个 key 的分配尺寸 = hash_node_t(32) + key(16,"key:%012d") + value(16) = 64B，
# 正好是自研内存池档位表里的一档（src/util/kv_pool.c 的 k_class_sizes），
# 原档命中、不向上取整，零碎片；jemalloc 的 malloc_usable_size(64) 也是 64。
awk -v n="$KEYS" -v vs="$VSIZE" '
    BEGIN {
        for (j = 0; j < vs; j++) v = v sprintf("%c", 65 + (j % 26));
        for (i = 0; i < n; i++) {
            k = sprintf("key:%012d", i);
            printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(k), k, vs, v;
        }
    }' | redis-cli -p "$PORT" --pipe

read -r _ _ p1 h1 <<<"$(mem)"

redis-cli -p "$PORT" FLUSHALL >/dev/null

read -r v2 r2 _ _ <<<"$(mem)"

fuser -k -n tcp "$PORT" 2>/dev/null

echo
echo "  ① 启动        VmSize=$v0  VmRSS=$r0"
echo "  ② 填充后      VmPeak=$p1  VmHWM=$h1"
echo "  ③ FLUSHALL后  VmSize=$v2  VmRSS=$r2"
