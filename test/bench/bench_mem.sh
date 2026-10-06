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
#   ② 填充 2000000 个 key（value 16B）后   读 VmPeak / VmHWM —— 峰值水位线
#   ③ FLUSHALL 后                         读 VmSize / VmRSS —— 回落水位
# VmSize/VmRSS 是读的那一刻的数，会随着归还内存掉下来；VmPeak/VmHWM 只增不减，
# 填充撑到多高只有它记得住，所以峰值看 ② 的这两个。
#
# 归还内存靠两边各自的「立即归还」，除此之外全走 allocator 默认行为：
#   jemalloc  由本脚本注入环境变量 MALLOC_CONF=dirty_decay_ms:0,muzzy_decay_ms:0
#   glibc     没有环境变量版，写在代码里（kvs_flush_all 的 malloc_trim(0)），
#             脚本不管。MALLOC_TRIM_THRESHOLD_ 不是它的替代品：本负载每次 free
#             的是 64B，够不着 glibc 那条「≥64KiB 才进 consolidation」的门槛，
#             单独注它等于没注（探针里与默认逐 kB 相同，kvstore 真机上只差几十 kB
#             残余），所以这里不注。
#
# 关掉 ASLR（setarch -R，走 personality()，不需要 root）。它只固定内存布局，分配器
# 行为一字不变，但组内散布从 80/152/68 kB（启动/峰值/结束）收到 0/44/4 kB，且谁跑
# 都是同一个数。启动 RSS 尤其吃这一口：它 96% 是文件映射的常驻页，ASLR 一变哪些
# 库页驻留就变，±44 kB 的抖动足以盖住任何 kB 级的配置差异。
# 代价：这是非默认条件，不照做的话数会略有出入。
#
# 特别注意：不要加 MALLOC_MMAP_THRESHOLD_=0。它会让 glibc 每次需要新空间都改走
# mmap（而不是从堆顶扩），小对象再也合不成大块，峰值直接放大 2.5 倍，量出来的
# 是这套旋钮的形状而不是 glibc 的形状。隔离探针实测（200 万次 malloc(64) + 一个
# 常驻 16MB 指针数组，负载 137 MiB，与 kvstore 完全解耦）：
#   glibc 默认               峰值 170 MiB   free 后 169 MiB（不还）
#   glibc + 上面那个旋钮     峰值 421 MiB   free 后 164 MiB   ← 对照 kvstore 实测 423 MiB
#   jemalloc + MALLOC_CONF   峰值 146 MiB   free 后 24 MiB（含那 16MB 常驻数组，
#                                            扣掉它真残留 ≈8 MiB，与 kvstore 的 9.8 MiB 对得上）
#   jemalloc 默认参数        峰值 146 MiB   free 后 149 MiB —— free 后等 12 秒再读一模一样。
#   为什么默认不还：jemalloc 没开后台线程时 decay 是「惰性」触发的（下次分配时才处理），
#   进程 free 完就闲住的话永远不会跑。所以 MALLOC_CONF 那行不是装饰，是让 ③ 有意义的前提。
# =============================================================================

PORT=2000
KEYS=2000000
VSIZE=16

fuser -k -n tcp "$PORT" 2>/dev/null
sleep 0.5

# 回显本轮的出身：二进制链的哪个 allocator + conf 里 kv_pool 的开关。
# 这两样都要手动准备（重编 / 改 conf），跑完就查不到了，所以打在最前面跟着结果一起留档。
# 用 grep -q 判断而不是 grep -o 取名字：ldd 那行里库名出现两次（soname + 路径），
# grep -o 会吐两行，把后面那半句挤到第二行去。
if ldd build/kvstore | grep -q jemalloc; then ALLOC=jemalloc; else ALLOC='libc(glibc)'; fi
echo "  配置：$ALLOC  |  $(grep -E '^kv_pool' conf/kvstore-none.conf)"

MALLOC_CONF="dirty_decay_ms:0,muzzy_decay_ms:0" \
setarch "$(uname -m)" -R taskset -c 1 build/kvstore conf/kvstore-none.conf &
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
