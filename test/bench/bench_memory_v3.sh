#!/bin/bash
# =============================================================================
# bench_memory_v3.sh — 内存分配器多线程 + 多大小对比测试
#
# 对比 glibc malloc 与 jemalloc 在以下维度的内存表现:
#   - 线程数: 1, 2, 4（测试多线程并发分配的碎片/开销）
#   - Value 大小: 16B, 64B, 256B, 1KB, 4KB（测试不同 size class）
#   - 工作负载: 顺序写入、随机更新
#
# 用法:
#   sudo ./test/bench/bench_memory_v3.sh
#
# 环境变量:
#   JEMALLOC_TUNE=1  对 jemalloc 做调优（dirty_decay/muzzy_decay/background_thread）
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

# Relax error handling — background kvstore processes may be killed by pkill,
# causing bash to report "Killed" which would otherwise trigger set -e
set +e
set +u
set +o pipefail

require_root
require_tools

# ── 全局清理 ────────────────────────────────────────────────────────────────

cleanup() {
    local ret=$?
    killall -9 redis-benchmark 2>/dev/null || true
    killall -9 kvstore 2>/dev/null || true
    rm -f /tmp/kvstore_mem_test_t*.conf
    exit $ret
}
trap cleanup EXIT

# ── 测试参数 ──────────────────────────────────────────────────────────────

THREAD_COUNTS=(1 4)         # 线程数维度（单线程 vs 多线程）
VALUE_SIZES=(64 256 1024 4096)  # value 大小维度

# Key count per value size: keep theoretical memory at ~25-200 MB
# 64B: 200K keys × 128B/key ≈ 25 MB
# 256B: 200K keys × 320B/key ≈ 62 MB
# 1KB: 200K keys × 1088B/key ≈ 212 MB → reduce to 100K ≈ 106 MB
# 4KB: 50K keys × 4160B/key ≈ 203 MB → reduce to 50K ≈ 203 MB
get_key_count() {
    case $1 in
        64)   echo 200000 ;;
        256)  echo 200000 ;;
        1024) echo 100000 ;;
        4096) echo 50000 ;;
        *)    echo 200000 ;;
    esac
}

KVS_JEMALLOC="${PROJECT_DIR}/build/kvstore"
KVS_GLIBC="${PROJECT_DIR}/build-glibc/kvstore"

RESULT_FILE="${PROJECT_DIR}/test/bench/results_memory_v3.txt"
RESULT_CSV="${PROJECT_DIR}/test/bench/results_memory_v3.csv"

JEMALLOC_TUNE="${JEMALLOC_TUNE:-0}"

# jemalloc 调优参数（降低 dirty page 保留时间，启用后台线程）
JEMALLOC_TUNED_ENV="MALLOC_CONF=dirty_decay_ms:1000,muzzy_decay_ms:1000,background_thread:true"

# ── 工具函数 ──────────────────────────────────────────────────────────────

init_output() {
    mkdir -p "$(dirname "$RESULT_FILE")"

    cat > "$RESULT_FILE" << 'HEADER'
=============================================================================
内存分配器对比测试 V3 — 多线程 + 多大小
=============================================================================
HEADER
    echo "测试时间: $(date '+%Y-%m-%d %H:%M:%S')" >> "$RESULT_FILE"
    echo "测试配置:" >> "$RESULT_FILE"
    echo "  - Key 数量: 64B/256B=200K, 1KB=100K, 4KB=50K" >> "$RESULT_FILE"
    echo "  - 线程数: ${THREAD_COUNTS[*]}" >> "$RESULT_FILE"
    echo "  - Value 大小: ${VALUE_SIZES[*]} bytes" >> "$RESULT_FILE"
    echo "  - jemalloc 调优: $( [ "$JEMALLOC_TUNE" = "1" ] && echo 'enabled' || echo 'disabled (default)' )" >> "$RESULT_FILE"
    echo "" >> "$RESULT_FILE"

    echo "allocator,threads,value_size,scenario,idle_vmrss,peak_vmrss,stable_vmrss,utilization_pct,fragmentation_pct,reclaim_pct,duration_s" > "$RESULT_CSV"
}

sample_memory_avg() {
    local pid=$1 samples=${2:-5}
    local sum_rss=0
    for i in $(seq 1 $samples); do
        if ! kill -0 "$pid" 2>/dev/null; then echo "0"; return; fi
        local rss=$(grep "^VmRSS:" "/proc/$pid/status" 2>/dev/null | awk '{print $2}' || true)
        sum_rss=$((sum_rss + ${rss:-0}))
        [ $i -lt $samples ] && sleep 0.3 || true
    done
    echo "$((sum_rss / samples))"
}

wait_memory_stable() {
    local pid=$1 max_wait=${2:-90}
    local prev=999999999 stable_count=0
    log "  等待内存稳定..."
    for i in $(seq 1 $((max_wait / 3))); do
        sleep 3
        if ! kill -0 "$pid" 2>/dev/null; then warn "  进程已退出"; return 1; fi
        local curr=$(grep "^VmRSS:" "/proc/$pid/status" 2>/dev/null | awk '{print $2}' || true)
        curr=${curr:-0}
        local pct=0
        if [ "$prev" -gt 0 ] && [ "$prev" -ne 999999999 ]; then
            pct=$(echo "scale=2; ($prev - $curr) * 100 / $prev" | bc -l 2>/dev/null || echo 0)
        fi
        have_stable=$(echo "$pct < 1" | bc -l 2>/dev/null || echo 0)
        if [ "$have_stable" = "1" ]; then
            stable_count=$((stable_count + 1))
            [ $stable_count -ge 3 ] && { log "  内存已稳定 (RSS=${curr} kB, 耗时 $((i*3))s)"; return 0; }
        else
            stable_count=0
        fi
        prev=$curr
    done
    warn "  等待超时 ${max_wait}s，取当前值"
    return 0
}

calc_metrics() {
    local nkeys=$1 peak_rss=$2 value_size=$3
    local theoretical=$(( nkeys * (KEY_LEN + value_size + KEY_OVERHEAD) / 1024 ))
    local util=0 frag=0
    if [ "$peak_rss" -gt 0 ]; then
        util=$(echo "scale=1; $theoretical * 100 / $peak_rss" | bc -l 2>/dev/null || echo 0)
        frag=$(echo "scale=1; ($peak_rss - $theoretical) * 100 / $peak_rss" | bc -l 2>/dev/null || echo 0)
    fi
    echo "$util $frag"
}

# ── 工作负载 ──────────────────────────────────────────────────────────────

# Key format: "key000001" = 9 chars
KEY_OVERHEAD=48
KEY_LEN=9

GEN_RESP="${PROJECT_DIR}/test/bench/gen_resp_data.py"
# Key format matching redis-benchmark -r: "key:000000000001" = 16 chars
KEY_OVERHEAD=48
KEY_LEN=16
RB_KEY_FMT="--redis-benchmark-keys"

fill_sequential() {
    local port=$1 nkeys=$2 vsize=$3
    log "  写入 ${nkeys} keys (sequential, ${vsize}B value)..."
    python3 "$GEN_RESP" "$nkeys" "$vsize" $RB_KEY_FMT \
        | redis-cli -p "$port" --pipe >/dev/null 2>&1 || true
    local count
    count=$(redis-cli -p "$port" DBSIZE 2>/dev/null || echo 0)
    log "  写入完成: ${count} keys"
}

fill_random() {
    local port=$1 nkeys=$2 vsize=$3
    # Fill with sequential keys first
    log "  初始填充 ${nkeys} keys..."
    python3 "$GEN_RESP" "$nkeys" "$vsize" $RB_KEY_FMT \
        | redis-cli -p "$port" --pipe >/dev/null 2>&1 || true
    # Pure in-place overwrite: same keys, random order (100% overlap)
    log "  随机覆盖 ${nkeys} ops (same keys, shuffled order, ${vsize}B value)..."
    python3 "$GEN_RESP" "$nkeys" "$vsize" $RB_KEY_FMT --random 42 \
        | redis-cli -p "$port" --pipe >/dev/null 2>&1 || true
    local count
    count=$(redis-cli -p "$port" DBSIZE 2>/dev/null || echo 0)
    log "  写入完成: ${count} keys"
}

# ── 测试主函数 ────────────────────────────────────────────────────────────

test_one() {
    local allocator=$1 bin=$2 threads=$3 vsize=$4 scenario=$5
    local nkeys
    nkeys=$(get_key_count "$vsize")

    local label="${allocator}-t${threads}-v${vsize}-${scenario}"
    log "========================================"
    log "测试: $label (${nkeys} keys)"
    log "========================================"

    # 生成临时配置
    local conf="/tmp/kvstore_mem_test_t${threads}.conf"
    cat > "$conf" << CONFEOF
port 2000
network proactor
maxclients 10000
loglevel error
persistence none
threads ${threads}
cpu_pin auto
rdma_sync off
CONFEOF

    # 杀掉所有残留进程（包括前序测试遗留的 redis-benchmark）
    killall -9 redis-benchmark 2>/dev/null || true
    killall -9 kvstore 2>/dev/null || true
    sleep 1
    rm -f "${PROJECT_DIR}/build/dump.rdb" "${PROJECT_DIR}/build/appendonly.aof"
    rm -f "${PROJECT_DIR}/build-glibc/dump.rdb" "${PROJECT_DIR}/build-glibc/appendonly.aof"

    # 构建启动命令（jemalloc 调优用 env）
    local launch_cmd="nohup"
    if [ "$allocator" = "jemalloc-tuned" ]; then
        launch_cmd="nohup env ${JEMALLOC_TUNED_ENV}"
    fi
    $launch_cmd "$bin" "$conf" > /tmp/kvstore_mem_test.log 2>&1 &
    local pid=$!
    sleep 2

    if ! kill -0 "$pid" 2>/dev/null; then
        err "启动失败: $label"
        tail -20 /tmp/kvstore_mem_test.log
        rm -f "$conf"
        return 1
    fi

    # 等待端口就绪
    wait_for_port 127.0.0.1 2000 15 || { err "端口就绪超时"; kill "$pid" 2>/dev/null; rm -f "$conf"; return 1; }

    # 1) 空闲态
    local idle_rss=$(sample_memory_avg "$pid" 5)
    log "  空闲 RSS: ${idle_rss} kB"

    # 2) 写入数据
    local t0=$(date +%s)
    case "$scenario" in
        sequential) fill_sequential 2000 "$nkeys" "$vsize" ;;
        random)     fill_random     2000 "$nkeys" "$vsize" ;;
    esac
    local t1=$(date +%s)

    # 峰值态
    sleep 1
    peak_mem=$(sample_memory_avg "$pid" 5)
    log "  峰值 RSS: ${peak_mem} kB"

    # 计算指标
    local metric
    metric=$(calc_metrics "$nkeys" "$peak_mem" "$vsize")
    local util=$(echo "$metric" | awk '{print $1}')
    local frag=$(echo "$metric" | awk '{print $2}')

    # 3) FLUSHALL + 等待稳定
    redis-cli -p 2000 FLUSHALL >/dev/null 2>&1 || true
    wait_memory_stable "$pid" 45
    local stable_rss
    stable_rss=$(sample_memory_avg "$pid" 5)
    log "  稳定 RSS: ${stable_rss} kB"

    # 回收率
    local reclaim=0
    local delta=$((peak_mem - idle_rss))
    if [ "$delta" -gt 0 ]; then
        reclaim=$(echo "scale=1; ($peak_mem - $stable_rss) * 100 / $delta" | bc -l 2>/dev/null || echo 0)
    fi

    local duration=$((t1 - t0))
    log "  利用率=${util}% 碎片率=${frag}% 回收率=${reclaim}% 写入耗时=${duration}s"

    echo "${allocator},${threads},${vsize},${scenario},${idle_rss},${peak_mem},${stable_rss},${util},${frag},${reclaim},${duration}" >> "$RESULT_CSV"

    # SIGKILL to guarantee the kvstore exits (SIGTERM may be ignored by proactor threads)
    kill -9 "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    # Belt-and-suspenders: kill any leftover redis-benchmark from this test
    killall -9 redis-benchmark 2>/dev/null || true
    sleep 1
    rm -f "$conf"
}

# ── 编译检查 ──────────────────────────────────────────────────────────────

ensure_build() {
    local label=$1 bin=$2 use_jemalloc=$3
    if [ -x "$bin" ]; then
        log "${label} 二进制已存在: $bin"
        return
    fi
    warn "${label} 二进制不存在，开始编译..."
    local build_dir=$(dirname "$bin")
    mkdir -p "$build_dir" && cd "$build_dir"
    if [ "$use_jemalloc" = "yes" ]; then
        cmake "${PROJECT_DIR}" -DCMAKE_BUILD_TYPE=Release >/dev/null 2>&1
    else
        cmake "${PROJECT_DIR}" -DCMAKE_BUILD_TYPE=Release -DUSE_JEMALLOC=OFF >/dev/null 2>&1
    fi
    make -j$(nproc) kvstore >/dev/null 2>&1
    cd "$PROJECT_DIR"
    [ -x "$bin" ] || { err "${label} 编译失败"; exit 1; }
    log "${label} 编译完成: $bin"
}

# ── 汇总表格 ──────────────────────────────────────────────────────────────

generate_summary() {
    local out="$RESULT_FILE"

    {
    echo ""
    echo "============================================================================="
    echo "  汇总对比表"
    echo "============================================================================="
    echo ""

    for threads in "${THREAD_COUNTS[@]}"; do
        for vsize in "${VALUE_SIZES[@]}"; do
            local nk
            nk=$(get_key_count "$vsize")
            echo "── 线程数=${threads}  Value=${vsize}B (${nk} keys, 理论: $(calc_theoretical_mem "$nk" "$vsize")) ──"
            printf "  %-16s | %10s | %10s | %10s | %8s | %8s | %8s\n" \
                "分配器" "空闲RSS" "峰值RSS" "稳定RSS" "利用率" "碎片率" "回收率"
            printf "  %-16s | %10s | %10s | %10s | %8s | %8s | %8s\n" \
                "----------------" "----------" "----------" "----------" "--------" "--------" "--------"

            for alloc in "jemalloc" "glibc"; do
                for scenario in "sequential" "random"; do
                    local line
                    line=$(grep "^${alloc},${threads},${vsize},${scenario}," "$RESULT_CSV" 2>/dev/null)
                    if [ -n "$line" ]; then
                        local idle peak stable util frag reclaim
                        idle=$(echo "$line"   | cut -d, -f5)
                        peak=$(echo "$line"   | cut -d, -f6)
                        stable=$(echo "$line" | cut -d, -f7)
                        util=$(echo "$line"   | cut -d, -f8)
                        frag=$(echo "$line"   | cut -d, -f9)
                        reclaim=$(echo "$line"| cut -d, -f10)

                        printf "  %-16s | %10s | %10s | %10s | %7s%% | %7s%% | %7s%%\n" \
                            "${alloc}/${scenario}" "$idle" "$peak" "$stable" "$util" "$frag" "$reclaim"
                    fi
                done
            done
            echo ""
        done
    done

    echo "============================================================================="
    echo "  关键发现"
    echo "============================================================================="
    } >> "$out"
}

calc_theoretical_mem() {
    local nkeys=$1 vsize=$2
    echo "$(( nkeys * (KEY_LEN + vsize + KEY_OVERHEAD) / 1024 )) kB"
}

# ── 主流程 ────────────────────────────────────────────────────────────────

init_output

ensure_build "jemalloc" "$KVS_JEMALLOC" "yes"
ensure_build "glibc"   "$KVS_GLIBC"   "no"

# 构建分配器列表（可选调优 jemalloc）
ALLOCATORS=("jemalloc:${KVS_JEMALLOC}" "glibc:${KVS_GLIBC}")
if [ "$JEMALLOC_TUNE" = "1" ]; then
    ALLOCATORS=("jemalloc-tuned:${KVS_JEMALLOC}" "glibc:${KVS_GLIBC}")
fi

SCENARIOS=("sequential" "random")

for threads in "${THREAD_COUNTS[@]}"; do
    for vsize in "${VALUE_SIZES[@]}"; do
        for scenario in "${SCENARIOS[@]}"; do
            for entry in "${ALLOCATORS[@]}"; do
                alloc=$(echo "$entry" | cut -d: -f1)
                bin=$(echo "$entry"   | cut -d: -f2)
                test_one "$alloc" "$bin" "$threads" "$vsize" "$scenario" || {
                    warn "测试失败: ${alloc} t=${threads} v=${vsize} ${scenario}，跳过"
                }
            done
        done
    done
done

generate_summary

echo ""
log "测试完成！结果已保存："
log "  文本报告: $RESULT_FILE"
log "  CSV数据:  $RESULT_CSV"
echo ""
cat "$RESULT_FILE"
