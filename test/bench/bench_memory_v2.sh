#!/bin/bash
# bench_memory_v2.sh — 内存分配器对比测试（增强版）

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

require_root
require_tools

VALUE_SIZE=64
KEY_OVERHEAD=48

if [ $# -ge 1 ]; then
    KEY_COUNTS=("$@")
else
    KEY_COUNTS=(100000)
fi

KVS_JEMALLOC="${PROJECT_DIR}/build/kvstore"
KVS_GLIBC="${PROJECT_DIR}/build-glibc/kvstore"
RESULT_FILE="${PROJECT_DIR}/test/bench/results_memory.txt"
RESULT_CSV="${PROJECT_DIR}/test/bench/results_memory.csv"

log "bench_memory_v2.sh 脚本已创建"
log "测试配置: ${KEY_COUNTS[*]} keys"

sample_memory_avg() {
    local pid=$1 samples=${2:-3}
    local sum_vmsize=0 sum_vmrss=0 sum_vmdata=0
    for i in $(seq 1 $samples); do
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "0 0 0"
            return
        fi
        local status="/proc/$pid/status"
        local vmsize=$(grep "^VmSize:" "$status" 2>/dev/null | awk '{print $2}')
        local vmrss=$(grep "^VmRSS:" "$status" 2>/dev/null | awk '{print $2}')
        local vmdata=$(grep "^VmData:" "$status" 2>/dev/null | awk '{print $2}')
        sum_vmsize=$((sum_vmsize + ${vmsize:-0}))
        sum_vmrss=$((sum_vmrss + ${vmrss:-0}))
        sum_vmdata=$((sum_vmdata + ${vmdata:-0}))
        [ $i -lt $samples ] && sleep 0.5
    done
    echo "$((sum_vmsize / samples)) $((sum_vmrss / samples)) $((sum_vmdata / samples))"
}

wait_memory_stable() {
    local pid=$1 max_wait=${2:-60}
    local prev_rss=999999999 stable_count=0
    log "等待内存归还..."
    for i in $(seq 1 $((max_wait / 5))); do
        sleep 5
        if ! kill -0 "$pid" 2>/dev/null; then
            warn "进程已退出"
            return 1
        fi
        local curr_rss=$(grep "^VmRSS:" "/proc/$pid/status" 2>/dev/null | awk '{print $2}')
        curr_rss=${curr_rss:-0}
        local change_pct=0
        if [ "$prev_rss" -gt 0 ]; then
            change_pct=$(echo "scale=2; ($prev_rss - $curr_rss) * 100 / $prev_rss" | bc -l 2>/dev/null || echo 0)
        fi
        if [ "$(echo "$change_pct < 1" | bc -l 2>/dev/null)" = "1" ]; then
            stable_count=$((stable_count + 1))
            if [ $stable_count -ge 2 ]; then
                log "内存已稳定 (RSS=${curr_rss} kB, 耗时 $((i*5))s)"
                return 0
            fi
        else
            stable_count=0
        fi
        prev_rss=$curr_rss
    done
    warn "等待超时 ${max_wait}s"
    return 0
}

calc_theoretical_memory() {
    local nkeys=$1 key_len=9
    echo $(echo "scale=0; $nkeys * ($key_len + $VALUE_SIZE + $KEY_OVERHEAD) / 1024" | bc -l)
}

calc_metrics() {
    local nkeys=$1 vmrss_kb=$2
    local theoretical_kb=$(calc_theoretical_memory "$nkeys")
    local utilization=0
    if [ "$vmrss_kb" -gt 0 ]; then
        utilization=$(echo "scale=1; $theoretical_kb * 100 / $vmrss_kb" | bc -l 2>/dev/null || echo 0)
    fi
    local fragmentation=0
    if [ "$vmrss_kb" -gt 0 ]; then
        fragmentation=$(echo "scale=1; ($vmrss_kb - $theoretical_kb) * 100 / $vmrss_kb" | bc -l 2>/dev/null || echo 0)
    fi
    echo "$utilization $fragmentation"
}

ensure_build() {
    local label=$1 bin=$2 use_jemalloc=$3
    if [ -x "$bin" ]; then
        log "${label} 二进制已存在"
        return
    fi
    warn "${label} 二进制不存在，开始编译..."
    local build_dir=$(dirname "$bin")
    mkdir -p "$build_dir" && cd "$build_dir"
    if [ "$use_jemalloc" = "yes" ]; then
        cmake "${PROJECT_DIR}" -DCMAKE_BUILD_TYPE=Release > /dev/null 2>&1
    else
        cmake "${PROJECT_DIR}" -DCMAKE_BUILD_TYPE=Release -DUSE_JEMALLOC=OFF > /dev/null 2>&1
    fi
    make -j$(nproc) kvstore > /dev/null 2>&1
    cd "$PROJECT_DIR"
    if [ -x "$bin" ]; then
        log "${label} 编译完成"
    else
        err "${label} 编译失败"
        exit 1
    fi
}

ensure_build "jemalloc" "$KVS_JEMALLOC" "yes"
ensure_build "glibc" "$KVS_GLIBC" "no"

scenario_sequential() {
    log "场景A: 顺序写入"
    redis-benchmark -h "$1" -p "$2" -t set -n "$3" -r "$3" -d "$VALUE_SIZE" -q >/dev/null 2>&1 || true
}

scenario_random_update() {
    log "场景B: 随机更新"
    redis-benchmark -h "$1" -p "$2" -t set -n "$3" -r "$3" -d "$VALUE_SIZE" -q >/dev/null 2>&1 || true
    redis-benchmark -h "$1" -p "$2" -t set -n "$3" -r "$3" -d "$VALUE_SIZE" -q >/dev/null 2>&1 || true
}

test_allocator_scenario() {
    local label=$1 bin=$2 nkeys=$3 scenario_name=$4 scenario_func=$5

    log "测试: ${label} - ${scenario_name} (${nkeys} keys)"

    stop_redis
    start_kvs_bin "$bin" "kvstore-none.conf"
    local port
    port=$(grep '^port ' "${CONF_DIR}/kvstore-none.conf" | awk '{print $2}')
    port=${port:-$KVS_PORT}
    local pid
    pid=$(pgrep -f "$bin")

    if [ -z "$pid" ]; then
        err "无法获取进程 PID"
        return 1
    fi

    sleep 1
    local idle_mem
    idle_mem=$(sample_memory_avg "$pid" 3)
    local idle_vmsize idle_vmrss idle_vmdata
    idle_vmsize=$(echo "$idle_mem" | awk '{print $1}')
    idle_vmrss=$(echo "$idle_mem" | awk '{print $2}')
    idle_vmdata=$(echo "$idle_mem" | awk '{print $3}')

    $scenario_func 127.0.0.1 "$port" "$nkeys"

    sleep 1
    local peak_mem
    peak_mem=$(sample_memory_avg "$pid" 5)
    local peak_vmsize peak_vmrss peak_vmdata
    peak_vmsize=$(echo "$peak_mem" | awk '{print $1}')
    peak_vmrss=$(echo "$peak_mem" | awk '{print $2}')
    peak_vmdata=$(echo "$peak_mem" | awk '{print $3}')

    redis-cli -h 127.0.0.1 -p "$port" FLUSHALL >/dev/null 2>&1 || true

    wait_memory_stable "$pid" 60

    local final_mem
    final_mem=$(sample_memory_avg "$pid" 5)
    local final_vmsize final_vmrss final_vmdata
    final_vmsize=$(echo "$final_mem" | awk '{print $1}')
    final_vmrss=$(echo "$final_mem" | awk '{print $2}')
    final_vmdata=$(echo "$final_mem" | awk '{print $3}')

    local metrics
    metrics=$(calc_metrics "$nkeys" "$peak_vmrss")
    local utilization fragmentation
    utilization=$(echo "$metrics" | awk '{print $1}')
    fragmentation=$(echo "$metrics" | awk '{print $2}')

    local reclaim_pct=0
    if [ "$peak_vmrss" -gt "$idle_vmrss" ]; then
        local delta=$((peak_vmrss - idle_vmrss))
        local reclaimed=$((peak_vmrss - final_vmrss))
        if [ "$delta" -gt 0 ]; then
            reclaim_pct=$(echo "scale=1; $reclaimed * 100 / $delta" | bc -l 2>/dev/null || echo 0)
        fi
    fi

    echo "${label},${scenario_name},${nkeys},${idle_vmsize},${idle_vmrss},${idle_vmdata},${peak_vmsize},${peak_vmrss},${peak_vmdata},${final_vmsize},${final_vmrss},${final_vmdata},${utilization},${fragmentation},${reclaim_pct}" >> "$RESULT_CSV"

    log "  空闲: RSS=${idle_vmrss} kB"
    log "  峰值: RSS=${peak_vmrss} kB (利用率=${utilization}%, 碎片率=${fragmentation}%)"
    log "  结束: RSS=${final_vmrss} kB (回收率=${reclaim_pct}%)"

    stop_kvs
}

start_kvs_bin() {
    local bin=$1 conf=$2
    local conf_path="${CONF_DIR}/${conf}"
    if [ ! -f "$conf_path" ]; then
        err "配置文件不存在: $conf_path"
        exit 1
    fi
    log "启动 kvstore: $bin"
    nohup "$bin" "$conf_path" > /tmp/kvstore_new.log 2>&1 &
    sleep 3
    if ! pgrep -f "$bin" >/dev/null; then
        err "kvstore 启动失败"
        cat /tmp/kvstore_new.log
        exit 1
    fi
}

stop_kvs() {
    pkill -9 kvstore 2>/dev/null || true
    sleep 1
}

init_output() {
    local output_dir="${PROJECT_DIR}/test/bench"
    mkdir -p "$output_dir"

    cat > "$RESULT_FILE" << EOF
=============================================================================
内存分配器对比测试 (jemalloc vs glibc)
=============================================================================
测试时间: $(date '+%Y-%m-%d %H:%M:%S')
测试配置:
  - 线程数: 1
  - 持久化: 关闭 (persistence=none)
  - Value大小: ${VALUE_SIZE} bytes
  - 数据集: ${KEY_COUNTS[*]} keys

测试场景:
  场景A: 顺序写入
  场景B: 随机更新

测量指标:
  - VmSize: 虚拟内存 (kB)
  - VmRSS:  物理内存 (kB)
  - VmData: 堆内存 (kB)
  - 利用率: 理论内存 / 实际RSS × 100%
  - 碎片率: (实际RSS - 理论内存) / 实际RSS × 100%
  - 回收率: (峰值RSS - 结束RSS) / (峰值RSS - 空闲RSS) × 100%

=============================================================================

EOF

    echo "allocator,scenario,keys,idle_vmsize,idle_vmrss,idle_vmdata,peak_vmsize,peak_vmrss,peak_vmdata,final_vmsize,final_vmrss,final_vmdata,utilization_pct,fragmentation_pct,reclaim_pct" > "$RESULT_CSV"
}

generate_summary() {
    echo "" | tee -a "$RESULT_FILE"
    echo "════════════════════════════════════════════════" | tee -a "$RESULT_FILE"
    echo "  汇总对比表" | tee -a "$RESULT_FILE"
    echo "════════════════════════════════════════════════" | tee -a "$RESULT_FILE"
    echo "" | tee -a "$RESULT_FILE"

    for nkeys in "${KEY_COUNTS[@]}"; do
        echo "数据集: ${nkeys} keys" | tee -a "$RESULT_FILE"
        printf "  %-12s | %-10s | %12s | %12s | %12s | %8s | %8s | %8s\n" \
            "场景" "分配器" "空闲RSS" "峰值RSS" "结束RSS" "利用率" "碎片率" "回收率" | tee -a "$RESULT_FILE"
        printf "  %-12s | %-10s | %12s | %12s | %12s | %8s | %8s | %8s\n" \
            "------------" "----------" "------------" "------------" "------------" "--------" "--------" "--------" | tee -a "$RESULT_FILE"

        for scenario in "顺序写入" "随机更新"; do
            for alloc in "jemalloc" "glibc"; do
                local line
                line=$(grep "^${alloc},${scenario},${nkeys}," "$RESULT_CSV")
                if [ -n "$line" ]; then
                    local idle_rss peak_rss final_rss util frag reclaim
                    idle_rss=$(echo "$line" | cut -d, -f5)
                    peak_rss=$(echo "$line" | cut -d, -f8)
                    final_rss=$(echo "$line" | cut -d, -f11)
                    util=$(echo "$line" | cut -d, -f13)
                    frag=$(echo "$line" | cut -d, -f14)
                    reclaim=$(echo "$line" | cut -d, -f15)

                    printf "  %-12s | %-10s | %12s | %12s | %12s | %7s%% | %7s%% | %7s%%\n" \
                        "$scenario" "$alloc" "$idle_rss" "$peak_rss" "$final_rss" "$util" "$frag" "$reclaim" | tee -a "$RESULT_FILE"
                fi
            done
        done
        echo "" | tee -a "$RESULT_FILE"
    done

    echo "==============================================================================" | tee -a "$RESULT_FILE"
    echo "" | tee -a "$RESULT_FILE"
}

init_output

declare -A SCENARIOS=(
    ["顺序写入"]="scenario_sequential"
    ["随机更新"]="scenario_random_update"
)

for nkeys in "${KEY_COUNTS[@]}"; do
    echo ""
    echo "════════════════════════════════════════════════"
    step "数据集: ${nkeys} keys"
    echo "════════════════════════════════════════════════"

    for scenario_name in "顺序写入" "随机更新"; do
        scenario_func="${SCENARIOS[$scenario_name]}"

        test_allocator_scenario "jemalloc" "$KVS_JEMALLOC" "$nkeys" "$scenario_name" "$scenario_func" | tee -a "$RESULT_FILE"

        test_allocator_scenario "glibc" "$KVS_GLIBC" "$nkeys" "$scenario_name" "$scenario_func" | tee -a "$RESULT_FILE"
    done
done

generate_summary

echo ""
log "测试完成！结果已保存："
log "  文本报告: $RESULT_FILE"
log "  CSV数据:  $RESULT_CSV"
echo ""
