#!/bin/bash
# =============================================================================
# bench_memory_comparison.sh — jemalloc vs glibc 深度对比测试
#
# 设计目标：通过针对性压力场景突出两者优缺点
#
# 测试场景：
#   场景B: 混合大小分配（碎片炸弹）
#   场景C: 内存回收时间曲线
#   场景D: 长时间运行稳定性（10轮循环）
#   场景E: Churn工作负载（流失模拟）
#
# 用法:
#   sudo ./test/bench/bench_memory_comparison.sh
#
# 环境变量:
#   JEMALLOC_TUNE=1  启用 jemalloc 调优参数
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

# Relax error handling
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
    rm -f /tmp/kvstore_cmp_*.conf
    exit $ret
}
trap cleanup EXIT

# ── 测试参数 ──────────────────────────────────────────────────────────────

KVS_JEMALLOC="${PROJECT_DIR}/build/kvstore"
KVS_GLIBC="${PROJECT_DIR}/build-glibc/kvstore"

RESULT_FILE="${PROJECT_DIR}/test/bench/results_memory_comparison.txt"
RESULT_CSV="${PROJECT_DIR}/test/bench/results_memory_comparison.csv"

JEMALLOC_TUNE="${JEMALLOC_TUNE:-1}"
JEMALLOC_TUNED_ENV="MALLOC_CONF=dirty_decay_ms:1000,muzzy_decay_ms:1000,background_thread:true"

GEN_RESP="${PROJECT_DIR}/test/bench/gen_resp_data.py"
KEY_OVERHEAD=48
KEY_LEN=16
RB_KEY_FMT="--redis-benchmark-keys"

# ── 工具函数 ──────────────────────────────────────────────────────────────

init_output() {
    mkdir -p "$(dirname "$RESULT_FILE")"

    cat > "$RESULT_FILE" << 'HEADER'
=============================================================================
jemalloc vs glibc 内存分配器深度对比测试
=============================================================================
HEADER
    echo "测试时间: $(date '+%Y-%m-%d %H:%M:%S')" >> "$RESULT_FILE"
    echo "jemalloc 调优: $( [ "$JEMALLOC_TUNE" = "1" ] && echo 'enabled' || echo 'disabled' )" >> "$RESULT_FILE"
    echo "" >> "$RESULT_FILE"

    echo "scenario,allocator,metric,value" > "$RESULT_CSV"
}

sample_memory_avg() {
    local pid=$1 samples=${2:-5}
    local sum_rss=0
    for i in $(seq 1 $samples); do
        if ! kill -0 "$pid" 2>/dev/null; then echo "0"; return; fi
        local rss=$(grep "^VmRSS:" "/proc/$pid/status" 2>/dev/null | awk '{print $2}' || echo "0")
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
        local curr=$(grep "^VmRSS:" "/proc/$pid/status" 2>/dev/null | awk '{print $2}' || echo "0")
        curr=${curr:-0}
        local pct=0
        if [ "$prev" -gt 0 ] && [ "$prev" -ne 999999999 ]; then
            pct=$(echo "scale=2; ($prev - $curr) * 100 / $prev" | bc -l 2>/dev/null || echo 0)
        fi
        local have_stable=$(echo "$pct < 1" | bc -l 2>/dev/null || echo 0)
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

# ── 启动/停止 kvstore ─────────────────────────────────────────────────────

start_kvstore() {
    local allocator=$1 bin=$2
    local conf="/tmp/kvstore_cmp_${allocator}.conf"
    cat > "$conf" << CONFEOF
port 2000
network proactor
maxclients 10000
loglevel error
persistence none
threads 1
cpu_pin auto
rdma_sync off
CONFEOF

    killall -9 redis-benchmark 2>/dev/null || true
    killall -9 kvstore 2>/dev/null || true
    sleep 1
    rm -f "${PROJECT_DIR}/build/dump.rdb" "${PROJECT_DIR}/build/appendonly.aof"
    rm -f "${PROJECT_DIR}/build-glibc/dump.rdb" "${PROJECT_DIR}/build-glibc/appendonly.aof"

    local launch_cmd="nohup"
    if [ "$allocator" = "jemalloc" ] && [ "$JEMALLOC_TUNE" = "1" ]; then
        launch_cmd="nohup env ${JEMALLOC_TUNED_ENV}"
    fi
    $launch_cmd "$bin" "$conf" > /tmp/kvstore_cmp.log 2>&1 &
    local pid=$!
    sleep 2

    if ! kill -0 "$pid" 2>/dev/null; then
        err "启动失败: $allocator"
        tail -20 /tmp/kvstore_cmp.log
        rm -f "$conf"
        return 1
    fi

    wait_for_port 127.0.0.1 2000 15 || { err "端口就绪超时"; kill "$pid" 2>/dev/null; rm -f "$conf"; return 1; }
    echo "$pid"
}

stop_kvstore() {
    local pid=$1
    kill -9 "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    killall -9 redis-benchmark 2>/dev/null || true
    sleep 1
}

# ── 场景 B: 混合大小分配（碎片炸弹）──────────────────────────────────────

test_fragmentation_bomb() {
    local allocator=$1 bin=$2

    log "========================================"
    log "场景B: 混合大小分配（碎片炸弹）- ${allocator}"
    log "========================================"

    local pid
    pid=$(start_kvstore "$allocator" "$bin")
    [ -z "$pid" ] && return 1

    local idle_rss=$(sample_memory_avg "$pid" 5)
    log "  空闲 RSS: ${idle_rss} kB"

    # 写入 100K × 64B (key:000000000001 ~ key:000000100000)
    log "  写入 100K × 64B..."
    python3 "$GEN_RESP" 100000 64 $RB_KEY_FMT \
        | redis-cli -p 2000 --pipe >/dev/null 2>&1 || true
    sleep 1
    local count_64b=$(redis-cli -p 2000 DBSIZE 2>/dev/null || echo 0)
    log "  当前 key 数量: ${count_64b}"

    # 写入 100K × 4KB (key:000000200001 ~ key:000000300000)
    log "  写入 100K × 4KB (交错)..."
    python3 "$GEN_RESP" 100000 4096 $RB_KEY_FMT --offset 200000 \
        | redis-cli -p 2000 --pipe >/dev/null 2>&1 || true
    sleep 1
    local count_mixed=$(redis-cli -p 2000 DBSIZE 2>/dev/null || echo 0)
    log "  当前 key 数量: ${count_mixed}"

    local peak_rss=$(sample_memory_avg "$pid" 5)
    log "  峰值 RSS (混合): ${peak_rss} kB"

    # 删除所有 4KB 对象 (key:000000200001 ~ key:000000300000)
    log "  删除所有 4KB 对象..."
    local deleted=0

    # 使用正确的 RESP 格式批量删除
    for i in $(seq 200001 300000); do
        printf '*2\r\n$3\r\nDEL\r\n$16\r\nkey:%012d\r\n' $i
    done | redis-cli -p 2000 --pipe >/dev/null 2>&1 || true
    sleep 2

    local count_after_del=$(redis-cli -p 2000 DBSIZE 2>/dev/null || echo 0)
    deleted=$((count_mixed - count_after_del))
    log "  删除了 ${deleted} 个 key，剩余 ${count_after_del} 个"

    # 等待内存回收
    wait_memory_stable "$pid" 90
    local stable_rss=$(sample_memory_avg "$pid" 5)
    log "  稳定 RSS (删除后): ${stable_rss} kB"

    # 计算碎片率（基于删除后应该只剩 64B 对象）
    local theoretical=$((100000 * (KEY_LEN + 64 + KEY_OVERHEAD) / 1024))
    log "  理论内存 (100K × 64B): ${theoretical} kB"

    local frag=0
    local overhead=0
    if [ "$stable_rss" -gt "$theoretical" ] && [ "$stable_rss" -gt 0 ]; then
        frag=$(echo "scale=1; ($stable_rss - $theoretical) * 100 / $stable_rss" | bc -l 2>/dev/null || echo 0)
        overhead=$(echo "scale=1; $stable_rss / $theoretical" | bc -l 2>/dev/null || echo 0)
    fi
    log "  碎片率: ${frag}% (内存开销: ${overhead}x)"

    echo "fragmentation_bomb,${allocator},idle_rss,${idle_rss}" >> "$RESULT_CSV"
    echo "fragmentation_bomb,${allocator},peak_rss,${peak_rss}" >> "$RESULT_CSV"
    echo "fragmentation_bomb,${allocator},stable_rss,${stable_rss}" >> "$RESULT_CSV"
    echo "fragmentation_bomb,${allocator},theoretical_kb,${theoretical}" >> "$RESULT_CSV"
    echo "fragmentation_bomb,${allocator},fragmentation_pct,${frag}" >> "$RESULT_CSV"
    echo "fragmentation_bomb,${allocator},memory_overhead,${overhead}" >> "$RESULT_CSV"
    echo "fragmentation_bomb,${allocator},keys_deleted,${deleted}" >> "$RESULT_CSV"

    stop_kvstore "$pid"
}

# ── 场景 C: 内存回收时间曲线 ──────────────────────────────────────────────

test_reclaim_curve() {
    local allocator=$1 bin=$2

    log "========================================"
    log "场景C: 内存回收时间曲线 - ${allocator}"
    log "========================================"

    local pid
    pid=$(start_kvstore "$allocator" "$bin")
    [ -z "$pid" ] && return 1

    local idle_rss=$(sample_memory_avg "$pid" 3)
    log "  空闲 RSS: ${idle_rss} kB"

    # 写入 500K × 256B
    log "  写入 500K × 256B..."
    python3 "$GEN_RESP" 500000 256 $RB_KEY_FMT \
        | redis-cli -p 2000 --pipe >/dev/null 2>&1 || true
    sleep 1

    local peak_rss=$(sample_memory_avg "$pid" 5)
    log "  峰值 RSS: ${peak_rss} kB"

    # FLUSHALL
    redis-cli -p 2000 FLUSHALL >/dev/null 2>&1 || true
    log "  FLUSHALL 完成，开始采样回收曲线..."

    # 每 5 秒采样一次，持续 120 秒
    local t0=$(date +%s)
    for i in $(seq 0 5 120); do
        sleep 5
        if ! kill -0 "$pid" 2>/dev/null; then break; fi
        local curr_rss=$(sample_memory_avg "$pid" 3)
        local elapsed=$(($(date +%s) - t0))
        echo "reclaim_curve,${allocator},rss_at_${elapsed}s,${curr_rss}" >> "$RESULT_CSV"
        log "    ${elapsed}s: ${curr_rss} kB"
    done

    local final_rss=$(sample_memory_avg "$pid" 5)
    local reclaim_pct=0
    local delta=$((peak_rss - idle_rss))
    if [ "$delta" -gt 0 ]; then
        reclaim_pct=$(echo "scale=1; ($peak_rss - $final_rss) * 100 / $delta" | bc -l 2>/dev/null || echo 0)
    fi
    log "  最终回收率: ${reclaim_pct}%"

    echo "reclaim_curve,${allocator},peak_rss,${peak_rss}" >> "$RESULT_CSV"
    echo "reclaim_curve,${allocator},final_rss,${final_rss}" >> "$RESULT_CSV"
    echo "reclaim_curve,${allocator},reclaim_pct,${reclaim_pct}" >> "$RESULT_CSV"

    stop_kvstore "$pid"
}

# ── 场景 D: 长时间运行稳定性（10轮循环）──────────────────────────────────

test_long_running_stability() {
    local allocator=$1 bin=$2

    log "========================================"
    log "场景D: 长时间运行稳定性 - ${allocator}"
    log "========================================"

    local pid
    pid=$(start_kvstore "$allocator" "$bin")
    [ -z "$pid" ] && return 1

    local idle_rss=$(sample_memory_avg "$pid" 3)
    log "  空闲 RSS: ${idle_rss} kB"

    local first_stable=0
    for round in $(seq 1 10); do
        log "  === 第 ${round} 轮 ==="

        # 写入 200K × 256B
        python3 "$GEN_RESP" 200000 256 $RB_KEY_FMT \
            | redis-cli -p 2000 --pipe >/dev/null 2>&1 || true
        sleep 1

        local peak_rss=$(sample_memory_avg "$pid" 3)
        log "    峰值 RSS: ${peak_rss} kB"

        # FLUSHALL
        redis-cli -p 2000 FLUSHALL >/dev/null 2>&1 || true
        wait_memory_stable "$pid" 90

        local stable_rss=$(sample_memory_avg "$pid" 5)
        log "    稳定 RSS: ${stable_rss} kB"

        echo "long_running,${allocator},round_${round}_peak_rss,${peak_rss}" >> "$RESULT_CSV"
        echo "long_running,${allocator},round_${round}_stable_rss,${stable_rss}" >> "$RESULT_CSV"

        [ $round -eq 1 ] && first_stable=$stable_rss
    done

    local last_stable=$(grep "long_running,${allocator},round_10_stable_rss," "$RESULT_CSV" | cut -d, -f4)
    local bloat_ratio=1.0
    if [ "$first_stable" -gt 0 ]; then
        bloat_ratio=$(echo "scale=2; $last_stable / $first_stable" | bc -l 2>/dev/null || echo 1.0)
    fi
    local bloat_pct=$(echo "scale=1; ($bloat_ratio - 1) * 100" | bc -l 2>/dev/null || echo 0)
    log "  内存膨胀率: ${bloat_pct}% (第1轮: ${first_stable} kB, 第10轮: ${last_stable} kB)"

    echo "long_running,${allocator},first_stable,${first_stable}" >> "$RESULT_CSV"
    echo "long_running,${allocator},last_stable,${last_stable}" >> "$RESULT_CSV"
    echo "long_running,${allocator},bloat_pct,${bloat_pct}" >> "$RESULT_CSV"

    stop_kvstore "$pid"
}

# ── 场景 E: Churn 工作负载（流失模拟）─────────────────────────────────────

test_churn_workload() {
    local allocator=$1 bin=$2

    log "========================================"
    log "场景E: Churn 工作负载 - ${allocator}"
    log "========================================"

    local pid
    pid=$(start_kvstore "$allocator" "$bin")
    [ -z "$pid" ] && return 1

    local idle_rss=$(sample_memory_avg "$pid" 3)
    log "  空闲 RSS: ${idle_rss} kB"

    # 初始写入 200K keys
    log "  初始写入 200K keys..."
    python3 "$GEN_RESP" 200000 256 $RB_KEY_FMT \
        | redis-cli -p 2000 --pipe >/dev/null 2>&1 || true
    sleep 1

    local initial_peak=$(sample_memory_avg "$pid" 5)
    log "  初始峰值 RSS: ${initial_peak} kB"

    # 20 轮 Churn: 删除 50K + 写入 50K
    local next_key=200001
    for round in $(seq 1 20); do
        log "  === Churn 第 ${round} 轮 ==="

        # 删除 50K 随机 keys (简化：删除前 50K)
        local del_start=$(( (round - 1) * 50000 + 1 ))
        local del_end=$(( del_start + 49999 ))
        for i in $(seq $del_start 1000 $del_end); do
            local batch_end=$((i + 999))
            [ $batch_end -gt $del_end ] && batch_end=$del_end
            for j in $(seq $i $batch_end); do
                printf "DEL key:%012d\r\n" $j
            done | redis-cli -p 2000 --pipe >/dev/null 2>&1 || true
        done

        # 写入 50K 新 keys
        python3 "$GEN_RESP" 50000 256 $RB_KEY_FMT --offset $((next_key - 1)) \
            | redis-cli -p 2000 --pipe >/dev/null 2>&1 || true
        next_key=$((next_key + 50000))

        sleep 1
        local curr_rss=$(sample_memory_avg "$pid" 3)
        log "    当前 RSS: ${curr_rss} kB"

        echo "churn,${allocator},round_${round}_rss,${curr_rss}" >> "$RESULT_CSV"
    done

    local final_rss=$(sample_memory_avg "$pid" 5)
    local growth_pct=0
    if [ "$initial_peak" -gt 0 ]; then
        growth_pct=$(echo "scale=1; ($final_rss - $initial_peak) * 100 / $initial_peak" | bc -l 2>/dev/null || echo 0)
    fi
    log "  RSS 增长率: ${growth_pct}% (初始: ${initial_peak} kB, 最终: ${final_rss} kB)"

    echo "churn,${allocator},initial_peak,${initial_peak}" >> "$RESULT_CSV"
    echo "churn,${allocator},final_rss,${final_rss}" >> "$RESULT_CSV"
    echo "churn,${allocator},growth_pct,${growth_pct}" >> "$RESULT_CSV"

    stop_kvstore "$pid"
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

# ── 生成报告 ──────────────────────────────────────────────────────────────

generate_report() {
    local out="$RESULT_FILE"

    {
    echo ""
    echo "============================================================================="
    echo "  场景 B: 混合大小分配（碎片炸弹）"
    echo "============================================================================="
    echo ""
    printf "  %-12s | %12s | %12s | %12s | %12s | %10s | %10s\n" \
        "分配器" "空闲RSS" "峰值RSS" "稳定RSS" "理论内存" "碎片率" "内存开销"
    printf "  %-12s | %12s | %12s | %12s | %12s | %10s | %10s\n" \
        "------------" "------------" "------------" "------------" "------------" "----------" "----------"

    for alloc in "jemalloc" "glibc"; do
        local idle=$(grep "fragmentation_bomb,${alloc},idle_rss," "$RESULT_CSV" | cut -d, -f4)
        local peak=$(grep "fragmentation_bomb,${alloc},peak_rss," "$RESULT_CSV" | cut -d, -f4)
        local stable=$(grep "fragmentation_bomb,${alloc},stable_rss," "$RESULT_CSV" | cut -d, -f4)
        local theoretical=$(grep "fragmentation_bomb,${alloc},theoretical_kb," "$RESULT_CSV" | cut -d, -f4)
        local frag=$(grep "fragmentation_bomb,${alloc},fragmentation_pct," "$RESULT_CSV" | cut -d, -f4)
        local overhead=$(grep "fragmentation_bomb,${alloc},memory_overhead," "$RESULT_CSV" | cut -d, -f4)
        local deleted=$(grep "fragmentation_bomb,${alloc},keys_deleted," "$RESULT_CSV" | cut -d, -f4)
        printf "  %-12s | %12s | %12s | %12s | %12s | %9s%% | %9sx\n" \
            "$alloc" "$idle" "$peak" "$stable" "$theoretical" "$frag" "$overhead"
        echo "    (删除了 ${deleted} 个 4KB key)"
    done

    echo ""
    echo "关键发现："
    local je_frag=$(grep "fragmentation_bomb,jemalloc,fragmentation_pct," "$RESULT_CSV" | cut -d, -f4)
    local gl_frag=$(grep "fragmentation_bomb,glibc,fragmentation_pct," "$RESULT_CSV" | cut -d, -f4)
    local je_overhead=$(grep "fragmentation_bomb,jemalloc,memory_overhead," "$RESULT_CSV" | cut -d, -f4)
    local gl_overhead=$(grep "fragmentation_bomb,glibc,memory_overhead," "$RESULT_CSV" | cut -d, -f4)

    local winner=""
    if [ "$(echo "$je_frag < $gl_frag" | bc -l 2>/dev/null)" = "1" ]; then
        winner="jemalloc"
        echo "  ✓ jemalloc 碎片率更低: ${je_frag}% vs ${gl_frag}%"
        echo "  ✓ jemalloc 内存开销: ${je_overhead}x vs glibc ${gl_overhead}x"
    else
        winner="glibc"
        echo "  ✓ glibc 碎片率更低: ${gl_frag}% vs ${je_frag}%"
        echo "  ✓ glibc 内存开销: ${gl_overhead}x vs jemalloc ${je_overhead}x"
    fi

    echo ""
    echo "============================================================================="
    echo "  场景 C: 内存回收时间曲线"
    echo "============================================================================="
    echo ""

    for alloc in "jemalloc" "glibc"; do
        echo "  ${alloc}:"
        local peak=$(grep "reclaim_curve,${alloc},peak_rss," "$RESULT_CSV" | cut -d, -f4)
        local final=$(grep "reclaim_curve,${alloc},final_rss," "$RESULT_CSV" | cut -d, -f4)
        local reclaim=$(grep "reclaim_curve,${alloc},reclaim_pct," "$RESULT_CSV" | cut -d, -f4)
        local reclaimed_kb=$((peak - final))
        echo "    峰值 RSS: ${peak} kB"
        echo "    最终 RSS: ${final} kB"
        echo "    回收量: ${reclaimed_kb} kB"
        echo "    回收率: ${reclaim}%"
        echo ""
    done

    local je_reclaim=$(grep "reclaim_curve,jemalloc,reclaim_pct," "$RESULT_CSV" | cut -d, -f4)
    local gl_reclaim=$(grep "reclaim_curve,glibc,reclaim_pct," "$RESULT_CSV" | cut -d, -f4)
    if [ "$(echo "$je_reclaim > $gl_reclaim" | bc -l 2>/dev/null)" = "1" ]; then
        echo "关键发现："
        echo "  ✓ jemalloc 回收率更高: ${je_reclaim}% vs ${gl_reclaim}%"
    else
        echo "关键发现："
        echo "  ✗ glibc 回收率更高: ${gl_reclaim}% vs ${je_reclaim}%"
        echo "  ⚠ jemalloc 可能需要更长的 decay 时间或手动触发"
    fi

    echo ""
    echo "============================================================================="
    echo "  场景 D: 长时间运行稳定性（10轮循环）"
    echo "============================================================================="
    echo ""
    printf "  %-12s | %15s | %15s | %12s\n" \
        "分配器" "第1轮稳定RSS" "第10轮稳定RSS" "膨胀率"
    printf "  %-12s | %15s | %15s | %12s\n" \
        "------------" "---------------" "---------------" "------------"

    for alloc in "jemalloc" "glibc"; do
        local r1=$(grep "long_running,${alloc},first_stable," "$RESULT_CSV" | cut -d, -f4)
        local r10=$(grep "long_running,${alloc},last_stable," "$RESULT_CSV" | cut -d, -f4)
        local bloat=$(grep "long_running,${alloc},bloat_pct," "$RESULT_CSV" | cut -d, -f4)
        printf "  %-12s | %15s | %15s | %11s%%\n" \
            "$alloc" "$r1" "$r10" "$bloat"
    done

    echo ""
    echo "关键发现："
    local je_bloat=$(grep "long_running,jemalloc,bloat_pct," "$RESULT_CSV" | cut -d, -f4)
    local gl_bloat=$(grep "long_running,glibc,bloat_pct," "$RESULT_CSV" | cut -d, -f4)
    if [ "$(echo "$je_bloat < $gl_bloat" | bc -l 2>/dev/null)" = "1" ]; then
        echo "  ✓ jemalloc 膨胀率更低: ${je_bloat}% vs ${gl_bloat}%"
    else
        echo "  ✗ glibc 膨胀率更低: ${gl_bloat}% vs ${je_bloat}%"
    fi

    echo ""
    echo "============================================================================="
    echo "  场景 E: Churn 工作负载（流失模拟）"
    echo "============================================================================="
    echo ""
    printf "  %-12s | %15s | %15s | %12s\n" \
        "分配器" "初始峰值RSS" "最终RSS" "增长率"
    printf "  %-12s | %15s | %15s | %12s\n" \
        "------------" "---------------" "---------------" "------------"

    for alloc in "jemalloc" "glibc"; do
        local init=$(grep "churn,${alloc},initial_peak," "$RESULT_CSV" | cut -d, -f4)
        local final=$(grep "churn,${alloc},final_rss," "$RESULT_CSV" | cut -d, -f4)
        local growth=$(grep "churn,${alloc},growth_pct," "$RESULT_CSV" | cut -d, -f4)
        printf "  %-12s | %15s | %15s | %11s%%\n" \
            "$alloc" "$init" "$final" "$growth"
    done

    echo ""
    echo "关键发现："
    local je_growth=$(grep "churn,jemalloc,growth_pct," "$RESULT_CSV" | cut -d, -f4)
    local gl_growth=$(grep "churn,glibc,growth_pct," "$RESULT_CSV" | cut -d, -f4)
    if [ "$(echo "$je_growth < $gl_growth" | bc -l 2>/dev/null)" = "1" ]; then
        echo "  ✓ jemalloc 增长率更低: ${je_growth}% vs ${gl_growth}%"
        echo "  ✓ 在频繁增删场景下，jemalloc 内存控制更好"
    else
        echo "  ✗ glibc 增长率更低: ${gl_growth}% vs ${je_growth}%"
    fi

    echo ""
    echo "============================================================================="
    echo "  综合结论"
    echo "============================================================================="
    echo ""

    # 统计各场景胜负
    local je_wins=0
    local gl_wins=0

    # 场景B
    if [ "$(echo "$je_frag < $gl_frag" | bc -l 2>/dev/null)" = "1" ]; then
        je_wins=$((je_wins + 1))
    else
        gl_wins=$((gl_wins + 1))
    fi

    # 场景C
    if [ "$(echo "$je_reclaim > $gl_reclaim" | bc -l 2>/dev/null)" = "1" ]; then
        je_wins=$((je_wins + 1))
    else
        gl_wins=$((gl_wins + 1))
    fi

    # 场景D
    if [ "$(echo "$je_bloat < $gl_bloat" | bc -l 2>/dev/null)" = "1" ]; then
        je_wins=$((je_wins + 1))
    else
        gl_wins=$((gl_wins + 1))
    fi

    # 场景E
    if [ "$(echo "$je_growth < $gl_growth" | bc -l 2>/dev/null)" = "1" ]; then
        je_wins=$((je_wins + 1))
    else
        gl_wins=$((gl_wins + 1))
    fi

    echo "  测试配置："
    echo "    - 线程数: 1 (单线程)"
    echo "    - jemalloc 调优: $([ "$JEMALLOC_TUNE" = "1" ] && echo 'enabled (dirty_decay_ms:1000)' || echo 'disabled')"
    echo ""
    echo "  场景胜负统计："
    echo "    jemalloc 胜出: ${je_wins}/4 场景"
    echo "    glibc 胜出: ${gl_wins}/4 场景"
    echo ""

    if [ $je_wins -gt $gl_wins ]; then
        echo "  总体评价："
        echo "    ✓ jemalloc 在多数场景下表现更优"
        echo "    ✓ 推荐用于长时间运行、内存敏感的服务"
    elif [ $gl_wins -gt $je_wins ]; then
        echo "  总体评价："
        echo "    ✓ glibc 在多数场景下表现更优"
        echo "    ⚠ 可能需要调整 jemalloc 配置参数"
    else
        echo "  总体评价："
        echo "    ≈ 两者表现相当，各有优劣"
        echo "    建议根据具体场景选择"
    fi

    echo ""
    echo "============================================================================="
    } >> "$out"
}

# ── 主流程 ────────────────────────────────────────────────────────────────

init_output

ensure_build "jemalloc" "$KVS_JEMALLOC" "yes"
ensure_build "glibc"   "$KVS_GLIBC"   "no"

# 构建分配器列表
if [ "$JEMALLOC_TUNE" = "1" ]; then
    ALLOCATORS=("jemalloc:${KVS_JEMALLOC}" "glibc:${KVS_GLIBC}")
    log "jemalloc 调优已启用: dirty_decay_ms=1000, muzzy_decay_ms=1000, background_thread=true"
else
    ALLOCATORS=("jemalloc:${KVS_JEMALLOC}" "glibc:${KVS_GLIBC}")
    log "jemalloc 使用默认配置"
fi

# 场景 B: 混合大小分配
for entry in "${ALLOCATORS[@]}"; do
    alloc=$(echo "$entry" | cut -d: -f1)
    bin=$(echo "$entry"   | cut -d: -f2)
    test_fragmentation_bomb "$alloc" "$bin" || warn "场景B失败: ${alloc}"
done

# 场景 C: 内存回收曲线
for entry in "${ALLOCATORS[@]}"; do
    alloc=$(echo "$entry" | cut -d: -f1)
    bin=$(echo "$entry"   | cut -d: -f2)
    test_reclaim_curve "$alloc" "$bin" || warn "场景C失败: ${alloc}"
done

# 场景 D: 长时间运行稳定性
for entry in "${ALLOCATORS[@]}"; do
    alloc=$(echo "$entry" | cut -d: -f1)
    bin=$(echo "$entry"   | cut -d: -f2)
    test_long_running_stability "$alloc" "$bin" || warn "场景D失败: ${alloc}"
done

# 场景 E: Churn 工作负载
for entry in "${ALLOCATORS[@]}"; do
    alloc=$(echo "$entry" | cut -d: -f1)
    bin=$(echo "$entry"   | cut -d: -f2)
    test_churn_workload "$alloc" "$bin" || warn "场景E失败: ${alloc}"
done

generate_report

echo ""
log "测试完成！结果已保存："
log "  文本报告: $RESULT_FILE"
log "  CSV数据:  $RESULT_CSV"
echo ""
cat "$RESULT_FILE"
