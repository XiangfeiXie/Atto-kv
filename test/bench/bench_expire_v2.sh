#!/bin/bash
# bench_expire_v2.sh — 过期键性能测试（优化版）
#
# 改进点:
#   1. 使用 redis-benchmark 批量写入（替代循环）
#   2. 增加过期延迟精确测量
#   3. 增加多线程场景
#   4. 增加并发读写压力测试

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

set +e
set +o pipefail

require_tools

# 测试参数
VALUE_SIZE=64

KVS_BIN="${PROJECT_DIR}/build/kvstore"
RESULT_FILE="${PROJECT_DIR}/test/bench/results_expire_v2.txt"
RESULT_CSV="${PROJECT_DIR}/test/bench/results_expire_v2.csv"

# 初始化输出
init_output() {
    mkdir -p "$(dirname "$RESULT_FILE")"

    cat > "$RESULT_FILE" << 'HEADER'
=============================================================================
过期键性能测试 V2 — 优化版
=============================================================================
HEADER
    echo "测试时间: $(date '+%Y-%m-%d %H:%M:%S')" >> "$RESULT_FILE"
    echo "" >> "$RESULT_FILE"

    echo "scenario,threads,keys,ttl_sec,write_duration_s,expired_before,expired_after,deleted_count,cleanup_duration_s,vmrss_before_kb,vmrss_after_kb,reclaim_kb,reclaim_pct,qps_baseline,qps_during,qps_impact_pct" > "$RESULT_CSV"
}

# 获取 VmRSS
get_vmrss() {
    local pid=$1
    grep "^VmRSS:" "/proc/$pid/status" 2>/dev/null | awk '{print $2}' || echo 0
}

# 获取 expired_keys
get_expired_keys() {
    redis-cli -p 2000 INFO Stats 2>/dev/null | grep "^expired_keys:" | cut -d: -f2 | tr -d '\r' || echo 0
}

# 启动 kvstore
start_kvs() {
    local threads=$1
    local conf="/tmp/kvstore_expire_t${threads}.conf"

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

    pkill -9 kvstore 2>/dev/null || true
    sleep 1

    nohup "$KVS_BIN" "$conf" > /tmp/kvstore_expire.log 2>&1 &
    sleep 2

    if ! redis-cli -p 2000 PING >/dev/null 2>&1; then
        err "kvstore 启动失败"
        tail -20 /tmp/kvstore_expire.log
        return 1
    fi

    local pid=$(pgrep -f kvstore | head -1)
    echo "$pid"
}

# 场景1: 突发过期测试
test_burst_expire() {
    local threads=$1
    local nkeys=$2
    local ttl=$3

    log "========================================"
    log "突发过期: threads=${threads}, keys=${nkeys}, TTL=${ttl}s"
    log "========================================"

    local pid=$(start_kvs "$threads")
    if [ -z "$pid" ]; then
        err "启动失败"
        return 1
    fi

    redis-cli -p 2000 FLUSHALL >/dev/null 2>&1
    sleep 1

    # 基线 QPS
    local qps_baseline=$(redis-benchmark -p 2000 -t get -n 10000 -q 2>/dev/null | grep "GET:" | awk '{print $2}')

    # 批量写入带 TTL 的 key
    log "  写入 ${nkeys} keys (TTL=${ttl}s)..."
    local t0=$(date +%s)
    python3 "${PROJECT_DIR}/test/bench/gen_expire_data.py" "$nkeys" "$VALUE_SIZE" "$ttl" --redis-benchmark-keys \
        | redis-cli -p 2000 --pipe >/dev/null 2>&1
    local t1=$(date +%s)
    local write_duration=$((t1 - t0))

    local dbsize=$(redis-cli -p 2000 DBSIZE 2>/dev/null)
    log "  写入完成: DBSIZE=${dbsize}, 耗时=${write_duration}s"

    sleep 1
    local vmrss_before=$(get_vmrss "$pid")
    local expired_before=$(get_expired_keys)

    # 等待过期
    log "  等待过期 (${ttl}s)..."
    sleep "$ttl"

    # 监控主动删除
    log "  监控主动删除..."
    local cleanup_start=$(date +%s)
    local max_wait=60
    for i in $(seq 1 $max_wait); do
        sleep 1
        local dbsize_now=$(redis-cli -p 2000 DBSIZE 2>/dev/null)
        local expired_now=$(get_expired_keys)
        local deleted=$((expired_now - expired_before))
        local pct=0
        [ "$nkeys" -gt 0 ] && pct=$((deleted * 100 / nkeys))

        if [ $((i % 5)) -eq 0 ] || [ "$dbsize_now" -eq 0 ]; then
            log "    [${i}s] DBSIZE=${dbsize_now}, deleted=${deleted}/${nkeys} (${pct}%)"
        fi

        if [ "$dbsize_now" -eq 0 ]; then
            log "  所有 key 已删除，耗时 ${i}s"
            break
        fi
    done
    local cleanup_end=$(date +%s)
    local cleanup_duration=$((cleanup_end - cleanup_start))

    sleep 2
    local vmrss_after=$(get_vmrss "$pid")
    local expired_after=$(get_expired_keys)
    local deleted_count=$((expired_after - expired_before))

    # 过期后 QPS
    local qps_during=$(redis-benchmark -p 2000 -t get -n 10000 -q 2>/dev/null | grep "GET:" | awk '{print $2}')

    local reclaim_kb=$((vmrss_before - vmrss_after))
    local reclaim_pct=0
    [ "$vmrss_before" -gt 0 ] && reclaim_pct=$(echo "scale=1; $reclaim_kb * 100 / $vmrss_before" | bc -l 2>/dev/null || echo 0)

    local qps_impact_pct=0
    if [ -n "$qps_baseline" ] && [ -n "$qps_during" ]; then
        qps_impact_pct=$(echo "scale=1; ($qps_baseline - $qps_during) * 100 / $qps_baseline" | bc -l 2>/dev/null || echo 0)
    fi

    log "  结果: 删除=${deleted_count}, 清理耗时=${cleanup_duration}s, 内存回收=${reclaim_kb}kB (${reclaim_pct}%), QPS影响=${qps_impact_pct}%"

    echo "burst,${threads},${nkeys},${ttl},${write_duration},${expired_before},${expired_after},${deleted_count},${cleanup_duration},${vmrss_before},${vmrss_after},${reclaim_kb},${reclaim_pct},${qps_baseline},${qps_during},${qps_impact_pct}" >> "$RESULT_CSV"

    pkill -9 kvstore 2>/dev/null || true
    sleep 1
}

# 场景2: 混合负载测试
test_mixed_workload() {
    local threads=$1
    local nkeys=$2
    local ttl=$3

    log "========================================"
    log "混合负载: threads=${threads}, keys=${nkeys}, TTL=${ttl}s, 后台读写"
    log "========================================"

    local pid=$(start_kvs "$threads")
    if [ -z "$pid" ]; then
        err "启动失败"
        return 1
    fi

    redis-cli -p 2000 FLUSHALL >/dev/null 2>&1
    sleep 1

    # 基线 QPS
    local qps_baseline=$(redis-benchmark -p 2000 -t get,set -n 10000 -q 2>/dev/null | tail -1 | awk '{print $2}')

    # 写入带 TTL 的 key
    log "  写入 ${nkeys} keys (TTL=${ttl}s)..."
    local t0=$(date +%s)
    python3 "${PROJECT_DIR}/test/bench/gen_expire_data.py" "$nkeys" "$VALUE_SIZE" "$ttl" --redis-benchmark-keys \
        | redis-cli -p 2000 --pipe >/dev/null 2>&1
    local t1=$(date +%s)
    local write_duration=$((t1 - t0))

    sleep 1
    local vmrss_before=$(get_vmrss "$pid")
    local expired_before=$(get_expired_keys)

    # 启动后台读写负载 - 只操作已有 key 范围，不创建新 key
    log "  启动后台读写负载（只操作已有 key）..."
    redis-benchmark -p 2000 -t get,set -n 500000 -r "$nkeys" -d "$VALUE_SIZE" -q > /tmp/bench_mixed.log 2>&1 &
    local bench_pid=$!

    # 等待过期
    log "  等待过期 (${ttl}s)..."
    sleep "$ttl"

    # 监控主动删除
    log "  监控主动删除..."
    local cleanup_start=$(date +%s)
    local max_wait=60
    for i in $(seq 1 $max_wait); do
        sleep 1
        local dbsize_now=$(redis-cli -p 2000 DBSIZE 2>/dev/null)
        local expired_now=$(get_expired_keys)
        local deleted=$((expired_now - expired_before))

        if [ $((i % 5)) -eq 0 ] || [ "$dbsize_now" -le 10000 ]; then
            log "    [${i}s] DBSIZE=${dbsize_now}, deleted=${deleted}/${nkeys}"
        fi

        if [ "$dbsize_now" -le 10000 ]; then
            log "  过期 key 已基本清理完毕，耗时 ${i}s"
            break
        fi
    done
    local cleanup_end=$(date +%s)
    local cleanup_duration=$((cleanup_end - cleanup_start))

    # 停止后台负载
    kill "$bench_pid" 2>/dev/null || true
    wait "$bench_pid" 2>/dev/null || true

    sleep 2
    local vmrss_after=$(get_vmrss "$pid")
    local expired_after=$(get_expired_keys)
    local deleted_count=$((expired_after - expired_before))

    # 过期后 QPS
    local qps_during=$(redis-benchmark -p 2000 -t get,set -n 10000 -q 2>/dev/null | tail -1 | awk '{print $2}')

    local reclaim_kb=$((vmrss_before - vmrss_after))
    local reclaim_pct=0
    [ "$vmrss_before" -gt 0 ] && reclaim_pct=$(echo "scale=1; $reclaim_kb * 100 / $vmrss_before" | bc -l 2>/dev/null || echo 0)

    local qps_impact_pct=0
    if [ -n "$qps_baseline" ] && [ -n "$qps_during" ]; then
        qps_impact_pct=$(echo "scale=1; ($qps_baseline - $qps_during) * 100 / $qps_baseline" | bc -l 2>/dev/null || echo 0)
    fi

    log "  结果: 删除=${deleted_count}, 清理耗时=${cleanup_duration}s, 内存回收=${reclaim_kb}kB (${reclaim_pct}%), QPS影响=${qps_impact_pct}%"

    echo "mixed,${threads},${nkeys},${ttl},${write_duration},${expired_before},${expired_after},${deleted_count},${cleanup_duration},${vmrss_before},${vmrss_after},${reclaim_kb},${reclaim_pct},${qps_baseline},${qps_during},${qps_impact_pct}" >> "$RESULT_CSV"

    pkill -9 kvstore 2>/dev/null || true
    sleep 1
}

# 主流程
init_output

log "开始过期键性能测试 V2"
echo ""

# 突发过期测试 - 不同规模
test_burst_expire 1 10000 5
test_burst_expire 1 50000 5
test_burst_expire 1 100000 5

# 多线程突发过期
test_burst_expire 4 100000 5

# 混合负载测试
test_mixed_workload 1 50000 5
test_mixed_workload 4 100000 5

echo ""
log "测试完成！结果已保存："
log "  文本报告: $RESULT_FILE"
log "  CSV数据:  $RESULT_CSV"
echo ""
