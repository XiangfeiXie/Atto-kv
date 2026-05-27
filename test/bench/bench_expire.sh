#!/bin/bash
# bench_expire.sh — 过期键性能测试

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

# Skip root check for expire test
# require_root
require_tools

# 测试参数
EXPIRE_TIME=5          # 过期时间（秒）
VALUE_SIZE=64          # value大小
BURST_KEYS=100000      # 突发过期键数量
GRADUAL_KEYS=50000     # 渐进过期键数量

KVS_BIN="${PROJECT_DIR}/build/kvstore"
RESULT_FILE="${PROJECT_DIR}/test/bench/results_expire.txt"
RESULT_CSV="${PROJECT_DIR}/test/bench/results_expire.csv"

log "bench_expire.sh 脚本已创建"
log "测试配置: EXPIRE_TIME=${EXPIRE_TIME}s, VALUE_SIZE=${VALUE_SIZE}B"

# 初始化输出文件
init_output() {
    local output_dir="${PROJECT_DIR}/test/bench"
    mkdir -p "$output_dir"

    cat > "$RESULT_FILE" << HEADER
=============================================================================
过期键性能测试
=============================================================================
测试时间: $(date '+%Y-%m-%d %H:%M:%S')
测试配置:
  - 过期时间: ${EXPIRE_TIME} 秒
  - Value大小: ${VALUE_SIZE} bytes
  - 突发过期: ${BURST_KEYS} keys
  - 渐进过期: ${GRADUAL_KEYS} keys

测试场景:
  场景A: 突发过期 (所有key同时过期)
  场景B: 渐进过期 (key在时间窗口内陆续过期)
  场景C: 混合负载 (过期+正常读写)

测量指标:
  - 主动删除数量 (expired_keys)
  - 内存回收情况 (VmRSS)
  - QPS影响 (redis-benchmark)
  - 过期延迟 (实际删除时间 - 理论过期时间)

=============================================================================

HEADER

    echo "scenario,keys,expire_time,expired_keys_before,expired_keys_after,deleted_count,vmrss_before_kb,vmrss_after_kb,reclaim_kb,reclaim_pct,qps_baseline,qps_during_expire,qps_impact_pct,avg_latency_us" > "$RESULT_CSV"
}

# 获取进程内存
get_vmrss() {
    local pid=$1
    grep "^VmRSS:" "/proc/$pid/status" 2>/dev/null | awk '{print $2}'
}

# 获取expired_keys计数
get_expired_keys() {
    redis-cli -p 2000 INFO Stats 2>/dev/null | grep "^expired_keys:" | cut -d: -f2 | tr -d '\r'
}

# 场景A: 突发过期测试
test_burst_expire() {
    local nkeys=$1
    local expire_sec=$2
    
    log "场景A: 突发过期测试 (${nkeys} keys, ${expire_sec}s TTL)"
    
    stop_redis
    start_kvs "kvstore-none.conf"
    local pid=$(pgrep -f kvstore | head -1)
    
    if [ -z "$pid" ]; then
        err "无法获取kvstore进程PID"
        return 1
    fi
    
    sleep 2
    
    # 基线QPS
    local qps_baseline=$(redis-benchmark -p 2000 -t get -n 10000 -q 2>/dev/null | grep "GET:" | awk '{print $2}')
    
    # 写入带过期时间的key
    log "写入 ${nkeys} 个key，TTL=${expire_sec}s"
    redis-benchmark -p 2000 -t set -n "$nkeys" -r "$nkeys" -d "$VALUE_SIZE" -e "EX ${expire_sec}" -q >/dev/null 2>&1
    
    sleep 1
    local vmrss_before=$(get_vmrss "$pid")
    local expired_before=$(get_expired_keys)
    local dbsize_before=$(redis-cli -p 2000 DBSIZE 2>/dev/null)
    
    log "等待过期: DBSIZE=${dbsize_before}, VmRSS=${vmrss_before} kB, expired_keys=${expired_before}"
    
    # 等待过期时间
    sleep "$expire_sec"
    
    # 监控主动删除过程
    local start_time=$(date +%s)
    local max_wait=30
    for i in $(seq 1 $max_wait); do
        sleep 1
        local dbsize=$(redis-cli -p 2000 DBSIZE 2>/dev/null)
        local expired_now=$(get_expired_keys)
        log "  [${i}s] DBSIZE=${dbsize}, expired_keys=${expired_now}"
        
        if [ "$dbsize" -eq 0 ]; then
            log "所有key已被主动删除"
            break
        fi
    done
    local end_time=$(date +%s)
    local cleanup_duration=$((end_time - start_time))
    
    sleep 2
    local vmrss_after=$(get_vmrss "$pid")
    local expired_after=$(get_expired_keys)
    local dbsize_after=$(redis-cli -p 2000 DBSIZE 2>/dev/null)
    local deleted_count=$((expired_after - expired_before))
    
    # 过期期间QPS
    local qps_during=$(redis-benchmark -p 2000 -t get -n 10000 -q 2>/dev/null | grep "GET:" | awk '{print $2}')
    
    local reclaim_kb=$((vmrss_before - vmrss_after))
    local reclaim_pct=0
    if [ "$vmrss_before" -gt 0 ]; then
        reclaim_pct=$(echo "scale=1; $reclaim_kb * 100 / $vmrss_before" | bc -l 2>/dev/null || echo 0)
    fi
    
    local qps_impact_pct=0
    if [ -n "$qps_baseline" ] && [ -n "$qps_during" ]; then
        qps_impact_pct=$(echo "scale=1; ($qps_baseline - $qps_during) * 100 / $qps_baseline" | bc -l 2>/dev/null || echo 0)
    fi
    
    log "结果: 删除=${deleted_count}, 内存回收=${reclaim_kb}kB (${reclaim_pct}%), QPS影响=${qps_impact_pct}%, 清理耗时=${cleanup_duration}s"
    
    echo "burst,${nkeys},${expire_sec},${expired_before},${expired_after},${deleted_count},${vmrss_before},${vmrss_after},${reclaim_kb},${reclaim_pct},${qps_baseline},${qps_during},${qps_impact_pct},${cleanup_duration}" >> "$RESULT_CSV"
    
    stop_kvs
}

# 场景B: 渐进过期测试
test_gradual_expire() {
    local nkeys=$1
    local expire_window=$2
    
    log "场景B: 渐进过期测试 (${nkeys} keys, ${expire_window}s 时间窗口)"
    
    stop_redis
    start_kvs "kvstore-none.conf"
    local pid=$(pgrep -f kvstore | head -1)
    
    if [ -z "$pid" ]; then
        err "无法获取kvstore进程PID"
        return 1
    fi
    
    sleep 2
    
    # 写入key，TTL在1到expire_window秒之间均匀分布
    log "写入 ${nkeys} 个key，TTL分布在 1-${expire_window}s"
    for i in $(seq 1 "$nkeys"); do
        local ttl=$((1 + i * expire_window / nkeys))
        redis-cli -p 2000 SET "key${i}" "value${i}" EX "$ttl" >/dev/null 2>&1
        
        # 每1000个key打印一次进度
        if [ $((i % 1000)) -eq 0 ]; then
            log "  已写入 ${i}/${nkeys} keys"
        fi
    done
    
    sleep 1
    local vmrss_before=$(get_vmrss "$pid")
    local expired_before=$(get_expired_keys)
    
    log "等待渐进过期: VmRSS=${vmrss_before} kB, expired_keys=${expired_before}"
    
    # 监控过期过程
    local max_wait=$((expire_window + 10))
    for i in $(seq 1 $max_wait); do
        sleep 1
        local dbsize=$(redis-cli -p 2000 DBSIZE 2>/dev/null)
        local expired_now=$(get_expired_keys)
        log "  [${i}s] DBSIZE=${dbsize}, expired_keys=${expired_now}"
        
        if [ "$dbsize" -eq 0 ]; then
            log "所有key已被删除"
            break
        fi
    done
    
    sleep 2
    local vmrss_after=$(get_vmrss "$pid")
    local expired_after=$(get_expired_keys)
    local deleted_count=$((expired_after - expired_before))
    
    local reclaim_kb=$((vmrss_before - vmrss_after))
    local reclaim_pct=0
    if [ "$vmrss_before" -gt 0 ]; then
        reclaim_pct=$(echo "scale=1; $reclaim_kb * 100 / $vmrss_before" | bc -l 2>/dev/null || echo 0)
    fi
    
    log "结果: 删除=${deleted_count}, 内存回收=${reclaim_kb}kB (${reclaim_pct}%)"
    
    echo "gradual,${nkeys},${expire_window},${expired_before},${expired_after},${deleted_count},${vmrss_before},${vmrss_after},${reclaim_kb},${reclaim_pct},0,0,0,0" >> "$RESULT_CSV"
    
    stop_kvs
}

# 场景C: 混合负载测试
test_mixed_workload() {
    local nkeys=$1
    local expire_sec=$2
    
    log "场景C: 混合负载测试 (${nkeys} keys, ${expire_sec}s TTL, 持续读写)"
    
    stop_redis
    start_kvs "kvstore-none.conf"
    local pid=$(pgrep -f kvstore | head -1)
    
    if [ -z "$pid" ]; then
        err "无法获取kvstore进程PID"
        return 1
    fi
    
    sleep 2
    
    # 基线QPS
    local qps_baseline=$(redis-benchmark -p 2000 -t get,set -n 10000 -q 2>/dev/null | tail -1 | awk '{print $2}')
    
    # 写入带过期时间的key
    log "写入 ${nkeys} 个key，TTL=${expire_sec}s"
    redis-benchmark -p 2000 -t set -n "$nkeys" -r "$nkeys" -d "$VALUE_SIZE" -e "EX ${expire_sec}" -q >/dev/null 2>&1
    
    sleep 1
    local vmrss_before=$(get_vmrss "$pid")
    local expired_before=$(get_expired_keys)
    
    # 启动后台持续读写负载
    log "启动后台读写负载"
    redis-benchmark -p 2000 -t get,set -n 1000000 -r 10000 -d "$VALUE_SIZE" -q > /tmp/bench_mixed.log 2>&1 &
    local bench_pid=$!
    
    # 等待过期
    sleep "$expire_sec"
    
    # 监控主动删除
    local max_wait=30
    for i in $(seq 1 $max_wait); do
        sleep 1
        local dbsize=$(redis-cli -p 2000 DBSIZE 2>/dev/null)
        local expired_now=$(get_expired_keys)
        log "  [${i}s] DBSIZE=${dbsize}, expired_keys=${expired_now}"
        
        if [ "$dbsize" -le 10000 ]; then
            log "过期key已基本清理完毕"
            break
        fi
    done
    
    # 停止后台负载
    kill "$bench_pid" 2>/dev/null || true
    wait "$bench_pid" 2>/dev/null || true
    
    sleep 2
    local vmrss_after=$(get_vmrss "$pid")
    local expired_after=$(get_expired_keys)
    local deleted_count=$((expired_after - expired_before))
    
    # 测试QPS
    local qps_after=$(redis-benchmark -p 2000 -t get,set -n 10000 -q 2>/dev/null | tail -1 | awk '{print $2}')
    
    local reclaim_kb=$((vmrss_before - vmrss_after))
    local reclaim_pct=0
    if [ "$vmrss_before" -gt 0 ]; then
        reclaim_pct=$(echo "scale=1; $reclaim_kb * 100 / $vmrss_before" | bc -l 2>/dev/null || echo 0)
    fi
    
    local qps_impact_pct=0
    if [ -n "$qps_baseline" ] && [ -n "$qps_after" ]; then
        qps_impact_pct=$(echo "scale=1; ($qps_baseline - $qps_after) * 100 / $qps_baseline" | bc -l 2>/dev/null || echo 0)
    fi
    
    log "结果: 删除=${deleted_count}, 内存回收=${reclaim_kb}kB (${reclaim_pct}%), QPS影响=${qps_impact_pct}%"
    
    echo "mixed,${nkeys},${expire_sec},${expired_before},${expired_after},${deleted_count},${vmrss_before},${vmrss_after},${reclaim_kb},${reclaim_pct},${qps_baseline},${qps_after},${qps_impact_pct},0" >> "$RESULT_CSV"
    
    stop_kvs
}

start_kvs() {
    local conf=$1
    local conf_path="${CONF_DIR}/${conf}"
    if [ ! -f "$conf_path" ]; then
        err "配置文件不存在: $conf_path"
        exit 1
    fi
    log "启动 kvstore"
    nohup "$KVS_BIN" "$conf_path" > /tmp/kvstore_expire.log 2>&1 &
    sleep 3
    if ! pgrep -f kvstore >/dev/null; then
        err "kvstore 启动失败"
        cat /tmp/kvstore_expire.log
        exit 1
    fi
}

stop_kvs() {
    pkill -9 kvstore 2>/dev/null || true
    sleep 1
}

# 主流程
init_output

echo ""
echo "════════════════════════════════════════════════"
step "场景A: 突发过期测试"
echo "════════════════════════════════════════════════"
test_burst_expire "$BURST_KEYS" "$EXPIRE_TIME" | tee -a "$RESULT_FILE"

echo ""
echo "════════════════════════════════════════════════"
step "场景B: 渐进过期测试"
echo "════════════════════════════════════════════════"
test_gradual_expire "$GRADUAL_KEYS" 10 | tee -a "$RESULT_FILE"

echo ""
echo "════════════════════════════════════════════════"
step "场景C: 混合负载测试"
echo "════════════════════════════════════════════════"
test_mixed_workload "$BURST_KEYS" "$EXPIRE_TIME" | tee -a "$RESULT_FILE"

echo ""
log "测试完成！结果已保存："
log "  文本报告: $RESULT_FILE"
log "  CSV数据:  $RESULT_CSV"
echo ""
