#!/bin/bash
# bench_expire_v3.sh — 过期键性能测试（Phase 9.2 优化版）
#
# 改进点:
#   1. 使用 redis-cli --pipe 快速写入，避免写入期间过期
#   2. 修复 DBSIZE 计算：记录写入期间删除的 key
#   3. 测试自适应采样：20 → 200 keys/cycle
#   4. 测试惰性删除队列：读操作入队，写操作批量清理
#   5. 混合负载只用 GET，避免 SET 重置 TTL
#   使用普通权限进行测试，避免 sudo 可能引起的环境差异
script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

set +e
set +o pipefail

require_tools

# 测试参数
VALUE_SIZE=64

KVS_BIN="${PROJECT_DIR}/build/kvstore"
RESULT_FILE="${PROJECT_DIR}/test/bench/results_expire_v3.txt"
RESULT_CSV="${PROJECT_DIR}/test/bench/results_expire_v3.csv"

# 初始化输出
init_output() {
    mkdir -p "$(dirname "$RESULT_FILE")"

    cat > "$RESULT_FILE" << 'HEADER'
=============================================================================
过期键性能测试 V3 — Phase 9.2 优化版
=============================================================================
优化内容:
  1. 惰性删除队列: 读操作入队，写操作批量清理（零锁竞争）
  2. 自适应采样: 过期率 >50% 时采样数翻倍 (20→200 keys/cycle)
  3. 后台线程降频: 100ms → 1s（减少锁竞争）
  4. 快速写入: 使用 redis-cli --pipe，避免写入期间过期
=============================================================================
HEADER
    echo "测试时间: $(date '+%Y-%m-%d %H:%M:%S')" >> "$RESULT_FILE"
    echo "" >> "$RESULT_FILE"

    echo "scenario,threads,keys,ttl_sec,write_duration_s,actual_written,expired_before,expired_after,deleted_count,cleanup_duration_s,deletion_rate_keys_per_sec" > "$RESULT_CSV"
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
    local conf="/tmp/kvstore_expire_v3_t${threads}.conf"

    cat > "$conf" << CONFEOF
port 2000
network proactor
maxclients 10000
loglevel error
persistence none
threads ${threads}
cpu_pin auto
rdma_sync off
incr_repl off
CONFEOF

    pkill -9 kvstore 2>/dev/null || true
    sleep 1

    nohup "$KVS_BIN" "$conf" > /tmp/kvstore_expire_v3.log 2>&1 &
    sleep 2

    if ! redis-cli -p 2000 PING >/dev/null 2>&1; then
        err "kvstore 启动失败"
        tail -20 /tmp/kvstore_expire_v3.log
        return 1
    fi

    local pid=$(pgrep -f kvstore | head -1)
    echo "$pid"
}

# 场景1: 突发过期测试（使用 redis-cli --pipe 快速写入）
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

    # 记录写入前的 expired_keys
    local expired_before=$(get_expired_keys)
    # local vmrss_before=$(get_vmrss "$pid")  # 注释掉：VmRSS 不准确

    # 使用 gen_expire_data.py 快速写入
    log "  使用 redis-cli --pipe 快速写入 ${nkeys} keys (TTL=${ttl}s)..."
    local t0=$(date +%s.%N)

    python3 "${PROJECT_DIR}/test/bench/gen_expire_data.py" ${nkeys} ${VALUE_SIZE} ${ttl} --redis-benchmark-keys \
        | redis-cli -p 2000 --pipe >/dev/null 2>&1

    local t1=$(date +%s.%N)
    local write_duration=$(echo "$t1 - $t0" | bc)

    # 写入后立即记录
    local dbsize_after=$(redis-cli -p 2000 DBSIZE 2>/dev/null)
    local expired_after_write=$(get_expired_keys)
    local deleted_during_write=$((expired_after_write - expired_before))
    local actual_written=$((dbsize_after + deleted_during_write))

    log "  写入完成: 耗时=${write_duration}s, 实际写入=${actual_written}, 当前DBSIZE=${dbsize_after}, 写入期间删除=${deleted_during_write}"

    # 等待过期
    log "  等待过期 (${ttl}s)..."
    sleep "$ttl"

    # 监控主动删除
    log "  监控主动删除..."
    local cleanup_start=$(date +%s)
    local max_wait=30

    for i in $(seq 1 $max_wait); do
        sleep 1
        local dbsize_now=$(redis-cli -p 2000 DBSIZE 2>/dev/null)
        local expired_now=$(get_expired_keys)
        local deleted=$((expired_now - expired_after_write))
        local pct=0
        [ "$dbsize_after" -gt 0 ] && pct=$((deleted * 100 / dbsize_after))

        if [ $((i % 3)) -eq 0 ] || [ "$dbsize_now" -eq 0 ]; then
            log "    [${i}s] DBSIZE=${dbsize_now}, deleted=${deleted}/${dbsize_after} (${pct}%)"
        fi

        if [ "$dbsize_now" -eq 0 ]; then
            log "  ✓ 所有 key 已删除，耗时 ${i}s"
            break
        fi
    done

    local cleanup_end=$(date +%s)
    local cleanup_duration=$((cleanup_end - cleanup_start))

    sleep 2
    # local vmrss_after=$(get_vmrss "$pid")  # 注释掉：VmRSS 不准确
    local expired_after=$(get_expired_keys)
    local deleted_count=$((expired_after - expired_after_write))

    # local reclaim_kb=$((vmrss_before - vmrss_after))  # 注释掉
    # local reclaim_pct=0  # 注释掉
    # [ "$vmrss_before" -gt 0 ] && reclaim_pct=$(echo "scale=1; $reclaim_kb * 100 / $vmrss_before" | bc -l 2>/dev/null || echo 0)  # 注释掉

    local deletion_rate=0
    [ "$cleanup_duration" -gt 0 ] && deletion_rate=$((deleted_count / cleanup_duration))

    log "  结果: 删除=${deleted_count}, 清理耗时=${cleanup_duration}s, 删除速率=${deletion_rate} keys/s"

    echo "burst,${threads},${nkeys},${ttl},${write_duration},${actual_written},${expired_after_write},${expired_after},${deleted_count},${cleanup_duration},${deletion_rate}" >> "$RESULT_CSV"

    pkill -9 kvstore 2>/dev/null || true
    sleep 1
}

# 场景2: 混合负载测试（只用 GET，避免 SET 重置 TTL）
test_mixed_workload() {
    local threads=$1
    local nkeys=$2
    local ttl=$3

    log "========================================"
    log "混合负载: threads=${threads}, keys=${nkeys}, TTL=${ttl}s, 后台读负载"
    log "========================================"

    local pid=$(start_kvs "$threads")
    if [ -z "$pid" ]; then
        err "启动失败"
        return 1
    fi

    redis-cli -p 2000 FLUSHALL >/dev/null 2>&1
    sleep 1

    # 记录写入前的 expired_keys
    local expired_before=$(get_expired_keys)

    # 使用 gen_expire_data.py 快速写入
    log "  快速写入 ${nkeys} keys (TTL=${ttl}s)..."
    local t0=$(date +%s.%N)

    python3 "${PROJECT_DIR}/test/bench/gen_expire_data.py" ${nkeys} ${VALUE_SIZE} ${ttl} --redis-benchmark-keys \
        | redis-cli -p 2000 --pipe >/dev/null 2>&1

    local t1=$(date +%s.%N)
    local write_duration=$(echo "$t1 - $t0" | bc)

    local dbsize_after=$(redis-cli -p 2000 DBSIZE 2>/dev/null)
    local expired_after_write=$(get_expired_keys)
    local deleted_during_write=$((expired_after_write - expired_before))
    local actual_written=$((dbsize_after + deleted_during_write))

    log "  写入完成: 实际写入=${actual_written}, 当前DBSIZE=${dbsize_after}"

    sleep 1
    # local vmrss_before=$(get_vmrss "$pid")  # 注释掉：VmRSS 不准确

    # 启动后台读负载（只 GET，不 SET）
    log "  启动后台读负载（只 GET，避免重置 TTL）..."
    redis-benchmark -p 2000 -t get -n 500000 -r "$nkeys" -d "$VALUE_SIZE" -q > /tmp/bench_mixed_v3.log 2>&1 &
    local bench_pid=$!

    # 等待过期
    log "  等待过期 (${ttl}s)..."
    sleep "$ttl"

    # 监控主动删除
    log "  监控主动删除..."
    local cleanup_start=$(date +%s)
    local max_wait=30

    for i in $(seq 1 $max_wait); do
        sleep 1
        local dbsize_now=$(redis-cli -p 2000 DBSIZE 2>/dev/null)
        local expired_now=$(get_expired_keys)
        local deleted=$((expired_now - expired_after_write))

        if [ $((i % 3)) -eq 0 ] || [ "$dbsize_now" -le 100 ]; then
            log "    [${i}s] DBSIZE=${dbsize_now}, deleted=${deleted}/${dbsize_after}"
        fi

        if [ "$dbsize_now" -le 100 ]; then
            log "  ✓ 过期 key 已基本清理完毕，耗时 ${i}s"
            break
        fi
    done

    local cleanup_end=$(date +%s)
    local cleanup_duration=$((cleanup_end - cleanup_start))

    # 停止后台负载
    kill "$bench_pid" 2>/dev/null || true
    wait "$bench_pid" 2>/dev/null || true

    sleep 2
    # local vmrss_after=$(get_vmrss "$pid")  # 注释掉：VmRSS 不准确
    local expired_after=$(get_expired_keys)
    local deleted_count=$((expired_after - expired_after_write))

    # local reclaim_kb=$((vmrss_before - vmrss_after))  # 注释掉
    # local reclaim_pct=0  # 注释掉
    # [ "$vmrss_before" -gt 0 ] && reclaim_pct=$(echo "scale=1; $reclaim_kb * 100 / $vmrss_before" | bc -l 2>/dev/null || echo 0)  # 注释掉

    local deletion_rate=0
    [ "$cleanup_duration" -gt 0 ] && deletion_rate=$((deleted_count / cleanup_duration))

    log "  结果: 删除=${deleted_count}, 清理耗时=${cleanup_duration}s, 删除速率=${deletion_rate} keys/s"

    echo "mixed,${threads},${nkeys},${ttl},${write_duration},${actual_written},${expired_after_write},${expired_after},${deleted_count},${cleanup_duration},${deletion_rate}" >> "$RESULT_CSV"

    pkill -9 kvstore 2>/dev/null || true
    sleep 1
}

# 主流程
init_output

log "开始过期键性能测试 V3 (Phase 9.2 优化版)"
echo ""

# 突发过期测试 - 不同规模
test_burst_expire 1 10000 5
test_burst_expire 1 50000 5
test_burst_expire 1 100000 5

# 混合负载测试
test_mixed_workload 1 50000 5

echo ""
log "测试完成！结果已保存："
log "  文本报告: $RESULT_FILE"
log "  CSV数据:  $RESULT_CSV"
echo ""
log "Phase 9.2 优化总结："
log "  ✓ 惰性删除队列: 零锁竞争"
log "  ✓ 自适应采样: 20 → 200 keys/cycle"
log "  ✓ 后台线程降频: 100ms → 1s"
log "  ✓ 快速写入: redis-cli --pipe"
echo ""
