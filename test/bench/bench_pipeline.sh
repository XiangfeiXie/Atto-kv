#!/bin/bash
# =============================================================================
# bench_pipeline.sh — 批量处理（Pipeline）性能对比测试
#
# 对比不同 Pipeline 深度下 kvstore 与 Redis 的 QPS。
#
# 用法:
#   sudo ./test/bench/bench_pipeline.sh           # 全部 pipeline 深度
#   sudo ./test/bench/bench_pipeline.sh 40        # 只测 P=40
#
# Pipeline 深度: 1, 10, 20, 40, 80, 160
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

require_root
require_tools

BENCH_N=1000000
BENCH_C=50

# Pipeline 深度列表
if [ $# -ge 1 ]; then
    PIPELINES=("$@")
else
    PIPELINES=(1 10 20 40 80 160)
fi

init_results
record_raw "=== Pipeline 批量处理性能对比 ==="
record_raw "压测: redis-benchmark -n $BENCH_N -c $BENCH_C -P <N>"
record_raw ""

# ---- 启动服务 ----
step "启动 kvstore (AOF 关闭, 纯内存)"
stop_redis
start_kvs "kvstore-none.conf"

step "启动 Redis (无持久化)"
start_redis
redis_configure none

# ---- 测试函数 ----
test_pipeline() {
    local P=$1
    step "Pipeline=$P — SET"

    # kvstore SET
    local kvs_set=$(run_bench 127.0.0.1 "$KVS_PORT" set $BENCH_N $BENCH_C "$P")
    record_result "Pipeline-${P}-SET" "kvstore" "$kvs_set"

    # Redis SET
    local redis_set=$(run_bench 127.0.0.1 "$REDIS_PORT" set $BENCH_N $BENCH_C "$P")
    record_result "Pipeline-${P}-SET" "Redis" "$redis_set"

    # 对比
    local ratio=$(echo "scale=2; $kvs_set / $redis_set" | bc -l 2>/dev/null || echo "N/A")
    record_raw "  Pipeline=$P SET: kvstore=${kvs_set}, Redis=${redis_set}, 比值=${ratio}"

    step "Pipeline=$P — GET"

    # 先填充一些数据
    populate 127.0.0.1 "$KVS_PORT" 10000
    populate 127.0.0.1 "$REDIS_PORT" 10000

    # kvstore GET
    local kvs_get=$(run_bench 127.0.0.1 "$KVS_PORT" get $BENCH_N $BENCH_C "$P")
    record_result "Pipeline-${P}-GET" "kvstore" "$kvs_get"

    # Redis GET
    local redis_get=$(run_bench 127.0.0.1 "$REDIS_PORT" get $BENCH_N $BENCH_C "$P")
    record_result "Pipeline-${P}-GET" "Redis" "$redis_get"

    local ratio_get=$(echo "scale=2; $kvs_get / $redis_get" | bc -l 2>/dev/null || echo "N/A")
    record_raw "  Pipeline=$P GET: kvstore=${kvs_get}, Redis=${redis_get}, 比值=${ratio_get}"
    record_raw ""
}

# ---- 执行测试 ----
for P in "${PIPELINES[@]}"; do
    test_pipeline "$P"
done

# ---- 汇总表格 ----
record_raw ""
record_raw "=== Pipeline 测试汇总 ==="
record_raw ""
printf "  %-8s | %12s | %12s | %12s | %12s\n" "Pipeline" "kvs-SET" "Redis-SET" "kvs-GET" "Redis-GET" >> "$RESULT_FILE"
printf "  %-8s | %12s | %12s | %12s | %12s\n" "--------" "------------" "------------" "------------" "------------" >> "$RESULT_FILE"
for P in "${PIPELINES[@]}"; do
    kvss=$(grep "Pipeline-${P}-SET,kvstore," "$RESULT_CSV" | cut -d',' -f3)
    rs=$(grep "Pipeline-${P}-SET,Redis," "$RESULT_CSV" | cut -d',' -f3)
    kvsg=$(grep "Pipeline-${P}-GET,kvstore," "$RESULT_CSV" | cut -d',' -f3)
    rg=$(grep "Pipeline-${P}-GET,Redis," "$RESULT_CSV" | cut -d',' -f3)
    printf "  %-8s | %12s | %12s | %12s | %12s\n" "P=$P" "$kvss" "$rs" "$kvsg" "$rg" >> "$RESULT_FILE"
done

stop_kvs
stop_redis

echo ""
cat "$RESULT_FILE"
echo ""
log "结果保存至: $RESULT_FILE / $RESULT_CSV"
