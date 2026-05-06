#!/bin/bash
# =============================================================================
# bench_aof.sh — AOF 增量持久化性能测试
#
# 对比 4 种场景的 SET QPS:
#   A. kvstore AOF 关闭
#   B. kvstore AOF 开启（AOF+RDB 双开）
#   C. Redis    无持久化
#   D. Redis    AOF 开启 (appendfsync everysec)
#
# 用法:
#   sudo ./test/bench/bench_aof.sh
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

require_root
require_tools

BENCH_N=1000000
BENCH_C=50
BENCH_P=20

init_results
record_raw "=== AOF 持久化性能测试 ==="
record_raw "压测: redis-benchmark -n $BENCH_N -c $BENCH_C -P $BENCH_P"
record_raw ""

# ---- A. kvstore AOF 关闭 ----
step "A. kvstore SET (AOF 关闭)"
start_kvs "kvstore-none.conf"
populate 127.0.0.1 "$KVS_PORT" 1000
kvs_none_qps=$(run_bench 127.0.0.1 "$KVS_PORT" set $BENCH_N $BENCH_C $BENCH_P)
record_result "AOF-SET" "A-kvstore-AOF关闭" "$kvs_none_qps"
stop_kvs

# ---- B. kvstore AOF 开启 ----
step "B. kvstore SET (AOF 开启, AOF+RDB 双开)"
start_kvs "kvstore-aof.conf"
populate 127.0.0.1 "$KVS_PORT" 1000
    kvs_aof_qps=$(run_bench 127.0.0.1 "$KVS_PORT" set $BENCH_N $BENCH_C $BENCH_P)
record_result "AOF-SET" "B-kvstore-AOF开启" "$kvs_aof_qps"
# 计算 AOF 开销
    aof_overhead=$(echo "scale=1; (1 - $kvs_aof_qps / $kvs_none_qps) * 100" | bc -l 2>/dev/null || echo "N/A")
record_raw "  kvstore AOF 性能损耗: ${aof_overhead}%"
# AOF 文件大小
    aof_file="${PROJECT_DIR}/build/appendonly.aof"
    aof_size=0
[ -f "$aof_file" ] && aof_size=$(stat -c%s "$aof_file" 2>/dev/null || echo 0)
record_raw "  AOF 文件大小: $(echo "scale=1; $aof_size/1024/1024" | bc -l) MB"
record_raw ""
stop_kvs

# ---- C. Redis 无持久化 ----
step "C. Redis SET (无持久化)"
start_redis
redis_configure none
    redis_none_qps=$(run_bench 127.0.0.1 "$REDIS_PORT" set $BENCH_N $BENCH_C $BENCH_P)
record_result "AOF-SET" "C-Redis-无持久化" "$redis_none_qps"

# ---- D. Redis AOF 开启 ----
step "D. Redis SET (AOF 开启, appendfsync everysec)"
redis_configure aof
    redis_aof_qps=$(run_bench 127.0.0.1 "$REDIS_PORT" set $BENCH_N $BENCH_C $BENCH_P)
record_result "AOF-SET" "D-Redis-AOF开启" "$redis_aof_qps"
    redis_overhead=$(echo "scale=1; (1 - $redis_aof_qps / $redis_none_qps) * 100" | bc -l 2>/dev/null || echo "N/A")
record_raw "  Redis AOF 性能损耗: ${redis_overhead}%"
record_raw ""

stop_redis
stop_kvs

# ---- 汇总 ----
record_raw ""
record_raw "=== AOF 测试汇总 ==="
record_raw ""
record_raw "  场景                         | QPS"
record_raw "  -----------------------------|----------"
record_raw "  A. kvstore SET (AOF 关闭)    | $kvs_none_qps"
record_raw "  B. kvstore SET (AOF+RDB)     | $kvs_aof_qps  (损耗 ${aof_overhead}%)"
record_raw "  C. Redis SET  (无持久化)     | $redis_none_qps"
record_raw "  D. Redis SET  (AOF everysec) | $redis_aof_qps  (损耗 ${redis_overhead}%)"
record_raw ""

echo ""
cat "$RESULT_FILE"
echo ""
log "结果保存至: $RESULT_FILE / $RESULT_CSV"
