#!/bin/bash
# =============================================================================
# bench_save.sh — SAVE 指令性能影响测试
#
# 测试不同数据量下执行 SAVE（RDB fork+write）对 SET QPS 的影响。
#
# 用法:
#   sudo ./test/bench/bench_save.sh              # 全部数据量
#   sudo ./test/bench/bench_save.sh 1000         # 只测 1k
#   sudo ./test/bench/bench_save.sh 1000000      # 只测 100w
#
# 参数: key 数量（默认: 1000 10000 100000 1000000）
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

require_root
require_tools

BENCH_N=300000    # 每轮压测请求数
BENCH_C=50        # 并发连接数
BENCH_P=20        # Pipeline 深度
SAVE_INTERVAL=3   # SAVE 间隔（秒）
SAVE_COUNT=10     # SAVE 次数

# 数据量列表（从命令行参数获取，或默认全部）
if [ $# -ge 1 ]; then
    KEY_COUNTS=("$@")
else
    KEY_COUNTS=(1000 10000 100000 1000000)
fi

init_results
record_raw "=== SAVE 性能测试 (fork + io_uring RDB write) ==="
record_raw ""

run_save_test() {
    local nkeys=$1
    step "=== 测试: ${nkeys} keys ==="

    stop_redis

    # 1. 启动 kvstore (RDB-only)
    start_kvs "kvstore-rdb.conf"
    local port=$(grep '^port ' "${CONF_DIR}/kvstore-rdb.conf" | awk '{print $2}')
    port=${port:-$KVS_PORT}

    # 2. 填充数据
    populate 127.0.0.1 "$port" "$nkeys"

    # 3. 基线（无 SAVE）
    step "基线: 纯 SET 压测（无 SAVE）..."
    local baseline_qps=$(run_bench 127.0.0.1 "$port" set $BENCH_N $BENCH_C $BENCH_P)
    record_result "SAVE-${nkeys}" "kvstore-无SAVE" "$baseline_qps"

    # 4. SAVE 场景: 后台周期性发 SAVE
    step "SAVE 场景: 每 ${SAVE_INTERVAL}s 一次 SAVE，共 ${SAVE_COUNT} 次..."
    # 后台线程: 周期性 SAVE
    (
        sleep 2  # 等压测先跑起来
        for i in $(seq 1 $SAVE_COUNT); do
            printf '*1\r\n$4\r\nSAVE\r\n' | nc -w 2 127.0.0.1 "$port" > /dev/null 2>&1
            sleep $SAVE_INTERVAL
        done
    ) &
    local save_bg_pid=$!

    local save_qps=$(run_bench 127.0.0.1 "$port" set $BENCH_N $BENCH_C $BENCH_P)
    wait $save_bg_pid 2>/dev/null || true

    local drop_pct=$(echo "scale=1; (1 - $save_qps / $baseline_qps) * 100" | bc -l 2>/dev/null || echo "N/A")
    record_result "SAVE-${nkeys}" "kvstore-周期SAVE" "$save_qps"
    record_raw "  SAVE 降幅: ${drop_pct}%"

    # 5. 记录 RDB 文件大小
    local rdb_file="${PROJECT_DIR}/build/dump.rdb"
    local rdb_size=0
    [ -f "$rdb_file" ] && rdb_size=$(stat -c%s "$rdb_file" 2>/dev/null || echo 0)
    record_raw "  RDB 大小: $(echo "scale=1; $rdb_size/1024/1024" | bc -l) MB"
    record_raw ""

    stop_kvs
    rm -f "$rdb_file" "${PROJECT_DIR}/build/dump.rdb.tmp"
}

step "SAVE 性能测试开始"
for n in "${KEY_COUNTS[@]}"; do
    run_save_test "$n"
done

echo ""
cat "$RESULT_FILE"
echo ""
log "结果保存至: $RESULT_FILE / $RESULT_CSV"
