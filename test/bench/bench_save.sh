#!/bin/bash
# =============================================================================
# bench_save.sh — BGSAVE 频率对性能影响测试
#
# 频率-数据集配对（间隔越大 → 数据集越大）：
#   1M 间隔  → 1M keys  (500万命令, 5次BGSAVE)
#   100K     → 100K keys (100万命令, 10次BGSAVE)
#   10K      → 10K keys  (50万命令,  50次BGSAVE)
#   1K       → 1K keys   (10万命令,  100次BGSAVE)
#
# 测量指标：
#   - 基线 QPS（无 SAVE 开销）
#   - 纯 SET QPS（仅 redis-benchmark 耗时，BGSAVE 等待不计入）
#   - BGSAVE 精确耗时（后台 50ms 轮询）
#   - BGSAVE 次数 / 跳过次数
#   - QPS 降幅 vs 同数据量基线
#   - RDB 文件大小
#
# 用法:
#   sudo ./test/bench/bench_save.sh
#
# 输出:
#   - results_save.txt  : 详细文本报告
#   - results_save.csv  : CSV 格式数据（便于分析）
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

require_root
require_tools

# ---- 参数配置 ----
BENCH_C=50            # 并发连接数
BENCH_P=20            # Pipeline 深度
BENCH_D=16            # Value 大小（字节）

# 频率-数据集配对：每个 BGSAVE 间隔对应一个适当大小的数据集
# 间隔越小 → BGSAVE 越频繁 → 数据集越小（模拟真实场景中频繁保存的小数据量）
# 间隔越大 → BGSAVE 越稀疏 → 数据集越大（模拟大容量批量持久化）
declare -A PAIR=(
    ["1000000"]="1000000:5000000"   # 1M间隔, 1M keys,  500万命令 → 5次BGSAVE
    ["100000"]="100000:1000000"     # 100K,   100K keys, 100万命令 → 10次BGSAVE
    ["10000"]="10000:500000"        # 10K,    10K keys,  50万命令  → 50次BGSAVE
    ["1000"]="1000:100000"          # 1K,     1K keys,   10万命令  → 100次BGSAVE
)
SAVE_INTERVALS=(1000000 100000 10000 1000)

# 基线测试的 key 数量（从 PAIR 的值中提取去重）
BASELINE_KEY_COUNTS=(1000 10000 100000 1000000)

RDB_FILE="${PROJECT_DIR}/build/dump.rdb"

# 输出文件
RESULT_FILE="${PROJECT_DIR}/test/bench/results_save.txt"
RESULT_CSV="${PROJECT_DIR}/test/bench/results_save.csv"

# ---- 辅助函数 ----

# 安全百分比计算：(1 - a/b) * 100
drop_pct() {
    local a=$1 b=$2
    if [[ "$a" =~ ^[0-9]+(\.[0-9]+)?$ ]] && \
       [[ "$b" =~ ^[0-9]+(\.[0-9]+)?$ ]] && \
       [ "$(echo "$b > 0" | bc -l 2>/dev/null)" = "1" ]; then
        printf "%.1f" "$(echo "scale=4; (1 - $a/$b)*100" | bc -l 2>/dev/null)"
    else
        echo "0.0"
    fi
}

# Wait for BGSAVE to complete by polling INFO persistence
wait_bgsave_done() {
    local host=$1 port=$2 timeout=${3:-30}
    local start=$(date +%s)
    while true; do
        local state
        state=$(redis-cli -h "$host" -p "$port" INFO persistence 2>/dev/null | grep '^rdb_bgsave_in_progress' | cut -d: -f2 | tr -d '\r')
        if [ "$state" = "0" ]; then
            return 0
        fi
        local now=$(date +%s)
        if [ $((now - start)) -ge $timeout ]; then
            echo "[WARN] BGSAVE timeout after ${timeout}s" >&2
            return 1
        fi
        sleep 0.1
    done
}

# 场景 C: 固定命令数 BGSAVE（跳过已在运行的 BGSAVE）
#
# QPS 计算：仅统计 redis-benchmark 纯耗时，BGSAVE 时间不计入。
# BGSAVE 精确计时：每触发一个 BGSAVE 后，后台轮询 rdb_bgsave_in_progress
# （间隔 50ms）直至完成，记录精确耗时。
#
# 参数: host port interval total_commands
# 返回: pure_qps:bgsave_count:bgsave_avg_ms:bgsave_total_ms:pure_set_ms:bgsave_skipped
bench_fixed_count_bgsave() {
    local host=$1 port=$2 interval=$3 total_commands=$4
    local chunks=$((total_commands / interval))

    [ "$chunks" -lt 1 ] && chunks=1

    local bgsave_count=0
    local bgsave_skipped=0
    local bgsave_total_ms=0
    local pure_set_ms=0

    # 临时文件：后台 poller 写入本次 BGSAVE 耗时(ms)
    local poller_tmp
    poller_tmp=$(mktemp -t bgsave_duration.XXXXXX)
    local poller_pid=0

    for i in $(seq 1 $chunks); do
        # —— 计时：单个 redis-benchmark chunk ——
        local chunk_t0=$(date +%s%3N)
        redis-benchmark -h "$host" -p "$port" -t set \
            -n "$interval" -c $BENCH_C -P $BENCH_P -d $BENCH_D -q >/dev/null 2>&1 || true
        local chunk_t1=$(date +%s%3N)
        pure_set_ms=$((pure_set_ms + (chunk_t1 - chunk_t0)))

        # —— 检查 BGSAVE 状态 ——
        local bgsave_state
        bgsave_state=$(redis-cli -h "$host" -p "$port" INFO persistence 2>/dev/null \
            | grep '^rdb_bgsave_in_progress' | cut -d: -f2 | tr -d '\r')

        if [ "$bgsave_state" = "0" ]; then
            # 上一轮 BGSAVE 已完成 → 收集后台 poller 结果
            if [ "$poller_pid" -gt 0 ]; then
                wait "$poller_pid" 2>/dev/null
                local dur
                dur=$(cat "$poller_tmp" 2>/dev/null || echo 0)
                bgsave_total_ms=$((bgsave_total_ms + dur))
                poller_pid=0
            fi

            # 触发新的 BGSAVE（非阻塞）并启动后台精确计时
            local bgsave_start=$(date +%s%3N)
            redis-cli -h "$host" -p "$port" BGSAVE >/dev/null 2>&1 || true
            bgsave_count=$((bgsave_count + 1))

            # 后台 poller：每 50ms 轮询一次，精确捕捉 BGSAVE 结束时刻
            (
                while true; do
                    s=$(redis-cli -h "$host" -p "$port" INFO persistence 2>/dev/null \
                        | grep '^rdb_bgsave_in_progress' | cut -d: -f2 | tr -d '\r')
                    if [ "$s" = "0" ]; then
                        echo $(( $(date +%s%3N) - bgsave_start ))
                        break
                    fi
                    sleep 0.05
                done
            ) > "$poller_tmp" &
            poller_pid=$!
        else
            bgsave_skipped=$((bgsave_skipped + 1))
        fi
    done

    # —— 等待最后一个 BGSAVE 完成并收集结果 ——
    if [ "$poller_pid" -gt 0 ]; then
        wait "$poller_pid" 2>/dev/null
        local dur
        dur=$(cat "$poller_tmp" 2>/dev/null || echo 0)
        bgsave_total_ms=$((bgsave_total_ms + dur))
        poller_pid=0
    fi

    rm -f "$poller_tmp"

    # —— 计算指标 ——
    local pure_qps=0
    if [ "$pure_set_ms" -gt 0 ]; then
        pure_qps=$(echo "scale=2; $total_commands * 1000 / $pure_set_ms" | bc -l 2>/dev/null || echo 0)
    fi

    local bgsave_avg_ms=0
    if [ "$bgsave_count" -gt 0 ]; then
        bgsave_avg_ms=$((bgsave_total_ms / bgsave_count))
    fi

    if [ "$bgsave_skipped" -gt 0 ]; then
        echo "[INFO] ${bgsave_skipped}/${chunks} BGSAVE calls skipped (already in progress)" >&2
    fi

    echo "${pure_qps}:${bgsave_count}:${bgsave_avg_ms}:${bgsave_total_ms}:${pure_set_ms}:${bgsave_skipped}"
}

# ---- 基线测试 ----
run_baseline() {
    local nkeys=$1

    echo ""
    echo "════════════════════════════════════════════════"
    step "基线: ${nkeys} keys（无 SAVE）"
    echo "════════════════════════════════════════════════"

    stop_redis
    start_kvs "kvstore-rdb.conf"
    local port
    port=$(grep '^port ' "${CONF_DIR}/kvstore-rdb.conf" | awk '{print $2}')
    port=${port:-$KVS_PORT}

    populate 127.0.0.1 "$port" "$nkeys"

    step "执行基线压测..."
    local baseline_n=1000000
    local baseline
    baseline=$(run_bench 127.0.0.1 "$port" set $baseline_n $BENCH_C $BENCH_P $BENCH_D)

    echo "  keys=${nkeys}  基线 QPS: ${baseline} (命令数: ${baseline_n})"
    echo "${nkeys},baseline,${baseline},0,0,0,0.0,${baseline},${baseline_n},0,0" >> "$RESULT_CSV"

    stop_kvs
    rm -f "$RDB_FILE" "${PROJECT_DIR}/build/dump.rdb.tmp"
}

# ---- 单个配对场景测试 ----
# 参数: nkeys interval total_commands baseline_qps
run_bgsave_pair() {
    local nkeys=$1 interval=$2 total_commands=$3 baseline=$4

    local interval_label
    if [ "$interval" -ge 1000000 ]; then
        interval_label="$((interval / 1000000))M"
    elif [ "$interval" -ge 1000 ]; then
        interval_label="$((interval / 1000))K"
    else
        interval_label="${interval}"
    fi

    # 独立启动
    start_kvs "kvstore-rdb.conf"
    local test_port
    test_port=$(grep '^port ' "${CONF_DIR}/kvstore-rdb.conf" | awk '{print $2}')
    test_port=${test_port:-$KVS_PORT}
    populate 127.0.0.1 "$test_port" "$nkeys"

    # 返回: pure_qps:bgsave_count:bgsave_avg_ms:bgsave_total_ms:pure_set_ms:bgsave_skipped
    local result
    result=$(bench_fixed_count_bgsave 127.0.0.1 "$test_port" "$interval" "$total_commands")

    local pure_qps bgsave_count bgsave_avg_ms bgsave_total_ms pure_set_ms bgsave_skipped
    pure_qps=$(echo       "$result" | cut -d: -f1)
    bgsave_count=$(echo   "$result" | cut -d: -f2)
    bgsave_avg_ms=$(echo  "$result" | cut -d: -f3)
    bgsave_total_ms=$(echo "$result" | cut -d: -f4)
    pure_set_ms=$(echo    "$result" | cut -d: -f5)
    bgsave_skipped=$(echo "$result" | cut -d: -f6)

    local qps_drop
    qps_drop=$(drop_pct "$pure_qps" "$baseline")

    printf "  %-10s | %6s keys | %10s | %10s | %6s | %6s | %8s | %7s%%\n" \
        "$interval_label" "$nkeys" "$total_commands" "$pure_qps" "$bgsave_count" \
        "$bgsave_skipped" "$bgsave_avg_ms" "$qps_drop"

    echo "${nkeys},${interval_label},${pure_qps},${bgsave_count},${bgsave_avg_ms},${bgsave_total_ms},${qps_drop},${baseline},${total_commands},${pure_set_ms},${bgsave_skipped}" >> "$RESULT_CSV"

    # RDB 文件大小
    local rdb_mb="0.0"
    if [ -f "$RDB_FILE" ]; then
        local rdb_size
        rdb_size=$(stat -c%s "$RDB_FILE" 2>/dev/null || echo 0)
        rdb_mb=$(echo "scale=1; $rdb_size/1048576" | bc -l 2>/dev/null || echo 0.0)
    fi
    echo "    RDB: ${rdb_mb} MB  |  BGSAVE 总耗时: ${bgsave_total_ms} ms  |  SET 纯耗时: ${pure_set_ms} ms"

    stop_kvs
    rm -f "$RDB_FILE" "${PROJECT_DIR}/build/dump.rdb.tmp"
    sleep 1
}

# ---- 初始化输出文件 ----
init_output() {
    local output_dir="${PROJECT_DIR}/test/bench"
    mkdir -p "$output_dir"

    # 文本报告
    cat > "$RESULT_FILE" << EOF
=============================================================================
BGSAVE 频率对性能影响测试
=============================================================================
测试时间: $(date '+%Y-%m-%d %H:%M:%S')
测试参数:
  - 并发连接: ${BENCH_C}
  - Pipeline: ${BENCH_P}
  - Value大小: ${BENCH_D} bytes
  - BGSAVE 精确计时: 后台每 50ms 轮询 rdb_bgsave_in_progress

频率-数据集配对:
  - 1M 间隔 → 1M keys  (5次BGSAVE)
  - 100K   → 100K keys (10次BGSAVE)
  - 10K    → 10K keys  (50次BGSAVE)
  - 1K     → 1K keys   (100次BGSAVE)

QPS 计算说明:
  - 纯 SET QPS = 总命令数 / sum(各 chunk 的 redis-benchmark 耗时)
  - BGSAVE 等待时间不计入 QPS
  - BGSAVE 对 SET 的 CPU/IO 争抢会反映在各 chunk 耗时中

测试场景:
  基线:      纯 SET，不触发 SAVE（每个 key 数量一条基线）
  BGSAVE:    每 N 条命令触发一次 BGSAVE，跳过已在运行的

=============================================================================

EOF

    # CSV 文件
    echo "keys,interval,pure_qps,bgsave_count,bgsave_avg_ms,bgsave_total_ms,qps_drop_pct,baseline_qps,total_commands,pure_set_ms,bgsave_skipped" > "$RESULT_CSV"
}

# ---- 生成汇总报告 ----
generate_summary() {
    echo "" | tee -a "$RESULT_FILE"
    echo "════════════════════════════════════════════════" | tee -a "$RESULT_FILE"
    echo "  汇总表" | tee -a "$RESULT_FILE"
    echo "════════════════════════════════════════════════" | tee -a "$RESULT_FILE"
    echo "" | tee -a "$RESULT_FILE"

    # ── 基线汇总 ──
    echo "【基线 QPS】（无 SAVE）" | tee -a "$RESULT_FILE"
    printf "  %-12s | %10s | %12s\n" \
        "keys" "命令数" "基线 QPS" | tee -a "$RESULT_FILE"
    printf "  %-12s | %10s | %12s\n" \
        "------------" "----------" "------------" | tee -a "$RESULT_FILE"

    for nkeys in "${BASELINE_KEY_COUNTS[@]}"; do
        local line
        line=$(grep "^${nkeys},baseline," "$RESULT_CSV" | tail -1)
        if [ -n "$line" ]; then
            local b_qps b_cmds
            b_qps=$(echo "$line"  | cut -d, -f3)
            b_cmds=$(echo "$line" | cut -d, -f9)
            printf "  %-12s | %10s | %12s\n" \
                "$nkeys" "$b_cmds" "$b_qps" | tee -a "$RESULT_FILE"
        fi
    done

    # ── BGSAVE 汇总 ──
    echo "" | tee -a "$RESULT_FILE"
    echo "【BGSAVE 频率影响】（间隔 → 配对数据集）" | tee -a "$RESULT_FILE"
    printf "  %-10s | %8s | %10s | %10s | %6s | %6s | %8s | %8s\n" \
        "间隔" "keys" "总命令" "纯SET QPS" "次数" "跳过" "avg(ms)" "QPS降幅" | tee -a "$RESULT_FILE"
    printf "  %-10s | %8s | %10s | %10s | %6s | %6s | %8s | %8s\n" \
        "----------" "--------" "----------" "----------" "------" "------" "--------" "--------" | tee -a "$RESULT_FILE"

    for interval in "${SAVE_INTERVALS[@]}"; do
        local interval_label
        if [ "$interval" -ge 1000000 ]; then
            interval_label="$((interval / 1000000))M"
        elif [ "$interval" -ge 1000 ]; then
            interval_label="$((interval / 1000))K"
        else
            interval_label="${interval}"
        fi

        # 该 interval 对应的 key 数量（从 PAIR 中提取）
        local pair="${PAIR[$interval]}"
        local nkeys="${pair%%:*}"

        local line
        line=$(grep "^${nkeys},${interval_label}," "$RESULT_CSV" | tail -1)
        if [ -n "$line" ]; then
            local pure_qps bgsave_count bgsave_avg_ms bgsave_total_ms qps_drop total_cmds bgsave_skipped pure_set_ms
            pure_qps=$(echo       "$line" | cut -d, -f3)
            bgsave_count=$(echo   "$line" | cut -d, -f4)
            bgsave_avg_ms=$(echo  "$line" | cut -d, -f5)
            bgsave_total_ms=$(echo "$line" | cut -d, -f6)
            qps_drop=$(echo       "$line" | cut -d, -f7)
            total_cmds=$(echo     "$line" | cut -d, -f9)
            bgsave_skipped=$(echo "$line" | cut -d, -f11)
            pure_set_ms=$(echo    "$line" | cut -d, -f10)

            printf "  %-10s | %8s | %10s | %10s | %6s | %6s | %8s | %7s%%\n" \
                "$interval_label" "$nkeys" "$total_cmds" "$pure_qps" "$bgsave_count" \
                "$bgsave_skipped" "$bgsave_avg_ms" "$qps_drop" | tee -a "$RESULT_FILE"

            # 额外细节
            echo "           → BGSAVE 总耗时: ${bgsave_total_ms} ms | SET 纯耗时: ${pure_set_ms} ms" | tee -a "$RESULT_FILE"
        fi
    done

    echo "" | tee -a "$RESULT_FILE"
    echo "==============================================================================" | tee -a "$RESULT_FILE"
    echo "" | tee -a "$RESULT_FILE"
}

# ---- 执行测试 ----
init_output

# 基线 QPS 缓存（key=nkeys, value=baseline_qps）
declare -A BASELINE_QPS

echo ""
step "═══ 第一阶段: 基线测试 ═══"
for nkeys in "${BASELINE_KEY_COUNTS[@]}"; do
    run_baseline "$nkeys" | tee -a "$RESULT_FILE"
    bq=$(grep "^${nkeys},baseline," "$RESULT_CSV" | tail -1 | cut -d, -f3)
    BASELINE_QPS["$nkeys"]="${bq:-0}"
done

echo ""
step "═══ 第二阶段: BGSAVE 频率测试 ═══"
echo "  配对规则: 间隔越大 → 数据集越大"

# 表头
printf "  %-10s | %10s | %10s | %10s | %6s | %6s | %8s | %8s\n" \
    "间隔" "keys" "总命令" "纯SET QPS" "次数" "跳过" "avg(ms)" "QPS降幅"
printf "  %-10s | %10s | %10s | %10s | %6s | %6s | %8s | %8s\n" \
    "----------" "----------" "----------" "----------" "------" "------" "--------" "--------"

for interval in "${SAVE_INTERVALS[@]}"; do
    pair="${PAIR[$interval]}"
    nkeys="${pair%%:*}"
    total_cmds="${pair##*:}"
    baseline="${BASELINE_QPS[$nkeys]}"

    run_bgsave_pair "$nkeys" "$interval" "$total_cmds" "$baseline" | tee -a "$RESULT_FILE"
done

generate_summary

echo ""
log "测试完成！结果已保存："
log "  文本报告: $RESULT_FILE"
log "  CSV数据:  $RESULT_CSV"
echo ""
