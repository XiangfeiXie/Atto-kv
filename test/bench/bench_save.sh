#!/bin/bash
# =============================================================================
# bench_save.sh — BGSAVE 频率对性能影响测试
#
# 测试场景：
#   场景 A: 基线       — 纯 SET，不触发任何 SAVE
#   场景 C: 固定命令数 — 每写入 N 条命令触发一次 BGSAVE，等待完成
#                        测试 N = 1M, 100K, 10K, 1K 四种频率
#
# 测量指标：
#   - 基线 QPS（无 SAVE 开销）
#   - 有效 QPS（含 BGSAVE 等待时间）
#   - BGSAVE 平均耗时
#   - BGSAVE 次数
#   - QPS 降幅 vs 基线
#   - BGSAVE 时间占比
#
# 用法:
#   sudo ./test/bench/bench_save.sh               # 默认: 10k / 100k / 1M keys
#   sudo ./test/bench/bench_save.sh 10000 100000  # 自定义数据集
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

# 固定命令数间隔配置（间隔:总命令数）
# 设计原则：测试不同BGSAVE频率对QPS的影响（纯异步，不等待完成）
declare -A SAVE_CONFIGS=(
    ["1000000"]="10000000"  # 1M间隔:  1000万命令 → 10次BGSAVE
    ["100000"]="10000000"   # 100K间隔: 1000万命令 → 100次BGSAVE
    ["10000"]="1000000"     # 10K间隔:  100万命令  → 100次BGSAVE
    ["1000"]="100000"       # 1K间隔:   10万命令   → 100次BGSAVE
)

RDB_FILE="${PROJECT_DIR}/build/dump.rdb"

# 数据集大小
if [ $# -ge 1 ]; then
    KEY_COUNTS=("$@")
else
    KEY_COUNTS=(10000 100000 1000000)
fi

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

# 场景 C: 固定命令数 BGSAVE（纯异步，不等待完成）
# 参数: host port interval total_commands
# 返回: effective_qps:bgsave_count:total_ms
bench_fixed_count_bgsave() {
    local host=$1 port=$2 interval=$3 total_commands=$4
    local chunks=$((total_commands / interval))

    # 如果 chunks < 1，至少执行一次
    [ "$chunks" -lt 1 ] && chunks=1

    local bgsave_count=0

    local t0=$(date +%s%3N)   # 毫秒级起始时间

    for i in $(seq 1 $chunks); do
        # 运行一个 chunk 的 SET 压测
        redis-benchmark -h "$host" -p "$port" -t set \
            -n "$interval" -c $BENCH_C -P $BENCH_P -d $BENCH_D -q >/dev/null 2>&1 || true

        # 触发 BGSAVE（不等待完成）
        redis-cli -h "$host" -p "$port" BGSAVE >/dev/null 2>&1 || true
        bgsave_count=$((bgsave_count + 1))
    done

    local t1=$(date +%s%3N)
    local elapsed_ms=$((t1 - t0))

    # 计算有效 QPS（纯异步场景）
    local eff_qps=0
    if [ "$elapsed_ms" -gt 0 ]; then
        eff_qps=$(echo "scale=2; $total_commands * 1000 / $elapsed_ms" | bc -l 2>/dev/null || echo 0)
    fi

    echo "${eff_qps}:${bgsave_count}:${elapsed_ms}"
}

# ---- 主测试函数 ----
run_save_test() {
    local nkeys=$1

    echo ""
    echo "════════════════════════════════════════════════"
    step "数据集: ${nkeys} keys"
    echo "════════════════════════════════════════════════"

    stop_redis
    start_kvs "kvstore-rdb.conf"
    local port
    port=$(grep '^port ' "${CONF_DIR}/kvstore-rdb.conf" | awk '{print $2}')
    port=${port:-$KVS_PORT}

    # 填充数据集
    populate 127.0.0.1 "$port" "$nkeys"

    # ─── 场景 A: 基线（无 SAVE）─────────────────────
    step "场景 A: 基线（无 SAVE）"
    # 使用最大的命令数作为基线测试
    local baseline_n=1000000
    local baseline
    baseline=$(run_bench 127.0.0.1 "$port" set $baseline_n $BENCH_C $BENCH_P $BENCH_D)

    echo "  基线 QPS: ${baseline} (测试命令数: ${baseline_n})"
    echo "${nkeys},baseline,${baseline},0,0.0,${baseline},${baseline_n}" >> "$RESULT_CSV"

    # ─── 场景 C: 固定命令数 BGSAVE ──────────────────
    echo ""
    step "场景 C: 固定命令数 BGSAVE（纯异步）"

    # 表头
    printf "  %-12s | %12s | %12s | %8s | %8s\n" \
        "间隔" "总命令数" "有效QPS" "次数" "QPS降幅"
    printf "  %-12s | %12s | %12s | %8s | %8s\n" \
        "------------" "------------" "------------" "--------" "--------"

    for interval in "${!SAVE_CONFIGS[@]}"; do
        local total_commands="${SAVE_CONFIGS[$interval]}"
        local interval_label
        if [ "$interval" -ge 1000000 ]; then
            interval_label="$((interval / 1000000))M"
        elif [ "$interval" -ge 1000 ]; then
            interval_label="$((interval / 1000))K"
        else
            interval_label="${interval}"
        fi

        # 运行测试
        local result
        result=$(bench_fixed_count_bgsave 127.0.0.1 "$port" "$interval" "$total_commands")

        # 解析结果: effective_qps:bgsave_count:total_ms
        local eff_qps bgsave_count total_ms
        eff_qps=$(echo "$result" | cut -d: -f1)
        bgsave_count=$(echo "$result" | cut -d: -f2)
        total_ms=$(echo "$result" | cut -d: -f3)

        # 计算 QPS 降幅
        local qps_drop
        qps_drop=$(drop_pct "$eff_qps" "$baseline")

        # 输出到屏幕
        printf "  %-12s | %12s | %12s | %8s | %7s%%\n" \
            "$interval_label" "$total_commands" "$eff_qps" "$bgsave_count" "$qps_drop"

        # 输出到 CSV
        echo "${nkeys},${interval_label},${eff_qps},${bgsave_count},${qps_drop},${baseline},${total_commands}" >> "$RESULT_CSV"
    done

    # RDB 文件大小
    local rdb_mb="0.0"
    if [ -f "$RDB_FILE" ]; then
        local rdb_size
        rdb_size=$(stat -c%s "$RDB_FILE" 2>/dev/null || echo 0)
        rdb_mb=$(echo "scale=1; $rdb_size/1048576" | bc -l 2>/dev/null || echo 0.0)
    fi
    echo ""
    echo "  RDB 文件大小: ${rdb_mb} MB"

    stop_kvs
    rm -f "$RDB_FILE" "${PROJECT_DIR}/build/dump.rdb.tmp"
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
  - BGSAVE间隔: 1M, 100K, 10K, 1K (条命令)
  - 数据集: ${KEY_COUNTS[*]} keys

测试场景:
  场景 A: 基线（无 SAVE）— 测量最大 QPS
  场景 C: 固定命令数 BGSAVE — 每 N 条命令触发一次 BGSAVE（纯异步，不等待完成）

=============================================================================

EOF

    # CSV 文件
    echo "keys,interval,effective_qps,bgsave_count,qps_drop_pct,baseline_qps,total_commands" > "$RESULT_CSV"
}

# ---- 生成汇总报告 ----
generate_summary() {
    echo "" | tee -a "$RESULT_FILE"
    echo "════════════════════════════════════════════════" | tee -a "$RESULT_FILE"
    echo "  汇总表" | tee -a "$RESULT_FILE"
    echo "════════════════════════════════════════════════" | tee -a "$RESULT_FILE"
    echo "" | tee -a "$RESULT_FILE"

    # 按数据集分组显示
    for nkeys in "${KEY_COUNTS[@]}"; do
        echo "数据集: ${nkeys} keys" | tee -a "$RESULT_FILE"
        printf "  %-12s | %12s | %12s | %8s | %8s\n" \
            "BGSAVE间隔" "总命令数" "有效QPS" "次数" "QPS降幅" | tee -a "$RESULT_FILE"
        printf "  %-12s | %12s | %12s | %8s | %8s\n" \
            "------------" "------------" "------------" "--------" "--------" | tee -a "$RESULT_FILE"

        # 基线
        local baseline
        baseline=$(grep "^${nkeys},baseline," "$RESULT_CSV" | cut -d, -f3)
        local baseline_n=$(grep "^${nkeys},baseline," "$RESULT_CSV" | cut -d, -f7)
        printf "  %-12s | %12s | %12s | %8s | %8s\n" \
            "基线(无SAVE)" "$baseline_n" "$baseline" "—" "—" | tee -a "$RESULT_FILE"

        # 各个间隔（按间隔大小排序）
        for interval in 1000000 100000 10000 1000; do
            local interval_label
            if [ "$interval" -ge 1000000 ]; then
                interval_label="$((interval / 1000000))M"
            elif [ "$interval" -ge 1000 ]; then
                interval_label="$((interval / 1000))K"
            else
                interval_label="${interval}"
            fi

            local line
            line=$(grep "^${nkeys},${interval_label}," "$RESULT_CSV")
            if [ -n "$line" ]; then
                local eff_qps bgsave_count qps_drop total_cmds
                eff_qps=$(echo "$line" | cut -d, -f3)
                bgsave_count=$(echo "$line" | cut -d, -f4)
                qps_drop=$(echo "$line" | cut -d, -f5)
                total_cmds=$(echo "$line" | cut -d, -f7)

                printf "  %-12s | %12s | %12s | %8s | %7s%%\n" \
                    "$interval_label" "$total_cmds" "$eff_qps" "$bgsave_count" "$qps_drop" | tee -a "$RESULT_FILE"
            fi
        done
        echo "" | tee -a "$RESULT_FILE"
    done

    echo "==============================================================================" | tee -a "$RESULT_FILE"
    echo "" | tee -a "$RESULT_FILE"
}

# ---- 执行测试 ----
init_output

for n in "${KEY_COUNTS[@]}"; do
    run_save_test "$n" | tee -a "$RESULT_FILE"
done

generate_summary

echo ""
log "测试完成！结果已保存："
log "  文本报告: $RESULT_FILE"
log "  CSV数据:  $RESULT_CSV"
echo ""
