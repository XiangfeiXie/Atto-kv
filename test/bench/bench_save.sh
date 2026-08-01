#!/bin/bash
# =============================================================================
# bench_save.sh — kvstore BGSAVE 频率对性能影响测试
#
# 频率-数据集配对（间隔越大 → 数据集越大）：
#   1M 间隔  → 1M keys  (500万命令, ~5次BGSAVE)
#   100K     → 100K keys (100万命令, ~10次BGSAVE)
#   10K      → 10K keys  (50万命令,  ~50次BGSAVE)
#   1K       → 1K keys   (10万命令,  ~100次BGSAVE)
#
# 用法:
#   sudo ./test/bench/bench_save.sh                       # full 维度
#   sudo ./test/bench/bench_save.sh --profile quick       # quick 维度 (2 点)
#
# 输出:
#   - results_save.txt  : 文本报告
#   - results_save.csv  : CSV 数据
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

require_root
require_tools

# ---- 参数 ----
TEST_PROFILE="full"

while [ $# -gt 0 ]; do
    case "$1" in
        --profile) TEST_PROFILE="$2"; shift 2 ;;
        *) echo "用法: $0 [--profile full|quick]"; exit 1 ;;
    esac
done

# ---- 配置 ----
BENCH_C=50; BENCH_P=20; BENCH_D=16

declare -A PAIR=(
    ["1000000"]="1000000:5000000"
    ["100000"]="100000:1000000"
    ["10000"]="10000:500000"
    ["1000"]="1000:100000"
)
INTERVALS_FULL=(1000000 100000 10000 1000)
INTERVALS_QUICK=(100000 10000)
BASELINE_KEYS_FULL=(1000 10000 100000 1000000)
BASELINE_KEYS_QUICK=(10000 100000)

if [ "$TEST_PROFILE" = "quick" ]; then
    INTERVALS=("${INTERVALS_QUICK[@]}")
    BASELINE_KEYS=("${BASELINE_KEYS_QUICK[@]}")
else
    INTERVALS=("${INTERVALS_FULL[@]}")
    BASELINE_KEYS=("${BASELINE_KEYS_FULL[@]}")
fi

RDB_FILE="${PROJECT_DIR}/build/dump.rdb"
RESULT_FILE="${PROJECT_DIR}/test/bench/results_save.txt"
RESULT_CSV="${PROJECT_DIR}/test/bench/results_save.csv"

# ---- 工具函数 ----

drop_pct() {
    local a=$1 b=$2
    if [[ "$a" =~ ^[0-9]+(\.[0-9]+)?$ ]] && \
       [[ "$b" =~ ^[0-9]+(\.[0-9]+)?$ ]] && \
       [ "$(echo "$b > 0" | bc -l 2>/dev/null)" = "1" ]; then
        printf "%.1f" "$(echo "scale=4; (1 - $a/$b)*100" | bc -l 2>/dev/null)"
    else echo "0.0"; fi
}

interval_label() {
    local v=$1
    if   [ "$v" -ge 1000000 ]; then echo "$((v / 1000000))M"
    elif [ "$v" -ge 1000 ];    then echo "$((v / 1000))K"
    else echo "${v}"; fi
}

# ===========================================================================
# BGSAVE 测试核心
# 返回: pure_qps:bgsave_count:bgsave_avg_ms:bgsave_total_ms:pure_set_ms:bgsave_skipped
# ===========================================================================

bench_fixed_count_bgsave() {
    local host=$1 port=$2 interval=$3 total_commands=$4 key_space=$5
    local chunks=$((total_commands / interval))
    [ "$chunks" -lt 1 ] && chunks=1

    local bgsave_count=0 bgsave_skipped=0 bgsave_total_ms=0 pure_set_ms=0
    local poller_tmp poller_pid=0
    poller_tmp=$(mktemp -t bgsave_dur.XXXXXX)

    for i in $(seq 1 $chunks); do
        local t0=$(date +%s%3N)
        redis-benchmark -h "$host" -p "$port" -t set \
            -n "$interval" -c $BENCH_C -P $BENCH_P -d $BENCH_D \
            -r "$key_space" -q >/dev/null 2>&1 || true
        local t1=$(date +%s%3N)
        pure_set_ms=$((pure_set_ms + (t1 - t0)))

        local state
        state=$(redis-cli -h "$host" -p "$port" INFO persistence 2>/dev/null \
            | grep '^rdb_bgsave_in_progress' | cut -d: -f2 | tr -d '\r')

        if [ "$state" = "0" ]; then
            if [ "$poller_pid" -gt 0 ]; then
                wait "$poller_pid" 2>/dev/null
                local d=$(cat "$poller_tmp" 2>/dev/null || echo 0)
                bgsave_total_ms=$((bgsave_total_ms + d))
                poller_pid=0
            fi

            local bg_start=$(date +%s%3N)
            redis-cli -h "$host" -p "$port" BGSAVE >/dev/null 2>&1 || true
            bgsave_count=$((bgsave_count + 1))

            (
                while true; do
                    s=$(redis-cli -h "$host" -p "$port" INFO persistence 2>/dev/null \
                        | grep '^rdb_bgsave_in_progress' | cut -d: -f2 | tr -d '\r')
                    if [ "$s" = "0" ]; then
                        echo $(( $(date +%s%3N) - bg_start ))
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

    if [ "$poller_pid" -gt 0 ]; then
        wait "$poller_pid" 2>/dev/null
        local d=$(cat "$poller_tmp" 2>/dev/null || echo 0)
        bgsave_total_ms=$((bgsave_total_ms + d))
    fi
    rm -f "$poller_tmp"

    local pure_qps=0
    if [ "$pure_set_ms" -gt 0 ]; then
        pure_qps=$(echo "scale=2; $total_commands * 1000 / $pure_set_ms" | bc -l 2>/dev/null || echo 0)
    fi
    local bgsave_avg_ms=0
    if [ "$bgsave_count" -gt 0 ]; then
        bgsave_avg_ms=$((bgsave_total_ms / bgsave_count))
    fi

    [ "$bgsave_skipped" -gt 0 ] && echo "[INFO] ${bgsave_skipped}/${chunks} skipped (in progress)" >&2
    echo "${pure_qps}:${bgsave_count}:${bgsave_avg_ms}:${bgsave_total_ms}:${pure_set_ms}:${bgsave_skipped}"
}

# ---- 基线 ----
run_baseline() {
    local nkeys=$1
    echo ""
    echo "════════════════════════════════════════════════"
    step "基线: ${nkeys} keys（无 BGSAVE）"
    echo "════════════════════════════════════════════════"

    stop_kvs
    start_kvs "kvstore-rdb.conf"
    local port=$(grep '^port ' "${CONF_DIR}/kvstore-rdb.conf" | awk '{print $2}')
    port=${port:-$KVS_PORT}

    populate 127.0.0.1 "$port" "$nkeys"

    step "执行基线压测 (1M 命令)..."
    local baseline_n=1000000
    local baseline
    baseline=$(run_bench_r 127.0.0.1 "$port" set $baseline_n $BENCH_C $BENCH_P $BENCH_D "$nkeys")

    echo "  keys=${nkeys}  基线 QPS: ${baseline}"
    echo "${nkeys},baseline,${baseline},0,0,0,0.0,${baseline},${baseline_n},0,0" >> "$RESULT_CSV"

    stop_kvs
    rm -f "$RDB_FILE" "${PROJECT_DIR}/build/dump.rdb.tmp"
}

# ---- BGSAVE 配对测试 ----
run_bgsave_pair() {
    local nkeys=$1 interval=$2 total_commands=$3 baseline=$4
    local ilabel=$(interval_label "$interval")

    start_kvs "kvstore-rdb.conf"
    local port=$(grep '^port ' "${CONF_DIR}/kvstore-rdb.conf" | awk '{print $2}')
    port=${port:-$KVS_PORT}
    populate 127.0.0.1 "$port" "$nkeys"

    local result
    result=$(bench_fixed_count_bgsave 127.0.0.1 "$port" "$interval" "$total_commands" "$nkeys")

    local pure_qps bgsave_count bgsave_avg_ms bgsave_total_ms pure_set_ms bgsave_skipped
    pure_qps=$(echo       "$result" | cut -d: -f1)
    bgsave_count=$(echo   "$result" | cut -d: -f2)
    bgsave_avg_ms=$(echo  "$result" | cut -d: -f3)
    bgsave_total_ms=$(echo "$result" | cut -d: -f4)
    pure_set_ms=$(echo    "$result" | cut -d: -f5)
    bgsave_skipped=$(echo "$result" | cut -d: -f6)

    local qps_drop=$(drop_pct "$pure_qps" "$baseline")

    printf "  %-8s | %6s keys | %10s | %10s | %5s | %5s | %8s | %7s%%\n" \
        "$ilabel" "$nkeys" "$total_commands" "$pure_qps" "$bgsave_count" \
        "$bgsave_skipped" "$bgsave_avg_ms" "$qps_drop"

    echo "${nkeys},${ilabel},${pure_qps},${bgsave_count},${bgsave_avg_ms},${bgsave_total_ms},${qps_drop},${baseline},${total_commands},${pure_set_ms},${bgsave_skipped}" >> "$RESULT_CSV"

    local rdb_mb="0.0"
    if [ -f "$RDB_FILE" ]; then
        local sz=$(stat -c%s "$RDB_FILE" 2>/dev/null || echo 0)
        rdb_mb=$(echo "scale=1; $sz/1048576" | bc -l 2>/dev/null || echo 0.0)
    fi
    echo "    RDB: ${rdb_mb} MB | BGSAVE 总耗时: ${bgsave_total_ms} ms | SET 纯耗时: ${pure_set_ms} ms"

    stop_kvs
    rm -f "$RDB_FILE" "${PROJECT_DIR}/build/dump.rdb.tmp"
    sleep 1
}

# ---- 输出 ----
init_output() {
    mkdir -p "$(dirname "$RESULT_FILE")"

    cat > "$RESULT_FILE" << EOF
=============================================================================
kvstore BGSAVE 频率对性能影响测试
=============================================================================
测试时间: $(date '+%Y-%m-%d %H:%M:%S')
维度: ${TEST_PROFILE}
参数: 并发=${BENCH_C}  Pipeline=${BENCH_P}  Value=${BENCH_D}B

频率-数据集配对:
  1M 间隔 → 1M keys (~5次BGSAVE)    100K → 100K keys (~10次)
  10K    → 10K keys (~50次)         1K   → 1K keys  (~100次)

QPS 计算: 总命令数 / sum(chunk 耗时), BGSAVE 后台等待不计入

=============================================================================

EOF

    echo "keys,interval,pure_qps,bgsave_count,bgsave_avg_ms,bgsave_total_ms,qps_drop_pct,baseline_qps,total_commands,pure_set_ms,bgsave_skipped" > "$RESULT_CSV"
}

generate_summary() {
    {
        echo ""
        echo "════════════════════════════════════════════════"
        echo "  汇总表  (profile=${TEST_PROFILE})"
        echo "════════════════════════════════════════════════"
        echo ""

        echo "【基线 QPS】"
        printf "  %-12s | %10s | %12s\n" "keys" "命令数" "QPS"
        printf "  %-12s | %10s | %12s\n" "------------" "----------" "------------"
        for nkeys in "${BASELINE_KEYS[@]}"; do
            local line=$(grep "^${nkeys},baseline," "$RESULT_CSV" | tail -1)
            if [ -n "$line" ]; then
                printf "  %-12s | %10s | %12s\n" \
                    "$nkeys" "$(echo "$line" | cut -d, -f9)" "$(echo "$line" | cut -d, -f3)"
            fi
        done

        echo ""
        echo "【BGSAVE 频率影响】"
        printf "  %-8s | %8s | %10s | %10s | %5s | %5s | %8s | %8s\n" \
            "间隔" "keys" "总命令" "纯SET QPS" "执行" "跳过" "avg(ms)" "降幅"
        printf "  %-8s | %8s | %10s | %10s | %5s | %5s | %8s | %8s\n" \
            "--------" "--------" "----------" "----------" "-----" "-----" "--------" "--------"

        for interval in "${INTERVALS[@]}"; do
            local ilabel=$(interval_label "$interval")
            local pair="${PAIR[$interval]}"; local nkeys="${pair%%:*}"

            local line=$(grep "^${nkeys},${ilabel}," "$RESULT_CSV" | grep -v baseline | tail -1)
            if [ -n "$line" ]; then
                local pq=$(echo "$line" | cut -d, -f3)
                local bc=$(echo "$line" | cut -d, -f4)
                local ba=$(echo "$line" | cut -d, -f5)
                local tc=$(echo "$line" | cut -d, -f9)
                local bs=$(echo "$line" | cut -d, -f11)
                local qd=$(echo "$line" | cut -d, -f7)
                printf "  %-8s | %8s | %10s | %10s | %5s | %5s | %8s | %7s%%\n" \
                    "$ilabel" "$nkeys" "$tc" "$pq" "$bc" "$bs" "$ba" "$qd"
            fi
        done

        echo ""
        echo "=============================================================================="
        echo ""
    } | tee -a "$RESULT_FILE"
}

# ---- 主流程 ----
init_output

declare -A BASELINE_QPS

echo ""
step "═══ 第一阶段: 基线测试 ═══"
for nkeys in "${BASELINE_KEYS[@]}"; do
    run_baseline "$nkeys" | tee -a "$RESULT_FILE"
    bq=$(grep "^${nkeys},baseline," "$RESULT_CSV" | tail -1 | cut -d, -f3)
    BASELINE_QPS["$nkeys"]="${bq:-0}"
done

echo ""
step "═══ 第二阶段: BGSAVE 频率测试 ═══"
printf "  %-8s | %8s | %10s | %10s | %5s | %5s | %8s | %8s\n" \
    "间隔" "keys" "总命令" "纯SET QPS" "执行" "跳过" "avg(ms)" "降幅"
printf "  %-8s | %8s | %10s | %10s | %5s | %5s | %8s | %8s\n" \
    "--------" "--------" "----------" "----------" "-----" "-----" "--------" "--------"

for interval in "${INTERVALS[@]}"; do
    pair="${PAIR[$interval]}"
    nkeys="${pair%%:*}"
    total_cmds="${pair##*:}"
    baseline="${BASELINE_QPS[$nkeys]}"
    run_bgsave_pair "$nkeys" "$interval" "$total_cmds" "$baseline" | tee -a "$RESULT_FILE"
done

generate_summary

echo ""
log "测试完成！"
log "  报告: $RESULT_FILE"
log "  CSV:  $RESULT_CSV"
echo ""
