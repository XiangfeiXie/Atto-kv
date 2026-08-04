#!/bin/bash
# =============================================================================
# bench_bgsave_redis.sh — Redis BGSAVE 频率影响测试
# 固定数据集 + 每间隔独立实例
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

require_root
require_tools

PROFILE="full"
while [ $# -gt 0 ]; do
    case "$1" in
        --profile) PROFILE="$2"; shift 2 ;;
        *) echo "用法: $0 [--profile full|quick]"; exit 1 ;;
    esac
done

BENCH_C=50; BENCH_P=20; BENCH_D=16; WARMUP_CMDS=200000; BASELINE_CMDS=1000000
FIXED_NKEYS=100000
REDIS_HOST=127.0.0.1; REDIS_PORT=6379

declare -A PAIR=(
    ["1000000"]="5000000" ["100000"]="1000000"
    ["10000"]="500000"   ["1000"]="100000"
)
declare -A MIN_SLEEP=(
    ["1000000"]="1.0" ["100000"]="0.2" ["10000"]="0.05" ["1000"]="0.01"
)

INTERVALS_FULL=(1000000 100000 10000 1000)
INTERVALS_QUICK=(100000 1000)

if [ "$PROFILE" = "quick" ]; then
    INTERVALS=("${INTERVALS_QUICK[@]}")
else
    INTERVALS=("${INTERVALS_FULL[@]}")
fi

RDB_FILE="/var/lib/redis/dump.rdb"
RESULT_FILE="${PROJECT_DIR}/test/bench/results_bgsave_redis.txt"
RESULT_CSV="${PROJECT_DIR}/test/bench/results_bgsave_redis.csv"

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

start_redis_clean() {
    stop_redis
    start_redis
    redis-cli -p "$REDIS_PORT" CONFIG SET save "" >/dev/null
    redis-cli -p "$REDIS_PORT" CONFIG SET appendonly no >/dev/null
    redis-cli -p "$REDIS_PORT" FLUSHALL >/dev/null
    log "Redis: ready"
}

wait_bgsave() {
    local timeout=${1:-60}
    local start=$(date +%s)
    while true; do
        local s=$(redis-cli -p "$REDIS_PORT" INFO persistence 2>/dev/null \
            | grep 'rdb_bgsave_in_progress' | cut -d: -f2 | tr -d '\r')
        [ "$s" = "0" ] && return 0
        [ $(($(date +%s) - start)) -ge "$timeout" ] && return 1
        sleep 0.1
    done
}

populate_keys() {
    local n=$1
    local oversample=$((n * 10))
    redis-benchmark -h "$REDIS_HOST" -p "$REDIS_PORT" -t set \
        -n "$oversample" -c 50 -P 20 -d 16 -r "$n" -q >/dev/null 2>&1 || true
}

get_stat() {
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" INFO "$1" 2>/dev/null | grep "$2" | cut -d: -f2 | tr -d '\r'
}

run_benchmark() {
    local nkeys=$1 ncmds=$2
    local cmds_before=$(get_stat stats 'total_commands_processed')
    cmds_before=${cmds_before:-0}
    local t0=$(date +%s%3N)

    local output
    output=$(redis-benchmark -h "$REDIS_HOST" -p "$REDIS_PORT" -t set \
        -n "$ncmds" -c "$BENCH_C" -P "$BENCH_P" -d "$BENCH_D" \
        -r "$nkeys" --csv 2>/dev/null)

    local t1=$(date +%s%3N)
    local cmds_after=$(get_stat stats 'total_commands_processed')
    cmds_after=${cmds_after:-0}

    local server_cmds=$((cmds_after - cmds_before))
    local duration_ms=$((t1 - t0))
    local server_qps=0
    if [ "$duration_ms" -gt 0 ]; then
        server_qps=$(echo "scale=2; $server_cmds * 1000 / $duration_ms" | bc -l 2>/dev/null || echo 0)
    fi

    local csv_data=$(echo "$output" | grep -E '^"SET"' | tail -1)
    local p50=$(echo "$csv_data" | cut -d',' -f5 | tr -d '"')
    local p99=$(echo "$csv_data" | cut -d',' -f7 | tr -d '"')
    p50=${p50:-0}; p99=${p99:-0}
    echo "${server_qps}:${p50}:${p99}:${duration_ms}"
}

run_one_interval() {
    local interval=$1 total_cmds=$2
    local ilabel=$(interval_label "$interval")

    echo ""
    echo "════════════════════════════════════════════════"
    step "${ilabel} interval — 独立实例（${FIXED_NKEYS} keys）"
    echo "════════════════════════════════════════════════"

    start_redis_clean
    populate_keys "$FIXED_NKEYS"
    local dbsize=$(redis-cli -p "$REDIS_PORT" DBSIZE 2>/dev/null)
    log "DBSIZE=${dbsize:-0}, 预热 ${WARMUP_CMDS} SETs..."
    redis-benchmark -h "$REDIS_HOST" -p "$REDIS_PORT" -t set \
        -n "$WARMUP_CMDS" -c "$BENCH_C" -P "$BENCH_P" -d "$BENCH_D" \
        -r "$FIXED_NKEYS" -q >/dev/null 2>&1 || true

    # 基线
    log "基线: ${BASELINE_CMDS} 条 SET..."
    local bl_result=$(run_benchmark "$FIXED_NKEYS" "$BASELINE_CMDS")
    bl_qps=$(echo "$bl_result" | cut -d: -f1)
    bl_p50=$(echo "$bl_result" | cut -d: -f2)
    bl_p99=$(echo "$bl_result" | cut -d: -f3)
    bl_dur=$(echo "$bl_result" | cut -d: -f4)
    log "  基线: QPS=${bl_qps:-0}  p50=${bl_p50:-0}ms  p99=${bl_p99:-0}ms"

    # trigger_sleep
    local trigger_sleep=0.5
    if [ "$(echo "${bl_qps:-0} > 0" | bc -l 2>/dev/null)" = "1" ]; then
        trigger_sleep=$(echo "scale=3; $interval / ${bl_qps:-1}" | bc -l 2>/dev/null)
    fi
    local min_sleep="${MIN_SLEEP[$interval]:-0.05}"
    if [ "$(echo "$trigger_sleep < $min_sleep" | bc -l 2>/dev/null)" = "1" ]; then
        trigger_sleep="$min_sleep"
    fi

    # BGSAVE 触发器（命令返回值统计）
    local trig_log=$(mktemp -t redis_bgsave_trig.XXXXXX)
    local skip_log=$(mktemp -t redis_bgsave_skip.XXXXXX)
    echo 0 > "$trig_log"; echo 0 > "$skip_log"
    (
        local wd=0.3
        [ "$(echo "$trigger_sleep < 0.1" | bc -l 2>/dev/null)" = "1" ] && wd=0.05
        sleep "$wd"
        while true; do
            local resp
            resp=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" BGSAVE 2>&1 || true)
            if echo "$resp" | grep -qi "started"; then
                local e=$(cat "$trig_log" 2>/dev/null || echo 0)
                echo $((e + 1)) > "$trig_log"
            else
                local s=$(cat "$skip_log" 2>/dev/null || echo 0)
                echo $((s + 1)) > "$skip_log"
            fi
            sleep "$trigger_sleep" 2>/dev/null || sleep 0.05
        done
    ) &
    local trig_pid=$!
    disown "$trig_pid" 2>/dev/null || true

    # BGSAVE 压测
    log "BGSAVE: trigger_every=${trigger_sleep}s, ${total_cmds} 条 SET..."
    local bg_result=$(run_benchmark "$FIXED_NKEYS" "$total_cmds")
    bg_qps=$(echo "$bg_result" | cut -d: -f1)
    bg_p50=$(echo "$bg_result" | cut -d: -f2)
    bg_p99=$(echo "$bg_result" | cut -d: -f3)
    bg_dur=$(echo "$bg_result" | cut -d: -f4)

    kill "$trig_pid" 2>/dev/null || true
    sleep 0.3
    kill -9 "$trig_pid" 2>/dev/null || true
    local bgsave_exec=$(cat "$trig_log" 2>/dev/null || echo 0)
    local bgsave_skip=$(cat "$skip_log" 2>/dev/null || echo 0)
    rm -f "$trig_log" "$skip_log"

    wait_bgsave 60 || log "WARN: BGSAVE timeout"
    local trig_cnt=$((bgsave_exec + bgsave_skip))
    local qps_drop=$(drop_pct "${bg_qps:-0}" "${bl_qps:-1}")

    local rdb_kb="0"
    if [ -f "$RDB_FILE" ]; then
        local sz=$(stat -c%s "$RDB_FILE" 2>/dev/null || echo 0)
        rdb_kb=$(echo "scale=1; $sz/1024" | bc -l 2>/dev/null || echo 0)
    fi

    printf "  %-8s | exec=%-3s skip=%-3s trig=%-3s | base_QPS=%-10s | bg_QPS=%-10s | p50=%-5s/%-5s | p99=%-5s/%-5s | drop=%-6s%% | RDB=%sK\n" \
        "$ilabel" "$bgsave_exec" "$bgsave_skip" "$trig_cnt" \
        "${bl_qps:-0}" "${bg_qps:-0}" \
        "${bl_p50:-0}ms" "${bg_p50:-0}ms" "${bl_p99:-0}ms" "${bg_p99:-0}ms" \
        "$qps_drop" "$rdb_kb"

    echo "${FIXED_NKEYS},${ilabel},${bl_qps:-0},${bl_p50:-0},${bl_p99:-0},${bg_qps:-0},${bg_p50:-0},${bg_p99:-0},${trigger_sleep},${bgsave_exec},${bgsave_skip},${trig_cnt},${qps_drop},${bl_dur:-0},${bg_dur:-0},${rdb_kb},${total_cmds}" >> "$RESULT_CSV"

    stop_redis
    rm -f "$RDB_FILE"
    sleep 1
}

init_output() {
    mkdir -p "$(dirname "$RESULT_FILE")"
    local redis_ver=$(redis-cli -p "$REDIS_PORT" INFO server 2>/dev/null | grep '^redis_version' | cut -d: -f2 | tr -d '\r' || echo '?')
    cat > "$RESULT_FILE" << EOF
=============================================================================
Redis BGSAVE 频率影响测试  (profile=${PROFILE})
=============================================================================
测试时间: $(date '+%Y-%m-%d %H:%M:%S')
Redis: ${redis_ver}
参数: 并发=${BENCH_C}  Pipeline=${BENCH_P}  Value=${BENCH_D}B  数据集=${FIXED_NKEYS} keys (固定)
预热=${WARMUP_CMDS}  基线命令=${BASELINE_CMDS}

每间隔独立实例：populate → warmup → 基线 → BGSAVE
唯一变量：BGSAVE 触发频率 (${FIXED_NKEYS} keys 固定)

=============================================================================

EOF
    echo "keys,interval,bl_qps,bl_p50,bl_p99,bg_qps,bg_p50,bg_p99,trigger_sleep,bgsave_exec,bgsave_skip,trigger_count,qps_drop_pct,bl_dur_ms,bg_dur_ms,rdb_kb,total_cmds" > "$RESULT_CSV"
}

generate_summary() {
    {
        echo ""
        echo "════════════════════════════════════════════════"
        echo "  Redis 汇总表  (${FIXED_NKEYS} keys 固定, profile=${PROFILE})"
        echo "════════════════════════════════════════════════"
        echo ""
        echo "  间隔   | 执行 | 跳过 | 触发 |  基线 QPS | BGSAVE QPS |  基线p50 | BGp50 |  基线p99 | BGp99 |  降幅% |  RDB(K)"
        echo "  -------- | ---- | ---- | ---- | ---------- | ----------- | -------- | ----- | -------- | ----- | ------ | ------"

        for interval in "${INTERVALS[@]}"; do
            local ilabel=$(interval_label "$interval")
            local line=$(grep "^${FIXED_NKEYS},${ilabel}," "$RESULT_CSV" | tail -1)
            if [ -n "$line" ]; then
                printf "  %-8s | %4s | %4s | %4s | %10s | %11s | %7sms | %5sms | %8sms | %5sms | %6s%% | %6sK\n" \
                    "$ilabel" \
                    "$(echo "$line" | cut -d, -f10)" "$(echo "$line" | cut -d, -f11)" \
                    "$(echo "$line" | cut -d, -f12)" \
                    "$(echo "$line" | cut -d, -f3)"  "$(echo "$line" | cut -d, -f6)" \
                    "$(echo "$line" | cut -d, -f4)"  "$(echo "$line" | cut -d, -f7)" \
                    "$(echo "$line" | cut -d, -f5)"  "$(echo "$line" | cut -d, -f8)" \
                    "$(echo "$line" | cut -d, -f13)" "$(echo "$line" | cut -d, -f16)"
            fi
        done

        echo ""
        echo "=============================================================================="
        echo ""
    } | tee -a "$RESULT_FILE"
}

# ---- 主流程 ----
init_output

echo ""
echo "════════════════════════════════════════════════"
step "Redis BGSAVE 频率测试 — ${FIXED_NKEYS} keys 固定, 每间隔独立实例"
echo "════════════════════════════════════════════════"

for interval in "${INTERVALS[@]}"; do
    total_cmds="${PAIR[$interval]}"
    run_one_interval "$interval" "$total_cmds" | tee -a "$RESULT_FILE"
done

generate_summary

echo ""
log "测试完成！"
log "  报告: $RESULT_FILE"
log "  CSV:  $RESULT_CSV"
echo ""
