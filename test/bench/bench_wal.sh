#!/bin/bash
# =============================================================================
# bench_wal.sh — WAL/AOF 持久化性能对比（6 维度独立测试）
#
# 测试维度:
#   T1: kvstore PING (echo)  — 纯 echo 吞吐
#   T2: kvstore AOF OFF      — persistence=none, SET+GET pipeline
#   T3: kvstore AOF ON       — persistence=aof,  SET+GET pipeline
#   T4: Redis   PING (echo)  — 纯 echo 吞吐
#   T5: Redis   AOF OFF      — appendonly=no, SET+GET pipeline
#   T6: Redis   AOF ON       — appendonly=yes, SET+GET pipeline
#
# 用法:
#   sudo ./test/bench/bench_wal.sh
#
# 输出:
#   终端彩色表格 + test/bench/results_wal.txt + test/bench/results_wal.csv
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

require_root
require_tools

# ---- 压测参数 ----
BENCH_N=1000000
BENCH_C=50
WARMUP_N=200000
PIPELINES=(1 10 20 40 80 160)

# ---- 输出文件 ----
RESULT_TXT="${PROJECT_DIR}/test/bench/results_wal.txt"
RESULT_CSV="${PROJECT_DIR}/test/bench/results_wal.csv"

# ---- 安全除法 ----
safe_ratio() {
    local a=$1 b=$2
    if [[ "$a" =~ ^[0-9]+(\.[0-9]+)?$ ]] && \
       [[ "$b" =~ ^[0-9]+(\.[0-9]+)?$ ]] && \
       [ "$(echo "$b > 0" | bc -l 2>/dev/null)" = "1" ]; then
        printf "%.2fx" "$(echo "scale=4; $a / $b" | bc -l 2>/dev/null)"
    else
        echo "N/A"
    fi
}

# ---- 结果存储（关联数组） ----
declare -A KVS_AOF_OFF_SET  KVS_AOF_ON_SET  KVS_AOF_OFF_GET  KVS_AOF_ON_GET
declare -A REDIS_AOF_OFF_SET REDIS_AOF_ON_SET REDIS_AOF_OFF_GET REDIS_AOF_ON_GET
KVS_PING_QPS=0
REDIS_PING_QPS=0

# ---- PING 测试 ----
run_ping_test() {
    local host=$1 port=$2 label=$3 qps="0" retry=0
    while [ $retry -lt 2 ]; do
        local csv_line
        csv_line=$(redis-benchmark -h "$host" -p "$port" -t ping \
            -n $BENCH_N -c $BENCH_C --csv 2>/dev/null | tail -1)
        if [ -n "$csv_line" ]; then
            qps=$(echo "$csv_line" | cut -d',' -f2 | tr -d '"')
            if [[ "$qps" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
                echo "$qps"
                return 0
            fi
        fi
        retry=$((retry + 1))
        [ $retry -lt 2 ] && sleep 0.5
    done
    echo "0"
}

# ---- 预热函数（写入 WARMUP_N 个 key）----
warmup_keys() {
    local host=$1 port=$2 P=$3
    redis-benchmark -h "$host" -p "$port" -t set \
        -n $WARMUP_N -c $BENCH_C -P "$P" -d 16 -q >/dev/null 2>&1 || true
}

# ---- 初始化结果文件 ----
init_wal_results() {
    echo "# WAL/AOF 持久化性能对比 — $(date '+%Y-%m-%d %H:%M:%S')" > "$RESULT_TXT"
    echo "压测参数: redis-benchmark -n $BENCH_N -c $BENCH_C -P <depth> -d 16" >> "$RESULT_TXT"
    echo "" >> "$RESULT_TXT"
    echo "test,system,aof,qps" > "$RESULT_CSV"
}

record_wal() {
    local test=$1 system=$2 aof=$3 qps=$4
    echo "${test} | ${system} AOF=${aof}: ${qps} req/s" >> "$RESULT_TXT"
    echo "${test},${system},${aof},${qps}" >> "$RESULT_CSV"
}

# ═══════════════════════════════════════════════════════════════════════════
# T1: kvstore PING
# ═══════════════════════════════════════════════════════════════════════════
test_t1_kvs_ping() {
    echo ""
    echo "════════════════════════════════════════════════"
    step "T1  kvstore PING (echo) — 纯 echo 吞吐"
    echo "════════════════════════════════════════════════"

    stop_redis
    start_kvs "kvstore-none.conf"

    local qps
    qps=$(run_ping_test 127.0.0.1 "$KVS_PORT" "kvstore-PING")
    KVS_PING_QPS=$qps
    record_wal "PING" "kvstore" "N/A" "$qps"

    echo -e "  ${GREEN}→ kvstore PING: ${qps} req/s${NC}"

    stop_kvs
}

# ═══════════════════════════════════════════════════════════════════════════
# T2: kvstore AOF OFF — pipeline SET + GET
# ═══════════════════════════════════════════════════════════════════════════
test_t2_kvs_aof_off() {
    echo ""
    echo "════════════════════════════════════════════════"
    step "T2  kvstore AOF OFF — pipeline SET + GET"
    echo "════════════════════════════════════════════════"

    stop_redis

    for P in "${PIPELINES[@]}"; do
        # Fresh kvstore + clean WAL for each P (isolates I/O effects)
        start_kvs "kvstore-none.conf"
        rm -f "${PROJECT_DIR}/build/incr.wal"

        # Warmup to eliminate cold-start bias (200K ops, not recorded)
        redis-benchmark -h 127.0.0.1 -p "$KVS_PORT" -t set \
            -n $WARMUP_N -c $BENCH_C -P 16 -d 16 -q >/dev/null 2>&1 || true

        # SET
        local s_qps
        s_qps=$(run_bench 127.0.0.1 "$KVS_PORT" set $BENCH_N $BENCH_C "$P" 16)
        KVS_AOF_OFF_SET[$P]=$s_qps
        record_wal "Pipeline-${P}-SET" "kvstore" "off" "$s_qps"
        echo -e "  P=${P} SET  → ${GREEN}${s_qps} req/s${NC}"

        # Warmup + GET
        warmup_keys 127.0.0.1 "$KVS_PORT" "$P"
        local g_qps
        g_qps=$(run_bench 127.0.0.1 "$KVS_PORT" get $BENCH_N $BENCH_C "$P" 16)
        KVS_AOF_OFF_GET[$P]=$g_qps
        record_wal "Pipeline-${P}-GET" "kvstore" "off" "$g_qps"
        echo -e "  P=${P} GET  → ${GREEN}${g_qps} req/s${NC}"

        stop_kvs
    done
}

# ═══════════════════════════════════════════════════════════════════════════
# T3: kvstore AOF ON — pipeline SET + GET
# ═══════════════════════════════════════════════════════════════════════════
test_t3_kvs_aof_on() {
    echo ""
    echo "════════════════════════════════════════════════"
    step "T3  kvstore AOF ON — pipeline SET + GET"
    echo "════════════════════════════════════════════════"

    stop_redis

    for P in "${PIPELINES[@]}"; do
        # Fresh kvstore + clean WAL for each P (isolates I/O effects)
        start_kvs "kvstore-aof.conf"
        rm -f "${PROJECT_DIR}/build/incr.wal"

        # Warmup to eliminate cold-start bias (200K ops, not recorded)
        redis-benchmark -h 127.0.0.1 -p "$KVS_PORT" -t set \
            -n $WARMUP_N -c $BENCH_C -P 16 -d 16 -q >/dev/null 2>&1 || true

        # SET
        local s_qps
        s_qps=$(run_bench 127.0.0.1 "$KVS_PORT" set $BENCH_N $BENCH_C "$P" 16)
        KVS_AOF_ON_SET[$P]=$s_qps
        record_wal "Pipeline-${P}-SET" "kvstore" "on" "$s_qps"
        echo -e "  P=${P} SET  → ${GREEN}${s_qps} req/s${NC}"

        # Warmup + GET
        warmup_keys 127.0.0.1 "$KVS_PORT" "$P"
        local g_qps
        g_qps=$(run_bench 127.0.0.1 "$KVS_PORT" get $BENCH_N $BENCH_C "$P" 16)
        KVS_AOF_ON_GET[$P]=$g_qps
        record_wal "Pipeline-${P}-GET" "kvstore" "on" "$g_qps"
        echo -e "  P=${P} GET  → ${GREEN}${g_qps} req/s${NC}"

        stop_kvs
    done
}

# ═══════════════════════════════════════════════════════════════════════════
# T4: Redis PING
# ═══════════════════════════════════════════════════════════════════════════
test_t4_redis_ping() {
    echo ""
    echo "════════════════════════════════════════════════"
    step "T4  Redis PING (echo) — 纯 echo 吞吐"
    echo "════════════════════════════════════════════════"

    stop_kvs
    stop_redis
    start_redis
    redis_configure none

    local qps
    qps=$(run_ping_test 127.0.0.1 "$REDIS_PORT" "Redis-PING")
    REDIS_PING_QPS=$qps
    record_wal "PING" "redis" "N/A" "$qps"

    echo -e "  ${GREEN}→ Redis PING: ${qps} req/s${NC}"

    stop_redis
}

# ═══════════════════════════════════════════════════════════════════════════
# T5: Redis AOF OFF — pipeline SET + GET
# ═══════════════════════════════════════════════════════════════════════════
test_t5_redis_aof_off() {
    echo ""
    echo "════════════════════════════════════════════════"
    step "T5  Redis AOF OFF — pipeline SET + GET"
    echo "════════════════════════════════════════════════"

    stop_kvs

    for P in "${PIPELINES[@]}"; do
        # Fresh Redis + clean AOF for each P (isolates I/O effects)
        stop_redis
        start_redis
        redis_configure none

        # Warmup to eliminate cold-start bias
        warmup_keys 127.0.0.1 "$REDIS_PORT" 10

        # SET
        local s_qps
        s_qps=$(run_bench 127.0.0.1 "$REDIS_PORT" set $BENCH_N $BENCH_C "$P" 16)
        REDIS_AOF_OFF_SET[$P]=$s_qps
        record_wal "Pipeline-${P}-SET" "redis" "off" "$s_qps"
        echo -e "  P=${P} SET  → ${GREEN}${s_qps} req/s${NC}"

        # Warmup + GET
        warmup_keys 127.0.0.1 "$REDIS_PORT" "$P"
        local g_qps
        g_qps=$(run_bench 127.0.0.1 "$REDIS_PORT" get $BENCH_N $BENCH_C "$P" 16)
        REDIS_AOF_OFF_GET[$P]=$g_qps
        record_wal "Pipeline-${P}-GET" "redis" "off" "$g_qps"
        echo -e "  P=${P} GET  → ${GREEN}${g_qps} req/s${NC}"
    done

    stop_redis
}

# ═══════════════════════════════════════════════════════════════════════════
# T6: Redis AOF ON — pipeline SET + GET
# ═══════════════════════════════════════════════════════════════════════════
test_t6_redis_aof_on() {
    echo ""
    echo "════════════════════════════════════════════════"
    step "T6  Redis AOF ON — pipeline SET + GET"
    echo "════════════════════════════════════════════════"

    stop_kvs

    for P in "${PIPELINES[@]}"; do
        # Fresh Redis + clean AOF for each P (isolates I/O effects)
        stop_redis
        start_redis
        redis_configure aof

        # Warmup to eliminate cold-start bias
        warmup_keys 127.0.0.1 "$REDIS_PORT" 10

        # SET
        local s_qps
        s_qps=$(run_bench 127.0.0.1 "$REDIS_PORT" set $BENCH_N $BENCH_C "$P" 16)
        REDIS_AOF_ON_SET[$P]=$s_qps
        record_wal "Pipeline-${P}-SET" "redis" "on" "$s_qps"
        echo -e "  P=${P} SET  → ${GREEN}${s_qps} req/s${NC}"

        # Warmup + GET
        warmup_keys 127.0.0.1 "$REDIS_PORT" "$P"
        local g_qps
        g_qps=$(run_bench 127.0.0.1 "$REDIS_PORT" get $BENCH_N $BENCH_C "$P" 16)
        REDIS_AOF_ON_GET[$P]=$g_qps
        record_wal "Pipeline-${P}-GET" "redis" "on" "$g_qps"
        echo -e "  P=${P} GET  → ${GREEN}${g_qps} req/s${NC}"
    done

    stop_redis
}

# ═══════════════════════════════════════════════════════════════════════════
# 打印汇总表格
# ═══════════════════════════════════════════════════════════════════════════
print_summary() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════════════════════╗"
    echo "║                      WAL / AOF 持久化性能对比                            ║"
    echo "╚══════════════════════════════════════════════════════════════════════════╝"
    echo ""

    # ── PING 对比 ──
    echo "── [PING (echo) 吞吐对比] ──"
    printf "  %-20s | %12s\n" "系统" "PING QPS"
    printf "  %-20s | %12s\n" "--------------------" "------------"
    printf "  %-20s | %12s\n" "kvstore" "$KVS_PING_QPS"
    printf "  %-20s | %12s\n" "Redis"   "$REDIS_PING_QPS"
    printf "  %-20s | %12s\n" "比值 (kvs/redis)" "$(safe_ratio "$KVS_PING_QPS" "$REDIS_PING_QPS")"
    echo ""

    {
        echo "── [PING (echo) 吞吐对比] ──"
        printf "  %-20s | %12s\n" "系统" "PING QPS"
        printf "  %-20s | %12s\n" "kvstore" "$KVS_PING_QPS"
        printf "  %-20s | %12s\n" "Redis"   "$REDIS_PING_QPS"
        printf "  %-20s | %12s\n\n" "比值 (kvs/redis)" "$(safe_ratio "$KVS_PING_QPS" "$REDIS_PING_QPS")"
    } >> "$RESULT_TXT"

    # ── SET 对比表 ──
    echo "── [SET 吞吐对比 (req/s)] ──"
    HDR=$(printf "  %-6s | %14s | %14s | %10s | %14s | %14s | %10s" \
        "P" "kvs-AOF-off" "kvs-AOF-on" "AOF开销" "redis-AOF-off" "redis-AOF-on" "AOF开销")
    SEP=$(printf "  %-6s | %14s | %14s | %10s | %14s | %14s | %10s" \
        "------" "--------------" "--------------" "----------" "--------------" "--------------" "----------")
    echo "$HDR"
    echo "$SEP"
    {
        echo "$HDR"
        echo "$SEP"
    } >> "$RESULT_TXT"

    for P in "${PIPELINES[@]}"; do
        local kvs_off=${KVS_AOF_OFF_SET[$P]:-0}
        local kvs_on=${KVS_AOF_ON_SET[$P]:-0}
        local red_off=${REDIS_AOF_OFF_SET[$P]:-0}
        local red_on=${REDIS_AOF_ON_SET[$P]:-0}

        # AOF 开销 = (1 - on/off) * 100%
        local kvs_cost kvs_cost_str red_cost red_cost_str
        if [ "$(echo "$kvs_off > 0" | bc -l 2>/dev/null)" = "1" ]; then
            kvs_cost=$(echo "scale=2; (1 - $kvs_on/$kvs_off)*100" | bc -l 2>/dev/null || echo "N/A")
            kvs_cost_str="${kvs_cost}%"
        else
            kvs_cost_str="N/A"
        fi
        if [ "$(echo "$red_off > 0" | bc -l 2>/dev/null)" = "1" ]; then
            red_cost=$(echo "scale=2; (1 - $red_on/$red_off)*100" | bc -l 2>/dev/null || echo "N/A")
            red_cost_str="${red_cost}%"
        else
            red_cost_str="N/A"
        fi

        ROW=$(printf "  P=%-4s | %14s | %14s | %10s | %14s | %14s | %10s" \
            "$P" "$kvs_off" "$kvs_on" "$kvs_cost_str" "$red_off" "$red_on" "$red_cost_str")
        echo "$ROW"
        echo "$ROW" >> "$RESULT_TXT"
    done
    echo ""

    # ── GET 对比表 ──
    echo "── [GET 吞吐对比 (req/s)] ──"
    HDR=$(printf "  %-6s | %14s | %14s | %10s | %14s | %14s | %10s" \
        "P" "kvs-AOF-off" "kvs-AOF-on" "AOF开销" "redis-AOF-off" "redis-AOF-on" "AOF开销")
    SEP=$(printf "  %-6s | %14s | %14s | %10s | %14s | %14s | %10s" \
        "------" "--------------" "--------------" "----------" "--------------" "--------------" "----------")
    echo "$HDR"
    echo "$SEP"
    {
        echo ""
        echo "$HDR"
        echo "$SEP"
    } >> "$RESULT_TXT"

    for P in "${PIPELINES[@]}"; do
        local kvs_off=${KVS_AOF_OFF_GET[$P]:-0}
        local kvs_on=${KVS_AOF_ON_GET[$P]:-0}
        local red_off=${REDIS_AOF_OFF_GET[$P]:-0}
        local red_on=${REDIS_AOF_ON_GET[$P]:-0}

        local kvs_cost kvs_cost_str red_cost red_cost_str
        if [ "$(echo "$kvs_off > 0" | bc -l 2>/dev/null)" = "1" ]; then
            kvs_cost=$(echo "scale=2; (1 - $kvs_on/$kvs_off)*100" | bc -l 2>/dev/null || echo "N/A")
            kvs_cost_str="${kvs_cost}%"
        else
            kvs_cost_str="N/A"
        fi
        if [ "$(echo "$red_off > 0" | bc -l 2>/dev/null)" = "1" ]; then
            red_cost=$(echo "scale=2; (1 - $red_on/$red_off)*100" | bc -l 2>/dev/null || echo "N/A")
            red_cost_str="${red_cost}%"
        else
            red_cost_str="N/A"
        fi

        ROW=$(printf "  P=%-4s | %14s | %14s | %10s | %14s | %14s | %10s" \
            "$P" "$kvs_off" "$kvs_on" "$kvs_cost_str" "$red_off" "$red_on" "$red_cost_str")
        echo "$ROW"
        echo "$ROW" >> "$RESULT_TXT"
    done
    echo ""

    # ── 最终结论 ──
    echo "── [总结] ──"
    local p160_kvs_set_off=${KVS_AOF_OFF_SET[160]:-0}
    local p160_kvs_set_on=${KVS_AOF_ON_SET[160]:-0}
    local p160_red_set_off=${REDIS_AOF_OFF_SET[160]:-0}
    local p160_red_set_on=${REDIS_AOF_ON_SET[160]:-0}

    local p160_kvs_cost="N/A" p160_red_cost="N/A"
    if [ "$(echo "$p160_kvs_set_off > 0" | bc -l 2>/dev/null)" = "1" ]; then
        p160_kvs_cost="$(echo "scale=2; (1 - $p160_kvs_set_on/$p160_kvs_set_off)*100" | bc -l 2>/dev/null)%"
    fi
    if [ "$(echo "$p160_red_set_off > 0" | bc -l 2>/dev/null)" = "1" ]; then
        p160_red_cost="$(echo "scale=2; (1 - $p160_red_set_on/$p160_red_set_off)*100" | bc -l 2>/dev/null)%"
    fi

    echo "  压测参数: -n $BENCH_N -c $BENCH_C -d 16"
    echo "  PING:  kvstore=${KVS_PING_QPS} req/s,  Redis=${REDIS_PING_QPS} req/s"
    echo "  SET@P=160:"
    echo "    kvstore AOF off: ${p160_kvs_set_off} req/s,  on: ${p160_kvs_set_on} req/s  (开销: ${p160_kvs_cost})"
    echo "    Redis   AOF off: ${p160_red_set_off} req/s,  on: ${p160_red_set_on} req/s  (开销: ${p160_red_cost})"
    echo ""

    {
        echo ""
        echo "── [总结] ──"
        echo "  压测参数: -n $BENCH_N -c $BENCH_C -d 16"
        echo "  PING:  kvstore=${KVS_PING_QPS} req/s,  Redis=${REDIS_PING_QPS} req/s"
    } >> "$RESULT_TXT"
}

# ═══════════════════════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════════════════════

init_wal_results

log "=========================================="
log "  WAL / AOF 持久化性能对比 — 6 维度独立测试"
log "=========================================="
log "压测参数: N=${BENCH_N}, C=${BENCH_C}, P=${PIPELINES[*]}"
log "每个测试独立启停服务，互不影响"
log "开始时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# 按顺序执行 6 个独立测试
test_t1_kvs_ping
test_t2_kvs_aof_off
test_t3_kvs_aof_on
test_t4_redis_ping
test_t5_redis_aof_off
test_t6_redis_aof_on

# 打印汇总
print_summary

# 清理
stop_kvs
stop_redis

log "=========================================="
log "  测试完成"
log "  文本结果: $RESULT_TXT"
log "  CSV 结果: $RESULT_CSV"
log "=========================================="
