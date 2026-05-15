#!/bin/bash
# =============================================================================
# bench_save.sh — RDB SAVE 策略性能对比（三路对比）
#
# 场景 A: 基线       — 纯 SET，不触发任何 SAVE
# 场景 B: 固定时间   — 每 SAVE_INTERVAL_SEC 秒触发一次 BGSAVE（异步子进程）
#                      测量：运行时 QPS 被异步 SAVE 拉低多少
# 场景 C: 固定命令数 — 每写入 SAVE_EVERY_N 条命令触发一次 SAVE，等待完成
#                      测量：含 SAVE 等待的端到端有效 QPS
#
# 用法:
#   sudo ./test/bench/bench_save.sh               # 默认: 10k / 100k / 1M keys
#   sudo ./test/bench/bench_save.sh 10000 100000  # 自定义数据量
#
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

require_root
require_tools

# ---- 参数 ----
BENCH_N=2000000       # 总请求数（确保固定时间测试 ≥10 次 BGSAVE）
BENCH_C=50            # 并发连接数
BENCH_P=20            # Pipeline 深度
SAVE_INTERVAL_SEC=3   # 场景B: BGSAVE 触发间隔（秒）
SAVE_EVERY_N=200000   # 场景C: 每 N 条命令触发一次 SAVE（BENCH_N / SAVE_EVERY_N = 次数）

RDB_FILE="${PROJECT_DIR}/build/dump.rdb"

if [ $# -ge 1 ]; then
    KEY_COUNTS=("$@")
else
    KEY_COUNTS=(10000 100000 1000000)
fi

# ---- 辅助函数 ----

# 安全百分比降幅：(1 - a/b) * 100，任一为 0 返回 N/A
drop_pct() {
    local a=$1 b=$2
    if [[ "$a" =~ ^[0-9]+(\.[0-9]+)?$ ]] && \
       [[ "$b" =~ ^[0-9]+(\.[0-9]+)?$ ]] && \
       [ "$(echo "$b > 0" | bc -l 2>/dev/null)" = "1" ]; then
        printf "%.1f%%" "$(echo "scale=4; (1 - $a/$b)*100" | bc -l 2>/dev/null)"
    else
        echo "N/A"
    fi
}

# 等待 RDB 文件完成写入：轮询 mtime 变化，最多等 max_sec 秒
# 触发时间 t_trigger（秒级 unix timestamp）
wait_rdb_complete() {
    local t_trigger=$1 max_sec=${2:-60}
    local polls=$((max_sec * 5))   # 每 0.2s 轮询一次
    for i in $(seq 1 $polls); do
        sleep 0.2
        if [ -f "$RDB_FILE" ]; then
            local mtime
            mtime=$(stat -c%Y "$RDB_FILE" 2>/dev/null || echo 0)
            [ "$mtime" -ge "$t_trigger" ] && return 0
        fi
    done
    warn "wait_rdb_complete: 超时 ${max_sec}s，SAVE 可能未完成"
    return 1
}

# 场景B: 固定时间间隔异步 BGSAVE
# 在 redis-benchmark 运行期间，后台每 SAVE_INTERVAL_SEC 秒触发一次 BGSAVE
# 返回 redis-benchmark 报告的 QPS（不含 SAVE 等待，因 BGSAVE 是异步的）
bench_fixed_time() {
    local host=$1 port=$2
    local num_saves=0

    # 后台 BGSAVE 循环
    (
        sleep 1   # 等压测先建立连接
        while true; do
            redis-cli -h "$host" -p "$port" BGSAVE >/dev/null 2>&1 || true
            num_saves=$((num_saves + 1))
            sleep "$SAVE_INTERVAL_SEC"
        done
    ) &
    local loop_pid=$!

    local qps
    qps=$(run_bench "$host" "$port" set $BENCH_N $BENCH_C $BENCH_P 16)

    kill "$loop_pid" 2>/dev/null || true
    wait "$loop_pid" 2>/dev/null || true
    # BGSAVE 是异步的，QPS 已经测完，无需等待落盘
    echo "$qps"
}

# 场景C: 固定命令数同步 SAVE
# 将 BENCH_N 分成多个 chunk，每个 chunk 后发 SAVE 并等待完成
# 返回端到端有效 QPS = BENCH_N / 总耗时（含所有 SAVE 时间）
bench_fixed_count() {
    local host=$1 port=$2
    local chunk=$SAVE_EVERY_N
    local chunks=$((BENCH_N / chunk))
    local total_save_ms=0
    local save_count=0

    local t0
    t0=$(date +%s%3N)   # 毫秒级起始时间

    for i in $(seq 1 $chunks); do
        # 运行一个 chunk 的 SET 压测
        redis-benchmark -h "$host" -p "$port" -t set \
            -n "$chunk" -c $BENCH_C -P $BENCH_P -d 16 -q >/dev/null 2>&1 || true

        # 触发 SAVE 并计时等待
        # t_trigger 必须在 nc 调用前取，作为 mtime 轮询的锁定基准
        local ts
        ts=$(date +%s)
        local ts_ms
        ts_ms=$(date +%s%3N)
        # redis-cli 正确处理 RESP 协议，收到 +OK 后立即退出
        # nc 会等到连接超时（60s × N次 = 巨大开销）
        redis-cli -h "$host" -p "$port" SAVE >/dev/null 2>&1 || true

        # nc 返回后 SAVE 子进程可能仍在写盘，轮询 mtime >= ts 确认完成
        wait_rdb_complete "$ts" 30 || true

        local ts_save_done
        ts_save_done=$(date +%s%3N)
        total_save_ms=$(( total_save_ms + ts_save_done - ts_ms ))
        save_count=$((save_count + 1))
    done

    local t1
    t1=$(date +%s%3N)
    local elapsed_ms=$(( t1 - t0 ))

    # 有效 QPS（含 SAVE 开销）
    local eff_qps=0
    [ "$elapsed_ms" -gt 0 ] && \
        eff_qps=$(echo "scale=2; $BENCH_N * 1000 / $elapsed_ms" | bc -l 2>/dev/null || echo 0)

    # 单次 SAVE 平均耗时（ms）
    local avg_save_ms=0
    [ "$save_count" -gt 0 ] && \
        avg_save_ms=$(echo "scale=0; $total_save_ms / $save_count" | bc 2>/dev/null || echo 0)

    echo "${eff_qps}:${avg_save_ms}:${save_count}"
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

    # ─── 场景 A: 基线 ─────────────────────────────────
    step "A  基线（无 SAVE）"
    local baseline
    baseline=$(run_bench 127.0.0.1 "$port" set $BENCH_N $BENCH_C $BENCH_P 16)
    record_result "SAVE-${nkeys}-baseline" "kvstore" "$baseline"

    # ─── 场景 B: 固定时间 BGSAVE ──────────────────────
    step "B  固定时间 BGSAVE（每 ${SAVE_INTERVAL_SEC}s，共约 $((BENCH_N / (BENCH_C * BENCH_P * SAVE_INTERVAL_SEC * 100 + 1) + 1)) 次）"
    local timed_qps
    timed_qps=$(bench_fixed_time 127.0.0.1 "$port")
    local timed_drop
    timed_drop=$(drop_pct "$timed_qps" "$baseline")
    record_result "SAVE-${nkeys}-timed" "kvstore" "$timed_qps"
    record_raw "  场景B 降幅 vs 基线: ${timed_drop}"

    # ─── 场景 C: 固定命令数同步 SAVE ──────────────────
    local total_saves=$((BENCH_N / SAVE_EVERY_N))
    step "C  固定命令数 SAVE（每 ${SAVE_EVERY_N} 条，共 ${total_saves} 次，等待完成）"
    local count_result
    count_result=$(bench_fixed_count 127.0.0.1 "$port")
    local count_qps avg_save_ms save_cnt
    count_qps=$(echo "$count_result" | cut -d: -f1)
    avg_save_ms=$(echo "$count_result" | cut -d: -f2)
    save_cnt=$(echo "$count_result" | cut -d: -f3)
    local count_drop
    count_drop=$(drop_pct "$count_qps" "$baseline")
    record_result "SAVE-${nkeys}-count" "kvstore" "$count_qps"
    record_raw "  场景C 降幅 vs 基线: ${count_drop}  (SAVE 均耗 ${avg_save_ms}ms × ${save_cnt} 次)"

    # RDB 文件大小
    local rdb_mb="0.0"
    if [ -f "$RDB_FILE" ]; then
        local rdb_size
        rdb_size=$(stat -c%s "$RDB_FILE" 2>/dev/null || echo 0)
        rdb_mb=$(echo "scale=1; $rdb_size/1048576" | bc -l 2>/dev/null || echo 0)
    fi
    record_raw "  RDB 大小: ${rdb_mb} MB"
    record_raw ""

    # 本轮小表
    echo ""
    printf "  %-20s | %12s | %8s\n" "场景" "QPS" "降幅"
    printf "  %-20s | %12s | %8s\n" "--------------------" "------------" "--------"
    printf "  %-20s | %12s | %8s\n" "A 基线（无SAVE）" "$baseline" "—"
    printf "  %-20s | %12s | %8s\n" "B 固定时间BGSAVE" "$timed_qps" "$timed_drop"
    printf "  %-20s | %12s | %8s\n" "C 固定命令数SAVE" "$count_qps" "$count_drop"
    printf "  SAVE单次均耗: %s ms   RDB大小: %s MB\n" "$avg_save_ms" "$rdb_mb"
    echo ""

    stop_kvs
    rm -f "$RDB_FILE" "${PROJECT_DIR}/build/dump.rdb.tmp"
}

# ---- 执行 ----
init_results
record_raw "=== RDB SAVE 策略性能对比 ==="
record_raw "参数: N=${BENCH_N}, C=${BENCH_C}, P=${BENCH_P}, d=16B"
record_raw "  场景B: BGSAVE 每 ${SAVE_INTERVAL_SEC}s（异步）"
record_raw "  场景C: SAVE 每 ${SAVE_EVERY_N} 条（同步等待完成）"
record_raw ""

for n in "${KEY_COUNTS[@]}"; do
    run_save_test "$n"
done

# ---- 汇总表 ----
record_raw ""
record_raw "=== 汇总（所有数据集） ==="
record_raw ""
printf "  %-10s | %12s | %12s | %8s | %12s | %8s\n" \
    "Keys" "A-基线" "B-定时" "B降幅" "C-定量" "C降幅" >> "$RESULT_FILE"
printf "  %-10s | %12s | %12s | %8s | %12s | %8s\n" \
    "----------" "------------" "------------" "--------" "------------" "--------" >> "$RESULT_FILE"

echo ""
echo "════════════════════════════════════════════════"
echo "  汇总"
echo "════════════════════════════════════════════════"
printf "  %-10s | %12s | %12s | %8s | %12s | %8s\n" \
    "Keys" "A-基线" "B-定时" "B降幅" "C-定量" "C降幅"
printf "  %-10s | %12s | %12s | %8s | %12s | %8s\n" \
    "----------" "------------" "------------" "--------" "------------" "--------"

for n in "${KEY_COUNTS[@]}"; do
    base=$(grep "SAVE-${n}-baseline,kvstore," "$RESULT_CSV" | cut -d, -f3)
    timed=$(grep "SAVE-${n}-timed,kvstore,"   "$RESULT_CSV" | cut -d, -f3)
    cnt=$(grep  "SAVE-${n}-count,kvstore,"    "$RESULT_CSV" | cut -d, -f3)
    td=$(drop_pct "$timed" "$base")
    cd=$(drop_pct "$cnt"   "$base")
    ROW=$(printf "  %-10s | %12s | %12s | %8s | %12s | %8s" \
        "${n}" "$base" "$timed" "$td" "$cnt" "$cd")
    echo "$ROW"
    echo "$ROW" >> "$RESULT_FILE"
done

echo ""
log "完成。结果保存至:"
log "  文本: $RESULT_FILE"
log "  CSV : $RESULT_CSV"
