#!/bin/bash
# =============================================================================
# bench_aof.sh — AOF 增量持久化性能测试
#
# 对比 4 种场景的 SET QPS:
#   A. kvstore  — 无持久化 (proactor)
#   B. kvstore  — AOF 开启 (incr.wal, io_uring 异步写)
#   C. Redis    — 无持久化
#   D. Redis    — AOF 开启 (appendfsync everysec)
#
# 用法:
#   sudo ./test/bench/bench_aof.sh
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

require_root
require_tools

# ---- 参数 ----
BENCH_N=1000000
BENCH_C=50
BENCH_P=20
BENCH_D=16

# kvstore AOF 写到 build/incr.wal (AOF 与 WAL 统一路径)
WAL_FILE="${PROJECT_DIR}/build/incr.wal"

# 独立输出，不覆盖 pipeline/save 的 results.txt
AOF_RESULT="${PROJECT_DIR}/test/bench/results_aof.txt"
AOF_CSV="${PROJECT_DIR}/test/bench/results_aof.csv"

# ---- 工具函数 ----

# 安全计算降幅百分比: drop_pct <base_qps> <test_qps>
# 正数 = 下降，负数 = 上升（噪声），N/A = 无效输入
drop_pct() {
    local base=$1 test=$2
    if [[ "$base" =~ ^[0-9]+(\.[0-9]+)?$ ]] && \
       [[ "$test" =~ ^[0-9]+(\.[0-9]+)?$ ]] && \
       [ "$(echo "$base > 0" | bc -l 2>/dev/null)" = "1" ]; then
        printf "%.1f" "$(echo "scale=4; (1 - $test / $base) * 100" | bc -l 2>/dev/null)"
    else
        echo "N/A"
    fi
}

# 安全计算倍率: safe_ratio <a> <b>  → a/b
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

# 文件大小 (MB)
file_mb() {
    local f=$1
    if [ -f "$f" ]; then
        local sz
        sz=$(stat -c%s "$f" 2>/dev/null || echo 0)
        printf "%.1f" "$(echo "scale=2; $sz/1048576" | bc -l 2>/dev/null)"
    else
        echo "0.0"
    fi
}

# ---- 初始化输出文件 ----
{
    echo "# KVStore AOF Benchmark — $(date '+%Y-%m-%d %H:%M:%S')"
    echo "=== AOF 持久化性能测试 ==="
    echo "参数: N=$BENCH_N, C=$BENCH_C, P=$BENCH_P, d=${BENCH_D}B"
    echo ""
} > "$AOF_RESULT"
echo "scenario,qps" > "$AOF_CSV"

rec()   { echo "$*" >> "$AOF_RESULT"; }
rec_r() { echo "$*" >> "$AOF_RESULT"; echo "$*"; }

echo ""
step "AOF 持久化性能测试 — kvstore vs Redis"
echo "参数: N=$BENCH_N, C=$BENCH_C, P=$BENCH_P, d=${BENCH_D}B"
echo ""

# ============================================================
# 场景 A: kvstore 无持久化
# ============================================================
echo "────────────────────────────────────────────────"
step "A. kvstore SET — 无持久化 (proactor)"
echo "────────────────────────────────────────────────"
stop_kvs; stop_redis
start_kvs "kvstore-none.conf"
kvs_none_qps=$(run_bench 127.0.0.1 "$KVS_PORT" set $BENCH_N $BENCH_C $BENCH_P $BENCH_D)
echo "${BENCH_N}次SET,无持久化,${kvs_none_qps}" >> "$AOF_CSV"
rec "A. kvstore 无持久化 | $kvs_none_qps req/s"
echo -e "  → ${GREEN}${kvs_none_qps} req/s${NC}  (kvstore 基线)"
stop_kvs
echo ""

# ============================================================
# 场景 B: kvstore AOF 开启
# ============================================================
echo "────────────────────────────────────────────────"
step "B. kvstore SET — AOF 开启 (incr.wal, io_uring 异步写)"
echo "────────────────────────────────────────────────"
start_kvs "kvstore-aof.conf"
kvs_aof_qps=$(run_bench 127.0.0.1 "$KVS_PORT" set $BENCH_N $BENCH_C $BENCH_P $BENCH_D)
aof_overhead=$(drop_pct "$kvs_none_qps" "$kvs_aof_qps")
wal_size=$(file_mb "$WAL_FILE")
echo "${BENCH_N}次SET,kvstore-AOF,${kvs_aof_qps}" >> "$AOF_CSV"
rec "B. kvstore AOF     | $kvs_aof_qps req/s  (损耗 ${aof_overhead}%)  WAL: ${wal_size} MB"
echo -e "  → ${GREEN}${kvs_aof_qps} req/s${NC}  (kvstore AOF, 损耗 ${aof_overhead}%,  WAL: ${wal_size} MB)"
stop_kvs
echo ""

# ============================================================
# 场景 C: Redis 无持久化
# ============================================================
echo "────────────────────────────────────────────────"
step "C. Redis SET — 无持久化"
echo "────────────────────────────────────────────────"
start_redis
redis_configure none
redis_none_qps=$(run_bench 127.0.0.1 "$REDIS_PORT" set $BENCH_N $BENCH_C $BENCH_P $BENCH_D)
echo "${BENCH_N}次SET,Redis-无持久化,${redis_none_qps}" >> "$AOF_CSV"
rec "C. Redis 无持久化  | $redis_none_qps req/s"
echo -e "  → ${GREEN}${redis_none_qps} req/s${NC}  (Redis 基线)"
echo ""

# ============================================================
# 场景 D: Redis AOF everysec
# ============================================================
echo "────────────────────────────────────────────────"
step "D. Redis SET — AOF 开启 (appendfsync everysec)"
echo "────────────────────────────────────────────────"
redis_configure aof
redis_aof_qps=$(run_bench 127.0.0.1 "$REDIS_PORT" set $BENCH_N $BENCH_C $BENCH_P $BENCH_D)
redis_overhead=$(drop_pct "$redis_none_qps" "$redis_aof_qps")
echo "${BENCH_N}次SET,Redis-AOF,${redis_aof_qps}" >> "$AOF_CSV"
rec "D. Redis AOF       | $redis_aof_qps req/s  (损耗 ${redis_overhead}%)"
echo -e "  → ${GREEN}${redis_aof_qps} req/s${NC}  (Redis AOF, 损耗 ${redis_overhead}%)"
stop_redis
echo ""

# ============================================================
# 汇总表格
# ============================================================

# 横向对比指标
aof_vs_redis_base=$(safe_ratio "$kvs_none_qps" "$redis_none_qps")
aof_vs_redis_aof=$(safe_ratio "$kvs_aof_qps" "$redis_aof_qps")

HDR=$(printf "  %-28s | %12s | %8s" "场景" "QPS" "AOF损耗")
SEP=$(printf "  %-28s | %12s | %8s" "----------------------------" "------------" "--------")

ROW_A=$(printf "  %-28s | %12s | %8s" "A. kvstore 无持久化" "$kvs_none_qps" "—")
ROW_B=$(printf "  %-28s | %12s | %8s" "B. kvstore AOF (incr.wal)" "$kvs_aof_qps" "${aof_overhead}%")
ROW_C=$(printf "  %-28s | %12s | %8s" "C. Redis 无持久化" "$redis_none_qps" "—")
ROW_D=$(printf "  %-28s | %12s | %8s" "D. Redis AOF (everysec)" "$redis_aof_qps" "${redis_overhead}%")
ROW_R1=$(printf "  %-28s | %12s" "kvstore/Redis 无持久化倍率" "$aof_vs_redis_base")
ROW_R2=$(printf "  %-28s | %12s" "kvstore-AOF/Redis-AOF 倍率" "$aof_vs_redis_aof")

echo ""
echo "════════════════════════════════════════════════"
echo "  AOF 性能测试汇总"
echo "════════════════════════════════════════════════"
rec_r "$HDR"
rec_r "$SEP"
rec_r "$ROW_A"
rec_r "$ROW_B"
rec_r "$SEP"
rec_r "$ROW_C"
rec_r "$ROW_D"
rec_r "$SEP"
rec_r "$ROW_R1"
rec_r "$ROW_R2"
echo "────────────────────────────────────────────────"
echo ""
echo "  WAL 文件大小 (${BENCH_N} 次 SET × ${BENCH_D}B): ${wal_size} MB"
echo ""

rec ""
rec "  WAL 文件大小 (${BENCH_N} 次 SET × ${BENCH_D}B): ${wal_size} MB"
rec ""

log "结果已保存至: $AOF_RESULT"
log "CSV  : $AOF_CSV"
