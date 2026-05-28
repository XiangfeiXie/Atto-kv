#!/bin/bash
# =============================================================================
# bench_pipeline.sh — Pipeline 批量处理性能对比（kvstore vs Redis）
#
# 用法:
#   sudo ./test/bench/bench_pipeline.sh           # 测 P=10,20,40,80,160
#   sudo ./test/bench/bench_pipeline.sh 40 80     # 只测指定深度
#
# 输出:
#   终端彩色表格 + test/bench/results.txt + test/bench/results.csv
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

require_root
require_tools

# ---- 参数 ----
BENCH_N=1000000   # 每轮请求数
BENCH_C=50        # 并发连接数
WARMUP_N=200000   # GET 前预热 SET 数量（与 redis-benchmark 相同 key 格式）

BENCH_R=1000000   # 随机 key 范围（-r 参数，接近真实场景）

if [ $# -ge 1 ]; then
    PIPELINES=("$@")
else
    PIPELINES=(1 5 10 20 40 80 160)
fi

# ---- 安全除法：任一操作数为 0 或非数字时返回 N/A ----
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
declare -A KVS_SET REDIS_SET KVS_GET REDIS_GET
declare -A KVS_SET_R REDIS_SET_R KVS_GET_R REDIS_GET_R

init_results
record_raw "=== Pipeline 批量处理性能对比 ==="
record_raw "压测参数: redis-benchmark -n $BENCH_N -c $BENCH_C -P <depth> -d 16"
record_raw "测试时间: $(date '+%Y-%m-%d %H:%M:%S')"
record_raw ""

echo ""
log "开始 Pipeline 测试 — 深度: ${PIPELINES[*]}（每轮重启服务以隔离残留状态）"
echo ""

# ---- 测试循环 ----
for P in "${PIPELINES[@]}"; do

    echo "────────────────────────────────────────────────"
    step "Pipeline = $P  — 重启服务"
    echo "────────────────────────────────────────────────"
    stop_kvs
    start_kvs "kvstore-none.conf"
    stop_redis
    start_redis
    redis_configure none

    # ── SET ──────────────────────────────────────────
    step "P=$P  SET — kvstore"
    kvs_s=$(run_bench 127.0.0.1 "$KVS_PORT"   set $BENCH_N $BENCH_C "$P" 16)
    record_result "Pipeline-${P}-SET" "kvstore" "$kvs_s"
    KVS_SET[$P]=$kvs_s

    step "P=$P  SET — Redis"
    red_s=$(run_bench 127.0.0.1 "$REDIS_PORT" set $BENCH_N $BENCH_C "$P" 16)
    record_result "Pipeline-${P}-SET" "Redis"   "$red_s"
    REDIS_SET[$P]=$red_s

    ratio_s=$(safe_ratio "$kvs_s" "$red_s")
    record_raw "  SET 比值 (kvstore/Redis): $ratio_s"

    # ── GET 预热：用 redis-benchmark 本身写入 key:__rand_int__ 格式 ──
    # redis-benchmark GET 使用 key:__rand_int__ pattern，必须先 SET 同格式 key
    step "P=$P  GET 预热（写入 ${WARMUP_N} keys）"
    redis-benchmark -h 127.0.0.1 -p "$KVS_PORT"   -t set \
        -n $WARMUP_N -c $BENCH_C -P "$P" -d 16 -q >/dev/null 2>&1 || true
    redis-benchmark -h 127.0.0.1 -p "$REDIS_PORT" -t set \
        -n $WARMUP_N -c $BENCH_C -P "$P" -d 16 -q >/dev/null 2>&1 || true

    # ── GET ──────────────────────────────────────────
    step "P=$P  GET — kvstore"
    kvs_g=$(run_bench 127.0.0.1 "$KVS_PORT"   get $BENCH_N $BENCH_C "$P" 16)
    record_result "Pipeline-${P}-GET" "kvstore" "$kvs_g"
    KVS_GET[$P]=$kvs_g

    step "P=$P  GET — Redis"
    red_g=$(run_bench 127.0.0.1 "$REDIS_PORT" get $BENCH_N $BENCH_C "$P" 16)
    record_result "Pipeline-${P}-GET" "Redis"   "$red_g"
    REDIS_GET[$P]=$red_g

    ratio_g=$(safe_ratio "$kvs_g" "$red_g")
    record_raw "  GET 比值 (kvstore/Redis): $ratio_g"
    record_raw ""

    # ── 随机 Key 模式 (真实场景) ──────────────────────────
    step "P=$P  随机Key— kvstore"
    kvs_sr=$(run_bench_r 127.0.0.1 "$KVS_PORT"   set $BENCH_N $BENCH_C "$P" 16 $BENCH_R)
    record_result "Pipeline-${P}-SET-rnd" "kvstore" "$kvs_sr"
    KVS_SET_R[$P]=$kvs_sr

    step "P=$P  随机Key— Redis"
    red_sr=$(run_bench_r 127.0.0.1 "$REDIS_PORT" set $BENCH_N $BENCH_C "$P" 16 $BENCH_R)
    record_result "Pipeline-${P}-SET-rnd" "Redis"   "$red_sr"
    REDIS_SET_R[$P]=$red_sr

    ratio_sr=$(safe_ratio "$kvs_sr" "$red_sr")
    record_raw "  SET(rnd) 比值 (kvstore/Redis): $ratio_sr"

    # GET 预热：随机 key
    step "P=$P  随机Key GET 预热（写入 ${WARMUP_N} random keys）"
    redis-benchmark -h 127.0.0.1 -p "$KVS_PORT"   -t set \
        -n $WARMUP_N -c $BENCH_C -P "$P" -d 16 -r $BENCH_R -q >/dev/null 2>&1 || true
    redis-benchmark -h 127.0.0.1 -p "$REDIS_PORT" -t set \
        -n $WARMUP_N -c $BENCH_C -P "$P" -d 16 -r $BENCH_R -q >/dev/null 2>&1 || true

    step "P=$P  随机Key— kvstore"
    kvs_gr=$(run_bench_r 127.0.0.1 "$KVS_PORT"   get $BENCH_N $BENCH_C "$P" 16 $BENCH_R)
    record_result "Pipeline-${P}-GET-rnd" "kvstore" "$kvs_gr"
    KVS_GET_R[$P]=$kvs_gr

    step "P=$P  随机Key— Redis"
    red_gr=$(run_bench_r 127.0.0.1 "$REDIS_PORT" get $BENCH_N $BENCH_C "$P" 16 $BENCH_R)
    record_result "Pipeline-${P}-GET-rnd" "Redis"   "$red_gr"
    REDIS_GET_R[$P]=$red_gr

    ratio_gr=$(safe_ratio "$kvs_gr" "$red_gr")
    record_raw "  GET(rnd) 比值 (kvstore/Redis): $ratio_gr"
    record_raw ""

done

# ---- 汇总表格 ----
print_table() {
    local title=$1
    record_raw ""
    record_raw "=== $title ==="
    record_raw ""
    echo ""
    echo -e "  ${YELLOW}$title${NC}"

    HDR=$(printf "  %-6s | %12s | %12s | %6s | %12s | %12s | %6s" \
        "P" "kvs-SET" "redis-SET" "S倍率" "kvs-GET" "redis-GET" "G倍率")
    SEP=$(printf "  %-6s | %12s | %12s | %6s | %12s | %12s | %6s" \
        "------" "------------" "------------" "------" "------------" "------------" "------")
    record_raw "$HDR"
    record_raw "$SEP"
    echo "$HDR"
    echo "$SEP"

    shift  # discard title
    local -n _ks=$1 _rs=$2 _kg=$3 _rg=$4

    for P in "${PIPELINES[@]}"; do
        local kvss=${_ks[$P]:-0}
        local reds=${_rs[$P]:-0}
        local kvsg=${_kg[$P]:-0}
        local redg=${_rg[$P]:-0}
        local rs=$(safe_ratio "$kvss" "$reds")
        local rg=$(safe_ratio "$kvsg" "$redg")
        ROW=$(printf "  P=%-4s | %12s | %12s | %6s | %12s | %12s | %6s" \
            "$P" "$kvss" "$reds" "$rs" "$kvsg" "$redg" "$rg")
        record_raw "$ROW"
        echo "$ROW"
    done
    record_raw ""
    echo ""
}

# 表 1: 顺序 Key (理想)
print_table "顺序Key (理想)" KVS_SET REDIS_SET KVS_GET REDIS_GET
# 表 2: 随机 Key (真实)
print_table "随机Key (真实, -r $BENCH_R)" KVS_SET_R REDIS_SET_R KVS_GET_R REDIS_GET_R

# ---- 清理 ----
stop_kvs
stop_redis

log "完成。结果已保存至:"
log "  文本: $RESULT_FILE"
log "  CSV : $RESULT_CSV"
