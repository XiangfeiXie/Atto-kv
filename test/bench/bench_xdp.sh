#!/bin/bash
# =============================================================================
# bench_xdp.sh — XDP GET 加速性能对比
#
# 用法:
#   sudo ./test/bench/bench_xdp.sh           # 默认深度组合
#   sudo ./test/bench/bench_xdp.sh 1 4 10    # 只测指定深度
#
# 测试场景 (GET only — XDP 仅加速读路径):
#   A. kvstore (XDP off)  — proactor, 标准用户态 TCP 路径
#   B. kvstore (XDP on)   — proactor + XDP_TX 内核热路径 (lo, SKB mode)
#   C. Redis              — 单线程 ae 事件循环, 无 XDP 能力
#
# XDP 设计:
#   - 单 TCP segment 单条 GET (P=1) 命中 hot_cache 时由 XDP_TX 直接回包
#     (跳过内核 TCP 栈, 跳过 kvstore 用户态), 极低延迟.
#   - 一个 segment 内多条命令 (P>1, 即 redis-benchmark pipelining) 整包
#     回落到 XDP_PASS, 由 kvstore 正常处理 (因为 BPF 程序每个入包只能发
#     一个 XDP_TX 响应; 处理多条会导致客户端阻塞等待剩余响应).
#   - 任意命令 cache miss 回落到 XDP_PASS.
#   keyspace < 65536 槽, 预热一次填充 hot_cache; 同场景服务只启动一次.
#   结果覆盖 results.txt 和 results_xdp.csv.
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

require_root
require_tools

# ---- 参数 ----
BENCH_N=100000     # 每轮请求数 (10万/轮, 兼顾精度与 XDP 持久连接稳定性)
BENCH_C=50         # 并发连接数
XDP_KEYSPACE=20000 # key 空间 (< hot_cache 65536 槽)
WARMUP_N=$((XDP_KEYSPACE * 10))   # 10x keyspace, 覆盖率 ≈ 1-e^-10 ≈ 99.995%
                                  # XDP 场景下任何一次 cache miss 都会让该
                                  # 持久连接的内核 TCP RX seq 与 XDP 注入的
                                  # ACK 失步, 后续 GET 必然超时, 故必须保证
                                  # 命中率严格接近 100%.
# XDP 只加速 P=1 的 GET (单 segment 单命令); P>1 时 XDP_PASS 到 kvstore.
# 默认同时测 P=1,4,10 以全面展示 XDP 代价/收益: P=1 XDP 加速,
# P>1 XDP 退化为透传 (与 XDP off 场景同带宽, 不带加速也不带堆堈).
PIPELINES=(1 4 10)
if [ $# -ge 1 ]; then
    PIPELINES=("$@")
fi

XDP_CSV="${PROJECT_DIR}/test/bench/results_xdp.csv"
XDP_LOG="${LOG_DIR}/kvstore_kvstore-xdp.conf.log"

# ---- 工具函数 ----

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

# redis-benchmark 带 -r (keyspace) 支持，超时保护
# 参数: host port test n c P d keyspace
run_bench_r() {
    local host=$1 port=$2 test=$3 n=$4 c=$5 P=$6 d=$7 r=$8
    local csv_line qps rc=0
    # 用 || rc=$? 防止 set -e 在超时时中止脚本;
    # pipefail 确保 timeout 的退出码通过 pipe 传播
    csv_line=$(timeout 90 redis-benchmark -h "$host" -p "$port" -t "$test" \
        -n "$n" -c "$c" -P "$P" -d "$d" -r "$r" --csv 2>/dev/null | tail -1) || rc=$?
    if [ "$rc" -eq 124 ]; then
        warn "redis-benchmark 超时 (90s) at P=$P"
        echo "0"
        return
    fi
    qps=$(echo "$csv_line" | cut -d',' -f2 | tr -d '"')
    echo "${qps:-0}"
}

# 预热: 用固定 keyspace 填充 key (XDP on 时同步填充 hot_cache)
do_warmup() {
    local host=$1 port=$2
    redis-benchmark -h "$host" -p "$port" -t set \
        -n "$WARMUP_N" -c "$BENCH_C" -P 10 -d 16 -r "$XDP_KEYSPACE" \
        >/dev/null 2>&1 || true
}

# XDP 挂载验证 — 实际 printf: "[XDP] attached to <iface> ..."
verify_xdp_attached() {
    sleep 1  # 等待 libbpf 加载 + printf 写入日志
    if grep -qF '[XDP] attached to' "$XDP_LOG" 2>/dev/null; then
        log "XDP 挂载成功: $(grep -F '[XDP] attached to' "$XDP_LOG" | tail -1)"
        return 0
    else
        warn "XDP 挂载失败! 日志: $XDP_LOG"
        tail -10 "$XDP_LOG" >&2 || true
        warn "场景 B 数据将等同于 XDP off, 请勿用于对比"
        return 1
    fi
}

# ---- 初始化 (覆盖 results.txt 和 results.csv) ----
declare -A R_NOXDP R_XDP R_REDIS

init_results
echo "pipeline,kvs_noXDP_get,kvs_XDP_get,xdp_speedup,redis_get,xdp_vs_redis" \
    > "$XDP_CSV"

record_raw "=== XDP GET 热路径加速性能对比 ==="
record_raw "参数: N=$BENCH_N, C=$BENCH_C, -r $XDP_KEYSPACE (keyspace), d=16B"
record_raw "测试时间: $(date '+%Y-%m-%d %H:%M:%S')"
record_raw "场景 A: kvstore XDP off (proactor, 标准 TCP 路径)"
record_raw "场景 B: kvstore XDP on  (proactor + XDP_TX, lo SKB mode)"
record_raw "场景 C: Redis           (单线程, 无 XDP 能力)"
record_raw "Pipeline 深度: ${PIPELINES[*]}  (XDP 仅加速 P=1; P>1 透传到 kvstore)"
record_raw ""

echo ""
log "XDP GET 性能对比  —  Pipeline 深度: ${PIPELINES[*]}"
log "keyspace=$XDP_KEYSPACE (< hot_cache 65536 槽, 预热后命中率接近 100%)"
log "XDP 模式: P=1 命中走 XDP_TX 快路径; P>1 / cache miss 透传到 kvstore; C=${BENCH_C}"
log "同场景只启动一次服务, 预热一次, 连续测所有 Pipeline 深度"
echo ""

# ============================================================
# 场景 A: kvstore XDP off  (启动一次, 预热一次)
# ============================================================
echo "════════════════════════════════════════════════════════"
step "场景 A: kvstore XDP off — 标准 proactor TCP 路径"
echo "════════════════════════════════════════════════════════"

stop_kvs
stop_redis
start_kvs "kvstore-none.conf"

step "预热: SET ${WARMUP_N} 请求 / -r ${XDP_KEYSPACE} keyspace"
do_warmup 127.0.0.1 "$KVS_PORT"

for P in "${PIPELINES[@]}"; do
    step "P=$P  GET (kvstore, XDP off)"
    v=$(run_bench_r 127.0.0.1 "$KVS_PORT" get \
        "$BENCH_N" "$BENCH_C" "$P" 16 "$XDP_KEYSPACE")
    R_NOXDP[$P]=$v
    record_result "XDP-P${P}-GET" "kvs-noXDP" "$v"
done

stop_kvs
echo ""

# ============================================================
# 场景 B: kvstore XDP on  (启动一次, 预热一次填充 hot_cache)
# ============================================================
echo "════════════════════════════════════════════════════════"
step "场景 B: kvstore XDP on — XDP_TX 内核热路径 (lo)"
echo "════════════════════════════════════════════════════════"

start_kvs "kvstore-xdp.conf"
verify_xdp_attached

step "预热: SET ${WARMUP_N} 请求 / -r ${XDP_KEYSPACE} keyspace (同步填充 XDP hot_cache)"
do_warmup 127.0.0.1 "$KVS_PORT"

for P in "${PIPELINES[@]}"; do
    step "P=$P  GET (kvstore, XDP on)"
    v=$(run_bench_r 127.0.0.1 "$KVS_PORT" get \
        "$BENCH_N" "$BENCH_C" "$P" 16 "$XDP_KEYSPACE")
    R_XDP[$P]=$v
    record_result "XDP-P${P}-GET" "kvs-XDP" "$v"
done

stop_kvs
echo ""

# ============================================================
# 场景 C: Redis  (启动一次, 预热一次)
# ============================================================
echo "════════════════════════════════════════════════════════"
step "场景 C: Redis — 单线程, 无 XDP 能力"
echo "════════════════════════════════════════════════════════"

start_redis
redis_configure none

step "预热: SET ${WARMUP_N} 请求 / -r ${XDP_KEYSPACE} keyspace"
do_warmup 127.0.0.1 "$REDIS_PORT"

for P in "${PIPELINES[@]}"; do
    step "P=$P  GET (Redis)"
    v=$(run_bench_r 127.0.0.1 "$REDIS_PORT" get \
        "$BENCH_N" "$BENCH_C" "$P" 16 "$XDP_KEYSPACE")
    R_REDIS[$P]=$v
    record_result "XDP-P${P}-GET" "Redis" "$v"
done

stop_redis
echo ""

# ============================================================
# 汇总表格
# ============================================================
record_raw ""
record_raw "=== XDP 汇总 (GET QPS) ==="
record_raw ""

HDR=$(printf "  %-6s | %12s | %12s | %8s | %12s | %8s" \
    "P" "kvs-noXDP" "kvs-XDP" "XDP加速" "Redis" "XDP/Redis")
SEP=$(printf "  %-6s | %12s | %12s | %8s | %12s | %8s" \
    "------" "------------" "------------" "--------" "------------" "--------")

record_raw "$HDR"
record_raw "$SEP"
echo "$HDR"
echo "$SEP"

for P in "${PIPELINES[@]}"; do
    noX=${R_NOXDP[$P]:-0}
    xdp=${R_XDP[$P]:-0}
    red=${R_REDIS[$P]:-0}
    r1=$(safe_ratio "$xdp" "$noX")
    r2=$(safe_ratio "$xdp" "$red")
    ROW=$(printf "  P=%-4s | %12s | %12s | %8s | %12s | %8s" \
        "$P" "$noX" "$xdp" "$r1" "$red" "$r2")
    record_raw "$ROW"
    echo "$ROW"
    echo "$P,$noX,$xdp,$r1,$red,$r2" >> "$XDP_CSV"
done

record_raw ""
echo ""

log "完成。结果已保存至:"
log "  文本: $RESULT_FILE"
log "  CSV : $XDP_CSV"
log ""
log "XDP hot_cache 命中率 (需要 kvstore 以 XDP on 运行时查看):"
log "  sudo bpftool map dump name stats_map"
log "  key 0=hits  key 1=misses  key 2=tx_drop  key 3=bypass"
