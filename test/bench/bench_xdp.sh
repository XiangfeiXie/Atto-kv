#!/bin/bash
# =============================================================================
# bench_xdp.sh — XDP 被动监控开销对比 (Phase 1)
#
# 用法:
#   sudo ./test/bench/bench_xdp.sh           # 默认深度组合
#   sudo ./test/bench/bench_xdp.sh 1 4 10    # 只测指定深度
#
# 测试场景 (GET only):
#   A. kvstore (XDP off)     — proactor, 标准用户态 TCP 路径 (baseline)
#   B. kvstore (XDP monitor) — proactor + XDP 被动监控 (仅统计, 不加速)
#   C. Redis                  — 单线程 ae 事件循环
#
# Phase 1 设计说明:
#   - XDP 程序始终返回 XDP_PASS, 不会拦截任何包.
#   - 在 RX 路径上解析 RESP GET 并查 hot_cache, 记录 hit / miss
#     计数 —— 为 Phase 2 (sockmap fast-path) 提供命中率实测基线.
#   - 原有 XDP_TX 方案 (在内核里直接构造响应) 引发内核 TCP 状态双
#     轨漂移问题, P=1 场景会超时 — 已彻底下线, 等待 sockmap 接手.
#
# 预期结果:
#   - 场景 B 吞吐量 ≈ 场景 A (误差 ±5%, 即 XDP 监控开销 ≤ 5%)
#   - bpftool map dump name stats_map 可看到真实的 hits/misses 计数
#
#   结果覆盖 results.txt 和 results_xdp.csv.
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

require_root
require_tools

# ---- 参数 ----
BENCH_N=200000     # 每轮请求数 (20万/轮；Phase 1 XDP 全部 PASS, 不再需要超时折中)
BENCH_C=50         # 并发连接数
XDP_KEYSPACE=20000 # key 空间 (< hot_cache 65536 槽)
WARMUP_N=$((XDP_KEYSPACE * 5))    # 5x keyspace 足够填充绝大多数 key。
                                  # Phase 1 XDP 不会拦截, 偶发 cache miss
                                  # 不再引发连接卡死, 不需严格足 100% 命中.
# Phase 1 XDP 仅监控不加速, 各 P 值下场景 B 吞吐量预期 ≈ 场景 A.
# P=1,4,10 覆盖从请求密集 → pipelining 、一路验证监控开销趋近零.
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
echo "pipeline,kvs_noXDP_get,kvs_XDPmon_get,monitor_overhead,redis_get,kvs_vs_redis" \
    > "$XDP_CSV"

record_raw "=== XDP 被动监控开销对比 (Phase 1) ==="
record_raw "参数: N=$BENCH_N, C=$BENCH_C, -r $XDP_KEYSPACE (keyspace), d=16B"
record_raw "测试时间: $(date '+%Y-%m-%d %H:%M:%S')"
record_raw "场景 A: kvstore XDP off     (proactor, 标准 TCP 路径) — baseline"
record_raw "场景 B: kvstore XDP monitor (proactor + XDP 被动监控, 始终 XDP_PASS)"
record_raw "场景 C: Redis              (单线程, 无 XDP 能力)"
record_raw "Pipeline 深度: ${PIPELINES[*]}"
record_raw "预期: 场景 B 与场景 A 误差 ±5% (XDP 监控开销)"
record_raw ""

echo ""
log "XDP Phase 1 监控开销对比  —  Pipeline 深度: ${PIPELINES[*]}"
log "keyspace=$XDP_KEYSPACE, hot_cache=65536 槽, 预热=${WARMUP_N} SET"
log "XDP 模式: 被动监控 (始终 XDP_PASS), 只记录 hits/misses/bypass; C=${BENCH_C}"
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
step "场景 B: kvstore XDP monitor — 被动监控 (lo SKB mode)"
echo "════════════════════════════════════════════════════════"

start_kvs "kvstore-xdp.conf"
verify_xdp_attached

step "预热: SET ${WARMUP_N} 请求 / -r ${XDP_KEYSPACE} keyspace (同步填充 XDP hot_cache)"
do_warmup 127.0.0.1 "$KVS_PORT"

for P in "${PIPELINES[@]}"; do
    step "P=$P  GET (kvstore, XDP monitor)"
    v=$(run_bench_r 127.0.0.1 "$KVS_PORT" get \
        "$BENCH_N" "$BENCH_C" "$P" 16 "$XDP_KEYSPACE")
    R_XDP[$P]=$v
    record_result "XDP-P${P}-GET" "kvs-XDPmon" "$v"
done

# 在 stop 之前输出一份 hot_cache 命中统计
if command -v bpftool >/dev/null 2>&1; then
    record_raw ""
    record_raw "=== XDP stats_map (Phase 1 被动监控计数) ==="
    bpftool map dump name stats_map 2>/dev/null \
        | python3 -c '
import sys, json
try:
    arr = json.loads(sys.stdin.read())
    labels = {0: "hits", 1: "misses", 2: "tx_drop", 3: "bypass"}
    for e in arr:
        k = e.get("key", e.get("formatted", {}).get("key"))
        v = e.get("value", e.get("formatted", {}).get("value"))
        print(f"  {labels.get(int(k), k):<8} = {v}")
    hits = next((int(e.get("value", 0)) for e in arr if int(e.get("key", -1)) == 0), 0)
    misses = next((int(e.get("value", 0)) for e in arr if int(e.get("key", -1)) == 1), 0)
    if hits + misses > 0:
        print(f"  hit_rate = {100.0 * hits / (hits + misses):.2f}%")
except Exception as e:
    print(f"  (解析失败: {e})", file=sys.stderr)
' 2>&1 | tee -a "$RESULT_FILE" || true
    record_raw ""
fi

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
    "P" "kvs-noXDP" "kvs-XDPmon" "监控比" "Redis" "kvs/Redis")
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
log "如需实时查看 XDP 监控计数 (kvstore 运行中):"
log "  sudo bpftool map dump name stats_map"
log "  key 0=hits  key 1=misses  key 2=tx_drop(未使用)  key 3=bypass"
log ""
log "预期 Phase 1 现象: kvs-XDPmon 与 kvs-noXDP 吞吐量误差 ≤ 5%"
log "  (证明 XDP 被动监控本身几乎不引入额外开销, 为 Phase 2 提供干净 baseline)"
