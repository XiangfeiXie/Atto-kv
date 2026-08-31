#!/bin/bash
# =============================================================================
# bench_bgsave.sh — 大负载下定时 BGSAVE 对 kvstore(proactor) / redis 吞吐影响
#
# 流程: 一个 redis-benchmark 进程灌入海量 SET 撑大内存,
#       另一个后台进程从压测开始后按固定间隔持续触发 BGSAVE
#       (保存中重复触发是 no-op), 直到压测结束。
#       指标 = 整轮 QPS + p50/p99 延迟 (毫秒)。
#
# 触发间隔: 0.001 / 0.01 / 0.1 / 1 秒, 对 kvstore 与 redis 各测一遍。
#
# 用法（项目根目录执行）:
#   sudo ./test/bench/bench_bgsave.sh
#
# 输出: 数据直接写入 test/bench/results_bgsave.txt
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

# 固定参数
LOAD=10000000        # 压测条数, 也是 -r 随机 key 范围
CONN=50              # redis-benchmark -c
P=16                 # pipeline
TRIG_START=1.5       # 首触发延迟
INTERVALS=(0.001 0.01 0.1 1)
SYSTEMS=(kvstore redis)

WORK="$(mktemp -d /tmp/bgsavebench.XXXXXX)"
KVS_CONF="${WORK}/kvs.conf"
REDIS_CONF="${WORK}/redis.conf"
RESULT_FILE="test/bench/results_bgsave.txt"

cleanup() {
    stop_all
    rm -f build/dump.rdb build/dump.rdb.tmp
    rm -rf "$WORK"
}
trap cleanup EXIT

require_root
require_tools

write_kvs_conf "$KVS_CONF" "$KVS_PORT" proactor rdb
write_redis_conf "$REDIS_CONF" "$REDIS_PORT" off
echo "dir ${WORK}" >> "$REDIS_CONF"   # redis 落盘进临时目录, 防 bgsave 污染项目根目录 dump.rdb

# 单轮: 后台按间隔触发 BGSAVE + 前台压测 → 打印 "qps p50 p99"
run_one() { # $1=port  $2=interval
    local port=$1 interval=$2
    local out="$WORK/bench_${interval//./_}.out"

    # 另一个进程: 压测开始 TRIG_START 秒后, 每 $interval 秒触发一次
    ( sleep "$TRIG_START"
      while true; do
          redis-cli -h 127.0.0.1 -p "$port" bgsave >/dev/null 2>&1 || true
          sleep "$interval"
      done ) &
    local trig=$!

    # 压测进程: 灌数据 (压测结束即触发循环被杀); timeout 兜底防挂死
    timeout 900 taskset -c "$CLIENT_CORES" redis-benchmark -h 127.0.0.1 -p "$port" \
        -c "$CONN" -n "$LOAD" -r "$LOAD" -P "$P" \
        SET __rand_int__ __rand_int__ >"$out" 2>/dev/null || true

    kill "$trig" 2>/dev/null || true

    local qps p50 p99
    qps=$(grep -E "requests per second" "$out" | head -1 | grep -oE '[0-9]+([.][0-9]+)?' || echo "0")
    # 默认百分位分布无 99.000% bin (是 99.219%), 从 Summary 段 latency summary 读精确 p50/p99
    p50=$(awk '/latency summary/{getline; getline; print $3}' "$out" || echo "N/A")
    p99=$(awk '/latency summary/{getline; getline; print $5}' "$out" || echo "N/A")
    echo "$qps $p50 $p99"
}

# ── 主流程: 数据直接写入结果文件 ─────────────────────────────────────────
> "$RESULT_FILE"
{
    echo "bench_bgsave — $(date '+%Y-%m-%d %H:%M:%S')"
    echo "load=$LOAD  c=$CONN  P=$P  后台每 ${INTERVALS[*]} 秒触发一次 bgsave"
    echo "单位: QPS=requests/s, p50/p99=毫秒"
    echo ""
    printf "  %-7s | %-8s | %12s | %8s | %8s\n" "系统" "间隔(s)" "QPS" "p50" "p99"
} >> "$RESULT_FILE"

for sys in "${SYSTEMS[@]}"; do
    for interval in "${INTERVALS[@]}"; do
        stop_all                     # 每轮独立启动
        rm -f "$WORK/dump.rdb" "$WORK/dump.rdb.tmp"   # 清上一轮残留 RDB, 防 redis 启动时加载→-LOADING 全败
        if [ "$sys" = kvstore ]; then
            start_kvs "$KVS_CONF" "$KVS_PORT"; port=$KVS_PORT
        else
            start_redis "$REDIS_CONF"; port=$REDIS_PORT
        fi
        res=$(run_one "$port" "$interval") || true
        read -r qps p50 p99 <<< "$res"
        qps=${qps:-0}; p50=${p50:-N/A}; p99=${p99:-N/A}
        line="$(printf "  %-7s | %-8s | %12s | %8s | %8s" "$sys" "$interval" "$qps" "$p50" "$p99")"
        echo "$line"
        echo "$line" >> "$RESULT_FILE"
    done
done
stop_all

echo ""
echo "结果已写入: $RESULT_FILE"
