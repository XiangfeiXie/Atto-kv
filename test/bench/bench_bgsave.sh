#!/bin/bash
# =============================================================================
# bench_bgsave.sh — 大负载下 BGSAVE 对 kvstore(proactor) / Redis 吞吐影响
#
# 测试方式参考 run_official_redis_rdb_bench.sh:
#   - 海量 SET 压测 (LOAD 条, -r LOAD) 撑大内存, 生成大 RDB
#   - 压测开始 TRIG_START 秒后, 后台每 TRIG_SLEEP 秒触发一次 BGSAVE
#     (保存中重复触发是 no-op; 触发持续到压测结束)
#   - 指标 = redis-benchmark 整轮 requests per second, 与无持久化基线对比
#
# 场景: kvstore(proactor, persistence rdb) / redis(save "", aof off)
#       × none(基线) | bgsave      (每个组合独立全新实例)
#
# 固定参数 (默认即可, 无需设置):
#   LOAD=10000000  SET (-c 50 -P 16 -r 10000000)   大负载
#   TRIG_START=1.5  首触发延迟;  TRIG_SLEEP=0.5  触发间隔
#
# 用法:
#   sudo ./test/bench/bench_bgsave.sh          # 默认参数
#   sudo ./test/bench/bench_bgsave.sh --small  # 快速验证 (LOAD=1M)
#
# 输出: 终端汇总表 + test/bench/results_bgsave.txt
# 说明: common.sh 作为共享头保持不变; 本脚本自建临时配置, 不依赖 conf/ 目录。
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

WORK="$(mktemp -d /tmp/rdbbench.XXXXXX)"
KVS_CONF="${WORK}/kvs_rdb.conf"
RDB_FILE="${PROJECT_DIR}/build/dump.rdb"
RDB_TMP="${PROJECT_DIR}/build/dump.rdb.tmp"

cleanup() {
    pkill -x kvstore 2>/dev/null || true
    pkill -x redis-server 2>/dev/null || true
    pkill -x redis-benchmark 2>/dev/null || true
    rm -f "$RDB_FILE" "$RDB_TMP"
    rm -f dump.rdb dump.rdb.tmp
    rm -rf "$WORK"
    rm -rf "$LOG_DIR"
}
trap cleanup EXIT

# ── 固定参数 (默认即可) ─────────────────────────────────────────────────
LOAD=10000000        # 压测条数, 同时是 -r 随机 key 范围 (参考脚本)
CONN=50              # redis-benchmark -c
P=16                 # 每请求 pipeline (参考脚本)
TRIG_START=1.5       # 首触发延迟 (参考脚本)
TRIG_SLEEP=0.004       # 触发间隔 (参考脚本); 触发持续到压测结束

MODES=(none bgsave)
SYSTEMS=(kvstore redis)

# ── 参数解析 (仅 --small) ───────────────────────────────────────────────
help() {  # 只打印头部注释块 (到第一个非 # 行即停)
    awk '!/^#/ {exit} NR>=2 {sub(/^# */, ""); print}' "$0"
    exit 0
}
for a in "$@"; do
    case "$a" in
        --small) LOAD=1000000 ;;
        -h|--help) help ;;
        *) echo "未知选项: $a (用法: $0 [--small])" >&2; exit 1 ;;
    esac
done

require_root
require_tools

RESULT_FILE="${PROJECT_DIR}/test/bench/results_bgsave.txt"

# ── kvstore 临时配置 (proactor + persistence rdb) ───────────────────────
cat > "$KVS_CONF" <<-KCONF
port 2000
network proactor
persistence rdb
repl_role off
expire off
KCONF

# ── 服务器管理 ──────────────────────────────────────────────────────────
stop_all() {
    pkill -x kvstore 2>/dev/null || true
    pkill -x redis-server 2>/dev/null || true
    sleep 0.5
}

start_kvs_tmp() {
    rm -f "$RDB_FILE" "$RDB_TMP" dump.rdb dump.rdb.tmp
    taskset -c "$SERVER_CORES" "$KVS_BIN" "$KVS_CONF" >"$WORK/kvs.log" 2>&1 &
    local pid=$!
    sleep 0.2
    if ! kill -0 "$pid" 2>/dev/null; then
        err "kvstore 启动失败"
        tail -20 "$WORK/kvs.log"; exit 1
    fi
    if ! wait_for_port 127.0.0.1 "$KVS_PORT" 10; then
        err "kvstore 启动超时 (port=$KVS_PORT)"
        tail -20 "$WORK/kvs.log"; exit 1
    fi
    log "kvstore 已启动 (proactor, persistence rdb, pid=$pid)"
}

start_redis_tmp() {
    stop_all
    start_redis
    redis_configure none     # save "" + appendonly no + FLUSHALL
}

# ── 压测 ─────────────────────────────────────────────────────────────────
bench_mode() { # $1=system $2=port $3=mode(none|bgsave) ; prints 整轮 rps
    local sys=$1 port=$2 mode=$3
    local out="$WORK/bench_${sys}_${mode}.out"
    local trig_pid=""

    # 后台触发: 压测开始 TRIG_START 秒后, 每 TRIG_SLEEP 秒触发一次, 直到压测结束。
    # (保存中重复触发是 no-op)
    if [ "$mode" != none ]; then
        ( sleep "$TRIG_START"
          while true; do
              redis-cli -h 127.0.0.1 -p "$port" bgsave >/dev/null 2>&1 || true
              sleep "$TRIG_SLEEP" 2>/dev/null || sleep 0.5
          done ) &
        trig_pid=$!
    fi

    taskset -c "$CLIENT_CORES" redis-benchmark -h 127.0.0.1 -p "$port" \
        -c "$CONN" -n "$LOAD" -r "$LOAD" -P "$P" \
        SET __rand_int__ __rand_int__ 2>/dev/null >"$out" || true

    [ -n "$trig_pid" ] && kill "$trig_pid" 2>/dev/null || true

    # 整轮吞吐: 取 "requests per second" 前的数值 (吞吐行)
    grep -E "requests per second" "$out" | head -1 | grep -oE '[0-9]+([.][0-9]+)?' || true
}

# ── 降幅 (1 - on/off) ───────────────────────────────────────────────────
drop_pct() { # $1=on $2=off ; prints "12.3" 或 "N/A" (rps 为浮点, 用 bc 比较)
    local on=$1 off=$2
    if [ "$(echo "$off > 0" | bc -l 2>/dev/null)" = "1" ] && \
       [ "$(echo "$on >= 0" | bc -l 2>/dev/null)" = "1" ]; then
        awk "BEGIN{printf \"%.1f\", (1 - $on/$off)*100}"
    else
        echo "N/A"
    fi
}

# ── 主流程 ──────────────────────────────────────────────────────────────
declare -A RAWS

echo "# bench_bgsave — $(date '+%Y-%m-%d %H:%M:%S')"
echo "# systems: kvstore(proactor)/redis × mode(none/bgsave)  load=$LOAD  c=$CONN  P=$P  trig=${TRIG_START}s+${TRIG_SLEEP}s"
echo "# 指标: redis-benchmark 整轮 requests per second; 压测期间后台持续触发 BGSAVE"

for sys in "${SYSTEMS[@]}"; do
    for mode in "${MODES[@]}"; do
        stop_all
        if [ "$sys" = kvstore ]; then
            start_kvs_tmp; port=$KVS_PORT
        else
            start_redis_tmp; port=$REDIS_PORT
        fi
        rps=$(bench_mode "$sys" "$port" "$mode") || rps=""
        RAWS["${sys}_${mode}"]=$rps
        echo "$sys $mode rps=${rps:-0}"
    done
done
stop_all

# ── 汇总表: 一次生成, 终端与文件同源 ────────────────────────────────────
{
    echo "bench_bgsave — $(date '+%Y-%m-%d %H:%M:%S')"
    echo "systems: kvstore(proactor)/redis × mode(none/bgsave)  load=$LOAD  c=$CONN  P=$P  trig=${TRIG_START}s+${TRIG_SLEEP}s"
    echo "单位: requests per second (整轮); 降幅 = 1 - bgsave/none"
    echo ""

    printf "  %-7s | %12s | %12s | %9s\n" \
        "系统" "none" "bgsave" "降幅"
    echo "  ────────┼──────────────┼──────────────┼──────────"
    for sys in "${SYSTEMS[@]}"; do
        n=${RAWS["${sys}_none"]};  [ -n "$n" ] || n="-"
        b=${RAWS["${sys}_bgsave"]}; [ -n "$b" ] || b="-"
        printf "  %-7s | %12s | %12s | %9s\n" \
            "$sys" "$n" "$b" "$(drop_pct "$b" "$n")"
    done
    echo ""
} | tee "$RESULT_FILE"

echo "结果已保存: $RESULT_FILE"
