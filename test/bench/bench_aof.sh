#!/bin/bash
# =============================================================================
# bench_aof.sh — AOF 开关下 proactor / Redis 的 pipeline QPS 对比
#
# 场景 (2 系统 × 2 AOF 状态), 只测 proactor 对比 redis:
#   kvs-aof-off    kvstore (proactor) + persistence none
#   kvs-aof-on     kvstore (proactor) + persistence aof (aof_fsync_policy everysec)
#   redis-aof-off  Redis + appendonly no
#   redis-aof-on   Redis + appendonly yes (appendfsync everysec)
#
# 指标: SET/GET 的聚合 QPS (NCLIENTS 个 redis-benchmark 进程求和 —— 避免
#   单进程高 P 下客户端解析回复成为瓶颈), 并给出 AOF 开关开销 (1 - on/off)。
#
# 参数: 仅保留 P 大小设置, 其余用默认值 (单轮直出)。
# 用法:
#   sudo ./test/bench/bench_aof.sh                # 默认参数
#   sudo ./test/bench/bench_aof.sh 1 4 16 160     # 指定 pipeline 深度
#   sudo ./test/bench/bench_aof.sh --small        # 快速验证
#
# 输出: 终端汇总表 + test/bench/results_aof.txt
# 说明: common.sh 作为共享头保持不变; 本脚本自建临时配置, 不依赖 conf/ 目录。
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

# 配置文件/日志放临时目录, 退出时随 cleanup 一起删除。
# cleanup + trap 在创建 $WORK 后立即注册: 参数检查等早退路径也能清理。
WORK="$(mktemp -d /tmp/aofbench.XXXXXX)"
KVS_AOF_OFF_CONF="${WORK}/kvs_aof_off.conf"
KVS_AOF_ON_CONF="${WORK}/kvs_aof_on.conf"

cleanup() {
    pkill -x kvstore 2>/dev/null || true
    pkill -x redis-server 2>/dev/null || true
    pkill -x redis-benchmark 2>/dev/null || true
    rm -rf "$WORK"
    rm -rf "$LOG_DIR"
    # Redis 7 AOF 落盘会生成 appendonlydir/ (manifest + base.rdb + incr.aof),
    # 在 cwd 和 build/ 两处都清掉
    rm -rf appendonlydir "${PROJECT_DIR}/build/appendonlydir"
}
trap cleanup EXIT

# ── 默认参数 (固定) ────────────────────────────────────────────────────
PIPELINES=(1 4 8 16 20 40 80 160)   # 与 bench_pipeline 一致
TEST=both                           # SET+GET (AOF 影响写路径, GET 看开销)
NCLIENTS=2                          # 适中: 够对比, 不追极限吞吐
OPS=100000                          # 每个客户端的请求数
CONN=50                             # redis-benchmark -c (每进程并发)
DATA=16                             # value 字节
WARMUP_OPS=1000000                  # GET 前的 SET 预热量

SCENARIOS=(kvs-aof-off kvs-aof-on redis-aof-off redis-aof-on)
IFS=',' read -r -a CLIENT_CORE_LIST <<<"$CLIENT_CORES"

# ── 参数解析 (仅 P) ─────────────────────────────────────────────────────
help() {  # 只打印头部注释块 (到第一个非 # 行即停)
    awk '!/^#/ {exit} NR>=2 {sub(/^# */, ""); print}' "$0"
    exit 0
}
USER_PIPELINES=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --small)    PIPELINES=(1 8 160); OPS=50000; WARMUP_OPS=200000; shift ;;
        -h|--help)  help ;;
        -*)         echo "未知选项: $1" >&2; exit 1 ;;
        *)          USER_PIPELINES+=("$1"); shift ;;
    esac
done
if [ "${#USER_PIPELINES[@]}" -gt 0 ]; then
    PIPELINES=("${USER_PIPELINES[@]}")
fi

require_root
require_tools

RESULT_FILE="${PROJECT_DIR}/test/bench/results_aof.txt"

# ── 生成 kvstore 临时配置 (proactor, 每次重建) ─────────────────────────
cat > "$KVS_AOF_OFF_CONF" <<-KCONF
port 2000
network proactor
persistence none
repl_role off
expire off
KCONF
cat > "$KVS_AOF_ON_CONF" <<-KCONF
port 2000
network proactor
persistence aof
aof_fsync_policy everysec
repl_role off
expire off
KCONF

# ── 服务器管理 ──────────────────────────────────────────────────────────
start_kvs_tmp() { # $1 = 临时 conf
    local conf=$1
    # 清理旧持久化文件 (build/ 与 cwd 两处都清)
    rm -f "${PROJECT_DIR}/build/dump.rdb" "${PROJECT_DIR}/build/dump.rdb.tmp" \
          "${PROJECT_DIR}/build/appendonly.aof"
    rm -f dump.rdb dump.rdb.tmp appendonly.aof
    taskset -c "$SERVER_CORES" "$KVS_BIN" "$conf" >"$WORK/kvs.log" 2>&1 &
    local pid=$!
    sleep 0.2
    if ! kill -0 "$pid" 2>/dev/null; then
        err "kvstore 启动失败 ($conf)"
        tail -20 "$WORK/kvs.log"
        exit 1
    fi
    if ! wait_for_port 127.0.0.1 "$KVS_PORT" 10; then
        err "kvstore 启动超时 (port=$KVS_PORT)"
        tail -20 "$WORK/kvs.log"
        exit 1
    fi
    log "kvstore 已启动 (${conf##*/}, pid=$pid)"
}

stop_all() {
    pkill -x kvstore 2>/dev/null || true
    pkill -x redis-server 2>/dev/null || true
    sleep 0.5
    # 清掉 Redis 7 的 AOF 落盘目录, 避免上次 aof-on 场景残留 AOF 被重放
    rm -rf appendonlydir
}

start_scenario() { # $1 = 场景名
    local s=$1
    stop_all
    case "$s" in
    kvs-aof-on)    start_kvs_tmp "$KVS_AOF_ON_CONF" ;;
    kvs-aof-off)   start_kvs_tmp "$KVS_AOF_OFF_CONF" ;;
    redis-aof-on)  start_redis; redis_configure aof ;;
    redis-aof-off) start_redis; redis_configure none ;;
    esac
}

scenario_port() { # $1 = 场景名
    case "$1" in redis-*) echo "$REDIS_PORT" ;; *) echo "$KVS_PORT" ;; esac
}

# ── 压测 ─────────────────────────────────────────────────────────────────
warmup() { # $1 = port (预热跑在客户端核上, 不计入 QPS 统计)
    taskset -c "$CLIENT_CORES" redis-benchmark \
        -h 127.0.0.1 -p "$1" -t set -n "$WARMUP_OPS" \
        -c 50 -P 100 -d "$DATA" -q >/dev/null 2>&1 || true
}

bench() { # $1 = port  $2 = test(get/set)  $3 = pipeline P ; prints aggregate QPS
    local port=$1 t=$2 P=$3 pids=()
    local cli_to=$((OPS / 10000 + 60))   # P1 下每客户端 ~20K qps 的安全余量
    for i in $(seq 0 $((NCLIENTS - 1))); do
        taskset -c "${CLIENT_CORE_LIST[$((i % ${#CLIENT_CORE_LIST[@]}))]}" timeout "$cli_to" \
            redis-benchmark -h 127.0.0.1 -p "$port" -t "$t" -n "$OPS" \
            -c "$CONN" -P "$P" -d "$DATA" --csv 2>/dev/null >"$WORK/bench_c$i.csv" &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
    # 各客户端文件: CSV 第 2 列 = requests/sec; 服务器为瓶颈时即服务器 QPS。
    # 固定文件名写 $WORK, 每次调用覆盖, 退出时随 $WORK 一并清理。
    awk -F',' 'FNR>1 {gsub(/"/,"",$2); sum+=$2} END {printf "%.0f", sum}' \
        "$WORK"/bench_c*.csv || true
}

aof_cost() { # $1=off $2=on ; prints "12.3%" 或 "N/A"
    local off=$1 on=$2
    if [ "$off" -gt 0 ] 2>/dev/null && [ "$on" -ge 0 ] 2>/dev/null; then
        awk "BEGIN{printf \"%.1f%%\", (1 - $on/$off)*100}"
    else
        echo "N/A"
    fi
}

# ── 主流程 (单轮直出) ───────────────────────────────────────────────────
declare -A RAWS   # 只存实际跑过的 (场景, P, t)

tests=()
for t in get set; do
    [ "$TEST" = "$t" ] || [ "$TEST" = both ] || continue
    tests+=("$t")
done

echo "# bench_aof — $(date '+%Y-%m-%d %H:%M:%S')"
echo "# scenarios: proactor/redis × AOF off/on  test=$TEST  nclients=$NCLIENTS  conn=$CONN  ops=$OPS  data=$DATA"
echo "# pipelines: ${PIPELINES[*]}"
echo "# 指标: QPS (聚合吞吐, 服务器为瓶颈时即服务器 QPS)"

for s in "${SCENARIOS[@]}"; do
    start_scenario "$s"
    port=$(scenario_port "$s")

    # GET 前先 SET 预热 (每场景只做一次, 复用于所有 P)
    if [ "$TEST" != set ]; then
        echo "$s warmup(set $WARMUP_OPS)..."
        warmup "$port"
    fi

    for P in "${PIPELINES[@]}"; do
        for t in "${tests[@]}"; do
            q=$(bench "$port" "$t" "$P")
            RAWS["${s}_${P}_${t}"]="$q"
            echo "$s P=$P $t qps=$q"
        done
    done
done

# ── 汇总表: 一次生成, 终端与文件同源 ────────────────────────────────────
{
    echo "bench_aof — $(date '+%Y-%m-%d %H:%M:%S')"
    echo "scenarios: proactor/redis × AOF off/on  test=$TEST  nclients=$NCLIENTS  conn=$CONN  ops=$OPS  data=$DATA"
    echo "pipelines: ${PIPELINES[*]}   (单位: QPS; AOF开销 = 1 - on/off)"
    echo ""

    for t in "${tests[@]}"; do
        echo "════════════════ $t (QPS, ${NCLIENTS}×${OPS}) ════════════════"
        printf "  %-6s | %12s | %12s | %9s | %12s | %12s | %9s\n" \
            "P" "kvs-off" "kvs-on" "AOF开销" "redis-off" "redis-on" "AOF开销"
        for P in "${PIPELINES[@]}"; do
            koff=${RAWS["kvs-aof-off_${P}_${t}"]}
            kon=${RAWS["kvs-aof-on_${P}_${t}"]}
            roff=${RAWS["redis-aof-off_${P}_${t}"]}
            ron=${RAWS["redis-aof-on_${P}_${t}"]}
            printf "  P%-4s | %12s | %12s | %9s | %12s | %12s | %9s\n" \
                "$P" "$koff" "$kon" "$(aof_cost "$koff" "$kon")" \
                "$roff" "$ron" "$(aof_cost "$roff" "$ron")"
        done
        echo ""
    done
} | tee "$RESULT_FILE"

echo "结果已保存: $RESULT_FILE"
stop_all
