#!/bin/bash
# =============================================================================
# bench_qps.sh — proactor / redis 的 pipeline SET QPS 压测（基线与 AOF 二合一）
#
# 同一套系统（kvstore proactor / redis）× 同一套方法（本地回环，单客户端 QPS），
# 通过 --aof 开关选择测量目标：
#   --aof off（默认）→ 基准：kvstore(persistence none) / redis(save "" aof no)
#   --aof on         → AOF：kvstore(persistence aof everysec) / redis(appendonly yes)
#
# 指标：只测 SET 的 QPS。服务器与压测客户端同机、走 127.0.0.1 回环，
#       客户端只跑 1 个 redis-benchmark 进程（单线程，绑单核）。
#
# 用法（项目根目录执行）:
#   ./test/bench/bench_qps.sh               # 基准
#   ./test/bench/bench_qps.sh --aof on      # AOF
#   ./test/bench/bench_qps.sh 1 4 16 160    # 指定 P
#
# 输出: 终端 + test/bench/results_qps.txt
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

# 本脚本覆盖的默认参数（单核公平对比）
SERVER_CORES="5"
CLIENT_CORES="0"
NCLIENTS=1                                            # 单客户端：1 个 benchmark 进程
IFS=',' read -r -a CLIENT_CORE_LIST <<< "$CLIENT_CORES"
PIPELINES=(1 4 8 16 20 40 80 160)
AOF=off

WORK="$(mktemp -d /tmp/qpsbench.XXXXXX)"
KVS_OFF_CONF="${WORK}/kvs_off.conf"
KVS_ON_CONF="${WORK}/kvs_on.conf"
REDIS_OFF_CONF="${WORK}/redis_off.conf"
REDIS_ON_CONF="${WORK}/redis_on.conf"
RESULT_FILE="test/bench/results_qps.txt"

cleanup() {
    stop_all
    rm -f build/dump.rdb build/dump.rdb.tmp build/appendonly.aof
    rm -rf appendonlydir
    rm -f "${LOG_DIR}"/bench_c*.csv
    rm -rf "$WORK"
}
trap cleanup EXIT

# ── 参数解析 (仅 AOF 开关 + P) ─────────────────────────────────────────────
help() {  # 只打印头部注释块 (到第一个非 # 行即停)
    awk '!/^#/ {exit} NR>=2 {sub(/^# */, ""); print}' "$0"
    exit 0
}
USER_PIPELINES=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --aof)  AOF="$2"; shift 2 ;;
        -h|--help) help ;;
        -*)         echo "未知选项: $1" >&2; exit 1 ;;
        *)          USER_PIPELINES+=("$1"); shift ;;
    esac
done
if [ "${#USER_PIPELINES[@]}" -gt 0 ]; then
    PIPELINES=("${USER_PIPELINES[@]}")
fi
case "$AOF" in
    off|on) ;;
    *) echo "错误 --aof: $AOF (应为 off|on)" >&2; exit 1 ;;
esac

require_tools

# ── 生成临时配置 ───────────────────────────────────────────────────────────
write_kvs_conf "$KVS_OFF_CONF" "$KVS_PORT" proactor none
write_kvs_conf "$KVS_ON_CONF"  "$KVS_PORT" proactor aof "aof_fsync_policy everysec"
write_redis_conf "$REDIS_OFF_CONF" "$REDIS_PORT" off
write_redis_conf "$REDIS_ON_CONF"  "$REDIS_PORT" on

if [ "$AOF" = on ]; then
    MODE_LABEL="AOF(aof everysec)"
else
    MODE_LABEL="基准(persistence none)"
fi
SYSTEMS=(proactor redis)

# ── 启动指定系统 (端口写入 CURRENT_PORT) ───────────────────────────────────
CURRENT_PORT=0
start_one() { # $1 = proactor|redis
    if [ "$1" = proactor ]; then
        if [ "$AOF" = on ]; then
            start_kvs "$KVS_ON_CONF" "$KVS_PORT"
        else
            start_kvs "$KVS_OFF_CONF" "$KVS_PORT"
        fi
        CURRENT_PORT=$KVS_PORT
    else
        if [ "$AOF" = on ]; then
            start_redis "$REDIS_ON_CONF"
        else
            start_redis "$REDIS_OFF_CONF"
        fi
        CURRENT_PORT=$REDIS_PORT
    fi
}

# ── 主流程 (只测 SET, 直接陈列, 无比值) ────────────────────────────────────
declare -A RAWS

echo "# bench_qps — $(date '+%Y-%m-%d %H:%M:%S')"
echo "# aof=$AOF (${MODE_LABEL})  systems=${SYSTEMS[*]}  test=set  nclients=$NCLIENTS  conn=$CONN  ops=$OPS  data=$DATA"
echo "# pipelines: ${PIPELINES[*]}  → 指标: SET QPS (聚合吞吐)"

for eng in "${SYSTEMS[@]}"; do
    stop_all
    start_one "$eng"
    port=$CURRENT_PORT

    # SET 预热 (每系统只做一次, 复用于所有 P)
    echo "$eng warmup(set $WARMUP_OPS)..."
    warmup_server "$port"

    for P in "${PIPELINES[@]}"; do
        q=$(bench_qps "$port" set "$P")
        RAWS["${eng}_${P}"]="$q"
        echo "$eng P=$P set qps=$q"
    done
done
stop_all

# ── 汇总表: 终端与文件同源 ─────────────────────────────────────────────────
{
    echo "bench_qps — $(date '+%Y-%m-%d %H:%M:%S')"
    echo "aof=$AOF (${MODE_LABEL})  systems=${SYSTEMS[*]}  test=set  nclients=$NCLIENTS  conn=$CONN  ops=$OPS  data=$DATA"
    echo "pipelines: ${PIPELINES[*]}   (单位: SET QPS)"
    echo ""
    printf "  %-6s | %12s | %12s\n" "P" "proactor" "redis"
    for P in "${PIPELINES[@]}"; do
        printf "  P%-4s | %12s | %12s\n" "$P" "${RAWS["proactor_${P}"]:-N/A}" "${RAWS["redis_${P}"]:-N/A}"
    done
    echo ""
} | tee "$RESULT_FILE"

echo "结果已保存: $RESULT_FILE"
