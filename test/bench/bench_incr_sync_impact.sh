#!/bin/bash
# =============================================================================
# bench_incr_sync_impact.sh — 增量同步开关对 master 写吞吐的影响 (master 侧脚本)
#
# 协作方式: 本脚本跑在 master 主机 (启动 master + 测量);
#           从机 (slave / redis 副本) 由你手工启动 —— 每轮 P 起一个全新从机,
#           脚本轮询从机端口确认同步 (__incr_probe 到达从机) 后再测量。
#
# 引擎选择: --engine proactor (kvstore, 默认) | --engine redis
#   proactor: kvstore master (proactor, repl_role master) + 手工 slave
#   redis:    redis-server master + 手工副本 (REPLICAOF)
#
# 流程:
#   1) 关闭增量同步: master 单独跑 (proactor: incr_sync off) 测全部 P → 基线
#   2) 开启增量同步: master (incr_sync on), 每轮 P 请你启动从机, 同步后测量
#
# 指标: SET/GET 的聚合 QPS (NCLIENTS 个 redis-benchmark 进程求和), 开销 = 1 - on/off。
#   SET 体现同步开销 (写命令传播); GET 只读不传播, 应与基线持平。
#
# 用法:
#   sudo ./test/bench/bench_incr_sync_impact.sh                      # proactor
#   sudo ./test/bench/bench_incr_sync_impact.sh --engine redis       # redis
#   sudo ./test/bench/bench_incr_sync_impact.sh 1 4 16 160           # 指定 P
#   sudo ./test/bench/bench_incr_sync_impact.sh --small              # 快速验证
#
# 输出: 终端汇总表 + test/bench/results_incr_sync_impact.txt
# 说明: common.sh 作为共享头保持不变; 本脚本自建临时配置, 不依赖 conf/ 目录。
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

WORK="$(mktemp -d /tmp/incrbench.XXXXXX)"
KVS_MASTER_OFF_CONF="${WORK}/kvs_master_off.conf"
KVS_MASTER_ON_CONF="${WORK}/kvs_master_on.conf"
MASTER_LOG="${WORK}/master.log"
# 从机配置文件写到固定路径, 供你在另一个终端手工启动 (脚本退出后仍保留)
KVS_SLAVE_CONF="/tmp/kvs_incr_slave.conf"

SLAVE_CORES="4,5"            # 从机绑核 (master 0,3 / client 1,2 之外)
REDIS_REPLICA_PORT=6380

cleanup() {
    pkill -x kvstore 2>/dev/null || true
    pkill -x redis-server 2>/dev/null || true
    pkill -x redis-benchmark 2>/dev/null || true
    rm -f "${PROJECT_DIR}/build/dump.rdb" "${PROJECT_DIR}/build/dump.rdb.tmp" \
          "${PROJECT_DIR}/build/appendonly.aof"
    rm -f dump.rdb dump.rdb.tmp appendonly.aof
    rm -rf appendonlydir "${PROJECT_DIR}/build/appendonlydir"
    rm -rf "$WORK"
    rm -rf "$LOG_DIR"
}
trap cleanup EXIT

# ── 默认参数 (固定) ────────────────────────────────────────────────────
PIPELINES=(1 4 8 16 20 40 80 160)
TEST=both                           # SET+GET (同步影响写路径, GET 看开销)
NCLIENTS=2                          # 适中: 够对比, 不追极限吞吐
OPS=100000                          # 每个客户端的请求数
CONN=50                             # redis-benchmark -c (每进程并发)
DATA=16                             # value 字节
WARMUP_OPS=1000000                  # GET 前的 SET 预热量
SYSTEM=proactor                     # proactor | redis

# ── 参数解析 (引擎选择 + P) ─────────────────────────────────────────────
help() {  # 只打印头部注释块 (到第一个非 # 行即停)
    awk '!/^#/ {exit} NR>=2 {sub(/^# */, ""); print}' "$0"
    exit 0
}
USER_PIPELINES=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --engine)   SYSTEM="$2"; shift 2 ;;
        --small)    PIPELINES=(1 8 160); OPS=50000; WARMUP_OPS=200000; shift ;;
        -h|--help)  help ;;
        -*)         echo "未知选项: $1" >&2; exit 1 ;;
        *)          USER_PIPELINES+=("$1"); shift ;;
    esac
done
if [ "${#USER_PIPELINES[@]}" -gt 0 ]; then
    PIPELINES=("${USER_PIPELINES[@]}")
fi
case "$SYSTEM" in
    proactor|kvs) SYSTEM=proactor ;;
    redis)        SYSTEM=redis ;;
    *)            echo "错误 --engine: $SYSTEM (proactor|redis)" >&2; exit 1 ;;
esac

require_root
require_tools

RESULT_FILE="${PROJECT_DIR}/test/bench/results_incr_sync_impact.txt"

# ── 按引擎确定端口 / 从机启动命令 ───────────────────────────────────────
if [ "$SYSTEM" = proactor ]; then
    MASTER_PORT_NUM=$KVS_PORT             # 2000
    SLAVE_POLL_PORT=$KVS_SLAVE_PORT       # 2002
    SLAVE_CMD="sudo taskset -c ${SLAVE_CORES} ${KVS_BIN} ${KVS_SLAVE_CONF}"
    ENGINE_LABEL="kvstore(proactor)"
else
    MASTER_PORT_NUM=$REDIS_PORT           # 6379
    SLAVE_POLL_PORT=$REDIS_REPLICA_PORT   # 6380
    SLAVE_CMD="redis-server --port ${REDIS_REPLICA_PORT} --save \"\" --appendonly no --replicaof 127.0.0.1 ${REDIS_PORT} --daemonize yes"
    ENGINE_LABEL="redis"
fi

# ── 生成配置文件 (proactor, 每次重建) ─────────────────────────────────
write_kvs_master_conf() { # $1 = conf  $2 = incr_sync on/off
    cat > "$1" <<-KCONF
port ${KVS_PORT}
network proactor
persistence none
expire off
repl_role master
incr_sync $2
fullsync_transport sendfile
fullsync_listen_ip 127.0.0.1
fullsync_port 2101
KCONF
}
write_kvs_master_conf "$KVS_MASTER_OFF_CONF" off
write_kvs_master_conf "$KVS_MASTER_ON_CONF" on

cat > "$KVS_SLAVE_CONF" <<-KCONF
port ${KVS_SLAVE_PORT}
network proactor
persistence none
expire off
repl_role slave
master_port ${KVS_PORT}
incr_sync on
fullsync_master_ip 127.0.0.1
fullsync_port 2101
fullsync_transport sendfile
KCONF

# ── 服务器管理 ──────────────────────────────────────────────────────────
stop_all() {
    pkill -x kvstore 2>/dev/null || true
    pkill -x redis-server 2>/dev/null || true
    sleep 0.5
    rm -f "${PROJECT_DIR}/build/dump.rdb" "${PROJECT_DIR}/build/dump.rdb.tmp" \
          "${PROJECT_DIR}/build/appendonly.aof"
    rm -f dump.rdb dump.rdb.tmp appendonly.aof
    rm -rf appendonlydir
}

start_master() { # $1 = kvstore master conf (仅 proactor 用); redis 走 common.sh
    if [ "$SYSTEM" = proactor ]; then
        local conf=$1
        taskset -c "$SERVER_CORES" "$KVS_BIN" "$conf" >"$MASTER_LOG" 2>&1 &
        local pid=$!
        sleep 0.2
        if ! kill -0 "$pid" 2>/dev/null; then
            err "kvstore master 启动失败"
            tail -20 "$MASTER_LOG"; exit 1
        fi
        if ! wait_for_port 127.0.0.1 "$KVS_PORT" 10; then
            err "kvstore master 启动超时 (port=$KVS_PORT)"
            tail -20 "$MASTER_LOG"; exit 1
        fi
        log "kvstore master 已启动 (${conf##*/}, pid=$pid)"
    else
        start_redis
        redis_configure none
    fi
}

# ── 同步确认: 写探测 key 到 master, 轮询从机端口直到出现 ────────────────
wait_slave_synced() {
    redis-cli -h 127.0.0.1 -p "$MASTER_PORT_NUM" SET __incr_probe 1 >/dev/null 2>&1 || true
    local t0=$(date +%s)
    while [ "$(date +%s)" -lt $((t0 + 60)) ]; do
        [ "$(redis-cli -h 127.0.0.1 -p "$SLAVE_POLL_PORT" GET __incr_probe 2>/dev/null)" = "1" ] && return 0
        sleep 0.5
    done
    warn "从机未同步 __incr_probe (port=$SLAVE_POLL_PORT)"
    return 1
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
        taskset -c "${CLIENT_CORES%%,*},${CLIENT_CORES##*,}" timeout "$cli_to" \
            redis-benchmark -h 127.0.0.1 -p "$port" -t "$t" -n "$OPS" \
            -c "$CONN" -P "$P" -d "$DATA" --csv 2>/dev/null >"$WORK/bench_c$i.csv" &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
    # 各客户端文件: CSV 第 2 列 = requests/sec; 服务器为瓶颈时即服务器 QPS。
    awk -F',' 'FNR>1 {gsub(/"/,"",$2); sum+=$2} END {printf "%.0f", sum}' \
        "$WORK"/bench_c*.csv || true
}

repl_cost() { # $1=off $2=on ; prints "12.3%" 或 "N/A"
    local off=$1 on=$2
    if [ "$off" -gt 0 ] 2>/dev/null && [ "$on" -ge 0 ] 2>/dev/null; then
        awk "BEGIN{printf \"%.1f%%\", (1 - $on/$off)*100}"
    else
        echo "N/A"
    fi
}

# ── 主流程 ──────────────────────────────────────────────────────────────
declare -A RAWS   # off_<P>_<t> / on_<P>_<t>

tests=()
for t in get set; do
    [ "$TEST" = "$t" ] || [ "$TEST" = both ] || continue
    tests+=("$t")
done

echo "# bench_incr_sync_impact — $(date '+%Y-%m-%d %H:%M:%S')"
echo "# engine=${ENGINE_LABEL}  test=$TEST  nclients=$NCLIENTS  conn=$CONN  ops=$OPS  data=$DATA"
echo "# pipelines: ${PIPELINES[*]}"
echo "# 指标: QPS (聚合吞吐); SET 体现增量同步开销, GET 应为基线水平"
echo ""
echo "从机启动命令 (请记下, 每轮 P 在另一个终端运行):"
echo "  ${SLAVE_CMD}"
echo ""

# ── 阶段 1: 关闭增量同步 (master 单独, 无从机) → 基线 ──────────────────
echo "━━━━ 阶段 1: 关闭增量同步 (基线, master 单独运行) ━━━━"
start_master "$KVS_MASTER_OFF_CONF"
warmup "$MASTER_PORT_NUM"
for P in "${PIPELINES[@]}"; do
    for t in "${tests[@]}"; do
        q=$(bench "$MASTER_PORT_NUM" "$t" "$P")
        RAWS["off_${P}_${t}"]="$q"
        echo "off P=$P $t qps=$q"
    done
done
stop_all

# ── 阶段 2: 开启增量同步 (每轮 P 由你启动从机) ─────────────────────────
echo ""
echo "━━━━ 阶段 2: 开启增量同步 (每轮 P 请启动从机) ━━━━"
start_master "$KVS_MASTER_ON_CONF"
warmup "$MASTER_PORT_NUM"
for P in "${PIPELINES[@]}"; do
    skip=0
    echo ""
    echo "════ P=$P ════"
    read -r -p "请启动从机 (${SLAVE_CMD}), 同步完成后按回车: " _
    until wait_slave_synced; do
        read -r -p "从机未同步 (60s 超时). 检查后回车重试, 或输入 skip 跳过本轮: " r
        if [ "$r" = skip ]; then skip=1; break; fi
    done
    if [ "$skip" = 1 ]; then
        echo "P=$P 已跳过"
        continue
    fi
    for t in "${tests[@]}"; do
        q=$(bench "$MASTER_PORT_NUM" "$t" "$P")
        RAWS["on_${P}_${t}"]="$q"
        echo "on  P=$P $t qps=$q"
    done
    if [ "$P" = "${PIPELINES[-1]}" ]; then
        read -r -p "请停止从机, 回车结束: " _
    else
        read -r -p "请停止从机, 回车进入下一轮: " _
    fi
done
stop_all

# ── 汇总表: 一次生成, 终端与文件同源 ────────────────────────────────────
{
    echo "bench_incr_sync_impact — $(date '+%Y-%m-%d %H:%M:%S')"
    echo "engine=${ENGINE_LABEL}  test=$TEST  nclients=$NCLIENTS  conn=$CONN  ops=$OPS  data=$DATA"
    echo "pipelines: ${PIPELINES[*]}   (单位: QPS; 开销 = 1 - on/off)"
    echo ""

    for t in "${tests[@]}"; do
        echo "════════════════ $t (QPS, ${NCLIENTS}×${OPS}) ════════════════"
        printf "  %-6s | %12s | %12s | %10s\n" \
            "P" "off(无同步)" "on(同步)" "开销"
        for P in "${PIPELINES[@]}"; do
            off=${RAWS["off_${P}_${t}"]}
            on=${RAWS["on_${P}_${t}"]}
            printf "  P%-4s | %12s | %12s | %10s\n" \
                "$P" "${off:-N/A}" "${on:-N/A}" "$(repl_cost "${off:-0}" "${on:-0}")"
        done
        echo ""
    done
} | tee "$RESULT_FILE"

echo "结果已保存: $RESULT_FILE"
