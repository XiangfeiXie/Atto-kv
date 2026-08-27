#!/bin/bash
# =============================================================================
# bench_pipeline.sh — 多客户端 pipeline QPS 对比 (proactor / reactor / Redis)
#
# 指标: QPS (服务器每秒处理的查询数)。并发 NCLIENTS 个 redis-benchmark 进程
#   聚合吞吐 —— 避免单进程高 P 下客户端解析回复成为瓶颈。
#
# 参数: 仅保留存储引擎选择 (--engines) 与 P 大小设置, 其余用默认值 (单轮直出)。
# 用法:
#   ./test/bench/bench_pipeline.sh                       # 默认参数
#   ./test/bench/bench_pipeline.sh 1 4 16 160            # 指定 pipeline 深度
#   ./test/bench/bench_pipeline.sh --engines "proactor redis"
#   ./test/bench/bench_pipeline.sh --small               # 快速验证
#
# 输出: 终端汇总表 + test/bench/bench_pipeline_results.txt
# =============================================================================

set -u

PROJECT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
KVS="${PROJECT_DIR}/build/kvstore"
RESULT_FILE="${PROJECT_DIR}/test/bench/bench_pipeline_results.txt"

# 配置文件/日志放当前用户拥有的临时目录, 退出时随 cleanup 一起删除。
WORK="$(mktemp -d /tmp/kvsbench.XXXXXX)"
KVS_PROACTOR_CONF="${WORK}/kvs_proactor.conf"
KVS_REACTOR_CONF="${WORK}/kvs_reactor.conf"
REDIS_CONF="${WORK}/redis.conf"

cleanup() {
    pkill -x kvstore 2>/dev/null
    pkill -x redis-server 2>/dev/null
    pkill -x redis-benchmark 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT

# ── 默认参数 (固定) ────────────────────────────────────────────────────
PIPELINES=(1 4 8 16 20 40 80 160)
ENGINES=(proactor reactor redis)
TEST=both                    # 只测 GET (SET 仅用于预热)
NCLIENTS=2                  # 适中: 够对比, 不追极限吞吐
OPS=500000                  # 每个客户端的请求数
CONN=50                     # redis-benchmark -c (每进程并发)
DATA=16                     # value 字节
WARMUP_OPS=1000000          # GET 前的 SET 预热量

# 公平对比: proactor/reactor 与 redis 一样都只 pin 单核 (均为单线程引擎)
PROACTOR_CPU="5"
SINGLE_CPU="5"
CLIENT_CORES=(0 2)

KVS_PORT=2000
REDIS_PORT=2001

# ── 参数解析 (仅引擎选择 + P) ──────────────────────────────────────────
help() {  # 只打印头部注释块 (到第一个非 # 行即停)
    awk '!/^#/ {exit} NR>=2 {sub(/^# */, ""); print}' "$0"
    exit 0
}
USER_PIPELINES=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --engines)  read -r -a ENGINES <<<"$2"; shift 2 ;;
        --small)    PIPELINES=(1 8 160); OPS=50000; WARMUP_OPS=200000; shift ;;
        -h|--help)  help ;;
        -*)         echo "未知选项: $1" >&2; exit 1 ;;
        *)          USER_PIPELINES+=("$1"); shift ;;
    esac
done
if [ "${#USER_PIPELINES[@]}" -gt 0 ]; then
    PIPELINES=("${USER_PIPELINES[@]}")
fi

# ── 工具检查 ─────────────────────────────────────────────────────────────
for tool in redis-benchmark ss taskset; do
    command -v "$tool" >/dev/null 2>&1 || { echo "缺少工具: $tool" >&2; exit 1; }
done
[ -x "$KVS" ] || { echo "kvstore 未编译: $KVS" >&2; exit 1; }

# ── 生成配置文件 (自包含, 每次重建) ─────────────────────────────────────
cat > "$KVS_PROACTOR_CONF" <<-KCONF
port 2000
network proactor
persistence none
repl_role off
expire off
KCONF
cat > "$KVS_REACTOR_CONF" <<-KCONF
port 2000
network reactor
persistence none
repl_role off
expire off
KCONF
cat > "$REDIS_CONF" <<-RCONF
port 2001
bind 127.0.0.1
protected-mode no
save ""
appendonly no
maxclients 10000
tcp-backlog 4096
io-threads 1
loglevel warning
RCONF

# ── 服务器管理 ──────────────────────────────────────────────────────────
start_server() { # $1 = engine
    case "$1" in
    proactor) taskset -c "$PROACTOR_CPU" "$KVS" "$KVS_PROACTOR_CONF" >"$WORK/kvs_bench.log" 2>&1 & ;;
    reactor)  taskset -c "$SINGLE_CPU"   "$KVS" "$KVS_REACTOR_CONF"  >"$WORK/kvs_bench.log" 2>&1 & ;;
    redis)    taskset -c "$SINGLE_CPU"   redis-server "$REDIS_CONF"  >"$WORK/redis_bench.log" 2>&1 & ;;
    esac
    local port=$KVS_PORT; [ "$1" = redis ] && port=$REDIS_PORT
    for i in $(seq 1 80); do
        ss -tln 2>/dev/null | grep -q ":${port} " && return 0
        sleep 0.1
    done
    echo "[FATAL] $1 启动超时 (port=$port)" >&2
    tail -20 "$WORK/kvs_bench.log" >&2
    return 1
}

stop_server() {
    pkill -x kvstore 2>/dev/null
    pkill -x redis-server 2>/dev/null
    sleep 0.5
}

warmup() { # $1 = port (预热跑在客户端核上, 不计入 QPS 统计)
    taskset -c "${CLIENT_CORES[0]},${CLIENT_CORES[1]}" redis-benchmark \
        -h 127.0.0.1 -p "$1" -t set -n "$WARMUP_OPS" \
        -c 50 -P 100 -d "$DATA" -q >/dev/null 2>&1
}

bench() { # $1 = port  $2 = test(get/set)  $3 = pipeline P ; prints aggregate QPS
    local port=$1 t=$2 P=$3 pids=()
    local cli_to=$((OPS / 10000 + 60))   # P1 下每客户端 ~20K qps 的安全余量
    for i in $(seq 0 $((NCLIENTS - 1))); do
        taskset -c "${CLIENT_CORES[$((i % ${#CLIENT_CORES[@]}))]}" timeout "$cli_to" \
            redis-benchmark -h 127.0.0.1 -p "$port" -t "$t" -n "$OPS" \
            -c "$CONN" -P "$P" -d "$DATA" --csv 2>/dev/null >"$WORK/bench_c$i.csv" &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null; done
    # 各客户端文件: CSV 第 2 列 = requests/sec; 服务器为瓶颈时即服务器 QPS。
    awk -F',' 'FNR>1 {gsub(/"/,"",$2); sum+=$2} END {printf "%.0f", sum}' \
        "$WORK"/bench_c*.csv
}

ratio() { # $1=a $2=b ; prints "1.24x" or "  N/A "
    local a=$1 b=$2
    if [ "$b" -gt 0 ] 2>/dev/null && [ "$a" -gt 0 ] 2>/dev/null; then
        awk "BEGIN{printf \"%.2fx\", $a/$b}"
    else
        echo -n "  N/A "
    fi
}

# ── 主流程 (单轮直出) ───────────────────────────────────────────────────
declare -A RAWS   # 只存实际跑过的 (eng, P, t)

echo "# bench_pipeline — $(date '+%Y-%m-%d %H:%M:%S')"
echo "# engines=${ENGINES[*]}  test=$TEST  nclients=$NCLIENTS  conn=$CONN  ops=$OPS  data=$DATA"
echo "# pipelines: ${PIPELINES[*]}"
echo "# 指标: QPS (聚合吞吐, 服务器为瓶颈时即服务器 QPS)"

for eng in "${ENGINES[@]}"; do
    stop_server
    rm -f "${PROJECT_DIR}/build/dump.rdb" "${PROJECT_DIR}/build/dump.rdb.tmp" 2>/dev/null
    start_server "$eng" || exit 1
    port=$KVS_PORT; [ "$eng" = redis ] && port=$REDIS_PORT

    # GET 前先 SET 预热 (每引擎只做一次, 复用于所有 P)
    if [ "$TEST" != set ]; then
        echo "$eng warmup(set $WARMUP_OPS)..."
        warmup "$port"
    fi

    for P in "${PIPELINES[@]}"; do
        for t in get set; do
            [ "$TEST" = "$t" ] || [ "$TEST" = both ] || continue
            q=$(bench "$port" "$t" "$P")
            RAWS["${eng}_${P}_${t}"]="$q"
            echo "$eng P=$P $t qps=$q"
        done
    done
done

# ── 汇总表: 一次生成, 终端与文件同源 ────────────────────────────────────
BASE="${ENGINES[-1]}"   # 比值基准: 列表最后一个引擎 (默认 redis)

{
    echo "bench_pipeline — $(date '+%Y-%m-%d %H:%M:%S')"
    echo "engines=${ENGINES[*]}  test=$TEST  nclients=$NCLIENTS  conn=$CONN  ops=$OPS  data=$DATA"
    echo "pipelines: ${PIPELINES[*]}   (单位: QPS)"
    echo ""

    for t in get set; do
        [ "$TEST" = "$t" ] || [ "$TEST" = both ] || continue

        echo "════════════════ $t (QPS, ${NCLIENTS}×${OPS}) ════════════════"
        printf "  %-6s" "P"
        for eng in "${ENGINES[@]}"; do printf "%14s" "$eng"; done
        if [ "${#ENGINES[@]}" -ge 2 ]; then
            for eng in "${ENGINES[@]}"; do
                [ "$eng" = "$BASE" ] && continue
                printf "%10s" "${eng:0:1}/${BASE}"
            done
        fi
        echo ""

        for P in "${PIPELINES[@]}"; do
            printf "  %-6s" "P$P"
            for eng in "${ENGINES[@]}"; do
                printf "%14s" "${RAWS["${eng}_${P}_${t}"]}"
            done
            if [ "${#ENGINES[@]}" -ge 2 ]; then
                for eng in "${ENGINES[@]}"; do
                    [ "$eng" = "$BASE" ] && continue
                    printf "%10s" "$(ratio "${RAWS["${eng}_${P}_${t}"]}" "${RAWS["${BASE}_${P}_${t}"]}")"
                done
            fi
            echo ""
        done
        echo ""
    done
} | tee "$RESULT_FILE"

echo "结果已保存: $RESULT_FILE"
stop_server
