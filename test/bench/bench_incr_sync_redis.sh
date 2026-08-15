#!/bin/bash
# =============================================================================
# bench_incr_sync_redis.sh — Redis 主从增量同步的性能影响测试
#
# 方法论与 bench_incr_sync.sh 完全对齐，用来对照排除“脚本测量方法”本身的影响：
#   1. 默认单 key（不加 -r）：把命令路径压到最便宜，让同步开销占比最大
#      （redis-benchmark 不加 -r 时所有请求打到同一个字面量 key:__rand_int__）
#   2. 每个测量点跑 REPS 次取中位数（默认 3）
#   3. 每个 P 内 baseline→with-repl 背靠背测，最小化时间漂移
#   4. 测前预热（ramp CPU 频率 + 预填 key）
#   5. 单核绑定（master/slave/client 各占一个核，避免迁移抖动）
#   6. 额外记录 master 的 CPU 时间/请求（读 /proc/<pid>/stat，免 root、不依赖 perf）
#
# 对比两种场景：
#   baseline  : master 单独运行（无复制）
#   with-repl : master + slave（REPLICAOF 异步复制）
#
# 用法:
#   ./test/bench/bench_incr_sync_redis.sh                 # 默认所有 pipeline
#   ./test/bench/bench_incr_sync_redis.sh 40 80 160       # 只测指定深度
#   ./test/bench/bench_incr_sync_redis.sh --realistic     # 随机 key 模式（-r）
#
# 可调环境变量:
#   BENCH_N BENCH_C BENCH_D  WARMUP_MULT  REPS
#   MASTER_CORE SLAVE_CORE CLIENT_CORE
#
# 前置条件:
#   - redis-server, redis-cli, redis-benchmark, bc 可用
#
# 输出:  test/bench/results_incr_sync_redis.txt
# =============================================================================

script_name=$(basename "$0")
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/common.sh"
require_tools
ROOT_DIR="$PROJECT_DIR"

command -v redis-server >/dev/null 2>&1 || { err "缺少工具: redis-server"; exit 1; }

# Override record_result — text only
record_result() {
    local test=$1 scenario=$2 qps=$3
    echo "${test} | ${scenario}: ${qps} req/s" >> "$RESULT_FILE"
    echo -e "  ${GREEN}→ ${qps} req/s${NC}  (${scenario})"
}

# =============================================================================
# Argument parsing
# =============================================================================

REALISTIC=0
PIPELINES=()
while [ $# -gt 0 ]; do
    case "$1" in
        --realistic) REALISTIC=1; shift ;;
        -*) err "未知选项: $1"; exit 1 ;;
        *) PIPELINES+=("$1"); shift ;;
    esac
done
[ ${#PIPELINES[@]} -eq 0 ] && PIPELINES=(1 5 10 20 40 80 160)

# =============================================================================
# Configuration
# =============================================================================

BENCH_N=${BENCH_N:-1000000}
BENCH_C=${BENCH_C:-50}
BENCH_D=${BENCH_D:-16}
WARMUP_MULT=${WARMUP_MULT:-2}
REPS=${REPS:-3}
WARMUP_N=$((BENCH_N * WARMUP_MULT))

# Key mode: 默认单 key（不加 -r，压到最便宜的命令路径以暴露同步开销）；
# --realistic 切随机 key（-r，真实插入负载，同步开销占比小）。
if [ "$REALISTIC" -eq 1 ]; then
    R_FLAG="-r ${BENCH_RAND:-1000000}"
    KEY_DESC="realistic(-r ${BENCH_RAND:-1000000})"
else
    R_FLAG=""
    KEY_DESC="single-key(不加-r)"
fi

# Ports
MASTER_PORT=6379
SLAVE_PORT=6380

# CPU 单核绑定（每进程独占一核）
MASTER_CORE=${MASTER_CORE:-"0"}
SLAVE_CORE=${SLAVE_CORE:-"2"}
CLIENT_CORE=${CLIENT_CORE:-"1"}

RESULT_FILE="${ROOT_DIR}/test/bench/results_incr_sync_redis.txt"

# Temp dirs
TMPDIR=$(mktemp -d -t bench_redis_sync.XXXXXX)
trap "rm -rf $TMPDIR" EXIT

MASTER_LOG="${TMPDIR}/redis_master.log"
SLAVE_LOG="${TMPDIR}/redis_slave.log"

CLK_TCK=$(getconf CLK_TCK)

# =============================================================================
# Helpers
# =============================================================================

section() { printf "\n${CYAN}── %s ──${NC}\n" "$1"; }
ok()     { echo -e "  ${GREEN}OK${NC}    $1"; }

# ---- Kill all Redis processes ----
kill_redis_all() {
    pkill -x redis-server 2>/dev/null || true
    sleep 0.5
    pkill -9 -x redis-server 2>/dev/null || true
    sleep 0.3
}

force_release_ports() {
    local port
    for port in $MASTER_PORT $SLAVE_PORT; do
        ss -tlnp 2>/dev/null | grep -q ":${port} " && fuser -k ${port}/tcp 2>/dev/null || true
    done
    sleep 0.3
}

full_cleanup() {
    kill_redis_all
    force_release_ports
    rm -f /var/lib/redis/dump.rdb /home/xxf/dump.rdb 2>/dev/null || true
}

# ---- Wait for Redis port ready ----
redis_wait_port() {
    local port=$1 timeout=${2:-10}
    for ((i=0; i<timeout*2; i++)); do
        redis-cli -h 127.0.0.1 -p "$port" PING >/dev/null 2>&1 && return 0
        sleep 0.5
    done
    return 1
}

# ---- Redis 进程 PID（读 INFO server，稳定且按端口区分）----
redis_pid() {
    redis-cli -h 127.0.0.1 -p "$1" INFO server 2>/dev/null \
        | awk -F: '/^process_id:/{gsub(/[\r ]/,"",$2); print $2}'
}

# ---- Start a Redis instance（单核绑定 + 清空）----
start_redis_instance() {
    local port=$1 logfile=$2 core=$3
    redis-server --daemonize yes --port "$port" --logfile "$logfile" \
        --save "" --appendonly no \
        --repl-diskless-sync no --repl-backlog-size 1mb \
        >/dev/null 2>&1
    redis_wait_port "$port" 10 || { err "Redis port ${port} failed to start"; return 1; }

    local rpid
    rpid=$(redis_pid "$port")
    [ -n "$rpid" ] && taskset -pc "$core" "$rpid" >/dev/null 2>&1 || true

    redis-cli -h 127.0.0.1 -p "$port" FLUSHALL >/dev/null 2>&1
    return 0
}

# ---- Wait for replication to complete ----
wait_replication_ready() {
    local slave_port=$1 timeout=${2:-30}
    for ((i=0; i<timeout*2; i++)); do
        local state load
        state=$(redis-cli -h 127.0.0.1 -p "$slave_port" INFO replication 2>/dev/null \
                | awk -F: '/master_link_status:/{gsub(/[\r ]/,"",$2); print $2}')
        load=$(redis-cli -h 127.0.0.1 -p "$slave_port" INFO persistence 2>/dev/null \
                | awk -F: '/^loading:/{gsub(/[\r ]/,"",$2); print $2}')
        if [ "$state" = "up" ] && [ "$load" = "0" ]; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

# ---- master CPU ticks（utime+stime，/proc/<pid>/stat）----
cpu_ticks() {
    awk '{print $14+$15}' "/proc/$1/stat" 2>/dev/null || echo 0
}

# ---- CPU ns/req = delta_ticks / CLK_TCK * 1e9 / n ----
cpu_ns_per_req() {
    local dt=$1 n=$2
    [ "$n" -gt 0 ] || { echo 0; return; }
    awk -v dt="$dt" -v n="$n" -v clk="$CLK_TCK" \
        'BEGIN{printf "%.0f", dt*1000000000/(clk*n)}'
}

# ---- Run benchmark once, return QPS ----
run_bench_redis() {
    local host=$1 port=$2 test_type=$3 P=$4
    local csv_line qps retry=0
    while [ $retry -lt 2 ]; do
        csv_line=$(taskset -c "$CLIENT_CORE" redis-benchmark \
            -h "$host" -p "$port" -t "$test_type" \
            -n "$BENCH_N" -c "$BENCH_C" -P "$P" -d "$BENCH_D" \
            $R_FLAG --csv 2>/dev/null \
            | grep -E '^"[^"]+","[0-9]' | tail -1)
        if [ -n "$csv_line" ]; then
            qps=$(echo "$csv_line" | cut -d',' -f2 | tr -d '"')
            if [[ "$qps" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
                echo "$qps"
                return 0
            fi
        fi
        retry=$((retry + 1))
        [ $retry -lt 2 ] && sleep 0.5
    done
    echo "0"
}

median() {
    printf '%s\n' "$@" | sort -n | awk 'NF { a[++n]=$1 } END { if (n==0) print 0; else print a[int((n+1)/2)] }'
}

# ---- 重复 REPS 次取中位数 ----
run_bench_reps() {
    local host=$1 port=$2 test_type=$3 P=$4
    local -a vals=()
    local i v
    for ((i=0; i<REPS; i++)); do
        v=$(run_bench_redis "$host" "$port" "$test_type" "$P")
        vals+=("$v")
    done
    echo -e "      ${test_type} reps=[${vals[*]}]" >&2
    median "${vals[@]}"
}

warmup() {
    local host=$1 port=$2
    taskset -c "$CLIENT_CORE" redis-benchmark -h "$host" -p "$port" \
        -t set -n "$WARMUP_N" -c "$BENCH_C" -P 100 -d "$BENCH_D" \
        $R_FLAG -q >/dev/null 2>&1 || true
}

safe_ratio() {
    local a=$1 b=$2
    if [[ "$a" =~ ^[0-9]+(\.[0-9]+)?$ ]] && \
       [[ "$b" =~ ^[0-9]+(\.[0-9]+)?$ ]] && \
       [ "$(echo "$b > 0" | bc -l 2>/dev/null)" = "1" ]; then
        printf "%.1f%%" "$(echo "scale=4; ($a / $b) * 100" | bc -l 2>/dev/null)"
    else
        echo "N/A"
    fi
}

# =============================================================================
# Scenario setup / teardown / measurement
# =============================================================================

setup_topology() {
    local scenario=$1 P=$2
    case "$scenario" in
        baseline)
            start_redis_instance "$MASTER_PORT" "$MASTER_LOG" "$MASTER_CORE" || return 1
            ;;
        with-repl)
            start_redis_instance "$MASTER_PORT" "$MASTER_LOG" "$MASTER_CORE" || return 1
            start_redis_instance "$SLAVE_PORT" "$SLAVE_LOG" "$SLAVE_CORE" || return 1
            redis-cli -h 127.0.0.1 -p "$SLAVE_PORT" REPLICAOF 127.0.0.1 "$MASTER_PORT" >/dev/null 2>&1
            if ! wait_replication_ready "$SLAVE_PORT" 30; then
                err "P=${P} replication sync timeout"
                return 1
            fi
            ok "slave 已进入流式复制（master_link_status=up）"
            ;;
    esac
    return 0
}

teardown_topology() {
    full_cleanup
}

measure_scenario() {
    local scenario=$1 P=$2
    local set_qps get_qps set_cpu get_cpu t0 t1 mpid

    echo ""
    echo -n "  P=${P} [${scenario}] "

    full_cleanup
    setup_topology "$scenario" "$P" || { err "P=${P} ${scenario} 拓扑启动失败"; teardown_topology "$scenario"; return 1; }

    mpid=$(redis_pid "$MASTER_PORT")

    # 预热
    warmup 127.0.0.1 "$MASTER_PORT"

    # SET（取中位数 + 测量 master CPU 时间）
    t0=$(cpu_ticks "$mpid")
    set_qps=$(run_bench_reps 127.0.0.1 "$MASTER_PORT" set "$P")
    t1=$(cpu_ticks "$mpid")
    set_cpu=$(cpu_ns_per_req $((t1 - t0)) $((REPS * BENCH_N)))
    record_result "P${P}-SET" "$scenario" "$set_qps"
    record_raw "  P${P}-SET ${scenario} cpu_ns/req=${set_cpu}"

    # GET（取中位数 + 测量 master CPU 时间）
    t0=$(cpu_ticks "$mpid")
    get_qps=$(run_bench_reps 127.0.0.1 "$MASTER_PORT" get "$P")
    t1=$(cpu_ticks "$mpid")
    get_cpu=$(cpu_ns_per_req $((t1 - t0)) $((REPS * BENCH_N)))
    record_result "P${P}-GET" "$scenario" "$get_qps"
    record_raw "  P${P}-GET ${scenario} cpu_ns/req=${get_cpu}"

    case "$scenario" in
        baseline)  SET_BASE[$P]=$set_qps;  GET_BASE[$P]=$get_qps
                   SET_BASE_CPU[$P]=$set_cpu; GET_BASE_CPU[$P]=$get_cpu ;;
        with-repl) SET_REPL[$P]=$set_qps;  GET_REPL[$P]=$get_qps
                   SET_REPL_CPU[$P]=$set_cpu; GET_REPL_CPU[$P]=$get_cpu ;;
    esac

    record_raw ""
    teardown_topology "$scenario"
    return 0
}

# =============================================================================
# Setup
# =============================================================================

echo ""
step "Redis 主从增量同步性能影响测试（方法对齐 bench_incr_sync.sh）"
log "Pipeline: ${PIPELINES[*]}  |  n=${BENCH_N} c=${BENCH_C} d=${BENCH_D}"
log "键模式: ${KEY_DESC}  |  每点 REPS=${REPS}（中位数）  |  预热=${WARMUP_N}"
log "CPU: master=${MASTER_CORE} slave=${SLAVE_CORE} client=${CLIENT_CORE}"
echo ""

full_cleanup

# Result file header
{
    echo "# Redis 主从增量同步性能影响测试 — $(date '+%Y-%m-%d %H:%M:%S')"
    echo "# 场景: baseline(master 单独) / with-repl(master+slave 异步复制)"
    echo "# Pipeline: ${PIPELINES[*]}  n=${BENCH_N} c=${BENCH_C} d=${BENCH_D}"
    echo "# 键模式: ${KEY_DESC}  每点 REPS=${REPS}(中位数)  预热=${WARMUP_N}"
    echo "# CPU: master=${MASTER_CORE} slave=${SLAVE_CORE} client=${CLIENT_CORE}"
    echo "# Redis: $(redis-server --version 2>&1 | head -1)"
    echo ""
} > "$RESULT_FILE"

declare -A SET_BASE GET_BASE SET_REPL GET_REPL
declare -A SET_BASE_CPU GET_BASE_CPU SET_REPL_CPU GET_REPL_CPU

# =============================================================================
# Main loop — 每个 P 内 baseline→with-repl 背靠背测
# =============================================================================

section "Redis 主从增量同步性能影响（每个 P 内两场景背靠背）"
for P in "${PIPELINES[@]}"; do
    measure_scenario baseline "$P" || true
    measure_scenario with-repl "$P" || true
done

# =============================================================================
# Summary tables
# =============================================================================

QHDR=$(printf "  %-4s | %12s | %12s | %8s | %11s | %11s | %8s" \
    "P" "base-QPS" "repl-QPS" "QPS%" "base-cpu/req" "repl-cpu/req" "CPU%")
QSEP=$(printf "  %-4s-+-%12s-+-%12s-+-%8s-+-%11s-+-%11s-+-%8s" \
    "----" "------------" "------------" "--------" "-----------" "-----------" "--------")

echo ""
echo -e "  ${YELLOW}=== Redis 主从增量同步性能影响 ===${NC}"
echo ""

{
    echo ""
    echo "=== 结果汇总 ==="
    echo ""
} >> "$RESULT_FILE"

for op in SET GET; do
    if [ "$op" = SET ]; then
        op_desc="写命令：master 传播到 slave，含复制开销"
    else
        op_desc="读命令：不传播，仅受 slave ACK/连接固定开销影响"
    fi
    echo -e "  ${CYAN}— ${op}（${op_desc}）—${NC}"
    echo "$QHDR"
    echo "$QSEP"
    {
        echo "--- ${op} ---"
        echo "$QHDR"
        echo "$QSEP"
    } >> "$RESULT_FILE"

    for P in "${PIPELINES[@]}"; do
        if [ "$op" = SET ]; then
            b=${SET_BASE[$P]:-0}; r=${SET_REPL[$P]:-0}
            bcpu=${SET_BASE_CPU[$P]:-0}; rcpu=${SET_REPL_CPU[$P]:-0}
        else
            b=${GET_BASE[$P]:-0}; r=${GET_REPL[$P]:-0}
            bcpu=${GET_BASE_CPU[$P]:-0}; rcpu=${GET_REPL_CPU[$P]:-0}
        fi
        rq=$(safe_ratio "$r" "$b")
        rc=$(safe_ratio "$rcpu" "$bcpu")
        ROW=$(printf "  %-4s | %12s | %12s | %8s | %11s | %11s | %8s" \
            "$P" "$b" "$r" "$rq" "$bcpu" "$rcpu" "$rc")
        echo "$ROW"
        echo "$ROW" >> "$RESULT_FILE"
    done
    echo ""
done

{
    echo "QPS% = with-repl / baseline × 100%（wall-clock 吞吐比，受 client round-trip 干扰）"
    echo "CPU% = repl cpu/req / base cpu/req × 100%（master 每请求 CPU 时间比，真实复制开销）"
    echo "  - cpu/req 由 /proc/<pid>/stat 的 utime+stime 差分得到，免 root、不依赖 perf"
    echo "  - >100% 表示复制场景每请求消耗更多 CPU（真实开销）；"
    echo "    QPS% 在单 key 负载下可能 >100% 的抖动不代表加速，请以 CPU% 为准。"
    echo ""
    echo "说明:"
    echo "  - with-repl: Redis 异步复制，master 在主线程内把写命令 memcpy 到 repl-backlog"
    echo "              （1MB）+ slave 输出缓冲，再异步 send 给 slave；读命令不传播。"
    echo "  - 与 kvstore 的 epoll 增量同步同构，用于对照排除测量方法差异。"
} >> "$RESULT_FILE"

# =============================================================================
# Cleanup
# =============================================================================

full_cleanup
echo ""
log "结果: $RESULT_FILE"
echo ""
