#!/bin/bash
# =============================================================================
# bench_incr_sync_redis.sh — Redis 主从增量同步的性能影响测试
#
# 测试不同 pipeline 深度下，Redis 主从复制对 master 写入性能的影响。
# 每轮独立：启动 master + slave → 建立复制 → 压测 → 清理。
#
# 用法:
#   sudo ./test/bench/bench_incr_sync_redis.sh              # 默认所有 pipeline
#   sudo ./test/bench/bench_incr_sync_redis.sh 40 80 160    # 只测指定深度
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

# Override record_result — text only
record_result() {
    local test=$1 scenario=$2 qps=$3
    echo "${test} | ${scenario}: ${qps} req/s" >> "$RESULT_FILE"
    echo -e "  ${GREEN}→ ${qps} req/s${NC}  (${scenario})"
}

# =============================================================================
# Configuration
# =============================================================================

# Pipeline depths
if [ $# -ge 1 ]; then
    PIPELINES=("$@")
else
    PIPELINES=(1 5 10 20 40 80 160)
fi

# Benchmark parameters (overridable via env)
BENCH_N=${BENCH_N:-1000000}
BENCH_C=${BENCH_C:-50}
BENCH_D=${BENCH_D:-16}

# Ports
MASTER_PORT=6379
SLAVE_PORT=6380

# CPU pinning
SERVER_CORES=${SERVER_CORES:-"0,3"}
CLIENT_CORES=${CLIENT_CORES:-"1,2"}

RESULT_FILE="${ROOT_DIR}/test/bench/results_incr_sync_redis.txt"

# Temp dirs
TMPDIR=$(mktemp -d -t bench_redis_sync.XXXXXX)
trap "rm -rf $TMPDIR" EXIT

MASTER_LOG="${TMPDIR}/redis_master.log"
SLAVE_LOG="${TMPDIR}/redis_slave.log"

# =============================================================================
# Helpers
# =============================================================================

section() { printf "\n${CYAN}── %s ──${NC}\n" "$1"; }

# ---- Kill all Redis processes ----
kill_redis_all() {
    pkill -x redis-server 2>/dev/null || true
    sleep 0.5
    pkill -9 -x redis-server 2>/dev/null || true
    sleep 0.3
}

# ---- Wait for port ready ----
redis_wait_port() {
    local port=$1 timeout=${2:-10}
    for ((i=0; i<timeout*2; i++)); do
        redis-cli -h 127.0.0.1 -p "$port" PING >/dev/null 2>&1 && return 0
        sleep 0.5
    done
    return 1
}

# ---- Start a Redis instance ----
start_redis_instance() {
    local port=$1 logfile=$2
    redis-server --daemonize yes --port "$port" --logfile "$logfile" \
                 --save "" --appendonly no \
                 --repl-diskless-sync no \
                 >/dev/null 2>&1
    sleep 0.5
    redis_wait_port "$port" 10 || { err "Redis port ${port} failed to start"; return 1; }

    # Bind to server cores
    local rpid
    rpid=$(ss -tlnp 2>/dev/null | grep ":${port} " | grep -oP 'pid=\K\d+' | head -1)
    [ -n "$rpid" ] && taskset -pc "$SERVER_CORES" "$rpid" >/dev/null 2>&1 || true

    redis-cli -h 127.0.0.1 -p "$port" FLUSHALL >/dev/null 2>&1
}

# ---- Wait for replication to complete ----
wait_replication_ready() {
    local slave_port=$1 timeout=${2:-30}
    for ((i=0; i<timeout*2; i++)); do
        local state
        state=$(redis-cli -h 127.0.0.1 -p "$slave_port" INFO replication 2>/dev/null \
                | grep "master_link_status:" | cut -d: -f2 | tr -d '\r ')
        if [ "$state" = "up" ]; then
            # Also verify sync is complete (not still loading)
            local load=$(redis-cli -h 127.0.0.1 -p "$slave_port" INFO persistence 2>/dev/null \
                        | grep "^loading:" | cut -d: -f2 | tr -d '\r ')
            if [ "$load" = "0" ]; then
                return 0
            fi
        fi
        sleep 0.5
    done
    return 1
}

# ---- Cleanup between rounds ----
full_cleanup() {
    kill_redis_all
    sleep 0.3
    local port
    for port in $MASTER_PORT $SLAVE_PORT; do
        ss -tlnp 2>/dev/null | grep -q ":${port} " && fuser -k ${port}/tcp 2>/dev/null || true
    done
    sleep 0.3
    rm -f /var/lib/redis/dump.rdb /home/xxf/dump.rdb
}

# ---- Run benchmark, return QPS ----
run_bench_redis() {
    local host=$1 port=$2 test_type=$3 P=$4
    local csv_line qps retry=0
    while [ $retry -lt 2 ]; do
        csv_line=$(taskset -c "$CLIENT_CORES" redis-benchmark \
            -h "$host" -p "$port" -t "$test_type" \
            -n "$BENCH_N" -c "$BENCH_C" -P "$P" -d "$BENCH_D" \
            -r "$BENCH_RAND" --csv 2>/dev/null \
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
# Setup
# =============================================================================

echo ""
step "Redis 主从增量同步性能影响测试"
log "Pipeline: ${PIPELINES[*]}  |  n=${BENCH_N} c=${BENCH_C} d=${BENCH_D}  |  CPU: server=${SERVER_CORES} client=${CLIENT_CORES}"
echo ""

full_cleanup

# Result file header
{
    echo "# Redis 主从增量同步性能影响测试 — $(date '+%Y-%m-%d %H:%M:%S')"
    echo "# Pipeline: ${PIPELINES[*]}  n=${BENCH_N} c=${BENCH_C} d=${BENCH_D}"
    echo "# CPU: server=${SERVER_CORES} client=${CLIENT_CORES}"
    echo "# Redis: $(redis-server --version 2>&1 | head -1)"
    echo ""
} > "$RESULT_FILE"

declare -A SET_NO_REPL SET_WITH_REPL GET_NO_REPL GET_WITH_REPL

# =============================================================================
# Baseline (master only, no replication)
# =============================================================================

section "BASELINE — Redis Master only"
echo ""

for P in "${PIPELINES[@]}"; do
    echo -n "  P=${P}  "

    full_cleanup
    start_redis_instance "$MASTER_PORT" "$MASTER_LOG" || { err "skip P=${P}"; continue; }

    set_qps=$(run_bench_redis 127.0.0.1 "$MASTER_PORT" set "$P")
    record_result "P${P}-SET" "baseline" "$set_qps"
    SET_NO_REPL[$P]=$set_qps

    get_qps=$(run_bench_redis 127.0.0.1 "$MASTER_PORT" get "$P")
    record_result "P${P}-GET" "baseline" "$get_qps"
    GET_NO_REPL[$P]=$get_qps

    record_raw ""
done

# =============================================================================
# With replication (master + slave)
# =============================================================================

section "WITH REPLICATION — Master + Slave"
echo ""

for P in "${PIPELINES[@]}"; do
    echo -n "  P=${P}  "

    full_cleanup

    # Start master
    start_redis_instance "$MASTER_PORT" "$MASTER_LOG" || { err "skip P=${P}"; continue; }

    # Start slave
    start_redis_instance "$SLAVE_PORT" "$SLAVE_LOG" || { err "skip P=${P}"; continue; }

    # Establish replication
    redis-cli -h 127.0.0.1 -p "$SLAVE_PORT" REPLICAOF 127.0.0.1 "$MASTER_PORT" >/dev/null 2>&1

    # Wait for sync
    if ! wait_replication_ready "$SLAVE_PORT" 30; then
        err "Replication sync timeout for P=${P}"
        kill_redis_all
        continue
    fi

    set_qps_r=$(run_bench_redis 127.0.0.1 "$MASTER_PORT" set "$P")
    record_result "P${P}-SET" "with-repl" "$set_qps_r"
    SET_WITH_REPL[$P]=$set_qps_r

    get_qps_r=$(run_bench_redis 127.0.0.1 "$MASTER_PORT" get "$P")
    record_result "P${P}-GET" "with-repl" "$get_qps_r"
    GET_WITH_REPL[$P]=$get_qps_r

    record_raw ""
done

# =============================================================================
# Summary table
# =============================================================================

echo ""
echo -e "  ${YELLOW}=== Redis 主从增量同步性能影响 ===${NC}"
echo ""
HDR=$(printf "  %-6s | %14s | %14s | %8s | %14s | %14s | %8s" \
    "P" "SET-baseline" "SET-with-repl" "影响" "GET-baseline" "GET-with-repl" "影响")
SEP=$(printf "  %-6s-+-%14s-+-%14s-+-%8s-+-%14s-+-%14s-+-%8s" \
    "------" "--------------" "--------------" "--------" "--------------" "--------------" "--------")
echo "$HDR"
echo "$SEP"

{
    echo ""
    echo "=== 结果汇总 ==="
    echo ""
    echo "$HDR"
    echo "$SEP"
} >> "$RESULT_FILE"

for P in "${PIPELINES[@]}"; do
    bset=${SET_NO_REPL[$P]:-0}
    sset=${SET_WITH_REPL[$P]:-0}
    bget=${GET_NO_REPL[$P]:-0}
    sget=${GET_WITH_REPL[$P]:-0}
    rset=$(safe_ratio "$sset" "$bset")
    rget=$(safe_ratio "$sget" "$bget")
    ROW=$(printf "  P=%-4s | %14s | %14s | %8s | %14s | %14s | %8s" \
        "$P" "$bset" "$sset" "$rset" "$bget" "$sget" "$rget")
    echo "$ROW"
    echo "$ROW" >> "$RESULT_FILE"
done

{
    echo ""
    echo "影响 = with-repl / baseline × 100%"
    echo "SET: 主从复制的写入开销（master 需要将写命令发送给 slave）"
    echo "GET: 读操作不受主从复制影响（仅有 replication 维护开销）"
} >> "$RESULT_FILE"

# =============================================================================
# Cleanup
# =============================================================================

full_cleanup
echo ""
log "结果: $RESULT_FILE"
echo ""
