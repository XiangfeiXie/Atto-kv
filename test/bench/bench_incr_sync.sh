#!/bin/bash
# =============================================================================
# bench_incr_sync.sh — XDP 增量同步组件对 kvstore 的性能影响测试
#
# 测试不同 pipeline 深度下，XDP 增量同步组件对 kvstore master 的性能影响。
# 每轮独立：启动 master + slave → 启动 daemon → 压测 → 清理。
#
# 用法:
#   sudo ./test/bench/bench_incr_sync.sh              # 默认所有 pipeline
#   sudo ./test/bench/bench_incr_sync.sh 40 80 160    # 只测指定深度
#
# 前置条件:
#   - kvstore 已编译（build/kvstore）
#   - clang + libbpf 可用（编译 XDP daemon）
#   - redis-cli, redis-benchmark, bc, bpftool 可用
#   - root 权限（XDP attach）
#
# 输出:  test/bench/results_incr_sync.txt
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
MASTER_PORT=2000
SLAVE_PORT=2002

# CPU pinning
SERVER_CORES=${SERVER_CORES:-"0,3"}
CLIENT_CORES=${CLIENT_CORES:-"1,2"}
SYNC_CORES=${SYNC_CORES:-"4"}

# Paths
KVS_BIN="${ROOT_DIR}/build/kvstore"
SYNC_SRC_DIR="${ROOT_DIR}/src/repl-incr-xdptracepoint"
SYNC_BUILD_DIR="${SYNC_SRC_DIR}/build"
SYNC_BIN="${SYNC_BUILD_DIR}/kvstore-incr-sync"

RESULT_FILE="${ROOT_DIR}/test/bench/results_incr_sync.txt"

# Temp dirs
TMPDIR=$(mktemp -d -t bench_incr_sync.XXXXXX)
trap "rm -rf $TMPDIR" EXIT

MASTER_CONF="${TMPDIR}/master.conf"
SLAVE_CONF="${TMPDIR}/slave.conf"
MASTER_LOG="${TMPDIR}/master.log"
SLAVE_LOG="${TMPDIR}/slave.log"
SYNC_LOG="${TMPDIR}/sync.log"

# =============================================================================
# Helpers
# =============================================================================

section() { printf "\n${CYAN}── %s ──${NC}\n" "$1"; }
ok()     { echo -e "  ${GREEN}OK${NC}    $1"; }

wait_for_log() {
    local logfile=$1 pattern=$2 timeout=${3:-30}
    for ((i=0; i<timeout*2; i++)); do
        grep -q "$pattern" "$logfile" 2>/dev/null && return 0
        sleep 0.5
    done
    return 1
}

# ---- Build sync daemon ----
build_sync_daemon() {
    local rebuild=0
    [ ! -x "$SYNC_BIN" ] && rebuild=1

    if [ $rebuild -eq 1 ]; then
        mkdir -p "$SYNC_BUILD_DIR"
        if ! (cd "$SYNC_BUILD_DIR" && \
              cmake .. -DCMAKE_BUILD_TYPE=Release \
                      -DCMAKE_C_FLAGS="-DSLAVE_PORT=${SLAVE_PORT} -DKVS_FILTER_PORT=${MASTER_PORT}" \
                      >/dev/null 2>&1 && \
              make -j"$(nproc)" >/dev/null 2>&1); then
            err "Failed to build sync daemon"
            return 1
        fi
        return 0
    fi

    local compiled_ports
    compiled_ports=$(strings "$SYNC_BIN" 2>/dev/null | grep -oP 'slave=.*filter_port=\d+' | head -1 || true)
    if ! echo "$compiled_ports" | grep -q "slave=.*:${SLAVE_PORT}.*filter_port=${MASTER_PORT}"; then
        (cd "$SYNC_BUILD_DIR" && \
         cmake .. -DCMAKE_BUILD_TYPE=Release \
                 -DCMAKE_C_FLAGS="-DSLAVE_PORT=${SLAVE_PORT} -DKVS_FILTER_PORT=${MASTER_PORT}" \
                 >/dev/null 2>&1 && \
         make -j"$(nproc)" >/dev/null 2>&1) || { err "Failed to rebuild sync daemon"; return 1; }
    fi
    return 0
}

# ---- Process/port cleanup ----
kill_all() {
    sudo killall -INT kvstore-incr-sync 2>/dev/null || true
    sudo killall -9  kvstore-incr-sync 2>/dev/null || true
    pkill -x kvstore 2>/dev/null || true
    sleep 0.3
    pkill -9 -x kvstore 2>/dev/null || true
    sleep 0.3
}

force_release_ports() {
    local port
    for port in $MASTER_PORT $SLAVE_PORT; do
        ss -tlnp 2>/dev/null | grep -q ":${port} " && fuser -k ${port}/tcp 2>/dev/null || true
    done
    for ((i=0; i<20; i++)); do
        if ! ss -tlnp 2>/dev/null | grep -qE ":(${MASTER_PORT}|${SLAVE_PORT}) "; then
            break
        fi
        sleep 0.1
    done
    sleep 0.3
}

detach_xdp() {
    sudo bpftool link list 2>/dev/null | awk '/xdp.*lo/{print $1}' | tr -d ':' | while read -r lid; do
        [ -n "$lid" ] && sudo bpftool link detach id "$lid" 2>/dev/null || true
    done
    sudo ip link set dev lo xdpgeneric off 2>/dev/null || true
    sleep 0.3
}

full_cleanup() {
    kill_all
    sleep 0.5
    force_release_ports
    detach_xdp
    rm -f "${ROOT_DIR}/build/dump.rdb" "${ROOT_DIR}/build/dump.rdb.tmp" \
          "${ROOT_DIR}/build/appendonly.aof" "${ROOT_DIR}/build/incr.wal"
    sleep 0.3
}

# ---- Write minimal configs (no replication) ----
write_instance_conf() {
    local port=$1 conf=$2
    cat > "$conf" << EOF
port ${port}
network proactor
maxclients 1000
loglevel info
persistence none
expire off
EOF
}

# ---- Start a kvstore instance ----
start_instance() {
    local port=$1 conf=$2
    taskset -c "$SERVER_CORES" "$KVS_BIN" "$conf" &>/dev/null &
    local pid=$!
    if ! wait_for_port 127.0.0.1 "$port" 10; then
        err "kvstore port ${port} failed to start"
        return 1
    fi
    redis-cli -h 127.0.0.1 -p "$port" PING >/dev/null 2>&1 || { err "kvstore port ${port} not responding"; return 1; }
}

# ---- Start XDP sync daemon ----
start_sync_daemon() {
    detach_xdp
    sleep 0.2

    sudo taskset -c "$SYNC_CORES" "$SYNC_BIN" &>"$SYNC_LOG" &
    SYNC_PID=$!
    sleep 2

    kill -0 "$SYNC_PID" 2>/dev/null || { err "Sync daemon failed to start"; cat "$SYNC_LOG" >&2; return 1; }
    sudo ip link show dev lo 2>/dev/null | grep -q xdp || { err "XDP not attached to lo"; return 1; }
    wait_for_log "$SYNC_LOG" "connected to" 10 || { err "Daemon not connected to slave"; return 1; }
}

# ---- Stop sync daemon ----
stop_sync_daemon() {
    if [ -n "${SYNC_PID:-}" ] && kill -0 "$SYNC_PID" 2>/dev/null; then
        sudo kill -INT "$SYNC_PID" 2>/dev/null || true
        wait "$SYNC_PID" 2>/dev/null || true
    fi
    sudo killall -INT kvstore-incr-sync 2>/dev/null || true
    sleep 0.5
    sudo killall -9 kvstore-incr-sync 2>/dev/null || true
    sleep 0.3
    detach_xdp
}

# ---- Run benchmark, return QPS ----
run_bench_incr() {
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
step "XDP 增量同步性能影响测试"
log "Pipeline: ${PIPELINES[*]}  |  n=${BENCH_N} c=${BENCH_C} d=${BENCH_D}  |  CPU: server=${SERVER_CORES} client=${CLIENT_CORES} sync=${SYNC_CORES}"
echo ""

build_sync_daemon || exit 2
full_cleanup

# Result file header
{
    echo "# XDP 增量同步性能影响测试 — $(date '+%Y-%m-%d %H:%M:%S')"
    echo "# Pipeline: ${PIPELINES[*]}  n=${BENCH_N} c=${BENCH_C} d=${BENCH_D}"
    echo "# CPU: server=${SERVER_CORES} client=${CLIENT_CORES} sync=${SYNC_CORES}"
    echo ""
} > "$RESULT_FILE"

declare -A SET_NO_SYNC SET_WITH_SYNC GET_NO_SYNC GET_WITH_SYNC

# =============================================================================
# Baseline (no sync)
# =============================================================================

section "BASELINE — Master only"
echo ""

for P in "${PIPELINES[@]}"; do
    echo -n "  P=${P}  "

    full_cleanup
    write_instance_conf "$MASTER_PORT" "$MASTER_CONF"
    start_instance "$MASTER_PORT" "$MASTER_CONF" || { err "skip P=${P}"; continue; }

    set_qps=$(run_bench_incr 127.0.0.1 "$MASTER_PORT" set "$P")
    record_result "P${P}-SET" "baseline" "$set_qps"
    SET_NO_SYNC[$P]=$set_qps

    get_qps=$(run_bench_incr 127.0.0.1 "$MASTER_PORT" get "$P")
    record_result "P${P}-GET" "baseline" "$get_qps"
    GET_NO_SYNC[$P]=$get_qps

    record_raw ""
    kill_all
    force_release_ports
done

# =============================================================================
# With sync daemon
# =============================================================================

section "WITH SYNC — Master + Slave + Daemon"
echo ""

for P in "${PIPELINES[@]}"; do
    echo -n "  P=${P}  "

    full_cleanup

    # Start master
    write_instance_conf "$MASTER_PORT" "$MASTER_CONF"
    start_instance "$MASTER_PORT" "$MASTER_CONF" || { err "skip P=${P}"; continue; }

    # Start slave (simple instance, no replication)
    write_instance_conf "$SLAVE_PORT" "$SLAVE_CONF"
    start_instance "$SLAVE_PORT" "$SLAVE_CONF" || { err "skip P=${P}"; kill_all; continue; }

    # Start daemon
    start_sync_daemon || { err "skip P=${P}"; stop_sync_daemon; kill_all; force_release_ports; continue; }
    sleep 1

    set_qps_s=$(run_bench_incr 127.0.0.1 "$MASTER_PORT" set "$P")
    record_result "P${P}-SET" "with-sync" "$set_qps_s"
    SET_WITH_SYNC[$P]=$set_qps_s

    get_qps_s=$(run_bench_incr 127.0.0.1 "$MASTER_PORT" get "$P")
    record_result "P${P}-GET" "with-sync" "$get_qps_s"
    GET_WITH_SYNC[$P]=$get_qps_s

    record_raw ""
    stop_sync_daemon
    kill_all
    force_release_ports
done

# =============================================================================
# Summary table
# =============================================================================

echo ""
echo -e "  ${YELLOW}=== XDP 增量同步性能影响 ===${NC}"
echo ""
HDR=$(printf "  %-6s | %14s | %14s | %8s | %14s | %14s | %8s" \
    "P" "SET-baseline" "SET-with-sync" "影响" "GET-baseline" "GET-with-sync" "影响")
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
    bset=${SET_NO_SYNC[$P]:-0}
    sset=${SET_WITH_SYNC[$P]:-0}
    bget=${GET_NO_SYNC[$P]:-0}
    sget=${GET_WITH_SYNC[$P]:-0}
    rset=$(safe_ratio "$sset" "$bset")
    rget=$(safe_ratio "$sget" "$bget")
    ROW=$(printf "  P=%-4s | %14s | %14s | %8s | %14s | %14s | %8s" \
        "$P" "$bset" "$sset" "$rset" "$bget" "$sget" "$rget")
    echo "$ROW"
    echo "$ROW" >> "$RESULT_FILE"
done

{
    echo ""
    echo "影响 = with-sync / baseline × 100%"
    echo "SET: 写入增量同步开销（XDP 捕获 + TCP 重组 + RESP 解析 + 转发）"
    echo "GET: XDP BPF 固定开销（仅包过滤，无转发）"
} >> "$RESULT_FILE"

# =============================================================================
# Cleanup
# =============================================================================

full_cleanup
echo ""
log "结果: $RESULT_FILE"
echo ""
