#!/bin/bash
# =============================================================================
# bench_incr_sync.sh — 增量同步对 kvstore master 的性能影响对比
#
# 对比三种场景下 kvstore master 的 SET/GET 吞吐，量化两种增量同步方案的开销：
#   1. baseline : master 单独运行（无任何同步）
#   2. epoll    : 传统 epoll 增量同步（kvstore 内置 repl_incr，master+slave）
#   3. ebpf     : eBPF 组件增量同步（XDP 旁路 daemon，master+slave+daemon）
#
# 关键改进（相比早期版本，消除测量噪声、让服务端同步开销真正暴露出来）：
#   - 默认单 key（不加 -r）：把命令路径压到最便宜，让同步开销占比最大
#     （redis-benchmark 不加 -r 时所有请求打到同一个字面量 key:__rand_int__）
#     （用 --realistic 切回随机 key，此时应关注绝对 QPS 下降而非百分比）
#   - 每个测量点只测一轮（直接取该轮数据，反映单轮波动）
#   - 每个 P 内 baseline→epoll→ebpf 背靠背测，最小化时间漂移
#   - 测前预热（预填 key / 暖缓存）
#   - 单核绑定（master/slave/client/daemon 各占一个核，避免迁移抖动）
#   - 同步完整性由功能校验脚本负责，本脚本只记录 ebpf 的 ringbuf_full/ooo/send_drop 计数
#
# 用法:
#   sudo ./test/bench/bench_incr_sync.sh                 # 默认所有 pipeline
#   sudo ./test/bench/bench_incr_sync.sh 40 80 160       # 只测指定深度
#   sudo ./test/bench/bench_incr_sync.sh --realistic     # 随机 key 模式（-r）
#
# 可调环境变量:
#   BENCH_N BENCH_C BENCH_D  WARMUP_MULT  NET_MODEL
#   MASTER_CORE SLAVE_CORE CLIENT_CORE SYNC_CORE
#
# 前置条件:
#   - kvstore 已编译（build/kvstore）
#   - clang + libbpf 可用（编译 XDP daemon）
#   - redis-cli, redis-benchmark, bc, bpftool 可用
#   - root 权限（XDP attach）
#   - config.h 中 XDP_IFACE=lo、SLAVE_IP=127.0.0.1（本机回环压测）
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

# Benchmark parameters (overridable via env)
BENCH_N=${BENCH_N:-1000000}
BENCH_C=${BENCH_C:-50}
BENCH_D=${BENCH_D:-16}
WARMUP_MULT=${WARMUP_MULT:-2}     # 预热请求数 = WARMUP_MULT × BENCH_N
WARMUP_N=$((BENCH_N * WARMUP_MULT))

# 所有场景统一使用 reactor(epoll) 引擎，保证“传统 epoll 同步”与“eBPF 同步”
# 的 master 运行在相同网络模型下，唯一变量是同步机制。
NET_MODEL=${NET_MODEL:-"reactor"}

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
MASTER_PORT=2000            # master 主端口（也是 XDP 抓包端口）
SLAVE_PORT=2002             # slave 主端口（也是 XDP 回放目标端口）
INCR_REPL_PORT=2003         # 传统增量同步的独立 repl 端口（master 监听 / slave 连接）

# CPU 单核绑定（每进程独占一核，避免核间迁移抖动）
MASTER_CORE=${MASTER_CORE:-"0"}
SLAVE_CORE=${SLAVE_CORE:-"2"}
CLIENT_CORE=${CLIENT_CORE:-"1"}
SYNC_CORE=${SYNC_CORE:-"4"}

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
    local need=0
    if [ ! -x "$SYNC_BIN" ]; then
        need=1
    fi

    # 源码（含 config.h 的 IP/iface/tunable 改动）比二进制新 → 重编
    if [ $need -eq 0 ]; then
        local src
        for src in config.h xdp_common.h main.c xdp_loader.c stream.c replay.c xdp_filter.bpf.c; do
            [ "${SYNC_SRC_DIR}/${src}" -nt "$SYNC_BIN" ] && { need=1; break; }
        done
    fi

    # 二进制里编译进去的端口与当前不一致 → 重编
    if [ $need -eq 0 ]; then
        local compiled
        compiled=$(strings "$SYNC_BIN" 2>/dev/null | grep -oP 'slave=\S+:\d+ filter_port=\d+' | head -1 || true)
        echo "$compiled" | grep -q "slave=.*:${SLAVE_PORT} filter_port=${MASTER_PORT}" || need=1
    fi

    if [ $need -eq 1 ]; then
        mkdir -p "$SYNC_BUILD_DIR"
        if ! (cd "$SYNC_BUILD_DIR" && \
              cmake .. -DCMAKE_BUILD_TYPE=Release \
                      -DCMAKE_C_FLAGS="-DSLAVE_PORT=${SLAVE_PORT} -DKVS_FILTER_PORT=${MASTER_PORT}" \
                      >/dev/null 2>&1 && \
              make -j"$(nproc)" >/dev/null 2>&1); then
            err "Failed to build sync daemon"
            return 1
        fi
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
    for port in $MASTER_PORT $SLAVE_PORT $INCR_REPL_PORT; do
        ss -tlnp 2>/dev/null | grep -q ":${port} " && fuser -k ${port}/tcp 2>/dev/null || true
    done
    for ((i=0; i<20; i++)); do
        if ! ss -tlnp 2>/dev/null | grep -qE ":(${MASTER_PORT}|${SLAVE_PORT}|${INCR_REPL_PORT}) "; then
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

# ---- Write instance config ----
# mode: none        — 无同步（baseline / ebpf 场景）
#       repl-master — 传统增量同步 master（incr_sync on + rdma_role master）
#       repl-slave  — 传统增量同步 slave（incr_sync on + rdma_role slave）
write_instance_conf() {
    local port=$1 mode=$2 conf=$3
    case "$mode" in
        repl-master)
            cat > "$conf" << EOF
port ${port}
network ${NET_MODEL}
maxclients 1000
loglevel info
persistence none
expire off
incr_sync on
rdma_sync off
rdma_role master
incr_repl_port ${INCR_REPL_PORT}
EOF
            ;;
        repl-slave)
            cat > "$conf" << EOF
port ${port}
network ${NET_MODEL}
maxclients 1000
loglevel info
persistence none
expire off
incr_sync on
rdma_sync off
rdma_role slave
rdma_master_ip 127.0.0.1
incr_repl_port ${INCR_REPL_PORT}
EOF
            ;;
        *)
            cat > "$conf" << EOF
port ${port}
network ${NET_MODEL}
maxclients 1000
loglevel info
persistence none
expire off
EOF
            ;;
    esac
}

# ---- Start a kvstore instance (pinned to a single core) ----
start_instance() {
    local port=$1 conf=$2 logfile=${3:-/dev/null} core=${4:-"$MASTER_CORE"}
    taskset -c "$core" "$KVS_BIN" "$conf" &>"$logfile" &
    local pid=$!
    if ! wait_for_port 127.0.0.1 "$port" 10; then
        err "kvstore port ${port} failed to start"
        return 1
    fi
    redis-cli -h 127.0.0.1 -p "$port" PING >/dev/null 2>&1 || { err "kvstore port ${port} not responding"; return 1; }
}

# ---- Verify a key reached the slave (with retries) ----
wait_synced() {
    local port=$1 key=$2 val=$3 timeout=${4:-5}
    for ((i=0; i<timeout*2; i++)); do
        [ "$(redis-cli -h 127.0.0.1 -p "$port" GET "$key" 2>/dev/null)" = "$val" ] && return 0
        sleep 0.5
    done
    return 1
}

# ---- Start XDP sync daemon ----
start_sync_daemon() {
    detach_xdp
    sleep 0.2

    sudo taskset -c "$SYNC_CORE" "$SYNC_BIN" &>"$SYNC_LOG" &
    SYNC_PID=$!
    sleep 2

    kill -0 "$SYNC_PID" 2>/dev/null || { err "Sync daemon failed to start"; cat "$SYNC_LOG" >&2; return 1; }
    sudo ip link show dev lo 2>/dev/null | grep -q xdp || { err "XDP not attached to lo"; return 1; }
    wait_for_log "$SYNC_LOG" "connected to" 10 || { err "Daemon not connected to slave"; return 1; }
}

# ---- Stop sync daemon (SIGINT → 优雅退出 → 打印最终 [STATS]/[XDP-BPF]) ----
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

# ---- Run benchmark once, return QPS ----
run_bench_incr() {
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

# ---- 预热（预填 key / 暖缓存）----
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
            write_instance_conf "$MASTER_PORT" none "$MASTER_CONF"
            start_instance "$MASTER_PORT" "$MASTER_CONF" "$MASTER_LOG" "$MASTER_CORE" || return 1
            ;;
        epoll)
            write_instance_conf "$MASTER_PORT" repl-master "$MASTER_CONF"
            start_instance "$MASTER_PORT" "$MASTER_CONF" "$MASTER_LOG" "$MASTER_CORE" || return 1
            # 先写一条命令填充 backlog，让 slave 的 PSYNC 0 命中 +CONTINUE
            # （空 backlog 会触发 +FULLRESYNC，slave 静默不进入流式复制）。
            redis-cli -h 127.0.0.1 -p "$MASTER_PORT" SET __incr_probe 1 >/dev/null 2>&1 || true
            write_instance_conf "$SLAVE_PORT" repl-slave "$SLAVE_CONF"
            start_instance "$SLAVE_PORT" "$SLAVE_CONF" "$SLAVE_LOG" "$SLAVE_CORE" || return 1
            wait_for_log "$SLAVE_LOG" "entering streaming" 10 || { err "epoll: slave 未进入流式复制"; return 1; }
            wait_synced "$SLAVE_PORT" __incr_probe 1 5 || warn "epoll: 探测命令未同步到 slave"
            ;;
        ebpf)
            write_instance_conf "$MASTER_PORT" none "$MASTER_CONF"
            start_instance "$MASTER_PORT" "$MASTER_CONF" "$MASTER_LOG" "$MASTER_CORE" || return 1
            write_instance_conf "$SLAVE_PORT" none "$SLAVE_CONF"
            start_instance "$SLAVE_PORT" "$SLAVE_CONF" "$SLAVE_LOG" "$SLAVE_CORE" || return 1
            start_sync_daemon || return 1
            sleep 1
            ;;
    esac
    return 0
}

# ---- ebpf daemon 最终统计（丢包/乱序计数）----
report_ebpf_stats() {
    local xdp stats
    xdp=$(grep -oE '\[XDP-BPF\] total=[0-9]+ submitted=[0-9]+ ringbuf_full=[0-9]+ probe_fail=[0-9]+' "$SYNC_LOG" | tail -1)
    stats=$(grep -oE '\[STATS\] events=[0-9]+ written=[0-9]+ ooo=[0-9]+ send_drop=[0-9]+ conns=[0-9]+ holes=[0-9]+ dup=[0-9]+ corrupt=[0-9]+' "$SYNC_LOG" | tail -1)

    [ -n "$xdp" ] && { echo "  ${xdp}"; record_raw "  ${xdp}"; }
    [ -n "$stats" ] && { echo "  ${stats}"; record_raw "  ${stats}"; }

    if echo "$xdp" | grep -qE 'ringbuf_full=[1-9]'; then
        warn "ebpf: ringbuf_full > 0（观察者丢包，同步不完整）"
        record_raw "  ebpf: RINGBUF FULL (dropped events)"
    fi
    if echo "$stats" | grep -qE 'send_drop=[1-9]|ooo=[1-9]|corrupt=[1-9]'; then
        warn "ebpf: 检测到 send_drop/ooo/corrupt（同步有损）"
        record_raw "  ebpf: LOSS detected (send_drop/ooo/corrupt)"
    fi
}

teardown_topology() {
    local scenario=$1
    if [ "$scenario" = "ebpf" ]; then
        stop_sync_daemon
        report_ebpf_stats
    fi
    kill_all
    force_release_ports
}

measure_scenario() {
    local scenario=$1 P=$2
    local set_qps get_qps

    echo ""
    echo -n "  P=${P} [${scenario}] "

    full_cleanup
    setup_topology "$scenario" "$P" || { err "P=${P} ${scenario} 拓扑启动失败"; teardown_topology "$scenario"; return 1; }

    # 预热
    warmup 127.0.0.1 "$MASTER_PORT"

    # SET（单轮）
    set_qps=$(run_bench_incr 127.0.0.1 "$MASTER_PORT" set "$P")
    record_result "P${P}-SET" "$scenario" "$set_qps"

    # GET（单轮）
    get_qps=$(run_bench_incr 127.0.0.1 "$MASTER_PORT" get "$P")
    record_result "P${P}-GET" "$scenario" "$get_qps"

    case "$scenario" in
        baseline) SET_BASE[$P]=$set_qps; GET_BASE[$P]=$get_qps ;;
        epoll)    SET_EPOLL[$P]=$set_qps; GET_EPOLL[$P]=$get_qps ;;
        ebpf)     SET_EBPF[$P]=$set_qps; GET_EBPF[$P]=$get_qps ;;
    esac

    record_raw ""
    teardown_topology "$scenario"
    return 0
}

# =============================================================================
# Setup
# =============================================================================

echo ""
step "增量同步性能影响对比（epoll 内置同步 vs eBPF 组件）"
log "Pipeline: ${PIPELINES[*]}  |  n=${BENCH_N} c=${BENCH_C} d=${BENCH_D}  |  net=${NET_MODEL}"
log "键模式: ${KEY_DESC}  |  单轮测量  |  预热=${WARMUP_N}"
log "CPU: master=${MASTER_CORE} slave=${SLAVE_CORE} client=${CLIENT_CORE} sync=${SYNC_CORE}"
echo ""

build_sync_daemon || exit 2
full_cleanup

# Result file header
{
    echo "# 增量同步性能影响对比 — $(date '+%Y-%m-%d %H:%M:%S')"
    echo "# 场景: baseline(无同步) / epoll(内置 repl_incr) / ebpf(XDP 组件)"
    echo "# Pipeline: ${PIPELINES[*]}  n=${BENCH_N} c=${BENCH_C} d=${BENCH_D}  net=${NET_MODEL}"
    echo "# 键模式: ${KEY_DESC}  单轮测量  预热=${WARMUP_N}"
    echo "# CPU: master=${MASTER_CORE} slave=${SLAVE_CORE} client=${CLIENT_CORE} sync=${SYNC_CORE}"
    echo "# ports: master=${MASTER_PORT} slave=${SLAVE_PORT} incr_repl=${INCR_REPL_PORT}"
    echo ""
} > "$RESULT_FILE"

declare -A SET_BASE GET_BASE SET_EPOLL GET_EPOLL SET_EBPF GET_EBPF

# =============================================================================
# Main loop — 每个 P 内 baseline→epoll→ebpf 背靠背测
# =============================================================================

section "增量同步性能影响（每个 P 内三场景背靠背）"
for P in "${PIPELINES[@]}"; do
    measure_scenario baseline "$P" || true
    measure_scenario epoll "$P" || true
    measure_scenario ebpf "$P" || true
done

# =============================================================================
# Summary table
# =============================================================================

FMT="  %-5s | %12s | %12s | %9s | %12s | %9s"
SEP="  %-5s-+-%12s-+-%12s-+-%9s-+-%12s-+-%9s"
HDR=$(printf "$FMT" "P" "baseline" "epoll" "epoll%" "ebpf" "ebpf%")
SEPLINE=$(printf "$SEP" "-----" "------------" "------------" "---------" "------------" "---------")

echo ""
echo -e "  ${YELLOW}=== 增量同步性能影响对比 ===${NC}"
echo ""

{
    echo ""
    echo "=== 结果汇总 ==="
    echo ""
} >> "$RESULT_FILE"

echo -e "  ${CYAN}— SET（写入，含增量同步开销）—${NC}"
echo "$HDR"
echo "$SEPLINE"
{
    echo "--- SET ---"
    echo "$HDR"
    echo "$SEPLINE"
} >> "$RESULT_FILE"

for P in "${PIPELINES[@]}"; do
    base=${SET_BASE[$P]:-0}
    ep=${SET_EPOLL[$P]:-0}
    eb=${SET_EBPF[$P]:-0}
    r_ep=$(safe_ratio "$ep" "$base")
    r_eb=$(safe_ratio "$eb" "$base")
    ROW=$(printf "$FMT" "$P" "$base" "$ep" "$r_ep" "$eb" "$r_eb")
    echo "$ROW"
    echo "$ROW" >> "$RESULT_FILE"
done

echo ""
echo -e "  ${CYAN}— GET（读，仅 XDP 过滤固定开销）—${NC}"
echo "$HDR"
echo "$SEPLINE"
{
    echo ""
    echo "--- GET ---"
    echo "$HDR"
    echo "$SEPLINE"
} >> "$RESULT_FILE"

for P in "${PIPELINES[@]}"; do
    base=${GET_BASE[$P]:-0}
    ep=${GET_EPOLL[$P]:-0}
    eb=${GET_EBPF[$P]:-0}
    r_ep=$(safe_ratio "$ep" "$base")
    r_eb=$(safe_ratio "$eb" "$base")
    ROW=$(printf "$FMT" "$P" "$base" "$ep" "$r_ep" "$eb" "$r_eb")
    echo "$ROW"
    echo "$ROW" >> "$RESULT_FILE"
done

{
    echo ""
    echo "epoll% = 传统 epoll 增量同步 / baseline × 100%"
    echo "ebpf%  = eBPF 组件增量同步  / baseline × 100%"
    echo "100% = 无影响，越低表示同步开销越大（>100% 为测量抖动，不代表真的加速）。"
    echo ""
    echo "说明:"
    echo "  - epoll: 内置 repl_incr，写命令在主线程内 memcpy 到 backlog + slave 写缓冲"
    echo "          并触发 EPOLLOUT 冲刷；读命令不传播，故 GET 应与 baseline 持平。"
    echo "  - ebpf : XDP 旁路抓包 + 用户态 seq 重组 + verbatim 回放，不占用 master 线程；"
    echo "          但每个命中的包都要过一次 XDP BPF（读/写均如此），故 GET 也有固定开销。"
    echo "  - 每个数字为单轮测量结果；ebpf 场景额外记录回放/丢包统计。"
} >> "$RESULT_FILE"

# =============================================================================
# Cleanup
# =============================================================================

full_cleanup
echo ""
log "结果: $RESULT_FILE"
echo ""
