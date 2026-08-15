#!/bin/bash
# =============================================================================
# bench_xdpkcp.sh — AF_XDP+KCP 内核旁路增量同步对 kvstore master 的性能影响对比
#
# 对齐 bench_incr_sync.sh 的方法论，对比三种场景下 kvstore master 的 SET/GET 吞吐：
#   1. baseline : master 单独运行（无任何同步）
#   2. epoll    : 传统 epoll 增量同步（kvstore 内置 repl_incr，master+slave）
#   3. xdpkcp   : AF_XDP+KCP 内核旁路同步（master+slave 各配一个独立网关进程，
#                 两网关通过 veth 对用 raw UDP 互联，引擎与网关经 /dev/shm 通信）
#
# 与 ebpf（xdptracepoint）方案的关键架构差异，也是本脚本要量化的点：
#   - 同步流量走独立的 veth 网卡，客户端（127.0.0.1）路径上【无 XDP BPF 开销】；
#   - 写命令热路径仅多一次 SHM 入队（memcpy ≤8KB + 无锁 SPSC 两原子操作）；
#   - 读命令不触发任何钩子，GET 应与 baseline 持平（区别于 ebpf 的逐包 XDP 过滤）。
#
# 测量方法（同 bench_incr_sync.sh）：
#   - 默认单 key（不加 -r），把命令路径压到最便宜，让同步开销占比最大
#   - 每个测量点只测一轮（直接取该轮数据，反映单轮波动）
#   - 每个 P 内 baseline→epoll→xdpkcp 背靠背测，最小化时间漂移
#   - 测前预热 + 单核绑定（master/slave/client/两网关各占一个核）
#   - 同步完整性由功能脚本（test_xdpkcp.sh）负责；本脚本只记录网关 drop 计数
#
# 用法:
#   sudo ./test/bench/bench_xdpkcp.sh                 # 默认所有 pipeline
#   sudo ./test/bench/bench_xdpkcp.sh 40 80 160       # 只测指定深度
#   sudo ./test/bench/bench_xdpkcp.sh --realistic     # 随机 key 模式（-r）
#
# 可调环境变量:
#   BENCH_N BENCH_C BENCH_D  WARMUP_MULT  NET_MODEL  BUILD_DIR
#   MASTER_CORE SLAVE_CORE CLIENT_CORE GW_MASTER_CORE GW_SLAVE_CORE
#   SUDO_PASSWORD
#
# 前置条件:
#   - kvstore + kvstore-xdp-gateway + xdp_kcp.bpf.o 已编译（BUILD_DIR）
#   - redis-cli, redis-benchmark, bc, ip, taskset 可用
#
# 输出:  test/bench/results_xdpkcp.txt
# =============================================================================

script_name=$(basename "$0")
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/common.sh"
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

BENCH_N=${BENCH_N:-1000000}
BENCH_C=${BENCH_C:-50}
BENCH_D=${BENCH_D:-16}
WARMUP_MULT=${WARMUP_MULT:-2}     # 预热请求数 = WARMUP_MULT × BENCH_N
WARMUP_N=$((BENCH_N * WARMUP_MULT))

NET_MODEL=${NET_MODEL:-"proactor"}

if [ "$REALISTIC" -eq 1 ]; then
    R_FLAG="-r ${BENCH_RAND:-1000000}"
    KEY_DESC="realistic(-r ${BENCH_RAND:-1000000})"
else
    R_FLAG=""
    KEY_DESC="single-key(不加-r)"
fi

# Ports
MASTER_PORT=2000            # master 主端口
SLAVE_PORT=2002             # slave 主端口
INCR_REPL_PORT=2003         # 传统增量同步的独立 repl 端口
SYNC_PORT=8888              # AF_XDP+KCP 同步 UDP 端口（与 xdp_kcp.bpf.c 一致）

# CPU 单核绑定（每进程独占一核，避免核间迁移抖动）
MASTER_CORE=${MASTER_CORE:-"0"}
SLAVE_CORE=${SLAVE_CORE:-"2"}
CLIENT_CORE=${CLIENT_CORE:-"1"}
GW_MASTER_CORE=${GW_MASTER_CORE:-"4"}
GW_SLAVE_CORE=${GW_SLAVE_CORE:-"5"}

# Paths（AF_XDP+KCP 组件与 kvstore 一起编入 build/；baseline/epoll 复用同一二进制）
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
KVS_BIN="${BUILD_DIR}/kvstore"
GATEWAY_BIN="${BUILD_DIR}/kvstore-xdp-gateway"
BPF_OBJ="${BUILD_DIR}/xdp_kcp.bpf.o"

# veth 对（AF_XDP 不能跑在 lo 上，用 veth 把两网关二层互联）
VETH0="${VETH0:-vxdp0}"
VETH1="${VETH1:-vxdp1}"
VETH_IP0="10.200.0.1"
VETH_IP1="10.200.0.2"
SHM_MASTER="/kvxdp_bench_master"
SHM_SLAVE="/kvxdp_bench_slave"

SUDO_PASSWORD="${SUDO_PASSWORD:-1234}"

RESULT_FILE="${ROOT_DIR}/test/bench/results_xdpkcp.txt"

TMPDIR=$(mktemp -d -t bench_xdpkcp.XXXXXX)
trap "rm -rf $TMPDIR" EXIT

MASTER_CONF="${TMPDIR}/master.conf"
SLAVE_CONF="${TMPDIR}/slave.conf"
MASTER_LOG="${TMPDIR}/master.log"
SLAVE_LOG="${TMPDIR}/slave.log"
GW_M_LOG="${TMPDIR}/gw_master.log"
GW_S_LOG="${TMPDIR}/gw_slave.log"

GW_M_PID=""
GW_S_PID=""

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

# ---- sudo wrapper：root 直接跑，否则管道送密码 ----
run_sudo() {
    if [ "$(id -u)" -eq 0 ]; then
        "$@" 2>/dev/null
    else
        echo "$SUDO_PASSWORD" | sudo -S "$@" 2>/dev/null
    fi
}

# ---- 确保 kvstore + 网关 + BPF 已编译 ----
ensure_build() {
    if [ ! -x "$KVS_BIN" ] || [ ! -x "$GATEWAY_BIN" ] || [ ! -f "$BPF_OBJ" ]; then
        log "编译 kvstore + kvstore-xdp-gateway + xdp_kcp.bpf.o ..."
        if [ ! -f "$BUILD_DIR/CMakeCache.txt" ]; then
            cmake -S "$ROOT_DIR" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release >/dev/null 2>&1 || \
                { err "cmake configure 失败"; return 1; }
        fi
        cmake --build "$BUILD_DIR" --target kvstore kvstore-xdp-gateway >/dev/null 2>&1 || \
            { err "编译失败"; return 1; }
    fi
    return 0
}

# ---- 进程/端口/veth 清理 ----
kill_all() {
    run_sudo pkill -INT -f kvstore-xdp-gateway 2>/dev/null || true
    run_sudo pkill -9  -f kvstore-xdp-gateway 2>/dev/null || true
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

veth_teardown() {
    run_sudo ip link del "$VETH0" 2>/dev/null || true
    run_sudo ip link del "$VETH1" 2>/dev/null || true
}

full_cleanup() {
    kill_all
    sleep 0.5
    force_release_ports
    veth_teardown
    rm -f "${ROOT_DIR}/build/dump.rdb" "${ROOT_DIR}/build/dump.rdb.tmp" \
          "${ROOT_DIR}/build/appendonly.aof" "${ROOT_DIR}/build/incr.wal"
    sleep 0.3
}

# ---- 写实例配置 ----
# mode: none        — 无同步（baseline）
#       repl-master — 传统增量同步 master（incr_sync on + rdma_role master）
#       repl-slave  — 传统增量同步 slave
#       xdp-master  — AF_XDP+KCP master（xdp_sync on + rdma_role master）
#       xdp-slave   — AF_XDP+KCP slave
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
        xdp-master)
            cat > "$conf" << EOF
port ${port}
network ${NET_MODEL}
maxclients 1000
loglevel info
persistence none
expire off
xdp_sync on
rdma_role master
EOF
            ;;
        xdp-slave)
            cat > "$conf" << EOF
port ${port}
network ${NET_MODEL}
maxclients 1000
loglevel info
persistence none
expire off
xdp_sync on
rdma_role slave
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

# ---- 启动一个 kvstore 实例（单核绑定，可选注入 SHM 名）----
start_instance() {
    local port=$1 conf=$2 logfile=${3:-/dev/null} core=${4:-"$MASTER_CORE"} shm=${5:-}
    if [ -n "$shm" ]; then
        taskset -c "$core" env XDPKCP_SHM_NAME="$shm" "$KVS_BIN" "$conf" &>"$logfile" &
    else
        taskset -c "$core" "$KVS_BIN" "$conf" &>"$logfile" &
    fi
    if ! wait_for_port 127.0.0.1 "$port" 10; then
        err "kvstore port ${port} failed to start"
        return 1
    fi
    redis-cli -h 127.0.0.1 -p "$port" PING >/dev/null 2>&1 || { err "kvstore port ${port} not responding"; return 1; }
}

# ---- 校验 key 是否同步到 slave（带重试）----
wait_synced() {
    local port=$1 key=$2 val=$3 timeout=${4:-5}
    for ((i=0; i<timeout*2; i++)); do
        [ "$(redis-cli -h 127.0.0.1 -p "$port" GET "$key" 2>/dev/null)" = "$val" ] && return 0
        sleep 0.5
    done
    return 1
}

# ---- veth 对 + 双网关启动（AF_XDP+KCP 拓扑）----
veth_setup() {
    veth_teardown
    run_sudo ip link add "$VETH0" type veth peer name "$VETH1" || { err "veth 创建失败"; return 1; }
    run_sudo ip link set "$VETH0" up
    run_sudo ip link set "$VETH1" up
    run_sudo ip addr add "$VETH_IP0/24" dev "$VETH0" 2>/dev/null
    run_sudo ip addr add "$VETH_IP1/24" dev "$VETH1" 2>/dev/null
    return 0
}

start_gateways() {
    local mac0 mac1
    mac0=$(run_sudo ip link show "$VETH0" | awk '/link\/ether/{print $2; exit}')
    mac1=$(run_sudo ip link show "$VETH1" | awk '/link\/ether/{print $2; exit}')
    if [ -z "$mac0" ] || [ -z "$mac1" ]; then
        err "无法解析 veth MAC"
        return 1
    fi

    # master 网关：SHM 出队 → KCP → AF_XDP Tx；veth0
    echo "$SUDO_PASSWORD" | sudo -S env XDPKCP_SHM_NAME="$SHM_MASTER" \
        taskset -c "$GW_MASTER_CORE" "$GATEWAY_BIN" \
        --master --iface "$VETH0" --bpf "$BPF_OBJ" \
        --local-ip "$VETH_IP0" --peer-ip "$VETH_IP1" --peer-mac "$mac1" \
        --port "$SYNC_PORT" &>"$GW_M_LOG" &
    GW_M_PID=$!

    # slave 网关：AF_XDP Rx → KCP → SHM；veth1
    echo "$SUDO_PASSWORD" | sudo -S env XDPKCP_SHM_NAME="$SHM_SLAVE" \
        taskset -c "$GW_SLAVE_CORE" "$GATEWAY_BIN" \
        --slave --iface "$VETH1" --bpf "$BPF_OBJ" \
        --local-ip "$VETH_IP1" --peer-ip "$VETH_IP0" --peer-mac "$mac0" \
        --port "$SYNC_PORT" &>"$GW_S_LOG" &
    GW_S_PID=$!

    wait_for_log "$GW_M_LOG" "transport up" 15 || { err "master 网关未就绪"; return 1; }
    wait_for_log "$GW_S_LOG" "transport up" 15 || { err "slave 网关未就绪"; return 1; }
    sleep 1
    return 0
}

stop_gateways() {
    run_sudo pkill -INT -f kvstore-xdp-gateway 2>/dev/null || true
    sleep 0.5
    run_sudo pkill -9 -f kvstore-xdp-gateway 2>/dev/null || true
    sleep 0.3
}

# ---- xdpkcp 网关 drop 统计（热路径允许丢，这里只记录）----
report_xdpkcp_stats() {
    local mdrop sdrop
    mdrop=$(grep -oE 'drops=[0-9]+' "$GW_M_LOG" | tail -1 | cut -d= -f2)
    sdrop=$(grep -oE 'drops=[0-9]+' "$GW_S_LOG" | tail -1 | cut -d= -f2)
    echo "  [GW] master-drops=${mdrop:-0} slave-drops=${sdrop:-0}"
    record_raw "  [GW] master-drops=${mdrop:-0} slave-drops=${sdrop:-0}"
    if [ -n "$mdrop" ] && [ "$mdrop" -gt 0 ]; then
        warn "xdpkcp: master 网关有 ${mdrop} 次丢弃（KCP 窗口满，热路径允许丢）"
        record_raw "  xdpkcp: master-gateway LOSS (drops=${mdrop})"
    fi
}

# ---- 跑一次 benchmark，返回 QPS ----
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
# 场景拓扑
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
            # 先写一条命令填充 backlog，让 slave 的 PSYNC 命中 +CONTINUE
            redis-cli -h 127.0.0.1 -p "$MASTER_PORT" SET __incr_probe 1 >/dev/null 2>&1 || true
            write_instance_conf "$SLAVE_PORT" repl-slave "$SLAVE_CONF"
            start_instance "$SLAVE_PORT" "$SLAVE_CONF" "$SLAVE_LOG" "$SLAVE_CORE" || return 1
            wait_for_log "$SLAVE_LOG" "entering streaming" 10 || { err "epoll: slave 未进入流式复制"; return 1; }
            wait_synced "$SLAVE_PORT" __incr_probe 1 5 || warn "epoll: 探测命令未同步到 slave"
            ;;
        xdpkcp)
            veth_setup || return 1
            write_instance_conf "$MASTER_PORT" xdp-master "$MASTER_CONF"
            start_instance "$MASTER_PORT" "$MASTER_CONF" "$MASTER_LOG" "$MASTER_CORE" "$SHM_MASTER" || return 1
            write_instance_conf "$SLAVE_PORT" xdp-slave "$SLAVE_CONF"
            start_instance "$SLAVE_PORT" "$SLAVE_CONF" "$SLAVE_LOG" "$SLAVE_CORE" "$SHM_SLAVE" || return 1
            start_gateways || return 1
            # 探测命令验证全链路：master → SHM → 网关 → KCP/AF_XDP → 网关 → SHM → slave
            redis-cli -h 127.0.0.1 -p "$MASTER_PORT" SET __xdpkcp_probe 1 >/dev/null 2>&1 || true
            wait_synced "$SLAVE_PORT" __xdpkcp_probe 1 5 || warn "xdpkcp: 探测命令未同步到 slave"
            ;;
    esac
    return 0
}

teardown_topology() {
    local scenario=$1
    if [ "$scenario" = "xdpkcp" ]; then
        stop_gateways
        report_xdpkcp_stats
        veth_teardown
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

    warmup 127.0.0.1 "$MASTER_PORT"

    set_qps=$(run_bench_incr 127.0.0.1 "$MASTER_PORT" set "$P")
    record_result "P${P}-SET" "$scenario" "$set_qps"

    get_qps=$(run_bench_incr 127.0.0.1 "$MASTER_PORT" get "$P")
    record_result "P${P}-GET" "$scenario" "$get_qps"

    case "$scenario" in
        baseline) SET_BASE[$P]=$set_qps; GET_BASE[$P]=$get_qps ;;
        epoll)    SET_EPOLL[$P]=$set_qps; GET_EPOLL[$P]=$get_qps ;;
        xdpkcp)   SET_XDPKCP[$P]=$set_qps; GET_XDPKCP[$P]=$get_qps ;;
    esac

    record_raw ""
    teardown_topology "$scenario"
    return 0
}

# =============================================================================
# Setup
# =============================================================================

echo ""
step "增量同步性能影响对比（epoll 内置同步 vs AF_XDP+KCP 内核旁路）"
log "Pipeline: ${PIPELINES[*]}  |  n=${BENCH_N} c=${BENCH_C} d=${BENCH_D}  |  net=${NET_MODEL}"
log "键模式: ${KEY_DESC}  |  单轮测量  |  预热=${WARMUP_N}"
log "CPU: master=${MASTER_CORE} slave=${SLAVE_CORE} client=${CLIENT_CORE} gw_master=${GW_MASTER_CORE} gw_slave=${GW_SLAVE_CORE}"
echo ""

ensure_build || exit 2
full_cleanup

{
    echo "# AF_XDP+KCP 增量同步性能影响对比 — $(date '+%Y-%m-%d %H:%M:%S')"
    echo "# 场景: baseline(无同步) / epoll(内置 repl_incr) / xdpkcp(AF_XDP+KCP 网关)"
    echo "# Pipeline: ${PIPELINES[*]}  n=${BENCH_N} c=${BENCH_C} d=${BENCH_D}  net=${NET_MODEL}"
    echo "# 键模式: ${KEY_DESC}  单轮测量  预热=${WARMUP_N}"
    echo "# CPU: master=${MASTER_CORE} slave=${SLAVE_CORE} client=${CLIENT_CORE} gw_master=${GW_MASTER_CORE} gw_slave=${GW_SLAVE_CORE}"
    echo "# ports: master=${MASTER_PORT} slave=${SLAVE_PORT} incr_repl=${INCR_REPL_PORT} sync=${SYNC_PORT}"
    echo ""
} > "$RESULT_FILE"

declare -A SET_BASE GET_BASE SET_EPOLL GET_EPOLL SET_XDPKCP GET_XDPKCP

# =============================================================================
# Main loop — 每个 P 内 baseline→epoll→xdpkcp 背靠背测
# =============================================================================

section "增量同步性能影响（每个 P 内三场景背靠背）"
for P in "${PIPELINES[@]}"; do
    measure_scenario baseline "$P" || true
    measure_scenario epoll "$P" || true
    measure_scenario xdpkcp "$P" || true
done

# =============================================================================
# Summary table
# =============================================================================

FMT="  %-5s | %12s | %12s | %9s | %12s | %9s"
SEP="  %-5s-+-%12s-+-%12s-+-%9s-+-%12s-+-%9s"
HDR=$(printf "$FMT" "P" "baseline" "epoll" "epoll%" "xdpkcp" "xdpkcp%")
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
    xd=${SET_XDPKCP[$P]:-0}
    r_ep=$(safe_ratio "$ep" "$base")
    r_xd=$(safe_ratio "$xd" "$base")
    ROW=$(printf "$FMT" "$P" "$base" "$ep" "$r_ep" "$xd" "$r_xd")
    echo "$ROW"
    echo "$ROW" >> "$RESULT_FILE"
done

echo ""
echo -e "  ${CYAN}— GET（读，同步机制对读路径的影响）—${NC}"
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
    xd=${GET_XDPKCP[$P]:-0}
    r_ep=$(safe_ratio "$ep" "$base")
    r_xd=$(safe_ratio "$xd" "$base")
    ROW=$(printf "$FMT" "$P" "$base" "$ep" "$r_ep" "$xd" "$r_xd")
    echo "$ROW"
    echo "$ROW" >> "$RESULT_FILE"
done

{
    echo ""
    echo "epoll%  = 传统 epoll 增量同步 / baseline × 100%"
    echo "xdpkcp% = AF_XDP+KCP 增量同步  / baseline × 100%"
    echo "100% = 无影响，越低表示同步开销越大（>100% 为测量抖动，不代表真的加速）。"
    echo ""
    echo "说明:"
    echo "  - epoll : 内置 repl_incr，写命令在主线程内 memcpy 到 backlog + slave 写缓冲"
    echo "           并触发 EPOLLOUT 冲刷；读命令不传播，故 GET 应与 baseline 持平。"
    echo "  - xdpkcp: 同步流量走独立 veth 网卡，客户端(127.0.0.1)路径无 XDP BPF 开销；"
    echo "           写命令热路径仅多一次 SHM 入队（无锁 SPSC memcpy），读命令零开销，"
    echo "           故 GET 应与 baseline 持平，SET 略降。"
    echo "  - 每个数字为单轮测量结果；xdpkcp 场景额外记录网关 drop 计数。"
} >> "$RESULT_FILE"

# =============================================================================
# Cleanup
# =============================================================================

full_cleanup
echo ""
log "结果: $RESULT_FILE"
echo ""
