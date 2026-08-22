#!/bin/bash
# =============================================================================
# profile_xdpkcp.sh — 高流量下对 kvstore master + master 网关做 perf 采样
#
# 用法:  sudo ./test/bench/profile_xdpkcp.sh [采样秒数] [pipeline]
# 输出:  ~/perf.data   (~/perf.data.old 为上一次)
# =============================================================================
set -u

ROOT_DIR="/home/xxf/9.1-kvstore"
BUILD_DIR="${ROOT_DIR}/build"
KVS_BIN="${BUILD_DIR}/kvstore"
GATEWAY_BIN="${BUILD_DIR}/kvstore-xdp-gateway"
BPF_OBJ="${BUILD_DIR}/xdp_kcp.bpf.o"

PROFILE_SECS="${1:-8}"
PIPELINE="${2:-160}"
BENCH_N=100000000          # 足够大，perf 采样结束后手动 kill
BENCH_C=50
BENCH_D=16

MASTER_PORT=2000
SLAVE_PORT=2002
SYNC_PORT=8888
MASTER_CORE=0
SLAVE_CORE=2
CLIENT_CORE=1
GW_MASTER_CORE=4
GW_SLAVE_CORE=5
VETH0=vxdp0
VETH1=vxdp1
VETH_IP0=10.200.0.1
VETH_IP1=10.200.0.2
SHM_MASTER=/kvxdp_bench_master
SHM_SLAVE=/kvxdp_bench_slave

TMP=$(mktemp -d -t profxdp.XXXXXX)
M_CONF="${TMP}/master.conf"
S_CONF="${TMP}/slave.conf"
M_LOG="${TMP}/master.log"
S_LOG="${TMP}/slave.log"
GW_M_LOG="${TMP}/gw_m.log"
GW_S_LOG="${TMP}/gw_s.log"

cleanup() {
    pkill -9 -f kvstore-xdp-gateway 2>/dev/null || true
    pkill -9 -x kvstore 2>/dev/null || true
    ip link del "$VETH0" 2>/dev/null || true
    ip link del "$VETH1" 2>/dev/null || true
    sleep 0.3
}
cleanup
trap 'cleanup' EXIT

# ---- 配置 ----
cat > "$M_CONF" <<EOF
port ${MASTER_PORT}
network proactor
maxclients 1000
loglevel info
persistence none
expire off
xdp_sync on
rdma_role master
EOF
cat > "$S_CONF" <<EOF
port ${SLAVE_PORT}
network proactor
maxclients 1000
loglevel info
persistence none
expire off
xdp_sync on
rdma_role slave
EOF

# ---- veth ----
ip link add "$VETH0" type veth peer name "$VETH1" || { echo "veth 失败"; exit 1; }
ip link set "$VETH0" up; ip link set "$VETH1" up
ip addr add "$VETH_IP0/24" dev "$VETH0" 2>/dev/null
ip addr add "$VETH_IP1/24" dev "$VETH1" 2>/dev/null
MAC0=$(ip link show "$VETH0" | awk '/link\/ether/{print $2; exit}')
MAC1=$(ip link show "$VETH1" | awk '/link\/ether/{print $2; exit}')

# ---- kvstore master + slave ----
taskset -c "$MASTER_CORE" env XDPKCP_SHM_NAME="$SHM_MASTER" "$KVS_BIN" "$M_CONF" &>"$M_LOG" &
MASTER_PID=$!
taskset -c "$SLAVE_CORE"  env XDPKCP_SHM_NAME="$SHM_SLAVE"  "$KVS_BIN" "$S_CONF" &>"$S_LOG" &
SLAVE_PID=$!

for i in $(seq 1 40); do
    redis-cli -h 127.0.0.1 -p "$MASTER_PORT" PING >/dev/null 2>&1 && break
    sleep 0.5
done

# ---- 网关（env 传 SHM 名，与引擎一致）----
env XDPKCP_SHM_NAME="$SHM_MASTER" taskset -c "$GW_MASTER_CORE" "$GATEWAY_BIN" --master \
    --iface "$VETH0" --bpf "$BPF_OBJ" \
    --local-ip "$VETH_IP0" --peer-ip "$VETH_IP1" --peer-mac "$MAC1" \
    --port "$SYNC_PORT" &>"$GW_M_LOG" &
env XDPKCP_SHM_NAME="$SHM_SLAVE" taskset -c "$GW_SLAVE_CORE" "$GATEWAY_BIN" --slave \
    --iface "$VETH1" --bpf "$BPF_OBJ" \
    --local-ip "$VETH_IP1" --peer-ip "$VETH_IP0" --peer-mac "$MAC0" \
    --port "$SYNC_PORT" &>"$GW_S_LOG" &

for i in $(seq 1 40); do
    grep -q "transport up" "$GW_M_LOG" 2>/dev/null && grep -q "transport up" "$GW_S_LOG" 2>/dev/null && break
    sleep 0.5
done

# 探测命令验证全链路
redis-cli -h 127.0.0.1 -p "$MASTER_PORT" SET __probe 1 >/dev/null 2>&1
for i in $(seq 1 20); do
    [ "$(redis-cli -h 127.0.0.1 -p "$SLAVE_PORT" GET __probe 2>/dev/null)" = "1" ] && break
    sleep 0.3
done

# ---- 预热 ----
taskset -c "$CLIENT_CORE" redis-benchmark -h 127.0.0.1 -p "$MASTER_PORT" \
    -t set -n 2000000 -c "$BENCH_C" -P 100 -d "$BENCH_D" -q >/dev/null 2>&1 || true

# ---- 持续高流量负载（后台）----
taskset -c "$CLIENT_CORE" redis-benchmark -h 127.0.0.1 -p "$MASTER_PORT" \
    -t set -n "$BENCH_N" -c "$BENCH_C" -P "$PIPELINE" -d "$BENCH_D" \
    >/dev/null 2>&1 &
BENCH_PID=$!

sleep 2   # 让负载稳定进入稳态

GW_M_PID=$(pgrep -f "kvstore-xdp-gateway --master" | head -1)
echo "采样目标: master=$MASTER_PID  gw_master=$GW_M_PID  持续 ${PROFILE_SECS}s"

# ---- perf 采样 ----
if [ -f ~/perf.data ]; then mv -f ~/perf.data ~/perf.data.old 2>/dev/null; fi
perf record -F 1999 -g -p "$MASTER_PID","$GW_M_PID" -o ~/perf.data -- sleep "$PROFILE_SECS"

kill "$BENCH_PID" 2>/dev/null || true
sleep 0.5

echo "=== master 日志尾部 ==="
tail -3 "$M_LOG"
echo "=== 网关 drop 计数 ==="
grep -oE 'drops=[0-9]+' "$GW_M_LOG" | tail -1
grep -oE 'drops=[0-9]+' "$GW_S_LOG" | tail -1
echo "=== perf.data 已保存: ~/perf.data ==="
ls -la ~/perf.data
