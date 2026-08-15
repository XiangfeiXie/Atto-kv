#!/usr/bin/env bash
# ============================================================================
# repl-incr-xdpkcp component functional smoke test
# ============================================================================
# End-to-end test of the AF_XDP + KCP kernel-bypass incremental sync:
#
#   master kvstore ──SHM──▶ master gateway ──AF_XDP/UDP──▶ slave gateway
#                                                            │ SHM
#                                                            ▼
#                                                      slave kvstore
#
# Because AF_XDP cannot run on `lo`, this test creates a veth pair (vxdp0↔vxdp1)
# and runs master + slave gateways on opposite ends.  The client talks to the
# master over normal TCP; the slave is populated only via the XDP+KCP path.
#
# Usage:
#   ./test/smoke/test_xdpkcp.sh
#   MASTER_PORT=6666 SLAVE_PORT=6667 SYNC_PORT=8888 ./test/smoke/test_xdpkcp.sh
#
# Prerequisites:
#   - kvstore + kvstore-xdp-gateway + xdp_kcp.bpf.o (build/ or BUILD_DIR)
#   - redis-cli, sudo (password via SUDO_PASSWORD, default 1234), iproute2
# ============================================================================
set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ── Configuration ────────────────────────────────────────────────────────────

MASTER_PORT="${MASTER_PORT:-6666}"
SLAVE_PORT="${SLAVE_PORT:-6667}"
SYNC_PORT="${SYNC_PORT:-8888}"

BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build-xdpkcp}"
KVSTORE_BIN="${KVSTORE_BIN:-${BUILD_DIR}/kvstore}"
GATEWAY_BIN="${GATEWAY_BIN:-${BUILD_DIR}/kvstore-xdp-gateway}"
BPF_OBJ="${BPF_OBJ:-${BUILD_DIR}/xdp_kcp.bpf.o}"

VETH0="${VETH0:-vxdp0}"
VETH1="${VETH1:-vxdp1}"
VETH_IP0="10.200.0.1"
VETH_IP1="10.200.0.2"
SHM_MASTER="${SHM_MASTER:-/kvxdp_master}"
SHM_SLAVE="${SHM_SLAVE:-/kvxdp_slave}"

SUDO_PASSWORD="${SUDO_PASSWORD:-1234}"
HOST="${HOST:-127.0.0.1}"
TIMEOUT="${TIMEOUT:-3s}"

RCLI_M="timeout ${TIMEOUT} redis-cli -h ${HOST} -p ${MASTER_PORT}"
RCLI_S="timeout ${TIMEOUT} redis-cli -h ${HOST} -p ${SLAVE_PORT}"

MASTER_CONF=$(mktemp)
SLAVE_CONF=$(mktemp)
MASTER_LOG=$(mktemp)
SLAVE_LOG=$(mktemp)
GW_M_LOG=$(mktemp)
GW_S_LOG=$(mktemp)

MASTER_PID=""
SLAVE_PID=""
GW_M_PID=""
GW_S_PID=""

# ── Test framework ───────────────────────────────────────────────────────────

PASS=0; FAIL=0
FAILED_NAMES=()
RED=$'\033[31m' GRN=$'\033[32m' YEL=$'\033[33m' RST=$'\033[0m'

ok()  { PASS=$((PASS+1)); printf "  ${GRN}PASS${RST}  %s\n" "$1"; }
bad() { FAIL=$((FAIL+1)); FAILED_NAMES+=("$1");
        printf "  ${RED}FAIL${RST}  %s\n        expected: %s\n        actual:   %s\n" "$1" "$2" "$3"; }
section() { printf "\n${YEL}── %s ──${RST}\n" "$1"; }
chk() { [ "$3" = "$2" ] && ok "$1" || bad "$1" "$2" "$3"; }

sudo_cmd() { echo "$SUDO_PASSWORD" | sudo -S "$@" 2>/dev/null; }

# wait until `pattern` appears in `file` (default 10s)
wait_for_log() {
    local file="$1" pattern="$2" timeout="${3:-10}" i=0
    while [ "$i" -lt "$((timeout * 10))" ]; do
        grep -q "$pattern" "$file" 2>/dev/null && return 0
        sleep 0.1; i=$((i+1))
    done
    return 1
}

summary() {
    local total=$((PASS + FAIL))
    printf "\n────────────────────────────────────────────────────────\n"
    printf "XDP+KCP smoke: %d/%d passed" "$PASS" "$total"
    if [ "$FAIL" -gt 0 ]; then
        printf "  (${RED}%d failed${RST})\n" "$FAIL"
        for n in "${FAILED_NAMES[@]}"; do printf "    • %s\n" "$n"; done
        return 1
    fi
    printf "  ${GRN}ALL PASS${RST}\n"
    return 0
}

# ── Cleanup ──────────────────────────────────────────────────────────────────

cleanup() {
    for pid in "$GW_M_PID" "$GW_S_PID"; do
        [ -n "$pid" ] && sudo_cmd kill "$pid" 2>/dev/null
    done
    sudo_cmd pkill -f kvstore-xdp-gateway 2>/dev/null
    for pid in "$MASTER_PID" "$SLAVE_PID"; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null
    done
    wait 2>/dev/null

    # Delete the veth pair (this also detaches any XDP program).
    sudo_cmd ip link del "$VETH0" 2>/dev/null

    rm -f /dev/shm/"${SHM_MASTER#/}" /dev/shm/"${SHM_SLAVE#/}" 2>/dev/null
    rm -f "$MASTER_CONF" "$SLAVE_CONF" "$MASTER_LOG" "$SLAVE_LOG" "$GW_M_LOG" "$GW_S_LOG"
}
trap cleanup EXIT INT TERM

# ── Prerequisites ────────────────────────────────────────────────────────────

section "Prerequisites"

if [ ! -x "$KVSTORE_BIN" ] || [ ! -x "$GATEWAY_BIN" ] || [ ! -f "$BPF_OBJ" ]; then
    printf "${RED}FATAL${RST}: binaries not found under %s\n" "$BUILD_DIR" >&2
    printf "  build with: cmake -S . -B %s -DCMAKE_BUILD_TYPE=Release\n" "$BUILD_DIR" >&2
    printf "              cmake --build %s --target kvstore kvstore-xdp-gateway\n" "$BUILD_DIR" >&2
    exit 2
fi
ok "kvstore + gateway + BPF object present"

command -v redis-cli >/dev/null 2>&1 || { echo "FATAL: redis-cli not found" >&2; exit 2; }
ok "redis-cli available"

if ! echo "$SUDO_PASSWORD" | sudo -S true 2>/dev/null; then
    printf "${RED}FATAL${RST}: sudo unavailable (needed for veth + XDP attach)\n" >&2
    exit 2
fi
ok "sudo available"

# ── Network setup (veth pair) ────────────────────────────────────────────────

section "veth pair setup"

sudo_cmd ip link del "$VETH0" 2>/dev/null   # clear stale
sudo_cmd ip link add "$VETH0" type veth peer name "$VETH1" || \
    { echo "FATAL: cannot create veth pair" >&2; exit 2; }
sudo_cmd ip link set "$VETH0" up
sudo_cmd ip link set "$VETH1" up
sudo_cmd ip addr add "$VETH_IP0/24" dev "$VETH0" 2>/dev/null
sudo_cmd ip addr add "$VETH_IP1/24" dev "$VETH1" 2>/dev/null

MAC0=$(sudo_cmd ip link show "$VETH0" | awk '/link\/ether/{print $2; exit}')
MAC1=$(sudo_cmd ip link show "$VETH1" | awk '/link\/ether/{print $2; exit}')
if [ -z "$MAC0" ] || [ -z "$MAC1" ]; then
    echo "FATAL: cannot resolve veth MACs" >&2; exit 2
fi
ok "veth pair up ($VETH0=$MAC0 ↔ $VETH1=$MAC1)"

# ── Start kvstore master & slave ─────────────────────────────────────────────

section "Start kvstore master & slave"

cat > "$MASTER_CONF" <<EOF
port ${MASTER_PORT}
network reactor
maxclients 200
loglevel info
persistence none
expire off
rdma_sync off
incr_sync off
xdp_sync on
rdma_role master
EOF

cat > "$SLAVE_CONF" <<EOF
port ${SLAVE_PORT}
network reactor
maxclients 200
loglevel info
persistence none
expire off
rdma_sync off
incr_sync off
xdp_sync on
rdma_role slave
EOF

XDPKCP_SHM_NAME="$SHM_MASTER" "$KVSTORE_BIN" "$MASTER_CONF" &>"$MASTER_LOG" &
MASTER_PID=$!
XDPKCP_SHM_NAME="$SHM_SLAVE"  "$KVSTORE_BIN" "$SLAVE_CONF"  &>"$SLAVE_LOG" &
SLAVE_PID=$!

sleep 1
kill -0 "$MASTER_PID" 2>/dev/null || { echo "FATAL: master kvstore failed"; cat "$MASTER_LOG" >&2; exit 2; }
kill -0 "$SLAVE_PID"  2>/dev/null || { echo "FATAL: slave kvstore failed";  cat "$SLAVE_LOG"  >&2; exit 2; }
$RCLI_M PING >/dev/null 2>&1 || { echo "FATAL: master not responding"; exit 2; }
$RCLI_S PING >/dev/null 2>&1 || { echo "FATAL: slave not responding";  exit 2; }
ok "kvstore master + slave up (PING ok)"

# ── Start gateways ───────────────────────────────────────────────────────────

section "Start gateways"

# Gateways run under sudo (AF_XDP socket + XDP attach need root).  Capture both
# stdout and stderr so the readiness marker *and* any init error are visible.
echo "$SUDO_PASSWORD" | sudo -S env XDPKCP_SHM_NAME="$SHM_MASTER" "$GATEWAY_BIN" \
    --master --iface "$VETH0" --bpf "$BPF_OBJ" \
    --local-ip "$VETH_IP0" --peer-ip "$VETH_IP1" --peer-mac "$MAC1" \
    --port "$SYNC_PORT" &>"$GW_M_LOG" &
GW_M_PID=$!

echo "$SUDO_PASSWORD" | sudo -S env XDPKCP_SHM_NAME="$SHM_SLAVE" "$GATEWAY_BIN" \
    --slave --iface "$VETH1" --bpf "$BPF_OBJ" \
    --local-ip "$VETH_IP1" --peer-ip "$VETH_IP0" --peer-mac "$MAC0" \
    --port "$SYNC_PORT" &>"$GW_S_LOG" &
GW_S_PID=$!

if ! wait_for_log "$GW_M_LOG" "transport up" 15; then
    printf "${RED}FATAL${RST}: master gateway failed to init\n" >&2
    cat "$GW_M_LOG" >&2; exit 2
fi
if ! wait_for_log "$GW_S_LOG" "transport up" 15; then
    printf "${RED}FATAL${RST}: slave gateway failed to init\n" >&2
    cat "$GW_S_LOG" >&2; exit 2
fi
ok "master + slave gateways up (AF_XDP + KCP ready)"
sleep 1   # let KCP exchange an initial round-trip

# ── Replication tests ────────────────────────────────────────────────────────

section "Replication (master → XDP+KCP → slave)"

# SET
$RCLI_M SET xdpkcp:hello world >/dev/null
chk "SET replicated" "world" "$($RCLI_S GET xdpkcp:hello)"

# INCR (non-idempotent — must apply exactly once on the slave)
$RCLI_M SET xdpkcp:counter 0 >/dev/null
$RCLI_M INCR xdpkcp:counter >/dev/null
$RCLI_M INCR xdpkcp:counter >/dev/null
$RCLI_M INCR xdpkcp:counter >/dev/null
chk "INCR x3 replicated" "3" "$($RCLI_S GET xdpkcp:counter)"

# MSET / MGET (multi-key write)
$RCLI_M MSET xdpkcp:a 1 xdpkcp:b 2 xdpkcp:c 3 >/dev/null
chk "MSET replicated (MGET)" "1 2 3" "$($RCLI_S MGET xdpkcp:a xdpkcp:b xdpkcp:c | tr '\n' ' ' | sed 's/ $//')"

# value with spaces / binary-ish content
$RCLI_M SET xdpkcp:space "hello world 你好" >/dev/null
chk "SET with spaces replicated" "hello world 你好" "$($RCLI_S GET xdpkcp:space)"

# pipeline: 50 SETs in one go, then spot-check
for i in $(seq 1 50); do $RCLI_M SET "xdpkcp:key:$i" "val$i" >/dev/null; done
sleep 0.5
chk "pipeline of 50 SETs (key:25)" "val25" "$($RCLI_S GET xdpkcp:key:25)"
chk "pipeline of 50 SETs (key:50)" "val50" "$($RCLI_S GET xdpkcp:key:50)"

# DEL is a write too
$RCLI_M DEL xdpkcp:a >/dev/null
chk "DEL replicated" "" "$($RCLI_S GET xdpkcp:a)"

# ── Summary ──────────────────────────────────────────────────────────────────

summary
