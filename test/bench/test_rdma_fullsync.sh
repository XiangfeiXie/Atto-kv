#!/bin/bash
# =============================================================================
# test_rdma_fullsync.sh — RDMA full sync smoke test (fixed config)
#
# Architecture note:
#   Slave sends FULLSYNC TCP → rdma_master_port (must reach master's TCP listener)
#   Slave does RDMA connect → rdma_master_port (must reach master's RDMA child)
#   Master TCP listener is on `port`
#   Master RDMA child listens on `rdma_listen_port`
#
#   So we need: port == rdma_listen_port == rdma_master_port
#   RDMA CM uses RDMA_PS_TCP (separate namespace), so the RDMA child can
#   coexist with the application's TCP listener on the same port number.
# =============================================================================
set -u

PROJECT_DIR="/home/xxf/9.1-kvstore"
KVS="${PROJECT_DIR}/build/kvstore"
RDMA_IP="192.168.30.137"
PORT=2100              # Single shared port for TCP + RDMA
SLAVE_PORT=$((PORT + 1))
TEST_KEYS=1000
LOG_DIR="/tmp/kvs_rdma_test_$$"
mkdir -p "$LOG_DIR"

RED='\033[0;31m'; GRN='\033[0;32m'; CYN='\033[0;36m'; YEL='\033[1;33m'; NC='\033[0m'
pass() { echo -e "${GRN}[PASS]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; }
info() { echo -e "${CYN}[INFO]${NC} $*"; }
warn() { echo -e "${YEL}[WARN]${NC} $*"; }

_cleanup() {
    pkill -9 -x kvstore 2>/dev/null; wait 2>/dev/null
    sleep 0.5
    rm -f "${PROJECT_DIR}/build/dump.rdb" 2>/dev/null
}
_cleanup
trap _cleanup EXIT

# ── Prereqs ──────────────────────────────────────────────────────────────
info "Checking prerequisites..."
[ -x "$KVS" ] || { fail "kvstore not built"; exit 1; }
command -v redis-cli >/dev/null 2>&1 || { fail "redis-cli not found"; exit 1; }
[ -d /sys/class/infiniband/rxe0 ] || { fail "RDMA device rxe0 not found"; exit 1; }
ldd "$KVS" | grep -q librdmacm || { fail "librdmacm not linked"; exit 1; }
pass "All prerequisites met"

# Try to raise memlock
if command -v sudo &>/dev/null && sudo -n true 2>/dev/null; then
    sudo prlimit --pid $$ --memlock=unlimited 2>/dev/null
fi

# ── Configs: all use PORT=2100 ───────────────────────────────────────────
info "Creating configs (port=$PORT, slave_port=$SLAVE_PORT)..."

cat > "${LOG_DIR}/master.conf" <<EOF
port ${PORT}
network proactor
maxclients 100
loglevel info
persistence rdb
rdma_sync on
rdma_role master
rdma_listen_ip ${RDMA_IP}
rdma_listen_port ${PORT}
fullsync_transport rdma
expire off
EOF

cat > "${LOG_DIR}/slave.conf" <<EOF
port ${SLAVE_PORT}
network proactor
maxclients 100
loglevel info
persistence none
rdma_sync on
rdma_role slave
rdma_master_ip ${RDMA_IP}
rdma_master_port ${PORT}
fullsync_transport rdma
expire off
EOF

# ── Start master ─────────────────────────────────────────────────────────
info "Starting master..."
"$KVS" "${LOG_DIR}/master.conf" > "${LOG_DIR}/master.log" 2>&1 &
MASTER_PID=$!

for i in $(seq 1 30); do
    timeout 0.5 bash -c "exec 3<>/dev/tcp/127.0.0.1/${PORT}" 2>/dev/null && break
    sleep 0.5
done

if ! kill -0 "$MASTER_PID" 2>/dev/null; then
    fail "Master died"; tail -20 "${LOG_DIR}/master.log"; exit 1
fi

PONG=$(redis-cli -p "$PORT" PING 2>/dev/null)
[ "$PONG" = "PONG" ] || { fail "Master not responding"; tail -20 "${LOG_DIR}/master.log"; exit 1; }
pass "Master started (pid=$MASTER_PID, port=$PORT)"

# ── Populate data ────────────────────────────────────────────────────────
info "Populating $TEST_KEYS keys..."
python3 -c "
import socket
data = b''
for i in range($TEST_KEYS):
    key = f'key{i:06d}'.encode()
    val = f'val{i:06d}'.encode() + b'_x' * 8
    data += b'*3\r\n\$3\r\nSET\r\n\$%d\r\n%s\r\n\$%d\r\n%s\r\n' % (len(key), key, len(val), val)
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(10); s.connect(('127.0.0.1', $PORT))
s.sendall(data); s.recv(4096); s.close()
" 2>&1

DBSIZE=$(redis-cli -p "$PORT" DBSIZE 2>/dev/null)
[ "$DBSIZE" = "$TEST_KEYS" ] || { fail "Master DBSIZE=$DBSIZE, expected $TEST_KEYS"; exit 1; }
pass "Master DBSIZE = $DBSIZE"

# Verify a few keys
for i in 0 999; do
    V=$(redis-cli -p "$PORT" GET "key$(printf '%06d' $i)" 2>/dev/null)
    [ -n "$V" ] && pass "Master key$(printf '%06d' $i) = $V" || fail "Master key$(printf '%06d' $i) empty"
done

redis-cli -p "$PORT" SAVE >/dev/null 2>&1
sleep 1

# ── Start slave → trigger full sync ──────────────────────────────────────
info "Starting slave (triggers RDMA full sync)..."
info "  FULLSYNC TCP → ${RDMA_IP}:${PORT}"
info "  RDMA connect → ${RDMA_IP}:${PORT}"

SLAVE_START=$(date +%s.%N)
timeout 60 "$KVS" "${LOG_DIR}/slave.conf" > "${LOG_DIR}/slave.log" 2>&1 &
SLAVE_PID=$!

SLAVE_READY=0
for i in $(seq 1 120); do
    if timeout 0.5 bash -c "exec 3<>/dev/tcp/127.0.0.1/${SLAVE_PORT}" 2>/dev/null; then
        SLAVE_READY=1; break
    fi
    kill -0 "$SLAVE_PID" 2>/dev/null || break
    sleep 0.5
done

SLAVE_WALL=$(echo "$(date +%s.%N) - $SLAVE_START" | bc)

if [ "$SLAVE_READY" != "1" ]; then
    wait "$SLAVE_PID" 2>/dev/null
    fail "Slave not ready (wall=${SLAVE_WALL}s, exit=$?)"
    echo ""; echo "=== Master log ==="; cat "${LOG_DIR}/master.log"
    echo ""; echo "=== Slave log ==="; cat "${LOG_DIR}/slave.log"
    exit 1
fi
pass "Slave ready (pid=$SLAVE_PID, port=$SLAVE_PORT, wall=${SLAVE_WALL}s)"

# ── Verify sync ──────────────────────────────────────────────────────────
info "Verifying synced data..."

SLAVE_DBSIZE=$(redis-cli -p "$SLAVE_PORT" DBSIZE 2>/dev/null)
if [ "$SLAVE_DBSIZE" = "$TEST_KEYS" ]; then
    pass "Slave DBSIZE = $SLAVE_DBSIZE (expected $TEST_KEYS)"
else
    fail "Slave DBSIZE = $SLAVE_DBSIZE (expected $TEST_KEYS)"
fi

KEY_FAILS=0
for i in 0 42 500 999; do
    VAL=$(redis-cli -p "$SLAVE_PORT" GET "key$(printf '%06d' $i)" 2>/dev/null)
    EXPECTED="val$(printf '%06d' $i)_x_x_x_x_x_x_x_x"
    if [ "$VAL" = "$EXPECTED" ]; then
        pass "Slave key$(printf '%06d' $i) OK"
    else
        fail "Slave key$(printf '%06d' $i): expected '$EXPECTED', got '$VAL'"
        KEY_FAILS=$((KEY_FAILS + 1))
    fi
done

# ── Logs ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Master log ==="
cat "${LOG_DIR}/master.log"
echo ""
echo "=== Slave log ==="
cat "${LOG_DIR}/slave.log"

# ── Summary ──────────────────────────────────────────────────────────────
echo ""
echo "============================================================================"
if [ "$SLAVE_DBSIZE" = "$TEST_KEYS" ] && [ "$KEY_FAILS" = "0" ]; then
    echo -e "  ${GRN}RDMA Full Sync Test: PASSED${NC}"
    echo "  Keys: $TEST_KEYS → Slave: $SLAVE_DBSIZE (verified)"
    echo "  Total wall: ${SLAVE_WALL}s"
    RESULT=0
else
    echo -e "  ${RED}RDMA Full Sync Test: FAILED${NC}"
    echo "  Master: $TEST_KEYS keys → Slave: $SLAVE_DBSIZE keys"
    echo "  Key mismatches: $KEY_FAILS"
    RESULT=1
fi
echo "  Logs: ${LOG_DIR}/"
echo "============================================================================"

exit $RESULT
