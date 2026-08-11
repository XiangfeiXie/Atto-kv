#!/bin/bash
# =============================================================================
# test_rdma_fullsync.sh — RDMA full sync test (separated ports)
#
# Architecture:
#   Slave FULLSYNC TCP → master_port      (2000  master's service port)
#   Slave RDMA connect → rdma_master_port (2100  dedicated RDMA data port)
#   Master TCP service  → port            (2000  handles FULLSYNC + commands)
#   Master RDMA listen  → rdma_listen_port(2100  dedicated RDMA CM listener)
#
#   Port separation means RDMA data never interferes with client traffic.
# =============================================================================
set -u

PROJECT_DIR="/home/xxf/9.1-kvstore"
KVS="${PROJECT_DIR}/build/kvstore"
POPULATE_PY="${PROJECT_DIR}/test/bench/populate_kv.py"
RDMA_IP="192.168.30.137"

# ── Defaults ──────────────────────────────────────────────────────────────
PORT=2000              # Master TCP service port
RDMAPORT=2100          # RDMA CM / data port
SLAVE_PORT=2001
KEYS=20000
VSIZE=256
SMOKE=0
LOG_DIR=""

RED='\033[0;31m'; GRN='\033[0;32m'; CYN='\033[0;36m'; YEL='\033[1;33m'; NC='\033[0m'
pass() { echo -e "${GRN}[PASS]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; }
info() { echo -e "${CYN}[INFO]${NC} $*"; }
warn() { echo -e "${YEL}[WARN]${NC} $*"; }
die()  { echo -e "${RED}[DIE]${NC} $*"; exit 1; }

usage() {
    cat <<EOF
Usage: $0 [options]

Options:
  --smoke              Small smoke test (1000 keys, 256B values)
  --keys N             Number of keys (default: 1000)
  --vsize N            Value size in bytes (default: 256)
  --port N             Master service port (default: 2000)
  --rdma-port N        RDMA data port (default: 2100)
  --rdma-ip IP         RDMA interface IP (default: $RDMA_IP)
  -h, --help           This help

Examples:
  $0 --smoke                     # Quick 1K-key smoke test
  $0 --keys 400000 --vsize 1024  # Large-scale test
EOF
    exit 0
}

# ── Arg parsing ───────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --smoke)       SMOKE=1;      shift ;;
        --keys)        KEYS="$2";    shift 2 ;;
        --vsize)       VSIZE="$2";   shift 2 ;;
        --port)        PORT="$2";    shift 2 ;;
        --rdma-port)   RDMAPORT="$2"; shift 2 ;;
        --rdma-ip)     RDMA_IP="$2"; shift 2 ;;
        -h|--help)     usage ;;
        *) die "Unknown option: $1 (use -h)" ;;
    esac
done

SLAVE_PORT=$((PORT + 1))

# ── Setup ─────────────────────────────────────────────────────────────────
LOG_DIR="/tmp/kvs_rdma_test_$$"
mkdir -p "$LOG_DIR"
trap 'pkill -9 -x kvstore 2>/dev/null; pkill -9 -f "kvstore" 2>/dev/null; wait 2>/dev/null; rm -f "${PROJECT_DIR}/build/dump.rdb" 2>/dev/null' EXIT

_cleanup() {
    pkill -9 -x kvstore 2>/dev/null; pkill -9 -f "kvstore" 2>/dev/null; wait 2>/dev/null
    sleep 0.5
    rm -f "${PROJECT_DIR}/build/dump.rdb" 2>/dev/null
}
_cleanup

info "Test parameters:"
info "  keys=$KEYS  vsize=$VSIZE  port=$PORT  rdma_port=$RDMAPORT  slave_port=$SLAVE_PORT"
info "  rdma_ip=$RDMA_IP  log_dir=$LOG_DIR"

# ── Prereqs ───────────────────────────────────────────────────────────────
info "Checking prerequisites..."
[ -x "$KVS" ]            || die "kvstore not built: $KVS"
[ -f "$POPULATE_PY" ]    || die "populate_kv.py not found: $POPULATE_PY"
command -v redis-cli >/dev/null 2>&1 || die "redis-cli not found"
command -v bc >/dev/null 2>&1        || die "bc not found"
[ -d /sys/class/infiniband/rxe0 ]    || die "RDMA device rxe0 not found"
ldd "$KVS" | grep -q librdmacm       || die "librdmacm not linked"
pass "All prerequisites met"

# ── memlock ───────────────────────────────────────────────────────────────
MEMLOCK=$(ulimit -l 2>/dev/null || echo 0)
NEED_KB=2097152  # 2 GB for 8×256MB ring buffers
if [ "$MEMLOCK" = "unlimited" ]; then
    info "memlock: unlimited ✓"
elif [ "$MEMLOCK" -ge "$NEED_KB" ] 2>/dev/null; then
    info "memlock: ${MEMLOCK} KB (≥2 GB) ✓"
else
    warn "memlock=${MEMLOCK} KB — need ≥2 GB for RDMA buffer registration"
    warn "Attempting to raise via sudo..."
    if echo "1234" | sudo -S prlimit --pid $$ --memlock=unlimited 2>/dev/null; then
        info "memlock raised to unlimited ✓"
    elif sudo -n true 2>/dev/null; then
        sudo prlimit --pid $$ --memlock=unlimited 2>/dev/null && \
            info "memlock raised to unlimited ✓" || \
            warn "sudo prlimit failed — RDMA slave may fail"
    else
        warn "Cannot raise memlock — RDMA slave may fail"
        if [ "$SMOKE" != "1" ]; then
            die "memlock too low for large RDMA test"
        fi
    fi
fi

# ── Config generation ─────────────────────────────────────────────────────
info "Writing configs..."

cat > "${LOG_DIR}/master.conf" <<EOF
port ${PORT}
network proactor
maxclients 1000
loglevel info
persistence rdb
rdma_sync on
rdma_role master
rdma_listen_ip ${RDMA_IP}
rdma_listen_port ${RDMAPORT}
fullsync_transport rdma
expire off
EOF

cat > "${LOG_DIR}/slave.conf" <<EOF
port ${SLAVE_PORT}
network proactor
maxclients 1000
loglevel info
persistence none
rdma_sync on
rdma_role slave
master_port ${PORT}
rdma_master_ip ${RDMA_IP}
rdma_master_port ${RDMAPORT}
fullsync_transport rdma
expire off
EOF

pass "Configs written:"
info "  Master: port=${PORT}  rdma_listen=${RDMA_IP}:${RDMAPORT}"
info "  Slave:  port=${SLAVE_PORT}  master_port=${PORT}  rdma_master=${RDMA_IP}:${RDMAPORT}"

# ═══════════════════════════════════════════════════════════════════════════
# Phase 1: Start populate instance (reactor — stable for pipelined writes)
# ═══════════════════════════════════════════════════════════════════════════
info "Phase 1: Starting populate instance (reactor)..."

cat > "${LOG_DIR}/populate.conf" <<EOF
port ${PORT}
network reactor
maxclients 1000
persistence rdb
rdma_sync off
expire off
EOF

"$KVS" "${LOG_DIR}/populate.conf" > "${LOG_DIR}/populate.log" 2>&1 &
POP_PID=$!

for i in $(seq 1 30); do
    timeout 0.5 bash -c "exec 3<>/dev/tcp/127.0.0.1/${PORT}" 2>/dev/null && break
    sleep 0.5
done

if ! kill -0 "$POP_PID" 2>/dev/null; then
    fail "Populate instance died"; tail -20 "${LOG_DIR}/populate.log"; exit 1
fi
pass "Populate instance ready (pid=$POP_PID, port=$PORT)"
sleep 0.3  # Let reactor fully stabilize

# ═══════════════════════════════════════════════════════════════════════════
# Phase 2: Populate data
# ═══════════════════════════════════════════════════════════════════════════
info "Phase 2: Populating $KEYS keys × ${VSIZE}B..."

# For large datasets, use parallel workers; for small, single worker is fine
if [ "$KEYS" -le 2000 ]; then
    WORKERS=1
else
    WORKERS=4
fi

POP_T0=$(date +%s.%N)
chunk=$((KEYS / WORKERS))
declare -a worker_pids=()
for ((w=0; w<WORKERS; w++)); do
    w_start=$((w * chunk))
    w_end=$((w_start + chunk - 1))
    [ $w -eq $((WORKERS - 1)) ] && w_end=$((KEYS - 1))
    python3 "$POPULATE_PY" 127.0.0.1 "$PORT" "$w_start" "$w_end" "$VSIZE" \
        > "${LOG_DIR}/pop_worker_${w}.out" \
        2> "${LOG_DIR}/pop_worker_${w}.err" &
    worker_pids+=($!)
done

pop_failed=0
for ((w=0; w<WORKERS; w++)); do
    wait "${worker_pids[w]}" 2>/dev/null || pop_failed=$((pop_failed + 1))
done

POP_T1=$(date +%s.%N)
POP_SEC=$(echo "$POP_T1 - $POP_T0" | bc)

DBSIZE=$(redis-cli -p "$PORT" DBSIZE 2>/dev/null)
if [ "$DBSIZE" = "$KEYS" ]; then
    pop_rate=$(awk "BEGIN { printf \"%.0f\", ${KEYS}/${POP_SEC} }" 2>/dev/null || echo "?")
    pass "DBSIZE = $DBSIZE (${POP_SEC}s, ${pop_rate} keys/s)"
else
    warn "DBSIZE = $DBSIZE (expected $KEYS) — retrying..."
    # Retry once more with fresh worker
    info "  Retry populate..."
    python3 "$POPULATE_PY" 127.0.0.1 "$PORT" 0 $((KEYS - 1)) "$VSIZE" \
        > "${LOG_DIR}/pop_retry.out" 2> "${LOG_DIR}/pop_retry.err"
    DBSIZE=$(redis-cli -p "$PORT" DBSIZE 2>/dev/null)
fi

# Verify
if [ "$DBSIZE" != "$KEYS" ]; then
    fail "DBSIZE = $DBSIZE (expected $KEYS) after retry"
    kill $POP_PID 2>/dev/null; wait 2>/dev/null
    exit 1
fi
pass "DBSIZE = $DBSIZE ✓"

# Verify sample keys (use strlen to handle binary values with null bytes)
for i in 0 $((KEYS - 1)); do
    K="$(printf 'key:%012d' $i)"
    VLEN=$(redis-cli -p "$PORT" STRLEN "$K" 2>/dev/null)
    [ "$VLEN" = "$VSIZE" ] && pass "Master $K (${VSIZE}B) OK" || fail "Master $K: len=$VLEN expected $VSIZE"
done

# SAVE + record RDB, then kill populate instance
redis-cli -p "$PORT" SAVE >/dev/null 2>&1
for ((i=0; i<60; i++)); do
    [ -f "${PROJECT_DIR}/build/dump.rdb" ] && [ -s "${PROJECT_DIR}/build/dump.rdb" ] && break
    sleep 0.5
done
RDB_SIZE=$(stat -c %s "${PROJECT_DIR}/build/dump.rdb" 2>/dev/null || echo 0)
RDB_MB=$(awk "BEGIN { printf \"%.2f\", ${RDB_SIZE}/1048576 }")
info "RDB: ${RDB_SIZE} bytes (${RDB_MB} MB)"

# Copy RDB so master can use it
cp "${PROJECT_DIR}/build/dump.rdb" "${LOG_DIR}/pop_dump.rdb"

# Kill populate instance
kill $POP_PID 2>/dev/null; wait 2>/dev/null
sleep 0.5
pass "Populate phase complete"

# ═══════════════════════════════════════════════════════════════════════════
# Phase 3: Start master (proactor, with populated RDB)
# ═══════════════════════════════════════════════════════════════════════════
info "Phase 3: Starting master (proactor)..."

# Restore RDB
cp "${LOG_DIR}/pop_dump.rdb" "${PROJECT_DIR}/build/dump.rdb"

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

MKEYS=$(redis-cli -p "$PORT" DBSIZE 2>/dev/null)
[ "$MKEYS" = "$KEYS" ] || { fail "Master DBSIZE=$MKEYS, expected $KEYS"; exit 1; }
pass "Master started (pid=$MASTER_PID, port=$PORT, keys=$MKEYS)"

# ═══════════════════════════════════════════════════════════════════════════
# Phase 4: Start slave → trigger RDMA full sync
# ═══════════════════════════════════════════════════════════════════════════
info "Phase 4: Starting slave (triggers RDMA full sync)..."
info "  FULLSYNC TCP → ${RDMA_IP}:${PORT}  (master_port)"
info "  RDMA connect → ${RDMA_IP}:${RDMAPORT}  (rdma_master_port)"

SYNC_T0=$(date +%s.%N)

timeout 120 "$KVS" "${LOG_DIR}/slave.conf" > "${LOG_DIR}/slave.log" 2>&1 &
SLAVE_PID=$!

SLAVE_READY=0
for i in $(seq 1 240); do
    if timeout 0.5 bash -c "exec 3<>/dev/tcp/127.0.0.1/${SLAVE_PORT}" 2>/dev/null; then
        SLAVE_READY=1; break
    fi
    kill -0 "$SLAVE_PID" 2>/dev/null || break
    sleep 0.5
done

SYNC_T1=$(date +%s.%N)
SYNC_WALL=$(echo "$SYNC_T1 - $SYNC_T0" | bc)

if [ "$SLAVE_READY" != "1" ]; then
    wait "$SLAVE_PID" 2>/dev/null; EC=$?
    fail "Slave not ready (wall=${SYNC_WALL}s, exit=$EC)"
    echo ""; echo "=== Master log ==="; cat "${LOG_DIR}/master.log"
    echo ""; echo "=== Slave log ==="; cat "${LOG_DIR}/slave.log"
    exit 1
fi
pass "Slave ready (pid=$SLAVE_PID, port=$SLAVE_PORT, wall=${SYNC_WALL}s)"

# ═══════════════════════════════════════════════════════════════════════════
# Phase 5: Verify sync
# ═══════════════════════════════════════════════════════════════════════════
info "Phase 5: Verifying synced data..."

SLAVE_DBSIZE=$(redis-cli -p "$SLAVE_PORT" DBSIZE 2>/dev/null)
if [ "$SLAVE_DBSIZE" = "$KEYS" ]; then
    pass "Slave DBSIZE = $SLAVE_DBSIZE (expected $KEYS)"
else
    fail "Slave DBSIZE = $SLAVE_DBSIZE (expected $KEYS)"
fi

# Verify keys at both ends and middle
KEY_FAILS=0
SAMPLE_KEYS=(0 $((KEYS / 2)) $((KEYS - 1)))
for i in "${SAMPLE_KEYS[@]}"; do
    K="$(printf 'key:%012d' $i)"
    M_VLEN=$(redis-cli -p "$PORT" STRLEN "$K" 2>/dev/null || echo 0)
    S_VLEN=$(redis-cli -p "$SLAVE_PORT" STRLEN "$K" 2>/dev/null || echo 0)
    if [ "$M_VLEN" = "$S_VLEN" ] && [ "$S_VLEN" = "$VSIZE" ]; then
        pass "Key $i: master=slave (${VSIZE}B) OK"
    else
        fail "Key $i: master_len=$M_VLEN slave_len=$S_VLEN (expected $VSIZE)"
        KEY_FAILS=$((KEY_FAILS + 1))
    fi
done

# ── Parse transfer metrics from logs ──────────────────────────────────────
XFER_LINE=$(grep '\[master\] xfer-end:' "${LOG_DIR}/master.log" 2>/dev/null | tail -1)
if [ -n "$XFER_LINE" ]; then
    XFER_BYTES=$(echo "$XFER_LINE" | grep -oP 'bytes=\K[0-9]+' || echo "0")
    XFER_MS=$(echo "$XFER_LINE"   | grep -oP 'xfer=\K[0-9.]+'  || echo "0")
    PREP_MS=$(echo "$XFER_LINE"   | grep -oP 'prep=\K[0-9.]+'  || echo "0")
    BATCHES=$(echo "$XFER_LINE"   | grep -oP 'batches=\K[0-9]+' || echo "0")
    XFER_MB=$(awk "BEGIN { printf \"%.2f\", ${XFER_BYTES}/1048576 }")
    XFER_MBPS=$(awk "BEGIN { printf \"%.1f\", ${XFER_BYTES}/1048576/(${XFER_MS}/1000) }")
    info "Transfer: ${XFER_MB} MB in ${XFER_MS}ms (${XFER_MBPS} MB/s), prep=${PREP_MS}ms, batches=${BATCHES}"
fi

# ── Print logs ────────────────────────────────────────────────────────────
echo ""
echo "=== Master log ==="
cat "${LOG_DIR}/master.log"
echo ""
echo "=== Slave log ==="
cat "${LOG_DIR}/slave.log"

# ── Summary ───────────────────────────────────────────────────────────────
echo ""
echo "============================================================================"
if [ "$SLAVE_DBSIZE" = "$KEYS" ] && [ "$KEY_FAILS" = "0" ]; then
    echo -e "  ${GRN}RDMA Full Sync Test: PASSED${NC}"
    echo "  Keys:    $KEYS → Slave: $SLAVE_DBSIZE (verified)"
    echo "  RDB:     ${RDB_MB} MB ($RDB_SIZE bytes)"
    echo "  Populate: ${POP_SEC}s"
    echo "  Sync:    ${SYNC_WALL}s"
    [ -n "$XFER_MBPS" ] && echo "  Xfer:    ${XFER_MB} MB in ${XFER_MS}ms (${XFER_MBPS} MB/s)"
    echo ""
    echo "  Port map:"
    echo "    Master command  → 0.0.0.0:${PORT}"
    echo "    Master RDMA CM  → ${RDMA_IP}:${RDMAPORT}"
    echo "    Slave FULLSYNC  → ${RDMA_IP}:${PORT}"
    echo "    Slave RDMA data → ${RDMA_IP}:${RDMAPORT}"
    RESULT=0
else
    echo -e "  ${RED}RDMA Full Sync Test: FAILED${NC}"
    echo "  Master: $KEYS keys → Slave: $SLAVE_DBSIZE keys"
    echo "  Key mismatches: $KEY_FAILS"
    RESULT=1
fi
echo "  Logs: ${LOG_DIR}/"
echo "============================================================================"

# Cleanup running processes
kill $MASTER_PID $SLAVE_PID 2>/dev/null
wait 2>/dev/null

exit $RESULT
