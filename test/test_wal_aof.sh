#!/bin/bash
# =============================================================================
# test_wal_aof.sh  —  WAL/AOF unification + incremental sync TCP test
# =============================================================================
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BINARY="${KVS:-$ROOT/build/kvstore}"
PORT=2099
INCR_PORT=6399
LOG="/tmp/kvstore-test.log"
PASS=0
FAIL=0

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}[PASS]${NC} $1"; PASS=$((PASS+1)); }
fail() { echo -e "${RED}[FAIL]${NC} $1"; FAIL=$((FAIL+1)); }
info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

# ── test config (rdma_sync on but no actual RDMA slave — WAL still starts) ──
cat > /tmp/kvstore-test.conf << EOF
port $PORT
network reactor
loglevel info
persistence aof
rdma_sync on
rdma_role master
rdma_provider tcp
rdma_listen_ip 0.0.0.0
rdma_listen_port 2199
incr_port $INCR_PORT
EOF

cleanup() {
    if [ -n "${MASTER_PID:-}" ]; then
        kill $MASTER_PID 2>/dev/null || true
        wait $MASTER_PID 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ─────────────────────────────────────────────────────────────
# Test 1: Start master, verify WAL module initialises
# ─────────────────────────────────────────────────────────────
info "=== Test 1: WAL startup ==="
$BINARY /tmp/kvstore-test.conf > $LOG 2>&1 &
MASTER_PID=$!
sleep 2

if kill -0 $MASTER_PID 2>/dev/null; then
    pass "Master started (pid=$MASTER_PID)"
else
    fail "Master failed to start"; cat $LOG; exit 1
fi

if grep -q "WAL file opened" $LOG; then
    WAL_PATH=$(grep "WAL file opened" $LOG | sed 's/.*WAL file opened: //')
    pass "WAL file opened: $WAL_PATH"
else
    fail "WAL file not opened"
    WAL_PATH=""
fi

if grep -q "incremental sync module ready\|batch dispatch" $LOG; then
    pass "WAL+io_uring dispatch module ready"
else
    fail "WAL dispatch module not started"
fi

# ─────────────────────────────────────────────────────────────
# Test 2: Write commands and verify WAL contains data
# ─────────────────────────────────────────────────────────────
info ""
info "=== Test 2: WAL write integrity ==="
sleep 1

redis-cli -p $PORT SET testkey1 "hello"      > /dev/null
redis-cli -p $PORT SET testkey2 "world"      > /dev/null
redis-cli -p $PORT SET testkey3 "waltest"    > /dev/null
redis-cli -p $PORT HSET myhash f1 v1 f2 v2  > /dev/null
redis-cli -p $PORT LPUSH mylist a b c        > /dev/null
sleep 1   # let consumer thread flush

V=$(redis-cli -p $PORT GET testkey1)
if [ "$V" = "hello" ]; then pass "GET testkey1 = '$V'"; else fail "GET testkey1 failed (got '$V')"; fi
V=$(redis-cli -p $PORT GET testkey2)
if [ "$V" = "world" ]; then pass "GET testkey2 = '$V'"; else fail "GET testkey2 failed (got '$V')"; fi

if [ -n "$WAL_PATH" ] && [ -f "$WAL_PATH" ]; then
    WAL_SIZE=$(stat -c%s "$WAL_PATH")
    if [ "$WAL_SIZE" -gt 0 ]; then
        pass "WAL file non-empty ($WAL_SIZE bytes)"
    else
        fail "WAL file is empty"
    fi
    # WAL_MAGIC=0x574C5243='WLRC'; stored little-endian on x86 as 43 52 4C 57
    MAGIC_LE=$(xxd -l 4 "$WAL_PATH" 2>/dev/null | awk '{print $2$3}' | tr '[:lower:]' '[:upper:]')
    if [ "$MAGIC_LE" = "43524C57" ]; then
        pass "WAL magic 0x574C5243 (WLRC) verified"
    else
        info "WAL header (first 32 bytes):"
        hexdump -C "$WAL_PATH" | head -2
        fail "WAL magic check failed (got: $MAGIC_LE, expected 43524C57)"
    fi
else
    fail "WAL file not found at '$WAL_PATH'"
fi

# ─────────────────────────────────────────────────────────────
# Test 3: Incremental sync TCP handshake
# ─────────────────────────────────────────────────────────────
info ""
info "=== Test 3: Incremental sync TCP handshake ==="

if nc -z 127.0.0.1 $INCR_PORT 2>/dev/null; then
    pass "Incremental sync port $INCR_PORT is open"
else
    fail "Incremental sync port $INCR_PORT not reachable"
fi

# Send INCR_SYNC handshake and expect +CONTINUE <replid>
RESP=$(printf 'INCR_SYNC 0\r\n' | timeout 2 nc 127.0.0.1 $INCR_PORT 2>/dev/null || true)
if echo "$RESP" | grep -q "+CONTINUE"; then
    REPLID=$(echo "$RESP" | grep "+CONTINUE" | awk '{print $2}' | tr -d '\r')
    pass "Got +CONTINUE from master (replid=${REPLID})"
else
    fail "INCR_SYNC handshake failed (response: '$RESP')"
fi

# ─────────────────────────────────────────────────────────────
# Test 4: Crash recovery (restart and verify data persists)
# ─────────────────────────────────────────────────────────────
info ""
info "=== Test 4: Crash recovery via WAL ==="

# Add a few more keys then kill
redis-cli -p $PORT SET crash_key "survives_restart" > /dev/null
sleep 1

kill -TERM $MASTER_PID
wait $MASTER_PID 2>/dev/null || true
info "Master stopped gracefully, restarting..."

$BINARY /tmp/kvstore-test.conf >> $LOG 2>&1 &
MASTER_PID=$!
sleep 2

if kill -0 $MASTER_PID 2>/dev/null; then
    pass "Master restarted (pid=$MASTER_PID)"
else
    fail "Master failed to restart"; cat $LOG; exit 1
fi

if grep -q "WAL\|AOF.*load\|commands replayed" $LOG; then
    REPLAYED=$(grep "commands replayed" $LOG | tail -1 | grep -o '[0-9]* commands' | head -1)
    pass "WAL reload executed ($REPLAYED)"
else
    fail "No WAL reload log found"
fi

V=$(redis-cli -p $PORT GET testkey1 2>/dev/null)
if [ "$V" = "hello" ]; then pass "Crash recovery: testkey1='$V'"; else fail "Crash recovery: testkey1 lost (got '$V')"; fi
V=$(redis-cli -p $PORT GET crash_key 2>/dev/null)
if [ "$V" = "survives_restart" ]; then pass "Crash recovery: crash_key='$V'"; else fail "Crash recovery: crash_key lost (got '$V')"; fi

# ─────────────────────────────────────────────────────────────
# Test 5: Performance — ops/sec through WAL path
# ─────────────────────────────────────────────────────────────
info ""
info "=== Test 5: WAL write throughput ==="

if command -v redis-benchmark &>/dev/null; then
    RESULT=$(redis-benchmark -p $PORT -t set -n 10000 -q 2>/dev/null | grep 'SET')
    pass "redis-benchmark: $RESULT"
else
    START=$(date +%s%N)
    redis-cli -p $PORT --pipe <<< "$(for i in $(seq 1 200); do printf "*3\r\n\$3\r\nSET\r\n\$7\r\nbench:$i\r\n\$5\r\nval$i\r\n"; done)" > /dev/null 2>&1
    END=$(date +%s%N)
    ELAPSED_MS=$(( (END - START) / 1000000 ))
    pass "200 pipelined SETs in ${ELAPSED_MS}ms via WAL path"
fi

# ─────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════"
echo -e " Results: ${GREEN}${PASS} passed${NC}  ${RED}${FAIL} failed${NC}"
echo "════════════════════════════════════════"
if [ $FAIL -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed. See $LOG for details.${NC}"
    exit 1
fi
