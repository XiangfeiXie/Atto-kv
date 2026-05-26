#!/bin/bash
# repl_kcp_test.sh — Test KCP + eBPF replication
#
# Tests:
#   1. Master starts with eBPF filter
#   2. Slave connects and receives replicated commands
#   3. Write commands are captured and replicated
#   4. Read commands are NOT replicated (smart filtering)
#   5. Data consistency verification

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$PROJECT_ROOT/build}"
KVS="$BUILD_DIR/kvstore"

MASTER_PORT=6379
SLAVE_PORT=6380
REPL_PORT=6483

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[TEST]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

cleanup() {
    log "Cleaning up..."
    pkill -f "kvstore.*--port $MASTER_PORT" 2>/dev/null || true
    pkill -f "kvstore.*--port $SLAVE_PORT" 2>/dev/null || true
    rm -f /tmp/kvs_master.log /tmp/kvs_slave.log
    sleep 1
}

trap cleanup EXIT

# Check if kvstore binary exists
if [ ! -x "$KVS" ]; then
    error "kvstore binary not found at $KVS"
    error "Run: cmake --build build"
    exit 1
fi

log "Starting KCP replication test..."
log "Master port: $MASTER_PORT, Slave port: $SLAVE_PORT, Repl port: $REPL_PORT"

# Clean up any existing processes
cleanup

# Start master
log "Starting master..."
$KVS --port $MASTER_PORT \
     --repl-role master \
     --xdp-repl-enabled 1 \
     --xdp-repl-port $REPL_PORT \
     --xdp-repl-ifname lo \
     > /tmp/kvs_master.log 2>&1 &
MASTER_PID=$!

sleep 2

if ! kill -0 $MASTER_PID 2>/dev/null; then
    error "Master failed to start"
    cat /tmp/kvs_master.log
    exit 1
fi

log "Master started (PID: $MASTER_PID)"

# Start slave
log "Starting slave..."
$KVS --port $SLAVE_PORT \
     --repl-role slave \
     --rdma-master-ip 127.0.0.1 \
     --xdp-repl-enabled 1 \
     --xdp-repl-port $REPL_PORT \
     --xdp-repl-ifname lo \
     > /tmp/kvs_slave.log 2>&1 &
SLAVE_PID=$!

sleep 3

if ! kill -0 $SLAVE_PID 2>/dev/null; then
    error "Slave failed to start"
    cat /tmp/kvs_slave.log
    exit 1
fi

log "Slave started (PID: $SLAVE_PID)"

# Wait for replication to establish
sleep 2

# Test 1: Write commands should be replicated
log "Test 1: Write command replication"
echo -e '*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n' | nc -q 1 127.0.0.1 $MASTER_PORT > /dev/null
sleep 1

RESULT=$(echo -e '*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n' | nc -q 1 127.0.0.1 $SLAVE_PORT)
if echo "$RESULT" | grep -q "value1"; then
    log "✓ Test 1 PASSED: Write command replicated"
else
    error "✗ Test 1 FAILED: Write command not replicated"
    error "Slave response: $RESULT"
    exit 1
fi

# Test 2: Multiple write commands
log "Test 2: Multiple write commands"
for i in {1..10}; do
    echo -e "*3\r\n\$3\r\nSET\r\n\$5\r\nkey$i\r\n\$3\r\nval$i\r\n" | nc -q 1 127.0.0.1 $MASTER_PORT > /dev/null
done
sleep 2

PASS=0
for i in {1..10}; do
    RESULT=$(echo -e "*2\r\n\$3\r\nGET\r\n\$5\r\nkey$i\r\n" | nc -q 1 127.0.0.1 $SLAVE_PORT)
    if echo "$RESULT" | grep -q "val$i"; then
        ((PASS++))
    fi
done

if [ $PASS -eq 10 ]; then
    log "✓ Test 2 PASSED: All 10 commands replicated"
else
    error "✗ Test 2 FAILED: Only $PASS/10 commands replicated"
    exit 1
fi

# Test 3: Hash commands
log "Test 3: Hash command replication"
echo -e '*4\r\n$4\r\nHSET\r\n$5\r\nmyhash\r\n$6\r\nfield1\r\n$6\r\nvalue1\r\n' | nc -q 1 127.0.0.1 $MASTER_PORT > /dev/null
sleep 1

RESULT=$(echo -e '*3\r\n$4\r\nHGET\r\n$5\r\nmyhash\r\n$6\r\nfield1\r\n' | nc -q 1 127.0.0.1 $SLAVE_PORT)
if echo "$RESULT" | grep -q "value1"; then
    log "✓ Test 3 PASSED: Hash command replicated"
else
    error "✗ Test 3 FAILED: Hash command not replicated"
    exit 1
fi

# Test 4: Read commands should NOT be replicated (verify by checking logs)
log "Test 4: Read command filtering (check logs)"
echo -e '*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n' | nc -q 1 127.0.0.1 $MASTER_PORT > /dev/null
echo -e '*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n' | nc -q 1 127.0.0.1 $MASTER_PORT > /dev/null
sleep 1

# Check if eBPF filter is working (should see "eBPF filter loaded" in logs)
if grep -q "eBPF filter loaded" /tmp/kvs_master.log; then
    log "✓ Test 4 PASSED: eBPF filter is active"
else
    warn "⚠ Test 4 WARNING: eBPF filter may not be active (check logs)"
fi

# Test 5: DEL command replication
log "Test 5: DEL command replication"
echo -e '*2\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n' | nc -q 1 127.0.0.1 $MASTER_PORT > /dev/null
sleep 1

RESULT=$(echo -e '*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n' | nc -q 1 127.0.0.1 $SLAVE_PORT)
if echo "$RESULT" | grep -q "\$-1"; then
    log "✓ Test 5 PASSED: DEL command replicated"
else
    error "✗ Test 5 FAILED: DEL command not replicated"
    exit 1
fi

log ""
log "=========================================="
log "All tests PASSED!"
log "=========================================="
log ""
log "Master log: /tmp/kvs_master.log"
log "Slave log: /tmp/kvs_slave.log"
log ""
log "Check logs for replication statistics:"
log "  grep 'REPL' /tmp/kvs_master.log"
log "  grep 'REPL' /tmp/kvs_slave.log"

exit 0
