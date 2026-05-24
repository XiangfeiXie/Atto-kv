#!/bin/bash
# TCP + XDP Replication Test Script

set -e

KVSTORE=./build/kvstore
MASTER_CONF=./test_master.conf
SLAVE_CONF=./test_slave.conf
MASTER_LOG=/tmp/kvstore_master.log
SLAVE_LOG=/tmp/kvstore_slave.log
MASTER_PID=""
SLAVE_PID=""

cleanup() {
    if [ -n "$SLAVE_PID" ]; then
        kill "$SLAVE_PID" 2>/dev/null || true
        wait "$SLAVE_PID" 2>/dev/null || true
    fi
    if [ -n "$MASTER_PID" ]; then
        kill "$MASTER_PID" 2>/dev/null || true
        wait "$MASTER_PID" 2>/dev/null || true
    fi
    pkill -9 -f './build/kvstore ./test_master.conf' 2>/dev/null || true
    pkill -9 -f './build/kvstore ./test_slave.conf' 2>/dev/null || true
    fuser -k 7379/tcp 7380/tcp 2100/tcp 6483/tcp 2>/dev/null || true
}
trap cleanup EXIT

redis_set() {
    if command -v redis-cli >/dev/null 2>&1; then
        redis-cli -p 7379 SET "$1" "$2" >/dev/null
    else
        printf '*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n' \
            "${#1}" "$1" "${#2}" "$2" | nc -w 1 localhost 7379 >/dev/null 2>&1
    fi
}

redis_get() {
    if command -v redis-cli >/dev/null 2>&1; then
        redis-cli -p 7380 GET "$1" 2>/dev/null | tr -d '\r'
    else
        printf '*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n' "${#1}" "$1" | nc -w 1 localhost 7380 2>/dev/null | tail -1 | tr -d '\r'
    fi
}

echo "=========================================="
echo "  TCP + XDP Replication Test"
echo "=========================================="

echo "[0/5] Cleaning up old kvstore processes..."
cleanup
rm -f /tmp/kvstore_*.log
rm -f build/*.aof build/*.rdb build/*.wal

echo "[1/5] Starting Master on port 7379..."
$KVSTORE $MASTER_CONF > $MASTER_LOG 2>&1 &
MASTER_PID=$!
sleep 2

if ! kill -0 $MASTER_PID 2>/dev/null; then
    echo "ERROR: Master failed to start"
    cat $MASTER_LOG
    exit 1
fi
echo "Master started (PID: $MASTER_PID)"

echo "[2/5] Writing test data to Master..."
for i in {1..100}; do
    redis_set "key$i" "value$i"
done
echo "Wrote 100 keys to Master"

echo "[3/5] Starting Slave on port 7380..."
$KVSTORE $SLAVE_CONF > $SLAVE_LOG 2>&1 &
SLAVE_PID=$!
sleep 3

if ! kill -0 $SLAVE_PID 2>/dev/null; then
    echo "ERROR: Slave failed to start"
    cat $SLAVE_LOG
    exit 1
fi
echo "Slave started (PID: $SLAVE_PID)"

echo "[4/5] Waiting for replication handshake..."
sleep 5

echo "[5/5] Verifying replicated data..."
SUCCESS=0
FAILED=0

for i in {1..100}; do
    RESULT=$(redis_get "key$i")
    if [[ "$RESULT" == "value$i" ]]; then
        SUCCESS=$((SUCCESS + 1))
    else
        FAILED=$((FAILED + 1))
    fi
done

echo ""
echo "Stopping processes..."
cleanup

# Avoid double cleanup from trap after explicit stop.
MASTER_PID=""
SLAVE_PID=""

echo ""
echo "=========================================="
echo "  Test Results"
echo "=========================================="
echo "Success: $SUCCESS / 100"
echo "Failed:  $FAILED / 100"
echo ""

if [ $FAILED -eq 0 ]; then
    echo "✓ TCP + XDP Replication Test PASSED"
    exit 0
else
    echo "✗ TCP + XDP Replication Test FAILED"
    exit 1
fi
