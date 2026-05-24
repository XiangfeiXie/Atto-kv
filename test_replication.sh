#!/bin/bash
# TCP + XDP Replication Test Script

set -e

KVSTORE=./build/kvstore
MASTER_CONF=./test_master.conf
SLAVE_CONF=./test_slave.conf
MASTER_LOG=/tmp/kvstore_master.log
SLAVE_LOG=/tmp/kvstore_slave.log

echo "=========================================="
echo "  TCP + XDP Replication Test"
echo "=========================================="

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
    echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey$i\r\n\$6\r\nvalue$i\r" | nc -w 1 localhost 7379 > /dev/null 2>&1
done
echo "Wrote 100 keys to Master"

echo "[3/5] Starting Slave on port 7380..."
$KVSTORE $SLAVE_CONF > $SLAVE_LOG 2>&1 &
SLAVE_PID=$!
sleep 3

if ! kill -0 $SLAVE_PID 2>/dev/null; then
    echo "ERROR: Slave failed to start"
    cat $SLAVE_LOG
    kill $MASTER_PID 2>/dev/null
    exit 1
fi
echo "Slave started (PID: $SLAVE_PID)"

echo "[4/5] Waiting for replication..."
sleep 5

echo "[5/5] Verifying replicated data..."
SUCCESS=0
FAILED=0

for i in {1..100}; do
    RESULT=$(echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey$i\r" | nc -w 1 localhost 7380 2>/dev/null | tail -1)
    if [[ "$RESULT" == "value$i" ]]; then
        ((SUCCESS++))
    else
        ((FAILED++))
    fi
done

echo ""
echo "Stopping processes..."
kill $MASTER_PID $SLAVE_PID 2>/dev/null
wait $MASTER_PID $SLAVE_PID 2>/dev/null || true

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
