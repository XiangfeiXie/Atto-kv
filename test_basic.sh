#!/bin/bash
# Basic functionality test

set -e

echo "Starting kvstore..."
./build/kvstore test_perf_simple.conf > /tmp/kvstore_test.log 2>&1 &
KVS_PID=$!
sleep 2

echo "Testing PING..."
if redis-cli -p 7379 PING | grep -q PONG; then
    echo "✓ PING works"
else
    echo "✗ PING failed"
    kill -9 $KVS_PID
    exit 1
fi

echo "Testing SET..."
if redis-cli -p 7379 SET testkey testvalue | grep -q OK; then
    echo "✓ SET works"
else
    echo "✗ SET failed"
    kill -9 $KVS_PID
    exit 1
fi

echo "Testing GET..."
result=$(redis-cli -p 7379 GET testkey)
if [ "$result" = "testvalue" ]; then
    echo "✓ GET works"
else
    echo "✗ GET failed (got: $result)"
    kill -9 $KVS_PID
    exit 1
fi

echo "Testing multiple SETs..."
for i in {1..10}; do
    redis-cli -p 7379 SET key$i value$i > /dev/null
done
echo "✓ Multiple SETs work"

echo "Testing multiple GETs..."
for i in {1..10}; do
    result=$(redis-cli -p 7379 GET key$i)
    if [ "$result" != "value$i" ]; then
        echo "✗ GET key$i failed (got: $result)"
        kill -9 $KVS_PID
        exit 1
    fi
done
echo "✓ Multiple GETs work"

echo ""
echo "All basic tests passed!"
kill -9 $KVS_PID
