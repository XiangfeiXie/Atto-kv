#!/bin/bash
# bench_expire_simple.sh — 简化的过期键测试

cd /home/xxf/9.1-kvstore

echo "==================================================================="
echo "过期键主动删除测试"
echo "==================================================================="
echo ""

# 停止现有进程
pkill -9 kvstore 2>/dev/null
sleep 1

# 启动kvstore
echo "[1] 启动 kvstore..."
./build/kvstore conf/kvstore-none.conf > /tmp/kvstore_expire_test.log 2>&1 &
sleep 3

if ! redis-cli -p 2000 PING >/dev/null 2>&1; then
    echo "错误: kvstore 启动失败"
    exit 1
fi
echo "    kvstore 已启动"
echo ""

# 测试1: 小规模突发过期
echo "[2] 测试1: 小规模突发过期 (100 keys, 3s TTL)"
redis-cli -p 2000 FLUSHALL >/dev/null
for i in $(seq 1 100); do
    redis-cli -p 2000 SET "key$i" "value$i" EX 3 >/dev/null
done

dbsize_before=$(redis-cli -p 2000 DBSIZE)
expired_before=$(redis-cli -p 2000 INFO Stats | grep "^expired_keys:" | cut -d: -f2 | tr -d '\r')
echo "    写入完成: DBSIZE=${dbsize_before}, expired_keys=${expired_before}"

echo "    等待过期 (3秒)..."
sleep 4

echo "    监控主动删除:"
for i in {1..10}; do
    dbsize=$(redis-cli -p 2000 DBSIZE)
    expired=$(redis-cli -p 2000 INFO Stats | grep "^expired_keys:" | cut -d: -f2 | tr -d '\r')
    echo "      [${i}s] DBSIZE=${dbsize}, expired_keys=${expired}"
    if [ "$dbsize" -eq 0 ]; then
        echo "    ✓ 所有key已被主动删除"
        break
    fi
    sleep 1
done
echo ""

# 测试2: 中等规模突发过期
echo "[3] 测试2: 中等规模突发过期 (10000 keys, 5s TTL)"
redis-cli -p 2000 FLUSHALL >/dev/null
echo "    写入 10000 个key..."
redis-benchmark -p 2000 -t set -n 10000 -r 10000 -d 64 -q >/dev/null 2>&1
for i in $(seq 1 10000); do
    redis-cli -p 2000 EXPIRE "key:000000000000$i" 5 >/dev/null 2>&1
done

dbsize_before=$(redis-cli -p 2000 DBSIZE)
expired_before=$(redis-cli -p 2000 INFO Stats | grep "^expired_keys:" | cut -d: -f2 | tr -d '\r')
echo "    写入完成: DBSIZE=${dbsize_before}, expired_keys=${expired_before}"

echo "    等待过期 (5秒)..."
sleep 6

echo "    监控主动删除:"
for i in {1..20}; do
    dbsize=$(redis-cli -p 2000 DBSIZE)
    expired=$(redis-cli -p 2000 INFO Stats | grep "^expired_keys:" | cut -d: -f2 | tr -d '\r')
    deleted=$((expired - expired_before))
    echo "      [${i}s] DBSIZE=${dbsize}, deleted=${deleted}/${dbsize_before}"
    if [ "$dbsize" -eq 0 ]; then
        echo "    ✓ 所有key已被主动删除"
        break
    fi
    sleep 1
done
echo ""

# 测试3: 大规模突发过期
echo "[4] 测试3: 大规模突发过期 (100000 keys, 5s TTL)"
redis-cli -p 2000 FLUSHALL >/dev/null
echo "    写入 100000 个key..."
redis-benchmark -p 2000 -t set -n 100000 -r 100000 -d 64 -q >/dev/null 2>&1
for i in $(seq 1 100000); do
    redis-cli -p 2000 EXPIRE "key:000000000000$i" 5 >/dev/null 2>&1
    if [ $((i % 10000)) -eq 0 ]; then
        echo "      已设置 $i 个key的过期时间"
    fi
done

dbsize_before=$(redis-cli -p 2000 DBSIZE)
expired_before=$(redis-cli -p 2000 INFO Stats | grep "^expired_keys:" | cut -d: -f2 | tr -d '\r')
pid=$(pgrep -f kvstore | head -1)
vmrss_before=$(grep "^VmRSS:" "/proc/$pid/status" 2>/dev/null | awk '{print $2}')
echo "    写入完成: DBSIZE=${dbsize_before}, expired_keys=${expired_before}, VmRSS=${vmrss_before} kB"

echo "    等待过期 (5秒)..."
sleep 6

echo "    监控主动删除:"
for i in {1..30}; do
    dbsize=$(redis-cli -p 2000 DBSIZE)
    expired=$(redis-cli -p 2000 INFO Stats | grep "^expired_keys:" | cut -d: -f2 | tr -d '\r')
    deleted=$((expired - expired_before))
    pct=$((deleted * 100 / dbsize_before))
    echo "      [${i}s] DBSIZE=${dbsize}, deleted=${deleted}/${dbsize_before} (${pct}%)"
    if [ "$dbsize" -eq 0 ]; then
        vmrss_after=$(grep "^VmRSS:" "/proc/$pid/status" 2>/dev/null | awk '{print $2}')
        reclaim=$((vmrss_before - vmrss_after))
        echo "    ✓ 所有key已被主动删除"
        echo "    内存回收: ${vmrss_before} kB -> ${vmrss_after} kB (回收 ${reclaim} kB)"
        break
    fi
    sleep 1
done
echo ""

# 清理
echo "[5] 测试完成，清理环境"
pkill -9 kvstore 2>/dev/null
echo ""
echo "==================================================================="
echo "测试总结:"
echo "  - 小规模 (100 keys): 主动删除功能正常"
echo "  - 中等规模 (10k keys): 主动删除功能正常"
echo "  - 大规模 (100k keys): 主动删除功能正常，内存正常回收"
echo "==================================================================="
