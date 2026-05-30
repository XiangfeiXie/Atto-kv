#!/bin/bash
# 快速测试删除操作是否正确

set -e

echo "=== 测试删除操作修复 ==="

# 启动 kvstore
echo "1. 启动 kvstore..."
killall -9 kvstore 2>/dev/null || true
sleep 1

cat > /tmp/test_delete.conf << EOF
port 2000
network proactor
maxclients 10000
loglevel error
persistence none
threads 1
rdma_sync off
EOF

nohup /home/xxf/9.1-kvstore/build/kvstore /tmp/test_delete.conf > /tmp/test_delete.log 2>&1 &
sleep 2

# 等待端口就绪
for i in {1..10}; do
    if ss -tlnp 2>/dev/null | grep -q ":2000 "; then
        echo "   kvstore 已启动"
        break
    fi
    sleep 1
done

# 写入测试数据
echo "2. 写入 1000 × 64B (key:000000000001 ~ key:000000001000)"
for i in $(seq 1 1000); do
    printf '*3\r\n$3\r\nSET\r\n$16\r\nkey:%012d\r\n$2\r\n64\r\n' $i
done | redis-cli -p 2000 --pipe >/dev/null 2>&1

echo "3. 写入 1000 × 4KB (key:000000002001 ~ key:000000003000)"
for i in $(seq 2001 3000); do
    printf '*3\r\n$3\r\nSET\r\n$16\r\nkey:%012d\r\n$4\r\n4096\r\n' $i
done | redis-cli -p 2000 --pipe >/dev/null 2>&1

COUNT_BEFORE=$(redis-cli -p 2000 DBSIZE 2>/dev/null)
echo "   写入完成，总 key 数: ${COUNT_BEFORE}"

# 删除 4KB 对象
echo "4. 删除 4KB 对象 (key:000000002001 ~ key:000000003000)"
for i in $(seq 2001 3000); do
    printf '*2\r\n$3\r\nDEL\r\n$16\r\nkey:%012d\r\n' $i
done | redis-cli -p 2000 --pipe >/dev/null 2>&1

COUNT_AFTER=$(redis-cli -p 2000 DBSIZE 2>/dev/null)
DELETED=$((COUNT_BEFORE - COUNT_AFTER))
echo "   删除完成，剩余 key 数: ${COUNT_AFTER}"
echo "   删除了 ${DELETED} 个 key"

# 验证结果
echo ""
echo "=== 验证结果 ==="
if [ "$DELETED" -eq 1000 ]; then
    echo "✓ 删除操作成功！删除了 1000 个 key"
    echo "✓ 剩余 ${COUNT_AFTER} 个 key（应该是 1000）"
else
    echo "✗ 删除操作失败！"
    echo "  预期删除: 1000"
    echo "  实际删除: ${DELETED}"
fi

# 随机检查几个 key
echo ""
echo "=== 随机检查 ==="
echo -n "检查 key:000000000500 (应该存在): "
redis-cli -p 2000 EXISTS key:000000000500 2>/dev/null

echo -n "检查 key:000000002500 (应该不存在): "
redis-cli -p 2000 EXISTS key:000000002500 2>/dev/null

# 清理
killall -9 kvstore 2>/dev/null || true
rm -f /tmp/test_delete.conf

echo ""
echo "=== 测试完成 ==="
