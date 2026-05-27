#!/bin/bash
# =============================================================================
# check_replication.sh - 检查主从复制是否正常工作
# =============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

MASTER_IP="${1:-127.0.0.1}"
MASTER_PORT="${2:-2000}"
SLAVE_IP="${3:-127.0.0.1}"
SLAVE_PORT="${4:-2001}"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Replication Health Check${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Master: ${MASTER_IP}:${MASTER_PORT}"
echo -e "Slave : ${SLAVE_IP}:${SLAVE_PORT}"
echo

# 1. 检查 master 连通性
echo -e "${YELLOW}[1/6]${NC} Checking master connectivity..."
if ! redis-cli -h "$MASTER_IP" -p "$MASTER_PORT" PING >/dev/null 2>&1; then
    echo -e "${RED}[FAIL]${NC} Master not reachable"
    exit 1
fi
echo -e "${GREEN}[OK]${NC} Master is reachable"

# 2. 检查 slave 连通性
echo -e "${YELLOW}[2/6]${NC} Checking slave connectivity..."
if ! redis-cli -h "$SLAVE_IP" -p "$SLAVE_PORT" PING >/dev/null 2>&1; then
    echo -e "${RED}[FAIL]${NC} Slave not reachable"
    exit 1
fi
echo -e "${GREEN}[OK]${NC} Slave is reachable"

# 3. 不需要清空数据，使用唯一 key
echo -e "${YELLOW}[3/6]${NC} Preparing test..."
echo -e "${GREEN}[OK]${NC} Using unique test key"

# 4. 写入测试数据到 master
echo -e "${YELLOW}[4/6]${NC} Writing test data to master..."
TEST_KEY="repl_test_$(date +%s)"
TEST_VALUE="test_value_$(date +%s)"
redis-cli -h "$MASTER_IP" -p "$MASTER_PORT" SET "$TEST_KEY" "$TEST_VALUE" >/dev/null 2>&1
echo -e "${GREEN}[OK]${NC} Written key: $TEST_KEY"

# 5. 等待复制
echo -e "${YELLOW}[5/6]${NC} Waiting for replication (3 seconds)..."
sleep 3

# 6. 从 slave 读取数据
echo -e "${YELLOW}[6/6]${NC} Reading from slave..."
SLAVE_VALUE=$(redis-cli -h "$SLAVE_IP" -p "$SLAVE_PORT" GET "$TEST_KEY" 2>&1)

if [ "$SLAVE_VALUE" = "$TEST_VALUE" ]; then
    echo -e "${GREEN}[SUCCESS]${NC} Replication is working!"
    echo -e "  Master value: $TEST_VALUE"
    echo -e "  Slave value : $SLAVE_VALUE"
    echo
    echo -e "${GREEN}✓ Incremental sync is functioning correctly${NC}"
    exit 0
else
    echo -e "${RED}[FAIL]${NC} Replication not working"
    echo -e "  Expected: $TEST_VALUE"
    echo -e "  Got     : $SLAVE_VALUE"
    echo
    echo -e "${RED}✗ Incremental sync is NOT working${NC}"
    echo
    echo -e "${YELLOW}Troubleshooting steps:${NC}"
    echo -e "  1. Check master log: tail -50 /tmp/master.log | grep REPL"
    echo -e "  2. Check slave log: tail -50 /tmp/slave.log | grep REPL"
    echo -e "  3. Verify ports: netstat -tuln | grep -E '6483|2000|2001|2100'"
    echo -e "  4. Check firewall: sudo iptables -L -n | grep -E '6483|2100'"
    echo -e "  5. Verify network: ping $MASTER_IP"
    exit 1
fi
