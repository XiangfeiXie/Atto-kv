#!/bin/bash
# =============================================================================
# fill_data.sh - 主机数据填充脚本
#
# 用法: ./fill_data.sh <data_size_mb> [master_ip] [master_port]
#
# 功能:
#   - 使用 redis-benchmark 填充指定大小的数据到主机
#   - 计算并返回实际填充的数据量（字节）
#   - 用于 RDMA 全量同步性能测试
# =============================================================================

set -euo pipefail

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 默认参数
DATA_SIZE_MB=${1:-100}
MASTER_IP=${2:-127.0.0.1}
MASTER_PORT=${3:-2000}
VALUE_SIZE=1024  # 每个value的大小（字节）

# 检查必要命令
require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo -e "${RED}[ERROR]${NC} $1 not installed" >&2
        exit 1
    fi
}

require_cmd redis-cli
require_cmd redis-benchmark
require_cmd bc

# 检查主机连通性
echo -e "${YELLOW}[INFO]${NC} Checking master connectivity: $MASTER_IP:$MASTER_PORT"
if ! redis-cli -h "$MASTER_IP" -p "$MASTER_PORT" PING >/dev/null 2>&1; then
    echo -e "${RED}[ERROR]${NC} Master not reachable at $MASTER_IP:$MASTER_PORT" >&2
    exit 1
fi
echo -e "${GREEN}[OK]${NC} Master is reachable"

# 计算需要填充的key数量
# 数据量 = key数量 * (key_size + value_size + overhead)
# 简化估算: 每个key约占用 (value_size + 100) 字节
BYTES_PER_KEY=$((VALUE_SIZE + 100))
TARGET_BYTES=$((DATA_SIZE_MB * 1024 * 1024))
NKEYS=$((TARGET_BYTES / BYTES_PER_KEY))

echo -e "${YELLOW}[INFO]${NC} Target data size: ${DATA_SIZE_MB} MB"
echo -e "${YELLOW}[INFO]${NC} Estimated keys: $NKEYS (value_size=${VALUE_SIZE} bytes)"

# 清空现有数据
echo -e "${YELLOW}[INFO]${NC} Flushing existing data..."
redis-cli -h "$MASTER_IP" -p "$MASTER_PORT" FLUSHALL >/dev/null 2>&1 || true
sleep 0.5

# 填充数据
echo -e "${YELLOW}[INFO]${NC} Filling data with redis-benchmark..."
redis-benchmark -h "$MASTER_IP" -p "$MASTER_PORT" \
    -n "$NKEYS" \
    -r "$NKEYS" \
    -t set \
    -d "$VALUE_SIZE" \
    -c 50 \
    -P 100 \
    -q

# 添加标记key
redis-cli -h "$MASTER_IP" -p "$MASTER_PORT" SET __bench_marker__ "filled_$(date +%s)" >/dev/null 2>&1 || true

# 等待数据稳定
sleep 1

# 获取实际内存使用量（作为数据量的估算）
USED_MEMORY=$(redis-cli -h "$MASTER_IP" -p "$MASTER_PORT" INFO memory 2>/dev/null | grep "used_memory:" | cut -d: -f2 | tr -d '\r' || echo "0")
DBSIZE=$(redis-cli -h "$MASTER_IP" -p "$MASTER_PORT" DBSIZE 2>/dev/null | tr -d '\r' || echo "0")

if [ "$USED_MEMORY" -gt 0 ]; then
    ACTUAL_MB=$(echo "scale=2; $USED_MEMORY / 1048576" | bc -l)
    echo
    echo -e "${GREEN}[SUCCESS]${NC} Data filled successfully"
    echo -e "${GREEN}PAYLOAD_BYTES=$USED_MEMORY${NC}"
    echo -e "${GREEN}PAYLOAD_MB=$ACTUAL_MB${NC}"
    echo -e "${GREEN}KEY_COUNT=$DBSIZE${NC}"
    echo
    echo "Next step: Run on slave machine:"
    echo "  sudo ./test/bench/bench_rdma_provider.sh --payload $USED_MEMORY --master-ip $MASTER_IP"
else
    echo -e "${RED}[ERROR]${NC} Failed to get memory usage" >&2
    exit 1
fi
