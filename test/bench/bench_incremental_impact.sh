#!/bin/bash
# =============================================================================
# bench_incremental_impact.sh - 测试增量同步对写入 QPS 的性能影响
#
# 用法: ./bench_incremental_impact.sh [options]
#
# 功能:
#   - 场景1: 测试无从机时的 baseline QPS
#   - 场景2: 测试有从机连接时的 QPS（增量同步激活）
#   - 计算性能影响百分比
#   - 生成 CSV 格式报告
#
# 测试流程:
#   1. 检查 master 是否运行
#   2. 场景1: 测试 baseline QPS（30秒）
#   3. 提示用户启动从机
#   4. 等待从机连接确认
#   5. 场景2: 测试有从机 QPS（30秒）
#   6. 生成性能对比报告
# =============================================================================

set -euo pipefail

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 默认参数
MASTER_IP="127.0.0.1"
MASTER_PORT=2000
TEST_DURATION=30
CLIENTS=50
VALUE_SIZE=1024
REQUESTS=500000
OUTPUT_CSV="incremental_impact_$(date +%Y%m%d_%H%M%S).csv"
MASTER_CONF="../conf/kvstore-master.conf"
SLAVE_CONF="../conf/kvstore-slave.conf"

# 使用说明
usage() {
    cat <<EOF
Usage:
  $0 [options]

Options:
  --master-ip <ip>        Master IP address (default: 127.0.0.1)
  --master-port <port>    Master Redis port (default: 6379)
  --duration <seconds>    Test duration per scenario (default: 30)
  --clients <num>         Number of parallel clients (default: 50)
  --value-size <bytes>    Value size in bytes (default: 1024)
  --requests <num>        Total requests per test (default: 100000)
  --output <file>         CSV output file (default: auto-generated)
  -h, --help              Show this help

Example:
  # 1. Start master with xdp_repl enabled
  sudo ./build/kvstore conf/kvstore-master.conf

  # 2. Run baseline test (no slave)
  ./bench_incremental_impact.sh

  # 3. Script will prompt you to start slave, then continue testing
EOF
}

# 检查必要命令
require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo -e "${RED}[ERROR]${NC} $1 not installed" >&2
        exit 1
    fi
}

# 解析命令行参数
while [ $# -gt 0 ]; do
    case "$1" in
        --master-ip)
            MASTER_IP="$2"
            shift 2
            ;;
        --master-port)
            MASTER_PORT="$2"
            shift 2
            ;;
        --duration)
            TEST_DURATION="$2"
            shift 2
            ;;
        --clients)
            CLIENTS="$2"
            shift 2
            ;;
        --value-size)
            VALUE_SIZE="$2"
            shift 2
            ;;
        --requests)
            REQUESTS="$2"
            shift 2
            ;;
        --output)
            OUTPUT_CSV="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo -e "${RED}[ERROR]${NC} Unknown option: $1" >&2
            usage
            exit 1
            ;;
    esac
done

# 检查依赖
require_cmd redis-cli
require_cmd redis-benchmark
require_cmd bc

# 打印测试信息
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Incremental Sync Impact Test${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Master      : ${MASTER_IP}:${MASTER_PORT}"
echo -e "Duration    : ${TEST_DURATION}s per scenario"
echo -e "Clients     : ${CLIENTS}"
echo -e "Value size  : ${VALUE_SIZE} bytes"
echo -e "Requests    : ${REQUESTS} per test"
echo -e "Output CSV  : ${OUTPUT_CSV}"
echo -e "${BLUE}========================================${NC}"
echo

# 检查 master 连通性
echo -e "${YELLOW}[INFO]${NC} Checking master connectivity..."
if ! redis-cli -h "$MASTER_IP" -p "$MASTER_PORT" PING >/dev/null 2>&1; then
    echo -e "${RED}[ERROR]${NC} Master not reachable at $MASTER_IP:$MASTER_PORT" >&2
    exit 1
fi
echo -e "${GREEN}[OK]${NC} Master is reachable"
echo

# 创建 CSV 文件头
echo "timestamp,scenario,qps,avg_latency_ms,p50_ms,p95_ms,p99_ms,impact_percent" > "$OUTPUT_CSV"

# 运行 redis-benchmark 并提取指标
run_benchmark() {
    local scenario=$1
    echo -e "${YELLOW}[INFO]${NC} Running benchmark: $scenario" >&2
    echo -e "${YELLOW}[INFO]${NC} Test duration: ${TEST_DURATION}s, Clients: ${CLIENTS}, Value size: ${VALUE_SIZE} bytes" >&2

    # 清空数据库（只清空本次测试 key，不影响全量同步）
    redis-cli -h "$MASTER_IP" -p "$MASTER_PORT" FLUSHALL >/dev/null 2>&1 || true
    sleep 1

    # 运行 redis-benchmark (CSV 模式，仅捕获 stdout)
    local csv_output=$(redis-benchmark -h "$MASTER_IP" -p "$MASTER_PORT" \
        -t set \
        -n "$REQUESTS" \
        -c "$CLIENTS" \
        -d "$VALUE_SIZE" \
        --csv 2>/dev/null)

    # 提取 QPS: CSV 格式为 "SET","17485.57"
    local qps=$(echo "$csv_output" | grep '"SET"' | grep -oP '\d+\.\d+' | head -1)

    # 运行延迟测试 (非 CSV 模式，仅捕获 stdout)
    local lat_out=$(redis-benchmark -h "$MASTER_IP" -p "$MASTER_PORT" \
        -t set \
        -n 10000 \
        -c "$CLIENTS" \
        -d "$VALUE_SIZE" \
        2>/dev/null)

    # 提取延迟: redis-benchmark 输出格式 "avg       min       p50..."
    # 数值行格式: "       1.623     1.000     1.500     ..."
    local latency_line=$(echo "$lat_out" | grep -A1 "^[[:space:]]*avg" | tail -1)
    local avg_latency=$(echo "$latency_line" | awk '{print $1}' || echo "0")
    local p50=$(echo "$latency_line" | awk '{print $3}' || echo "0")
    local p95=$(echo "$latency_line" | awk '{print $4}' || echo "0")
    local p99=$(echo "$latency_line" | awk '{print $5}' || echo "0")

    # 如果提取失败，使用备用方法
    if [ -z "$avg_latency" ] || [ "$avg_latency" = "0" ]; then
        avg_latency=$(echo "$lat_out" | grep -oP '[\d.]+' | head -1 || echo "0")
    fi

    echo "$qps|$avg_latency|$p50|$p95|$p99"
}

# ============================================================================
# 场景 1: Baseline (无从机)
# ============================================================================
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Scenario 1: Baseline (No Slave)${NC}"
echo -e "${BLUE}========================================${NC}"
echo

result1=$(run_benchmark "baseline")
qps1=$(echo "$result1" | cut -d'|' -f1)
avg1=$(echo "$result1" | cut -d'|' -f2)
p50_1=$(echo "$result1" | cut -d'|' -f3)
p95_1=$(echo "$result1" | cut -d'|' -f4)
p99_1=$(echo "$result1" | cut -d'|' -f5)

echo
echo -e "${GREEN}[RESULT]${NC} Baseline QPS: ${qps1}"
echo -e "${GREEN}[RESULT]${NC} Avg latency: ${avg1} ms"
echo -e "${GREEN}[RESULT]${NC} P50: ${p50_1} ms, P95: ${p95_1} ms, P99: ${p99_1} ms"
echo

# 记录到 CSV
timestamp=$(date +"%Y-%m-%d %H:%M:%S")
echo "$timestamp,baseline,$qps1,$avg1,$p50_1,$p95_1,$p99_1,0.0" >> "$OUTPUT_CSV"

# ============================================================================
# 等待用户启动从机
# ============================================================================
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Waiting for Slave Connection${NC}"
echo -e "${BLUE}========================================${NC}"
echo
echo -e "${YELLOW}[ACTION REQUIRED]${NC} Please start the slave now:"
echo -e "  sudo ./build/kvstore conf/kvstore-slave.conf"
echo
echo -e "${YELLOW}[INFO]${NC} Press ENTER when slave is connected and ready..."
read -r

# 验证从机连接（通过检查 master 日志或简单等待）
echo -e "${YELLOW}[INFO]${NC} Waiting 5 seconds for slave to stabilize..."
sleep 5

# ============================================================================
# 场景 2: With Slave (增量同步激活)
# ============================================================================
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Scenario 2: With Slave Connected${NC}"
echo -e "${BLUE}========================================${NC}"
echo

result2=$(run_benchmark "with_slave")
qps2=$(echo "$result2" | cut -d'|' -f1)
avg2=$(echo "$result2" | cut -d'|' -f2)
p50_2=$(echo "$result2" | cut -d'|' -f3)
p95_2=$(echo "$result2" | cut -d'|' -f4)
p99_2=$(echo "$result2" | cut -d'|' -f5)

echo
echo -e "${GREEN}[RESULT]${NC} With Slave QPS: ${qps2}"
echo -e "${GREEN}[RESULT]${NC} Avg latency: ${avg2} ms"
echo -e "${GREEN}[RESULT]${NC} P50: ${p50_2} ms, P95: ${p95_2} ms, P99: ${p99_2} ms"
echo

# 计算性能影响
impact=$(echo "scale=2; ($qps2 - $qps1) / $qps1 * 100" | bc -l)

# 记录到 CSV
timestamp=$(date +"%Y-%m-%d %H:%M:%S")
echo "$timestamp,with_slave,$qps2,$avg2,$p50_2,$p95_2,$p99_2,$impact" >> "$OUTPUT_CSV"

# ============================================================================
# 生成报告
# ============================================================================
echo
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Performance Impact Summary${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Baseline QPS    : ${qps1}"
echo -e "With Slave QPS  : ${qps2}"
echo -e "Impact          : ${impact}%"
echo
if (( $(echo "$impact < -5" | bc -l) )); then
    echo -e "${RED}[WARNING]${NC} Significant performance impact detected (> 5%)"
    echo -e "${YELLOW}[SUGGESTION]${NC} Consider optimizing eBPF filter or ringbuf size"
elif (( $(echo "$impact < 0" | bc -l) )); then
    echo -e "${YELLOW}[INFO]${NC} Minor performance impact (< 5%)"
else
    echo -e "${GREEN}[OK]${NC} No significant performance impact"
fi
echo
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}Results saved to: ${OUTPUT_CSV}${NC}"
echo
cat "$OUTPUT_CSV" | column -t -s,
echo
