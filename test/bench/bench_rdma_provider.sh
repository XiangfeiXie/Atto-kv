#!/bin/bash
# =============================================================================
# bench_rdma_provider.sh - RDMA 全量同步性能测试脚本（从机端）
#
# 用法: sudo ./bench_rdma_provider.sh --payload <bytes> --master-ip <ip>
#
# 功能:
#   - 测试 RDMA 全量同步性能
#   - 启动从机，等待同步完成，记录性能指标
#   - 生成 CSV 格式的测试报告
#
# 注意:
#   - 主机和从机的 provider 必须一致（都用 tcp 或都用 verbs）
#   - 配置文件需要手动修改 provider
#   - 本脚本只负责单次测试和记录结果
#
# 测试流程:
#   1. 主机: 修改 conf/kvstore-master.conf 设置 provider
#   2. 主机: 启动 kvstore 并运行 fill_data.sh 填充数据
#   3. 从机: 修改 conf/kvstore-slave.conf 设置相同的 provider
#   4. 从机: 运行本脚本测试
# =============================================================================

set -euo pipefail

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 脚本路径
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
KVS_BIN="${PROJECT_DIR}/build/kvstore"
SLAVE_CONF_TEMPLATE="${PROJECT_DIR}/conf/kvstore-slave.conf"

# 默认参数
PAYLOAD_BYTES=0
MASTER_IP=""
MASTER_RDMA_PORT=2100
MASTER_REDIS_PORT=2000
SLAVE_PORT=2002
TIMEOUT=120
PROVIDER=""  # 从配置文件读取

# CSV 输出文件
RESULTS_CSV="rdma_benchmark_$(date +%Y%m%d_%H%M%S).csv"

# 使用说明
usage() {
    cat <<EOF
Usage:
  sudo $0 --payload <bytes> --master-ip <ip> [options]

Required:
  --payload <bytes>       Data size in bytes (from fill_data.sh output)
  --master-ip <ip>        Master IP address

Options:
  --master-rdma-port <port>  Master RDMA port (default: 2100)
  --master-redis-port <port> Master Redis port (default: 2000)
  --slave-port <port>        Slave service port (default: 2002)
  --timeout <seconds>        Sync timeout (default: 120)
  --output <file>            CSV output file (default: auto-generated)
  -h, --help                 Show this help

Note:
  - Master and slave must use the SAME provider (both tcp or both verbs)
  - Modify conf/kvstore-master.conf and conf/kvstore-slave.conf manually
  - This script tests once and records the result

Example:
  # 1. On master: edit conf/kvstore-master.conf, set rdma_provider tcp
  # 2. On master: start kvstore and fill data
  ./fill_data.sh 100 192.168.1.10 2000

  # 3. On slave: edit conf/kvstore-slave.conf, set rdma_provider tcp
  # 4. On slave: run test
  sudo ./bench_rdma_provider.sh --payload 104857600 --master-ip 192.168.1.10

  # 5. Repeat for verbs provider
EOF
}

# 检查必要命令
require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo -e "${RED}[ERROR]${NC} $1 not installed" >&2
        exit 1
    fi
}

# 检查 root 权限
require_root() {
    if [ "${EUID:-0}" -ne 0 ]; then
        echo -e "${RED}[ERROR]${NC} This script requires root privileges" >&2
        exit 1
    fi
}

# 解析命令行参数
while [ $# -gt 0 ]; do
    case "$1" in
        --payload)
            PAYLOAD_BYTES="$2"
            shift 2
            ;;
        --master-ip)
            MASTER_IP="$2"
            shift 2
            ;;
        --master-rdma-port)
            MASTER_RDMA_PORT="$2"
            shift 2
            ;;
        --master-redis-port)
            MASTER_REDIS_PORT="$2"
            shift 2
            ;;
        --slave-port)
            SLAVE_PORT="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        --output)
            RESULTS_CSV="$2"
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

# 参数验证
if [ "$PAYLOAD_BYTES" -le 0 ]; then
    echo -e "${RED}[ERROR]${NC} Missing or invalid --payload" >&2
    usage
    exit 1
fi

if [ -z "$MASTER_IP" ]; then
    echo -e "${RED}[ERROR]${NC} Missing --master-ip" >&2
    usage
    exit 1
fi

# 检查依赖
require_root
require_cmd bc
require_cmd nc
require_cmd fuser

if [ ! -x "$KVS_BIN" ]; then
    echo -e "${RED}[ERROR]${NC} kvstore binary not found: $KVS_BIN" >&2
    exit 1
fi

if [ ! -f "$SLAVE_CONF_TEMPLATE" ]; then
    echo -e "${RED}[ERROR]${NC} Slave config template not found: $SLAVE_CONF_TEMPLATE" >&2
    exit 1
fi

# 计算数据大小（MB）
PAYLOAD_MB=$(echo "scale=2; $PAYLOAD_BYTES / 1048576" | bc -l)

# 从配置文件读取 provider
PROVIDER=$(grep "^rdma_provider" "$SLAVE_CONF_TEMPLATE" | awk '{print $2}' || echo "unknown")

# 打印测试信息
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  RDMA Fullsync Performance Test${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Master      : ${MASTER_IP}:${MASTER_RDMA_PORT} (RDMA), :${MASTER_REDIS_PORT} (Redis)"
echo -e "Slave port  : ${SLAVE_PORT}"
echo -e "Payload     : ${PAYLOAD_MB} MB (${PAYLOAD_BYTES} bytes)"
echo -e "Provider    : ${PROVIDER}"
echo -e "Timeout     : ${TIMEOUT}s"
echo -e "Output CSV  : ${RESULTS_CSV}"
echo -e "${BLUE}========================================${NC}"
echo

# 检查主机连通性（检查 Redis 服务端口）
echo -e "${YELLOW}[INFO]${NC} Checking master connectivity at $MASTER_IP:$MASTER_REDIS_PORT..."
resp=$(printf '*1\r\n$4\r\nPING\r\n' | nc -w 3 "$MASTER_IP" "$MASTER_REDIS_PORT" 2>/dev/null | tr -d '\r\n' || true)
if [ "$resp" != "+PONG" ]; then
    echo -e "${RED}[ERROR]${NC} Master Redis service not reachable at $MASTER_IP:$MASTER_REDIS_PORT" >&2
    exit 1
fi
echo -e "${GREEN}[OK]${NC} Master is reachable"
echo

# 创建 CSV 文件头（如果文件不存在）
if [ ! -f "$RESULTS_CSV" ]; then
    echo "timestamp,provider,payload_mb,payload_bytes,sync_time_ms,throughput_mbps,keys_loaded,slave_port,master_ip" > "$RESULTS_CSV"
fi

# 生成从机配置文件（使用原配置，只修改必要参数）
conf_tmp="/tmp/kvstore_slave_bench_$$.conf"
sed -e "s/^rdma_master_ip .*/rdma_master_ip ${MASTER_IP}/" \
    -e "s/^rdma_master_port .*/rdma_master_port ${MASTER_RDMA_PORT}/" \
    -e "s/^port .*/port ${SLAVE_PORT}/" \
    "$SLAVE_CONF_TEMPLATE" > "$conf_tmp"

# 清理端口占用
fuser -k "${SLAVE_PORT}/tcp" 2>/dev/null || true
sleep 1

# 从机日志文件
slave_log="/tmp/kvstore_slave_$$.log"
rm -f "$slave_log"

# 启动从机
echo -e "${YELLOW}[INFO]${NC} Starting slave..."
nohup "$KVS_BIN" "$conf_tmp" > "$slave_log" 2>&1 &
slave_pid=$!

echo -e "${YELLOW}[INFO]${NC} Slave PID: $slave_pid"
echo -e "${YELLOW}[INFO]${NC} Waiting for fullsync to complete (timeout: ${TIMEOUT}s)..."

# 等待同步完成
synced=0
start_time=$(date +%s)

for ((i = 1; i <= TIMEOUT; i++)); do
    # 检查进程是否还在运行
    if ! kill -0 "$slave_pid" 2>/dev/null; then
        echo -e "${RED}[ERROR]${NC} Slave process died unexpectedly" >&2
        echo -e "${YELLOW}[LOG]${NC} Last 20 lines of log:"
        tail -20 "$slave_log" 2>/dev/null || true
        rm -f "$conf_tmp"
        exit 1
    fi

    # 检查同步完成标志
    if grep -q 'loaded.*keys from fullsync' "$slave_log" 2>/dev/null; then
        synced=1
        break
    fi

    sleep 1
done

end_time=$(date +%s)

if [ "$synced" -eq 0 ]; then
    echo -e "${RED}[ERROR]${NC} Fullsync timeout after ${TIMEOUT}s" >&2
    echo -e "${YELLOW}[LOG]${NC} Last 20 lines of log:"
    tail -20 "$slave_log" 2>/dev/null || true
    kill "$slave_pid" 2>/dev/null || true
    rm -f "$conf_tmp"
    exit 1
fi

# 提取性能指标
fullsync_time_ms=$(grep 'fullsync_time_ms=' "$slave_log" 2>/dev/null | tail -1 | sed 's/.*fullsync_time_ms=//' | awk '{print $1}' || echo "0")
keys_loaded=$(grep 'loaded.*keys from fullsync' "$slave_log" 2>/dev/null | tail -1 | sed 's/.*loaded //' | awk '{print $1}' || echo "0")

if [ -z "$fullsync_time_ms" ] || [ "$fullsync_time_ms" = "0" ]; then
    echo -e "${RED}[ERROR]${NC} Failed to extract fullsync_time_ms from log" >&2
    echo -e "${YELLOW}[LOG]${NC} Last 20 lines of log:"
    tail -20 "$slave_log" 2>/dev/null || true
    kill "$slave_pid" 2>/dev/null || true
    rm -f "$conf_tmp"
    exit 1
fi

# 计算吞吐量 (MB/s)
throughput_mbps=$(echo "scale=2; $PAYLOAD_BYTES / $fullsync_time_ms * 1000 / 1048576" | bc -l)

# 记录到 CSV
timestamp=$(date +"%Y-%m-%d %H:%M:%S")
echo "$timestamp,$PROVIDER,$PAYLOAD_MB,$PAYLOAD_BYTES,$fullsync_time_ms,$throughput_mbps,$keys_loaded,$SLAVE_PORT,$MASTER_IP" >> "$RESULTS_CSV"

# 打印结果
echo
echo -e "${GREEN}[SUCCESS]${NC} Fullsync completed"
echo -e "  Provider     : ${PROVIDER}"
echo -e "  Sync time    : ${fullsync_time_ms} ms"
echo -e "  Throughput   : ${throughput_mbps} MB/s"
echo -e "  Keys loaded  : ${keys_loaded}"
echo

# 停止从机
kill "$slave_pid" 2>/dev/null || true
fuser -k "${SLAVE_PORT}/tcp" 2>/dev/null || true
rm -f "$conf_tmp"

# 打印汇总
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Test Complete${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}Results saved to: ${RESULTS_CSV}${NC}"
echo
cat "$RESULTS_CSV" | column -t -s,
echo
