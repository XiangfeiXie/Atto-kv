#!/bin/bash
# =============================================================================
# bench_fullsync_master.sh — 主机端：填充测试数据 + 报告 RDB 大小
#
# 用法:
#   ./bench_fullsync_master.sh populate <nkeys> [选项]
#
# 选项:
#   --host IP         master 地址 (默认 192.168.30.135)
#   --port PORT       master 端口 (默认 2000)
#   --value-size N    每条 value 的字节数 (默认 16)
#
# 输出 (最后一行):
#   PAYLOAD=<bytes> KEY_COUNT=<nkeys>
#
# 示例:
#   # 启动 master
#   sudo ./build/kvstore conf/bench/kvstore-master-repl.conf &
#   # 填充 5 万条
#   ./test/bench/bench_fullsync_master.sh populate 50000
#   # → PAYLOAD=1250000 KEY_COUNT=50000
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# 默认值
MASTER_HOST="192.168.30.135"
MASTER_PORT="2000"
VALUE_SIZE="16"
NKEYS=""

RED='\033[0;31m'; GREEN='\033[0;32m'; NC='\033[0m'

usage() {
    echo "用法: $0 populate <nkeys> [选项]"
    echo ""
    echo "选项:"
    echo "  --host IP         master 地址 (默认 $MASTER_HOST)"
    echo "  --port PORT       master 端口 (默认 $MASTER_PORT)"
    echo "  --value-size N    每条 value 的字节数 (默认 $VALUE_SIZE)"
    echo ""
    echo "示例:"
    echo "  $0 populate 50000"
    echo "  $0 populate 1000000 --host 10.0.0.1 --port 6379"
    exit 1
}

# ---- 解析参数 ----
CMD=""
while [ $# -gt 0 ]; do
    case "$1" in
        populate)
            CMD="populate"; shift
            ;;
        --host)      MASTER_HOST="$2"; shift 2 ;;
        --port)      MASTER_PORT="$2"; shift 2 ;;
        --value-size) VALUE_SIZE="$2"; shift 2 ;;
        *)
            if [ -z "$NKEYS" ] && [[ "$1" =~ ^[0-9]+$ ]]; then
                NKEYS="$1"; shift
            else
                echo "未知选项: $1"; usage
            fi
            ;;
    esac
done

[ "$CMD" != "populate" ] && usage
[ -z "$NKEYS" ] && { echo -e "${RED}[ERROR]${NC} 缺少 nkeys"; usage; }

# ---- 检查依赖 ----
if ! command -v redis-cli >/dev/null 2>&1; then
    echo -e "${RED}[ERROR]${NC} redis-cli 未安装"
    exit 1
fi

# ---- 检查 master 可达 ----
echo "检查 master 连接 $MASTER_HOST:$MASTER_PORT ..."
if ! redis-cli -h "$MASTER_HOST" -p "$MASTER_PORT" PING >/dev/null 2>&1; then
    echo -e "${RED}[ERROR]${NC} master 未运行 ($MASTER_HOST:$MASTER_PORT)"
    exit 1
fi
echo -e "  ${GREEN}master 可达${NC}"

# ---- 构造 value 字符串 ----
VALUE_STR=$(printf '%-*s' "$VALUE_SIZE" ' ' | tr ' ' 'x')
VALUE_STR="${VALUE_STR:0:$VALUE_SIZE}"
VALUE_LEN=${#VALUE_STR}

echo "填充 $NKEYS 条数据 (value=${VALUE_LEN}B) ..."

# ---- 批量填充 ----
BATCH=10000
SENT=0
while [ $SENT -lt $NKEYS ]; do
    BATCH_N=$BATCH
    [ $((SENT + BATCH_N)) -gt $NKEYS ] && BATCH_N=$((NKEYS - SENT))

    # 构造一批 RESP SET 命令
    for i in $(seq $((SENT + 1)) $((SENT + BATCH_N))); do
        printf '*3\r\n$3\r\nSET\r\n$9\r\nkey%06d\r\n$%d\r\n%s\r\n' \
               "$i" "$VALUE_LEN" "$VALUE_STR"
    done | timeout 300 nc -w 10 "$MASTER_HOST" "$MASTER_PORT" > /dev/null 2>&1 || {
        echo ""
        echo -e "${RED}[ERROR]${NC} nc 发送失败 (sent=$SENT, batch=$BATCH_N)"
        exit 1
    }

    SENT=$((SENT + BATCH_N))
    printf "." >&2
done
echo ""
echo "填充完成: $NKEYS 条"

# ---- 触发 SAVE ----
echo "触发 SAVE ..."
redis-cli -h "$MASTER_HOST" -p "$MASTER_PORT" SAVE >/dev/null 2>&1 || true

# 轮询等待 dump.rdb
RDB_FILE="${PROJECT_DIR}/build/dump.rdb"
RDB_SIZE=0
for i in $(seq 1 60); do
    sleep 0.5
    if [ -f "$RDB_FILE" ]; then
        RDB_SIZE=$(stat -c%s "$RDB_FILE" 2>/dev/null || echo 0)
        [ "$RDB_SIZE" -gt 0 ] && break
    fi
    [ $((i % 10)) -eq 0 ] && echo "  等待 RDB 文件... (${i}x0.5s)"
done

if [ "$RDB_SIZE" -le 0 ]; then
    echo -e "${RED}[ERROR]${NC} SAVE 超时，未生成 dump.rdb"
    exit 1
fi

RDB_MB=$(echo "scale=2; $RDB_SIZE / 1048576" | bc -l 2>/dev/null || echo "N/A")
echo ""
echo -e "  ${GREEN}PAYLOAD=$RDB_SIZE${NC}  KEY_COUNT=$NKEYS  (${RDB_MB} MB)"
echo ""
echo "在从机执行:"
echo "  sudo ./test/bench/bench_fullsync_slave.sh kvs   --payload $RDB_SIZE"
echo "  sudo ./test/bench/bench_fullsync_slave.sh redis --payload $RDB_SIZE"
echo "  sudo ./test/bench/bench_fullsync_slave.sh redis --payload $RDB_SIZE --mode diskless"
