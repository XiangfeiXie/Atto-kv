#!/bin/bash
# =============================================================================
# bench_fullsync_master.sh — 主机端：填充测试数据 + 报告 RDB 大小
#
# 使用 redis-benchmark 填充随机 SET 命令到 kvstore 或 Redis。
#
# 用法:
#   ./bench_fullsync_master.sh kvs   <nkeys> [选项]
#   ./bench_fullsync_master.sh redis <nkeys> [选项]
#
# 选项:
#   --host IP         master 地址 (默认 192.168.30.135)
#   --port PORT       kvstore 端口 (默认 2000)
#   --redis-port PORT Redis 端口 (默认 6379)
#   --value-size N    每条 value 的字节数 (默认 16)
#
# 输出 (最后一行):
#   PAYLOAD=<bytes> KEY_COUNT=<nkeys>
#
# 示例:
#   ./bench_fullsync_master.sh kvs   50000
#   ./bench_fullsync_master.sh redis 50000
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# 默认值
MASTER_HOST="192.168.30.135"
MASTER_PORT="2000"           # kvstore
REDIS_MASTER_PORT="6379"    # Redis
VALUE_SIZE="16"
TARGET=""
NKEYS=""

RED='\033[0;31m'; GREEN='\033[0;32m'; NC='\033[0m'

usage() {
    echo "用法: $0 <kvs|redis> <nkeys> [选项]"
    echo ""
    echo "目标:"
    echo "  kvs     填充 kvstore master"
    echo "  redis   填充 Redis master"
    echo ""
    echo "选项:"
    echo "  --host IP         master 地址 (默认 $MASTER_HOST)"
    echo "  --port PORT       kvstore master 端口 (默认 $MASTER_PORT)"
    echo "  --redis-port PORT Redis master 端口 (默认 $REDIS_MASTER_PORT)"
    echo "  --value-size N    每条 value 的字节数 (默认 $VALUE_SIZE)"
    echo ""
    echo "示例:"
    echo "  $0 kvs   50000"
    echo "  $0 redis 50000"
    echo "  $0 redis 1000000 --host 10.0.0.1 --redis-port 6380"
    exit 1
}

# ---- 解析参数 ----
while [ $# -gt 0 ]; do
    case "$1" in
        kvs|redis)
            TARGET="$1"; shift
            ;;
        --host)       MASTER_HOST="$2"; shift 2 ;;
        --port)       MASTER_PORT="$2"; shift 2 ;;
        --redis-port) REDIS_MASTER_PORT="$2"; shift 2 ;;
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

[ -z "$TARGET" ] && usage
[ -z "$NKEYS"  ] && { echo -e "${RED}[ERROR]${NC} 缺少 nkeys"; usage; }

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo -e "${RED}[ERROR]${NC} $1 未安装"
        exit 1
    fi
}

# ---- 公共输出 ----

print_result() {
    local rdb_size="$1"
    local rdb_mb
    rdb_mb=$(echo "scale=2; $rdb_size / 1048576" | bc -l 2>/dev/null || echo "N/A")
    echo ""
    echo -e "  ${GREEN}PAYLOAD=$rdb_size${NC}  KEY_COUNT=$NKEYS  (${rdb_mb} MB)"
    echo ""
    if [ "$TARGET" = "kvs" ]; then
        echo "在从机执行:"
        echo "  sudo ./test/bench/bench_fullsync_kvs_slave.sh   --payload $rdb_size"
    else
        echo "在从机执行:"
        echo "  ./test/bench/bench_fullsync_redis_slave.sh --payload $rdb_size"
        echo "  ./test/bench/bench_fullsync_redis_slave.sh --payload $rdb_size --mode diskless"
    fi
}

# ================================================================
#  KVS 填充 (redis-benchmark)
# ================================================================

do_populate_kvs() {
    require_cmd redis-cli
    require_cmd redis-benchmark

    local port="$MASTER_PORT"

    echo "检查 kvstore master $MASTER_HOST:$port ..."
    if ! redis-cli -h "$MASTER_HOST" -p "$port" PING >/dev/null 2>&1; then
        echo -e "${RED}[ERROR]${NC} kvstore master 未运行 ($MASTER_HOST:$port)"
        exit 1
    fi
    echo -e "  ${GREEN}master 可达${NC}"

    # 清空旧数据 + 清除旧 dump.rdb
    echo "清空旧数据 ..."
    redis-cli -h "$MASTER_HOST" -p "$port" FLUSHALL >/dev/null 2>&1 || true
    redis-cli -h "$MASTER_HOST" -p "$port" SAVE >/dev/null 2>&1 || true
    sleep 0.5

    echo "填充 $NKEYS 条随机数据 (value=${VALUE_SIZE}B) via redis-benchmark ..."
    redis-benchmark -h "$MASTER_HOST" -p "$port" \
        -n "$NKEYS" -r "$NKEYS" -t set -d "$VALUE_SIZE" \
        -c 1 -P 16 -q 2>&1 || {
        echo ""
        echo -e "${RED}[ERROR]${NC} redis-benchmark 失败"
        exit 1
    }

    # 设置标记 key 供从机验证
    redis-cli -h "$MASTER_HOST" -p "$port" SET __bench_marker__ check >/dev/null 2>&1 || true

    echo "填充完成"

    # 触发 SAVE + 轮询 dump.rdb
    echo "触发 SAVE ..."
    redis-cli -h "$MASTER_HOST" -p "$port" SAVE >/dev/null 2>&1 || true

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

    print_result "$RDB_SIZE"
}

# ================================================================
#  Redis 填充 (redis-benchmark)
# ================================================================

do_populate_redis() {
    require_cmd redis-cli
    require_cmd redis-benchmark

    local port="$REDIS_MASTER_PORT"

    echo "检查 Redis master $MASTER_HOST:$port ..."
    if ! redis-cli -h "$MASTER_HOST" -p "$port" PING >/dev/null 2>&1; then
        echo -e "${RED}[ERROR]${NC} Redis master 未运行 ($MASTER_HOST:$port)"
        exit 1
    fi
    echo -e "  ${GREEN}master 可达${NC}"

    # 清空旧数据
    echo "清空旧数据 ..."
    redis-cli -h "$MASTER_HOST" -p "$port" FLUSHALL >/dev/null 2>&1 || true

    echo "填充 $NKEYS 条随机数据 (value=${VALUE_SIZE}B) via redis-benchmark ..."
    redis-benchmark -h "$MASTER_HOST" -p "$port" \
        -n "$NKEYS" -r "$NKEYS" -t set -d "$VALUE_SIZE" \
        -c 1 -P 16 -q 2>&1 || {
        echo ""
        echo -e "${RED}[ERROR]${NC} redis-benchmark 失败"
        exit 1
    }

    # 设置标记 key 供从机验证
    redis-cli -h "$MASTER_HOST" -p "$port" SET __bench_marker__ check >/dev/null 2>&1 || true

    echo "填充完成"

    # 触发 BGSAVE + 轮询完成
    echo "触发 BGSAVE ..."
    redis-cli -h "$MASTER_HOST" -p "$port" BGSAVE >/dev/null 2>&1 || true

    RDB_SIZE=0
    for i in $(seq 1 60); do
        sleep 0.5
        local status
        status=$(redis-cli -h "$MASTER_HOST" -p "$port" INFO persistence 2>/dev/null | \
                 grep 'rdb_last_bgsave_status' | cut -d: -f2 | tr -d '\r\n ')
        if [ "$status" = "ok" ]; then
            RDB_SIZE=$(redis-cli -h "$MASTER_HOST" -p "$port" INFO persistence 2>/dev/null | \
                       grep 'rdb_last_save_size' | cut -d: -f2 | tr -d '\r\n ')
            [ "${RDB_SIZE:-0}" -gt 0 ] && break
        fi
        [ $((i % 10)) -eq 0 ] && echo "  等待 BGSAVE... (${i}x0.5s)"
    done

    if [ "${RDB_SIZE:-0}" -le 0 ]; then
        echo -e "${RED}[ERROR]${NC} BGSAVE 超时"
        exit 1
    fi

    print_result "$RDB_SIZE"
}

# ---- 入口 ----

require_cmd bc

case "$TARGET" in
    kvs)   do_populate_kvs ;;
    redis) do_populate_redis ;;
esac
