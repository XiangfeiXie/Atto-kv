#!/bin/bash
# =============================================================================
# bench_fullsync_redis_slave.sh — Redis 从机端：启动从机、计时全量同步
#
# 用法:
#   ./bench_fullsync_redis_slave.sh --payload N [选项]
#
# Redis 计时窗口: SLAVEOF 发送 → master_link_status:up
#
# 选项:
#   --payload N              RDB 字节数 (必填，从 master 脚本输出获取)
#   --master-host IP         master 地址 (默认 192.168.30.135)
#   --redis-master-port PORT Redis master 端口 (默认 6379)
#   --redis-port PORT        Redis slave 端口 (默认 6380)
#   --redis-server PATH      redis-server 路径 (默认 redis-server)
#   --mode MODE              sendfile (默认) | diskless
#
# 示例:
#   ./bench_fullsync_redis_slave.sh --payload 1250000
#   ./bench_fullsync_redis_slave.sh --payload 1250000 --mode diskless
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# 默认值
MASTER_HOST="192.168.30.135"
REDIS_MASTER_PORT="6379"
REDIS_SLAVE_PORT="6380"
REDIS_SERVER="redis-server"
PAYLOAD="0"
MODE="sendfile"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'

usage() {
    echo "用法: $0 --payload N [选项]"
    echo ""
    echo "选项:"
    echo "  --payload N              RDB 字节数 (必填)"
    echo "  --master-host IP         master 地址 (默认 $MASTER_HOST)"
    echo "  --redis-master-port PORT Redis master 端口 (默认 $REDIS_MASTER_PORT)"
    echo "  --redis-port PORT        Redis slave 端口 (默认 $REDIS_SLAVE_PORT)"
    echo "  --redis-server PATH      redis-server 路径 (默认 $REDIS_SERVER)"
    echo "  --mode MODE              sendfile (默认) | diskless"
    echo ""
    echo "示例:"
    echo "  $0 --payload 1250000"
    echo "  $0 --payload 1250000 --mode diskless"
    exit 1
}

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo -e "${RED}[ERROR]${NC} $1 未安装"
        exit 1
    fi
}

# ---- 解析参数 ----

while [ $# -gt 0 ]; do
    case "$1" in
        --payload)           PAYLOAD="$2"; shift 2 ;;
        --master-host)       MASTER_HOST="$2"; shift 2 ;;
        --redis-master-port) REDIS_MASTER_PORT="$2"; shift 2 ;;
        --redis-port)        REDIS_SLAVE_PORT="$2"; shift 2 ;;
        --redis-server)      REDIS_SERVER="$2"; shift 2 ;;
        --mode)              MODE="$2"; shift 2 ;;
        -*) echo "未知选项: $1"; usage ;;
        *)  shift ;;
    esac
done

if [ "$PAYLOAD" -le 0 ]; then
    echo -e "${RED}[ERROR]${NC} 缺少 --payload，请从 master 脚本输出获取"
    exit 1
fi

if [ "$MODE" != "sendfile" ] && [ "$MODE" != "diskless" ]; then
    echo -e "${RED}[ERROR]${NC} --mode 必须是 sendfile 或 diskless"
    exit 1
fi

require_cmd bc
require_cmd redis-cli

label="Redis $MODE"

# 检查 master Redis 可达
echo "检查 master Redis 连接 $MASTER_HOST:$REDIS_MASTER_PORT ..."
if ! redis-cli -h "$MASTER_HOST" -p "$REDIS_MASTER_PORT" PING >/dev/null 2>&1; then
    echo -e "${RED}[ERROR]${NC} Redis master 未运行 ($MASTER_HOST:$REDIS_MASTER_PORT)"
    exit 1
fi
echo -e "  ${GREEN}master Redis 可达${NC}"

# 检查 redis-server
if ! command -v "$REDIS_SERVER" >/dev/null 2>&1; then
    echo -e "${RED}[ERROR]${NC} redis-server 未找到: $REDIS_SERVER"
    exit 1
fi

# 清理占用端口的进程
echo "清理占用端口 ${REDIS_SLAVE_PORT} 的进程 ..."
fuser -k "${REDIS_SLAVE_PORT}/tcp" 2>/dev/null || true
sleep 0.5

# 启动 Redis 从机 (如果未运行)
if ! redis-cli -p "$REDIS_SLAVE_PORT" PING >/dev/null 2>&1; then
    echo "启动 Redis 从机 (port $REDIS_SLAVE_PORT) ..."
    "$REDIS_SERVER" --port "$REDIS_SLAVE_PORT" \
                   --daemonize yes \
                   --loglevel warning \
                   --save "" \
                   --appendonly no \
                   --repl-diskless-sync no
    sleep 1
    if ! redis-cli -p "$REDIS_SLAVE_PORT" PING >/dev/null 2>&1; then
        echo -e "${RED}[ERROR]${NC} Redis 从机启动失败"
        exit 1
    fi
fi
echo -e "  ${GREEN}Redis 从机就绪 (port $REDIS_SLAVE_PORT)${NC}"

# 设置 master redis 的 diskless 模式
if [ "$MODE" = "diskless" ]; then
    redis-cli -h "$MASTER_HOST" -p "$REDIS_MASTER_PORT" \
              CONFIG SET repl-diskless-sync yes >/dev/null 2>&1 || true
    redis-cli -h "$MASTER_HOST" -p "$REDIS_MASTER_PORT" \
              CONFIG SET repl-diskless-sync-delay 0 >/dev/null 2>&1 || true
else
    redis-cli -h "$MASTER_HOST" -p "$REDIS_MASTER_PORT" \
              CONFIG SET repl-diskless-sync no >/dev/null 2>&1 || true
fi

payload_mb=$(echo "scale=2; $PAYLOAD / 1048576" | bc -l)

echo ""
echo "============================================"
echo "  全量同步测试 — $label"
echo "============================================"
echo "  Master:    $MASTER_HOST:$REDIS_MASTER_PORT"
echo "  Slave:     127.0.0.1:$REDIS_SLAVE_PORT"
echo "  Payload:   ${payload_mb} MB"
echo "============================================"
echo ""

# 断开旧复制关系
redis-cli -p "$REDIS_SLAVE_PORT" SLAVEOF NO ONE >/dev/null 2>&1 || true
sleep 1

# 开始同步并计时 (t0 = SLAVEOF 发出的时刻)
t0_ns=$(date +%s%N)
redis-cli -p "$REDIS_SLAVE_PORT" SLAVEOF "$MASTER_HOST" "$REDIS_MASTER_PORT" >/dev/null 2>&1

# 等待同步完成
timeout=120 synced=0
for ((i = 1; i <= timeout; i++)); do
    link_status=$(redis-cli -p "$REDIS_SLAVE_PORT" INFO replication 2>/dev/null | \
                  grep 'master_link_status' | cut -d: -f2 | tr -d '\r\n ')
    if [ "$link_status" = "up" ]; then
        synced=1; break
    fi
    [ $((i % 15)) -eq 0 ] && echo "    ...等待同步完成 (${i}s)"
    sleep 1
done

t1_ns=$(date +%s%N)

if [ "$synced" -eq 0 ]; then
    echo -e "${RED}[ERROR]${NC} Redis 全量同步超时 (${timeout}s)"
    redis-cli -p "$REDIS_SLAVE_PORT" SLAVEOF NO ONE >/dev/null 2>&1 || true
    exit 1
fi

# 计算耗时和吞吐量
elapsed=$(echo "scale=3; ($t1_ns - $t0_ns) / 1000000000" | bc -l)
if [ "$(echo "$elapsed < 0.001" | bc -l)" = "1" ]; then
    elapsed="0.001"
fi
speed_mbps=$(echo "scale=2; $PAYLOAD / $elapsed / 1048576" | bc -l)

echo ""
echo "  ═══════════════════════════════════"
echo "  $label 全量同步结果"
echo "  ═══════════════════════════════════"
echo "  数据量:       ${payload_mb} MB"
echo "  耗时:         ${elapsed} s"
echo -e "  吞吐量:       ${GREEN}${speed_mbps} MB/s${NC}"
echo ""

# 断开复制 (保留从机进程供后续测试)
redis-cli -p "$REDIS_SLAVE_PORT" SLAVEOF NO ONE >/dev/null 2>&1 || true
