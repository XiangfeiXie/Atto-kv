#!/bin/bash
# =============================================================================
# bench_fullsync_kvs_slave.sh — kvstore 从机端：启动从机、计时 RDMA 全量同步
#
# 用法:
#   sudo ./bench_fullsync_kvs_slave.sh --payload N [选项]
#
# kvstore 计时窗口: RDMA 建连完成 → EOF 接收完成 (纯传输+加载，不含进程启动)
#
# 选项:
#   --payload N            RDB 字节数 (必填，从 master 脚本输出获取)
#   --master-host IP       master 地址 (默认 192.168.30.135)
#   --master-port PORT     kvstore master 端口 (默认 2000)
#   --slave-port PORT      kvstore slave 端口 (默认 2002)
#
# 示例:
#   sudo ./bench_fullsync_kvs_slave.sh --payload 1250000
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
KVS_BIN="${PROJECT_DIR}/build/kvstore"
KVS_CONF_TEMPLATE="${PROJECT_DIR}/conf/bench/kvstore-slave-repl.conf"

# 默认值
MASTER_HOST="192.168.30.135"
MASTER_PORT="2000"
SLAVE_PORT="2002"
PAYLOAD="0"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'

usage() {
    echo "用法: $0 --payload N [选项]"
    echo ""
    echo "选项:"
    echo "  --payload N         RDB 字节数 (必填)"
    echo "  --master-host IP    master 地址 (默认 $MASTER_HOST)"
    echo "  --master-port PORT  kvstore master 端口 (默认 $MASTER_PORT)"
    echo "  --slave-port PORT   kvstore slave 端口 (默认 $SLAVE_PORT)"
    echo ""
    echo "示例:"
    echo "  sudo $0 --payload 1250000"
    exit 1
}

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo -e "${RED}[ERROR]${NC} $1 未安装"
        exit 1
    fi
}

require_root() {
    if [ "${EUID:-0}" -ne 0 ]; then
        echo -e "${RED}[ERROR]${NC} 需要 root 权限 (启动 kvstore 需要 proactor)"
        exit 1
    fi
}

# ---- 解析参数 ----

while [ $# -gt 0 ]; do
    case "$1" in
        --payload)      PAYLOAD="$2"; shift 2 ;;
        --master-host)  MASTER_HOST="$2"; shift 2 ;;
        --master-port)  MASTER_PORT="$2"; shift 2 ;;
        --slave-port)   SLAVE_PORT="$2"; shift 2 ;;
        -*) echo "未知选项: $1"; usage ;;
        *)  shift ;;
    esac
done

if [ "$PAYLOAD" -le 0 ]; then
    echo -e "${RED}[ERROR]${NC} 缺少 --payload，请从 master 脚本输出获取"
    exit 1
fi

require_root
require_cmd bc
require_cmd nc

# 检查 master kvstore 可达 (用 printf+nc 发送 RESP PING)
echo "检查 master kvstore 连接 $MASTER_HOST:$MASTER_PORT ..."
resp=$(printf '*1\r\n$4\r\nPING\r\n' | nc -w 3 "$MASTER_HOST" "$MASTER_PORT" 2>/dev/null | tr -d '\r\n' || true)
if [ "$resp" != "+PONG" ]; then
    echo -e "${RED}[ERROR]${NC} kvstore master 未运行 ($MASTER_HOST:$MASTER_PORT)"
    exit 1
fi
echo -e "  ${GREEN}master kvstore 可达${NC}"

# 检查 kvstore 二进制
if [ ! -x "$KVS_BIN" ]; then
    echo -e "${RED}[ERROR]${NC} kvstore 未编译: $KVS_BIN"
    exit 1
fi

# 生成临时配置
conf_tmp="/tmp/kvstore_slave_fullsync_$$.conf"
sed -e "s/^rdma_provider .*/rdma_provider verbs/" \
    -e "s/^rdma_master_ip .*/rdma_master_ip $MASTER_HOST/" \
    -e "s/^port .*/port $SLAVE_PORT/" \
    "$KVS_CONF_TEMPLATE" > "$conf_tmp"

# 清理旧从机
echo "清理占用端口 ${SLAVE_PORT} 的进程 ..."
fuser -k "${SLAVE_PORT}/tcp" 2>/dev/null || true
sleep 0.3
rm -f "${PROJECT_DIR}/build/dump.rdb"

slave_log="/tmp/kvstore_fullsync_slave.log"
rm -f "$slave_log"

payload_mb=$(echo "scale=2; $PAYLOAD / 1048576" | bc -l)

echo ""
echo "============================================"
echo "  全量同步测试 — kvstore verbs"
echo "============================================"
echo "  Master:    $MASTER_HOST:$MASTER_PORT"
echo "  Payload:   ${payload_mb} MB"
echo "============================================"
echo ""

# 启动从机
nohup "$KVS_BIN" "$conf_tmp" > "$slave_log" 2>&1 &
slave_pid=$!
echo "  从机 pid=$slave_pid"

# 等待全量同步完成 (双重检测)
timeout=120 synced=0
for ((i = 1; i <= timeout; i++)); do
    if grep -q 'loaded.*keys from fullsync' "$slave_log" 2>/dev/null; then
        synced=1; break
    fi
    if ss -tln 2>/dev/null | grep -q ":${SLAVE_PORT} "; then
        synced=1; break
    fi
    if ! kill -0 "$slave_pid" 2>/dev/null; then
        echo ""
        echo -e "${RED}[ERROR]${NC} 从机进程意外退出"
        echo "--- 最后 15 行日志 ---"
        tail -15 "$slave_log" 2>/dev/null || true
        rm -f "$conf_tmp"
        exit 1
    fi
    [ $((i % 15)) -eq 0 ] && echo "    ...等待同步完成 (${i}s)"
    sleep 1
done

if [ "$synced" -eq 0 ]; then
    echo -e "${RED}[ERROR]${NC} 全量同步超时 (${timeout}s)"
    echo "--- 最后 15 行日志 ---"
    tail -15 "$slave_log" 2>/dev/null || true
    kill "$slave_pid" 2>/dev/null || true
    rm -f "$conf_tmp"
    exit 1
fi

# 从日志提取 C 代码的纯传输时间
fullsync_ms=$(grep 'fullsync_time_ms=' "$slave_log" 2>/dev/null | tail -1 | sed 's/.*fullsync_time_ms=//' | tr -d ' ' || true)
if [ -z "$fullsync_ms" ]; then
    echo -e "${RED}[ERROR]${NC} 未从日志提取到 fullsync_time_ms (kvstore 需重新编译)"
    kill "$slave_pid" 2>/dev/null || true
    rm -f "$conf_tmp"
    exit 1
fi
fullsync_s=$(echo "scale=3; $fullsync_ms / 1000" | bc -l)
speed_mbps=$(echo "scale=2; $PAYLOAD / $fullsync_ms * 1000 / 1048576" | bc -l)

# 等待端口就绪
sleep 0.3

# 数据验证 — 检查 master 设置的标记 key
verify="no"
verify_raw=$(printf '*2\r\n$5\r\nEXIST\r\n$18\r\n__bench_marker__\r\n' | \
             nc -w 2 127.0.0.1 "$SLAVE_PORT" 2>/dev/null | \
             tr -d '\r\n' || true)
if [ "$verify_raw" = ":1" ]; then
    verify="yes"
else
    sleep 0.5
    verify_raw=$(printf '*2\r\n$5\r\nEXIST\r\n$18\r\n__bench_marker__\r\n' | \
                 nc -w 2 127.0.0.1 "$SLAVE_PORT" 2>/dev/null | \
                 tr -d '\r\n' || true)
    [ "$verify_raw" = ":1" ] && verify="yes"
fi

echo ""
echo "  ═══════════════════════════════════"
echo "  kvstore verbs 全量同步结果"
echo "  ═══════════════════════════════════"
echo "  数据量:       ${payload_mb} MB"
echo "  传输+加载:    ${fullsync_ms} ms  (纯传输，不含进程启动)"
echo -e "  吞吐量:       ${GREEN}${speed_mbps} MB/s${NC}"
echo -e "  数据验证:     $([ "$verify" = "yes" ] && echo -e "${GREEN}通过${NC}" || echo -e "${RED}失败${NC} (EXIST=$verify_raw)")"
echo ""

# 清理
kill "$slave_pid" 2>/dev/null || true
sleep 0.5
fuser -k "${SLAVE_PORT}/tcp" 2>/dev/null || true
rm -f "$conf_tmp"
