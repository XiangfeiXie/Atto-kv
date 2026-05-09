#!/bin/bash
# =============================================================================
# bench_fullsync_slave.sh — 从机端：启动从机、计时全量同步、验证数据
#
# 用法:
#   ./bench_fullsync_slave.sh kvs   --payload N [选项]
#   ./bench_fullsync_slave.sh redis --payload N [选项]
#
# kvstore 计时窗口: RDMA 建连完成 → EOF 接收完成 (纯传输+加载，不含进程启动)
# Redis   计时窗口: SLAVEOF 发送 → master_link_status:up
#
# 选项:
#   --payload N            RDB 字节数 (必填，从 master 脚本输出获取)
#   --master-host IP       master 地址 (默认 192.168.30.135)
#   --master-port PORT     kvstore master 端口 (默认 2000)
#   --redis-master-port PORT  Redis master 端口 (默认 6379)
#   --slave-port PORT      kvstore slave 端口 (默认 2002)
#   --redis-port PORT      Redis slave 端口 (默认 6380)
#   --redis-server PATH    redis-server 路径 (默认 redis-server)
#   --mode MODE            Redis 同步模式: sendfile (默认) | diskless
#
# 示例:
#   sudo ./test/bench/bench_fullsync_slave.sh kvs   --payload 1250000
#   sudo ./test/bench/bench_fullsync_slave.sh redis --payload 1250000
#   sudo ./test/bench/bench_fullsync_slave.sh redis --payload 1250000 --mode diskless
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
KVS_BIN="${PROJECT_DIR}/build/kvstore"
KVS_CONF_TEMPLATE="${PROJECT_DIR}/conf/bench/kvstore-slave-repl.conf"

# 默认值
MASTER_HOST="192.168.30.135"
MASTER_PORT="2000"          # kvstore master port
REDIS_MASTER_PORT="6379"    # Redis master port
KVS_SLAVE_PORT="2002"
REDIS_SLAVE_PORT="6380"
REDIS_SERVER="redis-server"
PAYLOAD="0"
MODE="sendfile"             # Redis: sendfile | diskless

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'

usage() {
    echo "用法: $0 <kvs|redis> [选项]"
    echo ""
    echo "场景:"
    echo "  kvs     测试 kvstore RDMA (verbs) 全量同步"
    echo "  redis   测试 Redis 全量同步"
    echo ""
    echo "选项:"
    echo "  --payload N             RDB 字节数 (必填)"
    echo "  --master-host IP        master 地址 (默认 $MASTER_HOST)"
    echo "  --master-port PORT      kvstore master 端口 (默认 $MASTER_PORT)"
    echo "  --redis-master-port PORT Redis master 端口 (默认 $REDIS_MASTER_PORT)"
    echo "  --slave-port PORT       kvstore slave 端口 (默认 $KVS_SLAVE_PORT)"
    echo "  --redis-port PORT       Redis slave 端口 (默认 $REDIS_SLAVE_PORT)"
    echo "  --redis-server PATH     redis-server 路径 (默认 $REDIS_SERVER)"
    echo "  --mode MODE             Redis: sendfile (默认) | diskless"
    echo ""
    echo "示例:"
    echo "  sudo $0 kvs   --payload 1250000"
    echo "  sudo $0 redis --payload 1250000"
    echo "  sudo $0 redis --payload 1250000 --mode diskless"
    exit 1
}

# ---- 依赖检查 ----

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

SCENARIO=""
while [ $# -gt 0 ]; do
    case "$1" in
        kvs|redis)
            SCENARIO="$1"; shift
            ;;
        --payload)            PAYLOAD="$2"; shift 2 ;;
        --master-host)        MASTER_HOST="$2"; shift 2 ;;
        --master-port)        MASTER_PORT="$2"; shift 2 ;;
        --redis-master-port)  REDIS_MASTER_PORT="$2"; shift 2 ;;
        --slave-port)         KVS_SLAVE_PORT="$2"; shift 2 ;;
        --redis-port)         REDIS_SLAVE_PORT="$2"; shift 2 ;;
        --redis-server)       REDIS_SERVER="$2"; shift 2 ;;
        --mode)               MODE="$2"; shift 2 ;;
        -*) echo "未知选项: $1"; usage ;;
        *)  break ;;
    esac
done

[ -z "$SCENARIO" ] && usage

if [ "$PAYLOAD" -le 0 ]; then
    echo -e "${RED}[ERROR]${NC} 缺少 --payload，请从 master 脚本输出获取"
    exit 1
fi

require_cmd bc

# ---- 公共函数 ----

# 检查 master kvstore 可达 (用 printf+nc 发送 RESP PING)
master_kvs_alive() {
    local resp
    resp=$(printf '*1\r\n$4\r\nPING\r\n' | nc -w 3 "$1" "$2" 2>/dev/null | tr -d '\r\n' || true)
    [ "$resp" = "+PONG" ]
}

# 从日志提取 fullsync_time_ms (C 代码计时)
extract_fullsync_ms() {
    grep 'fullsync_time_ms=' "$1" 2>/dev/null | tail -1 | sed 's/.*fullsync_time_ms=//' | tr -d ' '
}

# ================================================================
#  KVS 全量同步测试
# ================================================================

do_test_kvs() {
    require_root
    require_cmd nc

    # 检查 master kvstore 可达
    echo "检查 master kvstore 连接 $MASTER_HOST:$MASTER_PORT ..."
    if ! master_kvs_alive "$MASTER_HOST" "$MASTER_PORT"; then
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
    local conf_tmp="/tmp/kvstore_slave_fullsync_$$.conf"
    sed -e "s/^rdma_provider .*/rdma_provider verbs/" \
        -e "s/^rdma_master_ip .*/rdma_master_ip $MASTER_HOST/" \
        "$KVS_CONF_TEMPLATE" > "$conf_tmp"

    # 清理旧从机
    echo "清理旧从机 ..."
    fuser -k "${KVS_SLAVE_PORT}/tcp" 2>/dev/null || true
    sleep 0.3
    rm -f "${PROJECT_DIR}/build/dump.rdb"

    local slave_log="/tmp/kvstore_fullsync_slave.log"
    rm -f "$slave_log"

    local payload_mb
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
    local slave_pid=$!
    echo "  从机 pid=$slave_pid"

    # 等待全量同步完成 (双重检测)
    local timeout=120 synced=0
    for ((i = 1; i <= timeout; i++)); do
        if grep -q 'loaded.*keys from fullsync' "$slave_log" 2>/dev/null; then
            synced=1; break
        fi
        if ss -tln 2>/dev/null | grep -q ":${KVS_SLAVE_PORT} "; then
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
    local fullsync_ms
    fullsync_ms=$(extract_fullsync_ms "$slave_log" || true)
    if [ -z "$fullsync_ms" ]; then
        echo -e "${RED}[ERROR]${NC} 未从日志提取到 fullsync_time_ms (kvstore 需重新编译)"
        kill "$slave_pid" 2>/dev/null || true
        rm -f "$conf_tmp"
        exit 1
    fi
    local fullsync_s
    fullsync_s=$(echo "scale=3; $fullsync_ms / 1000" | bc -l)
    local speed_mbps
    speed_mbps=$(echo "scale=2; $PAYLOAD / $fullsync_ms * 1000 / 1048576" | bc -l)

    # 等待端口就绪
    sleep 0.3

    # 数据验证
    local verify="no" verify_raw
    verify_raw=$(printf '*2\r\n$5\r\nEXIST\r\n$9\r\nkey000001\r\n' | \
                 nc -w 2 127.0.0.1 "$KVS_SLAVE_PORT" 2>/dev/null | \
                 tr -d '\r\n' || true)
    if [ "$verify_raw" = ":1" ]; then
        verify="yes"
    else
        sleep 0.5
        verify_raw=$(printf '*2\r\n$5\r\nEXIST\r\n$9\r\nkey000001\r\n' | \
                     nc -w 2 127.0.0.1 "$KVS_SLAVE_PORT" 2>/dev/null | \
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
    fuser -k "${KVS_SLAVE_PORT}/tcp" 2>/dev/null || true
    rm -f "$conf_tmp"
}

# ================================================================
#  Redis 全量同步测试
# ================================================================

do_test_redis() {
    require_cmd redis-cli

    local label
    if [ "$MODE" = "diskless" ]; then
        label="Redis diskless"
    else
        label="Redis sendfile"
    fi

    # 检查 master Redis 可达 (用 Redis 专用端口)
    echo "检查 master Redis 连接 $MASTER_HOST:$REDIS_MASTER_PORT ..."
    if ! redis-cli -h "$MASTER_HOST" -p "$REDIS_MASTER_PORT" PING >/dev/null 2>&1; then
        echo -e "${RED}[ERROR]${NC} Redis master 未运行 ($MASTER_HOST:$REDIS_MASTER_PORT)"
        echo "  提示: 默认连接端口 $REDIS_MASTER_PORT，可用 --redis-master-port 指定"
        exit 1
    fi
    echo -e "  ${GREEN}master Redis 可达${NC}"

    # 检查 redis-server
    if ! command -v "$REDIS_SERVER" >/dev/null 2>&1; then
        echo -e "${RED}[ERROR]${NC} redis-server 未找到: $REDIS_SERVER"
        echo "  请指定: --redis-server /path/to/redis-server"
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
    redis-cli -h "$MASTER_HOST" -p "$REDIS_MASTER_PORT" \
              CONFIG SET repl-diskless-sync "$MODE" >/dev/null 2>&1 || true
    if [ "$MODE" = "diskless" ]; then
        redis-cli -h "$MASTER_HOST" -p "$REDIS_MASTER_PORT" \
                  CONFIG SET repl-diskless-sync-delay 0 >/dev/null 2>&1 || true
    fi

    local payload_mb
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
    local t0_ns t1_ns
    t0_ns=$(date +%s%N)
    redis-cli -p "$REDIS_SLAVE_PORT" SLAVEOF "$MASTER_HOST" "$REDIS_MASTER_PORT" >/dev/null 2>&1

    # 等待同步完成
    local timeout=120 synced=0
    for ((i = 1; i <= timeout; i++)); do
        local link_status
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
    local elapsed speed_mbps
    elapsed=$(echo "scale=3; ($t1_ns - $t0_ns) / 1000000000" | bc -l)
    if [ "$(echo "$elapsed < 0.001" | bc -l)" = "1" ]; then
        elapsed="0.001"
    fi
    speed_mbps=$(echo "scale=2; $PAYLOAD / $elapsed / 1048576" | bc -l)

    # 数据验证
    local verify="no" verify_val
    verify_val=$(redis-cli -p "$REDIS_SLAVE_PORT" GET key000001 2>/dev/null || true)
    if [ -n "$verify_val" ] && [ "$verify_val" != "nil" ] && [ "$verify_val" != "(nil)" ]; then
        verify="yes"
    fi

    echo ""
    echo "  ═══════════════════════════════════"
    echo "  $label 全量同步结果"
    echo "  ═══════════════════════════════════"
    echo "  数据量:       ${payload_mb} MB"
    echo "  耗时:         ${elapsed} s"
    echo -e "  吞吐量:       ${GREEN}${speed_mbps} MB/s${NC}"
    echo -e "  数据验证:     $([ "$verify" = "yes" ] && echo -e "${GREEN}通过${NC} (key000001=$verify_val)" || echo -e "${RED}失败${NC} (key000001=$verify_val)")"
    echo ""

    # 断开复制 (保留从机进程供后续测试)
    redis-cli -p "$REDIS_SLAVE_PORT" SLAVEOF NO ONE >/dev/null 2>&1 || true
}

# ================================================================
#  入口
# ================================================================

case "$SCENARIO" in
    kvs)   do_test_kvs ;;
    redis) do_test_redis ;;
    *)     usage ;;
esac
