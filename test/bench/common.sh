#!/bin/bash
# =============================================================================
# common.sh — 性能基准测试公共函数库
#
# 用法: source test/bench/common.sh
# =============================================================================

set -euo pipefail

# ---- 路径常量 ----
PROJECT_DIR="/home/xxf/9.1-kvstore"
KVS_BIN="${PROJECT_DIR}/build/kvstore"
CONF_DIR="${PROJECT_DIR}/conf/bench"
LOG_DIR="/tmp/kvstore_bench"

# ---- 默认端口 ----
KVS_PORT=2000
KVS_SLAVE_PORT=2002
REDIS_PORT=6379
MASTER_RDMA_PORT=2100

# ---- 结果输出 ----
RESULT_FILE="${PROJECT_DIR}/test/bench/results.txt"
RESULT_CSV="${PROJECT_DIR}/test/bench/results.csv"

# ---- 颜色 ----
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

log()  { echo -e "${GREEN}[INFO]${NC}  $(date +%H:%M:%S) $*" >&2; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $(date +%H:%M:%S) $*" >&2; }
err()  { echo -e "${RED}[ERROR]${NC} $(date +%H:%M:%S) $*" >&2; }
step() { echo -e "${CYAN}[STEP]${NC}  $(date +%H:%M:%S) $*" >&2; }

# ---- 预检 ----
require_root() {
    if [ "${EUID:-0}" -ne 0 ]; then
        err "需要 root 权限，请使用: sudo ./test/bench/$script_name"
        exit 1
    fi
}

require_tools() {
    for tool in redis-cli redis-benchmark nc bc; do
        if ! command -v "$tool" >/dev/null 2>&1; then
            err "缺少工具: $tool"
            exit 1
        fi
    done
    if [ ! -x "$KVS_BIN" ]; then
        err "kvstore 未编译: $KVS_BIN"
        exit 1
    fi
    mkdir -p "$LOG_DIR"
}

# ---- 进程/端口管理 ----
wait_for_port() {
    local host=$1 port=$2 timeout=${3:-10}
    for ((i=0; i<timeout*2; i++)); do
        if ss -tlnp 2>/dev/null | grep -q ":${port} "; then return 0; fi
        sleep 0.5
    done
    return 1
}

stop_kvs() {
    # 先通过 pid 文件杀
    local pidfile="${LOG_DIR}/kvstore.pid"
    if [ -f "$pidfile" ]; then
        local pid=$(cat "$pidfile")
        kill "$pid" 2>/dev/null || true
        rm -f "$pidfile"
    fi
    # 杀所有残留 kvstore 进程（排除本脚本自身）
    for sig in TERM KILL; do
        for pid in $(pgrep -x "kvstore" 2>/dev/null || true); do
            kill -$sig "$pid" 2>/dev/null || true
        done
        sleep 0.3
    done
    sleep 0.5
    # 确保端口释放
    local port
    for port in $KVS_PORT $KVS_SLAVE_PORT $MASTER_RDMA_PORT; do
        if ss -tlnp 2>/dev/null | grep -q ":${port} "; then
            fuser -k ${port}/tcp 2>/dev/null || true
        fi
    done
    # 等待端口完全释放，避免随后的启动立即命中 EADDRINUSE。
    for ((i=0; i<20; i++)); do
        if ! ss -tlnp 2>/dev/null | grep -qE ":(${KVS_PORT}|${KVS_SLAVE_PORT}|${MASTER_RDMA_PORT}|6382) "; then
            break
        fi
        sleep 0.1
    done
    sleep 0.3
}

start_kvs() {
    local conf=$1
    local conf_path="${CONF_DIR}/${conf}"
    if [ ! -f "$conf_path" ]; then
        err "配置文件不存在: $conf_path"
        exit 1
    fi
    stop_kvs
    # 清理旧持久化文件
    rm -f "${PROJECT_DIR}/build/dump.rdb" "${PROJECT_DIR}/build/dump.rdb.tmp" \
          "${PROJECT_DIR}/build/appendonly.aof" "${PROJECT_DIR}/build/incr.wal"
    # 提取配置中 port 的值
    local port=$(grep '^port ' "$conf_path" | awk '{print $2}')
    port=${port:-$KVS_PORT}
    nohup "$KVS_BIN" "$conf_path" > "${LOG_DIR}/kvstore_${conf}.log" 2>&1 &
    local pid=$!
    sleep 0.2
    if ! kill -0 "$pid" 2>/dev/null; then
        err "kvstore 启动失败 (conf=$conf, pid=$pid)"
        tail -20 "${LOG_DIR}/kvstore_${conf}.log"
        exit 1
    fi
    echo "$pid" > "${LOG_DIR}/kvstore.pid"
    if ! wait_for_port 127.0.0.1 "$port" 10; then
        err "kvstore 启动超时 ($conf, port=$port)"
        tail -20 "${LOG_DIR}/kvstore_${conf}.log"
        exit 1
    fi
    if ! ss -tnlp 2>/dev/null | grep -q ":${port} .*pid=${pid}"; then
        err "kvstore 未绑定到端口 ${port} (conf=$conf, pid=$pid)"
        tail -20 "${LOG_DIR}/kvstore_${conf}.log"
        kill "$pid" 2>/dev/null || true
        exit 1
    fi
    log "kvstore 已启动 (conf=$conf, pid=$pid, port=$port)"
}

start_kvs_master() {
    start_kvs "kvstore-master-repl.conf"
    # 等待 RDMA 端口就绪
    if ! wait_for_port 0.0.0.0 $MASTER_RDMA_PORT 5; then
        warn "RDMA 端口 $MASTER_RDMA_PORT 未监听（可能 rdma_sync 未开启）"
    fi
}

start_kvs_slave() {
    local conf_path="${CONF_DIR}/kvstore-slave-repl.conf"
    if [ ! -f "$conf_path" ]; then
        err "配置文件不存在: $conf_path"
        exit 1
    fi
    # 不调 stop_kvs，因为 master 还在跑
    rm -f "${PROJECT_DIR}/build/dump.rdb" "${PROJECT_DIR}/build/dump.rdb.tmp" \
          "${PROJECT_DIR}/build/appendonly.aof"
    nohup "$KVS_BIN" "$conf_path" > "${LOG_DIR}/kvstore_slave.log" 2>&1 &
    local pid=$!
    echo "$pid" > "${LOG_DIR}/kvstore_slave.pid"
    if ! wait_for_port 127.0.0.1 $KVS_SLAVE_PORT 10; then
        err "kvstore slave 启动超时"
        tail -20 "${LOG_DIR}/kvstore_slave.log"
        exit 1
    fi
    log "kvstore slave 已启动 (pid=$pid, port=$KVS_SLAVE_PORT)"
}

# ---- Redis 管理 ----
start_redis() {
    if systemctl is-active --quiet redis-server 2>/dev/null; then
        systemctl stop redis-server 2>/dev/null || true
    fi
    # 清理持久化文件
    rm -f /var/lib/redis/dump.rdb /var/lib/redis/appendonly.aof
    systemctl start redis-server
    sleep 1
    redis-cli -p $REDIS_PORT PING >/dev/null 2>&1 || { err "Redis 启动失败"; exit 1; }
    redis-cli -p $REDIS_PORT FLUSHALL >/dev/null
    log "Redis 已启动 (port=$REDIS_PORT)"
}

redis_configure() {
    local mode=$1  # none | aof
    case "$mode" in
        none)
            redis-cli -p $REDIS_PORT CONFIG SET save "" >/dev/null
            redis-cli -p $REDIS_PORT CONFIG SET appendonly no >/dev/null
            log "Redis: 持久化已关闭"
            ;;
        aof)
            redis-cli -p $REDIS_PORT CONFIG SET appendonly yes >/dev/null
            redis-cli -p $REDIS_PORT CONFIG SET appendfsync everysec >/dev/null
            log "Redis: AOF 已开启 (appendfsync everysec)"
            ;;
        *)
            err "未知 Redis 模式: $mode"
            exit 1
            ;;
    esac
    redis-cli -p $REDIS_PORT FLUSHALL >/dev/null
}

stop_redis() {
    systemctl stop redis-server 2>/dev/null || true
    sleep 0.5
}

# ---- 数据填充 ----
populate() {
    local host=$1 port=$2 n=$3
    local batch_size=10000
    log "填充 ${n} 条数据到 ${host}:${port}..."
    local remaining=$n
    local sent=0
    while [ $remaining -gt 0 ]; do
        local batch=$batch_size
        [ $batch -gt $remaining ] && batch=$remaining
        for i in $(seq $((sent+1)) $((sent+batch))); do
            printf 'SET key%06d benchmark_value_\r\n' $i
        done | timeout 300 redis-cli -h "$host" -p "$port" --pipe > /dev/null 2>&1
        sent=$((sent + batch))
        remaining=$((remaining - batch))
        echo -n "."
    done
    echo ""
    log "填充完成: ${n} 条"
}

# ---- redis-benchmark ----
run_bench() {
    local host=$1 port=$2 test=$3 n=${4:-1000000} c=${5:-50} P=${6:-20} d=${7:-3}
    local csv_line
    csv_line=$(redis-benchmark -h "$host" -p "$port" -t "$test" \
        -n "$n" -c "$c" -P "$P" -d "$d" --csv 2>/dev/null | tail -1)
    # 解析 CSV: "test","rps","..."
    local qps=$(echo "$csv_line" | cut -d',' -f2 | tr -d '"')
    echo "${qps:-0}"
}

run_bench_quiet() {
    run_bench "$@" 2>/dev/null
}

# 带随机 key 的压测（-r 参数，更接近真实场景）
run_bench_r() {
    local host=$1 port=$2 test=$3 n=${4:-1000000} c=${5:-50} P=${6:-20} d=${7:-3} r=${8:-1000000}
    local csv_line
    csv_line=$(redis-benchmark -h "$host" -p "$port" -t "$test" \
        -n "$n" -c "$c" -P "$P" -d "$d" -r "$r" --csv 2>/dev/null | tail -1)
    local qps=$(echo "$csv_line" | cut -d',' -f2 | tr -d '"')
    echo "${qps:-0}"
}

# ---- 内存采样 ----
sample_memory() {
    local pid=$1
    if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
        echo "0 0"
        return
    fi
    local status="/proc/$pid/status"
    local vmsize=$(grep VmSize "$status" 2>/dev/null | awk '{print $2}')
    local vmrss=$(grep VmRSS  "$status" 2>/dev/null | awk '{print $2}')
    echo "${vmsize:-0} ${vmrss:-0}"
}

# ---- 结果记录 ----
init_results() {
    echo "# KVStore Performance Benchmark Results — $(date '+%Y-%m-%d %H:%M:%S')" > "$RESULT_FILE"
    echo "test,scenario,qps" > "$RESULT_CSV"
}

record_result() {
    local test=$1 scenario=$2 qps=$3
    echo "${test} | ${scenario}: ${qps} req/s" >> "$RESULT_FILE"
    echo "${test},${scenario},${qps}" >> "$RESULT_CSV"
    echo -e "  ${GREEN}→ ${qps} req/s${NC}  (${scenario})"
}

record_raw() {
    echo "$@" >> "$RESULT_FILE"
}
