#!/bin/bash
# =============================================================================
# common.sh — 性能基准测试公共函数库
#
# 用法: source common.sh      # 统一在项目根目录下执行所有 bench_*.sh
#
# 职责:
#   · 日志（纯文本，无颜色）/ 预检 / 速率计算
#   · 临时配置生成（write_kvs_conf / write_redis_conf）
#   · 服务器启动与清理（start_kvs / start_redis / stop_all）
#   · 聚合 QPS 压测（bench_qps）+ 预热（warmup_server）
#   · 数据填充（pipe_populate：awk → redis-cli --pipe，不依赖 python）
#
# 设计约定:
#   · 相对项目根目录使用路径（build/kvstore 等），无 PROJECT_DIR 等派生常量
#   · 无颜色、无比值列、无兼容旧调用的包装函数、无运行时改配置
#   · 清理一律杀干净（pkill kvstore/redis-server/redis-benchmark），不做
#     进程区分；每轮都是独立启动的干净实例
# =============================================================================

set -euo pipefail
shopt -s nullglob

# ---- 基础路径（相对项目根目录）----
KVS_BIN="build/kvstore"
LOG_DIR="/tmp/kvstore_bench_$$"
mkdir -p "$LOG_DIR"

# ---- 默认端口（脚本可按需覆盖）----
KVS_PORT=2000
KVS_SLAVE_PORT=2002
REDIS_PORT=2001

# ---- CPU 核绑定（server / client 分核）----
SERVER_CORES=${SERVER_CORES:-"0,3"}
CLIENT_CORES=${CLIENT_CORES:-"1,2"}
IFS=',' read -r -a CLIENT_CORE_LIST <<< "$CLIENT_CORES"

# ---- 压测默认参数（脚本可按需覆盖）----
NCLIENTS=${NCLIENTS:-2}
OPS=${OPS:-500000}
CONN=${CONN:-50}
DATA=${DATA:-16}
WARMUP_OPS=${WARMUP_OPS:-1000000}

# ---- 日志（纯文本，直接可写入 txt）----
log()  { echo "[$(date +%H:%M:%S)] $*"; }
warn() { echo "[WARN] $(date +%H:%M:%S) $*" >&2; }
err()  { echo "[ERROR] $(date +%H:%M:%S) $*" >&2; }
step() { echo "[STEP] $(date +%H:%M:%S) $*" >&2; }
die()  { err "$@"; exit 1; }

# ---- 速率计算（字节数 / 秒 → MB/s；条目 / 秒）----
_fmt() { local v=$1; [ "${v:0:1}" = "." ] && echo "0$v" || echo "$v"; }
calc_mbps() {
    if [ "$(echo "$2 > 0" | bc 2>/dev/null)" = 1 ]; then
        _fmt "$(echo "scale=2; $1 / $2 / 1048576" | bc)"
    else
        echo "0.00"
    fi
}
calc_entps() {
    if [ "$(echo "$2 > 0" | bc 2>/dev/null)" = 1 ]; then
        echo "scale=0; $1 / $2" | bc
    else
        echo "0"
    fi
}

# ---- 预检 ----
require_root() {
    if [ "${EUID:-0}" -ne 0 ]; then
        err "需要 root 权限，请使用: sudo ./test/bench/$script_name"
        exit 1
    fi
}
require_tools() {
    for tool in redis-cli redis-benchmark bc taskset; do
        command -v "$tool" >/dev/null 2>&1 || { err "缺少工具: $tool"; exit 1; }
    done
    [ -x "$KVS_BIN" ] || { err "kvstore 未编译: $KVS_BIN"; exit 1; }
}

# ---- 端口等待 ----
wait_for_port() {
    local host=$1 port=$2 timeout=${3:-10} i
    for ((i=0; i<timeout*2; i++)); do
        if ss -tln 2>/dev/null | grep -q ":${port} "; then return 0; fi
        sleep 0.5
    done
    return 1
}

# ---- 进程 / 端口清理（一律杀干净）----
stop_all() {
    pkill -x kvstore 2>/dev/null || true
    pkill -x redis-server 2>/dev/null || true
    pkill -x redis-benchmark 2>/dev/null || true
    sleep 0.5
}

# ---- 临时配置生成（统一走临时目录，不依赖 conf/）----
# write_kvs_conf <文件> <port> <network> <persistence> [额外配置行...]
write_kvs_conf() {
    local file=$1 port=$2 net=$3 pers=$4; shift 4
    {
        echo "port ${port}"
        echo "network ${net}"
        echo "persistence ${pers}"
        echo "repl_role off"
        echo "expire off"
        for kv in "$@"; do echo "$kv"; done
    } > "$file"
}
# write_redis_conf <文件> <port> <aof on|off>
write_redis_conf() {
    local file=$1 port=$2 aof=$3
    local aof_yes="no"; [ "$aof" = on ] && aof_yes="yes"
    cat > "$file" <<EOF
port ${port}
bind 127.0.0.1
protected-mode no
save ""
appendonly ${aof_yes}
appendfsync everysec
maxclients 10000
loglevel warning
EOF
}

# ---- 服务器管理（每轮独立启动）----
# start_kvs <绝对conf路径> <port>
start_kvs() {
    local conf=$1 port=$2
    rm -f build/dump.rdb build/dump.rdb.tmp build/appendonly.aof \
          dump.rdb dump.rdb.tmp appendonly.aof
    nohup taskset -c "$SERVER_CORES" "$KVS_BIN" "$conf" > "${LOG_DIR}/kvstore.log" 2>&1 &
    local pid=$!
    sleep 0.2
    if ! kill -0 "$pid" 2>/dev/null; then
        err "kvstore 启动失败 (conf=$conf)"; tail -20 "${LOG_DIR}/kvstore.log"; exit 1
    fi
    if ! wait_for_port 127.0.0.1 "$port" 10; then
        err "kvstore 启动超时 (port=$port)"; tail -20 "${LOG_DIR}/kvstore.log"; exit 1
    fi
    log "kvstore 已启动 (${conf##*/}, pid=$pid, port=$port)"
}

# start_redis <绝对conf路径>
start_redis() {
    local conf=$1 port
    port=$(grep '^port ' "$conf" | awk '{print $2}')
    nohup taskset -c "$SERVER_CORES" redis-server "$conf" > "${LOG_DIR}/redis.log" 2>&1 &
    local pid=$!
    if ! wait_for_port 127.0.0.1 "$port" 10; then
        err "redis 启动失败 (conf=$conf)"; tail -20 "${LOG_DIR}/redis.log"; exit 1
    fi
    redis-cli -p "$port" FLUSHALL >/dev/null 2>&1 || true
    log "redis 已启动 (${conf##*/}, pid=$pid, port=$port)"
}

# ---- 压测 ----
# warmup_server <port> —— SET 预热（不计入 QPS）
warmup_server() {
    taskset -c "$CLIENT_CORES" redis-benchmark -h 127.0.0.1 -p "$1" -t set \
        -n "$WARMUP_OPS" -c 50 -P 100 -d "$DATA" -q >/dev/null 2>&1 || true
}

# bench_qps <port> <test> <P> —— NCLIENTS 并行 redis-benchmark，CSV 第2列求和
bench_qps() {
    local port=$1 t=$2 P=$3 pids=() i q=""
    local cli_to=$((OPS / 10000 + 60))   # P1 下每客户端 ~20K qps 的安全余量
    for i in $(seq 0 $((NCLIENTS - 1))); do
        local core="${CLIENT_CORE_LIST[$((i % ${#CLIENT_CORE_LIST[@]}))]}"
        taskset -c "$core" timeout "$cli_to" \
            redis-benchmark -h 127.0.0.1 -p "$port" -t "$t" -n "$OPS" \
            -c "$CONN" -P "$P" -d "$DATA" --csv 2>/dev/null >"${LOG_DIR}/bench_c$i.csv" &
        pids+=($!)
    done
    for pid in "${pids[@]:-}"; do wait "$pid" 2>/dev/null || true; done
    # 各客户端 CSV 第 2 列 = requests/sec；服务器为瓶颈时求和即服务器 QPS
    q=$(awk -F',' 'FNR>1 {gsub(/"/,"",$2); sum+=$2} END {printf "%.0f", sum}' \
        "${LOG_DIR}"/bench_c*.csv 2>/dev/null || true)
    echo "${q:-0}"
}

# ---- 数据填充：awk 生成 RESP 流 → redis-cli --pipe（不依赖 python）----
# pipe_populate <host> <port> <n> <vsize>
pipe_populate() {
    local host=$1 port=$2 n=$3 vs=$4
    awk -v n="$n" -v vs="$vs" '
        BEGIN {
            for (j = 0; j < vs; j++) v = v sprintf("%c", 65 + (j % 26));  /* ABC.. 填充 */
            for (i = 0; i < n; i++) {
                k = sprintf("key:%012d", i);
                printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(k), k, vs, v;
            }
        }' | timeout 300 redis-cli -h "$host" -p "$port" --pipe >/dev/null 2>&1
}
