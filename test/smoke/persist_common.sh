#!/bin/bash
# =============================================================================
# persist_common.sh — AOF/RDB 持久化重启恢复功能测试公共库
#
# 提供: 进程/端口管理、RESP 数据填充、全量校验、BGSAVE 落盘等待。
# 用法: source "$(dirname "${BASH_SOURCE[0]}")/persist_common.sh"
# 依赖: bash + python3 + 已编译的 build/kvstore (无需 redis-cli / root)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
KVS_BIN="${PROJECT_DIR}/build/kvstore"
CONF_DIR="${PROJECT_DIR}/conf"
LOG_DIR="$(mktemp -d -t kvstore_persist.XXXXXX)"

# 持久化文件路径 (与 src/persist/persistence.h 中的相对路径一致，基于项目根)
RDB_FILE="${PROJECT_DIR}/build/dump.rdb"
RDB_TMP="${PROJECT_DIR}/build/dump.rdb.tmp"
AOF_FILE="${PROJECT_DIR}/build/appendonly.aof"

HOST="${HOST:-127.0.0.1}"
PORT=""           # start_kvs 填充
KVS_PID=""
DEFAULT_N=12000

# ---- 颜色 ----
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

log()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*" >&2; }
step() { echo -e "${CYAN}[STEP]${NC}  $*"; }

# ---- 预检 ----
require_kvs() {
    [ -x "$KVS_BIN" ] || { err "kvstore 未编译: $KVS_BIN (先 cmake --build build)"; exit 1; }
    command -v python3 >/dev/null 2>&1 || { err "缺少 python3"; exit 1; }
}

# 从 conf 提取 port (默认 2000)
conf_port() {
    local v
    v="$(grep -E '^[[:space:]]*port[[:space:]]' "$1" | awk '{print $2}' | head -1)"
    echo "${v:-2000}"
}

# 清理旧持久化文件 (确保干净起点)
clean_persist_files() {
    rm -f "$RDB_FILE" "$RDB_TMP" "$AOF_FILE"
}

# 等待 kvstore 响应 PING (真正就绪，而非仅端口监听)
wait_ping() {
    local port=$1 timeout=${2:-20}
    local i
    for ((i = 0; i < timeout * 2; i++)); do
        if python3 - "$port" <<'PY' 2>/dev/null
import socket, sys
p = int(sys.argv[1])
try:
    s = socket.create_connection(('127.0.0.1', p), timeout=1)
    s.sendall(b'*1\r\n$4\r\nPING\r\n')
    s.settimeout(1)
    if b'PONG' in s.recv(128):
        sys.exit(0)
except Exception:
    pass
sys.exit(1)
PY
        then return 0; fi
        sleep 0.5
    done
    return 1
}

# 启动 kvstore。注意: 不清理持久化文件——清理由调用方显式执行，
# 以便「重启」场景保留上一次落盘的数据。
start_kvs() {
    local conf="$1"
    local conf_path="${CONF_DIR}/${conf}"
    [ -f "$conf_path" ] || { err "配置文件不存在: $conf_path"; exit 1; }

    # 确保无残留 kvstore 进程，避免端口冲突
    pkill -x kvstore 2>/dev/null || true
    sleep 0.3

    local port
    port="$(conf_port "$conf_path")"
    PORT=$port

    nohup "$KVS_BIN" "$conf_path" > "${LOG_DIR}/kvstore_${conf}.log" 2>&1 &
    KVS_PID=$!
    sleep 0.3

    if ! kill -0 "$KVS_PID" 2>/dev/null; then
        err "kvstore 启动失败 (conf=$conf)"
        tail -20 "${LOG_DIR}/kvstore_${conf}.log"
        exit 1
    fi
    if ! wait_ping "$port" 20; then
        err "kvstore 未就绪 (conf=$conf port=$port)"
        tail -20 "${LOG_DIR}/kvstore_${conf}.log"
        kill "$KVS_PID" 2>/dev/null || true
        exit 1
    fi
    log "kvstore 已启动 (conf=$conf pid=$KVS_PID port=$port)"
}

# 优雅停 (SIGTERM → 触发 kvs_aof_persist_stop() flush+fsync)。
# 超时未退判失败并 KILL 兜底清理 (此时持久化不可信)。
stop_kvs_graceful() {
    local pid="${1:-$KVS_PID}"
    [ -n "$pid" ] || return 0
    if ! kill -0 "$pid" 2>/dev/null; then KVS_PID=""; return 0; fi

    kill -TERM "$pid" 2>/dev/null || true
    local i
    for ((i = 0; i < 40; i++)); do
        if ! kill -0 "$pid" 2>/dev/null; then
            KVS_PID=""
            log "kvstore 已优雅退出 (pid=$pid)"
            return 0
        fi
        sleep 0.25
    done
    err "kvstore SIGTERM 后 10s 仍未退出 (pid=$pid) — 持久化可能未 flush"
    kill -KILL "$pid" 2>/dev/null || true
    KVS_PID=""
    return 1
}

# ---- RESP 数据填充 ----
# 用 RESP 数组格式填充 N 条 key:<i>=value:<i> (流水线，逐条确认 +OK)。
# 必须用 RESP 而非 redis-cli --pipe 的 inline 格式: AOF 重放走 resp_parse_slices，
# 只认 *N\r\n$L\r\n 数组形式。
# 输出: "OK <count>" (成功) 或 "FAIL <reason>" (失败)
populate_resp() {
    local port=$1 n=$2
    python3 "$SCRIPT_DIR/kv_resp.py" fill --port "$port" --n "$n"
}

# ---- RESP 全量校验 ----
# 校验: DBSIZE == N 且逐 key GET 值与预期完全一致。
# 输出: "OK verified=<n>" (成功) 或 "FAIL <reason>" (失败)
verify_resp() {
    local port=$1 n=$2
    python3 "$SCRIPT_DIR/kv_resp.py" verify --port "$port" --n "$n" \
        --once --expect-dbsize "$n"
}

# ---- BGSAVE (RDB 异步落盘) ----
bgsave() {
    python3 - "$PORT" <<'PY'
import socket, sys
p = int(sys.argv[1])
s = socket.create_connection(('127.0.0.1', p), timeout=5)
s.sendall(b'*1\r\n$6\r\nBGSAVE\r\n')
s.settimeout(5)
print(s.recv(128).decode(errors='replace').strip())
s.close()
PY
}

# 等待 RDB 文件生成 (BGSAVE 子进程 rename 是最后一步，即落盘完成)
wait_rdb_file() {
    local timeout=${1:-30}
    local i
    for ((i = 0; i < timeout * 10; i++)); do
        if [ -f "$RDB_FILE" ] && [ -s "$RDB_FILE" ]; then return 0; fi
        sleep 0.1
    done
    return 1
}
