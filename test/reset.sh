#!/bin/bash
# =============================================================================
# reset.sh  —  彻底重置 KV Store 和 Redis 到原始状态
#
# 用途:
#   在对 KV Store 和 Redis 做性能对比测试之前，清除所有数据和持久化文件，
#   重启两个服务，确保每次测试都从完全相同的初始状态开始。
#
# 用法:
#   sudo ./test/reset.sh
#
# 为什么需要 sudo:
#   - Redis 通过 systemd 管理，启停需要 root
#   - KV Store 使用 proactor (io_uring) 需要 CAP_SYS_IO_URING
#
# 完成后可运行 redis-benchmark 进行性能对比:
#   redis-benchmark -h 127.0.0.1 -p 2000 -t set,get -n 100000 -q
#   redis-benchmark -h 127.0.0.1 -p 6379 -t set,get -n 100000 -q
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
PROJECT_DIR="/home/xxf/9.1-kvstore"
KVS_BIN="${PROJECT_DIR}/build/kvstore"
KVS_CONF="${PROJECT_DIR}/conf/kvstore-master.conf"
KVS_BUILD_DIR="${PROJECT_DIR}/build"
KVS_PORT=2000
KVS_SLAVE_PORT=2002
RDMA_PORT=2100
INCR_PORT=6382
KVS_HOST="127.0.0.1"

REDIS_PORT=6379
REDIS_HOST="127.0.0.1"
REDIS_DATA_DIR="/var/lib/redis"

# ---------------------------------------------------------------------------
# 颜色输出
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; }
info() { echo -e "${CYAN}[STEP]${NC}  $*"; }

# ---------------------------------------------------------------------------
# 预检
# ---------------------------------------------------------------------------
if [ "${EUID:-0}" -ne 0 ]; then
    err "请用 sudo 运行:  sudo ./test/reset.sh"
    exit 1
fi

if [ ! -x "$KVS_BIN" ]; then
    err "KV Store 二进制不存在: $KVS_BIN"
    err "请先编译: cd build && cmake .. && make"
    exit 1
fi

if [ ! -f "$KVS_CONF" ]; then
    err "配置文件不存在: $KVS_CONF"
    exit 1
fi

if ! command -v redis-cli >/dev/null 2>&1; then
    err "redis-cli 未安装"
    exit 1
fi

if ! command -v nc >/dev/null 2>&1; then
    err "netcat (nc) 未安装"
    exit 1
fi

echo ""
echo "============================================"
echo "  KV Store & Redis — 彻底重置"
echo "============================================"
echo ""

# ---------------------------------------------------------------------------
# 1. 停止 KV Store
# ---------------------------------------------------------------------------
info "1/8  停止 KV Store..."
# 先结束所有 kvstore 进程（匹配多种启动路径）
KILLED=0
for sig in TERM KILL; do
    for pid in $(pgrep -f "kvstore" 2>/dev/null || true); do
        kill -$sig "$pid" 2>/dev/null || true
        KILLED=1
    done
    [ $KILLED -eq 1 ] && sleep 1
done
if [ $KILLED -eq 1 ]; then
    log "KV Store 已停止"
else
    warn "KV Store 之前未在运行"
fi

# 确保所有相关端口释放 (client + slave + RDMA + incr)
for port in $KVS_PORT $KVS_SLAVE_PORT $RDMA_PORT $INCR_PORT; do
    if ss -tlnp 2>/dev/null | grep -q ":${port} "; then
        warn "端口 $port 仍被占用，强制释放..."
        fuser -k ${port}/tcp 2>/dev/null || true
        sleep 0.5
    fi
done

# ---------------------------------------------------------------------------
# 2. 停止 Redis
# ---------------------------------------------------------------------------
info "2/8  停止 Redis..."
if systemctl is-active --quiet redis-server 2>/dev/null; then
    systemctl stop redis-server
    sleep 1
    log "Redis 已停止"
else
    warn "Redis 之前未在运行"
fi

# 确保端口释放
if ss -tlnp | grep -q ":${REDIS_PORT} "; then
    warn "端口 $REDIS_PORT 仍被占用，强制释放..."
    fuser -k ${REDIS_PORT}/tcp 2>/dev/null || true
    sleep 1
fi

# ---------------------------------------------------------------------------
# 3. 删除 KV Store 持久化文件
# ---------------------------------------------------------------------------
info "3/8  清理 KV Store 持久化文件..."
KVS_FILES=(
    "${KVS_BUILD_DIR}/dump.rdb"
    "${KVS_BUILD_DIR}/dump.rdb.tmp"
    "${KVS_BUILD_DIR}/appendonly.aof"
)
cleaned=0
for f in "${KVS_FILES[@]}"; do
    if [ -f "$f" ]; then
        rm -f "$f"
        echo "       已删除: $f"
        cleaned=1
    fi
done
if [ $cleaned -eq 0 ]; then
    echo "       无需清理（文件不存在）"
fi

# ---------------------------------------------------------------------------
# 4. 删除 Redis 持久化文件
# ---------------------------------------------------------------------------
info "4/8  清理 Redis 持久化文件..."
REDIS_FILES=(
    "${REDIS_DATA_DIR}/dump.rdb"
    "${REDIS_DATA_DIR}/appendonly.aof"
)
cleaned=0
for f in "${REDIS_FILES[@]}"; do
    if [ -f "$f" ]; then
        rm -f "$f"
        echo "       已删除: $f"
        cleaned=1
    fi
done
if [ $cleaned -eq 0 ]; then
    echo "       无需清理（文件不存在）"
fi

# ---------------------------------------------------------------------------
# 5. 启动 Redis
# ---------------------------------------------------------------------------
info "5/8  启动 Redis..."
systemctl start redis-server
sleep 1

# 验证 Redis 启动
if redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" PING >/dev/null 2>&1; then
    log "Redis 已启动 (${REDIS_HOST}:${REDIS_PORT})"
else
    err "Redis 启动失败"
    exit 1
fi

# ---------------------------------------------------------------------------
# 6. 清空 Redis
# ---------------------------------------------------------------------------
info "6/8  清空 Redis 数据..."
redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" FLUSHALL >/dev/null
log "Redis FLUSHALL 完成"

# ---------------------------------------------------------------------------
# 7. 启动 KV Store
# ---------------------------------------------------------------------------
info "7/8  启动 KV Store (proactor, persistence=none)..."
cd "$PROJECT_DIR"
nohup "$KVS_BIN" "$KVS_CONF" > /tmp/kvstore.log 2>&1 &
KVS_PID=$!
sleep 2

# 验证 KV Store 启动（进程是否存活）
if ! kill -0 "$KVS_PID" 2>/dev/null; then
    err "KV Store 进程启动后立即退出，查看日志:"
    tail -20 /tmp/kvstore.log
    exit 1
fi

# 验证端口是否在监听
if ! ss -tlnp | grep -q ":${KVS_PORT} "; then
    err "KV Store 未在端口 ${KVS_PORT} 上监听"
    exit 1
fi

# 验证能否响应 PING
if echo -ne "*1\r\n\$4\r\nPING\r\n" | nc -w2 "$KVS_HOST" "$KVS_PORT" >/dev/null 2>&1; then
    log "KV Store 已启动 (${KVS_HOST}:${KVS_PORT}, pid=${KVS_PID})"
else
    err "KV Store PING 无响应"
    exit 1
fi

# ---------------------------------------------------------------------------
# 8. 清空 KV Store
# ---------------------------------------------------------------------------
info "8/8  清空 KV Store 数据..."
echo -ne "*1\r\n\$8\r\nFLUSHALL\r\n" | nc -w2 "$KVS_HOST" "$KVS_PORT" >/dev/null 2>&1
log "KV Store FLUSHALL 完成"

# ---------------------------------------------------------------------------
# 最终验证：确认两边都是空的
# ---------------------------------------------------------------------------
echo ""
echo "============================================"
echo "  验证: 确认两边均无残留数据"
echo "============================================"

# Redis DBSIZE
REDIS_SIZE=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" DBSIZE 2>/dev/null)
echo "  Redis DBSIZE:    ${REDIS_SIZE}"

# KV Store: 用 SET 后 GET 来验证空状态（没有 DBSIZE 命令，用 EXIST 探测典型 key）
KVS_EXIST=$(echo -ne "*2\r\n\$5\r\nEXIST\r\n\$7\r\nstrkey1\r\n" | nc -w2 "$KVS_HOST" "$KVS_PORT" 2>/dev/null | head -c 10)
echo "  KV Store EXIST:  ${KVS_EXIST} (:-1 表示空)"

# 持久化文件确认
echo ""
echo "  持久化文件状态:"
for f in "${KVS_FILES[@]}" "${REDIS_FILES[@]}"; do
    if [ -f "$f" ]; then
        echo "    [EXISTS] $f ($(stat -c%s "$f") bytes)"
    else
        echo "    [CLEAN]  $f"
    fi
done

# ---------------------------------------------------------------------------
# 完成
# ---------------------------------------------------------------------------
echo ""
echo "============================================"
echo "  重置完成 — 两个服务均处于原始状态"
echo "============================================"
echo ""
echo "  KV Store : ${KVS_HOST}:${KVS_PORT}  (proactor, persistence=none)"
echo "  Redis    : ${REDIS_HOST}:${REDIS_PORT}"
echo ""
echo "  ── redis-benchmark 性能对比命令 ──"
echo ""
echo "  # 测试 KV Store"
echo "  redis-benchmark -h ${KVS_HOST} -p ${KVS_PORT} -t set,get -n 100000 -q"
echo ""
echo "  # 测试 Redis"
echo "  redis-benchmark -h ${REDIS_HOST} -p ${REDIS_PORT} -t set,get -n 100000 -q"
echo ""
echo "  # 完整对比测试（常用组合）"
echo "  redis-benchmark -h ${KVS_HOST} -p ${KVS_PORT} -t set,get,lpush,lrange -n 100000 -q --csv"
echo "  redis-benchmark -h ${REDIS_HOST} -p ${REDIS_PORT} -t set,get,lpush,lrange -n 100000 -q --csv"
echo ""
