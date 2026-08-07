#!/bin/bash
# rdma_fullsync_bench.sh — kvstore 全量同步（RDMA verbs）传输速率
#
# 计时: [master] xfer-begin → [master] xfer-end（序列化+传输，不含连接握手）
# 用法: ./test/bench/rdma_fullsync_bench.sh [--keys N] [--vsize B]
set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
KVS="${PROJECT_DIR}/build/kvstore"
LOG_DIR="/tmp/kvs_fs_bench_$$"; mkdir -p "$LOG_DIR"

KEYS=60000; VSIZE=256
IP=192.168.30.135
PORT=$((30000 + $$ % 10000))
TIMEOUT=120

while [[ $# -gt 0 ]]; do
    case "$1" in
        --keys)  KEYS="$2";  shift 2 ;;
        --vsize) VSIZE="$2"; shift 2 ;;
        -h|--help) echo "用法: $0 [--keys N] [--vsize B]"; exit 0 ;;
        *) echo "未知参数: $1"; exit 1 ;;
    esac
done

for t in redis-cli python3 bc; do
    command -v "$t" >/dev/null 2>&1 || { echo "缺少工具: $t"; exit 1; }
done
[ -x "$KVS" ] || { echo "kvstore 未编译: $KVS"; exit 1; }

cleanup() {
    for pid in $(pgrep -x kvstore 2>/dev/null || true); do kill -9 $pid 2>/dev/null; done
    wait 2>/dev/null; sleep 1
}
await() {
    local log=$1 rx=$2 i=0 max=$((TIMEOUT * 10))
    while [ $i -lt $max ]; do
        [ -f "$log" ] && grep -qE "$rx" "$log" 2>/dev/null && return 0
        i=$((i+1)); sleep 0.1
    done
    return 1
}
wait_port() {
    local p=$1
    for ((i=0; i<30; i++)); do redis-cli -p "$p" PING >/dev/null 2>&1 && return 0; sleep 0.3; done
    return 1
}

# ── Phase 1: 灌数据 + SAVE ─────────────────────────────────────────

cleanup
rm -f "${PROJECT_DIR}/build/dump.rdb"

>&2 echo -n "[populate] $KEYS × ${VSIZE}B ... "

cat > "${LOG_DIR}/pop.conf" <<EOF
port $PORT
network proactor
maxclients 100
persistence rdb
rdma_sync off
rdma_role master
rdma_listen_ip $IP
rdma_listen_port $PORT
expire off
EOF

"$KVS" "${LOG_DIR}/pop.conf" > "${LOG_DIR}/pop.log" 2>&1 &
wait_port "$PORT" || { echo "FAIL"; cat "${LOG_DIR}/pop.log"; exit 1; }

python3 -c "
import sys,os; n=$KEYS; pad=os.urandom($VSIZE); pl=len(pad); o=sys.stdout.buffer
for i in range(1,n+1):
    k=f'key:{i:012d}'.encode(); kl=len(k)
    o.write(f'*3\r\n\$3\r\nSET\r\n\${kl}\r\n'.encode()); o.write(k)
    o.write(f'\r\n\${pl}\r\n'.encode()); o.write(pad); o.write(b'\r\n')
" | redis-cli -p "$PORT" --pipe >/dev/null 2>&1

keys=$(redis-cli -p "$PORT" DBSIZE 2>/dev/null || echo 0)
redis-cli -p "$PORT" SAVE >/dev/null 2>&1
for i in $(seq 1 120); do
    rdb=$(stat -c %s "${PROJECT_DIR}/build/dump.rdb" 2>/dev/null || echo 0)
    [ "$rdb" -gt 0 ] 2>/dev/null && break
    sleep 0.5
done
rdb_mb=$(awk "BEGIN { printf \"%.2f\", $rdb/1048576 }")
>&2 echo "$keys keys, $rdb_mb MB"

cleanup

# ── Phase 2: sync ──────────────────────────────────────────────────

cat > "${LOG_DIR}/master.conf" <<EOF
port $PORT
network proactor
maxclients 100
persistence rdb
rdma_sync on
rdma_role master
rdma_listen_ip $IP
rdma_listen_port $PORT
expire off
EOF
cat > "${LOG_DIR}/slave.conf" <<EOF
port 2001
network proactor
maxclients 100
persistence none
rdma_sync on
rdma_role slave
rdma_master_ip $IP
rdma_master_port $PORT
expire off
EOF

mlog="${LOG_DIR}/master.log" slog="${LOG_DIR}/slave.log"

>&2 echo -n "[sync] "

"$KVS" "${LOG_DIR}/master.conf" > "$mlog" 2>&1 &
sleep 3
wait_port "$PORT" || { echo "master FAIL"; cat "$mlog"; exit 1; }

timeout "$TIMEOUT" "$KVS" "${LOG_DIR}/slave.conf" > "$slog" 2>&1 &
await "$mlog" "\[master\] xfer-end:" || true
await "$slog" "\[slave\] xfer-end:" || true

# ── Parse ──────────────────────────────────────────────────────────

m_end=$(grep "\[master\] xfer-end:" "$mlog" 2>/dev/null | tail -1 || true)
s_end=$(grep "\[slave\] xfer-end:" "$slog" 2>/dev/null | tail -1 || true)

m_bytes=$(echo "$m_end" | grep -oP 'bytes=\K[0-9]+'  || echo "?")
m_ms=$(  echo "$m_end" | grep -oP 'ms=\K[0-9.]+'   || echo "?")
m_mbps=$(echo "$m_end" | grep -oP 'mbps=\K[0-9.]+'  || echo "?")
m_bat=$( echo "$m_end" | grep -oP 'batches=\K[0-9]+' || echo "?")
m_mb=$(awk "BEGIN { printf \"%.1f\", $m_bytes/1048576 }")
m_mb=${m_mb:-?}

s_ent=$(echo "$s_end" | grep -oP 'entries=\K[0-9]+' || echo "?")
s_bytes=$(echo "$s_end" | grep -oP 'bytes=\K[0-9]+' || echo "?")
s_ms=$(  echo "$s_end" | grep -oP 'ms=\K[0-9.]+'   || echo "?")
s_mbps=$(echo "$s_end" | grep -oP 'mbps=\K[0-9.]+'  || echo "?")

cleanup

echo ""
echo "  KEYS=$keys  VSIZE=$VSIZE B  RDB=$rdb_mb MB  PORT=$PORT"
echo "  ──────────  ──────────  ──────────  ──────────"
printf "  %-10s %10s %10s %10s\n" "" "DATA_MB" "TIME_MS" "MB/s"
printf "  %-10s %10s %10s %10s\n" "master" "${m_mb}" "${m_ms}" "${m_mbps}"
printf "  %-10s %10s %10s %10s\n" "slave"  "${s_mb:-?}" "${s_ms}" "${s_mbps}"
echo ""
