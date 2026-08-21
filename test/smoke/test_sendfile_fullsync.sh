#!/usr/bin/env bash
# ============================================================================
# kvstore sendfile 全量同步 smoke test（大流量：>10000 key）
# ============================================================================
# 专测 Bug: sendfile 全量同步下 slave 能收到数据但 load 不到（验证显示无数据）。
#
# 拓扑（单机 127.0.0.1，无需 RDMA 网卡 / SoftRoCE）：
#   Master  port=MASTER_PORT  fullsync_transport sendfile  rdma_listen_port=FULLSYNC_PORT
#   Slave   port=SLAVE_PORT   fullsync_transport sendfile  rdma_master_port=FULLSYNC_PORT
#
#   握手:  Slave → Master:MASTER_PORT 发 `FULLSYNC` (inline)
#   数据:  Master fork 子进程监听 FULLSYNC_PORT+1，rdb_save_sync() 落盘后 sendfile 推送
#
# 流程: 起 Master → kv_resp.py fill 灌 N 条 → 起 Slave 触发 FULLSYNC → 校验 Slave
#
# Usage:
#   ./test/smoke/test_sendfile_fullsync.sh [master_port] [slave_port] [fullsync_port]
#   N=15000 MASTER_PORT=7701 SLAVE_PORT=7702 FULLSYNC_PORT=7710 ./test/smoke/test_sendfile_fullsync.sh
#
# Prerequisites: build/kvstore 已编译、python3（无需 redis-cli / root）
# ============================================================================
set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ── Configuration ────────────────────────────────────────────────────────────
MASTER_PORT="${1:-${MASTER_PORT:-7701}}"
SLAVE_PORT="${2:-${SLAVE_PORT:-7702}}"
FULLSYNC_PORT="${3:-${FULLSYNC_PORT:-7710}}"   # sendfile 数据走 FULLSYNC_PORT+1
KVSTORE_BIN="${KVSTORE_BIN:-${ROOT_DIR}/build/kvstore}"
HOST="${HOST:-127.0.0.1}"
N="${N:-15000}"                                # 数据量，>10000
NETWORK="${NETWORK:-reactor}"                  # 网络引擎: reactor | proactor

MASTER_CONF=$(mktemp)
SLAVE_CONF=$(mktemp)
MASTER_LOG=$(mktemp)
SLAVE_LOG=$(mktemp)

MASTER_PID=""
SLAVE_PID=""

PASS=0; FAIL=0
RED=$'\033[31m' GRN=$'\033[32m' YEL=$'\033[33m' RST=$'\033[0m'

ok()   { PASS=$((PASS+1)); printf "  ${GRN}PASS${RST}  %s\n" "$1"; }
bad()  { FAIL=$((FAIL+1)); printf "  ${RED}FAIL${RST}  %s\n        expected: %s\n        actual:   %s\n" "$1" "$2" "$3"; }
section() { printf "\n${YEL}── %s ──${RST}\n" "$1"; }

# ── Cleanup ──────────────────────────────────────────────────────────────────
cleanup() {
    [ -n "$MASTER_PID" ] && kill "$MASTER_PID" 2>/dev/null || true
    [ -n "$SLAVE_PID" ]  && kill "$SLAVE_PID" 2>/dev/null || true
    for p in $MASTER_PORT $SLAVE_PORT $FULLSYNC_PORT $((FULLSYNC_PORT+1)); do
        pid=$(lsof -ti ":$p" 2>/dev/null) && [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    rm -f "$MASTER_CONF" "$SLAVE_CONF" "$MASTER_LOG" "$SLAVE_LOG"
    # 清理本次 SAVE 出的 RDB，避免污染下次运行
    rm -f "$ROOT_DIR/build/dump.rdb" "$ROOT_DIR/build/dump.rdb.tmp"
}
trap cleanup EXIT INT TERM

# ── Helpers ──────────────────────────────────────────────────────────────────
wait_ping() {
    local port=$1 timeout=${2:-30}
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

# ── Prerequisites ────────────────────────────────────────────────────────────
section "Prerequisites"

[ -x "$KVSTORE_BIN" ] || { printf "${RED}FATAL${RST}: kvstore binary not found at %s\n" "$KVSTORE_BIN" >&2; exit 2; }
ok "kvstore binary"

command -v python3 >/dev/null 2>&1 || { printf "${RED}FATAL${RST}: python3 not found\n" >&2; exit 2; }
ok "python3"

# 干净起点
rm -f "$ROOT_DIR/build/dump.rdb" "$ROOT_DIR/build/dump.rdb.tmp"

# ── 1. Start master ──────────────────────────────────────────────────────────
section "Start master (fullsync_transport=sendfile, rdma_sync on)"

cat > "$MASTER_CONF" << EOF
port ${MASTER_PORT}
network ${NETWORK}
maxclients 200
loglevel info
persistence rdb
expire off
rdma_sync on
rdma_role master
fullsync_transport sendfile
rdma_listen_ip ${HOST}
rdma_listen_port ${FULLSYNC_PORT}
incr_sync off
xdp_sync off
EOF

"$KVSTORE_BIN" "$MASTER_CONF" &>"$MASTER_LOG" &
MASTER_PID=$!
if ! wait_ping "$MASTER_PORT" 20; then
    printf "${RED}FATAL${RST}: master failed to start\n" >&2
    cat "$MASTER_LOG" >&2
    exit 2
fi
ok "master started (pid=$MASTER_PID, port=$MASTER_PORT, fullsync=$FULLSYNC_PORT)"

# ── 2. Fill master ───────────────────────────────────────────────────────────
section "Fill master with ${N} keys (RESP)"

if python3 "$SCRIPT_DIR/kv_resp.py" fill --host "$HOST" --port "$MASTER_PORT" --n "$N" --prefix key; then
    ok "master filled ${N} keys"
else
    printf "${RED}FATAL${RST}: fill master failed\n" >&2
    exit 2
fi

# ── 3. Start slave (triggers FULLSYNC → sendfile) ────────────────────────────
section "Start slave (fullsync_transport=sendfile, rdma_sync on)"

cat > "$SLAVE_CONF" << EOF
port ${SLAVE_PORT}
network ${NETWORK}
maxclients 200
loglevel info
persistence none
expire off
rdma_sync on
rdma_role slave
fullsync_transport sendfile
master_port ${MASTER_PORT}
rdma_master_ip ${HOST}
rdma_master_port ${FULLSYNC_PORT}
incr_sync off
xdp_sync off
EOF

"$KVSTORE_BIN" "$SLAVE_CONF" &>"$SLAVE_LOG" &
SLAVE_PID=$!

# slave 在 inline 全量同步完成后才进入事件循环开始监听，故 wait_ping 即等待全量同步结束。
if ! wait_ping "$SLAVE_PORT" 60; then
    printf "${RED}FATAL${RST}: slave failed to become ready after fullsync\n" >&2
    echo "--- slave log ---"; cat "$SLAVE_LOG" >&2
    echo "--- master log ---"; cat "$MASTER_LOG" >&2
    exit 2
fi
ok "slave started & fullsync handshake done (pid=$SLAVE_PID, port=$SLAVE_PORT)"

# ── 4. Verify slave data ─────────────────────────────────────────────────────
section "Verify slave has ${N} keys"

if python3 "$SCRIPT_DIR/kv_resp.py" verify --host "$HOST" --port "$SLAVE_PORT" \
     --n "$N" --prefix key --wait 120 --expect-dbsize "$N"; then
    ok "slave verified ${N} keys (dbsize=${N})"
else
    bad "slave data load" "${N} keys" "verify failed (see diagnostics below)"
fi

# ── 5. Diagnostics ───────────────────────────────────────────────────────────
section "Diagnostics (sendfile fullsync logs)"

echo "  ${YEL}── master ${RST}──"
grep -E '\[tcp-master\]|\[RDB\]|xfer-end|saved via' "$MASTER_LOG" 2>/dev/null || echo "  (no master sync lines)"
echo "  ${YEL}── slave ${RST}──"
grep -E '\[tcp-slave\]|\[RDB\]|loaded|stream begin|buf load' "$SLAVE_LOG" 2>/dev/null || echo "  (no slave sync lines)"

# ── Summary ──────────────────────────────────────────────────────────────────
printf "\n────────────────────────────────────────────────────────\n"
if [ "$FAIL" -eq 0 ]; then
    printf "SENDFILE FULLSYNC: ${GRN}ALL PASS${RST} (%d checks)\n" "$PASS"
    exit 0
else
    printf "SENDFILE FULLSYNC: ${RED}%d FAILED${RST}\n" "$FAIL"
    printf "  slave log: %s\n  master log: %s\n" "$SLAVE_LOG" "$MASTER_LOG"
    exit 1
fi
