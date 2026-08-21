#!/usr/bin/env bash
# ============================================================================
# kvstore 内置 epoll 直接 TCP 增量同步 smoke test（大流量：>10000 key）
# ============================================================================
# 专测内置 repl-incr 模块（PSYNC 握手 + 1MB backlog + 命令流回放），
# 走 `network reactor`(epoll)，不经全量同步、不经 XDP/KCP 网关。
#
# 流程: 起 Master(空) → 灌 1 条 seed 使 offset 0 有效 → 起 Slave(PSYNC 0 → +CONTINUE)
#        → 向 Master 灌 N 条 → Slave 实时追平 → 校验 Slave 有 seed+N 条。
#
# 说明: 空 Master 上 PSYNC 0 会因 backlog 为空回 +FULLRESYNC，故先灌 1 条 seed
#       让 offset 0 落在 backlog 窗口内（与 test_incr_repl.sh 的预灌做法一致）。
#
# Usage:
#   ./test/smoke/test_incr_tcp_sync.sh [master_port] [slave_port] [incr_port]
#   N=15000 MASTER_PORT=7801 SLAVE_PORT=7802 INCR_PORT=7803 ./test/smoke/test_incr_tcp_sync.sh
#
# Prerequisites: build/kvstore 已编译、python3（无需 redis-cli / root）
# ============================================================================
set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ── Configuration ────────────────────────────────────────────────────────────
MASTER_PORT="${1:-${MASTER_PORT:-7801}}"
SLAVE_PORT="${2:-${SLAVE_PORT:-7802}}"
INCR_PORT="${3:-${INCR_PORT:-7803}}"           # incr_repl_port（Master 监听 / Slave 连接）
KVSTORE_BIN="${KVSTORE_BIN:-${ROOT_DIR}/build/kvstore}"
HOST="${HOST:-127.0.0.1}"
N="${N:-15000}"                                # 数据量，>10000

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
    for p in $MASTER_PORT $SLAVE_PORT $INCR_PORT; do
        pid=$(lsof -ti ":$p" 2>/dev/null) && [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    rm -f "$MASTER_CONF" "$SLAVE_CONF" "$MASTER_LOG" "$SLAVE_LOG"
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

# ── 1. Start master (incr_sync on, rdma_sync off) ────────────────────────────
section "Start master (incr_sync on, rdma_sync off)"

cat > "$MASTER_CONF" << EOF
port ${MASTER_PORT}
network reactor
maxclients 200
loglevel info
persistence none
expire off
rdma_sync off
rdma_role master
incr_sync on
incr_repl_port ${INCR_PORT}
xdp_sync off
EOF

"$KVSTORE_BIN" "$MASTER_CONF" &>"$MASTER_LOG" &
MASTER_PID=$!
if ! wait_ping "$MASTER_PORT" 20; then
    printf "${RED}FATAL${RST}: master failed to start\n" >&2
    cat "$MASTER_LOG" >&2
    exit 2
fi
ok "master started (pid=$MASTER_PID, port=$MASTER_PORT)"

if ss -tlnp 2>/dev/null | grep -q ":$INCR_PORT "; then
    ok "master listening on incr_repl_port $INCR_PORT"
else
    bad "master listening on incr_repl_port" ":$INCR_PORT" "not found"
fi

# ── 2. Seed master (使 offset 0 落在 backlog 窗口内) ─────────────────────────
section "Seed master (1 key, so PSYNC 0 → +CONTINUE)"

if python3 "$SCRIPT_DIR/kv_resp.py" fill --host "$HOST" --port "$MASTER_PORT" --n 1 --prefix seed; then
    ok "master seeded 1 key"
else
    printf "${RED}FATAL${RST}: seed master failed\n" >&2
    exit 2
fi

# ── 3. Start slave (PSYNC 0 → +CONTINUE) ─────────────────────────────────────
section "Start slave (incr_sync on, rdma_sync off)"

cat > "$SLAVE_CONF" << EOF
port ${SLAVE_PORT}
network reactor
maxclients 200
loglevel info
persistence none
expire off
rdma_sync off
rdma_role slave
incr_sync on
incr_repl_port ${INCR_PORT}
rdma_master_ip ${HOST}
xdp_sync off
EOF

"$KVSTORE_BIN" "$SLAVE_CONF" &>"$SLAVE_LOG" &
SLAVE_PID=$!

if ! wait_ping "$SLAVE_PORT" 20; then
    printf "${RED}FATAL${RST}: slave failed to start\n" >&2
    cat "$SLAVE_LOG" >&2
    exit 2
fi
ok "slave started (pid=$SLAVE_PID, port=$SLAVE_PORT)"

if grep -q "+CONTINUE" "$SLAVE_LOG" 2>/dev/null; then
    ok "slave received +CONTINUE (streaming mode)"
else
    bad "slave received +CONTINUE" "+CONTINUE" "$(grep -E 'INCR-SLAVE|INCR-REPL' "$SLAVE_LOG" | head -5)"
fi

# ── 4. Fill master (live streaming) ──────────────────────────────────────────
section "Fill master with ${N} keys (live incremental)"

if python3 "$SCRIPT_DIR/kv_resp.py" fill --host "$HOST" --port "$MASTER_PORT" --n "$N" --prefix key; then
    ok "master filled ${N} keys"
else
    printf "${RED}FATAL${RST}: fill master failed\n" >&2
    exit 2
fi

# ── 5. Verify slave (seed + N 条) ────────────────────────────────────────────
section "Verify slave caught up (${N} keys + 1 seed)"

EXPECT_DBSIZE=$((N + 1))
if python3 "$SCRIPT_DIR/kv_resp.py" verify --host "$HOST" --port "$SLAVE_PORT" \
     --n "$N" --prefix key --wait 120 --expect-dbsize "$EXPECT_DBSIZE"; then
    ok "slave verified ${N} keys (dbsize=${EXPECT_DBSIZE})"
else
    bad "slave incremental sync" "${N} keys (dbsize=${EXPECT_DBSIZE})" "verify failed (see diagnostics below)"
fi

if python3 "$SCRIPT_DIR/kv_resp.py" verify --host "$HOST" --port "$SLAVE_PORT" \
     --n 1 --prefix seed --once; then
    ok "seed key replicated"
else
    bad "seed key replicated" "value:000000000000" "seed:000000000000 missing"
fi

# ── 6. Diagnostics ───────────────────────────────────────────────────────────
section "Diagnostics (incremental sync logs)"

echo "  ${YEL}── master ${RST}──"
grep -E '\[INCR-MASTER\]|PSYNC|FULLRESYNC|backfill|streaming' "$MASTER_LOG" 2>/dev/null | tail -10 || echo "  (no master incr lines)"
echo "  ${YEL}── slave ${RST}──"
grep -E '\[INCR-SLAVE\]|CONTINUE|FULLRESYNC|streaming|backfill' "$SLAVE_LOG" 2>/dev/null | tail -10 || echo "  (no slave incr lines)"

# ── Summary ──────────────────────────────────────────────────────────────────
printf "\n────────────────────────────────────────────────────────\n"
if [ "$FAIL" -eq 0 ]; then
    printf "EPOLL TCP INCR SYNC: ${GRN}ALL PASS${RST} (%d checks)\n" "$PASS"
    exit 0
else
    printf "EPOLL TCP INCR SYNC: ${RED}%d FAILED${RST}\n" "$FAIL"
    printf "  slave log: %s\n  master log: %s\n" "$SLAVE_LOG" "$MASTER_LOG"
    exit 1
fi
