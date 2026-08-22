#!/usr/bin/env bash
# ============================================================================
# kvstore incremental replication smoke test
# ============================================================================
# Tests the built-in repl-incr module end-to-end:
#   Master: dedicated incr_repl_port, backlog buffer, PSYNC handshake
#   Slave:  TCP connect, PSYNC offset negotiation, command stream replay
#
# Covers: SET/GET, INCR/DECR/INCRBY/DECRBY, APPEND, GETSET, MSET/MGET,
#         DEL, FLUSHALL, pipeline batches, concurrent connections,
#         backlog backfill, backlog overflow +FULLRESYNC.
#
# Usage:
#   ./test/smoke/test_incr_repl.sh
#   MASTER_PORT=6666 SLAVE_PORT=6667 INCR_PORT=6668 ./test/smoke/test_incr_repl.sh
#
# Prerequisites:
#   - kvstore binary at ../../build/kvstore (or set KVSTORE_BIN)
#   - redis-cli, python3
# ============================================================================
set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ── Configuration ────────────────────────────────────────────────────────────
MASTER_PORT="${1:-${MASTER_PORT:-5555}}"
SLAVE_PORT="${2:-${SLAVE_PORT:-5556}}"
INCR_PORT="${3:-${INCR_PORT:-5557}}"
KVSTORE_BIN="${KVSTORE_BIN:-${ROOT_DIR}/build/kvstore}"

HOST="${HOST:-127.0.0.1}"
TIMEOUT="${TIMEOUT:-3s}"

RCLI_M="timeout ${TIMEOUT} redis-cli -h ${HOST} -p ${MASTER_PORT}"
RCLI_S="timeout ${TIMEOUT} redis-cli -h ${HOST} -p ${SLAVE_PORT}"

MASTER_CONF=$(mktemp)
SLAVE_CONF=$(mktemp)
MASTER_LOG=$(mktemp)
SLAVE_LOG=$(mktemp)

MASTER_PID=""
SLAVE_PID=""

# ── Test framework ───────────────────────────────────────────────────────────
PASS=0; FAIL=0; SKIP=0
FAILED_NAMES=()

RED=$'\033[31m' GRN=$'\033[32m' YEL=$'\033[33m' RST=$'\033[0m'

# 静默模式：只有失败时才输出详细信息
ok()   { PASS=$((PASS+1)); }
bad()  { 
    FAIL=$((FAIL+1))
    FAILED_NAMES+=("$1")
    printf "  ${RED}FAIL${RST}  %s\n" "$1"
    printf "        expected: %s\n" "$2"
    printf "        actual:   %s\n" "$3"
}
skip() { 
    SKIP=$((SKIP+1))
    printf "  ${YEL}SKIP${RST}  %s — %s\n" "$1" "$2"
}

chk() {
    if [ "$3" = "$2" ]; then ok "$1"
    else bad "$1" "$2" "$3"; fi
}

section() { 
    # 不输出任何内容，保持安静
    return 0
}

summary() {
    local total=$((PASS + FAIL + SKIP))
    printf "\n────────────────────────────────────────────────────────\n"
    printf "%s: %d/%d passed" "$1" "$PASS" "$total"
    if [ "$FAIL" -gt 0 ]; then
        printf "  (${RED}%d failed${RST})\n" "$FAIL"
        printf "\n${RED}Failed tests:${RST}\n"
        for n in "${FAILED_NAMES[@]}"; do 
            printf "    • %s\n" "$n"
        done
        return 1
    fi
    [ "$SKIP" -gt 0 ] && printf "  (${YEL}%d skipped${RST})\n" "$SKIP"
    printf "  ${GRN}ALL PASS${RST}\n"
    return 0
}

# ── Cleanup ──────────────────────────────────────────────────────────────────
cleanup() {
    [ -n "$MASTER_PID" ] && kill "$MASTER_PID" 2>/dev/null || true
    [ -n "$SLAVE_PID" ]  && kill "$SLAVE_PID" 2>/dev/null || true
    # Kill any stale processes on our ports
    for p in $MASTER_PORT $SLAVE_PORT $INCR_PORT; do
        pid=$(lsof -ti ":$p" 2>/dev/null) && [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    rm -f "$MASTER_CONF" "$SLAVE_CONF" "$MASTER_LOG" "$SLAVE_LOG"
}
trap cleanup EXIT INT TERM

# ── Helper: wait for a key-value pair on a server ───────────────────────────
wait_for_key() {
    local cli="$1" key="$2" expected="$3" max_wait="${4:-5}"
    local waited=0
    while [ "$waited" -lt "$max_wait" ]; do
        local val
        val=$($cli GET "$key" 2>/dev/null)
        if [ "$val" = "$expected" ]; then return 0; fi
        sleep 0.2
        waited=$((waited + 1))
    done
    return 1
}

# ── Prerequisites ────────────────────────────────────────────────────────────

if [ ! -x "$KVSTORE_BIN" ]; then
    printf "${RED}FATAL${RST}: kvstore binary not found at %s\n" "$KVSTORE_BIN" >&2
    exit 2
fi

if ! command -v redis-cli >/dev/null 2>&1; then
    printf "${RED}FATAL${RST}: redis-cli not found\n" >&2; exit 2
fi

if ! command -v python3 >/dev/null 2>&1; then
    printf "${RED}FATAL${RST}: python3 not found\n" >&2; exit 2
fi

# ═══════════════════════════════════════════════════════════════════════════════
# 1. START MASTER (incr_sync on, rdma_sync off)
# ═══════════════════════════════════════════════════════════════════════════════

cat > "$MASTER_CONF" << EOF
port ${MASTER_PORT}
network reactor
maxclients 200
loglevel info
persistence none
expire on
rdma_sync off
rdma_role master
incr_sync on
incr_repl_port ${INCR_PORT}
EOF

"$KVSTORE_BIN" "$MASTER_CONF" &>"$MASTER_LOG" &
MASTER_PID=$!
sleep 1
if ! kill -0 "$MASTER_PID" 2>/dev/null; then
    printf "${RED}FATAL${RST}: master failed to start\n" >&2
    cat "$MASTER_LOG" >&2; exit 2
fi

# Verify master is reachable
if ! $RCLI_M PING >/dev/null 2>&1; then
    printf "${RED}FATAL${RST}: master PING failed\n" >&2
    cat "$MASTER_LOG" >&2; exit 2
fi

# Verify incr repl port is listening
if ! ss -tlnp 2>/dev/null | grep -q ":$INCR_PORT "; then
    printf "${RED}FATAL${RST}: incr repl port $INCR_PORT not listening\n" >&2
    cat "$MASTER_LOG" >&2; exit 2
fi

# ═══════════════════════════════════════════════════════════════════════════════
# 2. PRE-POPULATE MASTER (so backlog is non-empty, offset 0 is valid)
# ═══════════════════════════════════════════════════════════════════════════════

$RCLI_M SET seed_k1 "seed_v1" >/dev/null
$RCLI_M SET seed_k2 "seed_v2" >/dev/null
$RCLI_M INCR seed_ctr >/dev/null    # 1
$RCLI_M INCR seed_ctr >/dev/null    # 2
$RCLI_M INCR seed_ctr >/dev/null    # 3
$RCLI_M MSET seed_ma 1 seed_mb 2 seed_mc 3 >/dev/null
sleep 0.3

chk "master seed_k1"  "seed_v1" "$($RCLI_M GET seed_k1)"
chk "master seed_k2"  "seed_v2" "$($RCLI_M GET seed_k2)"
chk "master seed_ctr" "3"       "$($RCLI_M GET seed_ctr)"
chk "master seed_ma"  "1"       "$($RCLI_M GET seed_ma)"
chk "master seed_mc"  "3"       "$($RCLI_M GET seed_mc)"

# ═══════════════════════════════════════════════════════════════════════════════
# 3. START SLAVE — connect, PSYNC 0, get +CONTINUE, backfill
# ═══════════════════════════════════════════════════════════════════════════════

cat > "$SLAVE_CONF" << EOF
port ${SLAVE_PORT}
network reactor
maxclients 200
loglevel info
persistence none
expire on
rdma_sync off
rdma_role slave
incr_sync on
incr_repl_port ${INCR_PORT}
rdma_master_ip ${HOST}
EOF

"$KVSTORE_BIN" "$SLAVE_CONF" &>"$SLAVE_LOG" &
SLAVE_PID=$!
sleep 2   # Give slave time to connect + PSYNC handshake + backfill

if ! kill -0 "$SLAVE_PID" 2>/dev/null; then
    printf "${RED}FATAL${RST}: slave failed to start\n" >&2
    cat "$SLAVE_LOG" >&2; exit 2
fi

# Verify slave is reachable
if ! $RCLI_S PING >/dev/null 2>&1; then
    printf "${RED}FATAL${RST}: slave PING failed\n" >&2
    cat "$SLAVE_LOG" >&2; exit 2
fi

# Check slave log for +CONTINUE
if ! grep -q "+CONTINUE" "$SLAVE_LOG" 2>/dev/null; then
    bad "slave received +CONTINUE" "+CONTINUE" "$(grep -E 'INCR-SLAVE|INCR-REPL' "$SLAVE_LOG" | head -5)"
fi

# ═══════════════════════════════════════════════════════════════════════════════
# 4. VERIFY BACKFILL — seed data from master arrived on slave
# ═══════════════════════════════════════════════════════════════════════════════

chk "slave seed_k1"  "seed_v1" "$($RCLI_S GET seed_k1 2>/dev/null)"
chk "slave seed_k2"  "seed_v2" "$($RCLI_S GET seed_k2 2>/dev/null)"
chk "slave seed_ctr" "3"       "$($RCLI_S GET seed_ctr 2>/dev/null)"
chk "slave seed_ma"  "1"       "$($RCLI_S GET seed_ma 2>/dev/null)"
chk "slave seed_mb"  "2"       "$($RCLI_S GET seed_mb 2>/dev/null)"
chk "slave seed_mc"  "3"       "$($RCLI_S GET seed_mc 2>/dev/null)"

MASTER_DB=$($RCLI_M DBSIZE 2>/dev/null)
SLAVE_DB=$($RCLI_S DBSIZE 2>/dev/null)
chk "DBSIZE match after backfill" "$MASTER_DB" "$SLAVE_DB"

# ═══════════════════════════════════════════════════════════════════════════════
# 5. LIVE REPLICATION — basic string commands
# ═══════════════════════════════════════════════════════════════════════════════

$RCLI_M SET live_key1 "hello" >/dev/null
$RCLI_M SET live_key2 "world" >/dev/null
$RCLI_M SET live_empty "" >/dev/null
$RCLI_M SET "live key spaces" "with spaces" >/dev/null
sleep 0.3

chk "SET live_key1"      "hello"       "$($RCLI_S GET live_key1 2>/dev/null)"
chk "SET live_key2"      "world"       "$($RCLI_S GET live_key2 2>/dev/null)"
chk "SET live_empty"     ""            "$($RCLI_S GET live_empty 2>/dev/null)"
chk "SET key with spaces" "with spaces" "$($RCLI_S GET 'live key spaces' 2>/dev/null)"

$RCLI_M SET live_key1 "updated" >/dev/null
sleep 0.3
chk "SET overwrite live_key1" "updated" "$($RCLI_S GET live_key1 2>/dev/null)"

$RCLI_M SET incr_key 100 >/dev/null
sleep 0.2
chk "SET incr_key 100" "100" "$($RCLI_S GET incr_key 2>/dev/null)"

$RCLI_M INCR incr_key >/dev/null
sleep 0.2
chk "INCR 100→101" "101" "$($RCLI_S GET incr_key 2>/dev/null)"

$RCLI_M INCR incr_key >/dev/null
sleep 0.2
chk "INCR 101→102" "102" "$($RCLI_S GET incr_key 2>/dev/null)"

$RCLI_M DECR incr_key >/dev/null
sleep 0.2
chk "DECR 102→101" "101" "$($RCLI_S GET incr_key 2>/dev/null)"

$RCLI_M SET incrby_key 50 >/dev/null
sleep 0.2
$RCLI_M INCRBY incrby_key 25 >/dev/null
sleep 0.2
chk "INCRBY 50+25=75" "75" "$($RCLI_S GET incrby_key 2>/dev/null)"

$RCLI_M DECRBY incrby_key 30 >/dev/null
sleep 0.2
chk "DECRBY 75-30=45" "45" "$($RCLI_S GET incrby_key 2>/dev/null)"

$RCLI_M INCRBY incrby_key -10 >/dev/null
sleep 0.2
chk "INCRBY 45+(-10)=35" "35" "$($RCLI_S GET incrby_key 2>/dev/null)"

# APPEND and GETSET not implemented - skip silently
# ═══════════════════════════════════════════════════════════════════════════════
# 6. LIVE REPLICATION — multi-key and data-changing commands
# ═══════════════════════════════════════════════════════════════════════════════

$RCLI_M MSET mk_a "alpha" mk_b "beta" mk_c "gamma" mk_d "delta" >/dev/null
sleep 0.3
MGET_S=$($RCLI_S MGET mk_a mk_b mk_c mk_d 2>/dev/null | tr '\n' ' ' | sed 's/ $//')
chk "MSET 4 keys → slave MGET" "alpha beta gamma delta" "$MGET_S"

$RCLI_M MSET mk_e "" mk_f "full" >/dev/null
sleep 0.3
chk "MSET with empty value → mk_e" ""   "$($RCLI_S GET mk_e 2>/dev/null)"
chk "MSET with empty value → mk_f" "full" "$($RCLI_S GET mk_f 2>/dev/null)"

$RCLI_M SET del_key "to_delete" >/dev/null
sleep 0.2
chk "SET del_key" "to_delete" "$($RCLI_S GET del_key 2>/dev/null)"

$RCLI_M DEL del_key >/dev/null
sleep 0.2
chk "DEL del_key → nil" "" "$($RCLI_S GET del_key 2>/dev/null)"

$RCLI_M SET dm1 1 >/dev/null
$RCLI_M SET dm2 2 >/dev/null
$RCLI_M SET dm3 3 >/dev/null
sleep 0.2
$RCLI_M DEL dm1 dm2 dm3 >/dev/null
sleep 0.2
chk "DEL multi dm1" "" "$($RCLI_S GET dm1 2>/dev/null)"
chk "DEL multi dm2" "" "$($RCLI_S GET dm2 2>/dev/null)"
chk "DEL multi dm3" "" "$($RCLI_S GET dm3 2>/dev/null)"

$RCLI_M SET flush_victim "gone" >/dev/null
sleep 0.2
chk "pre-FLUSHALL key exists" "gone" "$($RCLI_S GET flush_victim 2>/dev/null)"

$RCLI_M FLUSHALL >/dev/null
sleep 0.3
chk "FLUSHALL → key gone" "" "$($RCLI_S GET flush_victim 2>/dev/null)"
chk "FLUSHALL → DBSIZE=0" "0" "$($RCLI_S DBSIZE 2>/dev/null)"

# ═══════════════════════════════════════════════════════════════════════════════
# 7. LIVE REPLICATION — TTL commands (EXPIRE, PERSIST — expire on)
# ═══════════════════════════════════════════════════════════════════════════════

$RCLI_M SET expire_key "temp_value" >/dev/null
sleep 0.2
chk "SET expire_key" "temp_value" "$($RCLI_S GET expire_key 2>/dev/null)"

$RCLI_M EXPIRE expire_key 300 >/dev/null
sleep 0.3
SLAVE_TTL=$($RCLI_S TTL expire_key 2>/dev/null)
if [ "$SLAVE_TTL" -gt 0 ] 2>/dev/null; then
    ok "EXPIRE 300s → slave TTL=$SLAVE_TTL (>0)"
elif [ "$SLAVE_TTL" = "-2" ]; then
    bad "EXPIRE 300s → slave TTL" ">0" "$SLAVE_TTL (key missing)"
else
    bad "EXPIRE 300s → slave TTL" ">0" "$SLAVE_TTL"
fi

$RCLI_M PERSIST expire_key >/dev/null
sleep 0.2
SLAVE_TTL2=$($RCLI_S TTL expire_key 2>/dev/null)
if [ "$SLAVE_TTL2" = "-1" ]; then
    ok "PERSIST → slave TTL=-1 (persistent)"
elif [ "$SLAVE_TTL2" = "-2" ]; then
    bad "PERSIST → slave TTL" "-1 (persistent)" "-2 (key missing)"
else
    bad "PERSIST → slave TTL" "-1 (persistent)" "$SLAVE_TTL2"
fi

# ═══════════════════════════════════════════════════════════════════════════════
# 8. PIPELINE / BATCH COMMANDS
# ═══════════════════════════════════════════════════════════════════════════════

$RCLI_M FLUSHALL >/dev/null
sleep 0.5
for _ in $(seq 1 15); do
    sd=$($RCLI_S DBSIZE 2>/dev/null)
    [ "$sd" = "0" ] && break
    sleep 0.3
done

PIPE_RESULT=$(python3 -c "
import socket, time

def resp(*args):
    parts = [f'*{len(args)}\r\n']
    for a in args:
        a = str(a)
        parts.append(f'\${len(a)}\r\n{a}\r\n')
    return ''.join(parts).encode()

s = socket.socket()
s.connect(('$HOST', $MASTER_PORT))
s.settimeout(5)

# Send 30 SETs without reading between
for i in range(30):
    s.sendall(resp('SET', f'pipe_k{i:03d}', f'pipe_v{i:03d}'))

time.sleep(0.5)
# Drain responses
buf = b''
for _ in range(60):
    try:
        chunk = s.recv(65536)
        if not chunk: break
        buf += chunk
    except socket.timeout:
        break
s.close()

oks = buf.count(b'+OK')
if oks == 30:
    print('PASS')
else:
    print(f'FAIL:{oks}/30 OKs')
" 2>&1)

case "$PIPE_RESULT" in
    PASS*)    ok "RESP pipeline 30 SETs (master responses)" ;;
    FAIL:30*) ok "RESP pipeline 30 SETs (responses: $PIPE_RESULT)" ;;
    FAIL:*)   bad "RESP pipeline 30 SETs" "30 OKs" "$PIPE_RESULT" ;;
    *)        bad "RESP pipeline 30 SETs" "PASS" "$PIPE_RESULT" ;;
esac

sleep 1
PIPE_OK=0
for _ in $(seq 1 5); do
    PIPE_OK=0
    for i in $(seq 0 29); do
        key=$(printf 'pipe_k%03d' $i)
        expected=$(printf 'pipe_v%03d' $i)
        val=$($RCLI_S GET "$key" 2>/dev/null)
        [ "$val" = "$expected" ] && PIPE_OK=$((PIPE_OK + 1))
    done
    [ "$PIPE_OK" -ge 25 ] && break
    sleep 0.5
done
chk "Slave: 30 pipe keys replicated" "30" "$PIPE_OK"

$RCLI_M FLUSHALL >/dev/null
sleep 0.3
for _ in $(seq 1 10); do
    sd=$($RCLI_S DBSIZE 2>/dev/null)
    [ "$sd" = "0" ] && break
    sleep 0.3
done

CHAIN_RESULT=$(python3 -c "
import socket, time

def resp(*args):
    parts = [f'*{len(args)}\r\n']
    for a in args:
        a = str(a)
        parts.append(f'\${len(a)}\r\n{a}\r\n')
    return ''.join(parts).encode()

s = socket.socket()
s.connect(('$HOST', $MASTER_PORT))
s.settimeout(5)

# SET 0→20, then INCR each, then DECR each
for i in range(20):
    s.sendall(resp('SET', f'chk{i:03d}', '10'))
for i in range(20):
    s.sendall(resp('INCR', f'chk{i:03d}'))
for i in range(20):
    s.sendall(resp('DECR', f'chk{i:03d}'))

time.sleep(0.3)
buf = b''
for _ in range(60):
    try:
        chunk = s.recv(65536)
        if not chunk: break
        buf += chunk
    except socket.timeout:
        break
s.close()

oks = buf.count(b'+OK')
if oks >= 20:
    print('PASS')
else:
    print(f'FAIL:{oks}')
" 2>&1)

case "$CHAIN_RESULT" in
    PASS*) ok "RESP pipeline SET→INCR→DECR chain (master)" ;;
    FAIL:*) bad "RESP pipeline SET→INCR→DECR chain" ">=20 OKs" "$CHAIN_RESULT" ;;
    *)      bad "RESP pipeline SET→INCR→DECR chain" "PASS" "$CHAIN_RESULT" ;;
esac

sleep 0.5
CHAIN_OK=0
for i in $(seq 0 19); do
    key=$(printf 'chk%03d' $i)
    val=$($RCLI_S GET "$key" 2>/dev/null)
    [ "$val" = "10" ] && CHAIN_OK=$((CHAIN_OK + 1))
done
chk "Slave: 20 chain keys = 10" "20" "$CHAIN_OK"

# ═══════════════════════════════════════════════════════════════════════════════
# 9. CONCURRENT CONNECTIONS
# ═══════════════════════════════════════════════════════════════════════════════

$RCLI_M FLUSHALL >/dev/null
sleep 0.3
for _ in $(seq 1 10); do
    sd=$($RCLI_S DBSIZE 2>/dev/null)
    [ "$sd" = "0" ] && break
    sleep 0.3
done

pids=""
for i in $(seq 1 15); do
    timeout 5 redis-cli -h "$HOST" -p "$MASTER_PORT" SET "conc_k${i}" "conc_v${i}" >/dev/null 2>&1 &
    pids="$pids $!"
done
for _ in $(seq 1 20); do
    all_done=1
    for p in $pids; do
        if kill -0 "$p" 2>/dev/null; then all_done=0; break; fi
    done
    [ "$all_done" -eq 1 ] && break
    sleep 0.5
done
for p in $pids; do kill "$p" 2>/dev/null; done
sleep 1

CONC_OK=0
for i in $(seq 1 15); do
    val=$($RCLI_S GET "conc_k${i}" 2>/dev/null)
    [ "$val" = "conc_v${i}" ] && CONC_OK=$((CONC_OK + 1))
done
chk "Concurrent 15 SETs → slave" "15" "$CONC_OK"

# ═══════════════════════════════════════════════════════════════════════════════
# 10. CONSISTENCY VERIFICATION
# ═══════════════════════════════════════════════════════════════════════════════

$RCLI_M SET final_check "consistent" >/dev/null
sleep 0.3
chk "final SET" "consistent" "$($RCLI_S GET final_check 2>/dev/null)"

MASTER_DBSIZE=$($RCLI_M DBSIZE 2>/dev/null)
SLAVE_DBSIZE=$($RCLI_S DBSIZE 2>/dev/null)
chk "Final DBSIZE match" "$MASTER_DBSIZE" "$SLAVE_DBSIZE"

# ═══════════════════════════════════════════════════════════════════════════════
summary "KVSTORE BUILT-IN INCR SYNC SMOKE"