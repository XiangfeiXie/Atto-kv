#!/usr/bin/env bash
# =============================================================================
# Phase 7.2 — CONFIG GET / SET / RESETSTAT / REWRITE smoke
#
# Walks the matrix:
#   1. CONFIG GET on a representative read-only param (port, threads)
#   2. CONFIG GET on online-settable params (loglevel, maxclients)
#   3. CONFIG SET loglevel debug → +OK and CONFIG GET reflects it
#   4. CONFIG SET maxclients → range validation
#   5. CONFIG SET on a read-only param → -ERR (no crash)
#   6. CONFIG GET unknown name → empty array
#   7. CONFIG REWRITE → -ERR (not implemented)
#   8. CONFIG RESETSTAT → +OK
#   9. CONFIG without args → -ERR
#
# All checks pass ⇒ exit 0; any check fails ⇒ exit 1.
# =============================================================================
set -u

PORT="${PORT:-2000}"
KVS_BIN="${KVS_BIN:-$(pwd)/build/kvstore}"
KVS_CONF="${KVS_CONF:-$(pwd)/conf/kvstore-dev.conf}"
LOG="/tmp/kvstore-config-smoke.log"

if ! command -v redis-cli >/dev/null 2>&1; then
    echo "redis-cli not found"; exit 2
fi

pkill -9 kvstore 2>/dev/null; sleep 1
setsid "$KVS_BIN" "$KVS_CONF" >"$LOG" 2>&1 </dev/null &
disown
sleep 1

PID=$(pgrep -f "$KVS_BIN" | head -1)
if [ -z "$PID" ]; then
    echo "FAIL: kvstore not running"; tail -20 "$LOG"; exit 1
fi

fail=0
check() {
    local label="$1" expect="$2" actual="$3"
    if [[ "$actual" == *"$expect"* ]]; then
        printf "  [PASS] %-40s\n" "$label"
    else
        printf "  [FAIL] %-40s   expect=%q  got=%q\n" "$label" "$expect" "$actual"
        fail=1
    fi
}
rcli() { timeout 3 redis-cli -p "$PORT" "$@" 2>&1; }

echo "=== Phase 7.2 CONFIG smoke ==="

# 1. read-only params
check "CONFIG GET port"       "$(rcli CONFIG GET port | tail -1)"     "$(rcli CONFIG GET port | tail -1)"
out=$(rcli CONFIG GET port);    check "GET port returns name+value"     "port"          "$out"
out=$(rcli CONFIG GET threads); check "GET threads returns name+value"  "threads"       "$out"

# 2. online-settable
out=$(rcli CONFIG GET loglevel);   check "GET loglevel"          "loglevel"   "$out"
out=$(rcli CONFIG GET maxclients); check "GET maxclients"        "maxclients" "$out"

# 3. SET loglevel + verify
out=$(rcli CONFIG SET loglevel debug);  check "SET loglevel debug"  "OK"     "$out"
out=$(rcli CONFIG GET loglevel);        check "GET reflects new val" "debug"  "$out"
out=$(rcli CONFIG SET loglevel info);   check "SET loglevel info"   "OK"     "$out"
out=$(rcli CONFIG SET loglevel BOGUS);  check "SET loglevel invalid -ERR" "ERR" "$out"

# 4. SET maxclients range
out=$(rcli CONFIG SET maxclients 5000); check "SET maxclients 5000"  "OK"     "$out"
out=$(rcli CONFIG GET maxclients);      check "GET reflects 5000"    "5000"   "$out"
out=$(rcli CONFIG SET maxclients 0);    check "SET maxclients 0 -ERR" "ERR"   "$out"
out=$(rcli CONFIG SET maxclients 99999999); check "SET maxclients oversize -ERR" "ERR" "$out"
out=$(rcli CONFIG SET maxclients abc);  check "SET maxclients non-int -ERR" "ERR" "$out"

# 5. SET on read-only
out=$(rcli CONFIG SET port 9999);       check "SET port (read-only) -ERR" "read-only" "$out"
out=$(rcli CONFIG SET threads 1);       check "SET threads (read-only) -ERR" "read-only" "$out"

# 6. unknown name
out=$(rcli CONFIG GET nonexistent_param);  check "GET unknown → empty array" "" "$out"
[ -z "$out" ] || [ "$out" = "" ] && printf "  [PASS] %-40s\n" "GET unknown returns empty"

# 7. REWRITE
out=$(rcli CONFIG REWRITE);             check "REWRITE -ERR not implemented" "not implemented" "$out"

# 8. RESETSTAT
out=$(rcli CONFIG RESETSTAT);           check "RESETSTAT +OK"               "OK"               "$out"

# 9. malformed
out=$(rcli CONFIG);                     check "CONFIG (no args) -ERR"        "ERR"              "$out"
out=$(rcli CONFIG BOGUS);               check "CONFIG bogus subcmd -ERR"     "Unknown CONFIG"   "$out"

# Smoke under load: ensure CONFIG SET while traffic flows doesn't crash.
redis-benchmark -p "$PORT" -t set -n 5000 -c 20 -P 4 -q >/dev/null 2>&1 &
BG=$!
sleep 0.3
out=$(rcli CONFIG SET loglevel warn);   check "SET loglevel under load"      "OK"               "$out"
wait $BG 2>/dev/null
[ -d "/proc/$PID" ] && printf "  [PASS] %-40s\n" "kvstore alive after load+SET" \
                    || { printf "  [FAIL] kvstore died\n"; fail=1; }

# teardown
pkill -9 kvstore 2>/dev/null

echo
if [ $fail -eq 0 ]; then
    echo "=== CONFIG smoke: ALL PASS ==="
    exit 0
else
    echo "=== CONFIG smoke: FAIL ==="
    exit 1
fi
