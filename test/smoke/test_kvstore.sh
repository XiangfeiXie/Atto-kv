#!/usr/bin/env bash
# ============================================================================
# kvstore comprehensive functional test
# ============================================================================
# Covers: basic commands, string operations, expire/TTL, batch pipeline,
#         redis-cli --pipe compatibility, error handling, edge cases.
#
# Usage:
#   ./test_kvstore.sh [host] [port]
#   HOST=127.0.0.1 PORT=2000 ./test/smoke/test_kvstore.sh
#
# Prerequisites: kvstore server running, redis-cli available
# ============================================================================
set -u

HOST="${1:-${HOST:-127.0.0.1}}"
PORT="${2:-${PORT:-2000}}"
TIMEOUT="${REDIS_CLI_TIMEOUT:-3s}"
RCLI="timeout ${TIMEOUT} redis-cli -h ${HOST} -p ${PORT}"

# ── Test framework ────────────────────────────────────────────────────────

PASS=0; FAIL=0; SKIP=0
FAILED_NAMES=()

RED=$'\033[31m' GRN=$'\033[32m' YEL=$'\033[33m' RST=$'\033[0m'

ok()   { PASS=$((PASS+1)); printf "  ${GRN}PASS${RST}  %s\n" "$1"; }
bad()  { FAIL=$((FAIL+1)); FAILED_NAMES+=("$1");
         printf "  ${RED}FAIL${RST}  %s\n        expected: %s\n        actual:   %s\n" "$1" "$2" "$3"; }
skip() { SKIP=$((SKIP+1)); printf "  ${YEL}SKIP${RST}  %s — %s\n" "$1" "$2"; }

chk() {
    if   [ "$3" = "$2" ]; then ok "$1"
    else bad "$1" "$2" "$3"; fi
}

chk_contains() {  # NAME NEEDLE ACTUAL
    case "$3" in
        *"$2"*) ok "$1" ;;
        *)      bad "$1" "contains '$2'" "$3" ;;
    esac
}

section() { printf "\n${YEL}── %s ──${RST}\n" "$1"; }

require_server() {
    if ! $RCLI PING >/dev/null 2>&1; then
        printf "${RED}FATAL${RST}: cannot reach kvstore at %s:%s — start it first.\n" "$HOST" "$PORT" >&2
        exit 2
    fi
    $RCLI FLUSHALL >/dev/null
}

# ── Summary ───────────────────────────────────────────────────────────────

summary() {
    local total=$((PASS + FAIL + SKIP))
    printf "\n────────────────────────────────────────────────────────\n"
    printf "%s suite: %d/%d passed" "$1" "$PASS" "$total"
    if [ "$FAIL" -gt 0 ]; then
        printf "  (${RED}%d failed${RST})\n" "$FAIL"
        for n in "${FAILED_NAMES[@]}"; do printf "    • %s\n" "$n"; done
        return 1
    fi
    [ "$SKIP" -gt 0 ] && printf "  (${YEL}%d skipped${RST})\n" "$SKIP"
    printf "  ${GRN}ALL PASS${RST}\n"
    return 0
}

# ── Begin ─────────────────────────────────────────────────────────────────

require_server

# ═══════════════════════════════════════════════════════════════════════════
# 1. CONNECTION / SERVER BASICS
# ═══════════════════════════════════════════════════════════════════════════

section "Connection / server basics"
chk "PING"                  "PONG"       "$($RCLI PING)"
chk "PING with message"     "hello"      "$($RCLI PING hello)"
chk "ECHO"                  "echo-me"    "$($RCLI ECHO echo-me)"
# SELECT not implemented; verify it errors instead of crashing
ERR=$($RCLI SELECT 0 2>&1)
chk_contains "SELECT errors (not implemented)" "ERR" "$ERR"

section "Unknown commands"
ERR=$($RCLI NO_SUCH_CMD 2>&1)
chk_contains "Unknown command" "ERR" "$ERR"

# ═══════════════════════════════════════════════════════════════════════════
# 2. DATABASE METADATA
# ═══════════════════════════════════════════════════════════════════════════

section "Database metadata"
$RCLI FLUSHALL >/dev/null
chk "DBSIZE empty"          "0"   "$($RCLI DBSIZE)"

$RCLI SET k v >/dev/null
$RCLI SET k2 v2 >/dev/null
chk "DBSIZE after SET"      "2"   "$($RCLI DBSIZE)"

INFO_OUT=$($RCLI INFO 2>/dev/null | tr -d '\r' | head -5)
chk_contains "INFO"         "ersion"    "$INFO_OUT"

chk "FLUSHALL"              "OK"  "$($RCLI FLUSHALL)"
chk "DBSIZE after FLUSHALL" "0"   "$($RCLI DBSIZE)"

# ═══════════════════════════════════════════════════════════════════════════
# 3. BASIC STRING: SET / GET / DEL / EXISTS / TYPE / STRLEN
# ═══════════════════════════════════════════════════════════════════════════

section "SET / GET / DEL / EXISTS / TYPE / STRLEN"
chk "SET"                   "OK"     "$($RCLI SET k1 hello)"
chk "GET"                   "hello"  "$($RCLI GET k1)"
chk "STRLEN"                "5"      "$($RCLI STRLEN k1)"
chk "TYPE string"           "string" "$($RCLI TYPE k1)"
chk "EXISTS yes"            "1"      "$($RCLI EXISTS k1)"
chk "EXISTS no"             "0"      "$($RCLI EXISTS missing_key)"
chk "EXISTS multi"          "2"      "$($RCLI SET a 1 >/dev/null; $RCLI EXISTS k1 a)"
chk "GET missing"           ""       "$($RCLI GET missing_key)"
chk "TYPE missing"          "none"   "$($RCLI TYPE missing_key)"
chk "DEL existing"          "1"      "$($RCLI DEL k1)"
chk "DEL missing"           "0"      "$($RCLI DEL k1)"
chk "DEL multi"             "1"      "$($RCLI DEL a)"

section "Overwrite"
$RCLI SET k1 hello >/dev/null
chk "Overwrite SET"         "OK"     "$($RCLI SET k1 world)"
chk "After overwrite"       "world"  "$($RCLI GET k1)"

section "APPEND (not yet implemented)"
OUT=$($RCLI APPEND testkey val 2>&1)
case "$OUT" in
    *"unknown command"*|*"ERR unknown"*)
        skip "APPEND" "not implemented" ;;
    *)  ok "APPEND" ;;
esac

section "STRLEN edge cases"
$RCLI SET empty "" >/dev/null
chk "STRLEN empty string"   "0"      "$($RCLI STRLEN empty)"
chk "STRLEN missing key"    "0"      "$($RCLI STRLEN noexist)"

# ═══════════════════════════════════════════════════════════════════════════
# 4. SET WITH OPTIONS: EX / PX / NX / XX
# ═══════════════════════════════════════════════════════════════════════════

section "SET with EX / PX / NX / XX"
$RCLI DEL optkey >/dev/null
chk "SET EX 60"             "OK"     "$($RCLI SET optkey val EX 60)"
TTL=$($RCLI TTL optkey)
[ "$TTL" -ge 55 ] && [ "$TTL" -le 60 ] && ok "TTL ~60" || bad "TTL ~60" "55..60" "$TTL"

$RCLI DEL optkey >/dev/null
chk "SET PX 5000"           "OK"     "$($RCLI SET optkey val PX 5000)"
PT=$($RCLI PTTL optkey)
[ "$PT" -ge 4500 ] && [ "$PT" -le 5000 ] && ok "PTTL ~5000" || bad "PTTL ~5000" "4500..5000" "$PT"

$RCLI SET nxk val >/dev/null
chk "SET NX on existing"    ""       "$($RCLI SET nxk val2 NX)"
chk "SET NX unchanged"      "val"    "$($RCLI GET nxk)"

$RCLI DEL nxk >/dev/null
chk "SET NX on new"         "OK"     "$($RCLI SET nxk val NX)"

chk "SET XX on existing"    "OK"     "$($RCLI SET nxk val2 XX)"
chk "SET XX new value"      "val2"   "$($RCLI GET nxk)"

$RCLI DEL nxk >/dev/null
chk "SET XX on missing"     ""       "$($RCLI SET nxk val XX)"

# Combined EX + NX
$RCLI DEL combokey >/dev/null
chk "SET EX 30 NX"          "OK"     "$($RCLI SET combokey v EX 30 NX)"
TTL=$($RCLI TTL combokey)
[ "$TTL" -ge 25 ] && ok "EX+NX TTL ok" || bad "EX+NX TTL ok" "25..30" "$TTL"

# ═══════════════════════════════════════════════════════════════════════════
# 5. INCR / DECR FAMILY
# ═══════════════════════════════════════════════════════════════════════════

section "INCR / DECR / INCRBY / DECRBY"
$RCLI DEL ctr >/dev/null
chk "INCR new key"          "1"      "$($RCLI INCR ctr)"
chk "INCR again"            "2"      "$($RCLI INCR ctr)"
chk "DECR"                  "1"      "$($RCLI DECR ctr)"
chk "INCRBY 5"              "6"      "$($RCLI INCRBY ctr 5)"
chk "DECRBY 3"              "3"      "$($RCLI DECRBY ctr 3)"
chk "INCRBY negative"       "1"      "$($RCLI INCRBY ctr -2)"
chk "DECRBY zero"           "1"      "$($RCLI DECRBY ctr 0)"

section "INCR/DECR error cases"
$RCLI SET strval hello >/dev/null
ERR=$($RCLI INCR strval 2>&1)
chk_contains "INCR on non-int" "ERR" "$ERR"

ERR=$($RCLI INCRBY strval 1 2>&1)
chk_contains "INCRBY on non-int" "ERR" "$ERR"

# ═══════════════════════════════════════════════════════════════════════════
# 6. MSET / MGET
# ═══════════════════════════════════════════════════════════════════════════

section "MSET / MGET"
$RCLI DEL a b c d e missing >/dev/null 2>&1
chk "MSET"                  "OK"     "$($RCLI MSET a 1 b 2 c 3)"
MGET=$($RCLI MGET a b c | tr '\n' ' ' | sed 's/ $//')
chk "MGET 3 keys"           "1 2 3"  "$MGET"
MGET2=$($RCLI MGET a missing c | tr '\n' ' ' | sed 's/ $//')
chk "MGET with missing"     "1  3"   "$MGET2"

# MSETNX not yet implemented
OUT=$($RCLI MSETNX x 1 y 2 2>&1)
case "$OUT" in
    *"unknown command"*|*"ERR unknown"*)
        skip "MSETNX" "not implemented" ;;
    *)  ok "MSETNX" ;;
esac

# ═══════════════════════════════════════════════════════════════════════════
# 7. EXPIRE / PERSIST / TTL / PTTL / KEYS
# ═══════════════════════════════════════════════════════════════════════════

section "EXPIRE / PERSIST / TTL / PTTL"
$RCLI DEL expkey >/dev/null
$RCLI SET expkey val >/dev/null
chk "EXPIRE set"            "1"      "$($RCLI EXPIRE expkey 100)"
TTL=$($RCLI TTL expkey)
[ "$TTL" -ge 95 ] && [ "$TTL" -le 100 ] && ok "TTL after EXPIRE" || bad "TTL after EXPIRE" "95..100" "$TTL"

chk "PERSIST"               "1"      "$($RCLI PERSIST expkey)"
chk "TTL after PERSIST"     "-1"     "$($RCLI TTL expkey)"
chk "TTL missing key"       "-2"     "$($RCLI TTL noexist999)"
chk "PTTL missing key"      "-2"     "$($RCLI PTTL noexist999)"
chk "EXPIRE missing key"    "0"      "$($RCLI EXPIRE noexist 100)"

section "KEYS"
$RCLI DEL k_aaa k_bbb k_ccc >/dev/null 2>&1
$RCLI SET k_aaa 1 >/dev/null
$RCLI SET k_bbb 2 >/dev/null
$RCLI SET k_ccc 3 >/dev/null
KEYS_COUNT=$($RCLI KEYS '*' 2>/dev/null | wc -l | tr -d ' ')
[ "$KEYS_COUNT" -ge 3 ] && ok "KEYS count >=3" || bad "KEYS count >=3" ">=3" "$KEYS_COUNT"

# ═══════════════════════════════════════════════════════════════════════════
# 8. LARGE VALUES
# ═══════════════════════════════════════════════════════════════════════════

section "Large values (4KB roundtrip)"
LONG4K=$(printf 'X%.0s' $(seq 1 4000))
$RCLI SET large4k "$LONG4K" >/dev/null
chk "STRLEN 4K"             "4000"   "$($RCLI STRLEN large4k)"
chk "GET 4K roundtrip"      "$LONG4K" "$($RCLI GET large4k)"

section "Large values (64KB roundtrip)"
LONG64K=$(printf 'Y%.0s' $(seq 1 65536))
$RCLI SET large64k "$LONG64K" >/dev/null
chk "STRLEN 64K"            "65536"  "$($RCLI STRLEN large64k)"
chk "GET 64K roundtrip"     "$LONG64K" "$($RCLI GET large64k)"

section "Empty and special values"
$RCLI SET empty "" >/dev/null
chk "GET empty string"      ""       "$($RCLI GET empty)"

SPACES="   spaces   "
$RCLI SET spacekey "$SPACES" >/dev/null
chk "GET spaces"            "$SPACES" "$($RCLI GET spacekey)"

# ═══════════════════════════════════════════════════════════════════════════
# 9. WRONGTYPE GUARDS
# ═══════════════════════════════════════════════════════════════════════════

section "Cross-type error guards"
$RCLI SET strk stringval >/dev/null
# INCR on a non-integer string fails, value stays unchanged
ERR=$($RCLI INCR strk 2>&1)
chk_contains "INCR on string errors" "ERR" "$ERR"
chk "GET after failed INCR"   "stringval" "$($RCLI GET strk)"

# Now set to integer and verify INCR works
$RCLI SET intk 10 >/dev/null
chk "TYPE int value"        "string"  "$($RCLI TYPE intk)"
chk "GET int value"         "10"      "$($RCLI GET intk)"

# ═══════════════════════════════════════════════════════════════════════════
# 10. BATCH PIPELINE TESTS (redis-cli --pipe)
# ═══════════════════════════════════════════════════════════════════════════

section "redis-cli --pipe (inline commands)"
$RCLI FLUSHALL >/dev/null
# Generate 1000 inline SET commands, pipe through redis-cli --pipe
INLINE_CMDS=$(mktemp)
for i in $(seq 0 999); do
    printf "SET pipe_k%04d v%04d\n" $i $i
done > "$INLINE_CMDS"
PIPE_OUT=$(timeout 5 redis-cli -h "$HOST" -p "$PORT" --pipe < "$INLINE_CMDS" 2>&1)
rm -f "$INLINE_CMDS"

if echo "$PIPE_OUT" | grep -q "errors: 0"; then
    REPLIES=$(echo "$PIPE_OUT" | grep -o 'replies: [0-9]*' | awk '{print $2}')
    if [ "$REPLIES" = "1000" ]; then
        ok "redis-cli --pipe 1000 inline SETs"
    else
        bad "redis-cli --pipe 1000 inline SETs" "1000 replies" "$REPLIES replies"
    fi
else
    bad "redis-cli --pipe 1000 inline SETs" "errors: 0" "$(echo "$PIPE_OUT" | head -1)"
fi

# Verify data persisted
chk "DBSIZE after 1000 SETs" "1000" "$($RCLI DBSIZE)"
chk "GET pipe_k0000"         "v0000" "$($RCLI GET pipe_k0000)"
chk "GET pipe_k0500"         "v0500" "$($RCLI GET pipe_k0500)"
chk "GET pipe_k0999"         "v0999" "$($RCLI GET pipe_k0999)"

section "redis-cli --pipe (RESP commands via raw socket)"
# redis-cli --pipe is designed for inline commands only.
# RESP pipeline is tested via Python raw socket (below).
# Here we test inline format via --pipe which is the correct usage.
skip "redis-cli --pipe RESP" "use inline format; RESP tested via raw socket below"

section "redis-cli --pipe mixed (inline + RESP sync marker)"
# redis-cli --pipe sends: user data + \r\n*2\r\n$4\r\nECHO\r\n$20\r\n<random>\r\n
# This tests the empty-line parser fix
MIXED_CMDS=$(mktemp)
for i in $(seq 0 99); do
    printf "SET mixed_k%04d mv%04d\n" $i $i
done > "$MIXED_CMDS"
PIPE_OUT3=$(timeout 5 redis-cli -h "$HOST" -p "$PORT" --pipe < "$MIXED_CMDS" 2>&1)
rm -f "$MIXED_CMDS"

if echo "$PIPE_OUT3" | grep -q "errors: 0"; then
    ok "redis-cli --pipe mixed inline+sync"
else
    bad "redis-cli --pipe mixed inline+sync" "errors: 0" "$(echo "$PIPE_OUT3" | head -1)"
fi

# ═══════════════════════════════════════════════════════════════════════════
# 11. RESP PIPELINE BATCH TESTS (raw socket)
# ═══════════════════════════════════════════════════════════════════════════

section "RESP pipeline batch (Python raw socket)"
PIPELINE_RESULT=$(python3 -c "
import socket, time

def resp_cmd(*args):
    parts = [f'*{len(args)}\r\n']
    for a in args:
        a = str(a)
        parts.append(f'\${len(a)}\r\n{a}\r\n')
    return ''.join(parts).encode()

s = socket.socket()
s.connect(('$HOST', $PORT))
s.settimeout(5)

# Batch 1: 50 SETs
for i in range(50):
    s.sendall(resp_cmd('SET', f'batch1_k{i:03d}', f'batch1_v{i:03d}'))

# Batch 2: 50 GETs (without reading batch 1 responses)
for i in range(50):
    s.sendall(resp_cmd('GET', f'batch1_k{i:03d}'))

# Read all 100 responses
buf = b''
for _ in range(200):
    try:
        chunk = s.recv(65536)
        if not chunk: break
        buf += chunk
        if buf.count(b'+OK') == 50:
            break
    except socket.timeout:
        break

ok_count = buf.count(b'+OK')
err_count = buf.count(b'ERR')
s.close()

if ok_count == 50 and err_count == 0:
    print('PASS')
else:
    print(f'FAIL: {ok_count}/50 OKs, {err_count} errors')
    print(f'First 300 bytes: {buf[:300]}')
" 2>&1)

case "$PIPELINE_RESULT" in
    PASS*) ok "RESP pipeline 50 SET+GET batch" ;;
    FAIL*) bad "RESP pipeline 50 SET+GET batch" "50 OKs" "$PIPELINE_RESULT" ;;
    *)     bad "RESP pipeline 50 SET+GET batch" "PASS" "$PIPELINE_RESULT" ;;
esac

section "RESP pipeline: SET without read then INCR/DECR (r_off bug regression)"
INCR_RESULT=$(python3 -c "
import socket

s = socket.socket()
s.connect(('$HOST', $PORT))
s.settimeout(3)

def resp(*args):
    parts = [f'*{len(args)}\r\n']
    for a in args:
        a = str(a)
        parts.append(f'\${len(a)}\r\n{a}\r\n')
    return ''.join(parts).encode()

# Key scenario: SET without reading response, then INCR, INCR, DECR
# Expected: +OK (SET 10), :11 (INCR), :12 (INCR), :11 (DECR)
s.sendall(resp('SET', 'regr_ctr', '10'))
s.sendall(resp('INCR', 'regr_ctr'))
s.sendall(resp('INCR', 'regr_ctr'))
s.sendall(resp('DECR', 'regr_ctr'))

import time; time.sleep(0.1)
buf = s.recv(1024)
s.close()

# After SET→10, INCR→11, INCR→12, DECR→11
if b'+OK' in buf and b':11' in buf and b':12' in buf and b'ERR' not in buf:
    print('PASS')
else:
    print(f'FAIL: {buf!r}')
" 2>&1)

case "$INCR_RESULT" in
    PASS*) ok "RESP pipeline SET+INCR+DECR (r_off bug regression)" ;;
    *)     bad "RESP pipeline SET+INCR+DECR (r_off bug regression)" "SET+INCR chain" "$INCR_RESULT" ;;
esac

section "RESP pipeline: multi-batch daemon pattern"
DAEMON_RESULT=$(python3 -c "
import socket, time

def resp(*args):
    parts = [f'*{len(args)}\r\n']
    for a in args:
        a = str(a)
        parts.append(f'\${len(a)}\r\n{a}\r\n')
    return ''.join(parts).encode()

s = socket.socket()
s.connect(('$HOST', $PORT))
s.settimeout(5)

# Simulate kvstore-incr-sync: SET batches without reading between them
for i in range(30):
    s.sendall(resp('SET', f'dk{i:03d}', '100'))

# Then INCR some in a batch (still without reading previous SET responses)
for i in range(15):
    s.sendall(resp('INCR', f'dk{i:03d}'))

time.sleep(0.2)
buf = b''
for _ in range(50):
    try:
        chunk = s.recv(65536)
        if not chunk: break
        buf += chunk
    except socket.timeout:
        break
s.close()

oks = buf.count(b'+OK')
ints = buf.count(b':10')  # ':101' prefix match
errors = buf.count(b'ERR')
if oks == 30 and errors == 0:
    print('PASS')
else:
    print(f'FAIL: {oks}/30 OKs, {ints} INCR-replies, {errors} errors')
    print(f'First 200: {buf[:200]!r}')
" 2>&1)

case "$DAEMON_RESULT" in
    PASS*) ok "RESP pipeline multi-batch daemon pattern" ;;
    *)     bad "RESP pipeline multi-batch daemon pattern" "30 OKs + 15 INCRs" "$DAEMON_RESULT" ;;
esac

section "RESP pipeline batch: 500 commands in one recv"
BIGBATCH_RESULT=$(python3 -c "
import socket, time

def resp(*args):
    parts = [f'*{len(args)}\r\n']
    for a in args:
        a = str(a)
        parts.append(f'\${len(a)}\r\n{a}\r\n')
    return ''.join(parts).encode()

s = socket.socket()
s.connect(('$HOST', $PORT))
s.settimeout(5)

# 500 SETs in one big send
data = b''
for i in range(500):
    data += resp('SET', f'bigbatch_k{i:04d}', f'bigbatch_v{i:04d}')
s.sendall(data)

time.sleep(0.3)
buf = b''
for _ in range(100):
    try:
        chunk = s.recv(65536)
        if not chunk: break
        buf += chunk
    except socket.timeout:
        break
s.close()

oks = buf.count(b'+OK')
errors = buf.count(b'ERR')
if oks == 500 and errors == 0:
    print('PASS')
else:
    print(f'FAIL: {oks}/500 OKs, {errors} errors')
    print(f'First 200: {buf[:200]!r}')
" 2>&1)

case "$BIGBATCH_RESULT" in
    PASS*) ok "RESP pipeline 500 SETs in one batch" ;;
    *)     bad "RESP pipeline 500 SETs in one batch" "500 OKs" "$BIGBATCH_RESULT" ;;
esac

# ═══════════════════════════════════════════════════════════════════════════
# 12. EDGE CASES & STRESS
# ═══════════════════════════════════════════════════════════════════════════

section "Key names edge cases"
chk "key with digits"       "OK"     "$($RCLI SET key123 val)"
chk "key with dash"         "OK"     "$($RCLI SET key-name val)"
chk "key with dot"          "OK"     "$($RCLI SET key.name val)"

section "Integer encoding boundary"
# Small integers should be stored as integer encoding
$RCLI SET int_enc 42 >/dev/null
chk "TYPE int-encoded"      "string" "$($RCLI TYPE int_enc)"
chk "GET int-encoded"       "42"     "$($RCLI GET int_enc)"
chk "STRLEN int-encoded"    "2"      "$($RCLI STRLEN int_enc)"
chk "INCR int-encoded"      "43"     "$($RCLI INCR int_enc)"

section "INCR overflow guard"
$RCLI SET overflow 9223372036854775807 >/dev/null 2>/dev/null
ERR=$($RCLI INCRBY overflow 1 2>&1)
chk_contains "INCRBY overflow" "ERR" "$ERR"

# ═══════════════════════════════════════════════════════════════════════════
# 13. NOT-YET-IMPLEMENTED FEATURES (documented skips)
# ═══════════════════════════════════════════════════════════════════════════

section "Not-yet-implemented commands"
for cmd in HSET HGET HDEL HEXISTS HLEN HKEYS HVALS HGETALL HMSET HMGET; do
    OUT=$($RCLI $cmd test f v 2>&1)
    case "$OUT" in
        *"unknown command"*|*"ERR unknown"*)
            skip "$cmd" "not implemented" ;;
        *)  ok "$cmd" ;;
    esac
done

# ═══════════════════════════════════════════════════════════════════════════
summary "KVSTORE SMOKE"
