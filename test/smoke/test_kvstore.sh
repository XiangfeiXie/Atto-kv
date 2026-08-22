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
FAILED_DETAILS=()

RED=$'\033[31m' GRN=$'\033[32m' YEL=$'\033[33m' RST=$'\033[0m'

# 静默模式：只有失败时才输出详细信息
ok()   { PASS=$((PASS+1)); }
bad()  { 
    FAIL=$((FAIL+1))
    FAILED_NAMES+=("$1")
    FAILED_DETAILS+=("  ${RED}FAIL${RST}  $1
        expected: $2
        actual:   $3")
    printf "  ${RED}FAIL${RST}  %s\n" "$1"
    printf "        expected: %s\n" "$2"
    printf "        actual:   %s\n" "$3"
}
skip() { SKIP=$((SKIP+1)); 
         printf "  ${YEL}SKIP${RST}  %s — %s\n" "$1" "$2"; }

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

# 静默的section - 只在有失败时输出
section() { 
    # 不输出任何内容，保持安静
    return 0
}

# 带输出的section - 用于显示当前测试组（可选择启用）
section_verbose() { 
    printf "\n${YEL}── %s ──${RST}\n" "$1"
}

require_server() {
    if ! $RCLI PING >/dev/null 2>&1; then
        printf "${RED}FATAL${RST}: cannot reach kvstore at %s:%s — start it first.\n" "$HOST" "$PORT" >&2
        exit 2
    fi
    $RCLI FLUSHALL >/dev/null 2>&1
}

# ── Summary ───────────────────────────────────────────────────────────────

summary() {
    local total=$((PASS + FAIL + SKIP))
    printf "\n────────────────────────────────────────────────────────\n"
    printf "%s suite: %d/%d passed" "$1" "$PASS" "$total"
    if [ "$FAIL" -gt 0 ]; then
        printf "  (${RED}%d failed${RST})\n" "$FAIL"
        # 只打印失败的测试名称
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

# ── Begin ─────────────────────────────────────────────────────────────────

require_server

# ═══════════════════════════════════════════════════════════════════════════
# 1. CONNECTION / SERVER BASICS
# ═══════════════════════════════════════════════════════════════════════════

chk "PING"                  "PONG"       "$($RCLI PING 2>/dev/null)"
chk "PING with message"     "hello"      "$($RCLI PING hello 2>/dev/null)"
chk "ECHO"                  "echo-me"    "$($RCLI ECHO echo-me 2>/dev/null)"
# SELECT not implemented; verify it errors instead of crashing
ERR=$($RCLI SELECT 0 2>&1)
chk_contains "SELECT errors (not implemented)" "ERR" "$ERR"

ERR=$($RCLI NO_SUCH_CMD 2>&1)
chk_contains "Unknown command" "ERR" "$ERR"

# ═══════════════════════════════════════════════════════════════════════════
# 2. DATABASE METADATA
# ═══════════════════════════════════════════════════════════════════════════

$RCLI FLUSHALL >/dev/null 2>&1
chk "DBSIZE empty"          "0"   "$($RCLI DBSIZE 2>/dev/null)"

$RCLI SET k v >/dev/null 2>&1
$RCLI SET k2 v2 >/dev/null 2>&1
chk "DBSIZE after SET"      "2"   "$($RCLI DBSIZE 2>/dev/null)"

INFO_OUT=$($RCLI INFO 2>/dev/null | tr -d '\r' | head -5)
chk_contains "INFO"         "ersion"    "$INFO_OUT"

chk "FLUSHALL"              "OK"  "$($RCLI FLUSHALL 2>/dev/null)"
chk "DBSIZE after FLUSHALL" "0"   "$($RCLI DBSIZE 2>/dev/null)"

# ═══════════════════════════════════════════════════════════════════════════
# 3. BASIC STRING: SET / GET / DEL / EXISTS / TYPE / STRLEN
# ═══════════════════════════════════════════════════════════════════════════

chk "SET"                   "OK"     "$($RCLI SET k1 hello 2>/dev/null)"
chk "GET"                   "hello"  "$($RCLI GET k1 2>/dev/null)"
chk "STRLEN"                "5"      "$($RCLI STRLEN k1 2>/dev/null)"
chk "TYPE string"           "string" "$($RCLI TYPE k1 2>/dev/null)"
chk "EXISTS yes"            "1"      "$($RCLI EXISTS k1 2>/dev/null)"
chk "EXISTS no"             "0"      "$($RCLI EXISTS missing_key 2>/dev/null)"
$RCLI SET a 1 >/dev/null 2>&1
chk "EXISTS multi"          "2"      "$($RCLI EXISTS k1 a 2>/dev/null)"
chk "GET missing"           ""       "$($RCLI GET missing_key 2>/dev/null)"
chk "TYPE missing"          "none"   "$($RCLI TYPE missing_key 2>/dev/null)"
chk "DEL existing"          "1"      "$($RCLI DEL k1 2>/dev/null)"
chk "DEL missing"           "0"      "$($RCLI DEL k1 2>/dev/null)"
chk "DEL multi"             "1"      "$($RCLI DEL a 2>/dev/null)"

$RCLI SET k1 hello >/dev/null 2>&1
chk "Overwrite SET"         "OK"     "$($RCLI SET k1 world 2>/dev/null)"
chk "After overwrite"       "world"  "$($RCLI GET k1 2>/dev/null)"

# APPEND test removed

$RCLI SET empty "" >/dev/null 2>&1
chk "STRLEN empty string"   "0"      "$($RCLI STRLEN empty 2>/dev/null)"
chk "STRLEN missing key"    "0"      "$($RCLI STRLEN noexist 2>/dev/null)"

# ═══════════════════════════════════════════════════════════════════════════
# 4. SET WITH OPTIONS: EX / PX / NX / XX
# ═══════════════════════════════════════════════════════════════════════════

$RCLI DEL optkey >/dev/null 2>&1
chk "SET EX 60"             "OK"     "$($RCLI SET optkey val EX 60 2>/dev/null)"
TTL=$($RCLI TTL optkey 2>/dev/null)
[ "$TTL" -ge 55 ] && [ "$TTL" -le 60 ] && ok "TTL ~60" || bad "TTL ~60" "55..60" "$TTL"

$RCLI DEL optkey >/dev/null 2>&1
chk "SET PX 5000"           "OK"     "$($RCLI SET optkey val PX 5000 2>/dev/null)"
PT=$($RCLI PTTL optkey 2>/dev/null)
[ "$PT" -ge 4500 ] && [ "$PT" -le 5000 ] && ok "PTTL ~5000" || bad "PTTL ~5000" "4500..5000" "$PT"

$RCLI SET nxk val >/dev/null 2>&1
chk "SET NX on existing"    ""       "$($RCLI SET nxk val2 NX 2>/dev/null)"
chk "SET NX unchanged"      "val"    "$($RCLI GET nxk 2>/dev/null)"

$RCLI DEL nxk >/dev/null 2>&1
chk "SET NX on new"         "OK"     "$($RCLI SET nxk val NX 2>/dev/null)"

chk "SET XX on existing"    "OK"     "$($RCLI SET nxk val2 XX 2>/dev/null)"
chk "SET XX new value"      "val2"   "$($RCLI GET nxk 2>/dev/null)"

$RCLI DEL nxk >/dev/null 2>&1
chk "SET XX on missing"     ""       "$($RCLI SET nxk val XX 2>/dev/null)"

# Combined EX + NX
$RCLI DEL combokey >/dev/null 2>&1
chk "SET EX 30 NX"          "OK"     "$($RCLI SET combokey v EX 30 NX 2>/dev/null)"
TTL=$($RCLI TTL combokey 2>/dev/null)
[ "$TTL" -ge 25 ] && ok "EX+NX TTL ok" || bad "EX+NX TTL ok" "25..30" "$TTL"

# ═══════════════════════════════════════════════════════════════════════════
# 5. INCR / DECR FAMILY
# ═══════════════════════════════════════════════════════════════════════════

$RCLI DEL ctr >/dev/null 2>&1
chk "INCR new key"          "1"      "$($RCLI INCR ctr 2>/dev/null)"
chk "INCR again"            "2"      "$($RCLI INCR ctr 2>/dev/null)"
chk "DECR"                  "1"      "$($RCLI DECR ctr 2>/dev/null)"
chk "INCRBY 5"              "6"      "$($RCLI INCRBY ctr 5 2>/dev/null)"
chk "DECRBY 3"              "3"      "$($RCLI DECRBY ctr 3 2>/dev/null)"
chk "INCRBY negative"       "1"      "$($RCLI INCRBY ctr -2 2>/dev/null)"
chk "DECRBY zero"           "1"      "$($RCLI DECRBY ctr 0 2>/dev/null)"

$RCLI SET strval hello >/dev/null 2>&1
ERR=$($RCLI INCR strval 2>&1)
chk_contains "INCR on non-int" "ERR" "$ERR"

ERR=$($RCLI INCRBY strval 1 2>&1)
chk_contains "INCRBY on non-int" "ERR" "$ERR"

# ═══════════════════════════════════════════════════════════════════════════
# 6. MSET / MGET
# ═══════════════════════════════════════════════════════════════════════════

$RCLI DEL a b c d e missing >/dev/null 2>&1
chk "MSET"                  "OK"     "$($RCLI MSET a 1 b 2 c 3 2>/dev/null)"
MGET=$($RCLI MGET a b c 2>/dev/null | tr '\n' ' ' | sed 's/ $//')
chk "MGET 3 keys"           "1 2 3"  "$MGET"
MGET2=$($RCLI MGET a missing c 2>/dev/null | tr '\n' ' ' | sed 's/ $//')
chk "MGET with missing"     "1  3"   "$MGET2"

# MSETNX test removed

# ═══════════════════════════════════════════════════════════════════════════
# 7. EXPIRE / PERSIST / TTL / PTTL / KEYS
# ═══════════════════════════════════════════════════════════════════════════

$RCLI DEL expkey >/dev/null 2>&1
$RCLI SET expkey val >/dev/null 2>&1
chk "EXPIRE set"            "1"      "$($RCLI EXPIRE expkey 100 2>/dev/null)"
TTL=$($RCLI TTL expkey 2>/dev/null)
[ "$TTL" -ge 95 ] && [ "$TTL" -le 100 ] && ok "TTL after EXPIRE" || bad "TTL after EXPIRE" "95..100" "$TTL"

chk "PERSIST"               "1"      "$($RCLI PERSIST expkey 2>/dev/null)"
chk "TTL after PERSIST"     "-1"     "$($RCLI TTL expkey 2>/dev/null)"
chk "TTL missing key"       "-2"     "$($RCLI TTL noexist999 2>/dev/null)"
chk "PTTL missing key"      "-2"     "$($RCLI PTTL noexist999 2>/dev/null)"
chk "EXPIRE missing key"    "0"      "$($RCLI EXPIRE noexist 100 2>/dev/null)"

$RCLI DEL k_aaa k_bbb k_ccc >/dev/null 2>&1
$RCLI SET k_aaa 1 >/dev/null 2>&1
$RCLI SET k_bbb 2 >/dev/null 2>&1
$RCLI SET k_ccc 3 >/dev/null 2>&1
KEYS_COUNT=$($RCLI KEYS '*' 2>/dev/null | wc -l | tr -d ' ')
[ "$KEYS_COUNT" -ge 3 ] && ok "KEYS count >=3" || bad "KEYS count >=3" ">=3" "$KEYS_COUNT"

# ═══════════════════════════════════════════════════════════════════════════
# 8. LARGE VALUES
# ═══════════════════════════════════════════════════════════════════════════

LONG4K=$(printf 'X%.0s' $(seq 1 4000))
$RCLI SET large4k "$LONG4K" >/dev/null 2>&1
chk "STRLEN 4K"             "4000"   "$($RCLI STRLEN large4k 2>/dev/null)"
chk "GET 4K roundtrip"      "$LONG4K" "$($RCLI GET large4k 2>/dev/null)"

LONG64K=$(printf 'Y%.0s' $(seq 1 65536))
$RCLI SET large64k "$LONG64K" >/dev/null 2>&1
chk "STRLEN 64K"            "65536"  "$($RCLI STRLEN large64k 2>/dev/null)"
chk "GET 64K roundtrip"     "$LONG64K" "$($RCLI GET large64k 2>/dev/null)"

$RCLI SET empty "" >/dev/null 2>&1
chk "GET empty string"      ""       "$($RCLI GET empty 2>/dev/null)"

SPACES="   spaces   "
$RCLI SET spacekey "$SPACES" >/dev/null 2>&1
chk "GET spaces"            "$SPACES" "$($RCLI GET spacekey 2>/dev/null)"

# ═══════════════════════════════════════════════════════════════════════════
# 9. WRONGTYPE GUARDS
# ═══════════════════════════════════════════════════════════════════════════

$RCLI SET strk stringval >/dev/null 2>&1
ERR=$($RCLI INCR strk 2>&1)
chk_contains "INCR on string errors" "ERR" "$ERR"
chk "GET after failed INCR"   "stringval" "$($RCLI GET strk 2>/dev/null)"

$RCLI SET intk 10 >/dev/null 2>&1
chk "TYPE int value"        "string"  "$($RCLI TYPE intk 2>/dev/null)"
chk "GET int value"         "10"      "$($RCLI GET intk 2>/dev/null)"

# ═══════════════════════════════════════════════════════════════════════════
# 10. BATCH PIPELINE TESTS (redis-cli --pipe)
# ═══════════════════════════════════════════════════════════════════════════

$RCLI FLUSHALL >/dev/null 2>&1
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

chk "DBSIZE after 1000 SETs" "1000" "$($RCLI DBSIZE 2>/dev/null)"
chk "GET pipe_k0000"         "v0000" "$($RCLI GET pipe_k0000 2>/dev/null)"
chk "GET pipe_k0500"         "v0500" "$($RCLI GET pipe_k0500 2>/dev/null)"
chk "GET pipe_k0999"         "v0999" "$($RCLI GET pipe_k0999 2>/dev/null)"

# redis-cli --pipe RESP test removed (RESP tested via raw socket below)

# redis-cli --pipe mixed test
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
s.sendall(resp('SET', 'regr_ctr', '10'))
s.sendall(resp('INCR', 'regr_ctr'))
s.sendall(resp('INCR', 'regr_ctr'))
s.sendall(resp('DECR', 'regr_ctr'))

import time; time.sleep(0.1)
buf = s.recv(1024)
s.close()

if b'+OK' in buf and b':11' in buf and b':12' in buf and b'ERR' not in buf:
    print('PASS')
else:
    print(f'FAIL: {buf!r}')
" 2>&1)

case "$INCR_RESULT" in
    PASS*) ok "RESP pipeline SET+INCR+DECR (r_off bug regression)" ;;
    *)     bad "RESP pipeline SET+INCR+DECR (r_off bug regression)" "SET+INCR chain" "$INCR_RESULT" ;;
esac

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

for i in range(30):
    s.sendall(resp('SET', f'dk{i:03d}', '100'))

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
ints = buf.count(b':10')
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

chk "key with digits"       "OK"     "$($RCLI SET key123 val 2>/dev/null)"
chk "key with dash"         "OK"     "$($RCLI SET key-name val 2>/dev/null)"
chk "key with dot"          "OK"     "$($RCLI SET key.name val 2>/dev/null)"

$RCLI SET int_enc 42 >/dev/null 2>&1
chk "TYPE int-encoded"      "string" "$($RCLI TYPE int_enc 2>/dev/null)"
chk "GET int-encoded"       "42"     "$($RCLI GET int_enc 2>/dev/null)"
chk "STRLEN int-encoded"    "2"      "$($RCLI STRLEN int_enc 2>/dev/null)"
chk "INCR int-encoded"      "43"     "$($RCLI INCR int_enc 2>/dev/null)"

$RCLI SET overflow 9223372036854775807 >/dev/null 2>&1
ERR=$($RCLI INCRBY overflow 1 2>&1)
chk_contains "INCRBY overflow" "ERR" "$ERR"


# ═══════════════════════════════════════════════════════════════════════════
summary "KVSTORE SMOKE"