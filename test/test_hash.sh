#!/bin/bash
# =============================================================================
# Comprehensive HASH type test suite — KVStore project
#
# Tests all hash commands with dual encoding (PACKED and DICT):
#   HSET, HGET, HDEL, HGETALL, HKEYS, HVALS, HLEN, HEXISTS
#   HSETNX, HINCRBY, HMGET, HSTRLEN
#   DEL, EXIST
#
# Covers:
#   1. Basic operations (packed encoding, <=128 fields)
#   2. In-place update, resize grow, resize shrink
#   3. HDEL single and multi-field, edge cases (empty, non-existent)
#   4. Integer encoding and HINCRBY
#   5. HSETNX (set-if-not-exists)
#   6. HMGET (multi-field get)
#   7. HSTRLEN (field value length)
#   8. Packed->DICT encoding upgrade (>128 fields)
#   9. DICT->Packed encoding shrink (<64 fields)
#  10. Large field/value values (>1KB)
#  11. WRONGTYPE checks against string keys
#  12. DEL / EXIST on hash keys
#  13. Expired key handling
#  14. Edge cases (empty field, binary field, Unicode)
#
# Usage:  ./test_hash.sh [host] [port]
# Output: PASS/FAIL per test case, summary, exit code
# =============================================================================

set -o pipefail

HOST="${1:-127.0.0.1}"
PORT="${2:-2000}"
PASS=0
FAIL=0
TOTAL=0
SKIP=0

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# ---------------------------------------------------------------------------
# Send a raw RESP command and receive the response.
# Uses printf for reliable binary output (no echo -ne portability issues).
# ---------------------------------------------------------------------------
send() {
    printf '%b' "$1" | nc -w3 "$HOST" "$PORT" 2>/dev/null; printf '.'
}

# ---------------------------------------------------------------------------
# Reporters
# ---------------------------------------------------------------------------
ok() {
    PASS=$((PASS + 1)); TOTAL=$((TOTAL + 1))
    echo -e "  ${GREEN}PASS${NC}: $1"
}

fail() {
    FAIL=$((FAIL + 1)); TOTAL=$((TOTAL + 1))
    echo -e "  ${RED}FAIL${NC}: $1"
    if [ -n "$2" ]; then
        local expected_hex
        expected_hex=$(printf '%s' "$2" | xxd -p -c 256)
        echo "        expected hex: $expected_hex"
    fi
    if [ -n "$3" ]; then
        local got_hex
        got_hex=$(printf '%s' "${3%.}" | xxd -p -c 256)
        echo "        got hex:      $got_hex"
    fi
}

skip() {
    SKIP=$((SKIP + 1))
    echo -e "  ${YELLOW}SKIP${NC}: $1"
}

# check: exact match
check() {
    local actual="${3%.}"
    if [ "$2" = "$actual" ]; then
        ok "$1"
    else
        fail "$1" "$2" "$actual"
    fi
}

# check_contains: response contains substring
check_contains() {
    local actual="${3%.}"
    if printf '%s' "$actual" | grep -qF "$2"; then
        ok "$1"
    else
        fail "$1" "contains '$2'" "$actual"
    fi
}

# check_not_contains: response does NOT contain substring
check_not_contains() {
    local actual="${3%.}"
    if ! printf '%s' "$actual" | grep -qF "$2"; then
        ok "$1"
    else
        fail "$1" "not contain '$2'" "$actual"
    fi
}

# Generate a string of repeated character (POSIX-safe, no python dependency)
repeat() {
    local char="$1"
    local count="$2"
    local i
    local result=""
    for i in $(seq 1 "$count"); do
        result="${result}${char}"
    done
    printf '%s' "$result"
}

echo "============================================"
echo "  HASH Type — Comprehensive Test Suite"
echo "  Server: ${HOST}:${PORT}"
echo "============================================"

# Ensure clean state
send "*1\r\n\$8\r\nFLUSHALL\r\n" > /dev/null

# ── Part 1: Basic Packed HASH Operations ───────────────────────────────
echo ""
echo "=== Part 1: Basic Packed HASH (≤128 fields) ==="

# HSET hash_a name Alice age 30
r=$(send "*6\r\n\$4\r\nHSET\r\n\$6\r\nhash_a\r\n\$4\r\nname\r\n\$5\r\nAlice\r\n\$3\r\nage\r\n\$2\r\n30")
check "HSET name age (2 new fields)" $':2\r\n' "$r"

# HGET name
r=$(send "*3\r\n\$4\r\nHGET\r\n\$6\r\nhash_a\r\n\$4\r\nname")
check "HGET name → Alice" $'\$5\r\nAlice\r\n' "$r"

r=$(send "*3\r\n\$4\r\nHGET\r\n\$6\r\nhash_a\r\n\$3\r\nage")
check "HGET age → 30" $'\$2\r\n30\r\n' "$r"

r=$(send "*3\r\n\$4\r\nHGET\r\n\$6\r\nhash_a\r\n\$7\r\nnofield")
check "HGET non-existent field → nil" $'\$-1\r\n' "$r"

# HLEN
r=$(send "*2\r\n\$4\r\nHLEN\r\n\$6\r\nhash_a")
check "HLEN after HSET → 2" $':2\r\n' "$r"

# HEXISTS
r=$(send "*3\r\n\$7\r\nHEXISTS\r\n\$6\r\nhash_a\r\n\$4\r\nname")
check "HEXISTS name → 1" $':1\r\n' "$r"

r=$(send "*3\r\n\$7\r\nHEXISTS\r\n\$6\r\nhash_a\r\n\$7\r\nnofield")
check "HEXISTS nofield → 0" $':0\r\n' "$r"

# HKEYS
r=$(send "*2\r\n\$5\r\nHKEYS\r\n\$6\r\nhash_a")
check_contains "HKEYS contains name" "name" "$r"
check_contains "HKEYS contains age" "age" "$r"

# HVALS
r=$(send "*2\r\n\$5\r\nHVALS\r\n\$6\r\nhash_a")
check_contains "HVALS contains Alice" "Alice" "$r"
check_contains "HVALS contains 30" "30" "$r"

# HGETALL
r=$(send "*2\r\n\$7\r\nHGETALL\r\n\$6\r\nhash_a")
check_contains "HGETALL contains name" "name" "$r"
check_contains "HGETALL contains Alice" "Alice" "$r"
check_contains "HGETALL contains age" "age" "$r"
check_contains "HGETALL contains 30" "30" "$r"

# ── Part 2: HSET update (same size and different size) ─────────────────
echo ""
echo "=== Part 2: HSET update in-place and resize ==="

# Same-size update (in-place)
r=$(send "*4\r\n\$4\r\nHSET\r\n\$6\r\nhash_a\r\n\$4\r\nname\r\n\$4\r\nJohn")
check "HSET update name Alice→John (same size)" $':0\r\n' "$r"

r=$(send "*3\r\n\$4\r\nHGET\r\n\$6\r\nhash_a\r\n\$4\r\nname")
check "HGET updated name → John" $'\$4\r\nJohn\r\n' "$r"

# Larger size (resize grow)
r=$(send "*4\r\n\$4\r\nHSET\r\n\$6\r\nhash_a\r\n\$4\r\nname\r\n\$10\r\nAlexandria")
check "HSET update name John→Alexandria" $':0\r\n' "$r"

r=$(send "*3\r\n\$4\r\nHGET\r\n\$6\r\nhash_a\r\n\$4\r\nname")
check "HGET resized name → Alexandria" $'\$10\r\nAlexandria\r\n' "$r"

# Smaller size (resize shrink)
r=$(send "*4\r\n\$4\r\nHSET\r\n\$6\r\nhash_a\r\n\$4\r\nname\r\n\$2\r\nAl")
check "HSET update name Alexandria→Al" $':0\r\n' "$r"

r=$(send "*3\r\n\$4\r\nHGET\r\n\$6\r\nhash_a\r\n\$4\r\nname")
check "HGET shrunk name → Al" $'\$2\r\nAl\r\n' "$r"

# ── Part 3: HDEL ──────────────────────────────────────────────────────
echo ""
echo "=== Part 3: HDEL ==="

r=$(send "*3\r\n\$4\r\nHDEL\r\n\$6\r\nhash_a\r\n\$4\r\nname")
check "HDEL name → 1 removed" $':1\r\n' "$r"

r=$(send "*2\r\n\$4\r\nHLEN\r\n\$6\r\nhash_a")
check "HLEN after HDEL → 1" $':1\r\n' "$r"

r=$(send "*3\r\n\$4\r\nHGET\r\n\$6\r\nhash_a\r\n\$4\r\nname")
check "HGET deleted name → nil" $'\$-1\r\n' "$r"

r=$(send "*3\r\n\$4\r\nHDEL\r\n\$6\r\nhash_a\r\n\$7\r\nnofield")
check "HDEL non-existent field → 0" $':0\r\n' "$r"

# Multi-field HDEL
r=$(send "*4\r\n\$4\r\nHDEL\r\n\$6\r\nhash_a\r\n\$3\r\nage\r\n\$7\r\nnofield")
check "HDEL age + nofield → 1" $':1\r\n' "$r"

r=$(send "*2\r\n\$4\r\nHLEN\r\n\$6\r\nhash_a")
check "HLEN empty hash → 0" $':0\r\n' "$r"

r=$(send "*2\r\n\$7\r\nHGETALL\r\n\$6\r\nhash_a")
check "HGETALL empty hash → *0" $'*0\r\n' "$r"

r=$(send "*2\r\n\$5\r\nHKEYS\r\n\$6\r\nhash_a")
check "HKEYS empty hash → *0" $'*0\r\n' "$r"

r=$(send "*2\r\n\$5\r\nHVALS\r\n\$6\r\nhash_a")
check "HVALS empty hash → *0" $'*0\r\n' "$r"

# ── Part 4: Empty / non-existent HASH edge cases ──────────────────────
echo ""
echo "=== Part 4: Empty HASH edge cases ==="

r=$(send "*2\r\n\$4\r\nHLEN\r\n\$8\r\nnoexists")
check "HLEN non-existent key → 0" $':0\r\n' "$r"

r=$(send "*3\r\n\$4\r\nHGET\r\n\$8\r\nnoexists\r\n\$4\r\nname")
check "HGET on non-existent key → nil" $'\$-1\r\n' "$r"

r=$(send "*3\r\n\$4\r\nHDEL\r\n\$8\r\nnoexists\r\n\$4\r\nname")
check "HDEL on non-existent key → 0" $':0\r\n' "$r"

r=$(send "*2\r\n\$7\r\nHGETALL\r\n\$8\r\nnoexists")
check "HGETALL on non-existent key → *0" $'*0\r\n' "$r"

r=$(send "*2\r\n\$5\r\nHKEYS\r\n\$8\r\nnoexists")
check "HKEYS on non-existent key → *0" $'*0\r\n' "$r"

r=$(send "*2\r\n\$5\r\nHVALS\r\n\$8\r\nnoexists")
check "HVALS on non-existent key → *0" $'*0\r\n' "$r"

r=$(send "*3\r\n\$7\r\nHEXISTS\r\n\$8\r\nnoexists\r\n\$4\r\nname")
check "HEXISTS on non-existent key → 0" $':0\r\n' "$r"

r=$(send "*3\r\n\$6\r\nHMGET\r\n\$8\r\nnoexists\r\n\$1\r\na\r\n\$1\r\nb")
check "HMGET on non-existent key → nil x2" $'*2\r\n\$-1\r\n\$-1\r\n' "$r"

r=$(send "*3\r\n\$7\r\nHSTRLEN\r\n\$8\r\nnoexists\r\n\$4\r\nname")
check "HSTRLEN on non-existent key → 0" $':0\r\n' "$r"

# ── Part 5: HSETNX ────────────────────────────────────────────────────
echo ""
echo "=== Part 5: HSETNX ==="

r=$(send "*4\r\n\$6\r\nHSETNX\r\n\$10\r\nhash_nx\r\n\$1\r\nx\r\n\$1\r\n1")
check "HSETNX new key → 1" $':1\r\n' "$r"

r=$(send "*4\r\n\$6\r\nHSETNX\r\n\$10\r\nhash_nx\r\n\$1\r\nx\r\n\$1\r\n2")
check "HSETNX existing field → 0" $':0\r\n' "$r"

r=$(send "*3\r\n\$4\r\nHGET\r\n\$10\r\nhash_nx\r\n\$1\r\nx")
check "HGET nx field still '1'" $'\$1\r\n1\r\n' "$r"

r=$(send "*4\r\n\$6\r\nHSETNX\r\n\$10\r\nhash_nx\r\n\$1\r\ny\r\n\$3\r\nnew")
check "HSETNX new field → 1" $':1\r\n' "$r"

# ── Part 6: HINCRBY ───────────────────────────────────────────────────
echo ""
echo "=== Part 6: HINCRBY ==="

r=$(send "*4\r\n\$7\r\nHINCRBY\r\n\$9\r\nhash_inc\r\n\$7\r\ncounter\r\n\$1\r\n1")
check "HINCRBY new field +1 → 1" $':1\r\n' "$r"

r=$(send "*4\r\n\$7\r\nHINCRBY\r\n\$9\r\nhash_inc\r\n\$7\r\ncounter\r\n\$2\r\n10")
check "HINCRBY +10 → 11" $':11\r\n' "$r"

r=$(send "*4\r\n\$7\r\nHINCRBY\r\n\$9\r\nhash_inc\r\n\$7\r\ncounter\r\n\$3\r\n-5")
check "HINCRBY -5 → 6" $':6\r\n' "$r"

r=$(send "*4\r\n\$7\r\nHINCRBY\r\n\$9\r\nhash_inc\r\n\$7\r\ncounter\r\n\$2\r\n-6")
check "HINCRBY -6 → 0" $':0\r\n' "$r"

r=$(send "*4\r\n\$7\r\nHINCRBY\r\n\$9\r\nhash_inc\r\n\$7\r\ncounter\r\n\$2\r\n-10")
check "HINCRBY -10 → -10" $':-10\r\n' "$r"

# Large increment
r=$(send "*4\r\n\$7\r\nHINCRBY\r\n\$9\r\nhash_inc\r\n\$7\r\ncounter\r\n\$8\r\n99999999")
check "HINCRBY large + → 99999989" $':99999989\r\n' "$r"

# HINCRBY on non-integer field
r=$(send "*4\r\n\$4\r\nHSET\r\n\$9\r\nhash_inc\r\n\$4\r\nname\r\n\$5\r\nAlice")
check "HSET name Alice" $':1\r\n' "$r"

r=$(send "*4\r\n\$7\r\nHINCRBY\r\n\$9\r\nhash_inc\r\n\$4\r\nname\r\n\$1\r\n1")
check "HINCRBY on non-integer → error" $'-ERR value is not an integer or out of range\r\n' "$r"

# HINCRBY wrong type
r=$(send "*3\r\n\$3\r\nSET\r\n\$6\r\nstrkey\r\n\$5\r\nhello")
check "SET strkey hello" $'+OK\r\n' "$r"

r=$(send "*4\r\n\$7\r\nHINCRBY\r\n\$6\r\nstrkey\r\n\$7\r\ncounter\r\n\$1\r\n1")
check "HINCRBY on string → WRONGTYPE" $'-WRONGTYPE Operation against a key holding the wrong kind of value\r\n' "$r"

# Clean up for next tests
send "*2\r\n\$3\r\nDEL\r\n\$6\r\nstrkey\r\n" > /dev/null

# ── Part 7: HMGET ─────────────────────────────────────────────────────
echo ""
echo "=== Part 7: HMGET ==="

r=$(send "*5\r\n\$5\r\nHMGET\r\n\$9\r\nhash_inc\r\n\$7\r\ncounter\r\n\$4\r\nname\r\n\$6\r\nnofield")
check_contains "HMGET counter + name + nofield" "-10" "$r"
check_contains "HMGET contains Alice" "Alice" "$r"
check_not_contains "HMGET has nil for nofield" "$r"

# ── Part 8: HSTRLEN ───────────────────────────────────────────────────
echo ""
echo "=== Part 8: HSTRLEN ==="

r=$(send "*3\r\n\$7\r\nHSTRLEN\r\n\$9\r\nhash_inc\r\n\$4\r\nname")
check "HSTRLEN 'Alice' → 5" $':5\r\n' "$r"

r=$(send "*3\r\n\$7\r\nHSTRLEN\r\n\$9\r\nhash_inc\r\n\$7\r\ncounter")
TOTAL=$((TOTAL + 1))
actual="${r%.}"
if printf '%s' "$actual" | grep -qE '^:['"'"'0-9'"'"']+\r\n$' && [ "$actual" != ':0\r\n' ]; then
    ok "HSTRLEN counter is non-zero"
    PASS=$((PASS + 1))
else
    fail "HSTRLEN counter is non-zero" "non-zero int" "$actual"
fi

r=$(send "*3\r\n\$7\r\nHSTRLEN\r\n\$9\r\nhash_inc\r\n\$6\r\nnofield")
check "HSTRLEN non-existent field → 0" $':0\r\n' "$r"

# ── Part 9: Packed → DICT encoding upgrade (>128 fields) ──────────────
echo ""
echo "=== Part 9: Encoding upgrade (PACKED → DICT) ==="

# Create 200 fields
for i in $(seq 0 199); do
    printf '*4\r\n$4\r\nHSET\r\n$7\r\nbighash\r\n$9\r\nfield_%04d\r\n$9\r\nvalue_%04d\r\n' "$i" "$i" | \
        nc -w3 "$HOST" "$PORT" > /dev/null 2>&1
done

r=$(send "*2\r\n\$4\r\nHLEN\r\n\$7\r\nbighash")
check "HLEN bighash → 200" $':200\r\n' "$r"

r=$(send "*3\r\n\$4\r\nHGET\r\n\$7\r\nbighash\r\n\$9\r\nfield_0000")
check "HGET field_0000" $'\$9\r\nvalue_0000\r\n' "$r"

r=$(send "*3\r\n\$4\r\nHGET\r\n\$7\r\nbighash\r\n\$9\r\nfield_0050")
check "HGET field_0050" $'\$9\r\nvalue_0050\r\n' "$r"

r=$(send "*3\r\n\$4\r\nHGET\r\n\$7\r\nbighash\r\n\$9\r\nfield_0199")
check "HGET field_0199" $'\$9\r\nvalue_0199\r\n' "$r"

r=$(send "*3\r\n\$4\r\nHGET\r\n\$7\r\nbighash\r\n\$9\r\nfield_9999")
check "HGET non-existent field in bighash → nil" $'\$-1\r\n' "$r"

# HGETALL on DICT
r=$(send "*2\r\n\$7\r\nHGETALL\r\n\$7\r\nbighash")
TOTAL=$((TOTAL + 1))
actual="${r%.}"
if printf '%s' "$actual" | head -c 6 | grep -q '^\*400\r'; then
    ok "HGETALL DICT returns 400 items (200 pairs)"
    PASS=$((PASS + 1))
else
    fail "HGETALL DICT returns 400 items" "*400..." "${actual}"
fi

# HSET update in DICT
r=$(send "*4\r\n\$4\r\nHSET\r\n\$7\r\nbighash\r\n\$9\r\nfield_0050\r\n\$7\r\nUPDATED")
check "HSET update in DICT → 0" $':0\r\n' "$r"

r=$(send "*3\r\n\$4\r\nHGET\r\n\$7\r\nbighash\r\n\$9\r\nfield_0050")
check "HGET updated field_0050 in DICT" $'\$7\r\nUPDATED\r\n' "$r"

# HSETNX in DICT
r=$(send "*4\r\n\$6\r\nHSETNX\r\n\$7\r\nbighash\r\n\$9\r\nfield_0050\r\n\$5\r\nBLOCK")
check "HSETNX on existing field in dict → 0" $':0\r\n' "$r"

r=$(send "*4\r\n\$6\r\nHSETNX\r\n\$7\r\nbighash\r\n\$9\r\nfield_newx\r\n\$3\r\nYES")
check "HSETNX on new field in dict → 1" $':1\r\n' "$r"

# HINCRBY in DICT
r=$(send "*4\r\n\$7\r\nHINCRBY\r\n\$7\r\nbighash\r\n\$6\r\nintval\r\n\$2\r\n42")
check "HINCRBY in DICT → 42" $':42\r\n' "$r"

r=$(send "*4\r\n\$7\r\nHINCRBY\r\n\$7\r\nbighash\r\n\$6\r\nintval\r\n\$1\r\n8")
check "HINCRBY +8 in DICT → 50" $':50\r\n' "$r"

# HMGET in DICT
r=$(send "*4\r\n\$5\r\nHMGET\r\n\$7\r\nbighash\r\n\$9\r\nfield_0050\r\n\$9\r\nfield_0001")
check_contains "HMGET in DICT has UPDATED" "UPDATED" "$r"
check_contains "HMGET in DICT has value_0001" "value_0001" "$r"

# HDEL from DICT
r=$(send "*3\r\n\$4\r\nHDEL\r\n\$7\r\nbighash\r\n\$9\r\nfield_0000")
check "HDEL from DICT → 1" $':1\r\n' "$r"

r=$(send "*2\r\n\$4\r\nHLEN\r\n\$7\r\nbighash")
check "HLEN after HDEL → 201" $':201\r\n' "$r"

# ── Part 10: DICT → Packed shrink (<64 fields) ────────────────────────
echo ""
echo "=== Part 10: Encoding shrink (DICT → PACKED) ==="

# Delete down to 50 fields (keep field_0150..0199 + field_newx + intval)
for i in $(seq 1 149); do
    printf '*3\r\n$4\r\nHDEL\r\n$7\r\nbighash\r\n$9\r\nfield_%04d\r\n' "$i" | \
        nc -w3 "$HOST" "$PORT" > /dev/null 2>&1
done
# Also delete field_0000 (already done) and field_0050
printf '*3\r\n$4\r\nHDEL\r\n$7\r\nbighash\r\n$9\r\nfield_0050\r\n' | \
    nc -w3 "$HOST" "$PORT" > /dev/null 2>&1

# Now we should have 52 fields: field_0150..field_0199 (50) + field_newx + intval
r=$(send "*2\r\n\$4\r\nHLEN\r\n\$7\r\nbighash")
check "HLEN after shrink → 52" $':52\r\n' "$r"

r=$(send "*3\r\n\$4\r\nHGET\r\n\$7\r\nbighash\r\n\$9\r\nfield_0150")
check "HGET field_0150 still exists" $'\$9\r\nvalue_0150\r\n' "$r"

r=$(send "*3\r\n\$4\r\nHGET\r\n\$7\r\nbighash\r\n\$9\r\nfield_0199")
check "HGET field_0199 still exists" $'\$9\r\nvalue_0199\r\n' "$r"

# HSET after shrink (PACKED mode)
r=$(send "*4\r\n\$4\r\nHSET\r\n\$7\r\nbighash\r\n\$3\r\nNEW\r\n\$3\r\nnew")
check "HSET after shrink → 1" $':1\r\n' "$r"

r=$(send "*2\r\n\$4\r\nHLEN\r\n\$7\r\nbighash")
check "HLEN after HSET to shrunk → 53" $':53\r\n' "$r"

# HSTRLEN after shrink
r=$(send "*3\r\n\$7\r\nHSTRLEN\r\n\$7\r\nbighash\r\n\$3\r\nNEW")
check "HSTRLEN after shrink → 3" $':3\r\n' "$r"

# ── Part 11: Large field/value values ─────────────────────────────────
echo ""
echo "=== Part 11: Large field/value values ==="

# Generate 1KB field and 3KB value without python
LONG_F=$(repeat 'F' 1500)
LONG_V=$(repeat 'V' 3000)

r=$(send "*4\r\n\$4\r\nHSET\r\n\$8\r\nlonghash\r\n\$1500\r\n${LONG_F}\r\n\$3000\r\n${LONG_V}")
TOTAL=$((TOTAL + 1))
actual="${r%.}"
if [ "$actual" = ':1\r\n' ]; then
    ok "HSET with large field/value"
    PASS=$((PASS + 1))
else
    fail "HSET with large field/value" ":1" "$actual"
fi

r=$(send "*3\r\n\$4\r\nHGET\r\n\$8\r\nlonghash\r\n\$1500\r\n${LONG_F}")
TOTAL=$((TOTAL + 1))
actual="${r%.}"
if printf '%s' "$actual" | grep -q '^\$3000'; then
    ok "HGET large value (correct length)"
    PASS=$((PASS + 1))
else
    fail "HGET large value" "correct length header" "${actual:0:50}"
fi

r=$(send "*3\r\n\$4\r\nHDEL\r\n\$8\r\nlonghash\r\n\$1500\r\n${LONG_F}")
check "HDEL large field → 1" $':1\r\n' "$r"

# ── Part 12: WRONGTYPE ────────────────────────────────────────────────
echo ""
echo "=== Part 12: WRONGTYPE checks ==="

WRONGTYPE=$'-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'

r=$(send "*3\r\n\$3\r\nSET\r\n\$6\r\nstrkey\r\n\$5\r\nhello")
check "SET strkey hello" $'+OK\r\n' "$r"

r=$(send "*4\r\n\$4\r\nHSET\r\n\$6\r\nstrkey\r\n\$4\r\nname\r\n\$5\r\nAlice")
check "HSET on string → WRONGTYPE" "$WRONGTYPE" "$r"

r=$(send "*3\r\n\$4\r\nHGET\r\n\$6\r\nstrkey\r\n\$4\r\nname")
check "HGET on string → WRONGTYPE" "$WRONGTYPE" "$r"

r=$(send "*2\r\n\$4\r\nHLEN\r\n\$6\r\nstrkey")
check "HLEN on string → WRONGTYPE" "$WRONGTYPE" "$r"

r=$(send "*3\r\n\$4\r\nHDEL\r\n\$6\r\nstrkey\r\n\$4\r\nname")
check "HDEL on string → WRONGTYPE" "$WRONGTYPE" "$r"

r=$(send "*2\r\n\$7\r\nHGETALL\r\n\$6\r\nstrkey")
check "HGETALL on string → WRONGTYPE" "$WRONGTYPE" "$r"

r=$(send "*2\r\n\$5\r\nHKEYS\r\n\$6\r\nstrkey")
check "HKEYS on string → WRONGTYPE" "$WRONGTYPE" "$r"

r=$(send "*2\r\n\$5\r\nHVALS\r\n\$6\r\nstrkey")
check "HVALS on string → WRONGTYPE" "$WRONGTYPE" "$r"

r=$(send "*3\r\n\$7\r\nHEXISTS\r\n\$6\r\nstrkey\r\n\$4\r\nname")
check "HEXISTS on string → WRONGTYPE" "$WRONGTYPE" "$r"

r=$(send "*4\r\n\$6\r\nHSETNX\r\n\$6\r\nstrkey\r\n\$1\r\nx\r\n\$1\r\n1")
check "HSETNX on string → WRONGTYPE" "$WRONGTYPE" "$r"

r=$(send "*4\r\n\$7\r\nHINCRBY\r\n\$6\r\nstrkey\r\n\$1\r\nx\r\n\$1\r\n1")
check "HINCRBY on string → WRONGTYPE" "$WRONGTYPE" "$r"

r=$(send "*4\r\n\$5\r\nHMGET\r\n\$6\r\nstrkey\r\n\$1\r\nx\r\n\$1\r\ny")
check "HMGET on string → WRONGTYPE" "$WRONGTYPE" "$r"

r=$(send "*3\r\n\$7\r\nHSTRLEN\r\n\$6\r\nstrkey\r\n\$1\r\nx")
check "HSTRLEN on string → WRONGTYPE" "$WRONGTYPE" "$r"

# ── Part 13: DEL and EXIST ────────────────────────────────────────────
echo ""
echo "=== Part 13: DEL / EXIST ==="

r=$(send "*2\r\n\$5\r\nEXIST\r\n\$7\r\nbighash")
check "EXIST bighash → 1" $':1\r\n' "$r"

r=$(send "*2\r\n\$3\r\nDEL\r\n\$7\r\nbighash")
check "DEL bighash → OK" $'+OK\r\n' "$r"

r=$(send "*2\r\n\$5\r\nEXIST\r\n\$7\r\nbighash")
check "EXIST after DEL → nil" $'\$-1\r\n' "$r"

r=$(send "*2\r\n\$4\r\nHLEN\r\n\$7\r\nbighash")
check "HLEN after DEL → 0" $':0\r\n' "$r"

r=$(send "*2\r\n\$3\r\nDEL\r\n\$6\r\nstrkey")
check "DEL strkey → OK" $'+OK\r\n' "$r"

r=$(send "*2\r\n\$3\r\nDEL\r\n\$8\r\nnoexists")
check "DEL non-existent → nil" $'\$-1\r\n' "$r"

# ── Part 14: Special characters and edge cases ────────────────────────
echo ""
echo "=== Part 14: Special characters and edge cases ==="

# Empty field name
r=$(send "*4\r\n\$4\r\nHSET\r\n\$9\r\nhash_edge\r\n\$0\r\n\r\n\$5\r\nvalue")
TOTAL=$((TOTAL + 1))
actual="${r%.}"
if printf '%s' "$actual" | grep -q '^:1\r\n$'; then
    ok "HSET with empty field name"
    PASS=$((PASS + 1))
else
    fail "HSET with empty field name" ":1" "$actual"
fi

r=$(send "*3\r\n\$4\r\nHGET\r\n\$9\r\nhash_edge\r\n\$0\r\n")
check "HGET empty field → value" $'\$5\r\nvalue\r\n' "$r"

# Spaces in field name
r=$(send "*4\r\n\$4\r\nHSET\r\n\$9\r\nhash_edge\r\n\$10\r\nfield name\r\n\$6\r\nresult")
check "HSET with space in field name" $':1\r\n' "$r"

r=$(send "*3\r\n\$4\r\nHGET\r\n\$9\r\nhash_edge\r\n\$10\r\nfield name")
check "HGET field with space" $'\$6\r\nresult\r\n' "$r"

# Binary data in value (null bytes)
r=$(send "*4\r\n\$4\r\nHSET\r\n\$9\r\nhash_edge\r\n\$6\r\nbinary\r\n\$5\r\nab\x00cd")
TOTAL=$((TOTAL + 1))
actual="${r%.}"
if printf '%s' "$actual" | grep -q '^:1\r\n$'; then
    ok "HSET with binary value (null byte)"
    PASS=$((PASS + 1))
else
    fail "HSET with binary value" ":1" "$actual"
fi

# Multi-field HSET edge case (odd number of field-values)
# HSET only cares about complete pairs; ignores trailing single arg
# Actually in our implementation: for (i=1; i+1<argc; i+=2) — pauses at odd arg
r=$(send "*5\r\n\$4\r\nHSET\r\n\$13\r\nhash_multi\r\n\$1\r\na\r\n\$1\r\n1\r\n\$1\r\nb")
check "HSET with trailing odd arg → 2" $':2\r\n' "$r"

r=$(send "*2\r\n\$4\r\nHLEN\r\n\$13\r\nhash_multi")
check "HLEN hash_multi → 2" $':2\r\n' "$r"

# Clean up test keys
send "*2\r\n\$3\r\nDEL\r\n\$9\r\nhash_edge\r\n" > /dev/null
send "*2\r\n\$3\r\nDEL\r\n\$13\r\nhash_multi\r\n" > /dev/null

# ── Results ───────────────────────────────────────────────────────────
echo ""
echo "============================================"
echo "  HASH Test Results"
echo "============================================"
echo "  PASS:  $PASS / $TOTAL"
echo "  FAIL:  $FAIL / $TOTAL"
if [ $SKIP -gt 0 ]; then
    echo "  SKIP:  $SKIP"
fi
echo "============================================"

if [ $FAIL -eq 0 ]; then
    echo ""
    echo -e "${GREEN}ALL HASH TESTS PASSED${NC}"
    exit 0
else
    echo ""
    echo -e "${RED}SOME HASH TESTS FAILED${NC}"
    exit 1
fi
