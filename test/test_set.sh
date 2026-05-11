#!/bin/bash
# Comprehensive SET type test suite
HOST="127.0.0.1"
PORT=2000
PASS=0
FAIL=0
TOTAL=0

send() {
    local result
    result=$(echo -ne "$1\r\n" | nc -w2 $HOST $PORT 2>/dev/null; echo .)
    printf '%s.' "${result%.}"
}

ok() {
    PASS=$((PASS + 1)); TOTAL=$((TOTAL + 1))
    echo "  PASS: $1"
}

fail() {
    FAIL=$((FAIL + 1)); TOTAL=$((TOTAL + 1))
    echo "  FAIL: $1"
    echo "        expected: $(echo -n "$2" | xxd | head -1)"
    echo "        got:      $(echo -n "$3" | xxd | head -1)"
}

check() {
    local actual="${3%.}"
    if [ "$2" = "$actual" ]; then ok "$1"; else fail "$1" "$2" "$actual"; fi
}

check_contains() {
    local actual="${3%.}"
    if echo "$actual" | grep -qF "$2"; then ok "$1"; else fail "$1" "contains '$2'" "$actual"; fi
}

echo "============================================"
echo "  SET Type — Comprehensive Test Suite"
echo "============================================"

# Ensure clean state
send "*1\r\n\$8\r\nFLUSHALL" > /dev/null

# ── Part 1: Basic Packed SET Operations ──────────────────────────
echo ""
echo "=== Part 1: Basic Packed SET (≤128 elements) ==="

# SADD key a b c  → 5 elements (SADD + key + a + b + c)
r=$(send "*5\r\n\$4\r\nSADD\r\n\$5\r\nset_a\r\n\$1\r\na\r\n\$1\r\nb\r\n\$1\r\nc")
check "SADD 3 elements" $':3\r\n' "$r"

# SCARD key  → 2 elements
r=$(send "*2\r\n\$5\r\nSCARD\r\n\$5\r\nset_a")
check "SCARD after SADD" $':3\r\n' "$r"

# SISMEMBER key a  → 3 elements
r=$(send "*3\r\n\$9\r\nSISMEMBER\r\n\$5\r\nset_a\r\n\$1\r\na")
check "SISMEMBER a → 1" $':1\r\n' "$r"

r=$(send "*3\r\n\$9\r\nSISMEMBER\r\n\$5\r\nset_a\r\n\$1\r\nz")
check "SISMEMBER z → 0" $':0\r\n' "$r"

# SMEMBERS key  → 2 elements
r=$(send "*2\r\n\$8\r\nSMEMBERS\r\n\$5\r\nset_a")
check_contains "SMEMBERS has a" "a" "$r"
check_contains "SMEMBERS has b" "b" "$r"
check_contains "SMEMBERS has c" "c" "$r"

# SADD key a (duplicate)  → 3 elements
r=$(send "*3\r\n\$4\r\nSADD\r\n\$5\r\nset_a\r\n\$1\r\na")
check "SADD duplicate a" $':0\r\n' "$r"

r=$(send "*2\r\n\$5\r\nSCARD\r\n\$5\r\nset_a")
check "SCARD still 3" $':3\r\n' "$r"

# SREM key b  → 3 elements
r=$(send "*3\r\n\$4\r\nSREM\r\n\$5\r\nset_a\r\n\$1\r\nb")
check "SREM b" $':1\r\n' "$r"

r=$(send "*2\r\n\$5\r\nSCARD\r\n\$5\r\nset_a")
check "SCARD after SREM" $':2\r\n' "$r"

r=$(send "*3\r\n\$9\r\nSISMEMBER\r\n\$5\r\nset_a\r\n\$1\r\nb")
check "SISMEMBER b after SREM → 0" $':0\r\n' "$r"

# SREM key z (non-existent)  → 3 elements
r=$(send "*3\r\n\$4\r\nSREM\r\n\$5\r\nset_a\r\n\$1\r\nz")
check "SREM non-existent z" $':0\r\n' "$r"

# SREM key a c  → 4 elements
r=$(send "*4\r\n\$4\r\nSREM\r\n\$5\r\nset_a\r\n\$1\r\na\r\n\$1\r\nc")
check "SREM a c" $':2\r\n' "$r"

r=$(send "*2\r\n\$5\r\nSCARD\r\n\$5\r\nset_a")
check "SCARD after all removed" $':0\r\n' "$r"

r=$(send "*2\r\n\$8\r\nSMEMBERS\r\n\$5\r\nset_a")
check "SMEMBERS empty set" $'*0\r\n' "$r"

# SRANDMEMBER on empty
r=$(send "*2\r\n\$11\r\nSRANDMEMBER\r\n\$5\r\nset_a")
check "SRANDMEMBER on empty" $'$-1\r\n' "$r"

# SPOP on empty
r=$(send "*2\r\n\$4\r\nSPOP\r\n\$5\r\nset_a")
check "SPOP on empty" $'$-1\r\n' "$r"

# ── Part 2: SPOP and SRANDMEMBER on non-empty set ───────────────
echo ""
echo "=== Part 2: SPOP & SRANDMEMBER ==="

# SADD key x y z w  → 6 elements
r=$(send "*6\r\n\$4\r\nSADD\r\n\$6\r\nset_sp\r\n\$1\r\nx\r\n\$1\r\ny\r\n\$1\r\nz\r\n\$1\r\nw")
check "SADD 4 elements" $':4\r\n' "$r"

# SRANDMEMBER key  → 2 elements
r=$(send "*2\r\n\$11\r\nSRANDMEMBER\r\n\$6\r\nset_sp")
TOTAL=$((TOTAL + 1))
actual="${r%.}"
if [ "$actual" != '$-1\r\n' ]; then
    ok "SRANDMEMBER returns element (not nil)"
    PASS=$((PASS + 1))
else
    fail "SRANDMEMBER returns element" "not nil" "$actual"
fi

# SPOP key  → 2 elements
r=$(send "*2\r\n\$4\r\nSPOP\r\n\$6\r\nset_sp")
TOTAL=$((TOTAL + 1))
actual="${r%.}"
if [ "$actual" != '$-1\r\n' ]; then
    ok "SPOP returns element (not nil)"
    PASS=$((PASS + 1))
else
    fail "SPOP returns element" "not nil" "$actual"
fi

r=$(send "*2\r\n\$5\r\nSCARD\r\n\$6\r\nset_sp")
check "SCARD after SPOP = 3" $':3\r\n' "$r"

# ── Part 3: Empty set edge cases ─────────────────────────────────
echo ""
echo "=== Part 3: Empty SET edge cases ==="

r=$(send "*2\r\n\$5\r\nSCARD\r\n\$8\r\nnoexists")
check "SCARD non-existent key" $':0\r\n' "$r"

r=$(send "*3\r\n\$9\r\nSISMEMBER\r\n\$8\r\nnoexists\r\n\$1\r\na")
check "SISMEMBER on non-existent key" $':0\r\n' "$r"

r=$(send "*2\r\n\$8\r\nSMEMBERS\r\n\$8\r\nnoexists")
check "SMEMBERS on non-existent key" $'*0\r\n' "$r"

r=$(send "*3\r\n\$4\r\nSREM\r\n\$8\r\nnoexists\r\n\$1\r\na")
check "SREM on non-existent key" $':0\r\n' "$r"

# ── Part 4: Packed → DICT encoding upgrade (>128 elements) ──────
echo ""
echo "=== Part 4: Encoding upgrade (PACKED → DICT) ==="

# Send 200 elements as individual SADD commands
for i in $(seq 0 199); do
    msg=$(printf '*3\r\n\$4\r\nSADD\r\n\$6\r\nbigset\r\n\$9\r\nelem_%04d\r\n' $i)
    send "$msg"
done > /dev/null

r=$(send "*2\r\n\$5\r\nSCARD\r\n\$6\r\nbigset")
check "SCARD bigset 200" $':200\r\n' "$r"

r=$(send "*3\r\n\$9\r\nSISMEMBER\r\n\$6\r\nbigset\r\n\$9\r\nelem_0000")
check "SISMEMBER elem_0000" $':1\r\n' "$r"

r=$(send "*3\r\n\$9\r\nSISMEMBER\r\n\$6\r\nbigset\r\n\$9\r\nelem_0050")
check "SISMEMBER elem_0050" $':1\r\n' "$r"

r=$(send "*3\r\n\$9\r\nSISMEMBER\r\n\$6\r\nbigset\r\n\$9\r\nelem_0199")
check "SISMEMBER elem_0199" $':1\r\n' "$r"

r=$(send "*3\r\n\$9\r\nSISMEMBER\r\n\$6\r\nbigset\r\n\$9\r\nelem_9999")
check "SISMEMBER non-member" $':0\r\n' "$r"

# SREM from DICT encoding  → 3 elements
r=$(send "*3\r\n\$4\r\nSREM\r\n\$6\r\nbigset\r\n\$9\r\nelem_0000")
check "SREM from DICT" $':1\r\n' "$r"

r=$(send "*2\r\n\$5\r\nSCARD\r\n\$6\r\nbigset")
check "SCARD after SREM 199" $':199\r\n' "$r"

r=$(send "*3\r\n\$9\r\nSISMEMBER\r\n\$6\r\nbigset\r\n\$9\r\nelem_0000")
check "SISMEMBER after SREM" $':0\r\n' "$r"

# SMEMBERS on DICT (verify element count)
r=$(send "*2\r\n\$8\r\nSMEMBERS\r\n\$6\r\nbigset")
TOTAL=$((TOTAL + 1))
actual="${r%.}"
if echo "$actual" | head -c 5 | grep -q '^\*199'; then
    ok "SMEMBERS DICT returns 199 elements"
    PASS=$((PASS + 1))
else
    fail "SMEMBERS DICT returns 199 elements" "*199..." "$actual"
fi

# ── Part 5: DICT → Packed shrink (<64 elements) ─────────────────
echo ""
echo "=== Part 5: Encoding shrink (DICT → PACKED) ==="

# Delete down to 50 elements (keep elem_0150..0199 = 50 elements)
for i in $(seq 1 149); do
    msg=$(printf '*3\r\n\$4\r\nSREM\r\n\$6\r\nbigset\r\n\$9\r\nelem_%04d\r\n' $i)
    send "$msg"
done > /dev/null

r=$(send "*2\r\n\$5\r\nSCARD\r\n\$6\r\nbigset")
check "SCARD after shrink 50" $':50\r\n' "$r"

r=$(send "*3\r\n\$9\r\nSISMEMBER\r\n\$6\r\nbigset\r\n\$9\r\nelem_0150")
check "SISMEMBER elem_0150" $':1\r\n' "$r"

r=$(send "*3\r\n\$9\r\nSISMEMBER\r\n\$6\r\nbigset\r\n\$9\r\nelem_0199")
check "SISMEMBER elem_0199" $':1\r\n' "$r"

# SADD to packed after shrink  → 3 elements
r=$(send "*3\r\n\$4\r\nSADD\r\n\$6\r\nbigset\r\n\$3\r\nNEW")
check "SADD to shrunk set" $':1\r\n' "$r"

r=$(send "*2\r\n\$5\r\nSCARD\r\n\$6\r\nbigset")
check "SCARD after SADD to shrunk" $':51\r\n' "$r"

# ── Part 6: Large element values ─────────────────────────────────
echo ""
echo "=== Part 6: Large element values ==="

# Create a 2000-byte value
LONG=$(python3 -c "print('X' * 2000)" 2>/dev/null || printf 'X%.0s' $(seq 1 2000))
LEN=2000
# SADD key a LONG  → 4 elements
r=$(send "*4\r\n\$4\r\nSADD\r\n\$7\r\nlongset\r\n\$1\r\na\r\n\$${LEN}\r\n${LONG}")
check "SADD large element" $':2\r\n' "$r"

r=$(send "*2\r\n\$5\r\nSCARD\r\n\$7\r\nlongset")
check "SCARD longset" $':2\r\n' "$r"

r=$(send "*3\r\n\$9\r\nSISMEMBER\r\n\$7\r\nlongset\r\n\$1\r\na")
check "SISMEMBER a in longset" $':1\r\n' "$r"

r=$(send "*3\r\n\$4\r\nSREM\r\n\$7\r\nlongset\r\n\$1\r\na")
check "SREM a from longset" $':1\r\n' "$r"

# ── Part 7: WRONGTYPE ────────────────────────────────────────────
echo ""
echo "=== Part 7: WRONGTYPE checks ==="

# SET key hello  → 3 elements
r=$(send "*3\r\n\$3\r\nSET\r\n\$6\r\nstrkey\r\n\$5\r\nhello")
check "SET strkey hello" $'+OK\r\n' "$r"

# SADD on string key → 4 elements (should trigger WRONGTYPE)
r=$(send "*4\r\n\$4\r\nSADD\r\n\$6\r\nstrkey\r\n\$1\r\na\r\n\$1\r\nb")
check "SADD on string → WRONGTYPE" $'-WRONGTYPE Operation against a key holding the wrong kind of value\r\n' "$r"

r=$(send "*3\r\n\$4\r\nSREM\r\n\$6\r\nstrkey\r\n\$1\r\na")
check "SREM on string → WRONGTYPE" $'-WRONGTYPE Operation against a key holding the wrong kind of value\r\n' "$r"

r=$(send "*2\r\n\$8\r\nSMEMBERS\r\n\$6\r\nstrkey")
check "SMEMBERS on string → WRONGTYPE" $'-WRONGTYPE Operation against a key holding the wrong kind of value\r\n' "$r"

r=$(send "*2\r\n\$5\r\nSCARD\r\n\$6\r\nstrkey")
check "SCARD on string → WRONGTYPE" $'-WRONGTYPE Operation against a key holding the wrong kind of value\r\n' "$r"

r=$(send "*3\r\n\$9\r\nSISMEMBER\r\n\$6\r\nstrkey\r\n\$1\r\na")
check "SISMEMBER on string → WRONGTYPE" $'-WRONGTYPE Operation against a key holding the wrong kind of value\r\n' "$r"

r=$(send "*2\r\n\$4\r\nSPOP\r\n\$6\r\nstrkey")
check "SPOP on string → WRONGTYPE" $'-WRONGTYPE Operation against a key holding the wrong kind of value\r\n' "$r"

r=$(send "*2\r\n\$11\r\nSRANDMEMBER\r\n\$6\r\nstrkey")
check "SRANDMEMBER on string → WRONGTYPE" $'-WRONGTYPE Operation against a key holding the wrong kind of value\r\n' "$r"

# ── Part 8: DEL and EXIST ────────────────────────────────────────
echo ""
echo "=== Part 8: DEL / EXIST ==="

r=$(send "*2\r\n\$5\r\nEXIST\r\n\$6\r\nbigset")
check "EXIST bigset" $':1\r\n' "$r"

r=$(send "*2\r\n\$3\r\nDEL\r\n\$6\r\nbigset")
check "DEL bigset" $'+OK\r\n' "$r"

r=$(send "*2\r\n\$5\r\nEXIST\r\n\$6\r\nbigset")
check "EXIST after DEL" $'$-1\r\n' "$r"

r=$(send "*2\r\n\$5\r\nSCARD\r\n\$6\r\nbigset")
check "SCARD after DEL" $':0\r\n' "$r"

r=$(send "*2\r\n\$3\r\nDEL\r\n\$6\r\nstrkey")
check "DEL strkey" $'+OK\r\n' "$r"

r=$(send "*2\r\n\$3\r\nDEL\r\n\$8\r\nnoexists")
check "DEL non-existent" $'$-1\r\n' "$r"

# ── Results ──────────────────────────────────────────────────────
echo ""
echo "============================================"
echo "SET Test Results: $PASS/$TOTAL passed, $FAIL/$TOTAL failed"
[ $FAIL -eq 0 ] && echo "ALL SET TESTS PASSED" || echo "SOME SET TESTS FAILED"
[ $FAIL -eq 0 ] && exit 0 || exit 1
