#!/bin/bash
# Comprehensive ZSET type test suite
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
echo "  ZSET Type — Comprehensive Test Suite"
echo "============================================"

# Ensure clean state
send "*1\r\n\$8\r\nFLUSHALL" > /dev/null

# ── Part 1: Basic Packed ZSET Operations ─────────────────────────
echo ""
echo "=== Part 1: Basic Packed ZSET (≤128 members) ==="

# ZADD key 10 apple 20 banana  → 6 elements (ZADD + key + score1 + m1 + score2 + m2)
r=$(send "*6\r\n\$4\r\nZADD\r\n\$6\r\nzset_a\r\n\$2\r\n10\r\n\$5\r\napple\r\n\$2\r\n20\r\n\$6\r\nbanana")
check "ZADD apple banana" $':2\r\n' "$r"

# ZCARD key  → 2 elements
r=$(send "*2\r\n\$5\r\nZCARD\r\n\$6\r\nzset_a")
check "ZCARD" $':2\r\n' "$r"

# ZSCORE key apple  → 3 elements
r=$(send "*3\r\n\$6\r\nZSCORE\r\n\$6\r\nzset_a\r\n\$5\r\napple")
check "ZSCORE apple" $'$2\r\n10\r\n' "$r"

r=$(send "*3\r\n\$6\r\nZSCORE\r\n\$6\r\nzset_a\r\n\$6\r\nbanana")
check "ZSCORE banana" $'$2\r\n20\r\n' "$r"

r=$(send "*3\r\n\$6\r\nZSCORE\r\n\$6\r\nzset_a\r\n\$6\r\norange")
check "ZSCORE non-existent" $'$-1\r\n' "$r"

# ZADD update score  → 4 elements
r=$(send "*4\r\n\$4\r\nZADD\r\n\$6\r\nzset_a\r\n\$2\r\n50\r\n\$5\r\napple")
check "ZADD update apple 10→50" $':0\r\n' "$r"

r=$(send "*3\r\n\$6\r\nZSCORE\r\n\$6\r\nzset_a\r\n\$5\r\napple")
check "ZSCORE updated apple" $'$2\r\n50\r\n' "$r"

# ZADD same score (no change)  → 4 elements
r=$(send "*4\r\n\$4\r\nZADD\r\n\$6\r\nzset_a\r\n\$2\r\n50\r\n\$5\r\napple")
check "ZADD same score" $':0\r\n' "$r"

# ZRANK key apple  → 3 elements
r=$(send "*3\r\n\$5\r\nZRANK\r\n\$6\r\nzset_a\r\n\$5\r\napple")
check "ZRANK apple = 1" $':1\r\n' "$r"

r=$(send "*3\r\n\$5\r\nZRANK\r\n\$6\r\nzset_a\r\n\$6\r\nbanana")
check "ZRANK banana = 0" $':0\r\n' "$r"

r=$(send "*3\r\n\$5\r\nZRANK\r\n\$6\r\nzset_a\r\n\$6\r\norange")
check "ZRANK non-existent" $'$-1\r\n' "$r"

# ZRANGE key 0 -1  → 4 elements
r=$(send "*4\r\n\$6\r\nZRANGE\r\n\$6\r\nzset_a\r\n\$1\r\n0\r\n\$2\r\n-1")
check "ZRANGE 0 -1" $'*2\r\n$6\r\nbanana\r\n$5\r\napple\r\n' "$r"

# ZRANGE WITHSCORES  → 5 elements
r=$(send "*5\r\n\$6\r\nZRANGE\r\n\$6\r\nzset_a\r\n\$1\r\n0\r\n\$2\r\n-1\r\n\$10\r\nWITHSCORES")
check "ZRANGE WITHSCORES" $'*4\r\n$6\r\nbanana\r\n$2\r\n20\r\n$5\r\napple\r\n$2\r\n50\r\n' "$r"

# ── Part 2: ZCOUNT and ZRANGEBYSCORE ────────────────────────────
echo ""
echo "=== Part 2: ZCOUNT & ZRANGEBYSCORE ==="

# ZADD key 30 cherry  → 4 elements
r=$(send "*4\r\n\$4\r\nZADD\r\n\$6\r\nzset_a\r\n\$2\r\n30\r\n\$6\r\ncherry")
check "ZADD cherry 30" $':1\r\n' "$r"

r=$(send "*4\r\n\$4\r\nZADD\r\n\$6\r\nzset_a\r\n\$2\r\n40\r\n\$4\r\ndate")
check "ZADD date 40" $':1\r\n' "$r"

# ZCOUNT key 20 50  → 4 elements
r=$(send "*4\r\n\$6\r\nZCOUNT\r\n\$6\r\nzset_a\r\n\$2\r\n20\r\n\$2\r\n50")
check "ZCOUNT 20 50" $':4\r\n' "$r"

r=$(send "*4\r\n\$6\r\nZCOUNT\r\n\$6\r\nzset_a\r\n\$2\r\n25\r\n\$2\r\n35")
check "ZCOUNT 25 35" $':1\r\n' "$r"

r=$(send "*4\r\n\$6\r\nZCOUNT\r\n\$6\r\nzset_a\r\n\$2\r\n50\r\n\$2\r\n50")
check "ZCOUNT 50 50" $':1\r\n' "$r"

r=$(send "*4\r\n\$6\r\nZCOUNT\r\n\$6\r\nzset_a\r\n\$2\r\n60\r\n\$3\r\n100")
check "ZCOUNT out of range" $':0\r\n' "$r"

# ZRANGEBYSCORE key 20 50  → 4 elements
r=$(send "*4\r\n\$13\r\nZRANGEBYSCORE\r\n\$6\r\nzset_a\r\n\$2\r\n20\r\n\$2\r\n50")
check "ZRANGEBYSCORE 20 50" $'*4\r\n$6\r\nbanana\r\n$6\r\ncherry\r\n$4\r\ndate\r\n$5\r\napple\r\n' "$r"

# ZRANGEBYSCORE WITHSCORES  → 5 elements
r=$(send "*5\r\n\$13\r\nZRANGEBYSCORE\r\n\$6\r\nzset_a\r\n\$2\r\n20\r\n\$2\r\n50\r\n\$10\r\nWITHSCORES")
check_contains "ZRANGEBYSCORE WITHSCORES has banana" "banana" "$r"
check_contains "ZRANGEBYSCORE WITHSCORES has 20"    "20"    "$r"
check_contains "ZRANGEBYSCORE WITHSCORES has apple" "apple" "$r"
check_contains "ZRANGEBYSCORE WITHSCORES has 50"    "50"    "$r"

r=$(send "*4\r\n\$13\r\nZRANGEBYSCORE\r\n\$6\r\nzset_a\r\n\$2\r\n30\r\n\$2\r\n30")
check "ZRANGEBYSCORE 30 30" $'*1\r\n$6\r\ncherry\r\n' "$r"

# ── Part 3: ZREM ─────────────────────────────────────────────────
echo ""
echo "=== Part 3: ZREM ==="

# ZREM key date  → 3 elements
r=$(send "*3\r\n\$4\r\nZREM\r\n\$6\r\nzset_a\r\n\$4\r\ndate")
check "ZREM date" $':1\r\n' "$r"

r=$(send "*2\r\n\$5\r\nZCARD\r\n\$6\r\nzset_a")
check "ZCARD after ZREM" $':3\r\n' "$r"

r=$(send "*3\r\n\$6\r\nZSCORE\r\n\$6\r\nzset_a\r\n\$4\r\ndate")
check "ZSCORE after ZREM" $'$-1\r\n' "$r"

r=$(send "*3\r\n\$4\r\nZREM\r\n\$6\r\nzset_a\r\n\$6\r\norange")
check "ZREM non-existent" $':0\r\n' "$r"

# ZREM key apple banana  → 4 elements
r=$(send "*4\r\n\$4\r\nZREM\r\n\$6\r\nzset_a\r\n\$5\r\napple\r\n\$6\r\nbanana")
check "ZREM apple banana" $':2\r\n' "$r"

r=$(send "*2\r\n\$5\r\nZCARD\r\n\$6\r\nzset_a")
check "ZCARD after ZREM multiple" $':1\r\n' "$r"

# ── Part 4: Empty ZSET edge cases ────────────────────────────────
echo ""
echo "=== Part 4: Empty ZSET edge cases ==="

r=$(send "*3\r\n\$4\r\nZREM\r\n\$6\r\nzset_a\r\n\$6\r\ncherry")
check "ZREM last element" $':1\r\n' "$r"

r=$(send "*2\r\n\$5\r\nZCARD\r\n\$6\r\nzset_a")
check "ZCARD empty zset" $':0\r\n' "$r"

r=$(send "*4\r\n\$6\r\nZRANGE\r\n\$6\r\nzset_a\r\n\$1\r\n0\r\n\$2\r\n-1")
check "ZRANGE empty zset" $'*0\r\n' "$r"

r=$(send "*2\r\n\$5\r\nZCARD\r\n\$8\r\nnoexists")
check "ZCARD non-existent key" $':0\r\n' "$r"

r=$(send "*3\r\n\$6\r\nZSCORE\r\n\$8\r\nnoexists\r\n\$1\r\na")
check "ZSCORE non-existent key" $'$-1\r\n' "$r"

r=$(send "*3\r\n\$5\r\nZRANK\r\n\$8\r\nnoexists\r\n\$1\r\na")
check "ZRANK non-existent key" $'$-1\r\n' "$r"

r=$(send "*4\r\n\$6\r\nZRANGE\r\n\$8\r\nnoexists\r\n\$1\r\n0\r\n\$2\r\n-1")
check "ZRANGE non-existent key" $'*0\r\n' "$r"

r=$(send "*4\r\n\$6\r\nZCOUNT\r\n\$8\r\nnoexists\r\n\$1\r\n0\r\n\$1\r\n1")
check "ZCOUNT non-existent key" $':0\r\n' "$r"

r=$(send "*4\r\n\$13\r\nZRANGEBYSCORE\r\n\$8\r\nnoexists\r\n\$1\r\n0\r\n\$1\r\n1")
check "ZRANGEBYSCORE non-existent key" $'*0\r\n' "$r"

# ── Part 5: Packed → SKIPLIST encoding upgrade (>128 members) ───
echo ""
echo "=== Part 5: Encoding upgrade (PACKED → SKIPLIST) ==="

# Create 200 members via for loop
for i in $(seq 0 199); do
    msg=$(printf '*4\r\n\$4\r\nZADD\r\n\$7\r\nbigzset\r\n\$3\r\n%d\r\n\$11\r\nmember_%04d\r\n' $((i * 10)) $i)
    send "$msg"
done > /dev/null

r=$(send "*2\r\n\$5\r\nZCARD\r\n\$7\r\nbigzset")
check "ZCARD bigzset 200" $':200\r\n' "$r"

r=$(send "*3\r\n\$6\r\nZSCORE\r\n\$7\r\nbigzset\r\n\$11\r\nmember_0000")
check "ZSCORE member_0000" $'$1\r\n0\r\n' "$r"

r=$(send "*3\r\n\$6\r\nZSCORE\r\n\$7\r\nbigzset\r\n\$11\r\nmember_0050")
check "ZSCORE member_0050" $'$3\r\n500\r\n' "$r"

r=$(send "*3\r\n\$6\r\nZSCORE\r\n\$7\r\nbigzset\r\n\$11\r\nmember_0199")
check "ZSCORE member_0199" $'$4\r\n1990\r\n' "$r"

r=$(send "*3\r\n\$6\r\nZSCORE\r\n\$7\r\nbigzset\r\n\$11\r\nmember_9999")
check "ZSCORE non-member" $'$-1\r\n' "$r"

# ZRANK in SKIPLIST
r=$(send "*3\r\n\$5\r\nZRANK\r\n\$7\r\nbigzset\r\n\$11\r\nmember_0000")
check "ZRANK member_0000 = 0" $':0\r\n' "$r"

r=$(send "*3\r\n\$5\r\nZRANK\r\n\$7\r\nbigzset\r\n\$11\r\nmember_0100")
check "ZRANK member_0100 = 100" $':100\r\n' "$r"

r=$(send "*3\r\n\$5\r\nZRANK\r\n\$7\r\nbigzset\r\n\$11\r\nmember_0199")
check "ZRANK member_0199 = 199" $':199\r\n' "$r"

# ZADD update score in SKIPLIST
r=$(send "*4\r\n\$4\r\nZADD\r\n\$7\r\nbigzset\r\n\$4\r\n9999\r\n\$11\r\nmember_0050")
check "ZADD update score in SKIPLIST" $':0\r\n' "$r"

r=$(send "*3\r\n\$6\r\nZSCORE\r\n\$7\r\nbigzset\r\n\$11\r\nmember_0050")
check "ZSCORE updated member_0050" $'$4\r\n9999\r\n' "$r"

# ZRANK after score change (now should be last)
r=$(send "*3\r\n\$5\r\nZRANK\r\n\$7\r\nbigzset\r\n\$11\r\nmember_0050")
check "ZRANK after score update = 199" $':199\r\n' "$r"

# ZREM from SKIPLIST
r=$(send "*3\r\n\$4\r\nZREM\r\n\$7\r\nbigzset\r\n\$11\r\nmember_0000")
check "ZREM from SKIPLIST" $':1\r\n' "$r"

r=$(send "*2\r\n\$5\r\nZCARD\r\n\$7\r\nbigzset")
check "ZCARD after ZREM 199" $':199\r\n' "$r"

# ZCOUNT in SKIPLIST
r=$(send "*4\r\n\$6\r\nZCOUNT\r\n\$7\r\nbigzset\r\n\$1\r\n0\r\n\$4\r\n9999")
check "ZCOUNT 0 9999" $':199\r\n' "$r"

r=$(send "*4\r\n\$6\r\nZCOUNT\r\n\$7\r\nbigzset\r\n\$2\r\n50\r\n\$3\r\n150")
check "ZCOUNT 50 150" $':11\r\n' "$r"

# ZRANGEBYSCORE in SKIPLIST
r=$(send "*4\r\n\$13\r\nZRANGEBYSCORE\r\n\$7\r\nbigzset\r\n\$1\r\n0\r\n\$2\r\n20")
check "ZRANGEBYSCORE 0 20" $'*3\r\n$10\r\nmember_0001\r\n$10\r\nmember_0002\r\n$10\r\nmember_0000\r\n' "$r"

# ── Part 6: SKIPLIST → Packed shrink (<64 members) ──────────────
echo ""
echo "=== Part 6: Encoding shrink (SKIPLIST → PACKED) ==="

# Delete down to 50 members (keep member_0150..0199 = 50 elements)
for i in $(seq 1 149); do
    msg=$(printf '*3\r\n\$4\r\nZREM\r\n\$7\r\nbigzset\r\n\$11\r\nmember_%04d\r\n' $i)
    send "$msg"
done > /dev/null

r=$(send "*2\r\n\$5\r\nZCARD\r\n\$7\r\nbigzset")
check "ZCARD after shrink 50" $':50\r\n' "$r"

r=$(send "*3\r\n\$6\r\nZSCORE\r\n\$7\r\nbigzset\r\n\$11\r\nmember_0150")
check "ZSCORE member_0150 still exists" $'$4\r\n1500\r\n' "$r"

r=$(send "*3\r\n\$6\r\nZSCORE\r\n\$7\r\nbigzset\r\n\$11\r\nmember_0199")
check "ZSCORE member_0199 still exists" $'$4\r\n1990\r\n' "$r"

# ZADD after shrink
r=$(send "*4\r\n\$4\r\nZADD\r\n\$7\r\nbigzset\r\n\$1\r\n5\r\n\$8\r\nnewfirst")
check "ZADD after shrink" $':1\r\n' "$r"

r=$(send "*2\r\n\$5\r\nZCARD\r\n\$7\r\nbigzset")
check "ZCARD after ZADD to shrunk" $':51\r\n' "$r"

r=$(send "*3\r\n\$5\r\nZRANK\r\n\$7\r\nbigzset\r\n\$8\r\nnewfirst")
check "ZRANK newfirst = 0" $':0\r\n' "$r"

# ── Part 7: Large member values ──────────────────────────────────
echo ""
echo "=== Part 7: Large member values ==="

LONG_M=$(python3 -c "print('M' * 2000)" 2>/dev/null || printf 'M%.0s' $(seq 1 2000))

r=$(send "*4\r\n\$4\r\nZADD\r\n\$8\r\nlongzset\r\n\$3\r\n100\r\n\$2000\r\n${LONG_M}")
TOTAL=$((TOTAL + 1))
actual="${r%.}"
if [ "$actual" = ':1\r\n' ]; then
    ok "ZADD with large member"
    PASS=$((PASS + 1))
else
    fail "ZADD with large member" ":1" "$actual"
fi

r=$(send "*3\r\n\$6\r\nZSCORE\r\n\$8\r\nlongzset\r\n\$2000\r\n${LONG_M}")
TOTAL=$((TOTAL + 1))
actual="${r%.}"
if echo "$actual" | grep -q '^\$3\r\n100'; then
    ok "ZSCORE large member"
    PASS=$((PASS + 1))
else
    fail "ZSCORE large member" "\$3\r\n100" "$actual"
fi

r=$(send "*3\r\n\$4\r\nZREM\r\n\$8\r\nlongzset\r\n\$2000\r\n${LONG_M}")
check "ZREM large member" $':1\r\n' "$r"

# ── Part 8: WRONGTYPE ────────────────────────────────────────────
echo ""
echo "=== Part 8: WRONGTYPE checks ==="

r=$(send "*3\r\n\$3\r\nSET\r\n\$6\r\nstrkey\r\n\$5\r\nhello")
check "SET strkey" $'+OK\r\n' "$r"

WRONGTYPE=$'-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'

r=$(send "*4\r\n\$4\r\nZADD\r\n\$6\r\nstrkey\r\n\$1\r\n1\r\n\$1\r\na")
check "ZADD on string → WRONGTYPE" "$WRONGTYPE" "$r"

r=$(send "*3\r\n\$4\r\nZREM\r\n\$6\r\nstrkey\r\n\$1\r\na")
check "ZREM on string → WRONGTYPE" "$WRONGTYPE" "$r"

r=$(send "*3\r\n\$6\r\nZSCORE\r\n\$6\r\nstrkey\r\n\$1\r\na")
check "ZSCORE on string → WRONGTYPE" "$WRONGTYPE" "$r"

r=$(send "*4\r\n\$6\r\nZRANGE\r\n\$6\r\nstrkey\r\n\$1\r\n0\r\n\$2\r\n-1")
check "ZRANGE on string → WRONGTYPE" "$WRONGTYPE" "$r"

r=$(send "*3\r\n\$5\r\nZRANK\r\n\$6\r\nstrkey\r\n\$1\r\na")
check "ZRANK on string → WRONGTYPE" "$WRONGTYPE" "$r"

r=$(send "*2\r\n\$5\r\nZCARD\r\n\$6\r\nstrkey")
check "ZCARD on string → WRONGTYPE" "$WRONGTYPE" "$r"

r=$(send "*4\r\n\$6\r\nZCOUNT\r\n\$6\r\nstrkey\r\n\$1\r\n0\r\n\$1\r\n1")
check "ZCOUNT on string → WRONGTYPE" "$WRONGTYPE" "$r"

r=$(send "*4\r\n\$13\r\nZRANGEBYSCORE\r\n\$6\r\nstrkey\r\n\$1\r\n0\r\n\$1\r\n1")
check "ZRANGEBYSCORE on string → WRONGTYPE" "$WRONGTYPE" "$r"

# ── Part 9: DEL and EXIST ────────────────────────────────────────
echo ""
echo "=== Part 9: DEL / EXIST ==="

r=$(send "*2\r\n\$5\r\nEXIST\r\n\$7\r\nbigzset")
check "EXIST bigzset" $':1\r\n' "$r"

r=$(send "*2\r\n\$3\r\nDEL\r\n\$7\r\nbigzset")
check "DEL bigzset" $'+OK\r\n' "$r"

r=$(send "*2\r\n\$5\r\nEXIST\r\n\$7\r\nbigzset")
check "EXIST after DEL" $'$-1\r\n' "$r"

r=$(send "*2\r\n\$5\r\nZCARD\r\n\$7\r\nbigzset")
check "ZCARD after DEL" $':0\r\n' "$r"

r=$(send "*2\r\n\$3\r\nDEL\r\n\$6\r\nstrkey")
check "DEL strkey" $'+OK\r\n' "$r"

r=$(send "*2\r\n\$3\r\nDEL\r\n\$8\r\nnoexists")
check "DEL non-existent" $'$-1\r\n' "$r"

# ── Part 10: DEL on ZSET with skiplist (cleanup path) ─────────────
echo ""
echo "=== Part 10: DEL on SKIPLIST ZSET cleanup ==="

# Re-create with enough elements to use skiplist (200 items, each with unique score)
for i in $(seq 0 199); do
    msg=$(printf '*4\r\n\$4\r\nZADD\r\n\$7\r\ncleanme\r\n\$3\r\n%d\r\n\$9\r\nitem_%04d\r\n' $i $i)
    send "$msg"
done > /dev/null

r=$(send "*2\r\n\$5\r\nZCARD\r\n\$7\r\ncleanme")
check "ZCARD cleanme" $':200\r\n' "$r"

r=$(send "*2\r\n\$3\r\nDEL\r\n\$7\r\ncleanme")
check "DEL cleanme (skiplist cleanup)" $'+OK\r\n' "$r"

r=$(send "*2\r\n\$5\r\nZCARD\r\n\$7\r\ncleanme")
check "ZCARD after DEL" $':0\r\n' "$r"

# ── Results ──────────────────────────────────────────────────────
echo ""
echo "============================================"
echo "ZSET Test Results: $PASS/$TOTAL passed, $FAIL/$TOTAL failed"
[ $FAIL -eq 0 ] && echo "ALL ZSET TESTS PASSED" || echo "SOME ZSET TESTS FAILED"
[ $FAIL -eq 0 ] && exit 0 || exit 1
