#!/bin/bash
# Comprehensive LIST type test suite
# Port 2000 (not 6379 — that's system Redis)
HOST="127.0.0.1"
PORT=2000
PASS=0
FAIL=0
TOTAL=0

# send: uses double-sentinel to prevent $() from stripping \n from RESP responses.
#   Inner: "echo ." after nc keeps \n before it
#   Outer: printf '%s.' keeps \n after it
send() {
    local result
    result=$(echo -ne "$1\r\n" | nc -q0 $HOST $PORT 2>/dev/null; echo .)
    printf '%s.' "${result%.}"
}

ok() {
    PASS=$((PASS + 1)); TOTAL=$((TOTAL + 1))
    echo "  PASS: $1"
}

fail() {
    FAIL=$((FAIL + 1)); TOTAL=$((TOTAL + 1))
    echo "  FAIL: $1"
    echo "        expected: $(echo -n "$2" | xxd)"
    echo "        got:      $(echo -n "$3" | xxd)"
}

# check strips the outer sentinel '.' before comparing
check() {
    local actual="${3%.}"
    if [ "$2" = "$actual" ]; then ok "$1"; else fail "$1" "$2" "$actual"; fi
}

echo "=== Part 1: STRING Regression Tests ==="

r=$(send "*1\r\n\$4\r\nPING")
check "PING" $'+PONG\r\n' "$r"

r=$(send "*3\r\n\$3\r\nSET\r\n\$7\r\nstrkey1\r\n\$5\r\nhello")
check "SET strkey1 hello" $'+OK\r\n' "$r"

r=$(send "*2\r\n\$3\r\nGET\r\n\$7\r\nstrkey1")
check "GET strkey1" $'$5\r\nhello\r\n' "$r"

r=$(send "*2\r\n\$5\r\nEXIST\r\n\$7\r\nstrkey1")
check "EXIST strkey1" $':1\r\n' "$r"

r=$(send "*2\r\n\$5\r\nEXIST\r\n\$8\r\nnope_key")
check "EXIST nonexistent" $'$-1\r\n' "$r"

r=$(send "*2\r\n\$3\r\nDEL\r\n\$7\r\nstrkey1")
check "DEL strkey1" $'+OK\r\n' "$r"

r=$(send "*2\r\n\$3\r\nGET\r\n\$7\r\nstrkey1")
check "GET deleted strkey1" $'$-1\r\n' "$r"

r=$(send "*2\r\n\$4\r\nECHO\r\n\$11\r\nhello world")
check "ECHO" $'$11\r\nhello world\r\n' "$r"

echo ""
echo "=== Part 2: Small Packed LIST ==="

r=$(send "*3\r\n\$5\r\nRPUSH\r\n\$7\r\nmylist1\r\n\$1\r\na")
check "RPUSH a" $':1\r\n' "$r"

r=$(send "*3\r\n\$5\r\nRPUSH\r\n\$7\r\nmylist1\r\n\$1\r\nb")
check "RPUSH b" $':2\r\n' "$r"

r=$(send "*3\r\n\$5\r\nRPUSH\r\n\$7\r\nmylist1\r\n\$1\r\nc")
check "RPUSH c" $':3\r\n' "$r"

r=$(send "*2\r\n\$4\r\nLLEN\r\n\$7\r\nmylist1")
check "LLEN" $':3\r\n' "$r"

r=$(send "*4\r\n\$6\r\nLRANGE\r\n\$7\r\nmylist1\r\n\$1\r\n0\r\n\$2\r\n-1")
check "LRANGE 0 -1" $'*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n' "$r"

r=$(send "*4\r\n\$6\r\nLRANGE\r\n\$7\r\nmylist1\r\n\$1\r\n0\r\n\$1\r\n0")
check "LRANGE 0 0" $'*1\r\n$1\r\na\r\n' "$r"

r=$(send "*3\r\n\$6\r\nLINDEX\r\n\$7\r\nmylist1\r\n\$1\r\n0")
check "LINDEX 0" $'$1\r\na\r\n' "$r"

r=$(send "*3\r\n\$6\r\nLINDEX\r\n\$7\r\nmylist1\r\n\$1\r\n1")
check "LINDEX 1" $'$1\r\nb\r\n' "$r"

r=$(send "*3\r\n\$6\r\nLINDEX\r\n\$7\r\nmylist1\r\n\$1\r\n2")
check "LINDEX 2" $'$1\r\nc\r\n' "$r"

r=$(send "*3\r\n\$6\r\nLINDEX\r\n\$7\r\nmylist1\r\n\$1\r\n3")
check "LINDEX out of range" $'$-1\r\n' "$r"

# LPOP
r=$(send "*2\r\n\$4\r\nLPOP\r\n\$7\r\nmylist1")
check "LPOP" $'$1\r\na\r\n' "$r"

r=$(send "*2\r\n\$4\r\nLLEN\r\n\$7\r\nmylist1")
check "LLEN after LPOP" $':2\r\n' "$r"

r=$(send "*4\r\n\$6\r\nLRANGE\r\n\$7\r\nmylist1\r\n\$1\r\n0\r\n\$2\r\n-1")
check "LRANGE after LPOP" $'*2\r\n$1\r\nb\r\n$1\r\nc\r\n' "$r"

# RPOP
r=$(send "*2\r\n\$4\r\nRPOP\r\n\$7\r\nmylist1")
check "RPOP" $'$1\r\nc\r\n' "$r"

r=$(send "*2\r\n\$4\r\nLLEN\r\n\$7\r\nmylist1")
check "LLEN after RPOP" $':1\r\n' "$r"

# LPUSH
r=$(send "*3\r\n\$5\r\nLPUSH\r\n\$7\r\nmylist1\r\n\$1\r\nx")
check "LPUSH x" $':2\r\n' "$r"

r=$(send "*4\r\n\$6\r\nLRANGE\r\n\$7\r\nmylist1\r\n\$1\r\n0\r\n\$2\r\n-1")
check "LRANGE after LPUSH" $'*2\r\n$1\r\nx\r\n$1\r\nb\r\n' "$r"

# DEL list
r=$(send "*2\r\n\$3\r\nDEL\r\n\$7\r\nmylist1")
check "DEL mylist1" $'+OK\r\n' "$r"

r=$(send "*2\r\n\$4\r\nLLEN\r\n\$7\r\nmylist1")
check "LLEN deleted list" $':0\r\n' "$r"

# WRONGTYPE: SET on an existing list key
r=$(send "*3\r\n\$5\r\nRPUSH\r\n\$7\r\nwlist42\r\n\$1\r\na")
r=$(send "*3\r\n\$3\r\nSET\r\n\$7\r\nwlist42\r\n\$3\r\nval")
check "SET on list type" $'-ERR unknown command\r\n' "$r"

echo ""
echo "=== Part 3: 200-element Quicklist ==="

# Push 200 elements (item_%04d = "item_0000" = 9 bytes → use $9)
{
    for i in $(seq 0 199); do
        printf "*3\r\n\$5\r\nRPUSH\r\n\$5\r\nqlist\r\n\$9\r\nitem_%04d\r\n" $i
    done
} | nc -q0 $HOST $PORT > /dev/null 2>&1

r=$(send "*2\r\n\$4\r\nLLEN\r\n\$5\r\nqlist")
check "LLEN qlist" $':200\r\n' "$r"

r=$(send "*2\r\n\$5\r\nEXIST\r\n\$5\r\nqlist")
check "EXIST qlist" $':1\r\n' "$r"

r=$(send "*4\r\n\$6\r\nLRANGE\r\n\$5\r\nqlist\r\n\$1\r\n0\r\n\$1\r\n2")
check "LRANGE 0 2 (begin)" $'*3\r\n$9\r\nitem_0000\r\n$9\r\nitem_0001\r\n$9\r\nitem_0002\r\n' "$r"

r=$(send "*4\r\n\$6\r\nLRANGE\r\n\$5\r\nqlist\r\n\$2\r\n-3\r\n\$2\r\n-1")
check "LRANGE -3 -1 (end)" $'*3\r\n$9\r\nitem_0197\r\n$9\r\nitem_0198\r\n$9\r\nitem_0199\r\n' "$r"

r=$(send "*3\r\n\$6\r\nLINDEX\r\n\$5\r\nqlist\r\n\$1\r\n0")
check "LINDEX 0" $'$9\r\nitem_0000\r\n' "$r"

r=$(send "*3\r\n\$6\r\nLINDEX\r\n\$5\r\nqlist\r\n\$2\r\n64")
check "LINDEX 64" $'$9\r\nitem_0064\r\n' "$r"

r=$(send "*3\r\n\$6\r\nLINDEX\r\n\$5\r\nqlist\r\n\$3\r\n128")
check "LINDEX 128" $'$9\r\nitem_0128\r\n' "$r"

r=$(send "*3\r\n\$6\r\nLINDEX\r\n\$5\r\nqlist\r\n\$3\r\n199")
check "LINDEX 199" $'$9\r\nitem_0199\r\n' "$r"

r=$(send "*3\r\n\$6\r\nLINDEX\r\n\$5\r\nqlist\r\n\$3\r\n200")
check "LINDEX out of range" $'$-1\r\n' "$r"

# LPOP
r=$(send "*2\r\n\$4\r\nLPOP\r\n\$5\r\nqlist")
check "LPOP qlist" $'$9\r\nitem_0000\r\n' "$r"

r=$(send "*2\r\n\$4\r\nLLEN\r\n\$5\r\nqlist")
check "LLEN after LPOP" $':199\r\n' "$r"

r=$(send "*3\r\n\$6\r\nLINDEX\r\n\$5\r\nqlist\r\n\$1\r\n0")
check "LINDEX 0 after LPOP" $'$9\r\nitem_0001\r\n' "$r"

# RPOP
r=$(send "*2\r\n\$4\r\nRPOP\r\n\$5\r\nqlist")
check "RPOP qlist" $'$9\r\nitem_0199\r\n' "$r"

r=$(send "*2\r\n\$4\r\nLLEN\r\n\$5\r\nqlist")
check "LLEN after RPOP" $':198\r\n' "$r"

r=$(send "*4\r\n\$6\r\nLRANGE\r\n\$5\r\nqlist\r\n\$2\r\n-1\r\n\$2\r\n-1")
check "LRANGE -1 -1 after RPOP" $'*1\r\n$9\r\nitem_0198\r\n' "$r"

# LPUSH
r=$(send "*3\r\n\$5\r\nLPUSH\r\n\$5\r\nqlist\r\n\$3\r\nNEW")
check "LPUSH on quicklist" $':199\r\n' "$r"

r=$(send "*3\r\n\$6\r\nLINDEX\r\n\$5\r\nqlist\r\n\$1\r\n0")
check "LINDEX 0 after LPUSH" $'$3\r\nNEW\r\n' "$r"

# RPUSH
r=$(send "*3\r\n\$5\r\nRPUSH\r\n\$5\r\nqlist\r\n\$3\r\nEND")
check "RPUSH on quicklist" $':200\r\n' "$r"

r=$(send "*4\r\n\$6\r\nLRANGE\r\n\$5\r\nqlist\r\n\$2\r\n-1\r\n\$2\r\n-1")
check "LRANGE -1 -1 after RPUSH" $'*1\r\n$3\r\nEND\r\n' "$r"

# DEL qlist
r=$(send "*2\r\n\$3\r\nDEL\r\n\$5\r\nqlist")
check "DEL qlist" $'+OK\r\n' "$r"

r=$(send "*2\r\n\$4\r\nLLEN\r\n\$5\r\nqlist")
check "LLEN deleted qlist" $':0\r\n' "$r"

echo ""
echo "============================================"
echo "Results: $PASS/$TOTAL passed, $FAIL/$TOTAL failed"
[ $FAIL -eq 0 ] && echo "ALL TESTS PASSED" || echo "SOME TESTS FAILED"
