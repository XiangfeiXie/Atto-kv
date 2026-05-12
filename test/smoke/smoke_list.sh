#!/usr/bin/env bash
# LIST data type smoke test (Redis-compatible).
set -u
DIR="$(cd "$(dirname "$0")" && pwd)"
. "${DIR}/_lib.sh"
require_server
P=$PORT

section "Push / pop / length / index / range"
chk "RPUSH"         "5"  "$($RCLI RPUSH L a b c d e)"
chk "LLEN"          "5"  "$($RCLI LLEN L)"
chk "LRANGE all"    "a b c d e" "$($RCLI LRANGE L 0 -1 | tr '\n' ' ' | sed 's/ $//')"
chk "LRANGE 1..3"   "b c d"     "$($RCLI LRANGE L 1 3 | tr '\n' ' ' | sed 's/ $//')"
chk "LRANGE -2..-1" "d e"       "$($RCLI LRANGE L -2 -1 | tr '\n' ' ' | sed 's/ $//')"
chk "LINDEX 0"      "a"         "$($RCLI LINDEX L 0)"
chk "LINDEX -1"     "e"         "$($RCLI LINDEX L -1)"
chk "LINDEX OOR"    ""          "$($RCLI LINDEX L 99)"
chk "LPUSH"         "7"         "$($RCLI LPUSH L H1 H2)"
chk "LRANGE after LPUSH" "H2 H1 a b c d e" "$($RCLI LRANGE L 0 -1 | tr '\n' ' ' | sed 's/ $//')"

section "LPOP / RPOP (single + count)"
$RCLI DEL L >/dev/null
$RCLI RPUSH L a b c d e f g h >/dev/null
chk "LPOP single"   "a"     "$($RCLI LPOP L)"
chk "RPOP single"   "h"     "$($RCLI RPOP L)"
chk "LPOP count=2"  "b c"   "$($RCLI LPOP L 2 | tr '\n' ' ' | sed 's/ $//')"
chk "RPOP count=2"  "g f"   "$($RCLI RPOP L 2 | tr '\n' ' ' | sed 's/ $//')"
chk "LLEN after"    "2"     "$($RCLI LLEN L)"
chk "LPOP > remaining" "d e" "$($RCLI LPOP L 100 | tr '\n' ' ' | sed 's/ $//')"
chk "LLEN empty"    "0"     "$($RCLI LLEN L)"
chk "LPOP empty"    ""      "$($RCLI LPOP L)"

section "LSET / LREM / LTRIM / LINSERT"
$RCLI DEL L >/dev/null
$RCLI RPUSH L a b c d e >/dev/null
chk "LSET 0"        "OK"    "$($RCLI LSET L 0 X)"
chk "LSET -1"       "OK"    "$($RCLI LSET L -1 Z)"
chk "After LSET"    "X b c d Z" "$($RCLI LRANGE L 0 -1 | tr '\n' ' ' | sed 's/ $//')"
ERR=$($RCLI LSET L 99 W 2>&1)
case "$ERR" in *ERR*) ok "LSET OOR errors" ;; *) bad "LSET OOR errors" "ERR..." "$ERR" ;; esac

$RCLI DEL L >/dev/null
$RCLI RPUSH L a b c b d b e >/dev/null
chk "LREM 2 b"      "2"     "$($RCLI LREM L 2 b)"
chk "After LREM 2"  "a c d b e" "$($RCLI LRANGE L 0 -1 | tr '\n' ' ' | sed 's/ $//')"
$RCLI DEL L >/dev/null
$RCLI RPUSH L a b c b d b e >/dev/null
chk "LREM -2 b"     "2"     "$($RCLI LREM L -2 b)"
chk "After LREM -2" "a b c d e" "$($RCLI LRANGE L 0 -1 | tr '\n' ' ' | sed 's/ $//')"
$RCLI DEL L >/dev/null
$RCLI RPUSH L a b c b d b e >/dev/null
chk "LREM 0 b"      "3"     "$($RCLI LREM L 0 b)"

$RCLI DEL L >/dev/null
$RCLI RPUSH L a b c d e >/dev/null
chk "LTRIM 1 3"     "OK"    "$($RCLI LTRIM L 1 3)"
chk "After LTRIM"   "b c d" "$($RCLI LRANGE L 0 -1 | tr '\n' ' ' | sed 's/ $//')"

$RCLI DEL L >/dev/null
$RCLI RPUSH L a b c d >/dev/null
chk "LINSERT BEFORE" "5"     "$($RCLI LINSERT L BEFORE c X)"
chk "LINSERT AFTER"  "6"     "$($RCLI LINSERT L AFTER c Y)"
chk "After LINSERT" "a b X c Y d" "$($RCLI LRANGE L 0 -1 | tr '\n' ' ' | sed 's/ $//')"
chk "LINSERT missing pivot" "-1" "$($RCLI LINSERT L BEFORE nope Z)"

section "LPOS / LMOVE / RPOPLPUSH"
$RCLI DEL L >/dev/null
$RCLI RPUSH L a b c d c b c >/dev/null
chk "LPOS first"    "2"     "$($RCLI LPOS L c)"
chk "LPOS COUNT 0"  "2 4 6" "$($RCLI LPOS L c COUNT 0 | tr '\n' ' ' | sed 's/ $//')"
chk "LPOS COUNT 2"  "2 4"   "$($RCLI LPOS L c COUNT 2 | tr '\n' ' ' | sed 's/ $//')"
chk "LPOS missing"  ""      "$($RCLI LPOS L zzz)"

$RCLI DEL src dst >/dev/null
$RCLI RPUSH src 1 2 3 >/dev/null
chk "LMOVE L→R"     "1"     "$($RCLI LMOVE src dst LEFT RIGHT)"
chk "src after"     "2 3"   "$($RCLI LRANGE src 0 -1 | tr '\n' ' ' | sed 's/ $//')"
chk "dst after"     "1"     "$($RCLI LRANGE dst 0 -1)"
chk "LMOVE R→L"     "3"     "$($RCLI LMOVE src dst RIGHT LEFT)"
chk "dst after R→L" "3 1"   "$($RCLI LRANGE dst 0 -1 | tr '\n' ' ' | sed 's/ $//')"

$RCLI DEL src dst >/dev/null
$RCLI RPUSH src a b c >/dev/null
chk "RPOPLPUSH"     "c"     "$($RCLI RPOPLPUSH src dst)"
chk "src after"     "a b"   "$($RCLI LRANGE src 0 -1 | tr '\n' ' ' | sed 's/ $//')"
$RCLI DEL rot >/dev/null
$RCLI RPUSH rot 1 2 3 >/dev/null
$RCLI RPOPLPUSH rot rot >/dev/null
chk "RPOPLPUSH self-rotate" "3 1 2" "$($RCLI LRANGE rot 0 -1 | tr '\n' ' ' | sed 's/ $//')"

section "LPUSHX / RPUSHX"
$RCLI DEL nk L >/dev/null
chk "LPUSHX missing" "0"    "$($RCLI LPUSHX nk a)"
chk "RPUSHX missing" "0"    "$($RCLI RPUSHX nk a)"
$RCLI RPUSH L a b >/dev/null
chk "LPUSHX existing" "3"   "$($RCLI LPUSHX L head)"
chk "RPUSHX existing" "4"   "$($RCLI RPUSHX L tail)"

section "Encoding upgrade (PACKED → QUICKLIST)"
$RCLI DEL big >/dev/null
ARGS=""; for i in $(seq 0 499); do ARGS="$ARGS e$i"; done
$RCLI RPUSH big $ARGS >/dev/null
chk "LLEN 500"      "500"   "$($RCLI LLEN big)"
chk "LINDEX 0"      "e0"    "$($RCLI LINDEX big 0)"
chk "LINDEX 250"    "e250"  "$($RCLI LINDEX big 250)"
chk "LINDEX -1"     "e499"  "$($RCLI LINDEX big -1)"
chk "LPOS e300"     "300"   "$($RCLI LPOS big e300)"
chk "LREM 1 e100"   "1"     "$($RCLI LREM big 1 e100)"
chk "LLEN after"    "499"   "$($RCLI LLEN big)"
$RCLI LTRIM big 0 99 >/dev/null
chk "LTRIM to 100"  "100"   "$($RCLI LLEN big)"

section "Wrong-type guard & missing-key"
$RCLI SET strk hello >/dev/null
W="WRONGTYPE Operation against a key holding the wrong kind of value"
chk "LPUSH wrongtype" "$W"  "$($RCLI LPUSH strk a 2>&1)"
chk "LLEN wrongtype"  "$W"  "$($RCLI LLEN strk 2>&1)"
chk "LLEN missing"    "0"   "$($RCLI LLEN missing)"
chk "LRANGE missing"  ""    "$($RCLI LRANGE missing 0 -1)"
chk "LINDEX missing"  ""    "$($RCLI LINDEX missing 0)"

summary "LIST"
