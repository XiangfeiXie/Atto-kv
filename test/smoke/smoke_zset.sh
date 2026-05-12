#!/usr/bin/env bash
# ZSET (sorted set) data type smoke test (Redis-compatible).
set -u
DIR="$(cd "$(dirname "$0")" && pwd)"
. "${DIR}/_lib.sh"
require_server
P=$PORT

section "ZADD / ZSCORE / ZCARD basics"
chk "ZADD 3 new"    "3"     "$($RCLI ZADD z 1 a 2 b 3 c)"
chk "ZADD dup same" "0"     "$($RCLI ZADD z 1 a)"
chk "ZADD update"   "0"     "$($RCLI ZADD z 10 a)"
chk "ZCARD"         "3"     "$($RCLI ZCARD z)"
chk "ZSCORE a"      "10"    "$($RCLI ZSCORE z a)"
chk "ZSCORE missing" ""     "$($RCLI ZSCORE z nope)"

section "ZADD flags: NX / XX / GT / LT / CH / INCR"
$RCLI DEL z >/dev/null
$RCLI ZADD z 1 a >/dev/null
chk "NX existing"   "0"     "$($RCLI ZADD z NX 99 a)"
chk "score unchanged" "1"   "$($RCLI ZSCORE z a)"
chk "NX new"        "1"     "$($RCLI ZADD z NX 5 b)"
chk "XX existing"   "0"     "$($RCLI ZADD z XX 7 a)"
chk "score now 7"   "7"     "$($RCLI ZSCORE z a)"
chk "XX missing"    "0"     "$($RCLI ZADD z XX 9 nope)"
chk "GT lower (rej)" "0"    "$($RCLI ZADD z GT 1 a)"
chk "GT still 7"    "7"     "$($RCLI ZSCORE z a)"
chk "GT higher CH"  "1"     "$($RCLI ZADD z GT CH 99 a)"
chk "LT lower CH"   "1"     "$($RCLI ZADD z LT CH 1 a)"
chk "CH update"     "1"     "$($RCLI ZADD z CH 2 a)"
chk "CH no-op"      "0"     "$($RCLI ZADD z CH 2 a)"

$RCLI DEL z >/dev/null
$RCLI ZADD z 1 a >/dev/null
chk "INCR existing" "6"     "$($RCLI ZADD z INCR 5 a)"
chk "INCR new"      "3"     "$($RCLI ZADD z INCR 3 new)"
chk "NX INCR exist" ""      "$($RCLI ZADD z NX INCR 5 a)"
chk "XX INCR miss"  ""      "$($RCLI ZADD z XX INCR 5 nope)"

section "ZINCRBY"
$RCLI DEL z >/dev/null
$RCLI ZADD z 1 a >/dev/null
chk "ZINCRBY existing" "4" "$($RCLI ZINCRBY z 3 a)"
chk "ZINCRBY new"   "5"     "$($RCLI ZINCRBY z 5 newm)"
chk "ZINCRBY neg"   "2"     "$($RCLI ZINCRBY z -2 a)"

section "ZRANK / ZREVRANK / ZRANGE / ZREVRANGE"
$RCLI DEL z >/dev/null
$RCLI ZADD z 1 a 2 b 3 c 4 d >/dev/null
chk "ZRANK first"   "0"     "$($RCLI ZRANK z a)"
chk "ZRANK last"    "3"     "$($RCLI ZRANK z d)"
chk "ZRANK missing" ""      "$($RCLI ZRANK z nope)"
chk "ZREVRANK first" "3"    "$($RCLI ZREVRANK z a)"
chk "ZREVRANK last"  "0"    "$($RCLI ZREVRANK z d)"
chk "ZRANGE all"    "a b c d" "$($RCLI ZRANGE z 0 -1 | tr '\n' ' ' | sed 's/ $//')"
chk "ZRANGE 1..2"   "b c"   "$($RCLI ZRANGE z 1 2 | tr '\n' ' ' | sed 's/ $//')"
chk "ZRANGE WITHSCORES" "a 1 b 2" "$($RCLI ZRANGE z 0 1 WITHSCORES | tr '\n' ' ' | sed 's/ $//')"
chk "ZREVRANGE"     "d c b a" "$($RCLI ZREVRANGE z 0 -1 | tr '\n' ' ' | sed 's/ $//')"
chk "ZREVRANGE WITHSCORES" "d 4 c 3" "$($RCLI ZREVRANGE z 0 1 WITHSCORES | tr '\n' ' ' | sed 's/ $//')"

section "ZCOUNT / ZRANGEBYSCORE / ZREVRANGEBYSCORE — inclusive, exclusive, LIMIT"
chk "ZCOUNT 2 3"        "2" "$($RCLI ZCOUNT z 2 3)"
chk "ZCOUNT -inf +inf"  "4" "$($RCLI ZCOUNT z -inf +inf)"
chk "ZCOUNT (1 3"       "2" "$($RCLI ZCOUNT z '(1' 3)"
chk "ZCOUNT 1 (3"       "2" "$($RCLI ZCOUNT z 1 '(3')"
chk "ZRANGEBYSCORE 2 3" "b c" "$($RCLI ZRANGEBYSCORE z 2 3 | tr '\n' ' ' | sed 's/ $//')"
chk "ZRANGEBYSCORE LIMIT" "b c" "$($RCLI ZRANGEBYSCORE z -inf +inf LIMIT 1 2 | tr '\n' ' ' | sed 's/ $//')"
chk "ZRANGEBYSCORE excl" "b c" "$($RCLI ZRANGEBYSCORE z '(1' '(4' | tr '\n' ' ' | sed 's/ $//')"
chk "ZREVRANGEBYSCORE"  "c b" "$($RCLI ZREVRANGEBYSCORE z 3 2 | tr '\n' ' ' | sed 's/ $//')"
chk "ZREVRANGEBYSCORE LIMIT" "c b" "$($RCLI ZREVRANGEBYSCORE z +inf -inf LIMIT 1 2 | tr '\n' ' ' | sed 's/ $//')"

section "ZMSCORE"
chk "ZMSCORE mixed" "1  2"  "$($RCLI ZMSCORE z a nope b | tr '\n' ' ' | sed 's/ $//')"

section "ZPOPMIN / ZPOPMAX"
$RCLI DEL z >/dev/null
$RCLI ZADD z 1 a 2 b 3 c 4 d >/dev/null
chk "ZPOPMIN"       "a 1"   "$($RCLI ZPOPMIN z | tr '\n' ' ' | sed 's/ $//')"
chk "ZPOPMAX"       "d 4"   "$($RCLI ZPOPMAX z | tr '\n' ' ' | sed 's/ $//')"
chk "ZCARD after"   "2"     "$($RCLI ZCARD z)"
chk "ZPOPMIN count=2" "b 2 c 3" "$($RCLI ZPOPMIN z 2 | tr '\n' ' ' | sed 's/ $//')"
chk "ZCARD empty"   "0"     "$($RCLI ZCARD z)"
chk "ZPOPMIN empty" ""      "$($RCLI ZPOPMIN z)"

section "ZREMRANGEBYRANK / ZREMRANGEBYSCORE / ZREM"
$RCLI DEL z >/dev/null
$RCLI ZADD z 1 a 2 b 3 c 4 d 5 e >/dev/null
chk "ZREMRANGEBYRANK 0 1" "2" "$($RCLI ZREMRANGEBYRANK z 0 1)"
chk "Remaining"     "c d e" "$($RCLI ZRANGE z 0 -1 | tr '\n' ' ' | sed 's/ $//')"
$RCLI DEL z >/dev/null
$RCLI ZADD z 1 a 2 b 3 c 4 d 5 e >/dev/null
chk "ZREMRANGEBYSCORE 2 4" "3" "$($RCLI ZREMRANGEBYSCORE z 2 4)"
chk "Remaining"     "a e"   "$($RCLI ZRANGE z 0 -1 | tr '\n' ' ' | sed 's/ $//')"
$RCLI DEL z >/dev/null
$RCLI ZADD z 1 a 2 b 3 c >/dev/null
chk "ZREM 1"        "1"     "$($RCLI ZREM z b)"
chk "ZREM missing"  "0"     "$($RCLI ZREM z nope)"
chk "ZREM multi"    "2"     "$($RCLI ZREM z a c x)"

section "ZRANDMEMBER"
$RCLI DEL z >/dev/null
$RCLI ZADD z 1 a 2 b 3 c 4 d 5 e >/dev/null
R=$($RCLI ZRANDMEMBER z)
[ -n "$R" ] && ok "ZRANDMEMBER single ($R)" || bad "ZRANDMEMBER single" "non-empty" ""
chk "ZRANDMEMBER 3 uniq" "3" "$($RCLI ZRANDMEMBER z 3 | sort -u | wc -l | tr -d ' ')"
chk "ZRANDMEMBER -7"     "7" "$($RCLI ZRANDMEMBER z -7 | wc -l | tr -d ' ')"
chk "ZCARD unchanged"    "5" "$($RCLI ZCARD z)"

section "Encoding upgrade (PACKED → SKIPLIST)"
$RCLI DEL bigz >/dev/null
ARGS=""; for i in $(seq 0 199); do ARGS="$ARGS $i m$i"; done
$RCLI ZADD bigz $ARGS >/dev/null
chk "ZCARD 200"     "200"   "$($RCLI ZCARD bigz)"
chk "ZSCORE m100"   "100"   "$($RCLI ZSCORE bigz m100)"
chk "ZRANK m100"    "100"   "$($RCLI ZRANK bigz m100)"
chk "ZREVRANK m100" "99"    "$($RCLI ZREVRANK bigz m100)"
chk "ZRANGE 0..2"   "m0 m1 m2" "$($RCLI ZRANGE bigz 0 2 | tr '\n' ' ' | sed 's/ $//')"
chk "ZRANGE -3..-1" "m197 m198 m199" "$($RCLI ZRANGE bigz -3 -1 | tr '\n' ' ' | sed 's/ $//')"
chk "ZCOUNT 50 99"  "50"    "$($RCLI ZCOUNT bigz 50 99)"

section "Wrong-type & missing-key"
$RCLI SET strk hello >/dev/null
W="WRONGTYPE Operation against a key holding the wrong kind of value"
chk "ZADD wrongtype"   "$W" "$($RCLI ZADD strk 1 a 2>&1)"
chk "ZCARD wrongtype"  "$W" "$($RCLI ZCARD strk 2>&1)"
chk "ZRANGE wrongtype" "$W" "$($RCLI ZRANGE strk 0 -1 2>&1)"
chk "ZCARD missing"    "0"  "$($RCLI ZCARD missing)"
chk "ZRANGE missing"   ""   "$($RCLI ZRANGE missing 0 -1)"
chk "ZSCORE missing"   ""   "$($RCLI ZSCORE missing m)"

summary "ZSET"
