#!/usr/bin/env bash
# HASH data type smoke test (Redis-compatible).
set -u
DIR="$(cd "$(dirname "$0")" && pwd)"
. "${DIR}/_lib.sh"
require_server
P=$PORT

section "HSET / HGET / HDEL / HEXISTS / HLEN"
chk "HSET 2 new"    "2"     "$($RCLI HSET H name Alice age 30)"
chk "HSET 0 update" "0"     "$($RCLI HSET H name Bob)"
chk "HGET name"     "Bob"   "$($RCLI HGET H name)"
chk "HGET age"      "30"    "$($RCLI HGET H age)"
chk "HGET missing"  ""      "$($RCLI HGET H nope)"
chk "HEXISTS yes"   "1"     "$($RCLI HEXISTS H name)"
chk "HEXISTS no"    "0"     "$($RCLI HEXISTS H nope)"
chk "HLEN"          "2"     "$($RCLI HLEN H)"
chk "HDEL 1"        "1"     "$($RCLI HDEL H age)"
chk "HDEL missing"  "0"     "$($RCLI HDEL H nope)"
chk "HLEN after"    "1"     "$($RCLI HLEN H)"

section "HSETNX / HSTRLEN / HINCRBY / HINCRBYFLOAT"
$RCLI DEL H >/dev/null
chk "HSETNX new"    "1"     "$($RCLI HSETNX H f v)"
chk "HSETNX existing" "0"   "$($RCLI HSETNX H f other)"
chk "Value unchanged" "v"   "$($RCLI HGET H f)"
chk "HSTRLEN"       "1"     "$($RCLI HSTRLEN H f)"
chk "HSTRLEN missing" "0"   "$($RCLI HSTRLEN H nope)"

$RCLI HSET H cnt 10 >/dev/null
chk "HINCRBY +5"    "15"    "$($RCLI HINCRBY H cnt 5)"
chk "HINCRBY -3"    "12"    "$($RCLI HINCRBY H cnt -3)"
chk "HINCRBY new"   "7"     "$($RCLI HINCRBY H newc 7)"
ERR=$($RCLI HINCRBY H f 1 2>&1)
case "$ERR" in *ERR*) ok "HINCRBY non-int errors" ;; *) bad "HINCRBY non-int errors" "ERR..." "$ERR" ;; esac

$RCLI HSET H pi 3.14 >/dev/null
RES=$($RCLI HINCRBYFLOAT H pi 0.86)
case "$RES" in 4|4.0|4.00) ok "HINCRBYFLOAT" ;; *) bad "HINCRBYFLOAT" "4 / 4.0" "$RES" ;; esac

section "HMGET / HMSET / HGETALL / HKEYS / HVALS"
$RCLI DEL H >/dev/null
$RCLI HSET H a 1 b 2 c 3 >/dev/null
chk "HMGET 3"       "1 2 3" "$($RCLI HMGET H a b c | tr '\n' ' ' | sed 's/ $//')"
chk "HMGET w/miss"  "1  3"  "$($RCLI HMGET H a nope c | tr '\n' ' ' | sed 's/ $//')"
chk "HMSET"         "OK"    "$($RCLI HMSET H d 4 e 5)"
chk "HLEN after HMSET" "5"  "$($RCLI HLEN H)"
chk "HKEYS count"   "5"     "$($RCLI HKEYS H | wc -l | tr -d ' ')"
chk "HVALS count"   "5"     "$($RCLI HVALS H | wc -l | tr -d ' ')"
chk "HGETALL count" "10"    "$($RCLI HGETALL H | wc -l | tr -d ' ')"
# Sorted content check
chk "HKEYS sorted"  "a b c d e" "$($RCLI HKEYS H | sort | tr '\n' ' ' | sed 's/ $//')"
chk "HVALS sorted"  "1 2 3 4 5" "$($RCLI HVALS H | sort -n | tr '\n' ' ' | sed 's/ $//')"

section "HRANDFIELD (if supported)"
# Skip silently if command not supported
OUT=$($RCLI HRANDFIELD H 2>&1)
case "$OUT" in
    *unknown*command*|*ERR*) printf "  ${YEL}SKIP${RST}  HRANDFIELD (not implemented)\n" ;;
    *) [ -n "$OUT" ] && ok "HRANDFIELD single" || bad "HRANDFIELD single" "some key" "$OUT" ;;
esac

section "Encoding upgrade (PACKED → DICT)"
$RCLI DEL big >/dev/null
ARGS=""; for i in $(seq 0 199); do ARGS="$ARGS f$i v$i"; done
$RCLI HSET big $ARGS >/dev/null
chk "HLEN 200"      "200"   "$($RCLI HLEN big)"
chk "HGET f0"       "v0"    "$($RCLI HGET big f0)"
chk "HGET f100"     "v100"  "$($RCLI HGET big f100)"
chk "HGET f199"     "v199"  "$($RCLI HGET big f199)"
chk "HEXISTS mid"   "1"     "$($RCLI HEXISTS big f150)"
chk "HEXISTS nope"  "0"     "$($RCLI HEXISTS big nope)"
chk "HKEYS count"   "200"   "$($RCLI HKEYS big | wc -l | tr -d ' ')"
chk "HGETALL lines" "400"   "$($RCLI HGETALL big | wc -l | tr -d ' ')"

section "Encoding shrink (DICT → PACKED)"
# Remove down to tiny size; HLEN must track accurately
for i in $(seq 5 199); do $RCLI HDEL big f$i >/dev/null; done
chk "HLEN after shrink" "5" "$($RCLI HLEN big)"
chk "HGET f0 ok"    "v0"    "$($RCLI HGET big f0)"
chk "HGET f4 ok"    "v4"    "$($RCLI HGET big f4)"
chk "HGET f5 gone"  ""      "$($RCLI HGET big f5)"
# Re-grow should still work
$RCLI HSET big f99 V99 >/dev/null
chk "HLEN re-grow"  "6"     "$($RCLI HLEN big)"
chk "HGET re-grown" "V99"   "$($RCLI HGET big f99)"

section "Large field / value"
LONG=$(printf 'Z%.0s' {1..2000})
$RCLI DEL H2 >/dev/null
chk "HSET large"    "1"     "$($RCLI HSET H2 big_field "$LONG")"
chk "HSTRLEN 2000"  "2000"  "$($RCLI HSTRLEN H2 big_field)"
chk "HGET roundtrip" "$LONG" "$($RCLI HGET H2 big_field)"

section "Wrong-type & missing-key"
$RCLI SET strk hello >/dev/null
W="WRONGTYPE Operation against a key holding the wrong kind of value"
chk "HSET wrongtype"    "$W" "$($RCLI HSET strk f v 2>&1)"
chk "HGET wrongtype"    "$W" "$($RCLI HGET strk f 2>&1)"
chk "HLEN wrongtype"    "$W" "$($RCLI HLEN strk 2>&1)"
chk "HLEN missing key"  "0"  "$($RCLI HLEN missing)"
chk "HGET missing key"  ""   "$($RCLI HGET missing f)"
chk "HDEL missing key"  "0"  "$($RCLI HDEL missing f)"
chk "HKEYS missing key" ""   "$($RCLI HKEYS missing)"

summary "HASH"
