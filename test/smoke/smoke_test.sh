#!/usr/bin/env bash
# kvstore basic functionality test (smoke test).
#
# Usage:  ./smoke_test.sh [host] [port]
#         HOST=127.0.0.1 PORT=2000 ./smoke_test.sh
set -u

DIR="$(cd "$(dirname "$0")" && pwd)"
export HOST="${1:-${HOST:-127.0.0.1}}"
export PORT="${2:-${PORT:-2000}}"

# ── Load shared helpers ────────────────────────────────────────────────

. "${DIR}/_lib.sh"
require_server

# ═══════════════════════════════════════════════════════════════════════
#  BASIC: Connection / server / metadata
# ═══════════════════════════════════════════════════════════════════════

section "Connection / server basics"
chk "PING"              "PONG"       "$($RCLI PING)"
chk "PING message"      "hello"      "$($RCLI PING hello)"
chk "ECHO"              "echo-value" "$($RCLI ECHO echo-value)"
chk "SELECT 0"          "OK"         "$($RCLI SELECT 0)"
ERR=$($RCLI SELECT 1 2>&1)
case "$ERR" in *ERR*) ok "SELECT non-zero errors" ;; *) bad "SELECT non-zero errors" "ERR..." "$ERR" ;; esac

section "Database metadata"
$RCLI FLUSHALL >/dev/null
chk "DBSIZE empty"      "0"   "$($RCLI DBSIZE)"
$RCLI SET k v >/dev/null
$RCLI HSET h f v >/dev/null
SIZE=$($RCLI DBSIZE)
[ "$SIZE" -ge 2 ] && ok "DBSIZE after writes" || bad "DBSIZE after writes" ">=2" "$SIZE"
INFO_OUT=$($RCLI INFO 2>/dev/null | tr -d '\r' | head -5)
case "$INFO_OUT" in *redis_version:*|*kvstore_version:*) ok "INFO" ;; *) bad "INFO" "version line" "$INFO_OUT" ;; esac

section "Unknown command and FLUSHALL"
ERR=$($RCLI NO_SUCH_COMMAND 2>&1)
case "$ERR" in *ERR*|*unknown*) ok "Unknown command errors" ;; *) bad "Unknown command errors" "ERR..." "$ERR" ;; esac
chk "FLUSHALL"          "OK"  "$($RCLI FLUSHALL)"
chk "DBSIZE after FLUSHALL" "0" "$($RCLI DBSIZE)"

# ═══════════════════════════════════════════════════════════════════════
#  STRING: SET / GET / DEL / EXISTS / TYPE / STRLEN
# ═══════════════════════════════════════════════════════════════════════

section "Basic SET / GET / DEL / EXISTS / TYPE / STRLEN"
chk "SET"               "OK"     "$($RCLI SET k1 hello)"
chk "GET"               "hello"  "$($RCLI GET k1)"
chk "STRLEN"            "5"      "$($RCLI STRLEN k1)"
chk "TYPE"              "string" "$($RCLI TYPE k1)"
chk "EXISTS yes"        "1"      "$($RCLI EXISTS k1)"
chk "EXISTS no"         "0"      "$($RCLI EXISTS missing)"
chk "GET missing"       ""       "$($RCLI GET missing)"
chk "DEL existing"      "1"      "$($RCLI DEL k1)"
chk "DEL missing"       "0"      "$($RCLI DEL k1)"
chk "TYPE missing"      "none"   "$($RCLI TYPE k1)"

section "Overwrite & APPEND"
$RCLI SET k1 hello >/dev/null
chk "Overwrite"         "OK"     "$($RCLI SET k1 world)"
chk "After overwrite"   "world"  "$($RCLI GET k1)"
chk "APPEND new"        "5"      "$($RCLI APPEND newk abcde)"
chk "APPEND extend"     "10"     "$($RCLI APPEND newk fghij)"
chk "GET appended"      "abcdefghij" "$($RCLI GET newk)"

section "SETNX / SETEX / PSETEX"
$RCLI DEL k >/dev/null
chk "SETNX new"         "1"      "$($RCLI SETNX k first)"
chk "SETNX existing"    "0"      "$($RCLI SETNX k second)"
chk "Value unchanged"   "first"  "$($RCLI GET k)"
chk "SETEX 100s"        "OK"     "$($RCLI SETEX exk 100 v)"
TTL=$($RCLI TTL exk)
[ "$TTL" -ge 95 ] && [ "$TTL" -le 100 ] && ok "TTL ~100" || bad "TTL ~100" "95..100" "$TTL"

section "SET with EX / PX / NX / XX / KEEPTTL"
$RCLI DEL k >/dev/null
chk "SET EX 60"         "OK"     "$($RCLI SET k v EX 60)"
TTL=$($RCLI TTL k); [ "$TTL" -ge 55 ] && [ "$TTL" -le 60 ] \
    && ok "TTL ~60" || bad "TTL ~60" "55..60" "$TTL"
chk "SET PX 5000"       "OK"     "$($RCLI SET k v PX 5000)"
PT=$($RCLI PTTL k); [ "$PT" -ge 4500 ] && [ "$PT" -le 5000 ] \
    && ok "PTTL ~5000" || bad "PTTL ~5000" "4500..5000" "$PT"
$RCLI SET k v >/dev/null
chk "SET NX on existing" ""      "$($RCLI SET k v2 NX)"
chk "SET NX value unchanged" "v" "$($RCLI GET k)"
$RCLI DEL k >/dev/null
chk "SET NX on new"     "OK"     "$($RCLI SET k v NX)"
chk "SET XX on existing" "OK"    "$($RCLI SET k v2 XX)"
chk "SET XX new value"  "v2"     "$($RCLI GET k)"
$RCLI DEL k >/dev/null
chk "SET XX on missing" ""       "$($RCLI SET k v XX)"

section "INCR / DECR / INCRBY"
$RCLI SET ctr 10 >/dev/null
chk "INCR"              "11"     "$($RCLI INCR ctr)"
chk "DECR"              "10"     "$($RCLI DECR ctr)"
chk "INCRBY 5"          "15"     "$($RCLI INCRBY ctr 5)"
chk "DECRBY 3"          "12"     "$($RCLI DECRBY ctr 3)"
$RCLI DEL ctr >/dev/null
chk "INCR new key"      "1"      "$($RCLI INCR ctr)"
$RCLI SET notint hello >/dev/null
ERR=$($RCLI INCR notint 2>&1)
case "$ERR" in
    *ERR*not*integer*|*ERR*value*) ok "INCR on non-int errors" ;;
    *) bad "INCR on non-int errors" "ERR ... not an integer" "$ERR" ;;
esac

section "MSET / MGET / MSETNX"
chk "MSET"              "OK"     "$($RCLI MSET a 1 b 2 c 3)"
chk "MGET 3"            "1 2 3"  "$($RCLI MGET a b c | tr '\n' ' ' | sed 's/ $//')"
chk "MGET w/ miss"      "1  3"   "$($RCLI MGET a missing c | tr '\n' ' ' | sed 's/ $//')"
$RCLI DEL x y z newk >/dev/null
chk "MSETNX new"        "1"      "$($RCLI MSETNX x 1 y 2 z 3)"
chk "MSETNX with conflict" "0"   "$($RCLI MSETNX x 9 newk 1)"
chk "MSETNX rollback"   ""       "$($RCLI GET newk)"

section "EXPIRE / PERSIST / TTL / PTTL"
$RCLI SET k v >/dev/null
chk "EXPIRE"            "1"      "$($RCLI EXPIRE k 100)"
TTL=$($RCLI TTL k); [ "$TTL" -ge 95 ] && [ "$TTL" -le 100 ] \
    && ok "TTL after EXPIRE" || bad "TTL after EXPIRE" "95..100" "$TTL"
chk "PERSIST"           "1"      "$($RCLI PERSIST k)"
chk "TTL no expire"     "-1"     "$($RCLI TTL k)"
chk "TTL missing"       "-2"     "$($RCLI TTL nokey)"
chk "EXPIRE missing"    "0"      "$($RCLI EXPIRE nokey 100)"

section "Large values"
LONG=$(printf 'Y%.0s' {1..4000})
$RCLI SET big "$LONG" >/dev/null
chk "STRLEN large"      "4000"   "$($RCLI STRLEN big)"
chk "GET large roundtrip" "$LONG" "$($RCLI GET big)"

section "Hash wrong-type guard"
$RCLI DEL k >/dev/null
$RCLI HSET k f v >/dev/null
W="WRONGTYPE Operation against a key holding the wrong kind of value"
chk "GET on hash"       "$W"     "$($RCLI GET k 2>&1)"
chk "INCR on hash"      "$W"     "$($RCLI INCR k 2>&1)"
chk "APPEND on hash"    "$W"     "$($RCLI APPEND k x 2>&1)"

# ═══════════════════════════════════════════════════════════════════════
#  HASH: HSET / HGET / HDEL / HEXISTS / HLEN
# ═══════════════════════════════════════════════════════════════════════

section "HSET / HGET / HDEL / HEXISTS / HLEN"
chk "HSET 2 new"        "2"     "$($RCLI HSET H name Alice age 30)"
chk "HSET 0 update"     "0"     "$($RCLI HSET H name Bob)"
chk "HGET name"         "Bob"   "$($RCLI HGET H name)"
chk "HGET age"          "30"    "$($RCLI HGET H age)"
chk "HGET missing"      ""      "$($RCLI HGET H nope)"
chk "HEXISTS yes"       "1"     "$($RCLI HEXISTS H name)"
chk "HEXISTS no"        "0"     "$($RCLI HEXISTS H nope)"
chk "HLEN"              "2"     "$($RCLI HLEN H)"
chk "HDEL 1"            "1"     "$($RCLI HDEL H age)"
chk "HDEL missing"      "0"     "$($RCLI HDEL H nope)"
chk "HLEN after"        "1"     "$($RCLI HLEN H)"

section "HSETNX / HSTRLEN / HINCRBY / HINCRBYFLOAT"
$RCLI DEL H >/dev/null
chk "HSETNX new"        "1"     "$($RCLI HSETNX H f v)"
chk "HSETNX existing"   "0"     "$($RCLI HSETNX H f other)"
chk "Value unchanged"   "v"     "$($RCLI HGET H f)"
chk "HSTRLEN"           "1"     "$($RCLI HSTRLEN H f)"
chk "HSTRLEN missing"   "0"     "$($RCLI HSTRLEN H nope)"

$RCLI HSET H cnt 10 >/dev/null
chk "HINCRBY +5"        "15"    "$($RCLI HINCRBY H cnt 5)"
chk "HINCRBY -3"        "12"    "$($RCLI HINCRBY H cnt -3)"
chk "HINCRBY new"       "7"     "$($RCLI HINCRBY H newc 7)"
ERR=$($RCLI HINCRBY H f 1 2>&1)
case "$ERR" in *ERR*) ok "HINCRBY non-int errors" ;; *) bad "HINCRBY non-int errors" "ERR..." "$ERR" ;; esac

$RCLI HSET H pi 3.14 >/dev/null
RES=$($RCLI HINCRBYFLOAT H pi 0.86)
case "$RES" in 4|4.0|4.00) ok "HINCRBYFLOAT" ;; *) bad "HINCRBYFLOAT" "4 / 4.0" "$RES" ;; esac

section "HMGET / HMSET / HGETALL / HKEYS / HVALS"
$RCLI DEL H >/dev/null
$RCLI HSET H a 1 b 2 c 3 >/dev/null
chk "HMGET 3"           "1 2 3" "$($RCLI HMGET H a b c | tr '\n' ' ' | sed 's/ $//')"
chk "HMGET w/miss"      "1  3"  "$($RCLI HMGET H a nope c | tr '\n' ' ' | sed 's/ $//')"
chk "HMSET"             "OK"    "$($RCLI HMSET H d 4 e 5)"
chk "HLEN after HMSET"  "5"     "$($RCLI HLEN H)"
chk "HKEYS count"       "5"     "$($RCLI HKEYS H | wc -l | tr -d ' ')"
chk "HVALS count"       "5"     "$($RCLI HVALS H | wc -l | tr -d ' ')"
chk "HGETALL count"     "10"    "$($RCLI HGETALL H | wc -l | tr -d ' ')"
chk "HKEYS sorted"      "a b c d e"   "$($RCLI HKEYS H | sort | tr '\n' ' ' | sed 's/ $//')"
chk "HVALS sorted"      "1 2 3 4 5"   "$($RCLI HVALS H | sort -n | tr '\n' ' ' | sed 's/ $//')"

section "HRANDFIELD"
OUT=$($RCLI HRANDFIELD H 2>&1)
case "$OUT" in
    *unknown*command*|*ERR*) printf "  ${YEL}SKIP${RST}  HRANDFIELD (not implemented)\n" ;;
    *) [ -n "$OUT" ] && ok "HRANDFIELD single" || bad "HRANDFIELD single" "some key" "$OUT" ;;
esac

section "Encoding upgrade (PACKED → DICT)"
$RCLI DEL big >/dev/null
ARGS=""; for i in $(seq 0 199); do ARGS="$ARGS f$i v$i"; done
$RCLI HSET big $ARGS >/dev/null
chk "HLEN 200"          "200"   "$($RCLI HLEN big)"
chk "HGET f0"           "v0"    "$($RCLI HGET big f0)"
chk "HGET f100"         "v100"  "$($RCLI HGET big f100)"
chk "HGET f199"         "v199"  "$($RCLI HGET big f199)"
chk "HEXISTS mid"       "1"     "$($RCLI HEXISTS big f150)"
chk "HEXISTS nope"      "0"     "$($RCLI HEXISTS big nope)"
chk "HKEYS count"       "200"   "$($RCLI HKEYS big | wc -l | tr -d ' ')"
chk "HGETALL lines"     "400"   "$($RCLI HGETALL big | wc -l | tr -d ' ')"

section "Encoding shrink (DICT → PACKED)"
for i in $(seq 5 199); do $RCLI HDEL big f$i >/dev/null; done
chk "HLEN after shrink" "5"     "$($RCLI HLEN big)"
chk "HGET f0 ok"        "v0"    "$($RCLI HGET big f0)"
chk "HGET f4 ok"        "v4"    "$($RCLI HGET big f4)"
chk "HGET f5 gone"      ""      "$($RCLI HGET big f5)"
$RCLI HSET big f99 V99 >/dev/null
chk "HLEN re-grow"      "6"     "$($RCLI HLEN big)"
chk "HGET re-grown"     "V99"   "$($RCLI HGET big f99)"

section "Large field / value"
LONG=$(printf 'Z%.0s' {1..2000})
$RCLI DEL H2 >/dev/null
chk "HSET large"        "1"     "$($RCLI HSET H2 big_field "$LONG")"
chk "HSTRLEN 2000"      "2000"  "$($RCLI HSTRLEN H2 big_field)"
chk "HGET roundtrip"    "$LONG" "$($RCLI HGET H2 big_field)"

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

# ═══════════════════════════════════════════════════════════════════════
summary "BASIC"
