#!/usr/bin/env bash
# STRING data type smoke test (Redis-compatible).
set -u
DIR="$(cd "$(dirname "$0")" && pwd)"
. "${DIR}/_lib.sh"
require_server
P=$PORT

section "Basic SET / GET / DEL / EXISTS / TYPE / STRLEN"
chk "SET"           "OK"     "$($RCLI SET k1 hello)"
chk "GET"           "hello"  "$($RCLI GET k1)"
chk "STRLEN"        "5"      "$($RCLI STRLEN k1)"
chk "TYPE"          "string" "$($RCLI TYPE k1)"
chk "EXISTS yes"    "1"      "$($RCLI EXISTS k1)"
chk "EXISTS no"     "0"      "$($RCLI EXISTS missing)"
chk "GET missing"   ""       "$($RCLI GET missing)"
chk "DEL existing"  "1"      "$($RCLI DEL k1)"
chk "DEL missing"   "0"      "$($RCLI DEL k1)"
chk "TYPE missing"  "none"   "$($RCLI TYPE k1)"

section "Overwrite & APPEND"
$RCLI SET k1 hello >/dev/null
chk "Overwrite"     "OK"     "$($RCLI SET k1 world)"
chk "After overwrite" "world" "$($RCLI GET k1)"
chk "APPEND new"    "5"      "$($RCLI APPEND newk abcde)"
chk "APPEND extend" "10"     "$($RCLI APPEND newk fghij)"
chk "GET appended"  "abcdefghij" "$($RCLI GET newk)"

section "SETNX / SETEX / PSETEX"
$RCLI DEL k >/dev/null
chk "SETNX new"     "1"      "$($RCLI SETNX k first)"
chk "SETNX existing" "0"     "$($RCLI SETNX k second)"
chk "Value unchanged" "first" "$($RCLI GET k)"
chk "SETEX 100s"    "OK"     "$($RCLI SETEX exk 100 v)"
TTL=$($RCLI TTL exk)
[ "$TTL" -ge 95 ] && [ "$TTL" -le 100 ] && ok "TTL ~100" || bad "TTL ~100" "95..100" "$TTL"

section "SET with EX / PX / NX / XX / KEEPTTL"
$RCLI DEL k >/dev/null
chk "SET EX 60"     "OK"     "$($RCLI SET k v EX 60)"
TTL=$($RCLI TTL k); [ "$TTL" -ge 55 ] && [ "$TTL" -le 60 ] \
    && ok "TTL ~60" || bad "TTL ~60" "55..60" "$TTL"
chk "SET PX 5000"   "OK"     "$($RCLI SET k v PX 5000)"
PT=$($RCLI PTTL k); [ "$PT" -ge 4900 ] && [ "$PT" -le 5000 ] \
    && ok "PTTL ~5000" || bad "PTTL ~5000" "4900..5000" "$PT"
$RCLI SET k v >/dev/null
chk "SET NX on existing" "" "$($RCLI SET k v2 NX)"
chk "SET NX value unchanged" "v" "$($RCLI GET k)"
$RCLI DEL k >/dev/null
chk "SET NX on new" "OK" "$($RCLI SET k v NX)"
chk "SET XX on existing" "OK" "$($RCLI SET k v2 XX)"
chk "SET XX new value"   "v2" "$($RCLI GET k)"
$RCLI DEL k >/dev/null
chk "SET XX on missing"  "" "$($RCLI SET k v XX)"

section "INCR / DECR / INCRBY"
$RCLI SET ctr 10 >/dev/null
chk "INCR"          "11"     "$($RCLI INCR ctr)"
chk "DECR"          "10"     "$($RCLI DECR ctr)"
chk "INCRBY 5"      "15"     "$($RCLI INCRBY ctr 5)"
chk "DECRBY 3"      "12"     "$($RCLI DECRBY ctr 3)"
$RCLI DEL ctr >/dev/null
chk "INCR new key"  "1"      "$($RCLI INCR ctr)"
$RCLI SET notint hello >/dev/null
ERR=$($RCLI INCR notint 2>&1)
case "$ERR" in
    *ERR*not*integer*|*ERR*value*) ok "INCR on non-int errors" ;;
    *) bad "INCR on non-int errors" "ERR ... not an integer" "$ERR" ;;
esac
# $RCLI SET f 3.14 >/dev/null
# RES=$($RCLI INCRBYFLOAT f 0.86)
# case "$RES" in 4|4.0|4.00) ok "INCRBYFLOAT" ;; *) bad "INCRBYFLOAT" "4 / 4.0 / 4.00" "$RES" ;; esac

section "MSET / MGET / MSETNX"
chk "MSET"          "OK"     "$($RCLI MSET a 1 b 2 c 3)"
chk "MGET 3"        "1 2 3"  "$($RCLI MGET a b c | tr '\n' ' ' | sed 's/ $//')"
chk "MGET w/ miss"  "1  3"   "$($RCLI MGET a missing c | tr '\n' ' ' | sed 's/ $//')"
$RCLI DEL x y z newk >/dev/null
chk "MSETNX new"    "1"      "$($RCLI MSETNX x 1 y 2 z 3)"
chk "MSETNX with conflict" "0" "$($RCLI MSETNX x 9 newk 1)"
chk "MSETNX rollback" ""     "$($RCLI GET newk)"

section "EXPIRE / PERSIST / TTL / PTTL"
$RCLI SET k v >/dev/null
chk "EXPIRE"        "1"      "$($RCLI EXPIRE k 100)"
TTL=$($RCLI TTL k); [ "$TTL" -ge 95 ] && [ "$TTL" -le 100 ] \
    && ok "TTL after EXPIRE" || bad "TTL after EXPIRE" "95..100" "$TTL"
chk "PERSIST"       "1"      "$($RCLI PERSIST k)"
chk "TTL no expire" "-1"     "$($RCLI TTL k)"
chk "TTL missing"   "-2"     "$($RCLI TTL nokey)"
chk "EXPIRE missing" "0"     "$($RCLI EXPIRE nokey 100)"

section "Large values"
LONG=$(printf 'Y%.0s' {1..4000})
$RCLI SET big "$LONG" >/dev/null
chk "STRLEN large"  "4000"   "$($RCLI STRLEN big)"
chk "GET large roundtrip" "$LONG" "$($RCLI GET big)"

section "Hash wrong-type guard"
$RCLI DEL k >/dev/null
$RCLI HSET k f v >/dev/null
W="WRONGTYPE Operation against a key holding the wrong kind of value"
chk "GET on hash"    "$W"     "$($RCLI GET k 2>&1)"
chk "INCR on hash"   "$W"     "$($RCLI INCR k 2>&1)"
chk "APPEND on hash" "$W"     "$($RCLI APPEND k x 2>&1)"

summary "STRING"
