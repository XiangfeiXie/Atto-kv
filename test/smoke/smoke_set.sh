#!/usr/bin/env bash
# SET data type smoke test (Redis-compatible).
set -u
DIR="$(cd "$(dirname "$0")" && pwd)"
. "${DIR}/_lib.sh"
require_server
P=$PORT

section "SADD / SREM / SCARD / SISMEMBER / SMEMBERS"
chk "SADD new"      "3"     "$($RCLI SADD s a b c)"
chk "SADD dup"      "1"     "$($RCLI SADD s a b d)"
chk "SCARD"         "4"     "$($RCLI SCARD s)"
chk "SISMEMBER yes" "1"     "$($RCLI SISMEMBER s a)"
chk "SISMEMBER no"  "0"     "$($RCLI SISMEMBER s zz)"
chk "SREM 1"        "1"     "$($RCLI SREM s a)"
chk "SREM missing"  "0"     "$($RCLI SREM s zz)"
chk "SCARD after"   "3"     "$($RCLI SCARD s)"
chk "SMEMBERS sorted" "b c d" "$($RCLI SMEMBERS s | sort | tr '\n' ' ' | sed 's/ $//')"

section "SMISMEMBER"
$RCLI DEL s >/dev/null
$RCLI SADD s a b c >/dev/null
chk "SMISMEMBER mix" "1 0 1 0" "$($RCLI SMISMEMBER s a x b y | tr '\n' ' ' | sed 's/ $//')"

section "SPOP (single + count)"
$RCLI DEL s >/dev/null
$RCLI SADD s a b c d e >/dev/null
P1=$($RCLI SPOP s)
[ -n "$P1" ] && ok "SPOP single ($P1)" || bad "SPOP single" "non-empty" ""
chk "SCARD after SPOP"  "4" "$($RCLI SCARD s)"
chk "SPOP count=2 lines" "2" "$($RCLI SPOP s 2 | wc -l | tr -d ' ')"
chk "SCARD after pop2"  "2" "$($RCLI SCARD s)"
chk "SPOP count > size" "2" "$($RCLI SPOP s 100 | wc -l | tr -d ' ')"
chk "SCARD empty"       "0" "$($RCLI SCARD s)"
chk "SPOP missing"      ""  "$($RCLI SPOP nokey)"

section "SRANDMEMBER"
$RCLI DEL s >/dev/null
$RCLI SADD s a b c d e >/dev/null
R=$($RCLI SRANDMEMBER s)
[ -n "$R" ] && ok "SRANDMEMBER single ($R)" || bad "SRANDMEMBER single" "non-empty" ""
chk "SRANDMEMBER 3 uniq" "3" "$($RCLI SRANDMEMBER s 3 | sort -u | wc -l | tr -d ' ')"
chk "SRANDMEMBER -7 size" "7" "$($RCLI SRANDMEMBER s -7 | wc -l | tr -d ' ')"
chk "SCARD unchanged"   "5" "$($RCLI SCARD s)"

section "SMOVE"
$RCLI DEL src dst >/dev/null
$RCLI SADD src a b c >/dev/null
chk "SMOVE existing" "1"  "$($RCLI SMOVE src dst a)"
chk "SMOVE missing"  "0"  "$($RCLI SMOVE src dst zz)"
chk "src has no a"   "0"  "$($RCLI SISMEMBER src a)"
chk "dst has a"      "1"  "$($RCLI SISMEMBER dst a)"

section "SINTER / SUNION / SDIFF + STORE"
$RCLI DEL A B R >/dev/null
$RCLI SADD A a b c d >/dev/null
$RCLI SADD B c d e f >/dev/null
chk "SINTER"    "c d"       "$($RCLI SINTER A B | sort | tr '\n' ' ' | sed 's/ $//')"
chk "SUNION"    "a b c d e f" "$($RCLI SUNION A B | sort | tr '\n' ' ' | sed 's/ $//')"
chk "SDIFF A B" "a b"       "$($RCLI SDIFF A B | sort | tr '\n' ' ' | sed 's/ $//')"
chk "SDIFF B A" "e f"       "$($RCLI SDIFF B A | sort | tr '\n' ' ' | sed 's/ $//')"
chk "SINTER w/missing" ""   "$($RCLI SINTER A nokey)"
chk "SINTERSTORE" "2"       "$($RCLI SINTERSTORE R A B)"
chk "R after INTER" "2"     "$($RCLI SCARD R)"
chk "SUNIONSTORE" "6"       "$($RCLI SUNIONSTORE R A B)"
chk "SDIFFSTORE"  "2"       "$($RCLI SDIFFSTORE R A B)"
chk "R after DIFF" "a b"    "$($RCLI SMEMBERS R | sort | tr '\n' ' ' | sed 's/ $//')"

section "Encoding upgrade (PACKED → DICT)"
$RCLI DEL big >/dev/null
ARGS=""; for i in $(seq 0 199); do ARGS="$ARGS m$i"; done
$RCLI SADD big $ARGS >/dev/null
chk "SCARD 200"     "200"   "$($RCLI SCARD big)"
chk "SISMEMBER m100" "1"    "$($RCLI SISMEMBER big m100)"
chk "SISMEMBER nope" "0"    "$($RCLI SISMEMBER big nope)"
chk "SMEMBERS count" "200"  "$($RCLI SMEMBERS big | wc -l | tr -d ' ')"
# Shrink
for i in $(seq 5 199); do $RCLI SREM big m$i >/dev/null; done
chk "SCARD shrunk"  "5"     "$($RCLI SCARD big)"
chk "SISMEMBER m0"  "1"     "$($RCLI SISMEMBER big m0)"
chk "SISMEMBER m5"  "0"     "$($RCLI SISMEMBER big m5)"
$RCLI SADD big newm >/dev/null
chk "SCARD re-grow" "6"     "$($RCLI SCARD big)"

section "Wrong-type & missing-key"
$RCLI SET strk hello >/dev/null
W="WRONGTYPE Operation against a key holding the wrong kind of value"
chk "SADD wrongtype"     "$W" "$($RCLI SADD strk x 2>&1)"
chk "SCARD wrongtype"    "$W" "$($RCLI SCARD strk 2>&1)"
chk "SMEMBERS wrongtype" "$W" "$($RCLI SMEMBERS strk 2>&1)"
chk "SCARD missing"      "0"  "$($RCLI SCARD missing)"
chk "SMEMBERS missing"   ""   "$($RCLI SMEMBERS missing)"
chk "SISMEMBER missing"  "0"  "$($RCLI SISMEMBER missing x)"

summary "SET"
