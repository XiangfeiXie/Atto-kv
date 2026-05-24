#!/usr/bin/env bash
set -u
DIR="$(cd "$(dirname "$0")" && pwd)"
. "${DIR}/_lib.sh"
require_server

section "Connection / server basics"
chk "PING" "PONG" "$($RCLI PING)"
chk "PING message" "hello" "$($RCLI PING hello)"
chk "ECHO" "echo-value" "$($RCLI ECHO echo-value)"
chk "SELECT 0" "OK" "$($RCLI SELECT 0)"
ERR=$($RCLI SELECT 1 2>&1)
case "$ERR" in *ERR*) ok "SELECT non-zero errors" ;; *) bad "SELECT non-zero errors" "ERR..." "$ERR" ;; esac

section "Database metadata"
$RCLI FLUSHALL >/dev/null
chk "DBSIZE empty" "0" "$($RCLI DBSIZE)"
$RCLI SET k v >/dev/null
$RCLI HSET h f v >/dev/null
SIZE=$($RCLI DBSIZE)
[ "$SIZE" -ge 2 ] && ok "DBSIZE after writes" || bad "DBSIZE after writes" ">=2" "$SIZE"
INFO_OUT=$($RCLI INFO 2>/dev/null | tr -d '\r' | head -5)
case "$INFO_OUT" in *redis_version:*|*kvstore_version:*) ok "INFO" ;; *) bad "INFO" "version line" "$INFO_OUT" ;; esac

section "Unknown command and FLUSHALL"
ERR=$($RCLI NO_SUCH_COMMAND 2>&1)
case "$ERR" in *ERR*|*unknown*) ok "Unknown command errors" ;; *) bad "Unknown command errors" "ERR..." "$ERR" ;; esac
chk "FLUSHALL" "OK" "$($RCLI FLUSHALL)"
chk "DBSIZE after FLUSHALL" "0" "$($RCLI DBSIZE)"

summary "BASIC"
