#!/bin/bash
# Phase 4B.6 — multi-thread smoke test.
#
# Runs the full surface (auto threads, all five datatypes, BLPOP cross-worker
# wakeup, AOF persistence + reload, RDB BGSAVE during writes + reload) under
# threads=4 and confirms every check passes.  Together with the per-sub-phase
# tests this is the gate for declaring Phase 4B "done".
#
# Pass criteria:
#   - banner prints "Workers: 4 [manual]"
#   - SET/GET/DEL round-trip under load
#   - HASH/LIST/SET/ZSET basic ops
#   - BLPOP timeout and cross-worker wakeup
#   - AOF: write 50k random keys, restart, DBSIZE preserved
#   - RDB: BGSAVE during writes, reload yields key count in [pre, post] window

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# Allow overriding for sanitizer builds: KVS=$(pwd)/build-asan/kvstore ./smoke.sh
KVS="${KVS:-$ROOT/build/kvstore}"

PASS=0
FAIL=0
log() { printf '%s\n' "$*"; }
ok()  { log "  PASS: $1"; PASS=$((PASS+1)); }
bad() { log "  FAIL: $1"; FAIL=$((FAIL+1)); }

start_kvs() {
  pkill -9 kvstore 2>/dev/null; sleep 1
  setsid "$KVS" "$1" >/tmp/kvs_smoke.log 2>&1 </dev/null &
  disown
  sleep 1
}

stop_kvs() { pkill -9 kvstore 2>/dev/null; sleep 1; }

# ── Common config ───────────────────────────────────────────────────────────
mkconf() {
  local persist="$1"
  cat <<EOF
port 2000
network proactor
maxclients 10000
loglevel notice
persistence $persist
rdma_sync off
xdp no
sockmap no
threads 4
EOF
}

CFG_NONE=/tmp/smoke_none.conf;  mkconf none > $CFG_NONE
CFG_AOF=/tmp/smoke_aof.conf;    mkconf aof  > $CFG_AOF
CFG_RDB=/tmp/smoke_rdb.conf;    mkconf rdb  > $CFG_RDB

rm -f "$ROOT/build/dump.rdb" "$ROOT/build/dump.rdb.tmp" \
      "$ROOT/build/incr.wal"  "$ROOT/build/appendonly.aof"

# ── Test 1: banner sanity ───────────────────────────────────────────────────
log "[1] Banner: threads=4 [manual] (nproc=N, 32 shards, SO_REUSEPORT)"
start_kvs $CFG_NONE
if grep -q "Workers       : 4 \[manual\]" /tmp/kvs_smoke.log; then ok "banner"
else bad "banner missing"; head -20 /tmp/kvs_smoke.log; fi

# ── Test 2: STRING SET/GET/DEL ──────────────────────────────────────────────
log "[2] STRING SET/GET/DEL round-trip"
redis-cli -p 2000 SET hello world >/dev/null
[ "$(redis-cli -p 2000 GET hello)" = "world" ] && ok "GET" || bad "GET"
redis-cli -p 2000 DEL hello >/dev/null
[ -z "$(redis-cli -p 2000 GET hello)" ] && ok "DEL" || bad "DEL"

# ── Test 3: HASH / LIST / SET / ZSET ────────────────────────────────────────
log "[3] HASH / LIST / SET / ZSET"
redis-cli -p 2000 HSET h1 f1 v1 f2 v2 >/dev/null
[ "$(redis-cli -p 2000 HGET h1 f1)" = "v1" ] && ok "HASH" || bad "HASH"

redis-cli -p 2000 RPUSH l1 a b c >/dev/null
[ "$(redis-cli -p 2000 LLEN l1)" = "3" ] && ok "LIST" || bad "LIST"

redis-cli -p 2000 SADD s1 a b c >/dev/null
[ "$(redis-cli -p 2000 SCARD s1)" = "3" ] && ok "SET" || bad "SET"

redis-cli -p 2000 ZADD z1 1 a 2 b 3 c >/dev/null
[ "$(redis-cli -p 2000 ZCARD z1)" = "3" ] && ok "ZSET" || bad "ZSET"

# ── Test 4: BLPOP timeout and cross-worker wakeup ───────────────────────────
log "[4] BLPOP timeout (~2s on empty list)"
T0=$(date +%s%3N)
OUT=$(redis-cli -p 2000 BLPOP empty 2)
T1=$(date +%s%3N)
EL=$((T1-T0))
if [ -z "$OUT" ] && [ $EL -ge 1800 ] && [ $EL -le 2400 ]; then
  ok "BLPOP timeout (${EL} ms, nil reply)"
else
  bad "BLPOP timeout (${EL} ms, reply='$OUT')"
fi

log "[5] BLPOP cross-conn wakeup (push from worker B wakes block on worker A)"
redis-cli -p 2000 DEL L_X >/dev/null
T0=$(date +%s%3N)
redis-cli -p 2000 BLPOP L_X 5 > /tmp/wake.out 2>&1 &
WPID=$!
sleep 0.5
redis-cli -p 2000 RPUSH L_X hello >/dev/null
wait $WPID
T1=$(date +%s%3N)
WAKE_OUT=$(tr -d '\r\n ' < /tmp/wake.out)
if [ "$WAKE_OUT" = "L_Xhello" ] && [ $((T1-T0)) -lt 2000 ]; then
  ok "BLPOP wakeup ($((T1-T0)) ms)"
else
  bad "BLPOP wakeup ($((T1-T0)) ms, reply='$WAKE_OUT')"
fi

stop_kvs

# ── Test 6: AOF MT — write 50k random keys, restart, reload ─────────────────
log "[6] AOF persistence under threads=4"
rm -f "$ROOT/build/incr.wal" "$ROOT/build/appendonly.aof"
start_kvs $CFG_AOF
redis-benchmark -p 2000 -t set -c 100 -n 50000 -P 10 --threads 4 -r 30000 -q \
  >/dev/null 2>&1
DB1=$(redis-cli -p 2000 DBSIZE)
stop_kvs
start_kvs $CFG_AOF
DB2=$(redis-cli -p 2000 DBSIZE)
if [ "$DB1" = "$DB2" ] && [ "$DB1" -gt 0 ]; then
  ok "AOF reload preserves $DB1 keys"
else
  bad "AOF reload diverged: pre=$DB1 post=$DB2"
fi
stop_kvs

# ── Test 7: RDB BGSAVE during writes ────────────────────────────────────────
log "[7] RDB BGSAVE point-in-time snapshot under threads=4"
rm -f "$ROOT/build/dump.rdb" "$ROOT/build/incr.wal"
start_kvs $CFG_RDB

redis-benchmark -p 2000 -t set -c 100 -n 20000 -P 10 --threads 4 -r 20000 -q \
  >/dev/null 2>&1
DB1=$(redis-cli -p 2000 DBSIZE)

( redis-benchmark -p 2000 -t set -c 100 -n 100000 -P 10 --threads 4 -r 30000 -q \
    >/dev/null 2>&1 ) &
WPID=$!
sleep 0.2
redis-cli -p 2000 BGSAVE >/dev/null
wait $WPID
sleep 1
DB2=$(redis-cli -p 2000 DBSIZE)

# Capture the BGSAVE stall line BEFORE stopping (and BEFORE restart, which
# would otherwise truncate /tmp/kvs_smoke.log).
STALL=$(grep "snapshot stall" /tmp/kvs_smoke.log | tail -1)

stop_kvs

start_kvs $CFG_RDB
DB3=$(redis-cli -p 2000 DBSIZE)
stop_kvs

if [ "$DB3" -ge "$DB1" ] && [ "$DB3" -le "$DB2" ]; then
  ok "RDB reload: $DB3 keys in snapshot window [$DB1, $DB2]"
else
  bad "RDB reload outside window: pre=$DB1, post=$DB2, reload=$DB3"
fi

if [ -n "$STALL" ]; then
  ok "stall log: $STALL"
else
  bad "no snapshot stall log"
fi

# ── Summary ─────────────────────────────────────────────────────────────────
echo "================================================="
echo "Phase 4B smoke results: PASS=$PASS  FAIL=$FAIL"
echo "================================================="
[ $FAIL -eq 0 ]
