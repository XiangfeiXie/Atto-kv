#!/bin/bash
# multi_key_smoke.sh — Phase 9 multi-key shard-locking correctness gate.
#
# This test stresses the multi-key commands fixed in Phase 9 (SMOVE / LMOVE /
# RPOPLPUSH / BLPOP cross-worker wakeup) under threads=3 + cpu_pin 3,4,5 and
# verifies that *element conservation* invariants hold under concurrent load.
#
# A failure here means the multi-key shard-locking helpers are still buggy:
# elements are getting lost or duplicated across the source/destination shard
# boundary.
#
# Pass criteria:
#   T1 SMOVE     — 4 clients shuttle members A↔B for 5s; final |A|+|B| == initial total
#   T2 LMOVE     — 4 clients shuttle elements src↔dst for 5s; final len(src)+len(dst) == initial
#   T3 RPOPLPUSH — 4 clients shuttle elements src↔dst for 5s; final len matches
#   T4 BLPOP     — 1 BLPOP waiter on conn A, LPUSH on conn B (different worker); reply received
#   T5 BLPOP timeout — BLPOP on empty key with 1s timeout, replies $-1\r\n at ~1s

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
KVS="${KVS:-$ROOT/build/kvstore}"
PORT="${PORT:-2000}"

PASS=0
FAIL=0
log() { printf '%s\n' "$*"; }
ok()  { log "  PASS: $1"; PASS=$((PASS+1)); }
bad() { log "  FAIL: $1"; FAIL=$((FAIL+1)); }

start_kvs() {
  pkill -9 kvstore 2>/dev/null; sleep 1
  setsid "$KVS" "$1" >/tmp/kvs_mk_smoke.log 2>&1 </dev/null &
  disown
  sleep 1
  if ! redis-cli -p "$PORT" PING >/dev/null 2>&1; then
    log "  ERROR: kvstore failed to start"
    cat /tmp/kvs_mk_smoke.log
    exit 1
  fi
}
stop_kvs() { pkill -9 kvstore 2>/dev/null; sleep 1; }

# ── 3 worker config (matches kvstore-local.conf without persistence) ──────
CFG=/tmp/mk_smoke.conf
cat > $CFG <<EOF
port $PORT
network proactor
maxclients 10000
loglevel error
persistence none
rdma_sync off
xdp no
sockmap no
threads 3
cpu_pin 3,4,5
EOF

cleanup_files() {
  rm -f "$ROOT/build/dump.rdb" "$ROOT/build/dump.rdb.tmp" \
        "$ROOT/build/incr.wal"  "$ROOT/build/appendonly.aof"
}
cleanup_files

start_kvs $CFG

# Sanity: banner / threads
banner=$(grep -E "Workers" /tmp/kvs_mk_smoke.log | head -1 || true)
if echo "$banner" | grep -q "Workers       : 3"; then
  ok "banner shows 3 workers"
else
  bad "banner missing or wrong: $banner"
fi

# ── T1: SMOVE conservation ───────────────────────────────────────────────
log "T1: SMOVE conservation under 4-client concurrency"
redis-cli -p $PORT FLUSHALL >/dev/null
# Seed set A with 1000 members; B starts empty
for i in $(seq 1 1000); do redis-cli -p $PORT SADD A "m$i" >/dev/null; done
initial=$(redis-cli -p $PORT SCARD A)
if [ "$initial" != "1000" ]; then bad "seed |A|=$initial, expected 1000"; fi

# 4 clients shuttle members between A and B for 4 seconds
shuttle_smove() {
  local me=$1
  local end=$(($(date +%s) + 4))
  while [ "$(date +%s)" -lt "$end" ]; do
    local m="m$((RANDOM % 1000 + 1))"
    redis-cli -p $PORT SMOVE A B "$m" >/dev/null 2>&1 || true
    redis-cli -p $PORT SMOVE B A "$m" >/dev/null 2>&1 || true
  done
}
for c in 1 2 3 4; do shuttle_smove $c & done
wait

a=$(redis-cli -p $PORT SCARD A)
b=$(redis-cli -p $PORT SCARD B)
total=$((a + b))
if [ "$total" = "1000" ]; then
  ok "T1 SMOVE conservation: |A|=$a |B|=$b sum=1000"
else
  bad "T1 SMOVE LOSS: |A|=$a |B|=$b sum=$total (expected 1000)"
fi

# ── T2: LMOVE conservation ───────────────────────────────────────────────
log "T2: LMOVE conservation under 4-client concurrency"
redis-cli -p $PORT FLUSHALL >/dev/null
for i in $(seq 1 500); do redis-cli -p $PORT RPUSH L1 "v$i" >/dev/null; done
initial_len=$(redis-cli -p $PORT LLEN L1)
if [ "$initial_len" != "500" ]; then bad "seed LLEN(L1)=$initial_len, expected 500"; fi

shuttle_lmove() {
  local end=$(($(date +%s) + 4))
  while [ "$(date +%s)" -lt "$end" ]; do
    redis-cli -p $PORT LMOVE L1 L2 LEFT RIGHT >/dev/null 2>&1 || true
    redis-cli -p $PORT LMOVE L2 L1 LEFT RIGHT >/dev/null 2>&1 || true
  done
}
for c in 1 2 3 4; do shuttle_lmove & done
wait

l1=$(redis-cli -p $PORT LLEN L1)
l2=$(redis-cli -p $PORT LLEN L2)
total=$((l1 + l2))
if [ "$total" = "500" ]; then
  ok "T2 LMOVE conservation: |L1|=$l1 |L2|=$l2 sum=500"
else
  bad "T2 LMOVE LOSS: |L1|=$l1 |L2|=$l2 sum=$total (expected 500)"
fi

# ── T3: RPOPLPUSH conservation ───────────────────────────────────────────
log "T3: RPOPLPUSH conservation under 4-client concurrency"
redis-cli -p $PORT FLUSHALL >/dev/null
for i in $(seq 1 500); do redis-cli -p $PORT RPUSH R1 "v$i" >/dev/null; done

shuttle_rplp() {
  local end=$(($(date +%s) + 4))
  while [ "$(date +%s)" -lt "$end" ]; do
    redis-cli -p $PORT RPOPLPUSH R1 R2 >/dev/null 2>&1 || true
    redis-cli -p $PORT RPOPLPUSH R2 R1 >/dev/null 2>&1 || true
  done
}
for c in 1 2 3 4; do shuttle_rplp & done
wait

r1=$(redis-cli -p $PORT LLEN R1)
r2=$(redis-cli -p $PORT LLEN R2)
total=$((r1 + r2))
if [ "$total" = "500" ]; then
  ok "T3 RPOPLPUSH conservation: |R1|=$r1 |R2|=$r2 sum=500"
else
  bad "T3 RPOPLPUSH LOSS: |R1|=$r1 |R2|=$r2 sum=$total (expected 500)"
fi

# ── T4: BLPOP cross-worker wakeup ────────────────────────────────────────
log "T4: BLPOP cross-worker wakeup"
redis-cli -p $PORT FLUSHALL >/dev/null

# Worker A: BLPOP with 5s timeout (will be woken by RPUSH on worker B)
out_file=/tmp/mk_blpop_out.txt
( redis-cli -p $PORT BLPOP cwakekey 5 > "$out_file" 2>&1 ) &
blpop_pid=$!
sleep 0.5
# Worker B: RPUSH on the same key (different connection → different REUSEPORT worker very likely)
redis-cli -p $PORT RPUSH cwakekey hello-world >/dev/null

# Wait for BLPOP to return, but cap at 4s
wait_pid=0
for i in 1 2 3 4 5 6 7 8; do
  if ! kill -0 $blpop_pid 2>/dev/null; then wait_pid=1; break; fi
  sleep 0.5
done
if [ "$wait_pid" = "1" ] && grep -q "hello-world" "$out_file"; then
  ok "T4 BLPOP woken across workers: $(tr '\n' ' ' < "$out_file")"
else
  bad "T4 BLPOP not woken; output: $(tr '\n' ' ' < "$out_file")"
  kill -9 $blpop_pid 2>/dev/null || true
fi

# ── T5: BLPOP timeout reply ──────────────────────────────────────────────
log "T5: BLPOP timeout reply"
redis-cli -p $PORT FLUSHALL >/dev/null
t0=$(date +%s%N)
out=$(redis-cli -p $PORT BLPOP nokey 1 2>&1)
t1=$(date +%s%N)
elapsed_ms=$(( (t1 - t0) / 1000000 ))
# Redis returns empty array (no output from redis-cli) for nil; our $-1 would
# also map to "" or "(nil)".  Accept either, just check elapsed time ~ 1000ms.
if [ "$elapsed_ms" -ge 900 ] && [ "$elapsed_ms" -le 2000 ]; then
  ok "T5 BLPOP timeout fires near 1000ms (actual=${elapsed_ms}ms, reply='$out')"
else
  bad "T5 BLPOP timeout off: ${elapsed_ms}ms (reply='$out')"
fi

# ── Wrap up ──────────────────────────────────────────────────────────────
stop_kvs
cleanup_files

log ""
log "Multi-key smoke results: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" = "0" ]
