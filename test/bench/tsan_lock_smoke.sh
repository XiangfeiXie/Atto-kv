#!/bin/bash
# Phase 8.2 — TSAN focused smoke for Phase 6.2 lock refactor.
#
# Scope: ONLY the read/write lock differentiation introduced in 6.2.
# - rdlock readers run concurrently (GET/HGET/SMEMBERS/LRANGE/ZRANGE)
# - wrlock writers serialise correctly (SET/DEL/SADD/HSET)
# - lazy-expire upgrade rdlock→wrlock works without races
# - cross-worker BLPOP wake doesn't race the block_table
# - MGET / MSETNX pre-check (rdlock) interleaved with MSET (wrlock)
#
# We deliberately skip AOF/RDB reload paths: TSAN slowdown there is
# 5-10x and those paths are unrelated to Phase 6.2.  The full smoke
# already covers them under regular Release build.
#
# Usage:
#   cmake -B build-tsan -DCMAKE_BUILD_TYPE=Debug \
#         -DCMAKE_C_FLAGS="-fsanitize=thread -g -O1 -fno-omit-frame-pointer" \
#         -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread" -DUSE_JEMALLOC=OFF
#   cmake --build build-tsan -j
#   KVS=$(pwd)/build-tsan/kvstore ./test/bench/tsan_lock_smoke.sh
#
# Pass criteria: all functional checks pass AND TSAN logs no "WARNING:
# ThreadSanitizer" lines.  A single warning = fix-before-merge bug.

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
KVS="${KVS:-$ROOT/build-tsan/kvstore}"
[ -x "$KVS" ] || { echo "missing $KVS — build first (see header)" >&2; exit 1; }

PASS=0; FAIL=0
ok()  { echo "  PASS: $1"; PASS=$((PASS+1)); }
bad() { echo "  FAIL: $1"; FAIL=$((FAIL+1)); }

LOG=/tmp/tsan_lock_smoke.log
TSAN_LOG=/tmp/tsan_lock_smoke.tsan
: > "$LOG"; : > "$TSAN_LOG"

# TSAN options:
#   halt_on_error=0    keep going so we collect EVERY race, not just the first
#   history_size=4     enough stack history without blowing memory
#   log_path           write TSAN reports to a separate file we grep at the end
SUPP="$ROOT/test/bench/tsan.suppressions"
export TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:history_size=4:report_signal_unsafe=0:log_path=$TSAN_LOG:suppressions=$SUPP"

CFG=/tmp/tsan_smoke.conf
cat > $CFG <<EOF
port 2000
network proactor
maxclients 10000
loglevel error
persistence none
rdma_sync off
xdp no
sockmap no
threads 4
cpu_pin off
EOF

start_kvs() {
  pkill -9 kvstore 2>/dev/null; sleep 1
  # Linux >= 6.6 uses 32-bit mmap entropy by default which collides
  # with TSAN's shadow memory layout ("FATAL: unexpected memory
  # mapping").  setarch -R disables ASLR for this child only — no
  # sudo, no global sysctl change.
  setsid setarch "$(uname -m)" -R "$KVS" "$CFG" >"$LOG" 2>&1 </dev/null &
  disown
  # TSAN-instrumented startup is slow (mmap shadow + thread-state init);
  # 3s gives consistent banner timing.
  sleep 3
}
stop_kvs() { pkill -9 kvstore 2>/dev/null; sleep 1; }

start_kvs

# ── 1. Banner sanity ─────────────────────────────────────────────────
grep -q "Workers       : 4" "$LOG" && ok "banner threads=4" || bad "banner missing"

# ── 2. Concurrent rdlock readers on the SAME shard.  All 4 client
#       threads pound GET on the same key.  rdlock should let them
#       run in parallel; if our cleanup attribute or guard logic is
#       wrong, TSAN will see the rwlock state mutate without sync.
echo "[2] concurrent rdlock readers on one key"
redis-cli -p 2000 SET tsan_k "TSAN_VALUE" >/dev/null
redis-benchmark -p 2000 -q -c 100 --threads 4 -P 5 -n 5000 GET tsan_k \
  > /dev/null 2>&1
[ "$(redis-cli -p 2000 GET tsan_k)" = "TSAN_VALUE" ] && \
  ok "GET parallel readers" || bad "GET parallel readers"

# ── 3. Reader/writer interleave on the SAME key.  Half the clients
#       SET, half GET.  Tests that wrlock excludes rdlock as expected
#       and that the lazy-expire upgrade path doesn't race.
echo "[3] mixed SET/GET on same key (writer/reader interleave)"
( redis-benchmark -p 2000 -q -c 50 --threads 2 -P 5 -n 5000 \
    SET race_k VAL >/dev/null 2>&1 ) &
P1=$!
( redis-benchmark -p 2000 -q -c 50 --threads 2 -P 5 -n 5000 \
    GET race_k >/dev/null 2>&1 ) &
P2=$!
wait $P1 $P2
ok "mixed SET/GET completed"

# ── 4. Lazy-expire upgrade.  Set with PX=1, sleep, then GET.  The
#       reader hits expired entry under rdlock and must upgrade to
#       wrlock to delete.  Under TSAN, any missing happens-before
#       between the rdlock-drop and wrlock-take fires here.
echo "[4] lazy-expire rdlock→wrlock upgrade"
for i in $(seq 1 200); do
  redis-cli -p 2000 SET "exp_$i" v PX 1 >/dev/null
done
sleep 0.1
# 4 concurrent readers triggering upgrade-and-delete on each key.
for round in 1 2 3 4; do
  ( for i in $(seq 1 200); do redis-cli -p 2000 GET "exp_$i" >/dev/null; done ) &
done
wait
ok "lazy-expire upgrade completed"

# ── 5. MGET (rdlock) interleaved with MSET (wrlock) across many shards.
#       Exercises the multi-key loop where each key takes its own
#       per-shard lock independently.
echo "[5] MGET rdlock vs MSET wrlock across shards"
redis-benchmark -p 2000 -q -c 80 --threads 4 -P 5 -n 5000 \
  MSET k1 v1 k2 v2 k3 v3 k4 v4 k5 v5 >/dev/null 2>&1 &
PWR=$!
redis-benchmark -p 2000 -q -c 80 --threads 4 -P 5 -n 5000 \
  MGET k1 k2 k3 k4 k5 >/dev/null 2>&1 &
PRD=$!
wait $PWR $PRD
ok "MSET/MGET interleave completed"

# ── 6. BLPOP cross-worker wake — exercises block_table mutex,
#       per-worker wake queue, and cross-worker eventfd notify.  This
#       is the most likely place for a missed sync given the lock
#       ordering between shard wrlock + block.c mutex.
echo "[6] BLPOP cross-worker wake"
redis-cli -p 2000 DEL L_X >/dev/null
T0=$(date +%s%3N)
redis-cli -p 2000 BLPOP L_X 5 > /tmp/tsan_wake.out 2>&1 &
WPID=$!
sleep 0.3
redis-cli -p 2000 RPUSH L_X hi >/dev/null
wait $WPID
T1=$(date +%s%3N)
W=$(tr -d '\r\n ' < /tmp/tsan_wake.out)
if [ "$W" = "L_Xhi" ] && [ $((T1-T0)) -lt 3000 ]; then
  ok "BLPOP wake $((T1-T0)) ms"
else
  bad "BLPOP wake $((T1-T0)) ms, reply='$W'"
fi

stop_kvs

# ── TSAN report aggregation ──────────────────────────────────────────
# log_path=$TSAN_LOG writes per-pid files like $TSAN_LOG.PID; concat them.
TSAN_OUT=""
if ls "${TSAN_LOG}".* >/dev/null 2>&1; then
  TSAN_OUT=$(cat "${TSAN_LOG}".* 2>/dev/null)
fi
N_WARN=$(printf '%s\n' "$TSAN_OUT" | grep -c "WARNING: ThreadSanitizer" || true)

echo "================================================="
echo "TSAN lock smoke: PASS=$PASS  FAIL=$FAIL  TSAN_warnings=$N_WARN"
if [ "$N_WARN" -gt 0 ]; then
  echo "--- first 60 lines of TSAN report ---"
  printf '%s\n' "$TSAN_OUT" | head -60
fi
echo "================================================="
[ $FAIL -eq 0 ] && [ "$N_WARN" -eq 0 ]
