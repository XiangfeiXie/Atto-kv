#!/bin/bash
# profile_capture.sh — Data-driven hot-path identification.
#
# Runs kvstore + redis-benchmark on disjoint P-core subsets, captures:
#   1. perf top   — top 20 functions by CPU time
#   2. perf stat  — IPC, cache miss rate, branch miss rate
#   3. /proc/<pid>/status — peak RSS, threads
#
# Output: test/bench/profile_<git>_<scenario>.txt
#
# Use this BEFORE deciding what to optimize.  Phase 6.3 (perfect-hash
# dispatch) and Phase 7.1 (B.6) are gated on what shows up here.
#
# Usage:
#   ./test/bench/profile_capture.sh             # default: t=4 SET-heavy
#   ./test/bench/profile_capture.sh GET 5       # GET-heavy with t=5

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
KVS="$ROOT/build/kvstore"
HOST=$(hostname -s)
GIT=$(cd "$ROOT" && git rev-parse --short HEAD 2>/dev/null || echo "?")

CMD="${1:-set}"            # set | get | mset | mixed
SERVER_T="${2:-4}"         # number of server workers
DURATION="${3:-30}"        # perf top sampling seconds

# CPU split on the 6 P-cores (0-5):
#   server: 0..(SERVER_T-1)        — pinned via cpu_pin auto inside kvstore
#   bench:  SERVER_T..5            — pinned via taskset -c
SERVER_MASK="0-$((SERVER_T-1))"
BENCH_THREADS=$((6 - SERVER_T))
[ $BENCH_THREADS -lt 1 ] && BENCH_THREADS=1
BENCH_MASK="$SERVER_T-5"

OUT="$ROOT/test/bench/profile_${GIT}_${CMD}_t${SERVER_T}.txt"

mkconf() {
  cat <<EOF
port 2000
network proactor
maxclients 10000
loglevel error
persistence none
rdma_sync off
xdp no
sockmap no
threads $SERVER_T
cpu_pin auto
EOF
}

mkconf > /tmp/profile_kvs.conf

pkill -9 kvstore 2>/dev/null; sleep 1
echo "[1/4] starting kvstore on cores $SERVER_MASK with t=$SERVER_T..."
setsid taskset -c "$SERVER_MASK" "$KVS" /tmp/profile_kvs.conf \
       >/tmp/kvs_profile.log 2>&1 </dev/null &
disown
sleep 2
KVS_PID=$(pgrep -nf "$KVS")
[ -z "$KVS_PID" ] && { echo "kvstore failed to start"; exit 1; }
echo "    pid=$KVS_PID, status=$(cat /proc/$KVS_PID/comm 2>/dev/null)"

# Start a long-running benchmark in background, then perf-attach to kvstore
echo "[2/4] starting redis-benchmark on cores $BENCH_MASK with $BENCH_THREADS threads ($CMD)..."
case "$CMD" in
  set|get|mset)
    BENCH_OPTS="-t $CMD"
    ;;
  mixed)
    BENCH_OPTS="-t set,get,mset,lpush,sadd,hset"
    ;;
  *)
    BENCH_OPTS="-t $CMD"
    ;;
esac
taskset -c "$BENCH_MASK" \
  redis-benchmark -p 2000 $BENCH_OPTS \
                  -c 200 --threads $BENCH_THREADS -P 10 \
                  -n 100000000 -q \
                  >/tmp/bench_profile.log 2>&1 &
BENCH_PID=$!
sleep 3   # let benchmark ramp up

{
  echo "============================================================"
  echo "kvstore profile — captured $(date -Is)"
  echo "  host         : $HOST"
  echo "  build hash   : $GIT"
  echo "  scenario     : $CMD heavy, t=$SERVER_T workers, ${BENCH_THREADS}x bench threads"
  echo "  server cores : $SERVER_MASK"
  echo "  bench cores  : $BENCH_MASK"
  echo "  duration     : ${DURATION}s"
  echo "============================================================"
  echo

  echo "## 1. perf top — top 20 functions"
  echo
  timeout $((DURATION + 5)) perf top -p $KVS_PID -d $DURATION --stdio 2>/dev/null \
    | head -45 || echo "(perf top may need root or 'perf_event_paranoid' lowered)"
  echo

  echo "## 2. perf stat — IPC, cache, branches"
  echo
  perf stat -p $KVS_PID --no-big-num \
    -e cycles,instructions,branches,branch-misses,cache-references,cache-misses,task-clock,context-switches \
    -- sleep $DURATION 2>&1 | tail -20
  echo

  echo "## 3. process status"
  echo
  if [ -d /proc/$KVS_PID ]; then
    grep -E '^(VmRSS|VmPeak|Threads|voluntary|nonvoluntary)' /proc/$KVS_PID/status
  fi
  echo

  echo "## 4. bench output (last lines)"
  echo
  cat /tmp/bench_profile.log | tr '\r' '\n' | grep -E 'requests per second|rps=' | tail -5
} | tee "$OUT"

echo
echo "[3/4] cleaning up..."
kill $BENCH_PID 2>/dev/null
pkill -9 kvstore 2>/dev/null
wait 2>/dev/null

echo "[4/4] profile written to: $OUT"
