#!/bin/bash
# 8c P-core baseline capture — for the i7-13620H VM with 6P+2E exposed and
# P-cores pinned via taskset mask 0x3f (cores 0-5).  Writes to a dedicated
# file so it does NOT overwrite the locked 6c Phase 8.4 baseline.
#
# This is a CO-LOCATED bench (client + server on the same VM).  The "real"
# scaling曲线 needs a 2nd VM (see vs_redis.sh for that).  This script
# documents the upper bound achievable with single-machine bench, which is
# still useful as a "fast loop" yardstick during code changes.

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
KVS="$ROOT/build/kvstore"
HOST=$(hostname -s)
KERNEL=$(uname -r)
GIT=$(cd "$ROOT" && git rev-parse --short HEAD 2>/dev/null || echo "?")

# Force the entire script (and child processes) onto P-cores only.
# Mask 0x3f = cores 0..5 = the 6 P-cores on this box.
export TASKSET_MASK="0-5"

OUT="$ROOT/test/bench/baseline_${HOST}_8c_pcore.txt"

BENCH_C=400
BENCH_N=1000000
BENCH_P=10

mkconf() {
  local t=$1 pin=$2
  cat <<EOF
port 2000
network proactor
maxclients 10000
loglevel error
persistence none
rdma_sync off
xdp no
sockmap no
threads $t
cpu_pin $pin
EOF
}

start_kvs() {
  pkill -9 kvstore 2>/dev/null; sleep 1
  setsid taskset -c "$TASKSET_MASK" "$KVS" "$1" >/tmp/kvs_baseline_8c.log 2>&1 </dev/null &
  disown
  sleep 1
}

run_one() {
  local t=$1 pin=$2 bench_threads=$3
  local cfg=/tmp/baseline_8c_${t}_${pin}.conf
  mkconf $t $pin > $cfg
  start_kvs $cfg
  local R
  R=$(taskset -c "$TASKSET_MASK" redis-benchmark -p 2000 -t set,get \
        -c $BENCH_C -n $BENCH_N -P $BENCH_P --threads $bench_threads -q \
        2>&1 | tr -d '\r')
  pkill -9 kvstore 2>/dev/null
  local SET GET
  SET=$(echo "$R" | grep -oE 'SET: [0-9.]+' | awk '{print $2}')
  GET=$(echo "$R" | grep -oE 'GET: [0-9.]+' | awk '{print $2}')
  printf "  t=%d pin=%-4s bench_threads=%d  SET=%10s  GET=%10s\n" \
         $t "$pin" $bench_threads "$SET" "$GET"
}

{
  echo "============================================================"
  echo "kvstore 8c P-core baseline — captured $(date -Is)"
  echo "  host       : $HOST"
  echo "  CPU        : i7-13620H (6P + 2E exposed in VM)"
  echo "  taskset    : 0x3f (cores 0-5 = 6 P-cores)"
  echo "  kernel     : $KERNEL"
  echo "  bench      : redis-benchmark -c $BENCH_C -n $BENCH_N -P $BENCH_P"
  echo "  build hash : $GIT"
  echo "  mode       : CO-LOCATED (client + server share same 6 P-cores)"
  echo "============================================================"
  echo
  BUILD_TYPE=$(awk -F= '/^CMAKE_BUILD_TYPE:/{print $2}' $ROOT/build/CMakeCache.txt 2>/dev/null)
  echo "  Build type  : ${BUILD_TYPE:-(unset)}"
  echo
  echo "Sweep — varying server worker count and bench_threads to expose the"
  echo "        co-location tradeoff (more server threads = fewer client threads):"
  echo
  # When server uses t workers and bench uses bench_threads, the total
  # CPU demand on 6 P-cores is t + bench_threads + ~1 (softirq).  We sweep
  # to find the optimal split.
  for t in 1 2 3 4 5; do
    bench_threads=$((6 - t))
    [ $bench_threads -lt 1 ] && bench_threads=1
    run_one $t auto $bench_threads
  done
  echo
  echo "Note: optimal split tends to be t≈2-3 with bench_threads≈3-4 because"
  echo "      the server is more CPU-bound than the bench client.  Real"
  echo "      scaling requires 2-VM separation; see test/bench/vs_redis.sh."
} | tee "$OUT"

echo
echo "Baseline written to: $OUT"
