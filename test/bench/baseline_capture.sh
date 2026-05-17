#!/bin/bash
# Phase 8.4 — Capture current 6c baseline so we have an honest reference
# point before any Phase 6/7 work begins.  Re-run on the bigger machine
# later with the same script to compare.
#
# Writes:
#   test/bench/baseline_6c.txt   (or baseline_<hostname>_<nproc>c.txt)
#
# Setup: persistence=none, rdma_sync=off, xdp=no, sockmap=no.
# Sweeps: t={1,2,4,5} × cpu_pin={off,auto} × {SET, GET}.

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
KVS="$ROOT/build/kvstore"

NPROC=$(nproc)
HOST=$(hostname -s)
# Default output: phase snapshot keyed by current git short-sha + host + nproc.
# Pass --baseline to (re)write the locked baseline file instead.  This guards
# against accidentally clobbering the Phase 8.4 reference.
TAG="$(cd "$ROOT" && git rev-parse --short HEAD 2>/dev/null || date +%s)"
if [ "${1:-}" = "--baseline" ]; then
  OUT="$ROOT/test/bench/baseline_${HOST}_${NPROC}c.txt"
else
  OUT="$ROOT/test/bench/snapshot_${TAG}_${HOST}_${NPROC}c.txt"
fi

# Bench params identical to the ones used in Phase 4 + Phase 5 notes so the
# numbers in the roadmap can be reproduced.
BENCH_C=400
BENCH_N=1000000
BENCH_P=10
BENCH_THREADS=6

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
  setsid "$KVS" "$1" >/tmp/kvs_baseline.log 2>&1 </dev/null &
  disown
  sleep 1
}

run_one() {
  local t=$1 pin=$2
  local cfg=/tmp/baseline_${t}_${pin}.conf
  mkconf $t $pin > $cfg
  start_kvs $cfg
  local R
  R=$(redis-benchmark -p 2000 -t set,get \
        -c $BENCH_C -n $BENCH_N -P $BENCH_P --threads $BENCH_THREADS -q \
        2>&1 | tr -d '\r')
  pkill -9 kvstore 2>/dev/null
  local SET GET
  SET=$(echo "$R" | grep -oE 'SET: [0-9.]+' | awk '{print $2}')
  GET=$(echo "$R" | grep -oE 'GET: [0-9.]+' | awk '{print $2}')
  printf "  t=%d pin=%-4s  SET=%10s  GET=%10s\n" $t "$pin" "$SET" "$GET"
}

{
  echo "============================================================"
  echo "kvstore baseline — captured $(date -Is)"
  echo "  host       : $HOST"
  echo "  nproc      : $NPROC"
  echo "  kernel     : $(uname -r)"
  echo "  bench      : redis-benchmark -c $BENCH_C -n $BENCH_N -P $BENCH_P --threads $BENCH_THREADS"
  echo "  build hash : $(cd $ROOT && git rev-parse --short HEAD 2>/dev/null || echo n/a)"
  echo "============================================================"
  echo ""
  BUILD_TYPE=$(awk -F= '/^CMAKE_BUILD_TYPE:/{print $2}' $ROOT/build/CMakeCache.txt 2>/dev/null)
  case "$BUILD_TYPE" in
    Release)        FLAGS="-O3 -DNDEBUG -fno-omit-frame-pointer" ;;
    RelWithDebInfo) FLAGS="-O2 -g -DNDEBUG -fno-omit-frame-pointer" ;;
    Debug)          FLAGS="-g" ;;
    "")             FLAGS="(none — default cmake build)" ;;
    *)              FLAGS="(custom $BUILD_TYPE)" ;;
  esac
  echo "  Build type  : ${BUILD_TYPE:-(unset)}"
  echo "  Build flags : $FLAGS"
  echo ""
  echo "Sweep (SET / GET QPS, requests-per-second):"
  echo ""
  for t in 1 2 4 5; do
    for pin in off auto; do
      run_one $t $pin
    done
  done
  echo ""
  echo "How to reproduce on a different machine:"
  echo "  ./test/bench/baseline_capture.sh"
  echo "Then diff baseline_<host>_<nproc>c.txt against this file."
} | tee "$OUT"

echo ""
echo "Baseline written to: $OUT"
