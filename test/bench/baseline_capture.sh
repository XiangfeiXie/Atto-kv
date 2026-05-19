#!/bin/bash
set -u

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
KVS="$ROOT/build/kvstore"
NPROC=$(nproc)
HOST=$(hostname -s)
TAG="$(cd "$ROOT" && git rev-parse --short HEAD 2>/dev/null || date +%s)"

MODE="standard"
WRITE_BASELINE=0
WORKER_SWEEP=""
PIN_SWEEP="off auto"
TASKSET_MASK=""
OUT=""

usage() {
  cat <<EOF
Usage: $0 [--baseline] [--pcore MASK] [--phase4] [--workers "1 2 4"] [--out FILE]

Modes:
  default        Snapshot current SET/GET baseline for local machine.
  --baseline     Write baseline_<host>_<nproc>c.txt instead of snapshot_<git>_*.txt.
  --pcore MASK   Run server and redis-benchmark under taskset, e.g. --pcore 0-5.
  --phase4       Compatibility mode for old bench_phase4.sh worker sweep.

Environment:
  BENCH_C=400 BENCH_N=1000000 BENCH_P=10 BENCH_THREADS=<nproc>
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --baseline) WRITE_BASELINE=1; shift ;;
    --pcore) MODE="pcore"; TASKSET_MASK="$2"; shift 2 ;;
    --phase4) MODE="phase4"; WORKER_SWEEP="1 2 4 8"; PIN_SWEEP="off"; shift ;;
    --workers) WORKER_SWEEP="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

BENCH_C=${BENCH_C:-400}
BENCH_N=${BENCH_N:-1000000}
BENCH_P=${BENCH_P:-10}
BENCH_THREADS=${BENCH_THREADS:-$NPROC}

if [ -z "$WORKER_SWEEP" ]; then
  WORKER_SWEEP="1 2 4 $((NPROC-1)) $NPROC"
fi

if [ -z "$OUT" ]; then
  if [ "$MODE" = "pcore" ]; then
    OUT="$ROOT/test/bench/baseline_${HOST}_${NPROC}c_pcore.txt"
  elif [ "$WRITE_BASELINE" = "1" ]; then
    OUT="$ROOT/test/bench/baseline_${HOST}_${NPROC}c.txt"
  else
    OUT="$ROOT/test/bench/snapshot_${TAG}_${HOST}_${NPROC}c.txt"
  fi
fi

[ -x "$KVS" ] || { echo "missing $KVS" >&2; exit 1; }
command -v redis-benchmark >/dev/null || { echo "redis-benchmark required" >&2; exit 1; }

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

run_cmd() {
  if [ -n "$TASKSET_MASK" ]; then
    taskset -c "$TASKSET_MASK" "$@"
  else
    "$@"
  fi
}

start_kvs() {
  local conf=$1
  pkill -9 kvstore 2>/dev/null; sleep 1
  if [ -n "$TASKSET_MASK" ]; then
    setsid taskset -c "$TASKSET_MASK" "$KVS" "$conf" >/tmp/kvs_baseline.log 2>&1 </dev/null &
  else
    setsid "$KVS" "$conf" >/tmp/kvs_baseline.log 2>&1 </dev/null &
  fi
  disown
  sleep 1
}

run_one() {
  local t=$1 pin=$2
  local cfg=/tmp/baseline_${t}_${pin}.conf
  mkconf "$t" "$pin" > "$cfg"
  start_kvs "$cfg"
  local R SET GET
  R=$(run_cmd redis-benchmark -p 2000 -t set,get         -c "$BENCH_C" -n "$BENCH_N" -P "$BENCH_P" --threads "$BENCH_THREADS" -q         2>&1 | tr -d '')
  pkill -9 kvstore 2>/dev/null
  SET=$(echo "$R" | grep -oE 'SET: [0-9.]+' | awk '{print $2}')
  GET=$(echo "$R" | grep -oE 'GET: [0-9.]+' | awk '{print $2}')
  printf "  t=%d pin=%-4s bench_threads=%-3s SET=%10s  GET=%10s
"          "$t" "$pin" "$BENCH_THREADS" "${SET:-?}" "${GET:-?}"
}

{
  echo "============================================================"
  echo "kvstore baseline — captured $(date -Is)"
  echo "  host       : $HOST"
  echo "  nproc      : $NPROC"
  echo "  kernel     : $(uname -r)"
  echo "  mode       : $MODE"
  [ -n "$TASKSET_MASK" ] && echo "  taskset    : $TASKSET_MASK"
  echo "  bench      : redis-benchmark -c $BENCH_C -n $BENCH_N -P $BENCH_P --threads $BENCH_THREADS"
  echo "  build hash : $(cd "$ROOT" && git rev-parse --short HEAD 2>/dev/null || echo n/a)"
  echo "============================================================"
  echo
  BUILD_TYPE=$(awk -F= '/^CMAKE_BUILD_TYPE:/{print $2}' "$ROOT/build/CMakeCache.txt" 2>/dev/null)
  echo "  Build type  : ${BUILD_TYPE:-(unset)}"
  echo
  echo "Sweep (SET / GET QPS, requests-per-second):"
  echo
  for t in $WORKER_SWEEP; do
    for pin in $PIN_SWEEP; do
      run_one "$t" "$pin"
    done
  done
  echo
  echo "Examples:"
  echo "  ./test/bench/baseline_capture.sh --baseline"
  echo "  ./test/bench/baseline_capture.sh --pcore 0-5 --workers '1 2 3 4 5'"
  echo "  ./test/bench/baseline_capture.sh --phase4"
} | tee "$OUT"

echo
echo "Baseline written to: $OUT"
