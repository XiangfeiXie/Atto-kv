#!/bin/bash
# vs_redis.sh — head-to-head benchmark vs Redis 7.x and (if available) KeyDB.
#
# Runs the same redis-benchmark workload against:
#   1. our kvstore     (varying -t worker count)
#   2. redis-server    (single-threaded by default)
#   3. keydb-server    (multi-threaded if installed)
#
# CO-LOCATED mode (default): all on this VM, P-cores 0-5.
#   bench gets cores SERVER_T..5, server gets 0..SERVER_T-1.
#
# 2-VM mode (set REMOTE_HOST=...): assumes the user has spun up a 2nd VM
# running the server, and this VM is the bench client only.
#
# Output: test/bench/vs_redis_<git>_<host>.txt + .csv
#
# Usage:
#   ./test/bench/vs_redis.sh                  # local 1-VM head-to-head
#   REMOTE_HOST=192.168.x.x ./test/bench/vs_redis.sh
#
# Note: this script only RUNS bench against an already-running server when
# REMOTE_HOST is set; it doesn't try to start the server remotely.

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
KVS="$ROOT/build/kvstore"
HOST=$(hostname -s)
GIT=$(cd "$ROOT" && git rev-parse --short HEAD 2>/dev/null || echo "?")
REMOTE_HOST="${REMOTE_HOST:-}"

# Bench params chosen to match dev_quick.sh — keeps numbers comparable.
BENCH_C=200
BENCH_N=500000
BENCH_P=10

OUT_TXT="$ROOT/test/bench/vs_redis_${GIT}_${HOST}.txt"
OUT_CSV="$ROOT/test/bench/vs_redis_${GIT}_${HOST}.csv"

# ---------- helpers ----------
mkconf_kvs() {
  local t=$1
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
cpu_pin auto
EOF
}

start_kvs() {
  local t=$1
  pkill -9 kvstore 2>/dev/null; sleep 1
  mkconf_kvs $t > /tmp/vs_redis_kvs.conf
  setsid taskset -c "0-$((t-1))" "$KVS" /tmp/vs_redis_kvs.conf \
         >/tmp/vs_redis_kvs.log 2>&1 </dev/null &
  disown
  sleep 1
}

start_redis() {
  pkill -9 redis-server 2>/dev/null; sleep 1
  setsid taskset -c "0" redis-server --port 6379 --daemonize no \
         --save '' --appendonly no --loglevel warning \
         >/tmp/vs_redis_redis.log 2>&1 </dev/null &
  disown
  sleep 1
}

start_keydb() {
  command -v keydb-server >/dev/null || return 1
  pkill -9 keydb-server 2>/dev/null; sleep 1
  setsid taskset -c "0-3" keydb-server --port 6380 --daemonize no \
         --save '' --appendonly no --loglevel warning \
         --server-threads 4 \
         >/tmp/vs_redis_keydb.log 2>&1 </dev/null &
  disown
  sleep 1
}

# Run bench against (host, port) using bench_threads on cores bench_mask
bench_one() {
  local label=$1 host=$2 port=$3 bench_mask=$4 bench_threads=$5
  local SET GET R
  R=$(taskset -c "$bench_mask" \
        redis-benchmark -h $host -p $port -t set,get \
        -c $BENCH_C -n $BENCH_N -P $BENCH_P --threads $bench_threads -q \
        2>&1 | tr -d '\r')
  SET=$(echo "$R" | grep -oE 'SET: [0-9.]+' | awk '{print $2}')
  GET=$(echo "$R" | grep -oE 'GET: [0-9.]+' | awk '{print $2}')
  printf "  %-30s SET=%10s  GET=%10s\n" "$label" "${SET:-?}" "${GET:-?}"
  printf "%s,%s,%s,%s\n" "$label" "${SET:-0}" "${GET:-0}" "$bench_threads" >> "$OUT_CSV"
}

# ---------- main ----------
> "$OUT_CSV"
echo "label,SET_qps,GET_qps,bench_threads" > "$OUT_CSV"

{
  echo "============================================================"
  echo "Head-to-head benchmark — $(date -Is)"
  echo "  host        : $HOST"
  echo "  build hash  : $GIT"
  echo "  bench       : redis-benchmark -c $BENCH_C -n $BENCH_N -P $BENCH_P"
  echo "  mode        : ${REMOTE_HOST:+REMOTE-VM ($REMOTE_HOST)}${REMOTE_HOST:-CO-LOCATED on 6 P-cores}"
  echo "============================================================"
  echo
  echo "Note: SET/GET QPS in requests/second."
  echo

  if [ -n "$REMOTE_HOST" ]; then
    # Remote mode: assumed server is already running on REMOTE_HOST
    echo "## remote server mode — bench client uses all 6 P-cores"
    bench_one "remote-server" "$REMOTE_HOST" 2000 "0-5" 6
  else
    # Local co-located sweep
    echo "## kvstore (ours) — varying server worker count"
    for t in 1 2 3 4 5; do
      bench_threads=$((6 - t))
      [ $bench_threads -lt 1 ] && bench_threads=1
      start_kvs $t
      bench_one "kvstore t=$t" 127.0.0.1 2000 "$t-5" $bench_threads
      pkill -9 kvstore 2>/dev/null; sleep 1
    done
    echo

    echo "## Redis 7.x — single-threaded server"
    if command -v redis-server >/dev/null; then
      start_redis
      # Redis is single-threaded → leave 5 cores for bench
      bench_one "redis-server t=1" 127.0.0.1 6379 "1-5" 5
      pkill -9 redis-server 2>/dev/null; sleep 1
    else
      echo "  (redis-server not installed — skipping)"
    fi
    echo

    echo "## KeyDB — multi-threaded server (if installed)"
    if command -v keydb-server >/dev/null; then
      start_keydb
      bench_one "keydb-server t=4" 127.0.0.1 6380 "4-5" 2
      pkill -9 keydb-server 2>/dev/null; sleep 1
    else
      echo "  (keydb-server not installed — install via 'apt install keydb' to compare)"
    fi
  fi

  echo
  echo "CSV (label, SET_qps, GET_qps, bench_threads): $OUT_CSV"
} | tee "$OUT_TXT"

echo
echo "Output: $OUT_TXT"
